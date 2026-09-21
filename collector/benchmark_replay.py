import argparse
import asyncio
import json
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from websockets.exceptions import ConnectionClosedOK

from collector.config import load_settings_from_env
from collector.control import CmdCounter
from collector.metrics import metrics, start_metrics_http_server
from collector.publisher import KafkaRawPublisher
from collector.runtime import receive_messages


class ReplayWebSocket:
    """Minimal websocket-compatible shim for receive_messages replay runs."""

    def __init__(self, messages: list[str]):
        self._messages = list(messages)

    async def recv(self):
        if self._messages:
            return self._messages.pop(0)
        raise ConnectionClosedOK(None, None)


class FileReplayWebSocket:
    """Websocket-compatible shim that streams compact JSON messages from a file."""

    def __init__(
        self,
        message_path: Path,
        *,
        loop_input: bool = False,
        stop_at_monotonic: float | None = None,
    ):
        self.message_path = message_path
        self.loop_input = loop_input
        self.stop_at_monotonic = stop_at_monotonic
        self._handle = message_path.open(encoding="utf-8")

    async def recv(self):
        if self.stop_at_monotonic is not None and time.monotonic() >= self.stop_at_monotonic:
            raise ConnectionClosedOK(None, None)

        while True:
            line = self._handle.readline()
            if line:
                return line.strip()
            if not self.loop_input:
                raise ConnectionClosedOK(None, None)
            self._handle.seek(0)
            if self._handle.readline() == "":
                raise ConnectionClosedOK(None, None)
            self._handle.seek(0)

    def close(self):
        self._handle.close()


@dataclass(frozen=True)
class SplitReplayInput:
    total_frames: int
    scenarios: set[str]
    channel_message_paths: dict[str, Path]


def iter_replay_records(input_path: Path):
    with input_path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                item = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid NDJSON at {input_path}:{line_number}: {exc}") from exc

            if not isinstance(item, dict) or "channel_id" not in item or "message" not in item:
                raise ValueError(f"Replay row must contain channel_id and message at {input_path}:{line_number}")
            yield item


def load_replay_records(input_path: Path) -> list[dict]:
    return list(iter_replay_records(input_path))


def split_replay_input_by_channel(input_path: Path, split_dir: Path) -> SplitReplayInput:
    split_dir.mkdir(parents=True, exist_ok=True)
    handles = {}
    channel_message_paths = {}
    scenarios = set()
    total_frames = 0

    try:
        for item in iter_replay_records(input_path):
            channel_id = str(item["channel_id"])
            scenarios.add(str(item.get("scenario", "unknown")))
            if channel_id not in handles:
                message_path = split_dir / f"channel-{len(handles)}.ndjson"
                channel_message_paths[channel_id] = message_path
                handles[channel_id] = message_path.open("w", encoding="utf-8")
            handles[channel_id].write(
                json.dumps(item["message"], ensure_ascii=False, separators=(",", ":")) + "\n"
            )
            total_frames += 1
    finally:
        for handle in handles.values():
            handle.close()

    return SplitReplayInput(
        total_frames=total_frames,
        scenarios=scenarios,
        channel_message_paths=channel_message_paths,
    )


def make_topic(base_topic: str, run_id: str) -> str:
    suffix = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in run_id)
    return f"{base_topic}.{suffix}"


async def _run_split_replay(
    *,
    split: SplitReplayInput,
    scenario: str,
    raw_publisher,
    counter: CmdCounter,
    started_at: float,
    duration_seconds: float | None,
    loop_input: bool,
) -> None:
    stop_at = time.monotonic() + duration_seconds if duration_seconds is not None else None

    async def run_channel(channel_id: str, message_path: Path):
        websocket = FileReplayWebSocket(
            message_path,
            loop_input=loop_input,
            stop_at_monotonic=stop_at,
        )
        try:
            await receive_messages(
                websocket,
                channel_id,
                scenario,
                counter,
                {"value": started_at},
                raw_publisher,
            )
        finally:
            websocket.close()

    if duration_seconds is None:
        for channel_id, message_path in split.channel_message_paths.items():
            await run_channel(channel_id, message_path)
        return

    await asyncio.gather(
        *(run_channel(channel_id, message_path) for channel_id, message_path in split.channel_message_paths.items())
    )


async def run_replay(
    *,
    input_path: Path,
    output_path: Path,
    raw_publisher,
    run_id: str,
    topic: str,
    warmup_seconds: float = 0.0,
    duration_seconds: float | None = None,
    loop_input: bool = False,
) -> dict:
    metrics.reset()
    split_dir = output_path.parent / f".{run_id}-split"
    if split_dir.exists():
        shutil.rmtree(split_dir)
    split = split_replay_input_by_channel(input_path, split_dir)
    scenario_names = sorted(split.scenarios)
    scenario = scenario_names[0] if len(scenario_names) == 1 else "mixed"
    publish_failures = 0
    await raw_publisher.start()
    measured_started_at = time.time()
    try:
        if warmup_seconds > 0:
            await _run_split_replay(
                split=split,
                scenario=scenario,
                raw_publisher=raw_publisher,
                counter=CmdCounter(),
                started_at=time.time(),
                duration_seconds=warmup_seconds,
                loop_input=loop_input,
            )
            metrics.reset()

        counter = CmdCounter()
        measured_started_at = time.time()
        await _run_split_replay(
            split=split,
            scenario=scenario,
            raw_publisher=raw_publisher,
            counter=counter,
            started_at=measured_started_at,
            duration_seconds=duration_seconds,
            loop_input=loop_input,
        )
    except Exception:
        publish_failures += 1
        raise
    finally:
        await raw_publisher.stop()
        shutil.rmtree(split_dir, ignore_errors=True)

    stats = await counter.snapshot()
    metrics_snapshot = metrics.snapshot()
    finished_at = time.time()
    summary = {
        "run_id": run_id,
        "topic": topic,
        "scenario": scenario,
        "input_frames": split.total_frames,
        "frames_replayed": int(metrics.counter_total("chzzk_ws_frames_received_total")),
        "acked_events": stats["total_collected_count"],
        "publish_failures": publish_failures,
        "events_dropped": int(metrics.counter_total("chzzk_events_dropped_total")),
        "publish_p95_seconds": metrics.histogram_p95("chzzk_raw_publish_seconds"),
        "duration_seconds": round(finished_at - measured_started_at, 6),
        "warmup_seconds": warmup_seconds,
        "requested_duration_seconds": duration_seconds,
        "loop_input": loop_input,
        "metrics": metrics_snapshot,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return summary


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Replay CHZZK websocket NDJSON into Kafka for Phase 0 benchmarking.")
    parser.add_argument("--input", required=True, type=Path, help="NDJSON replay input path")
    parser.add_argument("--output", required=True, type=Path, help="JSON summary output path")
    parser.add_argument("--run-id", default=os.environ.get("BENCHMARK_RUN_ID", str(int(time.time()))))
    parser.add_argument("--topic-prefix", default=os.environ.get("BENCHMARK_TOPIC_PREFIX"))
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=float(os.environ.get("BENCHMARK_WARMUP_SECONDS", "0")),
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=float(os.environ["BENCHMARK_DURATION_SECONDS"]) if os.environ.get("BENCHMARK_DURATION_SECONDS") else None,
    )
    parser.add_argument(
        "--loop-input",
        action="store_true",
        default=os.environ.get("BENCHMARK_LOOP_INPUT", "").lower() in {"1", "true", "yes", "on"},
    )
    parser.add_argument(
        "--metrics-linger-seconds",
        type=float,
        default=float(os.environ.get("BENCHMARK_METRICS_LINGER_SECONDS", "0")),
        help="Seconds to keep the metrics HTTP server alive after replay completes.",
    )
    return parser.parse_args(argv)


async def async_main(argv: list[str] | None = None) -> dict:
    args = parse_args(argv)
    settings = load_settings_from_env()
    metrics_server = None
    if settings.metrics_enabled:
        metrics_server = start_metrics_http_server(
            host=os.environ.get("HOST", "0.0.0.0"),
            port=int(os.environ.get("PORT", "8000")),
        )
    topic_prefix = args.topic_prefix or settings.kafka_topic
    topic = make_topic(topic_prefix, args.run_id)
    publisher_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "topic": topic,
        "client_id": f"{settings.kafka_client_id}-bench",
        "linger_ms": settings.kafka_producer_linger_ms,
        "max_batch_size": settings.kafka_producer_max_batch_size,
    }
    if hasattr(settings, "kafka_producer_compression_type"):
        publisher_kwargs["compression_type"] = settings.kafka_producer_compression_type
    if hasattr(settings, "raw_publish_control_frames"):
        publisher_kwargs["publish_control_frames"] = settings.raw_publish_control_frames
    publisher = KafkaRawPublisher(**publisher_kwargs)
    try:
        summary = await run_replay(
            input_path=args.input,
            output_path=args.output,
            raw_publisher=publisher,
            run_id=args.run_id,
            topic=topic,
            warmup_seconds=args.warmup_seconds,
            duration_seconds=args.duration_seconds,
            loop_input=args.loop_input,
        )
        if metrics_server is not None and args.metrics_linger_seconds > 0:
            await asyncio.sleep(args.metrics_linger_seconds)
        return summary
    finally:
        if metrics_server is not None:
            metrics_server.shutdown()
            metrics_server.server_close()


def main(argv: list[str] | None = None) -> None:
    summary = asyncio.run(async_main(argv))
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
