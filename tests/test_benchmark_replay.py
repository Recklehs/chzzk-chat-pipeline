import asyncio
import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

from websockets.exceptions import ConnectionClosedOK


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class FakeRawPublisher:
    metrics_backend = "kafka"

    def __init__(self):
        self.started = 0
        self.stopped = 0
        self.calls = []

    async def start(self):
        self.started += 1

    async def stop(self):
        self.stopped += 1

    async def publish(self, channel_id, payload, raw_payload=None):
        self.calls.append((channel_id, payload, raw_payload))


def write_replay_input(path):
    messages = [
        {
            "channel_id": "channel-1",
            "scenario": "steady-chat",
            "received_at": "2026-04-23T10:00:00+09:00",
            "message": {"cmd": 93101, "bdy": {"msg": "hello", "cmd": 93101}},
        },
        {
            "channel_id": "channel-1",
            "scenario": "steady-chat",
            "received_at": "2026-04-23T10:00:01+09:00",
            "message": {
                "cmd": 93101,
                "bdy": [
                    {"msg": "batch-1", "cmd": 93101},
                    {"msg": "batch-2", "cmd": 93101},
                ],
            },
        },
    ]
    path.write_text("\n".join(json.dumps(item, ensure_ascii=False) for item in messages) + "\n", encoding="utf-8")


def test_replay_websocket_replays_messages_then_closes():
    module = importlib.import_module("collector.benchmark_replay")
    websocket = module.ReplayWebSocket(['{"cmd": 1}', '{"cmd": 2}'])

    assert asyncio.run(websocket.recv()) == '{"cmd": 1}'
    assert asyncio.run(websocket.recv()) == '{"cmd": 2}'

    try:
        asyncio.run(websocket.recv())
    except ConnectionClosedOK:
        pass
    else:
        raise AssertionError("ReplayWebSocket should close after all messages")


def test_file_replay_websocket_loops_input_until_stopped(tmp_path):
    module = importlib.import_module("collector.benchmark_replay")
    message_path = tmp_path / "messages.ndjson"
    message_path.write_text('{"cmd":1}\n{"cmd":2}\n', encoding="utf-8")
    websocket = module.FileReplayWebSocket(message_path, loop_input=True)

    assert asyncio.run(websocket.recv()) == '{"cmd":1}'
    assert asyncio.run(websocket.recv()) == '{"cmd":2}'
    assert asyncio.run(websocket.recv()) == '{"cmd":1}'

    websocket.close()


def test_split_replay_input_by_channel_writes_compact_message_files(tmp_path):
    module = importlib.import_module("collector.benchmark_replay")
    input_path = tmp_path / "fanout.ndjson"
    input_path.write_text(
        "\n".join(
            json.dumps(row, ensure_ascii=False)
            for row in [
                {
                    "channel_id": "channel-1",
                    "scenario": "fanout-idle",
                    "received_at": "2026-04-23T10:00:00+09:00",
                    "message": {"cmd": 1, "bdy": {"msg": "one"}},
                },
                {
                    "channel_id": "channel-2",
                    "scenario": "fanout-idle",
                    "received_at": "2026-04-23T10:00:01+09:00",
                    "message": {"cmd": 2, "bdy": {"msg": "two"}},
                },
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    split_dir = tmp_path / "split"

    split = module.split_replay_input_by_channel(input_path, split_dir)

    assert split.total_frames == 2
    assert split.scenarios == {"fanout-idle"}
    assert sorted(split.channel_message_paths) == ["channel-1", "channel-2"]
    assert (split.channel_message_paths["channel-1"]).read_text(encoding="utf-8").strip() == (
        json.dumps({"cmd": 1, "bdy": {"msg": "one"}}, ensure_ascii=False, separators=(",", ":"))
    )


def test_run_replay_uses_receive_messages_and_writes_summary(tmp_path):
    module = importlib.import_module("collector.benchmark_replay")
    input_path = tmp_path / "steady-chat.ndjson"
    output_path = tmp_path / "summary.json"
    write_replay_input(input_path)
    publisher = FakeRawPublisher()

    summary = asyncio.run(
        module.run_replay(
            input_path=input_path,
            output_path=output_path,
            raw_publisher=publisher,
            run_id="run-test",
            topic="chzzk.events.raw.run-test",
        )
    )

    persisted = json.loads(output_path.read_text(encoding="utf-8"))
    assert publisher.started == 1
    assert publisher.stopped == 1
    assert len(publisher.calls) == 2
    assert publisher.calls[0][0] == "channel-1"
    assert summary == persisted
    assert summary["run_id"] == "run-test"
    assert summary["topic"] == "chzzk.events.raw.run-test"
    assert summary["scenario"] == "steady-chat"
    assert summary["frames_replayed"] == 2
    assert summary["acked_events"] == 3
    assert summary["publish_failures"] == 0
    assert summary["events_dropped"] == 0
    assert summary["publish_p95_seconds"] >= 0


def test_run_replay_preserves_channel_id_per_replay_row(tmp_path):
    module = importlib.import_module("collector.benchmark_replay")
    input_path = tmp_path / "fanout-idle.ndjson"
    output_path = tmp_path / "summary.json"
    rows = [
        {
            "channel_id": "channel-1",
            "scenario": "fanout-idle",
            "received_at": "2026-04-23T10:00:00+09:00",
            "message": {"cmd": 93101, "bdy": {"msg": "one", "cmd": 93101}},
        },
        {
            "channel_id": "channel-2",
            "scenario": "fanout-idle",
            "received_at": "2026-04-23T10:00:01+09:00",
            "message": {"cmd": 93101, "bdy": {"msg": "two", "cmd": 93101}},
        },
    ]
    input_path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8")
    publisher = FakeRawPublisher()

    summary = asyncio.run(
        module.run_replay(
            input_path=input_path,
            output_path=output_path,
            raw_publisher=publisher,
            run_id="run-fanout",
            topic="chzzk.events.raw.run-fanout",
        )
    )

    assert [call[0] for call in publisher.calls] == ["channel-1", "channel-2"]
    assert summary["frames_replayed"] == 2
    assert summary["acked_events"] == 2


def test_run_replay_writes_metrics_snapshot_to_summary(tmp_path):
    module = importlib.import_module("collector.benchmark_replay")
    input_path = tmp_path / "steady-chat.ndjson"
    output_path = tmp_path / "summary.json"
    write_replay_input(input_path)
    publisher = FakeRawPublisher()

    summary = asyncio.run(
        module.run_replay(
            input_path=input_path,
            output_path=output_path,
            raw_publisher=publisher,
            run_id="run-test",
            topic="chzzk.events.raw.run-test",
        )
    )

    assert "metrics" in summary
    assert "chzzk_raw_publish_success_total" in summary["metrics"]["counters"]
    assert "chzzk_raw_publish_seconds" in summary["metrics"]["histograms"]


def test_async_main_passes_kafka_batching_settings_to_benchmark_publisher(monkeypatch, tmp_path):
    module = importlib.import_module("collector.benchmark_replay")
    input_path = tmp_path / "steady-chat.ndjson"
    output_path = tmp_path / "summary.json"
    write_replay_input(input_path)
    captured = {}

    class CapturingKafkaRawPublisher:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    async def fake_run_replay(**kwargs):
        return {"raw_publisher": kwargs["raw_publisher"].__class__.__name__}

    monkeypatch.setattr(module, "KafkaRawPublisher", CapturingKafkaRawPublisher)
    monkeypatch.setattr(module, "run_replay", fake_run_replay)
    monkeypatch.setattr(module, "start_metrics_http_server", lambda *args, **kwargs: None)
    monkeypatch.setenv("API_KEY", "test-key")
    monkeypatch.setenv("EVENT_BUS_BACKEND", "kafka")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "chzzk.events.raw.benchmark")
    monkeypatch.setenv("KAFKA_CLIENT_ID", "chzzk-collector-benchmark")
    monkeypatch.setenv("KAFKA_PRODUCER_LINGER_MS", "20")
    monkeypatch.setenv("KAFKA_PRODUCER_MAX_BATCH_SIZE", "65536")

    result = asyncio.run(
        module.async_main(
            [
                "--input",
                str(input_path),
                "--output",
                str(output_path),
                "--run-id",
                "settings-test",
            ]
        )
    )

    assert result == {"raw_publisher": "CapturingKafkaRawPublisher"}
    assert captured["bootstrap_servers"] == "localhost:9092"
    assert captured["topic"] == "chzzk.events.raw.benchmark.settings-test"
    assert captured["client_id"] == "chzzk-collector-benchmark-bench"
    assert captured["linger_ms"] == 20
    assert captured["max_batch_size"] == 65536


def test_async_main_passes_optional_kafka_settings_when_present(monkeypatch, tmp_path):
    module = importlib.import_module("collector.benchmark_replay")
    input_path = tmp_path / "steady-chat.ndjson"
    output_path = tmp_path / "summary.json"
    write_replay_input(input_path)
    captured = {}

    class CapturingKafkaRawPublisher:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    async def fake_run_replay(**kwargs):
        return {"raw_publisher": kwargs["raw_publisher"].__class__.__name__}

    monkeypatch.setattr(module, "KafkaRawPublisher", CapturingKafkaRawPublisher)
    monkeypatch.setattr(module, "run_replay", fake_run_replay)
    monkeypatch.setattr(module, "load_settings_from_env", lambda: SimpleNamespace(
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="chzzk.events.raw.benchmark",
        kafka_client_id="chzzk-collector-benchmark",
        kafka_producer_linger_ms=20,
        kafka_producer_max_batch_size=65536,
        kafka_producer_compression_type="lz4",
        raw_publish_control_frames=False,
        metrics_enabled=False,
    ))

    result = asyncio.run(
        module.async_main(
            [
                "--input",
                str(input_path),
                "--output",
                str(output_path),
                "--run-id",
                "optional-settings-test",
            ]
        )
    )

    assert result == {"raw_publisher": "CapturingKafkaRawPublisher"}
    assert captured["compression_type"] == "lz4"
    assert captured["publish_control_frames"] is False
