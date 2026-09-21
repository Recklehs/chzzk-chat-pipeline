import argparse
import json
import os
import re
import statistics
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from collector.benchmark_input import write_replay_input_from_delta


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SNAPSHOT_ROOT = Path("/Users/kanghyoseung/PycharmProjects/chzzk_container/data/output/chat_bdy_stream")
DEFAULT_INPUT_ROOT = Path("data/output/benchmarks/input")
DEFAULT_RESULTS_ROOT = Path("data/output/benchmarks/results")
COMPOSE_FILES = ("kafka/compose.yaml", "kafka/compose.benchmark.yaml")
SCENARIO_DURATIONS = {
    "steady-chat": 15 * 60,
    "burst-batch": 10 * 60,
    "fanout-idle": 30 * 60,
}
JMX_URL = "http://127.0.0.1:19404/metrics"


@dataclass(frozen=True)
class RunSpec:
    phase: str
    scenario: str
    run_index: int
    run_id: str
    compose_project: str
    duration_seconds: int
    warmup_seconds: int
    input_path: Path
    output_path: Path


def sanitize_slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip("-").lower()


def parse_size_bytes(value: str) -> float:
    match = re.fullmatch(r"\s*([0-9.]+)\s*([kKMGT]?i?B|B)\s*", value)
    if not match:
        return 0.0
    number = float(match.group(1))
    unit = match.group(2)
    factors = {
        "B": 1,
        "kB": 1000,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "TiB": 1024**4,
    }
    return number * factors.get(unit, 1)


def parse_percent(value: str) -> float:
    return float(value.strip().removesuffix("%") or 0)


def parse_io_pair(value: str) -> tuple[float, float]:
    left, _, right = value.partition("/")
    return parse_size_bytes(left.strip()), parse_size_bytes(right.strip())


def metric_counter_total(summary: dict[str, Any], name: str) -> float:
    return sum(float(item["value"]) for item in summary.get("metrics", {}).get("counters", {}).get(name, []))


def metric_histogram_sum(summary: dict[str, Any], name: str) -> float:
    return sum(float(item["sum"]) for item in summary.get("metrics", {}).get("histograms", {}).get(name, []))


def compose_command(project: str, *args: str) -> list[str]:
    command = ["docker", "compose", "-p", project]
    for compose_file in COMPOSE_FILES:
        command.extend(["-f", compose_file])
    command.extend(args)
    return command


def run_checked(command: list[str], *, env: dict[str, str] | None = None, cwd: Path = PROJECT_ROOT) -> subprocess.CompletedProcess:
    return subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True, check=True)


def run_checked_streaming(command: list[str], *, env: dict[str, str] | None = None, cwd: Path = PROJECT_ROOT) -> None:
    subprocess.run(command, cwd=cwd, env=env, text=True, check=True)


def ensure_replay_input(snapshot_root: Path, input_root: Path, scenario: str) -> Path:
    input_root.mkdir(parents=True, exist_ok=True)
    output_path = input_root / f"{scenario}.ndjson"
    count = write_replay_input_from_delta(snapshot_root, output_path, scenario=scenario)
    if count == 0:
        raise RuntimeError(f"Replay input generation produced no records for {scenario}")
    return output_path


class DockerStatsSampler:
    def __init__(self, project: str, interval_seconds: float):
        self.project = project
        self.interval_seconds = interval_seconds
        self.samples: list[dict[str, Any]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._started = False
        self._stopped = False

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._thread.start()

    def stop(self) -> dict[str, Any]:
        if not self._started:
            return {}
        if not self._stopped:
            self._stop.set()
            self._thread.join(timeout=max(2.0, self.interval_seconds + 1.0))
            self._stopped = True
        return summarize_docker_stats(self.samples, self.project)

    def _run(self) -> None:
        while not self._stop.is_set():
            self.samples.extend(sample_docker_stats(self.project))
            self._stop.wait(self.interval_seconds)


def sample_docker_stats(project: str) -> list[dict[str, Any]]:
    result = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return []
    samples = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = item.get("Name", "")
        if name.startswith(f"{project}-"):
            item["sampled_at"] = time.time()
            samples.append(item)
    return samples


def _service_name(project: str, container_name: str) -> str:
    service = container_name.removeprefix(f"{project}-")
    return service.removesuffix("-1")


def summarize_docker_stats(samples: list[dict[str, Any]], project: str) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for sample in samples:
        service = _service_name(project, str(sample.get("Name", "")))
        grouped.setdefault(service, []).append(sample)

    summary = {}
    for service, service_samples in grouped.items():
        cpu_values = [parse_percent(str(sample.get("CPUPerc", "0%"))) for sample in service_samples]
        mem_values = [
            parse_io_pair(str(sample.get("MemUsage", "0B / 0B")))[0] / (1024 * 1024)
            for sample in service_samples
        ]
        net_values = [parse_io_pair(str(sample.get("NetIO", "0B / 0B"))) for sample in service_samples]
        rx_delta = max(0.0, net_values[-1][0] - net_values[0][0]) if len(net_values) >= 2 else 0.0
        tx_delta = max(0.0, net_values[-1][1] - net_values[0][1]) if len(net_values) >= 2 else 0.0
        summary[service] = {
            "samples": len(service_samples),
            "cpu_percent_avg": statistics.fmean(cpu_values) if cpu_values else 0.0,
            "cpu_percent_max": max(cpu_values) if cpu_values else 0.0,
            "memory_mib_max": max(mem_values) if mem_values else 0.0,
            "network_rx_bytes_delta": rx_delta,
            "network_tx_bytes_delta": tx_delta,
        }
    return summary


def scrape_kafka_jmx(topic: str, url: str = JMX_URL) -> dict[str, float]:
    try:
        payload = urlopen(url, timeout=5).read().decode("utf-8")
    except Exception:
        return {}

    metrics: dict[str, float] = {}
    for line in payload.splitlines():
        if not line or line.startswith("#") or f'topic="{topic}"' not in line:
            continue
        name, _, raw_value = line.partition(" ")
        metric_name = name.split("{", 1)[0]
        try:
            metrics[metric_name] = float(raw_value)
        except ValueError:
            continue
    return metrics


def delta_metrics(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    return {name: max(0.0, after.get(name, 0.0) - before.get(name, 0.0)) for name in sorted(set(before) | set(after))}


def build_run_specs(
    *,
    phase: str,
    scenarios: list[str],
    runs: int,
    warmup_seconds: int,
    input_root: Path,
    results_root: Path,
    compose_project_prefix: str,
    duration_override_seconds: int | None = None,
) -> list[RunSpec]:
    specs = []
    for scenario in scenarios:
        duration = duration_override_seconds if duration_override_seconds is not None else SCENARIO_DURATIONS[scenario]
        for run_index in range(1, runs + 1):
            run_id = sanitize_slug(f"{phase}-{scenario}-{run_index}")
            specs.append(
                RunSpec(
                    phase=phase,
                    scenario=scenario,
                    run_index=run_index,
                    run_id=run_id,
                    compose_project=sanitize_slug(f"{compose_project_prefix}-{phase}-{scenario}-{run_index}"),
                    duration_seconds=duration,
                    warmup_seconds=warmup_seconds,
                    input_path=input_root / f"{scenario}.ndjson",
                    output_path=results_root / phase / scenario / f"run-{run_index}.json",
                )
            )
    return specs


def run_one_spec(spec: RunSpec, sample_interval_seconds: float) -> dict[str, Any]:
    env = os.environ.copy()
    env.update(
        {
            "COMPOSE_PROJECT_NAME": spec.compose_project,
            "BENCHMARK_RUN_ID": spec.run_id,
            "BENCHMARK_INPUT": f"/app/data/output/benchmarks/input/{spec.scenario}.ndjson",
            "BENCHMARK_OUTPUT": f"/app/data/output/benchmarks/results/{spec.phase}/{spec.scenario}/run-{spec.run_index}.json",
            "BENCHMARK_WARMUP_SECONDS": str(spec.warmup_seconds),
            "BENCHMARK_DURATION_SECONDS": str(spec.duration_seconds),
            "BENCHMARK_LOOP_INPUT": "true",
            "BENCHMARK_METRICS_LINGER_SECONDS": "0",
        }
    )
    spec.output_path.parent.mkdir(parents=True, exist_ok=True)
    sampler = DockerStatsSampler(spec.compose_project, sample_interval_seconds)
    topic = f"chzzk.events.raw.benchmark.{spec.run_id}"

    try:
        run_checked(compose_command(spec.compose_project, "up", "-d", "kafka", "kafka-jmx-exporter"), env=env)
        subprocess.run(
            compose_command(spec.compose_project, "up", "-d", "cadvisor"),
            cwd=PROJECT_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        time.sleep(5)
        jmx_before = scrape_kafka_jmx(topic)
        sampler.start()
        run_checked_streaming(
            compose_command(
                spec.compose_project,
                "up",
                "--build",
                "--abort-on-container-exit",
                "--exit-code-from",
                "collector-bench",
                "collector-bench",
            ),
            env=env,
        )
        docker_stats = sampler.stop()
        jmx_after = scrape_kafka_jmx(topic)
        summary = json.loads(spec.output_path.read_text(encoding="utf-8"))
        summary["docker_stats"] = docker_stats
        summary["kafka_jmx_delta"] = delta_metrics(jmx_before, jmx_after)
        summary["benchmark_spec"] = {
            "phase": spec.phase,
            "scenario": spec.scenario,
            "run_index": spec.run_index,
            "warmup_seconds": spec.warmup_seconds,
            "duration_seconds": spec.duration_seconds,
            "compose_project": spec.compose_project,
        }
        spec.output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return summary
    finally:
        sampler.stop()
        subprocess.run(
            compose_command(spec.compose_project, "down", "-v", "--remove-orphans"),
            cwd=PROJECT_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )


def bytes_in_delta(summary: dict[str, Any]) -> float:
    for name, value in summary.get("kafka_jmx_delta", {}).items():
        if "bytesinpersec" in name.lower():
            return float(value)
    return 0.0


def messages_in_delta(summary: dict[str, Any]) -> float:
    for name, value in summary.get("kafka_jmx_delta", {}).items():
        if "messagesinpersec" in name.lower():
            return float(value)
    return 0.0


def derived_kpis(summary: dict[str, Any]) -> dict[str, float]:
    acked_events = max(float(summary.get("acked_events", 0)), 1.0)
    duration_seconds = max(float(summary.get("duration_seconds", 0)), 0.001)
    collector_stats = summary.get("docker_stats", {}).get("collector-bench", {})
    collector_cpu_sec = float(collector_stats.get("cpu_percent_avg", 0.0)) / 100.0 * duration_seconds
    return {
        "acked_events": float(summary.get("acked_events", 0)),
        "publish_failures": float(summary.get("publish_failures", 0)),
        "events_dropped": float(summary.get("events_dropped", 0)),
        "duration_seconds": duration_seconds,
        "publish_p95_seconds": float(summary.get("publish_p95_seconds", 0.0)),
        "ws_json_parse_seconds_sum": metric_histogram_sum(summary, "chzzk_ws_json_parse_seconds"),
        "kafka_payload_bytes_per_event": metric_counter_total(summary, "chzzk_kafka_payload_bytes_total") / acked_events,
        "broker_bytes_in_per_event": bytes_in_delta(summary) / acked_events,
        "broker_messages_in": messages_in_delta(summary),
        "collector_cpu_sec_per_10k_events": collector_cpu_sec / acked_events * 10_000,
        "collector_rss_mib": float(collector_stats.get("memory_mib_max", 0.0)),
        "collector_network_tx_bytes_per_event": float(collector_stats.get("network_tx_bytes_delta", 0.0)) / acked_events,
    }


def median_kpis(result_paths: list[Path]) -> dict[str, dict[str, float]]:
    grouped: dict[str, list[dict[str, float]]] = {}
    for result_path in result_paths:
        summary = json.loads(result_path.read_text(encoding="utf-8"))
        grouped.setdefault(str(summary["scenario"]), []).append(derived_kpis(summary))

    medians = {}
    for scenario, rows in grouped.items():
        medians[scenario] = {
            key: statistics.median(row[key] for row in rows)
            for key in rows[0]
        }
    return medians


def write_report(phase: str, phase_root: Path, baseline_root: Path | None = None) -> dict[str, Any]:
    result_paths = sorted(phase_root.glob("*/*.json"))
    medians = median_kpis(result_paths)
    baseline = median_kpis(sorted(baseline_root.glob("*/*.json"))) if baseline_root else {}
    report = {"phase": phase, "medians": medians, "baseline_medians": baseline}
    (phase_root / "summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    lines = [f"# Benchmark Report: {phase}", "", "| Scenario | Acked | Failures | Drops | p95 publish | CPU sec / 10k | RSS MiB | Broker bytes / event |"]
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for scenario, values in sorted(medians.items()):
        lines.append(
            "| {scenario} | {acked:.0f} | {failures:.0f} | {drops:.0f} | {p95:.6f} | {cpu:.6f} | {rss:.2f} | {broker:.2f} |".format(
                scenario=scenario,
                acked=values["acked_events"],
                failures=values["publish_failures"],
                drops=values["events_dropped"],
                p95=values["publish_p95_seconds"],
                cpu=values["collector_cpu_sec_per_10k_events"],
                rss=values["collector_rss_mib"],
                broker=values["broker_bytes_in_per_event"],
            )
        )
    (phase_root / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Run the full WebSocket Kafka benchmark matrix.")
    parser.add_argument("--phase", required=True)
    parser.add_argument("--snapshot-root", type=Path, default=DEFAULT_SNAPSHOT_ROOT)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--results-root", type=Path, default=DEFAULT_RESULTS_ROOT)
    parser.add_argument("--scenarios", nargs="+", choices=sorted(SCENARIO_DURATIONS), default=sorted(SCENARIO_DURATIONS))
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmup-seconds", type=int, default=120)
    parser.add_argument("--duration-override-seconds", type=int)
    parser.add_argument("--sample-interval-seconds", type=float, default=10.0)
    parser.add_argument("--compose-project-prefix", default="wsbench")
    parser.add_argument("--baseline-results", type=Path)
    parser.add_argument("--skip-input-generation", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_root = args.input_root
    results_root = args.results_root
    if not args.skip_input_generation:
        for scenario in args.scenarios:
            ensure_replay_input(args.snapshot_root, input_root, scenario)

    specs = build_run_specs(
        phase=args.phase,
        scenarios=args.scenarios,
        runs=args.runs,
        warmup_seconds=args.warmup_seconds,
        input_root=input_root,
        results_root=results_root,
        compose_project_prefix=args.compose_project_prefix,
        duration_override_seconds=args.duration_override_seconds,
    )
    if args.dry_run:
        print(json.dumps([spec.__dict__ | {"input_path": str(spec.input_path), "output_path": str(spec.output_path)} for spec in specs], indent=2))
        return 0

    for spec in specs:
        print(json.dumps({"event": "benchmark_run_start", "run_id": spec.run_id, "scenario": spec.scenario}))
        run_one_spec(spec, args.sample_interval_seconds)
        print(json.dumps({"event": "benchmark_run_done", "run_id": spec.run_id, "output": str(spec.output_path)}))

    write_report(args.phase, results_root / args.phase, args.baseline_results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
