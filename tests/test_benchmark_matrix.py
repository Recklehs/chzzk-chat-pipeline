import json
from pathlib import Path

from collector import benchmark_matrix as matrix


def test_parse_docker_size_and_io_values():
    assert matrix.parse_size_bytes("1.5kB") == 1500
    assert matrix.parse_size_bytes("2MiB") == 2 * 1024 * 1024
    assert matrix.parse_io_pair("1.5kB / 2MiB") == (1500, 2 * 1024 * 1024)


def test_summarize_docker_stats_groups_project_services():
    samples = [
        {
            "Name": "wsbench-main-steady-chat-1-collector-bench-1",
            "CPUPerc": "10.00%",
            "MemUsage": "100MiB / 512MiB",
            "NetIO": "1kB / 2kB",
        },
        {
            "Name": "wsbench-main-steady-chat-1-collector-bench-1",
            "CPUPerc": "20.00%",
            "MemUsage": "125MiB / 512MiB",
            "NetIO": "3kB / 7kB",
        },
        {
            "Name": "other-project-kafka-1",
            "CPUPerc": "99.00%",
            "MemUsage": "1GiB / 2GiB",
            "NetIO": "1GB / 1GB",
        },
    ]

    summary = matrix.summarize_docker_stats(samples, "wsbench-main-steady-chat-1")

    assert summary["collector-bench"]["samples"] == 2
    assert summary["collector-bench"]["cpu_percent_avg"] == 15.0
    assert summary["collector-bench"]["memory_mib_max"] == 125.0
    assert summary["collector-bench"]["network_rx_bytes_delta"] == 2000
    assert summary["collector-bench"]["network_tx_bytes_delta"] == 5000
    assert "kafka" not in summary


def test_docker_stats_sampler_stop_is_safe_before_and_after_start():
    sampler = matrix.DockerStatsSampler("wsbench-unused", interval_seconds=0.01)

    assert sampler.stop() == {}

    sampler.start()
    first_summary = sampler.stop()
    second_summary = sampler.stop()

    assert first_summary == second_summary


def test_build_run_specs_uses_unique_compose_projects(tmp_path: Path):
    specs = matrix.build_run_specs(
        phase="phase-2",
        scenarios=["steady-chat", "burst-batch"],
        runs=2,
        warmup_seconds=120,
        input_root=tmp_path / "input",
        results_root=tmp_path / "results",
        compose_project_prefix="wsbench",
        duration_override_seconds=5,
    )

    assert [spec.run_id for spec in specs] == [
        "phase-2-steady-chat-1",
        "phase-2-steady-chat-2",
        "phase-2-burst-batch-1",
        "phase-2-burst-batch-2",
    ]
    assert len({spec.compose_project for spec in specs}) == 4
    assert specs[0].duration_seconds == 5
    assert specs[0].output_path == tmp_path / "results" / "phase-2" / "steady-chat" / "run-1.json"


def test_write_report_computes_median_kpis(tmp_path: Path):
    phase_root = tmp_path / "phase-2"
    scenario_root = phase_root / "steady-chat"
    scenario_root.mkdir(parents=True)
    for index, acked_events in enumerate([10, 20, 30], start=1):
        (scenario_root / f"run-{index}.json").write_text(
            json.dumps(
                {
                    "scenario": "steady-chat",
                    "acked_events": acked_events,
                    "publish_failures": 0,
                    "events_dropped": 0,
                    "duration_seconds": 10,
                    "publish_p95_seconds": 0.01 * index,
                    "metrics": {
                        "counters": {"chzzk_kafka_payload_bytes_total": [{"labels": {}, "value": 1000}]},
                        "histograms": {"chzzk_ws_json_parse_seconds": [{"labels": {}, "sum": 0.1}]},
                    },
                    "docker_stats": {
                        "collector-bench": {
                            "cpu_percent_avg": 10,
                            "memory_mib_max": 100 + index,
                            "network_tx_bytes_delta": 500,
                        }
                    },
                    "kafka_jmx_delta": {"kafka_server_brokertopicmetrics_bytesinpersec_total": 2000},
                }
            ),
            encoding="utf-8",
        )

    report = matrix.write_report("phase-2", phase_root)

    assert report["medians"]["steady-chat"]["acked_events"] == 20
    assert (phase_root / "summary.json").is_file()
    assert "Benchmark Report: phase-2" in (phase_root / "report.md").read_text(encoding="utf-8")
