from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_benchmark_compose_defines_collector_bench_and_observability_services():
    compose_path = PROJECT_ROOT / "kafka" / "compose.benchmark.yaml"

    assert compose_path.is_file()
    compose_text = compose_path.read_text(encoding="utf-8")

    assert "  collector-bench:" in compose_text
    assert "  benchmark-topic-init:" in compose_text
    assert "kafka-topics.sh" in compose_text
    assert "--if-not-exists" in compose_text
    assert "python -m collector.benchmark_replay" in compose_text
    assert "KAFKA_BOOTSTRAP_SERVERS=kafka:29092" in compose_text
    assert "BENCHMARK_INPUT=${BENCHMARK_INPUT:-/app/tests/fixtures/replay/steady-chat.ndjson}" in compose_text
    assert "BENCHMARK_WARMUP_SECONDS=${BENCHMARK_WARMUP_SECONDS:-0}" in compose_text
    assert "BENCHMARK_DURATION_SECONDS=${BENCHMARK_DURATION_SECONDS:-}" in compose_text
    assert "BENCHMARK_LOOP_INPUT=${BENCHMARK_LOOP_INPUT:-false}" in compose_text
    assert "METRICS_ENABLED=true" in compose_text
    assert "cpus:" in compose_text
    assert 'mem_limit: "4g"' in compose_text
    assert "  prometheus:" in compose_text
    assert "  grafana:" in compose_text
    assert "  cadvisor:" in compose_text
    assert "  kafka-jmx-exporter:" in compose_text
    assert "bitnami/jmx-exporter@sha256:672e315e0f34b3d35bfc28e74bf7296a22883843d2d7773d53bae5385981ab06" in compose_text
    assert "/app/.env.test:ro" in compose_text
    assert "data/output/benchmarks" in compose_text
    assert "cp /app/kafka/benchmark.env /app/.env.test" not in compose_text


def test_benchmark_env_and_prometheus_configs_are_tracked_and_scrape_expected_targets():
    env_path = PROJECT_ROOT / "kafka" / "benchmark.env"
    prometheus_path = PROJECT_ROOT / "kafka" / "observability" / "prometheus.yml"
    jmx_path = PROJECT_ROOT / "kafka" / "observability" / "kafka-jmx.yml"

    assert env_path.is_file()
    assert prometheus_path.is_file()
    assert jmx_path.is_file()

    env_text = env_path.read_text(encoding="utf-8")
    assert "APP_ENV=test" in env_text
    assert "EVENT_BUS_BACKEND=kafka" in env_text
    assert "KAFKA_BOOTSTRAP_SERVERS=kafka:29092" in env_text
    assert "KAFKA_PRODUCER_LINGER_MS=0" in env_text
    assert "KAFKA_PRODUCER_MAX_BATCH_SIZE=65536" in env_text
    assert "KAFKA_PRODUCER_COMPRESSION_TYPE=lz4" in env_text
    assert "METRICS_ENABLED=true" in env_text
    assert "API_KEY=" in env_text

    prometheus_text = prometheus_path.read_text(encoding="utf-8")
    assert "collector-bench:8000" in prometheus_text
    assert "cadvisor:8080" in prometheus_text
    assert "kafka-jmx-exporter:9404" in prometheus_text

    jmx_text = jmx_path.read_text(encoding="utf-8")
    assert "lowercaseOutputName: true" in jmx_text
    assert "kafka.server<type=BrokerTopicMetrics" in jmx_text


def test_collector_requirements_include_cramjam_for_kafka_compression():
    requirements_text = (PROJECT_ROOT / "requirements-collector.txt").read_text(encoding="utf-8")

    assert "cramjam" in requirements_text


def test_dockerignore_excludes_local_runtime_data_from_benchmark_image_context():
    dockerignore_text = (PROJECT_ROOT / ".dockerignore").read_text(encoding="utf-8")

    for ignored_path in [
        ".env",
        ".env.*",
        ".venv/",
        "__pycache__/",
        ".pytest_cache/",
        ".git/",
        "data/",
    ]:
        assert ignored_path in dockerignore_text
