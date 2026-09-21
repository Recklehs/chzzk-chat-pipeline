import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_config_module():
    sys.modules.pop("collector.config", None)
    return importlib.import_module("collector.config")


def load_publisher_module():
    sys.modules.pop("collector.publisher", None)
    return importlib.import_module("collector.publisher")

def make_settings(control_module, *, backend="pubsub"):
    return control_module.AppSettings(
        api_key="test-key",
        event_bus_backend=backend,
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="chzzk.events.raw",
        kafka_client_id="chzzk-collector",
        pubsub_project_id="demo-project",
        pubsub_raw_topic="raw-topic",
    )


def test_load_settings_defaults_to_pubsub_and_uses_google_cloud_project(monkeypatch):
    module = load_config_module()
    monkeypatch.setattr(module, "load_runtime_env", lambda: None)
    monkeypatch.setenv("API_KEY", "test-key")
    monkeypatch.delenv("EVENT_BUS_BACKEND", raising=False)
    monkeypatch.delenv("PUBSUB_PROJECT_ID", raising=False)
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "fallback-project")
    monkeypatch.setenv("PUBSUB_RAW_TOPIC", "raw-topic")

    settings = module.load_settings_from_env()

    assert settings.event_bus_backend == "pubsub"
    assert settings.pubsub_project_id == "fallback-project"
    assert settings.pubsub_raw_topic == "raw-topic"
    assert settings.metrics_enabled is False


def test_load_settings_enables_metrics_from_env(monkeypatch):
    module = load_config_module()
    monkeypatch.setattr(module, "load_runtime_env", lambda: None)
    monkeypatch.setenv("API_KEY", "test-key")
    monkeypatch.setenv("EVENT_BUS_BACKEND", "kafka")
    monkeypatch.setenv("METRICS_ENABLED", "true")

    settings = module.load_settings_from_env()

    assert settings.metrics_enabled is True


def test_load_settings_reads_kafka_producer_batching_options(monkeypatch):
    module = load_config_module()
    monkeypatch.setattr(module, "load_runtime_env", lambda: None)
    monkeypatch.setenv("API_KEY", "test-key")
    monkeypatch.setenv("EVENT_BUS_BACKEND", "kafka")
    monkeypatch.setenv("KAFKA_PRODUCER_LINGER_MS", "20")
    monkeypatch.setenv("KAFKA_PRODUCER_MAX_BATCH_SIZE", "65536")

    settings = module.load_settings_from_env()

    assert settings.kafka_producer_linger_ms == 20
    assert settings.kafka_producer_max_batch_size == 65536


def test_load_settings_reads_live_poll_max_interval(monkeypatch):
    module = load_config_module()
    monkeypatch.setattr(module, "load_runtime_env", lambda: None)
    monkeypatch.setenv("API_KEY", "test-key")
    monkeypatch.setenv("CHZZK_LIVE_POLL_SECONDS", "15")
    monkeypatch.setenv("CHZZK_LIVE_POLL_MAX_SECONDS", "60")

    settings = module.load_settings_from_env()

    assert settings.chzzk_live_poll_seconds == 15
    assert settings.chzzk_live_poll_max_seconds == 60


def test_load_settings_reads_kafka_producer_compression_option(monkeypatch):
    module = load_config_module()
    monkeypatch.setattr(module, "load_runtime_env", lambda: None)
    monkeypatch.setenv("API_KEY", "test-key")
    monkeypatch.setenv("EVENT_BUS_BACKEND", "kafka")
    monkeypatch.setenv("KAFKA_PRODUCER_COMPRESSION_TYPE", "lz4")

    settings = module.load_settings_from_env()

    assert settings.kafka_producer_compression_type == "lz4"


def test_load_settings_rejects_invalid_event_bus_backend(monkeypatch):
    module = load_config_module()
    monkeypatch.setattr(module, "load_runtime_env", lambda: None)
    monkeypatch.setenv("API_KEY", "test-key")
    monkeypatch.setenv("EVENT_BUS_BACKEND", "invalid")

    with pytest.raises(ValueError, match="Unsupported EVENT_BUS_BACKEND"):
        module.load_settings_from_env()


def test_build_default_raw_publisher_selects_backend():
    control_module = importlib.import_module("collector.control")
    publisher_module = load_publisher_module()

    pubsub_settings = make_settings(control_module, backend="pubsub")
    kafka_settings = make_settings(control_module, backend="kafka")

    assert isinstance(publisher_module.build_default_raw_publisher(pubsub_settings), publisher_module.PubSubRawPublisher)
    assert isinstance(publisher_module.build_default_raw_publisher(kafka_settings), publisher_module.KafkaRawPublisher)


def test_build_default_raw_publisher_passes_kafka_batching_options():
    control_module = importlib.import_module("collector.control")
    publisher_module = load_publisher_module()
    settings = control_module.AppSettings(
        api_key="test-key",
        event_bus_backend="kafka",
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="chzzk.events.raw",
        kafka_client_id="chzzk-collector",
        kafka_producer_linger_ms=20,
        kafka_producer_max_batch_size=65536,
        kafka_producer_compression_type="lz4",
    )

    publisher = publisher_module.build_default_raw_publisher(settings)

    assert publisher.linger_ms == 20
    assert publisher.max_batch_size == 65536
    assert publisher.compression_type == "lz4"
