import importlib
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def load_module():
    sys.modules.pop("spark.kafka_raw_to_bronze", None)
    return importlib.import_module("spark.kafka_raw_to_bronze")


def test_runtime_settings_use_external_kafka_env(monkeypatch):
    module = load_module()
    monkeypatch.delenv("SPARK_BRONZE_OUTPUT_PATH", raising=False)
    monkeypatch.delenv("BRONZE_OUTPUT_PATH", raising=False)
    monkeypatch.delenv("SPARK_BRONZE_DEAD_LETTER_PATH", raising=False)
    monkeypatch.delenv("DEAD_LETTER_PATH", raising=False)
    monkeypatch.delenv("SPARK_BRONZE_CHECKPOINT_PATH", raising=False)
    monkeypatch.delenv("CHECKPOINT_PATH", raising=False)
    monkeypatch.setenv("GCS_BUCKET_URI", "gs://bucket/chzzk/local")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "custom.topic")
    monkeypatch.setenv("SPARK_APP_NAME", "custom-app")

    settings = module.load_runtime_settings()

    assert settings.spark_app_name == "custom-app"
    assert settings.kafka_bootstrap_servers == "kafka:9092"
    assert settings.kafka_topic == "custom.topic"
    assert settings.output_path == "gs://bucket/chzzk/local/bronze"
    assert settings.dead_letter_path == "gs://bucket/chzzk/local/dead_letter"
    assert settings.checkpoint_path == "gs://bucket/chzzk/local/checkpoint"


def test_runtime_settings_require_output_and_checkpoint(monkeypatch):
    module = load_module()
    monkeypatch.delenv("SPARK_BUCKET_URI", raising=False)
    monkeypatch.delenv("GCS_BUCKET_URI", raising=False)
    monkeypatch.delenv("SPARK_BRONZE_OUTPUT_PATH", raising=False)
    monkeypatch.delenv("BRONZE_OUTPUT_PATH", raising=False)
    monkeypatch.delenv("SPARK_BRONZE_CHECKPOINT_PATH", raising=False)
    monkeypatch.delenv("CHECKPOINT_PATH", raising=False)

    with pytest.raises(
        RuntimeError,
        match="SPARK_BUCKET_URI or GCS_BUCKET_URI or SPARK_BRONZE_OUTPUT_PATH or BRONZE_OUTPUT_PATH is required.",
    ):
        module.load_runtime_settings()


def test_create_spark_session_sets_delta_and_kafka_configs(monkeypatch):
    module = load_module()

    class FakeBuilder:
        def __init__(self):
            self.configs = []

        def appName(self, value):
            self.app_name = value
            return self

        def config(self, key, value):
            self.configs.append((key, value))
            return self

    class FakeSparkContext:
        def __init__(self):
            self.log_levels = []

        def setLogLevel(self, value):
            self.log_levels.append(value)

    class FakeSparkSession:
        def __init__(self):
            self.sparkContext = FakeSparkContext()

    fake_builder = FakeBuilder()
    fake_spark = FakeSparkSession()

    class FakePySpark:
        __version__ = "4.0.1"

        class sql:
            class SparkSession:
                builder = fake_builder

    captured = {}

    class FakeConfiguredBuilder:
        def getOrCreate(self):
            return fake_spark

    def fake_configure(builder, extra_packages):
        captured["builder"] = builder
        captured["extra_packages"] = extra_packages
        return FakeConfiguredBuilder()

    monkeypatch.setitem(sys.modules, "pyspark", FakePySpark)
    monkeypatch.setitem(
        sys.modules,
        "delta",
        type("FakeDeltaModule", (), {"configure_spark_with_delta_pip": fake_configure}),
    )

    spark = module.create_spark_session(
        module.RuntimeSettings(output_path="/shared/bronze", checkpoint_path="/shared/checkpoints")
    )

    assert spark is fake_spark
    assert fake_builder.app_name == module.DEFAULT_APP_NAME
    assert ("spark.databricks.delta.autoCompact.enabled", "true") in fake_builder.configs
    assert ("spark.databricks.delta.optimizeWrite.enabled", "true") in fake_builder.configs
    assert captured["extra_packages"] == ["org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"]
    assert fake_spark.sparkContext.log_levels == ["WARN"]


def test_build_stop_runtime_stops_query_and_spark_once():
    module = load_module()
    calls = []

    class FakeQuery:
        def stop(self):
            calls.append("query.stop")

    class FakeSpark:
        def stop(self):
            calls.append("spark.stop")

    stop_runtime = module.build_stop_runtime(FakeQuery(), FakeSpark())

    stop_runtime()
    stop_runtime()

    assert calls == ["query.stop", "spark.stop"]


def test_main_prints_ready_line(monkeypatch, capsys):
    module = load_module()

    class FakeSpark:
        def stop(self):
            return None

    fake_spark = FakeSpark()
    fake_query = object()

    monkeypatch.setattr(
        module,
        "load_runtime_settings",
        lambda: module.RuntimeSettings(output_path="/shared/bronze", checkpoint_path="/shared/checkpoints"),
    )
    monkeypatch.setattr(module, "create_spark_session", lambda settings: fake_spark)
    monkeypatch.setattr(module, "create_streaming_query", lambda spark, settings: fake_query)
    monkeypatch.setattr(module, "build_stop_runtime", lambda query, spark: lambda: None)
    monkeypatch.setattr(module, "install_signal_handlers", lambda: {"value": False})
    monkeypatch.setattr(module, "wait_for_query_termination", lambda query, shutdown_requested: True)

    module.main()

    assert module.READY_LINE in capsys.readouterr().out
