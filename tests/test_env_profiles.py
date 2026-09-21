import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from collector import env_profiles


def clear_runtime_env(monkeypatch):
    for name in (
        "APP_ENV",
        "HOST",
        "PORT",
        "KAFKA_CLIENT_ID",
        "EVENT_BUS_BACKEND",
        "PUBSUB_PROJECT_ID",
        "PUBSUB_RAW_TOPIC",
        "PUBSUB_EMULATOR_HOST",
        "GOOGLE_CLOUD_PROJECT",
    ):
        monkeypatch.delenv(name, raising=False)


def test_load_runtime_env_defaults_to_test_profile(tmp_path, monkeypatch):
    clear_runtime_env(monkeypatch)
    (tmp_path / ".env").write_text("HOST=127.0.0.1\nPORT=9000\n", encoding="utf-8")
    (tmp_path / ".env.test").write_text(
        "KAFKA_CLIENT_ID=test-client\nEVENT_BUS_BACKEND=pubsub\nPUBSUB_RAW_TOPIC=raw-topic\n",
        encoding="utf-8",
    )

    loaded = env_profiles.load_runtime_env(root_dir=tmp_path)
    runtime = env_profiles.collect_runtime_environment(root_dir=tmp_path)

    assert loaded.app_env == "test"
    assert loaded.profile_env_path == tmp_path / ".env.test"
    assert os.environ["KAFKA_CLIENT_ID"] == "test-client"
    assert runtime["host"] == "127.0.0.1"
    assert runtime["port"] == "9000"


def test_load_runtime_env_uses_prod_profile_when_selected(tmp_path, monkeypatch):
    clear_runtime_env(monkeypatch)
    (tmp_path / ".env").write_text("APP_ENV=prod\n", encoding="utf-8")
    (tmp_path / ".env.prod").write_text(
        "KAFKA_CLIENT_ID=prod-client\nEVENT_BUS_BACKEND=pubsub\nPUBSUB_RAW_TOPIC=raw-topic\n",
        encoding="utf-8",
    )

    loaded = env_profiles.load_runtime_env(root_dir=tmp_path)

    assert loaded.app_env == "prod"
    assert loaded.profile_env_path == tmp_path / ".env.prod"
    assert os.environ["KAFKA_CLIENT_ID"] == "prod-client"


def test_load_runtime_env_rejects_invalid_app_env(tmp_path, monkeypatch):
    clear_runtime_env(monkeypatch)
    (tmp_path / ".env").write_text("APP_ENV=staging\n", encoding="utf-8")
    (tmp_path / ".env.test").write_text("KAFKA_CLIENT_ID=test-client\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported APP_ENV"):
        env_profiles.load_runtime_env(root_dir=tmp_path)


def test_load_runtime_env_fails_when_profile_file_is_missing(tmp_path, monkeypatch):
    clear_runtime_env(monkeypatch)
    (tmp_path / ".env").write_text("APP_ENV=prod\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match=r"\.env\.prod"):
        env_profiles.load_runtime_env(root_dir=tmp_path)


def test_collect_runtime_environment_prefers_root_event_bus_backend_kafka(tmp_path, monkeypatch):
    clear_runtime_env(monkeypatch)
    (tmp_path / ".env").write_text("APP_ENV=test\nEVENT_BUS_BACKEND=kafka\n", encoding="utf-8")
    (tmp_path / ".env.test").write_text(
        "KAFKA_CLIENT_ID=test-client\nEVENT_BUS_BACKEND=pubsub\nPUBSUB_RAW_TOPIC=raw-topic\n",
        encoding="utf-8",
    )

    runtime = env_profiles.collect_runtime_environment(root_dir=tmp_path)

    assert runtime["event_bus_backend"] == "kafka"


def test_collect_runtime_environment_prefers_root_event_bus_backend_pubsub(tmp_path, monkeypatch):
    clear_runtime_env(monkeypatch)
    (tmp_path / ".env").write_text("APP_ENV=test\nEVENT_BUS_BACKEND=pubsub\n", encoding="utf-8")
    (tmp_path / ".env.test").write_text(
        "KAFKA_CLIENT_ID=test-client\nEVENT_BUS_BACKEND=kafka\nKAFKA_BOOTSTRAP_SERVERS=localhost:9092\n",
        encoding="utf-8",
    )

    runtime = env_profiles.collect_runtime_environment(root_dir=tmp_path)

    assert runtime["event_bus_backend"] == "pubsub"


def test_collect_runtime_environment_defaults_to_kafka_without_cloud_settings(tmp_path, monkeypatch):
    clear_runtime_env(monkeypatch)
    (tmp_path / ".env.test").write_text("KAFKA_CLIENT_ID=test-client\n", encoding="utf-8")

    assert env_profiles.collect_runtime_environment(root_dir=tmp_path)["event_bus_backend"] == "kafka"
