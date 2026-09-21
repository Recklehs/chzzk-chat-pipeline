import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from collector import watch_pubsub_raw_messages as watch_module


class FakeAlreadyExists(Exception):
    pass


class FakeSubscriberClient:
    def __init__(self, should_raise=False):
        self.should_raise = should_raise
        self.created_requests = []
        self.closed = 0

    def create_subscription(self, *, request):
        self.created_requests.append(request)
        if self.should_raise:
            raise FakeAlreadyExists("exists")

    def close(self):
        self.closed += 1


def test_default_debug_subscription_name_uses_topic_tail():
    assert watch_module.default_debug_subscription_name("chzzk-events-raw") == "chzzk-events-raw-debug"
    assert (
        watch_module.default_debug_subscription_name("projects/demo/topics/chzzk-events-raw")
        == "chzzk-events-raw-debug"
    )


def test_resolve_debug_subscription_path_rejects_non_pubsub_backend(monkeypatch):
    monkeypatch.setattr(watch_module, "load_runtime_env", lambda: None)
    monkeypatch.setenv("EVENT_BUS_BACKEND", "kafka")

    with pytest.raises(RuntimeError, match="EVENT_BUS_BACKEND=pubsub"):
        watch_module.resolve_debug_subscription_path()


def test_ensure_debug_subscription_creates_subscription(monkeypatch):
    fake_client = FakeSubscriberClient()

    monkeypatch.setattr(watch_module, "load_runtime_env", lambda: None)
    monkeypatch.setenv("EVENT_BUS_BACKEND", "pubsub")
    monkeypatch.setenv("PUBSUB_PROJECT_ID", "local-project")
    monkeypatch.setenv("PUBSUB_RAW_TOPIC", "raw-topic")
    monkeypatch.setattr(
        watch_module,
        "pubsub_v1",
        SimpleNamespace(SubscriberClient=lambda: fake_client),
    )
    monkeypatch.setattr(watch_module, "AlreadyExists", FakeAlreadyExists)

    topic_path, subscription_path, loaded_profile = watch_module.ensure_debug_subscription()

    assert topic_path == "projects/local-project/topics/raw-topic"
    assert subscription_path == "projects/local-project/subscriptions/raw-topic-debug"
    assert loaded_profile is None
    assert fake_client.created_requests == [
        {
            "name": "projects/local-project/subscriptions/raw-topic-debug",
            "topic": "projects/local-project/topics/raw-topic",
        }
    ]
    assert fake_client.closed == 1


def test_ensure_debug_subscription_accepts_existing_subscription(monkeypatch):
    fake_client = FakeSubscriberClient(should_raise=True)

    monkeypatch.setattr(watch_module, "load_runtime_env", lambda: None)
    monkeypatch.setenv("EVENT_BUS_BACKEND", "pubsub")
    monkeypatch.setenv("PUBSUB_PROJECT_ID", "local-project")
    monkeypatch.setenv("PUBSUB_RAW_TOPIC", "raw-topic")
    monkeypatch.setattr(
        watch_module,
        "pubsub_v1",
        SimpleNamespace(SubscriberClient=lambda: fake_client),
    )
    monkeypatch.setattr(watch_module, "AlreadyExists", FakeAlreadyExists)

    topic_path, subscription_path, loaded_profile = watch_module.ensure_debug_subscription()

    assert topic_path == "projects/local-project/topics/raw-topic"
    assert subscription_path == "projects/local-project/subscriptions/raw-topic-debug"
    assert loaded_profile is None
    assert fake_client.closed == 1


def test_format_message_output_pretty_prints_json_payload():
    message = SimpleNamespace(
        message_id="message-1",
        ordering_key="channel-open",
        attributes={"channel_id": "channel-open"},
        data=json.dumps({"cmd": 93101, "bdy": [{"msg": "hello"}]}, ensure_ascii=False).encode("utf-8"),
    )

    output = watch_module.format_message_output(message)

    assert "message_id=message-1" in output
    assert "ordering_key=channel-open" in output
    assert '"channel_id": "channel-open"' in output
    assert '"cmd": 93101' in output
    assert '"msg": "hello"' in output


def test_watcher_alias_module_exists():
    alias_path = ROOT / "collector" / "watcher_pubsub_raw_messages.py"
    assert alias_path.is_file()
