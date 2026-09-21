import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from collector import bootstrap_pubsub_raw_topic as bootstrap_module


class FakeAlreadyExists(Exception):
    pass


class FakePublisherClient:
    def __init__(self, should_raise=False):
        self.should_raise = should_raise
        self.created_requests = []
        self.closed = 0

    def create_topic(self, *, request):
        self.created_requests.append(request)
        if self.should_raise:
            raise FakeAlreadyExists("exists")

    def close(self):
        self.closed += 1


def test_bootstrap_creates_raw_topic_for_test_pubsub(monkeypatch):
    fake_client = FakePublisherClient()

    monkeypatch.setattr(
        bootstrap_module,
        "load_runtime_env",
        lambda: SimpleNamespace(app_env="test"),
    )
    monkeypatch.setenv("EVENT_BUS_BACKEND", "pubsub")
    monkeypatch.setenv("PUBSUB_PROJECT_ID", "local-project")
    monkeypatch.setenv("PUBSUB_RAW_TOPIC", "raw-topic")
    monkeypatch.setattr(
        bootstrap_module,
        "pubsub_v1",
        SimpleNamespace(
            PublisherClient=lambda publisher_options=None: fake_client,
            types=SimpleNamespace(PublisherOptions=lambda enable_message_ordering: {"enable_message_ordering": enable_message_ordering}),
        ),
    )
    monkeypatch.setattr(bootstrap_module, "AlreadyExists", FakeAlreadyExists)

    topic_path = asyncio.run(bootstrap_module.ensure_test_pubsub_raw_topic())

    assert topic_path == "projects/local-project/topics/raw-topic"
    assert fake_client.created_requests == [{"name": "projects/local-project/topics/raw-topic"}]
    assert fake_client.closed == 1


def test_bootstrap_treats_existing_topic_as_success(monkeypatch):
    fake_client = FakePublisherClient(should_raise=True)

    monkeypatch.setattr(
        bootstrap_module,
        "load_runtime_env",
        lambda: SimpleNamespace(app_env="test"),
    )
    monkeypatch.setenv("EVENT_BUS_BACKEND", "pubsub")
    monkeypatch.setenv("PUBSUB_PROJECT_ID", "local-project")
    monkeypatch.setenv("PUBSUB_RAW_TOPIC", "raw-topic")
    monkeypatch.setattr(
        bootstrap_module,
        "pubsub_v1",
        SimpleNamespace(
            PublisherClient=lambda publisher_options=None: fake_client,
            types=SimpleNamespace(PublisherOptions=lambda enable_message_ordering: {"enable_message_ordering": enable_message_ordering}),
        ),
    )
    monkeypatch.setattr(bootstrap_module, "AlreadyExists", FakeAlreadyExists)

    topic_path = asyncio.run(bootstrap_module.ensure_test_pubsub_raw_topic())

    assert topic_path == "projects/local-project/topics/raw-topic"
    assert fake_client.closed == 1


def test_bootstrap_skips_for_prod_and_kafka(monkeypatch):
    monkeypatch.setattr(
        bootstrap_module,
        "load_runtime_env",
        lambda: SimpleNamespace(app_env="prod"),
    )
    monkeypatch.setenv("EVENT_BUS_BACKEND", "pubsub")

    assert asyncio.run(bootstrap_module.ensure_test_pubsub_raw_topic()) is None

    monkeypatch.setattr(
        bootstrap_module,
        "load_runtime_env",
        lambda: SimpleNamespace(app_env="test"),
    )
    monkeypatch.setenv("EVENT_BUS_BACKEND", "kafka")

    assert asyncio.run(bootstrap_module.ensure_test_pubsub_raw_topic()) is None
