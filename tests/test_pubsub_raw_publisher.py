import asyncio
import importlib
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_publisher_module():
    sys.modules.pop("collector.publisher", None)
    return importlib.import_module("collector.publisher")


class FakeFuture:
    def __init__(self, *, should_fail: bool = False):
        self.should_fail = should_fail

    def result(self):
        if self.should_fail:
            raise RuntimeError("pubsub unavailable")
        return "message-id"


class FakePublisherOptions:
    def __init__(self, enable_message_ordering: bool = False):
        self.enable_message_ordering = enable_message_ordering


class FakePublisherClient:
    instances = []
    next_publish_should_fail = False

    def __init__(self, publisher_options=None):
        self.publisher_options = publisher_options
        self.get_topic_calls = []
        self.publish_calls = []
        self.resume_publish_calls = []
        self.closed = False
        type(self).instances.append(self)

    def get_topic(self, request):
        self.get_topic_calls.append(request)
        return object()

    def publish(self, topic_path, data, ordering_key="", **attrs):
        self.publish_calls.append(
            {
                "topic_path": topic_path,
                "data": data,
                "ordering_key": ordering_key,
                "attrs": attrs,
            }
        )
        return FakeFuture(should_fail=type(self).next_publish_should_fail)

    def resume_publish(self, topic_path, ordering_key):
        self.resume_publish_calls.append((topic_path, ordering_key))

    def close(self):
        self.closed = True


def test_pubsub_raw_publisher_publishes_raw_json_with_attributes_and_ordering(monkeypatch):
    module = load_publisher_module()
    fake_pubsub = type(
        "FakePubSub",
        (),
        {
            "PublisherClient": FakePublisherClient,
            "types": type("FakeTypes", (), {"PublisherOptions": FakePublisherOptions}),
        },
    )
    monkeypatch.setattr(module, "pubsub_v1", fake_pubsub)
    FakePublisherClient.instances.clear()
    FakePublisherClient.next_publish_should_fail = False

    publisher = module.PubSubRawPublisher(project_id="demo-project", topic="raw-topic")

    asyncio.run(publisher.start())
    asyncio.run(publisher.publish("channel-1", {"cmd": 93101, "msg": "hello"}))
    asyncio.run(publisher.stop())

    client = FakePublisherClient.instances[0]
    assert client.publisher_options.enable_message_ordering is True
    assert client.get_topic_calls == [{"topic": "projects/demo-project/topics/raw-topic"}]
    assert client.publish_calls == [
        {
            "topic_path": "projects/demo-project/topics/raw-topic",
            "data": json.dumps({"cmd": 93101, "msg": "hello"}, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            ),
            "ordering_key": "channel-1",
            "attrs": {"channel_id": "channel-1"},
        }
    ]
    assert client.closed is True


def test_pubsub_raw_publisher_prefers_raw_payload_bytes(monkeypatch):
    module = load_publisher_module()
    fake_pubsub = type(
        "FakePubSub",
        (),
        {
            "PublisherClient": FakePublisherClient,
            "types": type("FakeTypes", (), {"PublisherOptions": FakePublisherOptions}),
        },
    )
    monkeypatch.setattr(module, "pubsub_v1", fake_pubsub)
    FakePublisherClient.instances.clear()
    FakePublisherClient.next_publish_should_fail = False
    publisher = module.PubSubRawPublisher(project_id="demo-project", topic="raw-topic")

    asyncio.run(publisher.start())
    asyncio.run(publisher.publish("channel-1", {"cmd": 1}, raw_payload=b'{"cmd":1}'))

    assert FakePublisherClient.instances[0].publish_calls[0]["data"] == b'{"cmd":1}'


def test_pubsub_raw_publisher_resumes_ordering_key_on_publish_failure(monkeypatch):
    module = load_publisher_module()
    fake_pubsub = type(
        "FakePubSub",
        (),
        {
            "PublisherClient": FakePublisherClient,
            "types": type("FakeTypes", (), {"PublisherOptions": FakePublisherOptions}),
        },
    )
    monkeypatch.setattr(module, "pubsub_v1", fake_pubsub)
    FakePublisherClient.instances.clear()
    FakePublisherClient.next_publish_should_fail = True

    publisher = module.PubSubRawPublisher(project_id="demo-project", topic="raw-topic")
    asyncio.run(publisher.start())

    with pytest.raises(RuntimeError, match="pubsub unavailable"):
        asyncio.run(publisher.publish("channel-1", {"cmd": 1}))

    client = FakePublisherClient.instances[0]
    assert client.resume_publish_calls == [("projects/demo-project/topics/raw-topic", "channel-1")]
