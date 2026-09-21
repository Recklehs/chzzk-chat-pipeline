import asyncio
import importlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_publisher_module():
    sys.modules.pop("collector.publisher", None)
    return importlib.import_module("collector.publisher")


class FakeAIOKafkaProducer:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.started = False
        self.send_calls = []
        type(self).instances.append(self)

    async def start(self):
        self.started = True

    async def stop(self):
        self.started = False

    async def send_and_wait(self, topic, *, key, value):
        self.send_calls.append({"topic": topic, "key": key, "value": value})


def test_kafka_raw_publisher_passes_batching_options_to_aiokafka(monkeypatch):
    module = load_publisher_module()
    monkeypatch.setattr(module, "AIOKafkaProducer", FakeAIOKafkaProducer)
    FakeAIOKafkaProducer.instances.clear()
    publisher = module.KafkaRawPublisher(
        bootstrap_servers="localhost:9092",
        topic="chzzk.events.raw",
        client_id="chzzk-collector",
        linger_ms=20,
        max_batch_size=65536,
        compression_type="lz4",
    )

    asyncio.run(publisher.start())
    asyncio.run(publisher.stop())

    assert FakeAIOKafkaProducer.instances[0].kwargs == {
        "bootstrap_servers": "localhost:9092",
        "client_id": "chzzk-collector",
        "linger_ms": 20,
        "max_batch_size": 65536,
        "compression_type": "lz4",
    }


def test_kafka_raw_publisher_prefers_raw_payload_bytes(monkeypatch):
    module = load_publisher_module()
    monkeypatch.setattr(module, "AIOKafkaProducer", FakeAIOKafkaProducer)
    FakeAIOKafkaProducer.instances.clear()
    publisher = module.KafkaRawPublisher(
        bootstrap_servers="localhost:9092",
        topic="chzzk.events.raw",
    )

    asyncio.run(publisher.start())
    asyncio.run(publisher.publish("channel-1", {"cmd": 1}, raw_payload=b'{"cmd":1}'))
    asyncio.run(publisher.stop())

    assert FakeAIOKafkaProducer.instances[0].send_calls == [
        {"topic": "chzzk.events.raw", "key": b"channel-1", "value": b'{"cmd":1}'}
    ]
