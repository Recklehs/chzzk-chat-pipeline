import asyncio
import importlib
import json

from collector.event_bus import (
    EVENT_BUS_BACKEND_KAFKA,
    normalize_event_bus_backend,
    resolve_pubsub_resource_path,
)
from collector.metrics import metrics

AIOKafkaProducer = None
pubsub_v1 = None


def load_aiokafka_producer():
    global AIOKafkaProducer
    if AIOKafkaProducer is not None:
        return AIOKafkaProducer

    try:
        module = importlib.import_module("aiokafka")
    except ImportError:  # pragma: no cover - optional dependency until installed
        return None

    AIOKafkaProducer = module.AIOKafkaProducer
    return AIOKafkaProducer


def load_pubsub_module():
    global pubsub_v1
    if pubsub_v1 is not None:
        return pubsub_v1

    try:
        pubsub_v1 = importlib.import_module("google.cloud.pubsub_v1")
    except ImportError:  # pragma: no cover - optional dependency until installed
        return None

    return pubsub_v1


def encode_publish_payload(payload: dict, raw_payload: bytes | str | None = None) -> bytes:
    if raw_payload is None:
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if isinstance(raw_payload, bytes):
        return raw_payload
    if isinstance(raw_payload, str):
        return raw_payload.encode("utf-8")
    raise TypeError("raw_payload must be bytes, str, or None")


class KafkaRawPublisher:
    """Publish inbound raw CHZZK payloads to Kafka."""

    metrics_backend = "kafka"

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        client_id: str = "chzzk-collector",
        *,
        linger_ms: int = 0,
        max_batch_size: int = 16384,
        compression_type: str | None = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.client_id = client_id
        self.linger_ms = linger_ms
        self.max_batch_size = max_batch_size
        self.compression_type = compression_type
        self._producer = None

    async def start(self):
        if self._producer is not None:
            return
        producer_cls = load_aiokafka_producer()
        if producer_cls is None:
            raise RuntimeError("aiokafka is required to enable Kafka publishing.")

        self._producer = producer_cls(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            linger_ms=self.linger_ms,
            max_batch_size=self.max_batch_size,
            compression_type=self.compression_type,
        )
        await self._producer.start()

    async def stop(self):
        if self._producer is None:
            return
        await self._producer.stop()
        self._producer = None

    async def publish(self, channel_id: str, payload: dict, raw_payload: bytes | str | None = None):
        if self._producer is None:
            raise RuntimeError("KafkaRawPublisher has not been started.")

        data = encode_publish_payload(payload, raw_payload)
        metrics.increment("chzzk_kafka_payload_bytes_total", amount=len(data))

        await self._producer.send_and_wait(
            self.topic,
            key=channel_id.encode("utf-8"),
            value=data,
        )


class PubSubRawPublisher:
    """Publish inbound raw CHZZK payloads to Google Pub/Sub."""

    metrics_backend = "pubsub"

    def __init__(
        self,
        *,
        project_id: str | None,
        topic: str | None,
        publisher_client=None,
    ):
        self.project_id = project_id
        self.topic = topic
        self._publisher = publisher_client
        self._topic_path: str | None = None
        self._started = False

    async def start(self):
        if self._started:
            return
        pubsub_module = load_pubsub_module()
        if pubsub_module is None:
            raise RuntimeError("google-cloud-pubsub is required to enable Pub/Sub publishing.")

        if self._publisher is None:
            publisher_options = pubsub_module.types.PublisherOptions(enable_message_ordering=True)
            self._publisher = pubsub_module.PublisherClient(publisher_options=publisher_options)

        self._topic_path = resolve_pubsub_resource_path(
            self.project_id,
            self.topic,
            resource_kind="topics",
            env_var_name="PUBSUB_RAW_TOPIC",
        )
        await asyncio.to_thread(self._publisher.get_topic, request={"topic": self._topic_path})
        self._started = True

    async def stop(self):
        if self._publisher is None:
            return

        close_method = getattr(self._publisher, "stop", None) or getattr(self._publisher, "close", None)
        if close_method is not None:
            await asyncio.to_thread(close_method)

        self._publisher = None
        self._topic_path = None
        self._started = False

    async def publish(self, channel_id: str, payload: dict, raw_payload: bytes | str | None = None):
        if self._publisher is None or not self._started or self._topic_path is None:
            raise RuntimeError("PubSubRawPublisher has not been started.")

        data = encode_publish_payload(payload, raw_payload)
        metrics.increment("chzzk_pubsub_payload_bytes_total", amount=len(data))
        future = self._publisher.publish(
            self._topic_path,
            data=data,
            ordering_key=channel_id,
            channel_id=channel_id,
        )

        try:
            await asyncio.to_thread(future.result)
        except Exception:
            resume_publish = getattr(self._publisher, "resume_publish", None)
            if callable(resume_publish):
                await asyncio.to_thread(resume_publish, self._topic_path, channel_id)
            raise


def build_default_raw_publisher(settings):
    backend = normalize_event_bus_backend(getattr(settings, "event_bus_backend", None))

    if backend == EVENT_BUS_BACKEND_KAFKA:
        return KafkaRawPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic,
            client_id=settings.kafka_client_id,
            linger_ms=settings.kafka_producer_linger_ms,
            max_batch_size=settings.kafka_producer_max_batch_size,
            compression_type=settings.kafka_producer_compression_type,
        )

    return PubSubRawPublisher(
        project_id=settings.pubsub_project_id,
        topic=settings.pubsub_raw_topic,
    )
