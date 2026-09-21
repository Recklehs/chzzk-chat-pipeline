import os
from dataclasses import dataclass, field
from typing import AsyncIterator, Awaitable, Callable, Mapping, Protocol

EVENT_BUS_BACKEND_KAFKA = "kafka"
EVENT_BUS_BACKEND_PUBSUB = "pubsub"

AckHandler = Callable[[], Awaitable[None]]


async def noop_async() -> None:
    return None


@dataclass
class ControlBusMessage:
    value: bytes | str
    attributes: Mapping[str, str] = field(default_factory=dict)
    ack_handler: AckHandler = noop_async

    async def ack(self) -> None:
        await self.ack_handler()


class RawPublisher(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def publish(self, channel_id: str, payload: dict) -> None: ...


class StatusPublisher(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def publish(self, payload: dict) -> None: ...


class ControlConsumer(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    def messages(self) -> AsyncIterator[ControlBusMessage]: ...


def normalize_event_bus_backend(value: str | None) -> str:
    normalized = (value or EVENT_BUS_BACKEND_PUBSUB).strip().lower()
    if not normalized:
        normalized = EVENT_BUS_BACKEND_PUBSUB

    supported = {
        EVENT_BUS_BACKEND_KAFKA,
        EVENT_BUS_BACKEND_PUBSUB,
    }
    if normalized not in supported:
        choices = ", ".join(sorted(supported))
        raise ValueError(f"Unsupported EVENT_BUS_BACKEND '{value}'. Expected one of: {choices}")
    return normalized


def resolve_pubsub_project_id(explicit_project_id: str | None = None) -> str | None:
    explicit_value = (explicit_project_id or "").strip()
    if explicit_value:
        return explicit_value

    fallback_value = os.environ.get("GOOGLE_CLOUD_PROJECT", "").strip()
    return fallback_value or None


def resolve_pubsub_resource_path(
    project_id: str | None,
    resource_value: str | None,
    *,
    resource_kind: str,
    env_var_name: str,
) -> str:
    raw_value = (resource_value or "").strip()
    if not raw_value:
        raise ValueError(f"{env_var_name} is required when EVENT_BUS_BACKEND=pubsub.")

    if raw_value.startswith("projects/"):
        return raw_value

    resolved_project_id = resolve_pubsub_project_id(project_id)
    if not resolved_project_id:
        raise ValueError("PUBSUB_PROJECT_ID or GOOGLE_CLOUD_PROJECT is required when EVENT_BUS_BACKEND=pubsub.")

    return f"projects/{resolved_project_id}/{resource_kind}/{raw_value}"
