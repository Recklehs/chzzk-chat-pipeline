import asyncio
import os

from collector.env_profiles import load_runtime_env
from collector.event_bus import (
    EVENT_BUS_BACKEND_PUBSUB,
    normalize_event_bus_backend,
    resolve_pubsub_project_id,
    resolve_pubsub_resource_path,
)

try:
    from google.api_core.exceptions import AlreadyExists
    from google.cloud import pubsub_v1
except ImportError:  # pragma: no cover - optional dependency until installed
    AlreadyExists = None
    pubsub_v1 = None


async def ensure_test_pubsub_raw_topic() -> str | None:
    loaded_profile = load_runtime_env()
    backend = normalize_event_bus_backend(os.environ.get("EVENT_BUS_BACKEND", EVENT_BUS_BACKEND_PUBSUB))
    if loaded_profile.app_env != "test" or backend != EVENT_BUS_BACKEND_PUBSUB:
        return None

    if pubsub_v1 is None or AlreadyExists is None:
        raise RuntimeError("google-cloud-pubsub is required to bootstrap the Pub/Sub raw topic.")

    project_id = resolve_pubsub_project_id(os.environ.get("PUBSUB_PROJECT_ID"))
    topic_path = resolve_pubsub_resource_path(
        project_id,
        os.environ.get("PUBSUB_RAW_TOPIC"),
        resource_kind="topics",
        env_var_name="PUBSUB_RAW_TOPIC",
    )

    publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
    publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)

    try:
        try:
            await asyncio.to_thread(publisher.create_topic, request={"name": topic_path})
        except AlreadyExists:
            pass
    finally:
        close_method = getattr(publisher, "stop", None) or getattr(publisher, "close", None)
        if close_method is not None:
            await asyncio.to_thread(close_method)

    return topic_path


def main() -> None:
    topic_path = asyncio.run(ensure_test_pubsub_raw_topic())
    if topic_path is not None:
        print(f"[INFO] Pub/Sub raw topic ready: {topic_path}")


if __name__ == "__main__":
    main()
