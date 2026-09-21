import importlib
import json
import os
from datetime import datetime

from collector.env_profiles import LoadedEnvProfile, load_runtime_env
from collector.event_bus import (
    EVENT_BUS_BACKEND_PUBSUB,
    normalize_event_bus_backend,
    resolve_pubsub_project_id,
    resolve_pubsub_resource_path,
)

AlreadyExists = None
pubsub_v1 = None


def load_pubsub_dependencies():
    global AlreadyExists, pubsub_v1
    if pubsub_v1 is not None and AlreadyExists is not None:
        return pubsub_v1, AlreadyExists

    try:
        pubsub_v1 = importlib.import_module("google.cloud.pubsub_v1")
        exceptions_module = importlib.import_module("google.api_core.exceptions")
    except ImportError:  # pragma: no cover - optional dependency until installed
        return None, None

    AlreadyExists = exceptions_module.AlreadyExists
    return pubsub_v1, AlreadyExists


def default_debug_subscription_name(topic_value: str) -> str:
    topic_name = (topic_value or "").strip().rstrip("/").split("/")[-1]
    if not topic_name:
        raise ValueError("PUBSUB_RAW_TOPIC is required to derive the debug subscription name.")
    return f"{topic_name}-debug"


def resolve_debug_subscription_path(
    subscription_value: str | None = None,
    *,
    loaded_profile: LoadedEnvProfile | None = None,
) -> tuple[str, str, LoadedEnvProfile]:
    resolved_profile = loaded_profile or load_runtime_env()
    backend = normalize_event_bus_backend(os.environ.get("EVENT_BUS_BACKEND", EVENT_BUS_BACKEND_PUBSUB))
    if backend != EVENT_BUS_BACKEND_PUBSUB:
        raise RuntimeError("Pub/Sub debug subscriber can only run when EVENT_BUS_BACKEND=pubsub.")

    project_id = resolve_pubsub_project_id(os.environ.get("PUBSUB_PROJECT_ID"))
    raw_topic_value = os.environ.get("PUBSUB_RAW_TOPIC")
    topic_path = resolve_pubsub_resource_path(
        project_id,
        raw_topic_value,
        resource_kind="topics",
        env_var_name="PUBSUB_RAW_TOPIC",
    )
    subscription_path = resolve_pubsub_resource_path(
        project_id,
        subscription_value or default_debug_subscription_name(raw_topic_value or ""),
        resource_kind="subscriptions",
        env_var_name="PUBSUB_DEBUG_SUBSCRIPTION",
    )
    return topic_path, subscription_path, resolved_profile


def ensure_debug_subscription(
    subscription_value: str | None = None,
    *,
    loaded_profile: LoadedEnvProfile | None = None,
) -> tuple[str, str, LoadedEnvProfile]:
    pubsub_module, already_exists_exc = load_pubsub_dependencies()
    if pubsub_module is None or already_exists_exc is None:
        raise RuntimeError("google-cloud-pubsub is required to watch Pub/Sub raw messages.")

    topic_path, subscription_path, resolved_profile = resolve_debug_subscription_path(
        subscription_value=subscription_value,
        loaded_profile=loaded_profile,
    )
    subscriber = pubsub_module.SubscriberClient()
    try:
        try:
            subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
        except already_exists_exc:
            pass
    finally:
        close_method = getattr(subscriber, "close", None)
        if close_method is not None:
            close_method()
    return topic_path, subscription_path, resolved_profile


def format_message_output(message) -> str:
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    attributes = dict(getattr(message, "attributes", {}) or {})
    ordering_key = getattr(message, "ordering_key", "") or "-"
    message_id = getattr(message, "message_id", "") or "-"
    raw_data = getattr(message, "data", b"") or b""
    decoded = raw_data.decode("utf-8", errors="replace")

    try:
        rendered_payload = json.dumps(json.loads(decoded), ensure_ascii=False, indent=2)
    except json.JSONDecodeError:
        rendered_payload = decoded

    return "\n".join(
        [
            "=" * 80,
            f"[{timestamp}] message_id={message_id}",
            f"ordering_key={ordering_key}",
            f"attributes={json.dumps(attributes, ensure_ascii=False)}",
            "payload:",
            rendered_payload,
        ]
    )


def watch_messages(subscription_value: str | None = None) -> None:
    loaded_profile = load_runtime_env()
    pubsub_module, _already_exists_exc = load_pubsub_dependencies()
    if pubsub_module is None:
        raise RuntimeError("google-cloud-pubsub is required to watch Pub/Sub raw messages.")

    topic_path, subscription_path, loaded_profile = ensure_debug_subscription(
        subscription_value=subscription_value,
        loaded_profile=loaded_profile,
    )
    subscriber = pubsub_module.SubscriberClient()

    def callback(message) -> None:
        try:
            print(format_message_output(message), flush=True)
            message.ack()
        except Exception:
            message.nack()
            raise

    print(f"[INFO] APP_ENV={loaded_profile.app_env}", flush=True)
    print(f"[INFO] Loaded env profile: {loaded_profile.profile_env_path}", flush=True)
    print(f"[INFO] Watching Pub/Sub raw topic: {topic_path}", flush=True)
    print(f"[INFO] Using debug subscription: {subscription_path}", flush=True)
    streaming_future = subscriber.subscribe(subscription_path, callback=callback)

    try:
        streaming_future.result()
    except KeyboardInterrupt:
        print("\n[INFO] Stopping Pub/Sub raw message watcher.", flush=True)
        streaming_future.cancel()
    finally:
        close_method = getattr(subscriber, "close", None)
        if close_method is not None:
            close_method()


def main() -> None:
    subscription_name = os.environ.get("PUBSUB_DEBUG_SUBSCRIPTION", "").strip() or None
    watch_messages(subscription_value=subscription_name)


if __name__ == "__main__":
    main()
