import os
import socket

from collector.control import AppSettings
from collector.env_profiles import load_runtime_env
from collector.event_bus import normalize_event_bus_backend, resolve_pubsub_project_id


def _get_optional_env(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def _get_bool_env(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return int(value.strip())


def _get_float_env(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return float(value.strip())


def load_settings_from_env() -> AppSettings:
    load_runtime_env()

    event_bus_backend = normalize_event_bus_backend(os.environ.get("EVENT_BUS_BACKEND"))
    redis_url = _get_optional_env("REDIS_URL")

    return AppSettings(
        event_bus_backend=event_bus_backend,
        kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=os.environ.get("KAFKA_TOPIC", "chzzk.events.raw"),
        kafka_client_id=os.environ.get("KAFKA_CLIENT_ID", "chzzk-collector"),
        kafka_producer_linger_ms=_get_int_env("KAFKA_PRODUCER_LINGER_MS", 0),
        kafka_producer_max_batch_size=_get_int_env("KAFKA_PRODUCER_MAX_BATCH_SIZE", 16384),
        kafka_producer_compression_type=_get_optional_env("KAFKA_PRODUCER_COMPRESSION_TYPE"),
        collector_instance_id=os.environ.get("COLLECTOR_INSTANCE_ID", socket.gethostname()),
        pubsub_project_id=resolve_pubsub_project_id(_get_optional_env("PUBSUB_PROJECT_ID")),
        pubsub_raw_topic=_get_optional_env("PUBSUB_RAW_TOPIC"),
        pubsub_emulator_host=_get_optional_env("PUBSUB_EMULATOR_HOST"),
        chzzk_api_base_url=os.environ.get("CHZZK_API_BASE_URL", "https://api.chzzk.naver.com"),
        chzzk_api_timeout_seconds=float(os.environ.get("CHZZK_API_TIMEOUT_SECONDS", "10")),
        chzzk_live_poll_seconds=float(os.environ.get("CHZZK_LIVE_POLL_SECONDS", "15")),
        chzzk_live_poll_max_seconds=float(os.environ.get("CHZZK_LIVE_POLL_MAX_SECONDS", "60")),
        control_db_path=os.environ.get("CONTROL_DB_PATH", "data/control.db"),
        dashboard_refresh_seconds=int(os.environ.get("DASHBOARD_REFRESH_SECONDS", "5")),
        metrics_enabled=_get_bool_env("METRICS_ENABLED", False),
        redis_enabled=_get_bool_env("REDIS_ENABLED", bool(redis_url)),
        redis_url=redis_url,
        redis_host=os.environ.get("REDIS_HOST", "localhost"),
        redis_port=_get_int_env("REDIS_PORT", 6379),
        redis_db=_get_int_env("REDIS_DB", 0),
        redis_password=_get_optional_env("REDIS_PASSWORD"),
        redis_ssl=_get_bool_env("REDIS_SSL", False),
        redis_socket_timeout_seconds=_get_float_env("REDIS_SOCKET_TIMEOUT_SECONDS", 0.2),
    )
