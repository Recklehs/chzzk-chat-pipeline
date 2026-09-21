from __future__ import annotations

import importlib
import logging
from typing import Any

logger = logging.getLogger(__name__)


def build_redis_client(settings: Any):
    redis_url = getattr(settings, "redis_url", None)
    redis_enabled = bool(getattr(settings, "redis_enabled", False) or redis_url)
    if not redis_enabled:
        logger.info("redis cache disabled; set REDIS_ENABLED=true or REDIS_URL to enable")
        return None

    try:
        redis_asyncio = importlib.import_module("redis.asyncio")
    except ModuleNotFoundError:
        logger.warning("redis-py is not installed; redis cache disabled")
        return None

    socket_timeout = getattr(settings, "redis_socket_timeout_seconds", 0.2)
    common_options = {
        "encoding": "utf-8",
        "decode_responses": True,
        "socket_timeout": socket_timeout,
        "socket_connect_timeout": socket_timeout,
    }

    if redis_url:
        return redis_asyncio.from_url(redis_url, **common_options)

    return redis_asyncio.Redis(
        host=getattr(settings, "redis_host", "localhost"),
        port=getattr(settings, "redis_port", 6379),
        db=getattr(settings, "redis_db", 0),
        password=getattr(settings, "redis_password", None),
        ssl=getattr(settings, "redis_ssl", False),
        **common_options,
    )
