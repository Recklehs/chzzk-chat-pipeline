from __future__ import annotations

import inspect
import json
import logging
from datetime import date, datetime
from typing import Any, Awaitable, Callable, TypeVar

from collector.redis.client import build_redis_client

logger = logging.getLogger(__name__)

T = TypeVar("T")
Loader = Callable[[], T | Awaitable[T]]


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


class JsonRedisCache:
    def __init__(self, client: Any | None):
        self.client = client

    async def get_json_cache(self, key: str) -> T | None:
        if self.client is None:
            logger.debug("redis cache disabled key=%s", key)
            return None

        try:
            raw_value = await self.client.get(key)
        except Exception:
            logger.warning("redis get failed key=%s", key, exc_info=True)
            return None

        if raw_value is None:
            logger.info("cache miss key=%s", key)
            return None

        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8")

        try:
            value = json.loads(raw_value)
        except (TypeError, json.JSONDecodeError):
            logger.warning("json parse failed for redis cache key=%s", key, exc_info=True)
            return None

        logger.info("cache hit key=%s", key)
        return value

    async def set_json_cache(self, key: str, value: Any, ttl_seconds: int) -> None:
        if self.client is None:
            logger.debug("redis cache disabled; skip set key=%s", key)
            return

        try:
            payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=_json_default)
            await self.client.set(key, payload, ex=ttl_seconds)
        except Exception:
            logger.warning("redis set failed key=%s", key, exc_info=True)

    async def get_or_set_json_cache(self, key: str, ttl_seconds: int, loader: Loader[T]) -> T:
        cached = await self.get_json_cache(key)
        if cached is not None:
            return cached

        loaded = loader()
        if inspect.isawaitable(loaded):
            loaded = await loaded

        await self.set_json_cache(key, loaded, ttl_seconds)
        return loaded

    async def getJsonCache(self, key: str) -> T | None:
        return await self.get_json_cache(key)

    async def setJsonCache(self, key: str, value: Any, ttlSeconds: int) -> None:
        await self.set_json_cache(key, value, ttlSeconds)

    async def getOrSetJsonCache(self, key: str, ttlSeconds: int, loader: Loader[T]) -> T:
        return await self.get_or_set_json_cache(key, ttlSeconds, loader)

    async def close(self) -> None:
        if self.client is None:
            return

        close = getattr(self.client, "aclose", None) or getattr(self.client, "close", None)
        if close is None:
            return

        try:
            result = close()
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.warning("redis close failed", exc_info=True)


def build_json_cache(settings: Any) -> JsonRedisCache:
    return JsonRedisCache(build_redis_client(settings))


_default_cache = JsonRedisCache(None)


def configure_json_cache(cache_or_client: JsonRedisCache | Any | None) -> JsonRedisCache:
    global _default_cache
    if isinstance(cache_or_client, JsonRedisCache):
        _default_cache = cache_or_client
    else:
        _default_cache = JsonRedisCache(cache_or_client)
    return _default_cache


async def get_json_cache(key: str) -> T | None:
    return await _default_cache.get_json_cache(key)


async def set_json_cache(key: str, value: Any, ttl_seconds: int) -> None:
    await _default_cache.set_json_cache(key, value, ttl_seconds)


async def get_or_set_json_cache(key: str, ttl_seconds: int, loader: Loader[T]) -> T:
    return await _default_cache.get_or_set_json_cache(key, ttl_seconds, loader)


async def getJsonCache(key: str) -> T | None:
    return await get_json_cache(key)


async def setJsonCache(key: str, value: Any, ttlSeconds: int) -> None:
    await set_json_cache(key, value, ttlSeconds)


async def getOrSetJsonCache(key: str, ttlSeconds: int, loader: Loader[T]) -> T:
    return await get_or_set_json_cache(key, ttlSeconds, loader)
