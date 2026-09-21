import asyncio
import json
import logging

from collector.redis.cache import JsonRedisCache, getOrSetJsonCache
from collector.redis.keys import (
    dashboard_rankings_key,
    dashboard_realtime_key,
    dashboard_spikes_key,
    channel_timeline_key,
)


class FakeRedis:
    def __init__(self, values=None, *, fail_get=False, fail_set=False):
        self.values = dict(values or {})
        self.fail_get = fail_get
        self.fail_set = fail_set
        self.set_calls = []

    async def get(self, key):
        if self.fail_get:
            raise RuntimeError("redis get unavailable")
        return self.values.get(key)

    async def set(self, key, value, ex=None):
        if self.fail_set:
            raise RuntimeError("redis set unavailable")
        self.values[key] = value
        self.set_calls.append((key, value, ex))


def test_get_or_set_json_cache_returns_cached_json_without_calling_loader(caplog):
    async def scenario():
        cache = JsonRedisCache(FakeRedis({"chat:key": json.dumps({"ok": True}).encode("utf-8")}))

        async def loader():
            raise AssertionError("loader should not be called on cache hit")

        with caplog.at_level(logging.INFO, logger="collector.redis.cache"):
            result = await cache.get_or_set_json_cache("chat:key", 10, loader)

        assert result == {"ok": True}
        assert "cache hit" in caplog.text

    asyncio.run(scenario())


def test_get_or_set_json_cache_returns_loader_result_when_redis_fails(caplog):
    async def scenario():
        cache = JsonRedisCache(FakeRedis(fail_get=True, fail_set=True))

        async def loader():
            return {"source": "loader"}

        with caplog.at_level(logging.WARNING, logger="collector.redis.cache"):
            result = await cache.get_or_set_json_cache("chat:key", 10, loader)

        assert result == {"source": "loader"}
        assert "redis get failed" in caplog.text
        assert "redis set failed" in caplog.text

    asyncio.run(scenario())


def test_json_parse_failure_is_treated_as_cache_miss(caplog):
    async def scenario():
        fake_redis = FakeRedis({"chat:key": b"{broken json"})
        cache = JsonRedisCache(fake_redis)

        async def loader():
            return {"source": "loader"}

        with caplog.at_level(logging.WARNING, logger="collector.redis.cache"):
            result = await cache.get_or_set_json_cache("chat:key", 15, loader)

        assert result == {"source": "loader"}
        assert fake_redis.set_calls == [("chat:key", '{"source":"loader"}', 15)]
        assert "json parse failed" in caplog.text

    asyncio.run(scenario())


def test_module_level_camel_case_helper_uses_configured_cache(monkeypatch):
    async def scenario():
        fake_redis = FakeRedis()
        cache = JsonRedisCache(fake_redis)
        monkeypatch.setattr("collector.redis.cache._default_cache", cache)

        async def loader():
            return {"cached": "through default cache"}

        result = await getOrSetJsonCache("chat:key", 20, loader)

        assert result == {"cached": "through default cache"}
        assert fake_redis.set_calls == [("chat:key", '{"cached":"through default cache"}', 20)]

    asyncio.run(scenario())


def test_dashboard_cache_keys_are_stable_for_query_param_order():
    rankings_params_a = {"window": "5m", "metric": "message_count", "limit": 10}
    rankings_params_b = {"limit": 10, "metric": "message_count", "window": "5m"}
    spikes_params_a = {"window": "5m", "limit": 20, "min_msg_count": 50, "ratio_threshold": 2.5}
    spikes_params_b = {"ratio_threshold": 2.5, "min_msg_count": 50, "limit": 20, "window": "5m"}

    assert dashboard_realtime_key(window="5m") == "chat:dashboard:realtime:window=5m"
    assert dashboard_rankings_key(**rankings_params_a) == dashboard_rankings_key(**rankings_params_b)
    assert dashboard_rankings_key(**rankings_params_a) == (
        "chat:dashboard:rankings:window=5m:metric=message_count:limit=10"
    )
    assert dashboard_spikes_key(**spikes_params_a) == dashboard_spikes_key(**spikes_params_b)
    assert dashboard_spikes_key(**spikes_params_a) == (
        "chat:dashboard:spikes:window=5m:limit=20:min=50:ratio=2.5"
    )
    assert channel_timeline_key(channel_id="abc", from_="2026-04-26T00:00:00Z", to="2026-04-26T00:01:00Z") == (
        "chat:channel:abc:timeline:from=2026-04-26T00:00:00Z:to=2026-04-26T00:01:00Z"
    )
