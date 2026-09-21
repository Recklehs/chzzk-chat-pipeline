from __future__ import annotations

from typing import Any

DASHBOARD_REALTIME_TTL_SECONDS = 10
DASHBOARD_RANKINGS_TTL_SECONDS = 20
DASHBOARD_SPIKES_TTL_SECONDS = 10
CHANNEL_TIMELINE_TTL_SECONDS = 15


def _value(value: Any) -> str:
    return str(value).strip()


def dashboard_realtime_key(*, window: str) -> str:
    return f"chat:dashboard:realtime:window={_value(window)}"


def dashboard_rankings_key(*, window: str, metric: str, limit: int) -> str:
    return (
        f"chat:dashboard:rankings:window={_value(window)}"
        f":metric={_value(metric)}"
        f":limit={_value(limit)}"
    )


def dashboard_spikes_key(*, window: str, limit: int, min_msg_count: int, ratio_threshold: float) -> str:
    return (
        f"chat:dashboard:spikes:window={_value(window)}"
        f":limit={_value(limit)}"
        f":min={_value(min_msg_count)}"
        f":ratio={_value(ratio_threshold)}"
    )


def channel_timeline_key(*, channel_id: str, from_: str, to: str) -> str:
    return f"chat:channel:{_value(channel_id)}:timeline:from={_value(from_)}:to={_value(to)}"
