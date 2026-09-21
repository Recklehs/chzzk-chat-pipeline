import asyncio
import sqlite3
import threading
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Awaitable, Callable
from urllib.parse import urlparse

import requests

from collector.metrics import metrics


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


STOP_REASON_BROADCAST_ENDED = "BROADCAST_ENDED"
STOP_REASON_SERVER_SHUTDOWN = "SERVER_SHUTDOWN"
STOP_REASON_MANUAL_STOP = "MANUAL_STOP"
STOP_REASON_CHANNEL_REMOVED = "CHANNEL_REMOVED"
STOP_REASON_UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class AppSettings:
    event_bus_backend: str = "kafka"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "chzzk.events.raw"
    kafka_client_id: str = "chzzk-collector"
    kafka_producer_linger_ms: int = 0
    kafka_producer_max_batch_size: int = 16384
    kafka_producer_compression_type: str | None = None
    collector_instance_id: str = "collector"
    pubsub_project_id: str | None = None
    pubsub_raw_topic: str | None = None
    pubsub_emulator_host: str | None = None
    chzzk_api_base_url: str = "https://api.chzzk.naver.com"
    chzzk_api_timeout_seconds: float = 10
    chzzk_live_poll_seconds: float = 15
    chzzk_live_poll_max_seconds: float = 60
    control_db_path: str = "data/control.db"
    dashboard_refresh_seconds: int = 5
    metrics_enabled: bool = False
    redis_enabled: bool = False
    redis_url: str | None = None
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str | None = None
    redis_ssl: bool = False
    redis_socket_timeout_seconds: float = 0.2


@dataclass(frozen=True)
class LiveSnapshot:
    channel_id: str
    normalized_status: str
    raw_status: str | None
    is_live: bool
    live_title: str | None
    open_date: str | None
    close_date: str | None
    viewer_count: int | None
    chat_channel_id: str | None
    error_message: str | None
    checked_at: str


@dataclass(frozen=True)
class ChannelMetadata:
    channel_id: str
    channel_name: str | None
    channel_image_url: str | None
    verified_mark: bool
    open_live: bool
    valid: bool
    error_message: str | None


@dataclass(frozen=True)
class StoredChannel:
    channel_id: str
    alias: str
    channel_name: str | None
    enabled: bool
    last_live_status: str | None
    last_live_title: str | None
    last_open_date: str | None
    last_close_date: str | None
    last_checked_at: str | None
    last_error: str | None
    viewer_count: int | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class MonitoringSession:
    id: int
    channel_id: str
    channel_name: str | None
    started_at: str
    ended_at: str | None
    stop_reason: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ResolvedControlChannel:
    channel_id: str
    alias: str
    channel_name: str | None
    snapshot: "LiveSnapshot"


class ChannelCommandError(Exception):
    def __init__(self, detail: str, status_code: int = 400):
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code


def normalize_channel_input(channel_input: str) -> str:
    raw_value = (channel_input or "").strip()
    if not raw_value:
        raise ValueError("channel_input or channel_id is required.")

    if raw_value.startswith("http://") or raw_value.startswith("https://"):
        parsed = urlparse(raw_value)
        path_parts = [part for part in parsed.path.split("/") if part]
        if not path_parts:
            raise ValueError("Could not parse channel id from URL.")
        return path_parts[-1]

    sanitized = raw_value.rstrip("/")
    if not sanitized:
        raise ValueError("Invalid channel input.")
    return sanitized


class LiveStatusClient:
    def __init__(self, base_url: str, timeout_seconds: float = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    async def fetch(self, channel_id: str) -> LiveSnapshot:
        url = f"{self.base_url}/polling/v3.1/channels/{channel_id}/live-status"

        try:
            with metrics.time("chzzk_live_status_request_seconds", result="attempt"):
                response = await asyncio.to_thread(requests.get, url, timeout=self.timeout_seconds)
            payload = response.json()
        except Exception as exc:
            metrics.increment("chzzk_live_status_requests_total", result="failure")
            return LiveSnapshot(
                channel_id=channel_id,
                normalized_status="ERROR",
                raw_status=None,
                is_live=False,
                live_title=None,
                open_date=None,
                close_date=None,
                viewer_count=None,
                chat_channel_id=None,
                error_message=str(exc),
                checked_at=now_iso(),
            )

        code = payload.get("code")
        message = payload.get("message")
        content = payload.get("content") or {}

        if code == 404:
            metrics.increment("chzzk_live_status_requests_total", result="not_found")
            return LiveSnapshot(
                channel_id=channel_id,
                normalized_status="NOT_FOUND",
                raw_status=None,
                is_live=False,
                live_title=None,
                open_date=None,
                close_date=None,
                viewer_count=None,
                chat_channel_id=None,
                error_message=message,
                checked_at=now_iso(),
            )

        if code != 200 or not isinstance(content, dict):
            metrics.increment("chzzk_live_status_requests_total", result="error")
            return LiveSnapshot(
                channel_id=channel_id,
                normalized_status="ERROR",
                raw_status=None,
                is_live=False,
                live_title=None,
                open_date=None,
                close_date=None,
                viewer_count=None,
                chat_channel_id=None,
                error_message=message or f"Unexpected response code: {code}",
                checked_at=now_iso(),
            )

        raw_status = content.get("status")
        is_live = raw_status == "OPEN"
        normalized_status = "OPEN" if is_live else "NOT_LIVE"
        metrics.increment("chzzk_live_status_requests_total", result=normalized_status.lower())

        return LiveSnapshot(
            channel_id=channel_id,
            normalized_status=normalized_status,
            raw_status=raw_status,
            is_live=is_live,
            live_title=content.get("liveTitle"),
            open_date=content.get("openDate"),
            close_date=content.get("closeDate"),
            viewer_count=content.get("concurrentUserCount"),
            chat_channel_id=content.get("chatChannelId"),
            error_message=None,
            checked_at=now_iso(),
        )

    async def fetch_channel_metadata(self, channel_id: str) -> ChannelMetadata:
        url = f"{self.base_url}/service/v1/channels/{channel_id}"
        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            response = await asyncio.to_thread(requests.get, url, timeout=self.timeout_seconds, headers=headers)
            payload = response.json()
        except Exception as exc:
            return ChannelMetadata(
                channel_id=channel_id,
                channel_name=None,
                channel_image_url=None,
                verified_mark=False,
                open_live=False,
                valid=False,
                error_message=str(exc),
            )

        content = payload.get("content") or {}
        content_channel_id = content.get("channelId")
        channel_name = content.get("channelName")
        valid = (
            payload.get("code") == 200
            and isinstance(content, dict)
            and content_channel_id == channel_id
            and isinstance(channel_name, str)
            and channel_name.strip() != ""
            and channel_name != "(알 수 없음)"
        )

        return ChannelMetadata(
            channel_id=channel_id,
            channel_name=channel_name if valid else None,
            channel_image_url=content.get("channelImageUrl") if valid else None,
            verified_mark=bool(content.get("verifiedMark", False)),
            open_live=bool(content.get("openLive", False)),
            valid=valid,
            error_message=None if valid else payload.get("message"),
        )


class ChannelStore:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS monitored_channels (
                        channel_id TEXT PRIMARY KEY,
                        alias TEXT NOT NULL DEFAULT '',
                        channel_name TEXT,
                        enabled INTEGER NOT NULL DEFAULT 1,
                        last_live_status TEXT,
                        last_live_title TEXT,
                        last_open_date TEXT,
                        last_close_date TEXT,
                        last_checked_at TEXT,
                        last_error TEXT,
                        viewer_count INTEGER,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS monitoring_sessions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        channel_id TEXT NOT NULL,
                        channel_name TEXT,
                        started_at TEXT NOT NULL,
                        ended_at TEXT,
                        stop_reason TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(monitored_channels)").fetchall()
                }
                if "channel_name" not in columns:
                    connection.execute("ALTER TABLE monitored_channels ADD COLUMN channel_name TEXT")
                connection.commit()

    @staticmethod
    def _row_to_channel(row: sqlite3.Row | None) -> StoredChannel | None:
        if row is None:
            return None
        return StoredChannel(
            channel_id=row["channel_id"],
            alias=row["alias"],
            channel_name=row["channel_name"],
            enabled=bool(row["enabled"]),
            last_live_status=row["last_live_status"],
            last_live_title=row["last_live_title"],
            last_open_date=row["last_open_date"],
            last_close_date=row["last_close_date"],
            last_checked_at=row["last_checked_at"],
            last_error=row["last_error"],
            viewer_count=row["viewer_count"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_monitoring_session(row: sqlite3.Row | None) -> MonitoringSession | None:
        if row is None:
            return None
        return MonitoringSession(
            id=row["id"],
            channel_id=row["channel_id"],
            channel_name=row["channel_name"],
            started_at=row["started_at"],
            ended_at=row["ended_at"],
            stop_reason=row["stop_reason"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def list_channels(self) -> list[StoredChannel]:
        with self._lock:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT channel_id, alias, channel_name, enabled, last_live_status, last_live_title,
                           last_open_date, last_close_date, last_checked_at, last_error,
                           viewer_count, created_at, updated_at
                    FROM monitored_channels
                    ORDER BY created_at ASC, channel_id ASC
                    """
                ).fetchall()
        return [self._row_to_channel(row) for row in rows]

    def get_channel(self, channel_id: str) -> StoredChannel | None:
        with self._lock:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT channel_id, alias, channel_name, enabled, last_live_status, last_live_title,
                           last_open_date, last_close_date, last_checked_at, last_error,
                           viewer_count, created_at, updated_at
                    FROM monitored_channels
                    WHERE channel_id = ?
                    """,
                    (channel_id,),
                ).fetchone()
        return self._row_to_channel(row)

    def upsert_channel(
        self,
        channel_id: str,
        alias: str,
        enabled: bool,
        channel_name: str | None = None,
    ) -> StoredChannel:
        existing = self.get_channel(channel_id)
        timestamp = now_iso()
        created_at = existing.created_at if existing else timestamp
        resolved_alias = alias if alias is not None else (existing.alias if existing else "")
        resolved_channel_name = channel_name if channel_name is not None else (existing.channel_name if existing else None)

        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO monitored_channels (
                        channel_id, alias, channel_name, enabled, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(channel_id) DO UPDATE SET
                        alias = excluded.alias,
                        channel_name = excluded.channel_name,
                        enabled = excluded.enabled,
                        updated_at = excluded.updated_at
                    """,
                    (channel_id, resolved_alias, resolved_channel_name, int(enabled), created_at, timestamp),
                )
                connection.commit()

        return self.get_channel(channel_id)

    def set_enabled(self, channel_id: str, enabled: bool) -> StoredChannel | None:
        timestamp = now_iso()
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE monitored_channels
                    SET enabled = ?, updated_at = ?
                    WHERE channel_id = ?
                    """,
                    (int(enabled), timestamp, channel_id),
                )
                connection.commit()
        return self.get_channel(channel_id)

    def delete_channel(self, channel_id: str) -> bool:
        with self._lock:
            with self._connect() as connection:
                cursor = connection.execute(
                    "DELETE FROM monitored_channels WHERE channel_id = ?",
                    (channel_id,),
                )
                connection.commit()
        return cursor.rowcount > 0

    def update_snapshot(self, channel_id: str, snapshot: LiveSnapshot) -> StoredChannel | None:
        timestamp = now_iso()
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE monitored_channels
                    SET last_live_status = ?,
                        last_live_title = ?,
                        last_open_date = ?,
                        last_close_date = ?,
                        last_checked_at = ?,
                        last_error = ?,
                        viewer_count = ?,
                        updated_at = ?
                    WHERE channel_id = ?
                    """,
                    (
                        snapshot.normalized_status,
                        snapshot.live_title,
                        snapshot.open_date,
                        snapshot.close_date,
                        snapshot.checked_at,
                        snapshot.error_message,
                        snapshot.viewer_count,
                        timestamp,
                        channel_id,
                    ),
                )
                connection.commit()
        return self.get_channel(channel_id)

    def get_open_monitoring_session(self, channel_id: str) -> MonitoringSession | None:
        with self._lock:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT id, channel_id, channel_name, started_at, ended_at, stop_reason, created_at, updated_at
                    FROM monitoring_sessions
                    WHERE channel_id = ? AND ended_at IS NULL
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (channel_id,),
                ).fetchone()
        return self._row_to_monitoring_session(row)

    def start_monitoring_session(self, channel_id: str, channel_name: str | None) -> MonitoringSession:
        existing = self.get_open_monitoring_session(channel_id)
        if existing is not None:
            return existing

        timestamp = now_iso()
        with self._lock:
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    INSERT INTO monitoring_sessions (
                        channel_id, channel_name, started_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (channel_id, channel_name, timestamp, timestamp, timestamp),
                )
                session_id = cursor.lastrowid
                connection.commit()

                row = connection.execute(
                    """
                    SELECT id, channel_id, channel_name, started_at, ended_at, stop_reason, created_at, updated_at
                    FROM monitoring_sessions
                    WHERE id = ?
                    """,
                    (session_id,),
                ).fetchone()
        return self._row_to_monitoring_session(row)

    def finish_monitoring_session(self, channel_id: str, stop_reason: str) -> MonitoringSession | None:
        timestamp = now_iso()
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    UPDATE monitoring_sessions
                    SET ended_at = ?, stop_reason = ?, updated_at = ?
                    WHERE id = (
                        SELECT id
                        FROM monitoring_sessions
                        WHERE channel_id = ? AND ended_at IS NULL
                        ORDER BY id DESC
                        LIMIT 1
                    )
                    """,
                    (timestamp, stop_reason, timestamp, channel_id),
                )
                connection.commit()

                row = connection.execute(
                    """
                    SELECT id, channel_id, channel_name, started_at, ended_at, stop_reason, created_at, updated_at
                    FROM monitoring_sessions
                    WHERE channel_id = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (channel_id,),
                ).fetchone()
        return self._row_to_monitoring_session(row)

    def list_monitoring_sessions(self, channel_id: str | None = None) -> list[MonitoringSession]:
        with self._lock:
            with self._connect() as connection:
                if channel_id is None:
                    rows = connection.execute(
                        """
                        SELECT id, channel_id, channel_name, started_at, ended_at, stop_reason, created_at, updated_at
                        FROM monitoring_sessions
                        ORDER BY id ASC
                        """
                    ).fetchall()
                else:
                    rows = connection.execute(
                        """
                        SELECT id, channel_id, channel_name, started_at, ended_at, stop_reason, created_at, updated_at
                        FROM monitoring_sessions
                        WHERE channel_id = ?
                        ORDER BY id ASC
                        """,
                        (channel_id,),
                    ).fetchall()
        return [self._row_to_monitoring_session(row) for row in rows]


class CmdCounter:
    """asyncio-safe runtime statistics for global and per-channel collection."""

    def __init__(self, recent_window_seconds: int = 60):
        self.recent_window_seconds = recent_window_seconds
        self.counts: dict[int, int] = {}
        self.channel_totals: dict[str, int] = defaultdict(int)
        self.channel_cmd_counts: dict[str, dict[int, int]] = defaultdict(dict)
        self.channel_recent_buckets: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
        self.global_recent_buckets: dict[int, int] = defaultdict(int)
        self.last_message_at: dict[str, str] = {}
        self.lock = asyncio.Lock()

    def _prune(self, now_ts: float):
        cutoff_bucket = int(now_ts - self.recent_window_seconds)
        for bucket in list(self.global_recent_buckets.keys()):
            if bucket < cutoff_bucket:
                del self.global_recent_buckets[bucket]
        for channel_id in list(self.channel_recent_buckets.keys()):
            buckets = self.channel_recent_buckets[channel_id]
            for bucket in list(buckets.keys()):
                if bucket < cutoff_bucket:
                    del buckets[bucket]
            if not buckets and self.channel_totals.get(channel_id, 0) == 0:
                del self.channel_recent_buckets[channel_id]

    async def increment(self, cmd):
        async with self.lock:
            self.counts[cmd] = self.counts.get(cmd, 0) + 1

    async def record_records(self, records: list[dict], observed_at: float | None = None):
        if not records:
            return

        now_ts = observed_at if observed_at is not None else time.time()

        async with self.lock:
            self._prune(now_ts)
            bucket = int(now_ts)
            for record in records:
                cmd = int(record.get("cmd", -1))
                channel_id = str(record.get("channel_id", "unknown"))
                self.counts[cmd] = self.counts.get(cmd, 0) + 1
                self.channel_totals[channel_id] += 1

                channel_counts = self.channel_cmd_counts[channel_id]
                channel_counts[cmd] = channel_counts.get(cmd, 0) + 1

                self.global_recent_buckets[bucket] += 1
                self.channel_recent_buckets[channel_id][bucket] += 1
                self.last_message_at[channel_id] = datetime.fromtimestamp(now_ts).astimezone().isoformat()

    async def note_channel_activity(self, channel_id: str, observed_at: float | None = None):
        now_ts = observed_at if observed_at is not None else time.time()
        async with self.lock:
            self.last_message_at[channel_id] = datetime.fromtimestamp(now_ts).astimezone().isoformat()

    async def get_counts(self):
        async with self.lock:
            return {str(cmd): count for cmd, count in self.counts.items()}

    async def snapshot(self):
        now_ts = time.time()
        async with self.lock:
            self._prune(now_ts)
            per_channel = {}
            for channel_id, total_count in self.channel_totals.items():
                per_channel[channel_id] = {
                    "total_collected_count": total_count,
                    "recent_events_per_minute": sum(self.channel_recent_buckets.get(channel_id, {}).values()),
                    "cmd_counts": {
                        str(cmd): count for cmd, count in self.channel_cmd_counts.get(channel_id, {}).items()
                    },
                    "last_message_at": self.last_message_at.get(channel_id),
                }

            return {
                "collected_cmds": {str(cmd): count for cmd, count in self.counts.items()},
                "total_collected_count": sum(self.counts.values()),
                "recent_events_per_minute": sum(self.global_recent_buckets.values()),
                "per_channel": per_channel,
            }


MonitorTaskFactory = Callable[
    [str, str, CmdCounter, LiveStatusClient | None, object | None, Callable[[], None]],
    Awaitable[None],
]


class MonitorCoordinator:
    def __init__(
        self,
        store: ChannelStore,
        live_status_client: LiveStatusClient,
        raw_publisher,
        counter: CmdCounter,
        poll_interval_seconds: float,
        monitor_task_factory: MonitorTaskFactory,
        poll_max_interval_seconds: float | None = None,
    ):
        self.store = store
        self.live_status_client = live_status_client
        self.raw_publisher = raw_publisher
        self.counter = counter
        self.poll_interval_seconds = poll_interval_seconds
        self.poll_max_interval_seconds = max(
            poll_interval_seconds,
            poll_max_interval_seconds if poll_max_interval_seconds is not None else poll_interval_seconds,
        )
        self.monitor_task_factory = monitor_task_factory
        self.tasks: dict[str, asyncio.Task] = {}
        self._last_live_poll_at: dict[str, float] = {}
        self._explicit_stop_reasons: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._poll_task: asyncio.Task | None = None

    def _task_done_callback(self, channel_id: str):
        def callback(task: asyncio.Task):
            current = self.tasks.get(channel_id)
            if current is task:
                self.tasks.pop(channel_id, None)
            if channel_id in self._explicit_stop_reasons:
                return
            if self.store.get_open_monitoring_session(channel_id) is None:
                return
            try:
                loop = task.get_loop()
            except RuntimeError:
                return
            if loop.is_closed():
                return
            loop.create_task(self._finalize_unexpected_task_stop(channel_id))

        return callback

    def is_monitoring(self, channel_id: str) -> bool:
        task = self.tasks.get(channel_id)
        return task is not None and not task.done()

    def monitoring_status_for(self, channel: StoredChannel) -> str:
        if self.is_monitoring(channel.channel_id):
            return "monitoring"
        if not channel.enabled:
            return "disabled"
        if channel.last_live_status == "OPEN":
            return "idle"
        if channel.last_live_status == "ERROR":
            return "error"
        return "idle"

    async def startup(self, start_background_tasks: bool):
        await self.sync_all_channels(refresh_channel_metadata=True)
        if start_background_tasks:
            self._poll_task = asyncio.create_task(self._poll_loop())

    async def shutdown(self):
        if self._poll_task is not None and not self._poll_task.done():
            self._poll_task.cancel()
            await asyncio.gather(self._poll_task, return_exceptions=True)

        running_channel_ids = list(self.tasks.keys())
        for channel_id in running_channel_ids:
            await self.stop_monitoring(channel_id, STOP_REASON_SERVER_SHUTDOWN)

    async def _poll_loop(self):
        while True:
            try:
                await asyncio.sleep(self.poll_interval_seconds)
                await self.sync_all_channels(refresh_channel_metadata=False)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[ERROR] monitor poll loop failed: {exc}")

    def _poll_interval_for(self, channel: StoredChannel) -> float:
        if channel.last_live_status == "NOT_LIVE" and not self.is_monitoring(channel.channel_id):
            return self.poll_max_interval_seconds
        return self.poll_interval_seconds

    def _should_poll_channel(self, channel: StoredChannel, now_ts: float) -> bool:
        last_polled_at = self._last_live_poll_at.get(channel.channel_id)
        if last_polled_at is None:
            return True
        return now_ts - last_polled_at >= self._poll_interval_for(channel)

    async def sync_all_channels(self, refresh_channel_metadata: bool = False):
        async with self._lock:
            channels = self.store.list_channels()
            now_ts = time.time()
            for channel in channels:
                if not refresh_channel_metadata and not self._should_poll_channel(channel, now_ts):
                    continue
                if refresh_channel_metadata:
                    metadata = await self.live_status_client.fetch_channel_metadata(channel.channel_id)
                    if metadata.valid and metadata.channel_name:
                        self.store.upsert_channel(
                            channel.channel_id,
                            channel.alias,
                            channel.enabled,
                            channel_name=metadata.channel_name,
                        )
                snapshot = await self.live_status_client.fetch(channel.channel_id)
                self._last_live_poll_at[channel.channel_id] = now_ts
                self.store.update_snapshot(channel.channel_id, snapshot)
                refreshed_channel = self.store.get_channel(channel.channel_id)
                await self._reconcile_channel(refreshed_channel, snapshot)

    async def reconcile_single_channel(self, channel_id: str, snapshot: LiveSnapshot | None = None):
        async with self._lock:
            channel = self.store.get_channel(channel_id)
            if channel is None:
                return None

            current_snapshot = snapshot or await self.live_status_client.fetch(channel_id)
            self.store.update_snapshot(channel_id, current_snapshot)
            refreshed_channel = self.store.get_channel(channel_id)
            await self._reconcile_channel(refreshed_channel, current_snapshot)
            return refreshed_channel

    async def _reconcile_channel(self, channel: StoredChannel | None, snapshot: LiveSnapshot):
        if channel is None:
            await self.stop_monitoring(snapshot.channel_id, STOP_REASON_UNKNOWN)
            return

        if not channel.enabled:
            await self.stop_monitoring(channel.channel_id, STOP_REASON_MANUAL_STOP)
            return

        if snapshot.normalized_status == "OPEN":
            await self.ensure_monitoring(channel)
            return

        if snapshot.normalized_status == "NOT_LIVE":
            await self.stop_monitoring(channel.channel_id, STOP_REASON_BROADCAST_ENDED)
            return

        if snapshot.normalized_status == "NOT_FOUND":
            await self.stop_monitoring(channel.channel_id, STOP_REASON_UNKNOWN)
            return

        if snapshot.normalized_status == "ERROR" and not self.is_monitoring(channel.channel_id):
            return

    async def ensure_monitoring(self, channel: StoredChannel):
        if self.is_monitoring(channel.channel_id):
            return

        alias = channel.channel_name or channel.alias or f"unknown_{channel.channel_id[:6]}"

        def on_connected():
            self.store.start_monitoring_session(channel.channel_id, alias)

        task = asyncio.create_task(
            self.monitor_task_factory(
                channel.channel_id,
                alias,
                self.counter,
                self.live_status_client,
                self.raw_publisher,
                on_connected,
            )
        )
        task.add_done_callback(self._task_done_callback(channel.channel_id))
        self.tasks[channel.channel_id] = task
        await asyncio.sleep(0)

    async def _finalize_unexpected_task_stop(self, channel_id: str):
        if self.store.get_open_monitoring_session(channel_id) is None:
            return
        self.store.finish_monitoring_session(channel_id, STOP_REASON_UNKNOWN)

    async def stop_monitoring(self, channel_id: str, stop_reason: str):
        had_open_session = self.store.get_open_monitoring_session(channel_id) is not None
        task = self.tasks.pop(channel_id, None)
        if task is None:
            if had_open_session:
                self.store.finish_monitoring_session(channel_id, stop_reason)
            return
        self._explicit_stop_reasons[channel_id] = stop_reason
        task.cancel()
        try:
            await asyncio.gather(task, return_exceptions=True)
        finally:
            self._explicit_stop_reasons.pop(channel_id, None)
        self.store.finish_monitoring_session(channel_id, stop_reason)

    async def dashboard_state(self):
        channels = self.store.list_channels()
        stats = await self.counter.snapshot()
        channels_payload = []

        for channel in channels:
            channel_stats = stats["per_channel"].get(channel.channel_id, {})
            display_name = channel.channel_name or channel.alias or channel.channel_id
            channels_payload.append(
                {
                    **asdict(channel),
                    "display_name": display_name,
                    "live_status": channel.last_live_status,
                    "live_title": channel.last_live_title,
                    "open_date": channel.last_open_date,
                    "close_date": channel.last_close_date,
                    "monitoring_status": self.monitoring_status_for(channel),
                    "total_collected_count": channel_stats.get("total_collected_count", 0),
                    "recent_events_per_minute": channel_stats.get("recent_events_per_minute", 0),
                    "cmd_counts": channel_stats.get("cmd_counts", {}),
                    "last_message_at": channel_stats.get("last_message_at"),
                }
            )

        summary = {
            "registered_channel_count": len(channels_payload),
            "enabled_channel_count": sum(1 for channel in channels_payload if channel["enabled"]),
            "live_channel_count": sum(1 for channel in channels_payload if channel["last_live_status"] == "OPEN"),
            "monitoring_channel_count": sum(
                1 for channel in channels_payload if channel["monitoring_status"] == "monitoring"
            ),
            "total_collected_count": stats["total_collected_count"],
            "recent_events_per_minute": stats["recent_events_per_minute"],
        }

        return {
            "summary": summary,
            "channels": channels_payload,
            "collected_cmds": stats["collected_cmds"],
        }

def _coerce_control_channel_requests(channels: list[dict] | list[str]) -> list[dict[str, str | None]]:
    if not isinstance(channels, list):
        raise ChannelCommandError("channels must be a list.")

    deduped: dict[str, dict[str, str | None]] = {}
    for item in channels:
        if isinstance(item, str):
            raw_channel = item
            alias = None
        elif isinstance(item, dict):
            raw_channel = item.get("channel_id") or item.get("channel_input")
            alias = item.get("alias")
        else:
            raise ChannelCommandError("each channel entry must be a string or object.")

        try:
            channel_id = normalize_channel_input(raw_channel or "")
        except ValueError as exc:
            raise ChannelCommandError(str(exc)) from exc

        deduped[channel_id] = {
            "channel_id": channel_id,
            "alias": alias,
        }

    return list(deduped.values())


async def _resolve_control_channels(
    store: ChannelStore,
    live_status_client: LiveStatusClient,
    channels: list[dict[str, str | None]],
) -> list[ResolvedControlChannel]:
    resolved = []
    for item in channels:
        channel_id = str(item["channel_id"])
        alias = item.get("alias")
        snapshot = await live_status_client.fetch(channel_id)
        metadata = await live_status_client.fetch_channel_metadata(channel_id)

        if snapshot.normalized_status == "NOT_FOUND":
            raise ChannelCommandError(snapshot.error_message or "Channel not found.", status_code=404)
        if snapshot.normalized_status == "ERROR":
            raise ChannelCommandError(snapshot.error_message or "Live status lookup failed.", status_code=502)

        existing = store.get_channel(channel_id)
        resolved_alias = alias if alias is not None else (existing.alias if existing is not None else "")
        resolved_channel_name = (
            metadata.channel_name
            if metadata.valid
            else (existing.channel_name if existing is not None else None)
        )

        resolved.append(
            ResolvedControlChannel(
                channel_id=channel_id,
                alias=resolved_alias,
                channel_name=resolved_channel_name,
                snapshot=snapshot,
            )
        )
    return resolved


async def apply_control_command(
    store: ChannelStore,
    live_status_client: LiveStatusClient,
    coordinator: MonitorCoordinator,
    action: str,
    channels: list[dict] | list[str],
):
    normalized_action = (action or "").strip().lower()
    if normalized_action not in {"add", "remove", "replace"}:
        raise ChannelCommandError(f"Unsupported control action: {action}")

    normalized_channels = _coerce_control_channel_requests(channels)

    if normalized_action in {"add", "replace"}:
        resolved_channels = await _resolve_control_channels(store, live_status_client, normalized_channels)
        for item in resolved_channels:
            store.upsert_channel(
                item.channel_id,
                item.alias,
                True,
                channel_name=item.channel_name,
            )
            store.update_snapshot(item.channel_id, item.snapshot)
            await coordinator.reconcile_single_channel(item.channel_id, item.snapshot)

        if normalized_action == "replace":
            desired_ids = {item.channel_id for item in resolved_channels}
            for existing in store.list_channels():
                if existing.channel_id in desired_ids or not existing.enabled:
                    continue
                store.set_enabled(existing.channel_id, False)
                await coordinator.stop_monitoring(existing.channel_id, STOP_REASON_MANUAL_STOP)

        return {
            "action": normalized_action,
            "channel_ids": [item.channel_id for item in resolved_channels],
        }

    for item in normalized_channels:
        existing = store.get_channel(str(item["channel_id"]))
        if existing is None:
            continue
        store.set_enabled(existing.channel_id, False)
        await coordinator.stop_monitoring(existing.channel_id, STOP_REASON_MANUAL_STOP)

    return {
        "action": normalized_action,
        "channel_ids": [str(item["channel_id"]) for item in normalized_channels],
    }
