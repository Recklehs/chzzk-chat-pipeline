import asyncio
import importlib
import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def load_collector_module():
    sys.modules.pop("chzzk_control", None)
    sys.modules.pop("chzzk_collector_server", None)
    return importlib.import_module("chzzk_collector_server")


class FakeLiveStatusClient:
    def __init__(self, snapshots, metadata=None):
        self.snapshots = snapshots
        self.metadata = metadata or {}
        self.calls = []
        self.metadata_calls = []

    async def fetch(self, channel_id):
        self.calls.append(channel_id)
        result = self.snapshots[channel_id]
        if callable(result):
            return result()
        return result

    async def fetch_channel_metadata(self, channel_id):
        self.metadata_calls.append(channel_id)
        result = self.metadata.get(channel_id)
        if callable(result):
            return result()
        return result


class FakeMonitorTaskFactory:
    def __init__(self):
        self.started = []
        self.cancelled = []
        self.raw_publishers = []

    async def __call__(self, channel_id, streamer_nickname, counter, live_status_client=None, raw_publisher=None, on_connected=None):
        self.started.append((channel_id, streamer_nickname))
        self.raw_publishers.append(raw_publisher)
        on_connected()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.append(channel_id)
            raise


class CompletingMonitorTaskFactory:
    def __init__(self):
        self.started = []

    async def __call__(self, channel_id, streamer_nickname, counter, live_status_client=None, raw_publisher=None, on_connected=None):
        self.started.append((channel_id, streamer_nickname))
        on_connected()
        return


class FakeRawPublisher:
    def __init__(self):
        self.started = 0
        self.stopped = 0

    async def start(self):
        self.started += 1

    async def stop(self):
        self.stopped += 1


class FailingRawPublisher(FakeRawPublisher):
    async def start(self):
        self.started += 1
        raise RuntimeError("kafka unavailable")


class RecordingJsonCache:
    def __init__(self):
        self.calls = []

    async def get_or_set_json_cache(self, key, ttl_seconds, loader):
        self.calls.append((key, ttl_seconds))
        return await loader()


def make_settings(collector, tmp_path):
    return collector.AppSettings(
        event_bus_backend="pubsub",
        kafka_bootstrap_servers="localhost:9092",
        pubsub_project_id="demo-project",
        pubsub_raw_topic="raw-topic",
        chzzk_api_base_url="https://api.example.com",
        chzzk_api_timeout_seconds=5,
        chzzk_live_poll_seconds=15,
        control_db_path=str(tmp_path / "control.db"),
        dashboard_refresh_seconds=5,
    )


def make_live_snapshot(collector, channel_id, normalized_status="OPEN", *, title="Live now", error_message=None):
    return collector.LiveSnapshot(
        channel_id=channel_id,
        normalized_status=normalized_status,
        raw_status="OPEN" if normalized_status == "OPEN" else "CLOSE",
        is_live=normalized_status == "OPEN",
        live_title=title if normalized_status == "OPEN" else None,
        open_date="2026-03-13 20:00:00" if normalized_status == "OPEN" else None,
        close_date=None if normalized_status == "OPEN" else "2026-03-13 21:00:00",
        viewer_count=123 if normalized_status == "OPEN" else 0,
        chat_channel_id="chat-1" if normalized_status == "OPEN" else None,
        error_message=error_message,
        checked_at="2026-03-13T20:00:00+09:00",
    )


def make_channel_metadata(collector, channel_id, channel_name="공식 채널명", *, valid=True, error_message=None):
    return collector.ChannelMetadata(
        channel_id=channel_id,
        channel_name=channel_name if valid else None,
        channel_image_url="https://example.com/channel.png" if valid else None,
        verified_mark=False,
        open_live=True,
        valid=valid,
        error_message=error_message,
    )


def create_test_client(collector, tmp_path, live_status_client, monitor_factory):
    settings = make_settings(collector, tmp_path)
    raw_publisher = FakeRawPublisher()
    app = collector.create_app(
        settings=settings,
        live_status_client=live_status_client,
        monitor_task_factory=monitor_factory,
        start_background_tasks=False,
        raw_publisher=raw_publisher,
    )
    return TestClient(app)


def fetch_monitoring_sessions(db_path):
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT channel_id, channel_name, started_at, ended_at, stop_reason
            FROM monitoring_sessions
            ORDER BY id ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


async def build_active_coordinator(collector, tmp_path, *, channel_id="channel-open", channel_name="공식 오픈 채널"):
    live_client = FakeLiveStatusClient(
        {
            channel_id: make_live_snapshot(collector, channel_id, normalized_status="OPEN"),
        },
        metadata={
            channel_id: make_channel_metadata(collector, channel_id, channel_name=channel_name),
        },
    )
    monitor_factory = FakeMonitorTaskFactory()
    store = collector.ChannelStore(str(tmp_path / "control.db"))
    channel = store.upsert_channel(channel_id, "Open Channel", True, channel_name=channel_name)
    snapshot = make_live_snapshot(collector, channel_id, normalized_status="OPEN")
    store.update_snapshot(channel_id, snapshot)
    coordinator = collector.MonitorCoordinator(
        store=store,
        live_status_client=live_client,
        raw_publisher=FakeRawPublisher(),
        counter=collector.CmdCounter(),
        poll_interval_seconds=15,
        monitor_task_factory=monitor_factory,
    )
    await coordinator.ensure_monitoring(channel)
    return coordinator, live_client, monitor_factory, tmp_path / "control.db"


def test_channel_registration_toggle_and_dashboard_state(tmp_path):
    collector = load_collector_module()
    live_client = FakeLiveStatusClient(
        {
            "channel-open": make_live_snapshot(collector, "channel-open", normalized_status="OPEN"),
        },
        metadata={
            "channel-open": make_channel_metadata(collector, "channel-open", channel_name="공식 오픈 채널"),
        },
    )
    monitor_factory = FakeMonitorTaskFactory()

    with create_test_client(collector, tmp_path, live_client, monitor_factory) as client:
        add_response = client.post(
            "/channels",
            params={
                "channel_input": "https://chzzk.naver.com/live/channel-open",
                "alias": "Open Channel",
            },
        )
        assert add_response.status_code == 202
        assert monitor_factory.started == [("channel-open", "공식 오픈 채널")]
        assert len(monitor_factory.raw_publishers) == 1
        assert monitor_factory.raw_publishers[0] is not None

        list_response = client.get("/channels")
        assert list_response.status_code == 200
        body = list_response.json()
        assert body["monitoring_count"] == 1
        assert len(body["channels"]) == 1
        assert body["channels"][0]["channel_id"] == "channel-open"
        assert body["channels"][0]["alias"] == "Open Channel"
        assert body["channels"][0]["channel_name"] == "공식 오픈 채널"
        assert body["channels"][0]["display_name"] == "공식 오픈 채널"
        assert body["channels"][0]["enabled"] is True
        assert body["channels"][0]["live_status"] == "OPEN"
        assert body["channels"][0]["monitoring_status"] == "monitoring"

        dashboard_response = client.get("/dashboard")
        assert dashboard_response.status_code == 200
        assert 'id="channel-form"' in dashboard_response.text
        assert "set-cookie" not in dashboard_response.headers

        state_response = client.get("/dashboard/api/state")
        assert state_response.status_code == 200
        state = state_response.json()
        assert state["summary"]["registered_channel_count"] == 1
        assert state["summary"]["monitoring_channel_count"] == 1
        assert state["channels"][0]["channel_id"] == "channel-open"
        assert state["channels"][0]["channel_name"] == "공식 오픈 채널"
        assert state["channels"][0]["display_name"] == "공식 오픈 채널"

        disable_response = client.patch(
            "/channels/channel-open/enabled",
            json={"enabled": False},
        )
        assert disable_response.status_code == 200
        assert monitor_factory.cancelled == ["channel-open"]

        disabled_channels = client.get("/channels").json()
        assert disabled_channels["channels"][0]["enabled"] is False
        assert disabled_channels["channels"][0]["monitoring_status"] == "disabled"


def test_dashboard_state_uses_realtime_redis_cache_key(tmp_path):
    collector = load_collector_module()
    live_client = FakeLiveStatusClient({})
    monitor_factory = FakeMonitorTaskFactory()
    settings = make_settings(collector, tmp_path)
    app = collector.create_app(
        settings=settings,
        live_status_client=live_client,
        monitor_task_factory=monitor_factory,
        start_background_tasks=False,
        raw_publisher=FakeRawPublisher(),
    )
    cache = RecordingJsonCache()
    app.state.redis_cache = cache

    with TestClient(app) as client:
        response = client.get("/dashboard/api/state", params={"window": "60s"})

    assert response.status_code == 200
    assert cache.calls == [("chat:dashboard:realtime:window=60s", 10)]
    assert response.json()["refresh_seconds"] == 5


def test_monitoring_session_waits_for_connection_success(tmp_path):
    collector = load_collector_module()

    async def scenario():
        callbacks = []

        async def monitor(channel_id, nickname, counter, live_client, publisher, on_connected):
            callbacks.append(on_connected)
            await asyncio.Event().wait()

        store = collector.ChannelStore(str(tmp_path / "control.db"))
        channel = store.upsert_channel("channel-open", "Open Channel", True)
        coordinator = collector.MonitorCoordinator(
            store=store,
            live_status_client=FakeLiveStatusClient({}),
            raw_publisher=FakeRawPublisher(),
            counter=collector.CmdCounter(),
            poll_interval_seconds=15,
            monitor_task_factory=monitor,
        )
        try:
            await coordinator.ensure_monitoring(channel)
            assert store.list_monitoring_sessions() == []
            callbacks[0]()
            assert len(store.list_monitoring_sessions()) == 1
            assert store.get_open_monitoring_session("channel-open") is not None
        finally:
            await coordinator.shutdown()

    asyncio.run(scenario())


def test_channel_registration_records_monitoring_session_and_disable_closes_with_manual_stop(tmp_path):
    collector = load_collector_module()
    live_client = FakeLiveStatusClient(
        {
            "channel-open": make_live_snapshot(collector, "channel-open", normalized_status="OPEN"),
        },
        metadata={
            "channel-open": make_channel_metadata(collector, "channel-open", channel_name="공식 오픈 채널"),
        },
    )
    monitor_factory = FakeMonitorTaskFactory()
    db_path = tmp_path / "control.db"

    with create_test_client(collector, tmp_path, live_client, monitor_factory) as client:
        response = client.post(
            "/channels",
            params={"channel_input": "channel-open", "alias": "Open Channel"},
        )
        assert response.status_code == 202

        sessions = fetch_monitoring_sessions(db_path)
        assert sessions == [
            {
                "channel_id": "channel-open",
                "channel_name": "공식 오픈 채널",
                "started_at": sessions[0]["started_at"],
                "ended_at": None,
                "stop_reason": None,
            }
        ]

        response = client.patch(
            "/channels/channel-open/enabled",
            json={"enabled": False},
        )
        assert response.status_code == 200

        sessions = fetch_monitoring_sessions(db_path)
        assert len(sessions) == 1
        assert sessions[0]["channel_id"] == "channel-open"
        assert sessions[0]["channel_name"] == "공식 오픈 채널"
        assert sessions[0]["ended_at"] is not None
        assert sessions[0]["stop_reason"] == "MANUAL_STOP"


def test_delete_channel_closes_monitoring_session_with_channel_removed(tmp_path):
    collector = load_collector_module()
    live_client = FakeLiveStatusClient(
        {
            "channel-open": make_live_snapshot(collector, "channel-open", normalized_status="OPEN"),
        },
        metadata={
            "channel-open": make_channel_metadata(collector, "channel-open", channel_name="공식 오픈 채널"),
        },
    )
    monitor_factory = FakeMonitorTaskFactory()
    db_path = tmp_path / "control.db"

    with create_test_client(collector, tmp_path, live_client, monitor_factory) as client:
        response = client.post(
            "/channels",
            params={"channel_input": "channel-open", "alias": "Open Channel"},
        )
        assert response.status_code == 202

        response = client.delete(
            "/channels",
            params={"channel_id": "channel-open"},
        )
        assert response.status_code == 200

        sessions = fetch_monitoring_sessions(db_path)
        assert len(sessions) == 1
        assert sessions[0]["ended_at"] is not None
        assert sessions[0]["stop_reason"] == "CHANNEL_REMOVED"


def test_live_status_not_live_closes_monitoring_session_with_broadcast_ended(tmp_path):
    collector = load_collector_module()
    
    async def scenario():
        coordinator, live_client, _monitor_factory, db_path = await build_active_coordinator(collector, tmp_path)
        live_client.snapshots["channel-open"] = make_live_snapshot(
            collector,
            "channel-open",
            normalized_status="NOT_LIVE",
        )

        await coordinator.reconcile_single_channel("channel-open")

        sessions = fetch_monitoring_sessions(db_path)
        assert len(sessions) == 1
        assert sessions[0]["ended_at"] is not None
        assert sessions[0]["stop_reason"] == "BROADCAST_ENDED"

    asyncio.run(scenario())


def test_live_status_not_found_closes_monitoring_session_with_unknown(tmp_path):
    collector = load_collector_module()
    
    async def scenario():
        coordinator, live_client, _monitor_factory, db_path = await build_active_coordinator(collector, tmp_path)
        live_client.snapshots["channel-open"] = make_live_snapshot(
            collector,
            "channel-open",
            normalized_status="NOT_FOUND",
            error_message="gone",
        )

        await coordinator.reconcile_single_channel("channel-open")

        sessions = fetch_monitoring_sessions(db_path)
        assert len(sessions) == 1
        assert sessions[0]["ended_at"] is not None
        assert sessions[0]["stop_reason"] == "UNKNOWN"

    asyncio.run(scenario())


def test_app_shutdown_closes_monitoring_sessions_with_server_shutdown(tmp_path):
    collector = load_collector_module()
    
    async def scenario():
        coordinator, _live_client, _monitor_factory, db_path = await build_active_coordinator(collector, tmp_path)

        sessions = fetch_monitoring_sessions(db_path)
        assert len(sessions) == 1
        assert sessions[0]["ended_at"] is None

        await coordinator.shutdown()

        sessions = fetch_monitoring_sessions(db_path)
        assert len(sessions) == 1
        assert sessions[0]["ended_at"] is not None
        assert sessions[0]["stop_reason"] == "SERVER_SHUTDOWN"

    asyncio.run(scenario())


def test_unexpected_task_completion_closes_monitoring_session_with_unknown(tmp_path):
    collector = load_collector_module()

    async def scenario():
        store = collector.ChannelStore(str(tmp_path / "control.db"))
        channel = store.upsert_channel("channel-open", "Open Channel", True, channel_name="공식 오픈 채널")
        store.update_snapshot(
            "channel-open",
            make_live_snapshot(collector, "channel-open", normalized_status="OPEN"),
        )
        monitor_factory = CompletingMonitorTaskFactory()
        coordinator = collector.MonitorCoordinator(
            store=store,
            live_status_client=FakeLiveStatusClient({"channel-open": make_live_snapshot(collector, "channel-open")}),
            raw_publisher=FakeRawPublisher(),
            counter=collector.CmdCounter(),
            poll_interval_seconds=15,
            monitor_task_factory=monitor_factory,
        )

        await coordinator.ensure_monitoring(channel)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        sessions = fetch_monitoring_sessions(tmp_path / "control.db")
        assert len(sessions) == 1
        assert sessions[0]["ended_at"] is not None
        assert sessions[0]["stop_reason"] == "UNKNOWN"

    asyncio.run(scenario())


def test_sync_all_channels_backs_off_not_live_channels_but_keeps_open_channels_polling(monkeypatch, tmp_path):
    collector = load_collector_module()

    async def scenario():
        store = collector.ChannelStore(str(tmp_path / "control.db"))
        store.upsert_channel("channel-idle", "Idle", True, channel_name="Idle")
        store.upsert_channel("channel-open", "Open", True, channel_name="Open")
        store.update_snapshot("channel-idle", make_live_snapshot(collector, "channel-idle", normalized_status="NOT_LIVE"))
        store.update_snapshot("channel-open", make_live_snapshot(collector, "channel-open", normalized_status="OPEN"))
        live_client = FakeLiveStatusClient(
            {
                "channel-idle": make_live_snapshot(collector, "channel-idle", normalized_status="NOT_LIVE"),
                "channel-open": make_live_snapshot(collector, "channel-open", normalized_status="OPEN"),
            }
        )
        coordinator = collector.MonitorCoordinator(
            store=store,
            live_status_client=live_client,
            raw_publisher=FakeRawPublisher(),
            counter=collector.CmdCounter(),
            poll_interval_seconds=15,
            poll_max_interval_seconds=60,
            monitor_task_factory=FakeMonitorTaskFactory(),
        )
        coordinator._last_live_poll_at = {"channel-idle": 1000.0, "channel-open": 1000.0}
        monkeypatch.setattr(collector.time, "time", lambda: 1030.0)

        await coordinator.sync_all_channels(refresh_channel_metadata=False)

        assert live_client.calls == ["channel-open"]

    asyncio.run(scenario())


def test_sync_all_channels_polls_not_live_channels_at_max_interval(monkeypatch, tmp_path):
    collector = load_collector_module()

    async def scenario():
        store = collector.ChannelStore(str(tmp_path / "control.db"))
        store.upsert_channel("channel-idle", "Idle", True, channel_name="Idle")
        store.update_snapshot("channel-idle", make_live_snapshot(collector, "channel-idle", normalized_status="NOT_LIVE"))
        live_client = FakeLiveStatusClient(
            {
                "channel-idle": make_live_snapshot(collector, "channel-idle", normalized_status="NOT_LIVE"),
            }
        )
        coordinator = collector.MonitorCoordinator(
            store=store,
            live_status_client=live_client,
            raw_publisher=FakeRawPublisher(),
            counter=collector.CmdCounter(),
            poll_interval_seconds=15,
            poll_max_interval_seconds=60,
            monitor_task_factory=FakeMonitorTaskFactory(),
        )
        coordinator._last_live_poll_at = {"channel-idle": 1000.0}
        monkeypatch.setattr(collector.time, "time", lambda: 1060.0)

        await coordinator.sync_all_channels(refresh_channel_metadata=False)

        assert live_client.calls == ["channel-idle"]

    asyncio.run(scenario())


def test_invalid_channel_is_rejected(tmp_path):
    collector = load_collector_module()
    live_client = FakeLiveStatusClient(
        {
            "missing-channel": make_live_snapshot(
                collector,
                "missing-channel",
                normalized_status="NOT_FOUND",
                error_message="채널이 존재하지 않습니다.",
            ),
        },
        metadata={
            "missing-channel": make_channel_metadata(collector, "missing-channel", valid=False),
        },
    )
    monitor_factory = FakeMonitorTaskFactory()

    with create_test_client(collector, tmp_path, live_client, monitor_factory) as client:
        response = client.post(
            "/channels",
            params={"channel_input": "missing-channel"},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "채널이 존재하지 않습니다."
    assert monitor_factory.started == []


def test_startup_restores_enabled_live_channels(tmp_path):
    collector = load_collector_module()
    initial_live_client = FakeLiveStatusClient(
        {
            "channel-open": make_live_snapshot(collector, "channel-open", normalized_status="OPEN"),
        },
        metadata={
            "channel-open": make_channel_metadata(collector, "channel-open", channel_name="공식 복구 채널"),
        },
    )
    initial_monitor_factory = FakeMonitorTaskFactory()

    with create_test_client(collector, tmp_path, initial_live_client, initial_monitor_factory) as client:
        response = client.post(
            "/channels",
            params={"channel_input": "channel-open", "alias": "Remembered Channel"},
        )
        assert response.status_code == 202

    restored_live_client = FakeLiveStatusClient(
        {
            "channel-open": make_live_snapshot(collector, "channel-open", normalized_status="OPEN"),
        },
        metadata={
            "channel-open": make_channel_metadata(collector, "channel-open", channel_name="공식 복구 채널"),
        },
    )
    restored_monitor_factory = FakeMonitorTaskFactory()

    with create_test_client(collector, tmp_path, restored_live_client, restored_monitor_factory) as client:
        channels_response = client.get("/channels")
        assert channels_response.status_code == 200
        payload = channels_response.json()
        assert payload["monitoring_count"] == 1
        assert payload["channels"][0]["channel_id"] == "channel-open"
        assert payload["channels"][0]["channel_name"] == "공식 복구 채널"
        assert payload["channels"][0]["display_name"] == "공식 복구 채널"
        assert payload["channels"][0]["monitoring_status"] == "monitoring"

    assert restored_monitor_factory.started == [("channel-open", "공식 복구 채널")]


def test_registration_succeeds_when_metadata_lookup_fails(tmp_path):
    collector = load_collector_module()
    live_client = FakeLiveStatusClient(
        {
            "channel-open": make_live_snapshot(collector, "channel-open", normalized_status="OPEN"),
        },
        metadata={
            "channel-open": make_channel_metadata(collector, "channel-open", valid=False, error_message="lookup failed"),
        },
    )
    monitor_factory = FakeMonitorTaskFactory()

    with create_test_client(collector, tmp_path, live_client, monitor_factory) as client:
        response = client.post(
            "/channels",
            params={"channel_input": "channel-open", "alias": "Fallback Alias"},
        )

        assert response.status_code == 202
        payload = response.json()
        assert payload["channel"]["channel_name"] is None
        assert payload["channel"]["display_name"] == "Fallback Alias"
        assert monitor_factory.started == [("channel-open", "Fallback Alias")]


def test_channel_store_adds_channel_name_column_for_existing_db(tmp_path):
    collector = load_collector_module()
    db_path = tmp_path / "control.db"

    import sqlite3

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE monitored_channels (
                channel_id TEXT PRIMARY KEY,
                alias TEXT NOT NULL DEFAULT '',
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
        connection.commit()

    store = collector.ChannelStore(str(db_path))
    store.upsert_channel("channel-1", "alias-1", True, channel_name="공식 이름")
    stored = store.get_channel("channel-1")

    assert stored.channel_name == "공식 이름"

    with sqlite3.connect(db_path) as connection:
        table_names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert "monitoring_sessions" in table_names


def test_app_starts_and_stops_raw_publisher(tmp_path):
    collector = load_collector_module()
    settings = make_settings(collector, tmp_path)
    raw_publisher = FakeRawPublisher()
    live_client = FakeLiveStatusClient({})
    monitor_factory = FakeMonitorTaskFactory()

    app = collector.create_app(
        settings=settings,
        live_status_client=live_client,
        monitor_task_factory=monitor_factory,
        start_background_tasks=False,
        raw_publisher=raw_publisher,
    )

    with TestClient(app):
        assert raw_publisher.started == 1
        assert raw_publisher.stopped == 0

    assert raw_publisher.stopped == 1


def test_app_creates_default_raw_publisher_when_not_provided(tmp_path, monkeypatch):
    collector = load_collector_module()
    settings = make_settings(collector, tmp_path)
    created_publishers = []
    seen_settings = []

    def fake_factory(settings_arg):
        publisher = FakeRawPublisher()
        created_publishers.append(publisher)
        seen_settings.append(settings_arg)
        return publisher

    monkeypatch.setattr(collector, "build_default_raw_publisher", fake_factory)

    live_client = FakeLiveStatusClient({})
    monitor_factory = FakeMonitorTaskFactory()

    app = collector.create_app(
        settings=settings,
        live_status_client=live_client,
        monitor_task_factory=monitor_factory,
        start_background_tasks=False,
    )

    with TestClient(app):
        assert len(created_publishers) == 1
        assert app.state.raw_publisher is created_publishers[0]
        assert created_publishers[0].started == 1
        assert seen_settings == [settings]

    assert created_publishers[0].stopped == 1


def test_app_startup_fails_when_raw_publisher_cannot_start(tmp_path):
    collector = load_collector_module()
    settings = make_settings(collector, tmp_path)
    live_client = FakeLiveStatusClient({})
    monitor_factory = FakeMonitorTaskFactory()
    raw_publisher = FailingRawPublisher()

    app = collector.create_app(
        settings=settings,
        live_status_client=live_client,
        monitor_task_factory=monitor_factory,
        start_background_tasks=False,
        raw_publisher=raw_publisher,
    )

    with pytest.raises(RuntimeError, match="kafka unavailable"):
        with TestClient(app):
            pass


def test_app_starts_and_stops_without_control_plane(tmp_path):
    collector = load_collector_module()
    settings = make_settings(collector, tmp_path)
    live_client = FakeLiveStatusClient({})
    raw_publisher = FakeRawPublisher()

    app = collector.create_app(
        settings=settings,
        live_status_client=live_client,
        raw_publisher=raw_publisher,
        monitor_task_factory=FakeMonitorTaskFactory(),
        start_background_tasks=False,
    )

    with TestClient(app):
        assert raw_publisher.started == 1
