import asyncio
import importlib
import json
import sys
from urllib.request import urlopen

from fastapi.testclient import TestClient
from websockets.exceptions import ConnectionClosedOK


def load_collector_module():
    sys.modules.pop("chzzk_collector_server", None)
    return importlib.import_module("chzzk_collector_server")


class FakeLiveStatusClient:
    async def fetch(self, channel_id):
        raise AssertionError("fetch should not be called in metrics endpoint tests")

    async def fetch_channel_metadata(self, channel_id):
        raise AssertionError("fetch_channel_metadata should not be called in metrics endpoint tests")


class FakeMonitorTaskFactory:
    async def __call__(self, channel_id, streamer_nickname, counter, live_status_client=None, raw_publisher=None, on_connected=None):
        raise AssertionError("monitor task should not be started in metrics endpoint tests")


class FakeRawPublisher:
    metrics_backend = "kafka"

    async def start(self):
        return None

    async def stop(self):
        return None

    async def publish(self, channel_id, payload, raw_payload=None):
        return None


class FakeWebSocket:
    def __init__(self, messages):
        self._messages = list(messages)

    async def recv(self):
        if self._messages:
            return self._messages.pop(0)
        raise ConnectionClosedOK(None, None)


def make_single_message(message, cmd=93101):
    return {
        "svcid": "game",
        "ver": "1",
        "cmd": cmd,
        "tid": "4",
        "cid": "Hp4sXM",
        "bdy": {
            "uid": "user-1",
            "profile": json.dumps({"nickname": "tester"}, ensure_ascii=False),
            "msg": message,
            "msgTypeCode": 1,
            "extras": json.dumps({"streamingChannelId": "channel-1"}, ensure_ascii=False),
            "cmd": cmd,
        },
    }


def build_app(collector, tmp_path, *, metrics_enabled):
    settings = collector.AppSettings(
        event_bus_backend="kafka",
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="chzzk.events.raw",
        kafka_client_id="chzzk-collector",
        chzzk_api_base_url="https://api.example.com",
        chzzk_api_timeout_seconds=5,
        chzzk_live_poll_seconds=15,
        control_db_path=str(tmp_path / "control.db"),
        dashboard_refresh_seconds=5,
        metrics_enabled=metrics_enabled,
    )
    return collector.create_app(
        settings=settings,
        live_status_client=FakeLiveStatusClient(),
        monitor_task_factory=FakeMonitorTaskFactory(),
        raw_publisher=FakeRawPublisher(),
        start_background_tasks=False,
    )


def test_metrics_route_is_hidden_when_metrics_disabled(tmp_path):
    collector = load_collector_module()

    with TestClient(build_app(collector, tmp_path, metrics_enabled=False)) as client:
        response = client.get("/metrics")

    assert response.status_code == 404


def test_metrics_route_exposes_receive_message_counters_when_enabled(tmp_path):
    collector = load_collector_module()
    app = build_app(collector, tmp_path, metrics_enabled=True)
    websocket = FakeWebSocket([json.dumps(make_single_message("hello"), ensure_ascii=False)])

    with TestClient(app) as client:
        asyncio.run(
            collector.receive_messages(
                websocket,
                "channel-1",
                "streamer",
                app.state.counter,
                {"value": 0.0},
                app.state.raw_publisher,
            )
        )

        response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "chzzk_ws_frames_received_total" in response.text
    assert "chzzk_ws_frame_bytes_received_total" in response.text
    assert "chzzk_raw_publish_attempts_total" in response.text
    assert 'chzzk_raw_publish_success_total{backend="kafka"' in response.text
    assert "chzzk_ws_to_publish_ack_seconds_count" in response.text
    assert "chzzk_stats_records_total" in response.text


def test_metrics_http_server_exposes_same_metrics_registry():
    metrics_module = importlib.import_module("collector.metrics")
    metrics_module.metrics.increment("chzzk_test_metric_total", result="ok")
    server = metrics_module.start_metrics_http_server(host="127.0.0.1", port=0)

    try:
        host, port = server.server_address
        with urlopen(f"http://{host}:{port}/metrics", timeout=2) as response:
            body = response.read().decode("utf-8")
    finally:
        server.shutdown()
        server.server_close()

    assert "chzzk_test_metric_total" in body
    assert 'result="ok"' in body
