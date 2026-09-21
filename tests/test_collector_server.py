import asyncio
import importlib
import json
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from websockets.exceptions import ConnectionClosedOK


def load_collector_module():
    sys.modules.pop("chzzk_collector_server", None)
    return importlib.import_module("chzzk_collector_server")


def make_batch_item(message, cmd=93101):
    return {
        "svcid": "game",
        "cid": "Hp4sXM",
        "mbrCnt": 6411,
        "uid": "user-1",
        "profile": json.dumps(
            {
                "userIdHash": "user-1",
                "nickname": "tester",
                "verifiedMark": False,
                "userRoleCode": "common_user",
            },
            ensure_ascii=False,
        ),
        "msg": message,
        "msgTypeCode": 1,
        "msgStatusType": "NORMAL",
        "extras": json.dumps(
            {
                "chatType": "STREAMING",
                "osType": "PC",
                "streamingChannelId": "channel-1",
            },
            ensure_ascii=False,
        ),
        "ctime": 1772456186360,
        "utime": 1772456186360,
        "msgTid": None,
        "cuid": None,
        "msgTime": 1772456186360,
        "cmd": cmd,
    }


def make_single_message(message, cmd=93101):
    return {
        "svcid": "game",
        "ver": "1",
        "cmd": cmd,
        "tid": "4",
        "cid": "Hp4sXM",
        "bdy": make_batch_item(message, cmd=cmd),
    }


class FakeRawPublisher:
    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.calls = []

    async def publish(self, channel_id, payload, raw_payload=None):
        self.calls.append((channel_id, payload, raw_payload))
        if self.should_fail:
            raise RuntimeError("kafka unavailable")


class FakeWebSocket:
    def __init__(self, messages):
        self._messages = list(messages)
        self.recv_calls = 0

    async def recv(self):
        self.recv_calls += 1
        if self._messages:
            return self._messages.pop(0)
        raise ConnectionClosedOK(None, None)


def test_handle_batch_message_preserves_source_cmd_and_batch_marker():
    collector = load_collector_module()
    batch_data = {
        "svcid": "game",
        "ver": "1",
        "cmd": 93101,
        "tid": "4",
        "cid": "Hp4sXM",
        "bdy": [
            make_batch_item("hello-1"),
            make_batch_item("hello-2"),
        ],
    }

    records = collector.handle_batch_message(batch_data, "channel-1", "streamer")

    assert len(records) == 2
    assert all(row["source_cmd"] == 93101 for row in records)
    assert all(row["is_batch_item"] is True for row in records)


def test_receive_messages_publishes_single_event_to_kafka_and_counts_stats():
    collector = load_collector_module()
    counter = collector.CmdCounter()
    raw_publisher = FakeRawPublisher()
    message = make_single_message("hello-single")
    websocket = FakeWebSocket([json.dumps(message, ensure_ascii=False)])

    asyncio.run(
        collector.receive_messages(
            websocket,
            "channel-1",
            "streamer",
            counter,
            {"value": 0.0},
            raw_publisher,
        )
    )
    stats = asyncio.run(counter.snapshot())

    assert raw_publisher.calls == [
        ("channel-1", message, json.dumps(message, ensure_ascii=False).encode("utf-8"))
    ]
    assert stats["total_collected_count"] == 1
    assert stats["per_channel"]["channel-1"]["total_collected_count"] == 1
    assert stats["per_channel"]["channel-1"]["cmd_counts"] == {"93101": 1}


def test_receive_messages_publishes_original_batch_frame_to_kafka_and_counts_batch_items():
    collector = load_collector_module()
    counter = collector.CmdCounter()
    raw_publisher = FakeRawPublisher()
    batch_message = {
        "svcid": "game",
        "ver": "1",
        "cmd": 93101,
        "tid": "4",
        "cid": "Hp4sXM",
        "bdy": [
            make_batch_item("hello-1"),
            make_batch_item("hello-2"),
        ],
    }
    websocket = FakeWebSocket([json.dumps(batch_message, ensure_ascii=False)])

    asyncio.run(
        collector.receive_messages(
            websocket,
            "channel-1",
            "streamer",
            counter,
            {"value": 0.0},
            raw_publisher,
        )
    )
    stats = asyncio.run(counter.snapshot())

    assert raw_publisher.calls == [
        ("channel-1", batch_message, json.dumps(batch_message, ensure_ascii=False).encode("utf-8"))
    ]
    assert stats["total_collected_count"] == 2
    assert stats["per_channel"]["channel-1"]["total_collected_count"] == 2
    assert stats["per_channel"]["channel-1"]["cmd_counts"] == {"93101": 2}


def test_receive_messages_stops_when_kafka_publish_fails():
    collector = load_collector_module()
    counter = collector.CmdCounter()
    raw_publisher = FakeRawPublisher(should_fail=True)
    first_message = make_single_message("hello-after-kafka-failure")
    second_message = make_single_message("hello-should-not-run")
    websocket = FakeWebSocket(
        [
            json.dumps(first_message, ensure_ascii=False),
            json.dumps(second_message, ensure_ascii=False),
        ]
    )

    with pytest.raises(RuntimeError, match="kafka unavailable"):
        asyncio.run(
            collector.receive_messages(
                websocket,
                "channel-1",
                "streamer",
                counter,
                {"value": 0.0},
                raw_publisher,
            )
        )

    stats = asyncio.run(counter.snapshot())

    assert raw_publisher.calls == [
        ("channel-1", first_message, json.dumps(first_message, ensure_ascii=False).encode("utf-8"))
    ]
    assert websocket.recv_calls == 1
    assert stats["total_collected_count"] == 0
    assert stats["per_channel"] == {}


@pytest.mark.parametrize("ret_code", [0, 403, None])
def test_connection_ack_records_sqlite_session_without_publishing(tmp_path, ret_code):
    collector = load_collector_module()
    store = collector.ChannelStore(str(tmp_path / "control.db"))
    counter = collector.CmdCounter()
    publisher = FakeRawPublisher()
    last_chat_time = {"value": 0.0}
    ack = {"cmd": 10100, "retCode": ret_code, "bdy": {"sid": "test-session", "auth": "READ"}}
    chat = make_single_message("hello")
    websocket = FakeWebSocket([json.dumps(ack), json.dumps(ack), json.dumps(chat)])

    def on_connected():
        store.start_monitoring_session("channel-1", "streamer")

    receive = collector.receive_messages(
        websocket, "channel-1", "streamer", counter, last_chat_time, publisher,
        on_connected=on_connected,
    )
    if ret_code == 0:
        asyncio.run(receive)
        sessions = store.list_monitoring_sessions()
        assert len(sessions) == 1
        assert sessions[0].channel_id == "channel-1"
        assert sessions[0].started_at is not None
        assert [payload for _, payload, _ in publisher.calls] == [chat]
        assert asyncio.run(counter.snapshot())["total_collected_count"] == 1
    else:
        with pytest.raises(RuntimeError, match="connection rejected"):
            asyncio.run(receive)
        assert store.list_monitoring_sessions() == []
        assert publisher.calls == []
        assert last_chat_time["value"] == 0.0


@pytest.mark.parametrize("outcome", [403, None, "closed", "cancelled"])
def test_connection_cleans_up_workers_before_exit_or_retry(monkeypatch, outcome):
    from collector import runtime

    async def scenario():
        ready = asyncio.Event()
        workers = set()
        stopped = set()
        cleanup_at_close = []
        retries = []
        real_sleep = asyncio.sleep

        async def sleep(delay):
            if delay in (10, 20):
                worker = asyncio.current_task()
                workers.add(worker)
                if len(workers) == 2:
                    ready.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    await real_sleep(0)
                    stopped.add(worker)
            elif delay == 5:
                retries.append(workers == stopped and all(task.done() for task in workers))
                raise asyncio.CancelledError
            else:
                await real_sleep(delay)

        class Socket(FakeWebSocket):
            async def send(self, message):
                pass

            async def recv(self):
                await ready.wait()
                if outcome == "cancelled":
                    await asyncio.Event().wait()
                return await super().recv()

        messages = [] if outcome == "closed" else [json.dumps({"cmd": 10100, "retCode": outcome})]

        @asynccontextmanager
        async def connect(*args, **kwargs):
            try:
                yield Socket(messages)
            finally:
                cleanup_at_close.append(workers == stopped and all(task.done() for task in workers))

        monkeypatch.setattr(runtime.asyncio, "sleep", sleep)
        monkeypatch.setattr(runtime.websockets, "connect", connect)
        monkeypatch.setattr(runtime, "get_access_token", AsyncMock(return_value="token"))
        client = SimpleNamespace(
            timeout_seconds=1,
            fetch=AsyncMock(return_value=SimpleNamespace(chat_channel_id="chat-1")),
        )
        task = asyncio.create_task(runtime.connect_to_chzzk(
            "channel-1", "streamer", runtime.CmdCounter(), client, FakeRawPublisher(),
        ))
        if outcome == "cancelled":
            await asyncio.wait_for(ready.wait(), timeout=1)
            task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
        assert cleanup_at_close == [True]
        assert retries == ([] if outcome == "cancelled" else [True])

    asyncio.run(scenario())


def test_cmd_counter_recent_events_use_buckets_instead_of_per_event_deques():
    collector = load_collector_module()
    counter = collector.CmdCounter(recent_window_seconds=60)
    records = [{"cmd": 93101, "channel_id": "channel-1"} for _ in range(10_000)]

    asyncio.run(counter.record_records(records, observed_at=1000.25))

    assert not hasattr(counter, "global_recent_events")
    assert counter.global_recent_buckets[1000] == 10_000
    assert counter.channel_recent_buckets["channel-1"][1000] == 10_000


def test_cmd_counter_recent_bucket_counts_match_stats_window(monkeypatch):
    collector = load_collector_module()
    counter = collector.CmdCounter(recent_window_seconds=60)

    asyncio.run(counter.record_records([{"cmd": 93101, "channel_id": "channel-1"}], observed_at=1000.0))
    asyncio.run(counter.record_records([{"cmd": 93101, "channel_id": "channel-1"}], observed_at=1070.0))
    monkeypatch.setattr(collector.time, "time", lambda: 1070.0)

    stats = asyncio.run(counter.snapshot())

    assert stats["total_collected_count"] == 2
    assert stats["recent_events_per_minute"] == 1
    assert stats["per_channel"]["channel-1"]["recent_events_per_minute"] == 1
