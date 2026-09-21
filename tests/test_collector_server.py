import asyncio
import importlib
import json
import os
import sys

import pytest
from websockets.exceptions import ConnectionClosedOK


def load_collector_module():
    os.environ["API_KEY"] = "test-key"
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
