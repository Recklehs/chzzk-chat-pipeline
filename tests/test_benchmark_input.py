import json
from datetime import datetime, timezone
from pathlib import Path


def test_resolve_active_delta_parquet_paths_applies_add_and_remove_records(tmp_path: Path):
    from collector.benchmark_input import resolve_active_delta_parquet_paths

    log_dir = tmp_path / "_delta_log"
    log_dir.mkdir()
    (log_dir / "00000000000000000000.json").write_text(
        "\n".join(
            [
                json.dumps({"add": {"path": "event_date=2026-03-25/a.parquet"}}),
                json.dumps({"add": {"path": "event_date=2026-03-25/b.parquet"}}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (log_dir / "00000000000000000001.json").write_text(
        "\n".join(
            [
                json.dumps({"remove": {"path": "event_date=2026-03-25/a.parquet"}}),
                json.dumps({"add": {"path": "event_date=2026-03-26/c.parquet"}}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    assert resolve_active_delta_parquet_paths(tmp_path) == [
        tmp_path / "event_date=2026-03-25/b.parquet",
        tmp_path / "event_date=2026-03-26/c.parquet",
    ]


def test_build_frame_payload_recreates_websocket_batch_payload():
    from collector.benchmark_input import build_frame_payload

    rows = [
        {
            "svcid": "game",
            "ver": "3",
            "cmd": 93101,
            "tid": "1",
            "cid": "chat-channel",
            "body_svcid": "game",
            "body_cid": "chat-channel",
            "mbr_cnt": 10,
            "uid": "u1",
            "profile_json": "{}",
            "msg": "hello",
            "msg_type_code": 1,
            "msg_status_type": "NORMAL",
            "extras_json": "{\"streamingChannelId\":\"streaming-channel\"}",
            "ctime": 1,
            "utime": 2,
            "msg_tid": "m1",
            "cuid": "c1",
            "msg_time": 3,
        },
        {
            "svcid": "game",
            "ver": "3",
            "cmd": 93101,
            "tid": "1",
            "cid": "chat-channel",
            "body_svcid": "game",
            "body_cid": "chat-channel",
            "mbr_cnt": 11,
            "uid": "u2",
            "profile_json": "{}",
            "msg": "world",
            "msg_type_code": 1,
            "msg_status_type": "NORMAL",
            "extras_json": "{\"streamingChannelId\":\"streaming-channel\"}",
            "ctime": 4,
            "utime": 5,
            "msg_tid": "m2",
            "cuid": "c2",
            "msg_time": 6,
        },
    ]

    payload = build_frame_payload(rows)

    assert payload["cmd"] == 93101
    assert payload["cid"] == "chat-channel"
    assert [item["msg"] for item in payload["bdy"]] == ["hello", "world"]


def test_build_replay_record_uses_streaming_channel_id_and_received_at():
    from collector.benchmark_input import build_replay_record

    timestamp = datetime(2026, 4, 23, 12, 30, tzinfo=timezone.utc)
    rows = [
        {
            "partition": 0,
            "offset": 10,
            "kafka_timestamp": timestamp,
            "svcid": "game",
            "ver": "3",
            "cmd": 93101,
            "tid": "1",
            "cid": "chat-channel",
            "body_svcid": "game",
            "body_cid": "chat-channel",
            "mbr_cnt": 10,
            "uid": "u1",
            "profile_json": "{}",
            "msg": "hello",
            "msg_type_code": 1,
            "msg_status_type": "NORMAL",
            "extras_json": "{\"streamingChannelId\":\"streaming-channel\"}",
            "ctime": 1,
            "utime": 2,
            "msg_tid": "m1",
            "cuid": "c1",
            "msg_time": 3,
        }
    ]

    record = build_replay_record(rows, scenario="steady-chat")

    assert record["channel_id"] == "streaming-channel"
    assert record["received_at"] == "2026-04-23T12:30:00+00:00"
    assert record["scenario"] == "steady-chat"
    assert record["message"]["bdy"][0]["msg"] == "hello"
