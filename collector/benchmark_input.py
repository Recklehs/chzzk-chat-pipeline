import argparse
import json
import math
from collections import defaultdict
from datetime import timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


PARQUET_COLUMNS = [
    "partition",
    "offset",
    "kafka_timestamp",
    "svcid",
    "ver",
    "cmd",
    "tid",
    "cid",
    "body_svcid",
    "body_cid",
    "mbr_cnt",
    "uid",
    "profile_json",
    "msg",
    "msg_type_code",
    "msg_status_type",
    "extras_json",
    "ctime",
    "utime",
    "msg_tid",
    "cuid",
    "msg_time",
]


def resolve_active_delta_parquet_paths(snapshot_root: str | Path) -> list[Path]:
    root = Path(snapshot_root)
    log_dir = root / "_delta_log"
    active_paths: set[str] = set()

    for log_path in sorted(log_dir.glob("*.json")):
        with log_path.open(encoding="utf-8") as handle:
            for line in handle:
                record = json.loads(line)
                if "add" in record:
                    active_paths.add(record["add"]["path"])
                elif "remove" in record:
                    active_paths.discard(record["remove"]["path"])

    return sorted(root / relative_path for relative_path in active_paths if relative_path.endswith(".parquet"))


def _is_null(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _maybe_int(value: Any) -> int | None:
    if _is_null(value):
        return None
    return int(value)


def _maybe_value(value: Any) -> Any:
    if _is_null(value):
        return None
    return value


def build_frame_payload(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise ValueError("at least one exploded row is required")

    first = rows[0]
    bodies = []
    for row in rows:
        bodies.append(
            {
                "svcid": row["body_svcid"],
                "cid": row["body_cid"],
                "mbrCnt": _maybe_int(row["mbr_cnt"]),
                "uid": row["uid"],
                "profile": row["profile_json"],
                "msg": row["msg"],
                "msgTypeCode": _maybe_int(row["msg_type_code"]),
                "msgStatusType": row["msg_status_type"],
                "extras": row["extras_json"],
                "ctime": _maybe_int(row["ctime"]),
                "utime": _maybe_int(row["utime"]),
                "msgTid": _maybe_value(row["msg_tid"]),
                "cuid": _maybe_value(row["cuid"]),
                "msgTime": _maybe_int(row["msg_time"]),
            }
        )

    return {
        "svcid": first["svcid"],
        "ver": first["ver"],
        "cmd": int(first["cmd"]),
        "tid": first["tid"],
        "cid": first["cid"],
        "bdy": bodies,
    }


def _parse_streaming_channel_id(extras_json: str | None, fallback: str | None) -> str:
    if extras_json:
        try:
            payload = json.loads(extras_json)
        except json.JSONDecodeError:
            payload = {}
        if isinstance(payload, dict):
            streaming_channel_id = payload.get("streamingChannelId")
            if isinstance(streaming_channel_id, str) and streaming_channel_id.strip():
                return streaming_channel_id
    if fallback is None:
        raise ValueError("streaming channel id and fallback are both missing")
    return fallback


def _format_received_at(value: Any) -> str:
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if getattr(value, "tzinfo", None) is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def build_replay_record(rows: Sequence[Mapping[str, Any]], *, scenario: str) -> dict[str, Any]:
    if not rows:
        raise ValueError("at least one exploded row is required")

    first = rows[0]
    return {
        "channel_id": _parse_streaming_channel_id(first.get("extras_json"), first.get("cid")),
        "received_at": _format_received_at(first["kafka_timestamp"]),
        "scenario": scenario,
        "message": build_frame_payload(rows),
    }


def iter_replay_records(
    snapshot_root: str | Path,
    *,
    scenario: str,
    limit: int | None = None,
) -> Iterable[dict[str, Any]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - depends on local developer environment
        raise RuntimeError("pyarrow is required to build replay input from Delta parquet snapshots.") from exc

    frame_rows_by_key: dict[tuple[str, int, int], list[dict[str, Any]]] = defaultdict(list)
    for path in resolve_active_delta_parquet_paths(snapshot_root):
        table = pq.read_table(path, columns=PARQUET_COLUMNS)
        for row in table.to_pylist():
            frame_key = (str(path), int(row["partition"]), int(row["offset"]))
            frame_rows_by_key[frame_key].append(row)

    emitted = 0
    for _, rows in sorted(frame_rows_by_key.items(), key=lambda item: item[0]):
        if limit is not None and emitted >= limit:
            break
        yield build_replay_record(rows, scenario=scenario)
        emitted += 1


def write_replay_input_from_delta(
    snapshot_root: str | Path,
    output_path: str | Path,
    *,
    scenario: str,
    limit: int | None = None,
) -> int:
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with destination.open("w", encoding="utf-8") as handle:
        for record in iter_replay_records(snapshot_root, scenario=scenario, limit=limit):
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            count += 1
    return count


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Build benchmark replay NDJSON from a Delta parquet snapshot.")
    parser.add_argument("--snapshot-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--limit", type=int)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    count = write_replay_input_from_delta(
        args.snapshot_root,
        args.output,
        scenario=args.scenario,
        limit=args.limit,
    )
    print(json.dumps({"output": str(args.output), "records": count}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
