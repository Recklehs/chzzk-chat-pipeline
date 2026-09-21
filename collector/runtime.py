import asyncio
import json
import time
from datetime import datetime
from typing import Callable

import requests
import websockets

from collector.control import CmdCounter, LiveStatusClient
from collector.metrics import metrics


class InactivityTimeoutError(Exception):
    """채팅 비활성 시간이 9분을 초과할 때 발생"""


async def get_access_token(cid: str, timeout_seconds: float):
    """채팅 채널 ID(cid)로 Access Token을 가져옵니다."""

    def _request_token():
        url = f"https://comm-api.game.naver.com/nng_main/v1/chats/access-token?channelId={cid}&chatType=STREAMING"
        headers = {
            "Origin": "https://chzzk.naver.com",
            "Referer": "https://chzzk.naver.com/",
        }
        response = requests.get(url, headers=headers, timeout=timeout_seconds).json()
        if response["code"] == 200:
            return response["content"]["accessToken"]
        print(f"[{cid}] 오류: access-token API 호출 실패 (코드: {response['code']})")
        return None

    try:
        with metrics.time("chzzk_access_token_request_seconds", result="attempt"):
            token = await asyncio.to_thread(_request_token)
        metrics.increment("chzzk_access_token_requests_total", result="success" if token else "empty")
        return token
    except Exception as exc:
        metrics.increment("chzzk_access_token_requests_total", result="failure")
        print(f"[{cid}] 오류: {exc}")
        return None


def handle_single_message(data, channel_id: str, streamer_nickname: str) -> list[dict]:
    """단일 프레임을 통계용 레코드로 확장합니다."""
    if not isinstance(data, dict):
        return []

    cmd = data.get("cmd")
    if cmd is None:
        return []

    return [
        {
            "channel_id": channel_id,
            "streamer_nickname": streamer_nickname,
            "cmd": cmd,
            "source_cmd": cmd,
            "is_batch_item": False,
            "payload": data,
        }
    ]


def handle_batch_message(data, channel_id: str, streamer_nickname: str) -> list[dict]:
    """배치 프레임을 통계용 개별 레코드로 확장합니다."""
    if not isinstance(data, dict):
        return []

    records = []
    source_cmd = data.get("cmd")

    try:
        for item in data.get("bdy", []):
            if not isinstance(item, dict):
                continue
            item_cmd = item.get("cmd", source_cmd)
            if item_cmd is None:
                continue

            full_item_data = data.copy()
            full_item_data["bdy"] = item
            full_item_data["cmd"] = item_cmd

            records.append(
                {
                    "channel_id": channel_id,
                    "streamer_nickname": streamer_nickname,
                    "cmd": item_cmd,
                    "source_cmd": source_cmd,
                    "is_batch_item": True,
                    "payload": full_item_data,
                }
            )
    except Exception as exc:
        print(f"[ERROR] handle_batch_message 오류: {exc}")

    return records


async def format_and_print_stats(counter: CmdCounter):
    counts = await counter.get_counts()
    all_cmds = sorted(counts.keys())
    print("\n" + "=" * 60)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 채팅 통계 (수집된 cmd 개수)")
    if not all_cmds:
        print("  아직 수집된 메시지 없음.")
    else:
        max_len = max(len(str(cmd)) for cmd in all_cmds)
        print(f"  {'CMD 타입'.ljust(max_len)} | 개수")
        print("  " + "-" * (max_len + 10))
        for cmd in all_cmds:
            print(f"  {str(cmd).ljust(max_len)} | {counts.get(cmd, 0)}")
    print("=" * 60)


async def print_stats_periodically(counter: CmdCounter, interval_sec=30):
    while True:
        await asyncio.sleep(interval_sec)
        await format_and_print_stats(counter)


async def send_ping(ws):
    """20초마다 WebSocket PING을 전송합니다."""
    ping_payload = {
        "ver": "3",
        "cmd": 10000,
        "svcid": "game",
        "tid": 2,
    }
    while True:
        try:
            await asyncio.sleep(20)
            await ws.send(json.dumps(ping_payload))
        except websockets.ConnectionClosed:
            print("[INFO] PING 전송 실패. 연결이 끊겼습니다.")
            break
        except Exception as exc:
            print(f"[ERROR] PING 전송 중 오류: {exc}")
            break


async def check_inactivity(last_chat_time: dict, streamer_nickname: str):
    """
    채팅 메시지 비활성 상태를 감지합니다.
    2분, 5분(2+3), 9분(2+3+4) 단계로 경고 및 종료를 수행합니다.
    """
    timeout_levels = {
        1: (120, "2분"),
        2: (300, "5분"),
        3: (540, "9분"),
    }
    current_level = 1

    while True:
        await asyncio.sleep(10)

        now_ts = time.time()
        inactive_duration = now_ts - last_chat_time["value"]

        timeout_sec, timeout_str = timeout_levels[current_level]
        if inactive_duration >= timeout_sec:
            if current_level == 3:
                print(f"\n[WARN] [{streamer_nickname}] {timeout_str} (총 9분) 동안 채팅 메시지 없음. 모니터링을 종료합니다.")
                raise InactivityTimeoutError(f"{timeout_str} inactivity timeout")

            print(f"\n[INFO] [{streamer_nickname}] {timeout_str} 동안 채팅 메시지 없음. (다음 단계: {timeout_levels[current_level + 1][1]})")
            current_level += 1
        elif inactive_duration < 120 and current_level > 1:
            print(f"\n[INFO] [{streamer_nickname}] 채팅 메시지 수신됨. 비활성 타이머가 초기화됩니다.")
            current_level = 1


async def receive_messages(
    websocket,
    channel_id: str,
    streamer_nickname: str,
    counter: CmdCounter,
    last_chat_time: dict,
    raw_publisher=None,
    on_connected: Callable[[], None] | None = None,
):
    """연결 응답은 로컬에서 처리하고, 나머지는 Kafka 발행 후 통계에 반영합니다."""
    if raw_publisher is None:
        raise RuntimeError("Kafka raw publisher is required")

    while True:
        message = ""
        try:
            frame_started_at = time.perf_counter()
            message = await websocket.recv()
            raw_payload = message if isinstance(message, bytes) else message.encode("utf-8")
            backend = getattr(raw_publisher, "metrics_backend", "unknown")
            metrics.increment("chzzk_ws_frames_received_total", backend=backend)
            metrics.increment(
                "chzzk_ws_frame_bytes_received_total",
                amount=len(raw_payload),
                backend=backend,
            )
            with metrics.time("chzzk_ws_json_parse_seconds"):
                data = json.loads(message)
            observed_at = time.time()
            cmd = data.get("cmd") if isinstance(data, dict) else None
            if cmd == 10100:
                if data.get("retCode") == 0:
                    raise RuntimeError(f"CHZZK connection rejected: retCode={data.get('retCode')}")
                if on_connected is not None:
                    on_connected()
                continue

            metrics.increment("chzzk_raw_publish_attempts_total", backend=backend)
            try:
                with metrics.time("chzzk_raw_publish_seconds", backend=backend):
                    await raw_publisher.publish(channel_id, data, raw_payload=raw_payload)
            except Exception:
                metrics.increment("chzzk_raw_publish_failures_total", backend=backend)
                raise
            metrics.increment("chzzk_raw_publish_success_total", backend=backend)
            metrics.observe("chzzk_ws_to_publish_ack_seconds", time.perf_counter() - frame_started_at, backend=backend)

            if cmd not in [100, 10000, 10001]:
                last_chat_time["value"] = observed_at

            if isinstance(data, dict) and isinstance(data.get("bdy"), list):
                records = handle_batch_message(data, channel_id, streamer_nickname)
            else:
                records = handle_single_message(data, channel_id, streamer_nickname)
            await counter.record_records(records, observed_at=observed_at)
            metrics.increment("chzzk_stats_records_total", amount=len(records))
            metrics.observe("chzzk_batch_items_per_frame", len(records))

            if cmd not in [100, 10000, 10001]:
                await counter.note_channel_activity(channel_id, observed_at)

        except websockets.ConnectionClosed:
            print("[INFO] 수신 루프(recv)에서 연결 종료 감지.")
            break
        except json.JSONDecodeError:
            metrics.increment("chzzk_events_dropped_total", reason="json_decode")
            print(f"[ERROR] 메시지 JSON 파싱 실패: {message}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[ERROR] 메시지 처리 중 오류: {exc}")
            raise


async def connect_to_chzzk(
    channel_id,
    streamer_nickname,
    counter,
    live_status_client: LiveStatusClient | None = None,
    raw_publisher=None,
    on_connected: Callable[[], None] | None = None,
):
    """
    단일 채널에 대한 WebSocket 연결 및 자동 재연결을 관리합니다.
    API의 'del' 명령 또는 '비활성 타임아웃'에 의해 종료될 수 있습니다.
    """
    if raw_publisher is None:
        raise RuntimeError("Kafka raw publisher is required")

    timeout_seconds = live_status_client.timeout_seconds if live_status_client else 10

    try:
        while True:
            try:
                print(f"[INFO] [{streamer_nickname}] 채널 정보 조회 중...")
                snapshot = await live_status_client.fetch(channel_id) if live_status_client else None
                cid = snapshot.chat_channel_id if snapshot else None

                if not cid:
                    print(f"[{streamer_nickname}] cid 획득 실패. 30초 후 재시도...")
                    await asyncio.sleep(30)
                    continue

                acc_tkn = await get_access_token(cid, timeout_seconds=timeout_seconds)
                if not acc_tkn:
                    print(f"[{streamer_nickname}] accessToken 획득 실패. 30초 후 재시도...")
                    await asyncio.sleep(30)
                    continue

                last_chat_time = {"value": time.time()}

                print(f"[INFO] [{streamer_nickname}] 채널에 연결 시도 중... (CID: {cid})")

                url = "wss://kr-ss2.chat.naver.com/chat"
                headers = {
                    "Origin": "https://chzzk.naver.com",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ...",
                }

                async with websockets.connect(url, additional_headers=headers) as ws:
                    print(f"[INFO] [{streamer_nickname}] WebSocket 연결 성공")
                    auth_payload = {
                        "ver": "3",
                        "cmd": 100,
                        "svcid": "game",
                        "cid": cid,
                        "bdy": {
                            "uid": None,
                            "devType": 2001,
                            "accTkn": acc_tkn,
                            "auth": "READ",
                        },
                        "tid": 1,
                    }
                    await ws.send(json.dumps(auth_payload))
                    print(f"[{streamer_nickname}] 인증 요청 전송")

                    tasks = [
                        asyncio.create_task(coro)
                        for coro in (
                            receive_messages(
                                ws, channel_id, streamer_nickname, counter, last_chat_time,
                                raw_publisher, on_connected=on_connected,
                            ),
                            send_ping(ws),
                            check_inactivity(last_chat_time, streamer_nickname),
                        )
                    ]
                    try:
                        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                        for task in done:
                            task.result()
                    finally:
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)

            except websockets.ConnectionClosed as exc:
                metrics.increment("chzzk_ws_reconnects_total", reason="connection_closed")
                print(f"[ERROR] [{streamer_nickname}] WebSocket 연결 종료: {exc}")
            except InactivityTimeoutError as exc:
                print(f"[INFO] [{streamer_nickname}] 비활성 시간 초과로 인해 재연결 중단. ({exc})")
                break
            except Exception as exc:
                if isinstance(exc, asyncio.CancelledError):
                    raise
                metrics.increment("chzzk_ws_reconnects_total", reason="error")
                print(f"[ERROR] [{streamer_nickname}] 예기치 않은 오류 발생: {exc}")

            print(f"[{streamer_nickname}] 연결이 끊겼습니다. 5초 후 재접속을 시도합니다...")
            await asyncio.sleep(5)

    except asyncio.CancelledError:
        print(f"[INFO] [{streamer_nickname}] 모니터링이 중지되었습니다.")
        raise
    except Exception as exc:
        print(f"[FATAL] [{streamer_nickname}] 태스크 완전 종료: {exc}")
