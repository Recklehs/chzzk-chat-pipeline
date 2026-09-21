import asyncio
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chzzk_control import LiveStatusClient


class DummyResponse:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


def test_live_status_client_normalizes_open_response(monkeypatch):
    def fake_get(url, timeout):
        assert url == "https://example.com/polling/v3.1/channels/channel-open/live-status"
        assert timeout == 7
        return DummyResponse(
            {
                "code": 200,
                "message": "OK",
                "content": {
                    "status": "OPEN",
                    "liveTitle": "stream title",
                    "openDate": "2026-03-13 20:00:00",
                    "closeDate": None,
                    "concurrentUserCount": 321,
                    "chatChannelId": "chat-1",
                },
            }
        )

    monkeypatch.setattr(requests, "get", fake_get)

    client = LiveStatusClient(base_url="https://example.com", timeout_seconds=7)
    snapshot = asyncio.run(client.fetch("channel-open"))

    assert snapshot.channel_id == "channel-open"
    assert snapshot.normalized_status == "OPEN"
    assert snapshot.raw_status == "OPEN"
    assert snapshot.is_live is True
    assert snapshot.live_title == "stream title"
    assert snapshot.open_date == "2026-03-13 20:00:00"
    assert snapshot.viewer_count == 321
    assert snapshot.chat_channel_id == "chat-1"
    assert snapshot.error_message is None


def test_live_status_client_maps_non_open_response_to_not_live(monkeypatch):
    def fake_get(url, timeout):
        return DummyResponse(
            {
                "code": 200,
                "message": "OK",
                "content": {
                    "status": "CLOSE",
                    "liveTitle": None,
                    "openDate": None,
                    "closeDate": "2026-03-13 21:00:00",
                    "concurrentUserCount": 0,
                    "chatChannelId": None,
                },
            }
        )

    monkeypatch.setattr(requests, "get", fake_get)

    client = LiveStatusClient(base_url="https://example.com", timeout_seconds=5)
    snapshot = asyncio.run(client.fetch("channel-closed"))

    assert snapshot.normalized_status == "NOT_LIVE"
    assert snapshot.is_live is False
    assert snapshot.close_date == "2026-03-13 21:00:00"
    assert snapshot.error_message is None


def test_live_status_client_maps_404_to_not_found(monkeypatch):
    def fake_get(url, timeout):
        return DummyResponse({"code": 404, "message": "채널이 존재하지 않습니다.", "content": None})

    monkeypatch.setattr(requests, "get", fake_get)

    client = LiveStatusClient(base_url="https://example.com", timeout_seconds=5)
    snapshot = asyncio.run(client.fetch("missing-channel"))

    assert snapshot.normalized_status == "NOT_FOUND"
    assert snapshot.is_live is False
    assert snapshot.error_message == "채널이 존재하지 않습니다."


def test_live_status_client_maps_request_error_to_error(monkeypatch):
    def fake_get(url, timeout):
        raise requests.Timeout("boom")

    monkeypatch.setattr(requests, "get", fake_get)

    client = LiveStatusClient(base_url="https://example.com", timeout_seconds=5)
    snapshot = asyncio.run(client.fetch("broken-channel"))

    assert snapshot.normalized_status == "ERROR"
    assert snapshot.is_live is False
    assert "boom" in snapshot.error_message


def test_live_status_client_parses_channel_metadata(monkeypatch):
    def fake_get(url, timeout, headers=None):
        assert url == "https://example.com/service/v1/channels/channel-open"
        assert timeout == 7
        assert headers == {"User-Agent": "Mozilla/5.0"}
        return DummyResponse(
            {
                "code": 200,
                "message": None,
                "content": {
                    "channelId": "channel-open",
                    "channelName": "공식 채널명",
                    "channelImageUrl": "https://example.com/image.png",
                    "verifiedMark": True,
                    "openLive": True,
                },
            }
        )

    monkeypatch.setattr(requests, "get", fake_get)

    client = LiveStatusClient(base_url="https://example.com", timeout_seconds=7)
    metadata = asyncio.run(client.fetch_channel_metadata("channel-open"))

    assert metadata.channel_id == "channel-open"
    assert metadata.channel_name == "공식 채널명"
    assert metadata.channel_image_url == "https://example.com/image.png"
    assert metadata.verified_mark is True
    assert metadata.valid is True


def test_live_status_client_rejects_unknown_channel_metadata(monkeypatch):
    def fake_get(url, timeout):
        return DummyResponse(
            {
                "code": 200,
                "message": None,
                "content": {
                    "channelId": None,
                    "channelName": "(알 수 없음)",
                    "channelImageUrl": None,
                    "verifiedMark": False,
                    "openLive": False,
                },
            }
        )

    monkeypatch.setattr(requests, "get", fake_get)

    client = LiveStatusClient(base_url="https://example.com", timeout_seconds=7)
    metadata = asyncio.run(client.fetch_channel_metadata("missing-channel"))

    assert metadata.channel_id == "missing-channel"
    assert metadata.channel_name is None
    assert metadata.valid is False


def test_live_status_client_maps_metadata_request_error_to_invalid(monkeypatch):
    def fake_get(url, timeout, headers=None):
        raise requests.Timeout("metadata timeout")

    monkeypatch.setattr(requests, "get", fake_get)

    client = LiveStatusClient(base_url="https://example.com", timeout_seconds=7)
    metadata = asyncio.run(client.fetch_channel_metadata("broken-channel"))

    assert metadata.channel_id == "broken-channel"
    assert metadata.channel_name is None
    assert metadata.valid is False
    assert "metadata timeout" in metadata.error_message
