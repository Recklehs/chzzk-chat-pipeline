"""Gold dashboard API over small Delta tables written without Spark."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
import json

import pyarrow as pa
import pytest
from deltalake import write_deltalake
from fastapi.testclient import TestClient

from dashboard.app import create_app, live_channels

UTC = timezone.utc


def at(hour, minute, second=0):
    return datetime(2026, 9, 22, hour, minute, second, tzinfo=UTC)


def gold(tmp_path, computed_at=at(11, 0)):
    sessions = [
        {'session_id': 1, 'channel_id': 'a', 'channel_name': 'one', 'started_at': at(10, 0, 30), 'ended_at': at(10, 15),
         'stop_reason': 'BROADCAST_ENDED', 'broadcast_date_kst': date(2026, 9, 22), 'chat_count': 3,
         'data_status': 'CHECK_GAP', 'max_gap_minutes': 13.5},
        {'session_id': 2, 'channel_id': 'b', 'channel_name': 'two', 'started_at': at(10, 50), 'ended_at': None,
         'stop_reason': None, 'broadcast_date_kst': date(2026, 9, 22), 'chat_count': 0,
         'data_status': 'NO_DATA', 'max_gap_minutes': 10.0},
    ]
    zeros = {'blinded_chat_count': 0, 'unique_chatters': 1, 'donation_count': 0, 'donation_amount_currency': 0,
             'subscription_notification_count': 0, 'subscription_gift_count': 0}
    minutes = [{'channel_id': 'a', 'minute_start': at(10, 0), 'session_id': 1, 'chat_count': 2, **zeros},
               {'channel_id': 'a', 'minute_start': at(10, 14), 'session_id': 1, 'chat_count': 1, **zeros}]
    write_deltalake(str(tmp_path / 'broadcast_sessions'), pa.Table.from_pylist(sessions))
    write_deltalake(str(tmp_path / 'channel_minute_activity'), pa.Table.from_pylist(minutes))
    (tmp_path / '_gold_manifest.json').write_text(json.dumps({'mode': 'stream', 'gap_minutes': 10,
                                                              'computed_at': computed_at.isoformat()}))
    return str(tmp_path)


def client(path, live=lambda url: [{'channel_id': 'a', 'display_name': 'one', 'monitoring_status': 'monitoring',
                                     'recent_events_per_minute': 42}]):
    return TestClient(create_app(gold_path=path, collector_url='http://collector:8000', refresh_seconds=7, live=live))


def test_sessions_come_newest_first_with_manifest_time(tmp_path):
    body = client(gold(tmp_path)).get('/api/sessions').json()
    assert [s['session_id'] for s in body['sessions']] == [2, 1]
    assert body['computed_at'] == '2026-09-22T11:00:00+00:00' and body['mode'] == 'stream'
    assert body['sessions'][1]['started_at'] == '2026-09-22T10:00:30+00:00'
    assert body['sessions'][0]['ended_at'] is None
    # Session 1 spans 14.5 minutes: the 10:00 chats land in slice 0, the 10:14 minute (13.5 minutes in) in slice 22.
    spark = body['sessions'][1]['sparkline']
    assert len(spark) == 24 and spark[0] == 2 and spark[22] == 1 and sum(spark) == 3


def test_session_minutes_are_filled_with_zeros_and_long_silences_become_gaps(tmp_path):
    body = client(gold(tmp_path)).get('/api/sessions/1').json()
    # 10:00:30–10:15:00 covers minutes 10:00 … 10:14; only the first and last have events.
    assert len(body['minutes']) == 15
    assert [p['chat_count'] for p in body['minutes']] == [2] + [0] * 13 + [1]
    assert body['gaps'] == [{'start': '2026-09-22T10:01:00+00:00', 'end': '2026-09-22T10:13:00+00:00', 'minutes': 13}]


def test_ongoing_session_runs_until_the_last_gold_computation(tmp_path):
    body = client(gold(tmp_path)).get('/api/sessions/2').json()
    assert len(body['minutes']) == 10 and not any(p['chat_count'] for p in body['minutes'])
    assert body['gaps'] == [{'start': '2026-09-22T10:50:00+00:00', 'end': '2026-09-22T10:59:00+00:00', 'minutes': 10}]


def test_missing_session_and_missing_gold_are_explained(tmp_path):
    assert client(gold(tmp_path / 'gold')).get('/api/sessions/99').status_code == 404
    response = client(str(tmp_path / 'empty')).get('/api/sessions')
    assert response.status_code == 503 and 'silver_to_gold' in response.json()['detail']


def test_live_rate_comes_from_collector_and_degrades_when_it_is_down(tmp_path):
    up = client(gold(tmp_path)).get('/api/live').json()
    assert up == {'available': True, 'channels': [{'channel_id': 'a', 'display_name': 'one',
                                                   'monitoring_status': 'monitoring', 'recent_events_per_minute': 42}]}
    def down(_url):
        raise OSError('connection refused')
    assert client(str(tmp_path), live=down).get('/api/live').json() == {'available': False, 'channels': []}
    def odd(_url):
        return live_channels(['not', 'a', 'dict'])
    assert client(str(tmp_path), live=odd).get('/api/live').json() == {'available': False, 'channels': []}


def test_collector_payload_shape_is_checked():
    assert live_channels({'channels': [{'channel_id': 'a'}]})[0]['display_name'] == 'a'
    for payload in (['channels'], {'channels': {'a': 1}}, {'channels': ['a']}, {'channels': [{'name': 'no id'}]}):
        with pytest.raises(ValueError, match='Collector'):
            live_channels(payload)


def test_ongoing_sparkline_and_missing_manifest(tmp_path):
    body = client(gold(tmp_path)).get('/api/sessions').json()
    assert body['sessions'][0]['sparkline'] == [0] * 24  # ongoing session 2 has no minute rows yet
    (tmp_path / '_gold_manifest.json').unlink()
    body = client(str(tmp_path)).get('/api/sessions').json()
    assert body['computed_at'] is None and len(body['sessions']) == 2


def test_session_list_is_cached_until_gold_changes(tmp_path):
    path = gold(tmp_path)
    app = client(path)
    first = app.get('/api/sessions').json()['sessions'][1]['sparkline']
    late = {'channel_id': 'a', 'minute_start': at(10, 7), 'session_id': 1, 'chat_count': 5, 'blinded_chat_count': 0,
            'unique_chatters': 1, 'donation_count': 0, 'donation_amount_currency': 0,
            'subscription_notification_count': 0, 'subscription_gift_count': 0}
    write_deltalake(str(tmp_path / 'channel_minute_activity'), pa.Table.from_pylist([late]), mode='append')
    second = app.get('/api/sessions').json()['sessions'][1]['sparkline']
    assert sum(first) == 3 and sum(second) == 8


def test_page_renders_refresh_interval_and_collector_link(tmp_path):
    html = client(gold(tmp_path)).get('/').text
    assert 'const REFRESH_MS = 7 * 1000' in html and 'href="http://collector:8000/dashboard"' in html


def test_concurrent_first_polls_share_one_complete_listing(tmp_path):
    app = client(gold(tmp_path))
    with ThreadPoolExecutor(8) as pool:
        responses = list(pool.map(lambda _: app.get('/api/sessions'), range(16)))
    assert {r.status_code for r in responses} == {200}
    assert len({json.dumps(r.json(), sort_keys=True) for r in responses}) == 1


def test_new_computed_at_alone_reuses_cached_listing(tmp_path, monkeypatch):
    import dashboard.app as dashboard
    calls = []
    real = dashboard.sparklines
    monkeypatch.setattr(dashboard, 'sparklines', lambda *a, **k: calls.append(1) or real(*a, **k))
    path = gold(tmp_path)
    app = client(path)
    app.get('/api/sessions')
    # The Gold timer rewrites the manifest every trigger even when no row changed.
    (tmp_path / '_gold_manifest.json').write_text(json.dumps({'mode': 'stream', 'gap_minutes': 10,
                                                              'computed_at': at(11, 5).isoformat()}))
    body = app.get('/api/sessions').json()
    assert len(calls) == 1 and body['computed_at'] == '2026-09-22T11:05:00+00:00'

