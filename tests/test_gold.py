"""Gold session input contracts; Spark aggregation is covered by tests/check_gold.py."""
import sqlite3
from datetime import date, datetime, timedelta, timezone

import pytest

from spark.silver_to_gold import (EPOCH, changed_sessions, load_settings, micros, read_sessions, replace_conditions,
                                  seconds, widen_loose)

NOW = datetime(2026, 9, 23, 5, 0, tzinfo=timezone.utc)


def control_db(path, sessions, channels=()):
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE monitored_channels (channel_id TEXT PRIMARY KEY, channel_name TEXT)')
    db.execute('CREATE TABLE monitoring_sessions (id INTEGER PRIMARY KEY, channel_id TEXT, channel_name TEXT, '
               'started_at TEXT, ended_at TEXT, stop_reason TEXT)')
    db.executemany('INSERT INTO monitored_channels VALUES (?, ?)', channels)
    db.executemany('INSERT INTO monitoring_sessions VALUES (?, ?, ?, ?, ?, ?)', sessions)
    db.commit()
    db.close()
    return str(path)


def test_sessions_use_start_date_in_kst_and_fill_missing_names(tmp_path):
    path = control_db(tmp_path / 'control.db', [
        (1, 'a', None, '2026-09-22T17:09:55+09:00', '2026-09-23T03:40:51+09:00', 'BROADCAST_ENDED'),
        (2, 'b', 'named', '2026-09-23T00:30:00+09:00', None, None),
    ], [('a', 'from channels')])
    first, ongoing = read_sessions(path, NOW)
    assert first['broadcast_date_kst'] == date(2026, 9, 22)
    assert first['started_at'] == datetime(2026, 9, 22, 8, 9, 55, tzinfo=timezone.utc)
    assert first['channel_name'] == 'from channels'
    assert ongoing['broadcast_date_kst'] == date(2026, 9, 23)
    assert ongoing['ended_at'] is None and ongoing['effective_end'] == NOW


def test_overlapping_sessions_of_one_channel_are_rejected(tmp_path):
    path = control_db(tmp_path / 'control.db', [
        (1, 'a', 'a', '2026-09-22T17:00:00+09:00', '2026-09-22T19:00:00+09:00', None),
        (2, 'a', 'a', '2026-09-22T18:00:00+09:00', '2026-09-22T20:00:00+09:00', None),
    ])
    with pytest.raises(ValueError, match='overlap') as error:
        read_sessions(path, NOW)
    assert 'channel a' in str(error.value) and '--full' in str(error.value)


def test_session_ending_before_it_starts_names_the_row_to_fix(tmp_path):
    path = control_db(tmp_path / 'control.db', [
        (7, 'a', 'a', '2026-09-22T19:00:00+09:00', '2026-09-22T18:59:00+09:00', 'BROADCAST_ENDED'),
    ])
    with pytest.raises(ValueError, match='Session 7 of channel a ends before it starts'):
        read_sessions(path, NOW)


def test_back_to_back_sessions_and_missing_db(tmp_path):
    path = control_db(tmp_path / 'control.db', [
        (1, 'a', 'a', '2026-09-22T17:00:00+09:00', '2026-09-22T19:00:30+09:00', 'SERVER_SHUTDOWN'),
        (2, 'a', 'a', '2026-09-22T19:00:30+09:00', '2026-09-22T20:00:00+09:00', None),
        (3, 'b', 'b', '2026-09-22T18:00:00+09:00', '2026-09-22T20:00:00+09:00', None),
    ])
    assert [s['session_id'] for s in read_sessions(path, NOW)] == [1, 2, 3]
    with pytest.raises(FileNotFoundError):
        read_sessions(str(tmp_path / 'missing.db'), NOW)


def test_refresh_targets_ongoing_new_and_changed_sessions_only():
    start, ended = datetime(2026, 9, 22, 17, 0, tzinfo=timezone.utc), datetime(2026, 9, 22, 18, 0, tzinfo=timezone.utc)
    def session(session_id, ended_at, reason='BROADCAST_ENDED', started_at=start, channel='a'):
        return {'session_id': session_id, 'channel_id': channel, 'started_at': started_at, 'ended_at': ended_at,
                'stop_reason': reason, 'channel_name': 'name'}
    sessions = [session(1, ended), session(2, None), session(3, ended), session(4, ended),
                session(5, ended, started_at=start + timedelta(minutes=5)), session(6, ended, channel='b')]
    stored = {i: {'record': ('a', micros(start), micros(ended), 'BROADCAST_ENDED', 'name')} for i in (1, 5, 6)}
    stored[2] = {'record': ('a', micros(start), None, None, 'name')}
    stored[4] = {'record': ('a', micros(start), micros(ended), 'SERVER_SHUTDOWN', 'name')}
    # 1 is unchanged; 2 is live, 3 is not in Gold yet, 4 has a corrected end reason, 5 a corrected start,
    # 6 a corrected channel.
    assert changed_sessions(sessions, stored) == {2, 3, 4, 5, 6}

    # The Collector reopens a closed session when the same broadcast reconnects: Gold stored an end, the record has
    # none now, so the session is rewritten as live again rather than kept as a finished broadcast.
    assert changed_sessions([session(1, None)], stored) == {1}


def test_rewritten_sessions_also_rewrite_session_less_rows_their_boundaries_moved():
    minute = 60_000_000
    def at(m):
        return EPOCH + timedelta(minutes=m)
    def session(session_id, channel, start, end):
        return {'session_id': session_id, 'channel_id': channel, 'started_at': at(start), 'effective_end': at(end)}
    sessions = [session(1, 'a', 10, 20), session(2, 'b', 100, 130), session(3, 'c', 0, 5)]
    # Session 1 used to reach a 25-minute event; its end moved to 20, so minutes up to 25 become session-less.
    stored = {1: {'events': (10 * minute, 25 * minute)}}
    loose = widen_loose({'b': (90 * minute, 95 * minute)}, sessions, {1, 2}, stored)
    assert loose == {'a': (10 * minute, 25 * minute), 'b': (90 * minute, 129 * minute)}
    # Session 3 moved from channel 'old' to 'c': both channels' session-less rows in its span are rewritten.
    stored[3] = {'record': ('old',), 'events': (minute, 3 * minute)}
    assert widen_loose({}, sessions, {3}, stored) == {'c': (0, 4 * minute), 'old': (0, 4 * minute)}


def test_replace_conditions_cover_sessions_and_session_less_minute_ranges():
    minutes, sessions = replace_conditions({2, 1}, {'abc': (60_000_000, 120_000_000)})
    assert sessions == 'session_id IN (1, 2)'
    assert minutes == ("session_id IN (1, 2) OR (session_id IS NULL AND ((channel_id = 'abc' AND minute_start "
                       "BETWEEN timestamp_micros(60000000) AND timestamp_micros(120000000))))")
    assert replace_conditions({3}, {}) == ('session_id IN (3)', 'session_id IN (3)')
    assert replace_conditions(set(), {}) == (None, None)
    with pytest.raises(ValueError, match='channel_id'):
        replace_conditions(set(), {"x' OR true --": (0, 0)})


def test_trigger_intervals_fall_back_per_layer_and_paths_stay_apart(tmp_path):
    assert (seconds('10 seconds'), seconds('1 minute'), seconds(' 5 second ')) == (10, 60, 5)
    with pytest.raises(ValueError, match='interval'):
        seconds('500 milliseconds')
    base = tmp_path / 'data'
    props = tmp_path / 'local.properties'
    props.write_text(f'app.bucket.uri={base}\napp.kafka.bootstrap.servers=localhost:9092\napp.kafka.topic=raw\n'
                     'app.kafka.startingOffsets=earliest\nspark.streaming.trigger.interval=5 seconds\n')
    settings = load_settings(['--properties-file', str(props)])
    assert (settings.gold_path, settings.checkpoint_path) == (str(base / 'gold'), str(base / 'gold_checkpoint'))
    assert settings.silver.runtime.processing_time == settings.processing_time == '5 seconds' and not settings.full
    props.write_text(props.read_text() + 'app.silver.trigger.interval=10 seconds\napp.gold.trigger.interval=15 seconds\n')
    settings = load_settings(['--properties-file', str(props), '--full'])
    assert (settings.silver.runtime.processing_time, settings.processing_time, settings.full) == ('10 seconds', '15 seconds', True)
    props.write_text(props.read_text() + f'app.gold.checkpoint.path={base}/gold/checkpoint\n')
    with pytest.raises(ValueError, match='overlap'):
        load_settings(['--properties-file', str(props)])
