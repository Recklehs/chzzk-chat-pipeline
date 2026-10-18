"""Read-only Gold dashboard: broadcast sessions, per-minute activity, and the Collector's live rate.

Runs apart from the Collector so viewing analytics never starts Kafka or collection:
    uvicorn dashboard.app:app --port 8001
"""
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import threading
import urllib.request

from fastapi import FastAPI, HTTPException, Request
from fastapi.templating import Jinja2Templates

TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / 'templates'))
MINUTE = timedelta(minutes=1)
POINT_FIELDS = ('chat_count', 'blinded_chat_count', 'unique_chatters', 'donation_count', 'donation_amount_currency',
                'subscription_notification_count', 'subscription_gift_count')
EVENT_FIELDS = ('chat_count', 'donation_count', 'subscription_notification_count', 'subscription_gift_count')


def jsonable(row):
    return {k: v.isoformat() if isinstance(v, (datetime, date)) else v for k, v in row.items()}


def open_table(gold_path, name):
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError
    try:
        return DeltaTable(f'{gold_path}/{name}')
    except TableNotFoundError as exc:
        raise HTTPException(503, 'Gold 테이블이 아직 없습니다. spark/silver_to_gold.py를 먼저 실행하세요.') from exc


def read_table(gold_path, name, where=None):
    return open_table(gold_path, name).to_pyarrow_dataset().to_table(filter=where).to_pylist()


def read_manifest(gold_path):
    try:
        return json.loads((Path(gold_path) / '_gold_manifest.json').read_text())
    except FileNotFoundError:
        return {}


def session_end(session):
    """End of the span Gold measured for this row: ended_at, or for a live session the duration Gold wrote with it.

    Taking it from the same table version, not the manifest written after the tables, keeps rows and span in step.
    """
    return session['ended_at'] or session['started_at'] + timedelta(minutes=session['duration_minutes'] or 0)


def minute_series(session, rows):
    """One point per minute of the session, zeros where Gold has no row, so charts show silence as silence."""
    end = session_end(session)
    first = session['started_at'].replace(second=0, microsecond=0)
    last = (end - timedelta(microseconds=1)).replace(second=0, microsecond=0)
    by_minute = {r['minute_start']: r for r in rows}
    points, minute = [], first
    while minute <= last:
        row = by_minute.get(minute, {})
        points.append({'minute_start': minute.isoformat(), **{k: row.get(k) or 0 for k in POINT_FIELDS}})
        minute += MINUTE
    return points


def sparklines(minutes, sessions, buckets=24):
    """Chat counts folded into equal slices of each session, for the small trend lines in the session list.

    Reads three columns and aggregates in Arrow, so the Python work is per session, not per minute row.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    result = {s['session_id']: [0] * buckets for s in sessions}
    if not sessions:
        return result
    ids = pa.array([s['session_id'] for s in sessions], pa.int64())
    starts = pa.array([micros(s['started_at']) for s in sessions], pa.int64())
    spans = pa.array([max(micros(session_end(s)) - micros(s['started_at']), 1) for s in sessions], pa.float64())
    table = minutes.to_pyarrow_dataset().to_table(columns=['session_id', 'minute_start', 'chat_count'])
    position = pc.index_in(table['session_id'], value_set=ids)
    known = pc.is_valid(position)
    table, position = table.filter(known), position.filter(known)
    # Delta stores timestamps in microseconds, the same unit as `starts`.
    offset = pc.subtract(pc.cast(table['minute_start'], pa.int64()), pc.take(starts, position))
    bucket = pc.cast(pc.floor(pc.multiply(pc.divide(pc.cast(offset, pa.float64()), pc.take(spans, position)), buckets)), pa.int64())
    bucket = pc.min_element_wise(pc.max_element_wise(bucket, 0), buckets - 1)
    grouped = (pa.table({'session_id': table['session_id'], 'bucket': bucket, 'chats': pc.fill_null(table['chat_count'], 0)})
               .group_by(['session_id', 'bucket']).aggregate([('chats', 'sum')]))
    for session_id, slot, chats in zip(*(grouped[c].to_pylist() for c in ('session_id', 'bucket', 'chats_sum'))):
        result[session_id][slot] = chats
    return result


def micros(value):
    return (value - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)


def empty_runs(points, threshold):
    """Runs of at least `threshold` minutes without any event, for shading collection gaps on the chart."""
    runs, start = [], None
    for i, point in enumerate([*points, None]):
        empty = point is not None and not any(point[k] for k in EVENT_FIELDS)
        if empty and start is None:
            start = i
        elif not empty and start is not None:
            if i - start >= threshold:
                runs.append({'start': points[start]['minute_start'], 'end': points[i - 1]['minute_start'], 'minutes': i - start})
            start = None
    return runs


def profile_image(value):
    # The URL comes from the CHZZK API through the Collector and lands in an <img src>; only plain https passes.
    return value if isinstance(value, str) and value.startswith('https://') else None


def live_channels(payload):
    """Collector /channels payload -> live rate rows; anything else is a ValueError the endpoint reports as offline."""
    channels = payload.get('channels') if isinstance(payload, dict) else None
    if not isinstance(channels, list) or not all(isinstance(c, dict) and c.get('channel_id') for c in channels):
        raise ValueError('Unexpected Collector /channels response')
    return [{'channel_id': c['channel_id'], 'display_name': c.get('display_name') or c['channel_id'],
             'channel_image_url': profile_image(c.get('channel_image_url')),
             'monitoring_status': c.get('monitoring_status'), 'recent_events_per_minute': c.get('recent_events_per_minute') or 0}
            for c in channels]


def fetch_live(collector_url, timeout=1.0):
    with urllib.request.urlopen(f'{collector_url.rstrip("/")}/channels', timeout=timeout) as response:
        return live_channels(json.load(response))


def create_app(gold_path=None, collector_url=None, refresh_seconds=None, live=fetch_live, collector_link=None):
    gold_path = gold_path or os.environ.get('GOLD_PATH', 'data/output/local/frames/gold')
    collector_url = collector_url or os.environ.get('COLLECTOR_URL', 'http://127.0.0.1:8000')
    # The server calls COLLECTOR_URL; the browser follows COLLECTOR_PUBLIC_URL, which differs behind a proxy or container.
    collector_link = collector_link or os.environ.get('COLLECTOR_PUBLIC_URL') or collector_url
    refresh_seconds = refresh_seconds or int(os.environ.get('GOLD_DASHBOARD_REFRESH_SECONDS', '10'))
    app = FastAPI(title='CHZZK Broadcast Analytics')
    listing = {}  # the last /api/sessions body and the Gold versions it was built from, shared by every open tab
    listing_lock = threading.Lock()  # sync endpoints run in a thread pool; one rebuild at a time keeps the cache whole

    @app.get('/')
    def page(request: Request):
        return TEMPLATES.TemplateResponse(request, 'gold.html', {'refresh_seconds': refresh_seconds,
                                                                 'collector_url': collector_link})

    @app.get('/api/sessions')
    def sessions():
        # Versions are read under the lock, so a slow request for an older Gold version cannot overwrite a newer cache.
        with listing_lock:
            manifest = read_manifest(gold_path)
            tables = open_table(gold_path, 'broadcast_sessions'), open_table(gold_path, 'channel_minute_activity')
            # Only table versions key the cache: the Gold timer rewrites computed_at every trigger even when no row
            # changed, and every span comes from the session rows themselves. A live session's rows are rewritten each
            # trigger, so its trend still follows. The manifest only supplies the time shown on screen.
            key = (tables[0].version(), tables[1].version())
            if listing.get('key') != key:
                rows = sorted(tables[0].to_pyarrow_dataset().to_table().to_pylist(), key=lambda r: r['started_at'], reverse=True)
                trends = sparklines(tables[1], rows)
                listing.update(key=key, sessions=[{**jsonable(r), 'sparkline': trends[r['session_id']]} for r in rows])
            return {'computed_at': manifest.get('computed_at'), 'mode': manifest.get('mode'),
                    'gap_minutes': manifest.get('gap_minutes'), 'sessions': listing['sessions']}

    @app.get('/api/sessions/{session_id}')
    def session_detail(session_id: int):
        import pyarrow.compute as pc
        found = read_table(gold_path, 'broadcast_sessions', pc.field('session_id') == session_id)
        if not found:
            raise HTTPException(404, f'Session {session_id} is not in Gold')
        manifest = read_manifest(gold_path)
        rows = read_table(gold_path, 'channel_minute_activity', pc.field('session_id') == session_id)
        points = minute_series(found[0], rows)
        gap_minutes = manifest.get('gap_minutes') or 10
        return {'session': jsonable(found[0]), 'minutes': points, 'gap_minutes': gap_minutes,
                'gaps': empty_runs(points, gap_minutes)}

    @app.get('/api/live')
    def live_rate():
        # The Collector's in-memory stats are the fast path; Gold stays the source of exact numbers.
        try:
            return {'available': True, 'channels': live(collector_url)}
        except (OSError, ValueError):
            return {'available': False, 'channels': []}

    return app


app = create_app()
