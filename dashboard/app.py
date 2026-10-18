"""Read-only Gold dashboard: broadcast sessions, per-minute activity, and the Collector's live rate.

Runs apart from the Collector so viewing analytics never starts Kafka or collection:
    uvicorn dashboard.app:app --port 8001
"""
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
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


def read_table(gold_path, name, where=None):
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError
    try:
        dataset = DeltaTable(f'{gold_path}/{name}').to_pyarrow_dataset()
    except TableNotFoundError as exc:
        raise HTTPException(503, 'Gold 테이블이 아직 없습니다. spark/silver_to_gold.py를 먼저 실행하세요.') from exc
    return dataset.to_table(filter=where).to_pylist()


def read_manifest(gold_path):
    try:
        return json.loads((Path(gold_path) / '_gold_manifest.json').read_text())
    except FileNotFoundError:
        return {}


def minute_series(session, rows, computed_at):
    """One point per minute of the session, zeros where Gold has no row, so charts show silence as silence."""
    end = session['ended_at'] or computed_at
    first = session['started_at'].replace(second=0, microsecond=0)
    last = (end - timedelta(microseconds=1)).replace(second=0, microsecond=0)
    by_minute = {r['minute_start']: r for r in rows}
    points, minute = [], first
    while minute <= last:
        row = by_minute.get(minute, {})
        points.append({'minute_start': minute.isoformat(), **{k: row.get(k) or 0 for k in POINT_FIELDS}})
        minute += MINUTE
    return points


def sparkline(session, rows, computed_at, buckets=24):
    """Chat counts folded into equal slices of the session, for the small trend line in the session list."""
    start, end = session['started_at'], session['ended_at'] or computed_at
    span = max((end - start).total_seconds(), 1)
    values = [0] * buckets
    for row in rows:
        offset = (row['minute_start'] - start).total_seconds()
        values[min(buckets - 1, max(0, int(offset / span * buckets)))] += row.get('chat_count') or 0
    return values


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


def fetch_live(collector_url, timeout=1.0):
    with urllib.request.urlopen(f'{collector_url.rstrip("/")}/channels', timeout=timeout) as response:
        payload = json.load(response)
    return [{'channel_id': c['channel_id'], 'display_name': c.get('display_name') or c['channel_id'],
             'monitoring_status': c.get('monitoring_status'), 'recent_events_per_minute': c.get('recent_events_per_minute') or 0}
            for c in payload.get('channels', [])]


def create_app(gold_path=None, collector_url=None, refresh_seconds=None, live=fetch_live):
    gold_path = gold_path or os.environ.get('GOLD_PATH', 'data/output/local/frames/gold')
    collector_url = collector_url or os.environ.get('COLLECTOR_URL', 'http://127.0.0.1:8000')
    refresh_seconds = refresh_seconds or int(os.environ.get('GOLD_DASHBOARD_REFRESH_SECONDS', '10'))
    app = FastAPI(title='CHZZK Broadcast Analytics')

    def computed_at(manifest):
        value = manifest.get('computed_at')
        return datetime.fromisoformat(value) if value else datetime.now(timezone.utc)

    @app.get('/')
    def page(request: Request):
        return TEMPLATES.TemplateResponse(request, 'gold.html', {'refresh_seconds': refresh_seconds,
                                                                 'collector_url': collector_url})

    @app.get('/api/sessions')
    def sessions():
        manifest = read_manifest(gold_path)
        rows = sorted(read_table(gold_path, 'broadcast_sessions'), key=lambda r: r['started_at'], reverse=True)
        by_session = {}
        for minute in read_table(gold_path, 'channel_minute_activity'):
            by_session.setdefault(minute['session_id'], []).append(minute)
        end = computed_at(manifest)
        return {'computed_at': manifest.get('computed_at'), 'mode': manifest.get('mode'),
                'gap_minutes': manifest.get('gap_minutes'),
                'sessions': [{**jsonable(r), 'sparkline': sparkline(r, by_session.get(r['session_id'], []), end)}
                             for r in rows]}

    @app.get('/api/sessions/{session_id}')
    def session_detail(session_id: int):
        import pyarrow.compute as pc
        found = read_table(gold_path, 'broadcast_sessions', pc.field('session_id') == session_id)
        if not found:
            raise HTTPException(404, f'Session {session_id} is not in Gold')
        manifest = read_manifest(gold_path)
        rows = read_table(gold_path, 'channel_minute_activity', pc.field('session_id') == session_id)
        points = minute_series(found[0], rows, computed_at(manifest))
        gap_minutes = manifest.get('gap_minutes') or 10
        return {'session': jsonable(found[0]), 'minutes': points, 'gap_minutes': gap_minutes,
                'gaps': empty_runs(points, gap_minutes)}

    @app.get('/api/live')
    def live_rate():
        # The Collector's in-memory stats are the fast path; Gold stays the source of exact numbers.
        try:
            return {'available': True, 'channels': live(collector_url)}
        except (OSError, ValueError, KeyError):
            return {'available': False, 'channels': []}

    return app


app = create_app()
