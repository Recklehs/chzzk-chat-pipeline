"""Silver Delta + Collector sessions -> two Gold Delta tables.

Default: a stream that recomputes only the sessions touched since the last trigger.
--full: recompute everything from a pinned Silver snapshot (first build, reconciliation).
"""
import argparse
from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from functools import reduce
import json
from pathlib import Path
import re
import signal
import sqlite3
import sys
import threading
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from spark.config import parse_properties_file

KST = ZoneInfo('Asia/Seoul')
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MINUTE_US = 60_000_000
GAP_MINUTES = 10  # ponytail: fixed warning threshold, tune per channel if quiet streams get flagged
SOURCES = ('chat_messages', 'donations', 'subscription_notifications', 'subscription_gifts')
COUNTS = ('chat_count', 'blinded_chat_count', 'donation_count', 'anonymous_donation_count',
          'donation_amount_currency', 'other_pay_type_donation_count',
          'subscription_notification_count', 'subscription_gift_count')
SESSION_SCHEMA = ('session_id long, channel_id string, channel_name string, started_at timestamp, '
                  'ended_at timestamp, effective_end timestamp, stop_reason string, broadcast_date_kst date')
CHANNEL_ID = re.compile(r'[0-9A-Za-z_-]+')


@dataclass(frozen=True)
class GoldSettings:
    silver: object  # spark.bronze_to_silver.SilverSettings
    gold_path: str
    checkpoint_path: str
    control_db: str
    processing_time: str
    full: bool = False


def load_settings(argv=None):
    from spark.bronze_to_silver import load_settings as load_silver_settings
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--properties-file', required=True)
    parser.add_argument('--full', action='store_true', help='Recompute every session from the current Silver snapshot and exit')
    args = parser.parse_args(argv)
    silver = load_silver_settings(['--properties-file', args.properties_file])
    props = parse_properties_file(args.properties_file)
    base = Path(silver.bronze_path).parent
    gold = Path(props.get('app.gold.path', str(base / 'gold'))).expanduser().resolve()
    checkpoint = Path(props.get('app.gold.checkpoint.path', str(base / 'gold_checkpoint'))).expanduser().resolve()
    control_db = Path(props.get('app.control.db.path', 'data/control.db')).expanduser().resolve()
    paths = [Path(p).expanduser().resolve() for p in (silver.bronze_path, silver.runtime.checkpoint_path,
                                                      silver.output_path, silver.checkpoint_path)] + [gold, checkpoint]
    for i, path in enumerate(paths):
        if any(path == other or path in other.parents or other in path.parents for other in paths[i+1:]):
            raise ValueError('Bronze/Silver/Gold paths and checkpoints must not overlap')
    interval = props.get('app.gold.trigger.interval', silver.runtime.processing_time)
    seconds(interval)  # fail fast on an interval the timer cannot follow
    return GoldSettings(silver, str(gold), str(checkpoint), str(control_db), interval, args.full)


def seconds(interval):
    match = re.fullmatch(r'\s*(\d+)\s*(second|minute)s?\s*', interval)
    if not match:
        raise ValueError(f'Unsupported trigger interval {interval!r}; use "N seconds" or "N minutes"')
    return int(match[1]) * (60 if match[2] == 'minute' else 1)


def micros(value):
    return None if value is None else (value - EPOCH) // timedelta(microseconds=1)


def read_sessions(db_path, now):
    """Collector monitoring sessions as UTC instants; ongoing sessions end at `now`."""
    if not Path(db_path).exists():
        raise FileNotFoundError(f'Collector control DB not found: {db_path}')
    with closing(sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)) as db:
        rows = db.execute(
            'SELECT s.id, s.channel_id, COALESCE(s.channel_name, c.channel_name), s.started_at, s.ended_at, s.stop_reason '
            'FROM monitoring_sessions s LEFT JOIN monitored_channels c ON c.channel_id = s.channel_id '
            'ORDER BY s.channel_id, s.started_at').fetchall()
    sessions = []
    for session_id, channel_id, name, started, ended, reason in rows:
        started_at = datetime.fromisoformat(started).astimezone(timezone.utc)
        ended_at = datetime.fromisoformat(ended).astimezone(timezone.utc) if ended else None
        effective_end = ended_at or now
        if effective_end < started_at:
            raise ValueError(f'Session {session_id} ends before it starts')
        previous = sessions[-1] if sessions else None
        if previous and previous['channel_id'] == channel_id and previous['effective_end'] > started_at:
            raise ValueError(f'Sessions {previous["session_id"]} and {session_id} overlap; an event would count twice')
        sessions.append({'session_id': session_id, 'channel_id': channel_id, 'channel_name': name,
                         'started_at': started_at, 'ended_at': ended_at, 'effective_end': effective_end,
                         'stop_reason': reason, 'broadcast_date_kst': started_at.astimezone(KST).date()})
    return sessions


def record(session):
    """The Collector fields a Gold session row was computed from."""
    return micros(session['started_at']), micros(session['ended_at']), session['stop_reason'], session['channel_name']


def changed_sessions(sessions, stored):
    """Sessions Gold must refresh without new events: ongoing, new, or with a changed Collector record."""
    return {s['session_id'] for s in sessions
            if s['ended_at'] is None or stored.get(s['session_id'], {}).get('record') != record(s)}


def widen_loose(loose, sessions, affected, stored):
    """Add each rewritten session's span to the session-less minute ranges that get rewritten too.

    A changed start or end moves events between a session and no session. The span is stretched to the
    session's previously stored first/last event so both sides are rewritten and every event is counted once.
    """
    for s in sessions:
        if s['session_id'] not in affected:
            continue
        first, last = stored.get(s['session_id'], {}).get('events', (None, None))
        low = min(v for v in (micros(s['started_at']), first) if v is not None)
        high = max(v for v in (micros(s['effective_end']) - 1, last, low) if v is not None)
        low, high = low - low % MINUTE_US, high - high % MINUTE_US
        old = loose.get(s['channel_id'])
        loose[s['channel_id']] = (min(low, old[0]), max(high, old[1])) if old else (low, high)
    return loose


def replace_conditions(session_ids, loose):
    """Delta replaceWhere predicates for the rows one incremental refresh rewrites.

    `loose` maps channel -> (first, last) minute in epoch micros; every session-less row in that range is rewritten.
    """
    for channel in loose:
        if not CHANNEL_ID.fullmatch(channel):
            raise ValueError(f'Unexpected channel_id {channel!r}')
    ids = ', '.join(str(int(i)) for i in sorted(session_ids))
    sessions = f'session_id IN ({ids})' if ids else None
    loose = ' OR '.join(f"(channel_id = '{c}' AND minute_start BETWEEN timestamp_micros({int(lo)}) AND timestamp_micros({int(hi)}))"
                        for c, (lo, hi) in sorted(loose.items()))
    parts = [sessions] if sessions else []
    if loose:
        parts.append(f'(session_id IS NULL AND ({loose}))')
    return ' OR '.join(parts) or None, sessions


def assign_sessions(spark, events, sessions):
    """Left-join events to the session whose [started_at, effective_end) holds their event_time."""
    from pyspark.sql import functions as F
    windows = F.broadcast(spark.createDataFrame(sessions, SESSION_SCHEMA).select(
        F.col('session_id'), F.col('channel_id').alias('s_channel'), 'broadcast_date_kst', 'started_at', 'effective_end'))
    joined = events.join(windows, (events.channel_id == F.col('s_channel'))
                         & (events.event_time >= F.col('started_at')) & (events.event_time < F.col('effective_end')), 'left')
    return joined.drop('s_channel', 'effective_end')


def tagged_events(spark, silver_path, sessions, versions=None, windows=None):
    """One row per Silver event with metric flags and its session.

    `windows` [(channel_id, start, end)] limits the events read; a join keeps it one small plan for many windows.
    """
    from pyspark.sql import functions as F
    zero = F.lit(0).cast('long')
    extra = {
        'chat_messages': [F.lit(1).alias('chat_count'),
                          (F.col('message_status') == 'CBOTBLIND').cast('long').alias('blinded_chat_count'),
                          F.col('actor_user_id').alias('chatter')],
        'donations': [F.lit(1).alias('donation_count'), F.col('is_anonymous').cast('long').alias('anonymous_donation_count'),
                      F.when(F.col('pay_type') == 'CURRENCY', F.col('pay_amount')).otherwise(zero).alias('donation_amount_currency'),
                      (F.col('pay_type') != 'CURRENCY').cast('long').alias('other_pay_type_donation_count')],
        'subscription_notifications': [F.lit(1).alias('subscription_notification_count')],
        'subscription_gifts': [F.lit(1).alias('subscription_gift_count')],
    }
    events = None
    for table in SOURCES:
        reader = spark.read.format('delta')
        if versions:
            reader = reader.option('versionAsOf', versions[table])
        df = reader.load(f'{silver_path}/{table}')
        if windows:
            # One overall time range lets Delta skip files; the join below picks the exact windows.
            df = df.filter((F.col('event_time') >= F.lit(min(w[1] for w in windows)))
                           & (F.col('event_time') < F.lit(max(w[2] for w in windows))))
        df = df.select('channel_id', 'event_time', *extra[table])
        events = df if events is None else events.unionByName(df, allowMissingColumns=True)
    if windows:
        scope = F.broadcast(spark.createDataFrame(windows, 'w_channel string, w_start timestamp, w_end timestamp'))
        events = events.join(scope, (events.channel_id == scope.w_channel) & (events.event_time >= scope.w_start)
                             & (events.event_time < scope.w_end), 'left_semi')
    return assign_sessions(spark, events.fillna(0, subset=list(COUNTS)), sessions)


def minute_activity(events):
    from pyspark.sql import functions as F
    return (events.withColumn('minute_start', F.date_trunc('minute', 'event_time'))
            .groupBy('channel_id', 'minute_start', 'session_id', 'broadcast_date_kst')
            .agg(*[F.sum(c).cast('long').alias(c) for c in COUNTS], F.countDistinct('chatter').alias('unique_chatters')))


def broadcast_sessions(spark, events, minutes, sessions):
    from pyspark.sql import Window, functions as F
    seconds = lambda c: F.unix_micros(c) / 1e6
    in_session = events.filter(F.col('session_id').isNotNull())
    # Gap before each event, counting from the session start for the first one.
    previous = F.coalesce(F.lag('event_time').over(Window.partitionBy('session_id').orderBy('event_time')), F.col('started_at'))
    totals = (in_session.withColumn('gap_minutes', (seconds('event_time') - seconds(previous)) / 60)
              .groupBy('session_id')
              .agg(*[F.sum(c).cast('long').alias(c) for c in COUNTS], F.countDistinct('chatter').alias('unique_chatters'),
                   F.min('event_time').alias('first_event_at'), F.max('event_time').alias('last_event_at'),
                   F.max('gap_minutes').alias('max_inner_gap_minutes')))
    ranked = Window.partitionBy('session_id').orderBy(F.desc('chat_count'), 'minute_start')
    peaks = (minutes.filter(F.col('session_id').isNotNull() & (F.col('chat_count') > 0))
             .withColumn('rank', F.row_number().over(ranked)).filter('rank = 1')
             .select('session_id', F.col('minute_start').alias('peak_minute_start'),
                     F.col('chat_count').alias('peak_minute_chat_count')))
    active = minutes.filter(F.col('session_id').isNotNull()).groupBy('session_id').agg(F.count('*').alias('active_minutes'))
    duration = (seconds('effective_end') - seconds('started_at')) / 60
    max_gap = F.when(F.col('last_event_at').isNull(), duration).otherwise(
        F.greatest(F.col('max_inner_gap_minutes'), (seconds('effective_end') - seconds('last_event_at')) / 60))
    result = (spark.createDataFrame(sessions, SESSION_SCHEMA)
              .join(totals, 'session_id', 'left').join(peaks, 'session_id', 'left').join(active, 'session_id', 'left')
              .fillna(0, subset=[*COUNTS, 'unique_chatters', 'active_minutes'])
              .withColumn('duration_minutes', duration)
              .withColumn('chats_per_minute', F.when(duration > 0, F.col('chat_count') / duration))
              .withColumn('max_gap_minutes', max_gap)
              .withColumn('data_status', F.when(F.col('last_event_at').isNull(), 'NO_DATA')
                          .when(F.col('max_gap_minutes') >= GAP_MINUTES, 'CHECK_GAP').otherwise('OK')))
    return result.drop('effective_end', 'max_inner_gap_minutes')


def write_manifest(gold_path, manifest):
    pending = Path(gold_path) / '_gold_manifest.json.tmp'
    pending.write_text(json.dumps(manifest, indent=2) + '\n')
    pending.replace(Path(gold_path) / '_gold_manifest.json')


def run(spark, silver_path, gold_path, control_db, now=None):
    """Full recompute from one pinned version of each Silver table."""
    from delta.tables import DeltaTable
    now = now or datetime.now(timezone.utc)
    versions = {t: DeltaTable.forPath(spark, f'{silver_path}/{t}').history(1).first()['version'] for t in SOURCES}
    sessions = read_sessions(control_db, now)
    events = tagged_events(spark, silver_path, sessions, versions).cache()
    try:
        minutes = minute_activity(events).cache()
        outputs = {'channel_minute_activity': minutes,
                   'broadcast_sessions': broadcast_sessions(spark, events, minutes, sessions)}
        for table, df in outputs.items():
            df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(f'{gold_path}/{table}')
        minutes.unpersist()
    finally:
        events.unpersist()
    manifest = {'mode': 'full', 'computed_at': now.isoformat(), 'silver_path': silver_path, 'silver_versions': versions,
                'control_db_path': control_db, 'gap_minutes': GAP_MINUTES}
    write_manifest(gold_path, manifest)
    return manifest


def refresh(spark, silver_path, gold_path, control_db, events=None, now=None):
    """Recompute only the sessions (and session-less minutes) that new events or Collector records touched."""
    from pyspark.sql import functions as F
    now = now or datetime.now(timezone.utc)
    if not (Path(gold_path) / 'broadcast_sessions' / '_delta_log').exists():
        return run(spark, silver_path, gold_path, control_db, now)
    sessions = read_sessions(control_db, now)
    stored = {r.session_id: {'record': (r.started_us, r.ended_us, r.stop_reason, r.channel_name),
                             'events': (r.first_us, r.last_us)} for r in
              spark.read.format('delta').load(f'{gold_path}/broadcast_sessions')
              .select('session_id', F.unix_micros('started_at').alias('started_us'), F.unix_micros('ended_at').alias('ended_us'),
                      'stop_reason', 'channel_name', F.unix_micros('first_event_at').alias('first_us'),
                      F.unix_micros('last_event_at').alias('last_us')).collect()}
    affected = changed_sessions(sessions, stored)
    loose = {}
    if events is not None:
        # Reduce in Spark: an empty checkpoint replays all of Silver, so collect one row per session or channel.
        keys = (assign_sessions(spark, events.select('channel_id', 'event_time'), sessions)
                .withColumn('minute_us', F.unix_micros(F.date_trunc('minute', 'event_time'))).cache())
        try:
            affected |= {r.session_id for r in keys.filter('session_id IS NOT NULL').select('session_id').distinct().collect()}
            # Session-less minutes fold into one inclusive range per channel.
            loose = {r.channel_id: (r.low, r.high) for r in keys.filter('session_id IS NULL').groupBy('channel_id')
                     .agg(F.min('minute_us').alias('low'), F.max('minute_us').alias('high')).collect()}
        finally:
            keys.unpersist()
    loose = widen_loose(loose, sessions, affected, stored)
    minutes_where, sessions_where = replace_conditions(affected, loose)
    if minutes_where is not None:
        touched = [s for s in sessions if s['session_id'] in affected]
        windows = [(s['channel_id'], s['started_at'], s['effective_end']) for s in touched]
        windows += [(c, EPOCH + timedelta(microseconds=lo), EPOCH + timedelta(microseconds=hi) + timedelta(minutes=1))
                    for c, (lo, hi) in loose.items()]
        scoped = tagged_events(spark, silver_path, sessions, windows=windows).cache()
        try:
            minutes = minute_activity(scoped).cache()
            # A loose range can also hold minutes of untouched sessions; keep only the keys being rewritten.
            keep = F.col('session_id').isin(sorted(affected)) | (F.col('session_id').isNull() & F.expr(minutes_where))
            (minutes.filter(keep).write.format('delta').mode('overwrite').option('replaceWhere', minutes_where)
             .save(f'{gold_path}/channel_minute_activity'))
            if sessions_where:
                (broadcast_sessions(spark, scoped, minutes, touched).write.format('delta').mode('overwrite')
                 .option('replaceWhere', sessions_where).save(f'{gold_path}/broadcast_sessions'))
            minutes.unpersist()
        finally:
            scoped.unpersist()
    manifest = {'mode': 'stream', 'computed_at': now.isoformat(), 'silver_path': silver_path,
                'refreshed_sessions': sorted(affected), 'refreshed_loose_channels': sorted(loose),
                'control_db_path': control_db, 'gap_minutes': GAP_MINUTES}
    write_manifest(gold_path, manifest)
    return manifest


def create_query(spark, settings, on_batch):
    """One stream over the four Silver tables; each micro-batch hands its new events to `on_batch`."""
    silver_path = settings.silver.output_path
    # Silver deletes rows only on rare business-key conflicts; skip those commits instead of failing the stream.
    # Ongoing sessions are recomputed every trigger, so only ended sessions keep a skipped delete until --full.
    streams = [spark.readStream.format('delta').option('skipChangeCommits', 'true').load(f'{silver_path}/{t}')
               .select('channel_id', 'event_time') for t in SOURCES]
    events = reduce(lambda a, b: a.unionByName(b), streams)
    return (events.writeStream.option('checkpointLocation', settings.checkpoint_path)
            .foreachBatch(lambda batch, _batch_id: on_batch(batch))
            .trigger(processingTime=settings.processing_time).start())


@contextmanager
def gold_lock(settings):
    import fcntl
    path = Path(settings.gold_path)
    path.mkdir(parents=True, exist_ok=True)
    with (path / '.writer.lock').open('a+') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError('Another Gold writer is already using this output') from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def main(argv=None):
    from spark.bronze_to_silver import writer_lock
    from spark.kafka_raw_to_bronze import create_spark_session
    settings = load_settings(argv)
    silver = settings.silver
    runtime = replace(silver.runtime, spark_app_name='chzzk-silver-to-gold')
    args = (silver.output_path, settings.gold_path, settings.control_db)
    with gold_lock(settings):
        if settings.full:
            # Holding the Silver writer lock keeps a Silver batch from landing between our table reads.
            with writer_lock(silver):
                spark = create_spark_session(runtime, kafka=False)
                try:
                    manifest = run(spark, *args)
                finally:
                    spark.stop()
            print('[spark-gold] ' + json.dumps(manifest), flush=True)
            return
        spark = create_spark_session(runtime, kafka=False)
        lock, last = threading.Lock(), [time.monotonic()]
        def refresh_once(events=None):
            # The stream thread (new events) and the timer below (ongoing/ended sessions) both write Gold.
            with lock:
                manifest = refresh(spark, *args, events=events)
                last[0] = time.monotonic()
            print('[spark-gold] ' + json.dumps(manifest), flush=True)
        stopped = False
        def request_stop(_signum, _frame):
            nonlocal stopped
            stopped = True
        previous = {sig: signal.signal(sig, request_stop) for sig in (signal.SIGINT, signal.SIGTERM)}
        query = None
        try:
            query = create_query(spark, settings, refresh_once)
            print('SPARK_GOLD_READY', flush=True)
            interval = seconds(settings.processing_time)
            # No new Silver rows means no micro-batch, but an ongoing or just-ended session still changes.
            while not stopped and not query.awaitTermination(1):
                if time.monotonic() - last[0] >= interval:
                    refresh_once()
        finally:
            try:
                if query is not None:
                    query.stop()
            finally:
                spark.stop()
                for sig, handler in previous.items():
                    signal.signal(sig, handler)


if __name__ == '__main__':
    main()
