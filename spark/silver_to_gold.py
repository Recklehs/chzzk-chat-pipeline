"""Silver Delta + Collector sessions -> two Gold Delta tables.

Default: a stream that rewrites only the minutes touched since the last trigger and rebuilds the sessions
holding them from minute rows.
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
        # Collector writes both times from one clock and reuses a channel's open session, so these only fire on a
        # hand-edited row or a clock jump. Stop rather than count events twice; README lists the repair steps.
        if effective_end < started_at:
            raise ValueError(f'Session {session_id} of channel {channel_id} ends before it starts '
                             f'({started} -> {ended}); fix monitoring_sessions in {db_path}, then run --full')
        previous = sessions[-1] if sessions else None
        if previous and previous['channel_id'] == channel_id and previous['effective_end'] > started_at:
            raise ValueError(f'Sessions {previous["session_id"]} and {session_id} of channel {channel_id} overlap, '
                             f'so an event would count twice; fix monitoring_sessions in {db_path}, then run --full')
        sessions.append({'session_id': session_id, 'channel_id': channel_id, 'channel_name': name,
                         'started_at': started_at, 'ended_at': ended_at, 'effective_end': effective_end,
                         'stop_reason': reason, 'broadcast_date_kst': started_at.astimezone(KST).date()})
    return sessions


def record(session):
    """The Collector fields a Gold session row was computed from."""
    return (session['channel_id'], micros(session['started_at']), micros(session['ended_at']),
            session['stop_reason'], session['channel_name'])


def changed_sessions(sessions, stored):
    """Sessions whose minutes Gold must rebuild without new events: new, or with a changed Collector record."""
    return {s['session_id'] for s in sessions if stored.get(s['session_id'], {}).get('record') != record(s)}


def widen_ranges(ranges, sessions, rebuilt, stored):
    """Add each rebuilt session's span to the per-channel minute ranges that get rewritten.

    A changed start, end or channel moves events between a session and no session. The span is stretched to
    the session's previously stored first/last event, on both its current and stored channel, so both sides
    are rewritten and every event is counted once.
    """
    for s in sessions:
        if s['session_id'] not in rebuilt:
            continue
        previous = stored.get(s['session_id'], {})
        first, last = previous.get('events', (None, None))
        low = min(v for v in (micros(s['started_at']), first) if v is not None)
        high = max(v for v in (micros(s['effective_end']) - 1, last, low) if v is not None)
        low, high = low - low % MINUTE_US, high - high % MINUTE_US
        for channel in {s['channel_id'], previous.get('record', (s['channel_id'],))[0]}:
            old = ranges.get(channel)
            ranges[channel] = (min(low, old[0]), max(high, old[1])) if old else (low, high)
    return ranges


def overlapping(sessions, ranges):
    """Sessions with a minute inside a rewritten range; their rows are rebuilt from the new minute rows."""
    return {s['session_id'] for s in sessions if s['channel_id'] in ranges
            and micros(s['started_at']) < ranges[s['channel_id']][1] + MINUTE_US
            and micros(s['effective_end']) > ranges[s['channel_id']][0]}


def replace_conditions(session_ids, ranges):
    """Delta replaceWhere predicates for the rows one incremental refresh rewrites.

    `ranges` maps channel -> (first, last) minute in epoch micros; every row of that channel in the range is rewritten.
    """
    for channel in ranges:
        if not CHANNEL_ID.fullmatch(channel):
            raise ValueError(f'Unexpected channel_id {channel!r}')
    ids = ', '.join(str(int(i)) for i in sorted(session_ids))
    minutes = ' OR '.join(f"(channel_id = '{c}' AND minute_start BETWEEN timestamp_micros({int(lo)}) AND timestamp_micros({int(hi)}))"
                          for c, (lo, hi) in sorted(ranges.items()))
    return minutes or None, f'session_id IN ({ids})' if ids else None


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
            .agg(*[F.sum(c).cast('long').alias(c) for c in COUNTS], F.countDistinct('chatter').alias('unique_chatters'),
                 F.min('event_time').alias('first_event_at'), F.max('event_time').alias('last_event_at')))


def broadcast_sessions(spark, minutes, sessions):
    """Session rows built from minute rows alone, so a refresh never has to reread a session's Silver events."""
    from pyspark.sql import Window, functions as F
    seconds = lambda c: F.unix_micros(c) / 1e6
    minute_index = lambda c: F.floor(F.unix_micros(c) / MINUTE_US)
    spans = (spark.createDataFrame(sessions, SESSION_SCHEMA)
             .withColumn('first_minute', minute_index('started_at'))
             .withColumn('last_minute', minute_index(F.col('effective_end') - F.expr('INTERVAL 1 MICROSECOND'))))
    in_session = (minutes.filter(F.col('session_id').isNotNull())
                  .join(spans.select('session_id', 'first_minute'), 'session_id')
                  .withColumn('minute', minute_index('minute_start')))
    # Empty minutes before each event minute, counting from the session's first minute for the first one.
    previous = F.coalesce(F.lag('minute').over(Window.partitionBy('session_id').orderBy('minute')), F.col('first_minute') - 1)
    totals = (in_session.withColumn('empty_before', F.col('minute') - previous - 1)
              .groupBy('session_id')
              .agg(*[F.sum(c).cast('long').alias(c) for c in COUNTS], F.count('*').alias('active_minutes'),
                   F.min('first_event_at').alias('first_event_at'), F.max('last_event_at').alias('last_event_at'),
                   F.max('minute').alias('last_event_minute'), F.max('empty_before').alias('max_inner_gap')))
    ranked = Window.partitionBy('session_id').orderBy(F.desc('chat_count'), 'minute_start')
    peaks = (minutes.filter(F.col('session_id').isNotNull() & (F.col('chat_count') > 0))
             .withColumn('rank', F.row_number().over(ranked)).filter('rank = 1')
             .select('session_id', F.col('minute_start').alias('peak_minute_start'),
                     F.col('chat_count').alias('peak_minute_chat_count')))
    duration = (seconds('effective_end') - seconds('started_at')) / 60
    # The longest run of calendar minutes with no event, the same runs the dashboard shades as gaps.
    max_gap = F.when(F.col('last_event_at').isNull(), F.greatest(F.col('last_minute') - F.col('first_minute') + 1, F.lit(0))).otherwise(
        F.greatest(F.col('max_inner_gap'), F.col('last_minute') - F.col('last_event_minute')))
    result = (spans.join(totals, 'session_id', 'left').join(peaks, 'session_id', 'left')
              .fillna(0, subset=[*COUNTS, 'active_minutes'])
              .withColumn('duration_minutes', duration)
              .withColumn('chats_per_minute', F.when(duration > 0, F.col('chat_count') / duration))
              .withColumn('max_gap_minutes', max_gap.cast('double'))
              .withColumn('data_status', F.when(F.col('last_event_at').isNull(), 'NO_DATA')
                          .when(F.col('max_gap_minutes') >= GAP_MINUTES, 'CHECK_GAP').otherwise('OK')))
    return result.drop('effective_end', 'first_minute', 'last_minute', 'last_event_minute', 'max_inner_gap')


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
    minutes = minute_activity(events).cache()
    try:
        outputs = {'channel_minute_activity': minutes,
                   'broadcast_sessions': broadcast_sessions(spark, minutes, sessions)}
        for table, df in outputs.items():
            df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(f'{gold_path}/{table}')
    finally:
        minutes.unpersist()
        events.unpersist()
    manifest = {'mode': 'full', 'computed_at': now.isoformat(), 'silver_path': silver_path, 'silver_versions': versions,
                'control_db_path': control_db, 'gap_minutes': GAP_MINUTES}
    write_manifest(gold_path, manifest)
    return manifest


def refresh(spark, silver_path, gold_path, control_db, events=None, now=None):
    """Rewrite only the minutes that new events or Collector records touched, then the sessions holding them.

    Session rows come from minute rows, so an ongoing session without new events rereads no Silver at all.
    """
    from pyspark.sql import functions as F
    now = now or datetime.now(timezone.utc)
    minutes_path, sessions_path = f'{gold_path}/channel_minute_activity', f'{gold_path}/broadcast_sessions'
    # Minute rows written before session rows were built from them lack event times; rebuild everything once.
    if (not (Path(sessions_path) / '_delta_log').exists()
            or 'last_event_at' not in spark.read.format('delta').load(minutes_path).columns):
        return run(spark, silver_path, gold_path, control_db, now)
    sessions = read_sessions(control_db, now)
    stored = {r.session_id: {'record': (r.channel_id, r.started_us, r.ended_us, r.stop_reason, r.channel_name),
                             'events': (r.first_us, r.last_us)} for r in
              spark.read.format('delta').load(sessions_path)
              .select('session_id', 'channel_id', F.unix_micros('started_at').alias('started_us'),
                      F.unix_micros('ended_at').alias('ended_us'),
                      'stop_reason', 'channel_name', F.unix_micros('first_event_at').alias('first_us'),
                      F.unix_micros('last_event_at').alias('last_us')).collect()}
    rebuilt = changed_sessions(sessions, stored)
    ranges = {}
    if events is not None:
        # Reduce in Spark: an empty checkpoint replays all of Silver, so collect one row per channel.
        # ponytail: one range per channel, so a late event rewrites every minute since it; split ranges if late events get common
        minute = lambda c: F.unix_micros(F.date_trunc('minute', c))
        ranges = {r.channel_id: (r.low, r.high) for r in events.groupBy('channel_id')
                  .agg(minute(F.min('event_time')).alias('low'), minute(F.max('event_time')).alias('high')).collect()}
    ranges = widen_ranges(ranges, sessions, rebuilt, stored)
    # Live sessions get a new duration and trailing gap every trigger even when none of their minutes changed.
    refreshed = rebuilt | overlapping(sessions, ranges) | {s['session_id'] for s in sessions if s['ended_at'] is None}
    minutes_where, sessions_where = replace_conditions(refreshed, ranges)
    if minutes_where:
        windows = [(c, EPOCH + timedelta(microseconds=lo), EPOCH + timedelta(microseconds=hi) + timedelta(minutes=1))
                   for c, (lo, hi) in ranges.items()]
        (minute_activity(tagged_events(spark, silver_path, sessions, windows=windows)).write.format('delta')
         .mode('overwrite').option('replaceWhere', minutes_where).save(minutes_path))
    if sessions_where:
        minutes = spark.read.format('delta').load(minutes_path).filter(F.col('session_id').isin(sorted(refreshed)))
        (broadcast_sessions(spark, minutes, [s for s in sessions if s['session_id'] in refreshed]).write.format('delta')
         .mode('overwrite').option('replaceWhere', sessions_where).save(sessions_path))
    manifest = {'mode': 'stream', 'computed_at': now.isoformat(), 'silver_path': silver_path,
                'refreshed_sessions': sorted(refreshed), 'refreshed_channels': sorted(ranges),
                'control_db_path': control_db, 'gap_minutes': GAP_MINUTES}
    write_manifest(gold_path, manifest)
    return manifest


def create_query(spark, settings, on_batch):
    """One stream over the four Silver tables; each micro-batch hands its new events to `on_batch`."""
    silver_path = settings.silver.output_path
    # Silver deletes rows only on rare business-key conflicts; skip those commits instead of failing the stream.
    # A skipped delete stays in Gold until a later event rewrites that minute or --full reconciles it.
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
