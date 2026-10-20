"""Native Silver must equal the Python reference: JAVA_HOME=<JDK17> python tests/check_silver_native.py [--real].

Runs at Spark's default 1g driver memory on purpose: --real then proves a whole-Bronze backlog fits.

1. parse: thousands of synthetic frames, one field broken at a time, through parse_frame and silver_native.parse.
2. write: batch sequences with duplicates, replays, conflicts and earlier quarantine through both write_batch.
3. --real: native over the local Bronze, compared with the Silver the reference already built from it.
"""
import argparse
import copy
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

os.environ['TZ'] = 'UTC'
time.tzset()
os.environ['PYSPARK_PYTHON'] = sys.executable
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_silver import MS, donation, frame, message

NOW = datetime(2026, 9, 22, 15, 0, tzinfo=timezone.utc)
MISSING = object()
VALUES = [MISSING, None, True, False, 0, 1, -1, 2**31 - 1, 2**31, -2**31, -2**31 - 1, 2**63 - 1, 2**63, -2**63, 1.0, 1.5,
          1e3, '0', '1', '+1', '-1', ' 1', '1\n', '٣', '', ' ', '　', '\n', 'x', 'CHAT', 'CURRENCY', 'anonymous',
          'actor', 'channel', 'SUBSCRIPTION_GIFT_RECEIVER', [], {}, '{}', '{broken', 'null', '"s"', [1], {'a': 1}]
FRAME_SCHEMA = ('topic string, partition int, offset long, channel_id string, payload_json string, '
                'kafka_timestamp_ms long, ingested_at_ms long')
EXCLUDED = ('silver_processed_at', 'detected_at')


def spark_session():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder.master('local[4]').appName('check-silver-native')
               .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '4')
               .config('spark.sql.session.timeZone', 'UTC').config('spark.databricks.delta.snapshotPartitions', '2')
               .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
               .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog'))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    spark.conf.set('spark.sql.variant.allowDuplicateKeys', 'true')
    return spark


def with_value(container, key, value):
    result = copy.deepcopy(container)
    if value is MISSING:
        result.pop(key, None)
    else:
        result[key] = value
    return result


def corpus():
    """Frames that break one thing at a time, on every event type the parser routes."""
    bases = [
        (93101, message()),
        (93101, message(5)),
        (93102, donation()),
        (93102, message(10, extras={'donationType': 'CHAT', 'donationId': 'd', 'payType': 'CURRENCY', 'payAmount': 1000,
                                    'isAnonymous': False, 'streamingChannelId': 'channel'})),
        (93102, message(12, extras={'giftType': 'SUBSCRIPTION_GIFT_RECEIVER', 'giftId': 'g', 'receiverUserIdHash': 'r', 'giftTierNo': 1})),
        (93102, message(11, extras={'month': 8, 'tierNo': 1, 'tierName': 'fan'})),
        (93102, message(30, profile='{}')),
    ]
    payloads, offset = [], 0

    def add(cmd, body, **frame_changes):
        nonlocal offset
        payloads.append(frame(body, cmd=cmd, offset=offset, **frame_changes))
        offset += 1

    for cmd, item in bases:
        add(cmd, [item])
        for key in ('cmd', 'msgTypeCode', 'msgTime', 'uid', 'msg', 'msgStatusType', 'cid', 'profile', 'extras'):
            for value in VALUES:
                add(cmd, [with_value(item, key, value)])
        extras = item.get('extras') if isinstance(item.get('extras'), dict) else {}
        for key in (*extras, 'isAnonymous', 'streamingChannelId', 'donationType', 'giftType'):
            for value in VALUES:
                add(cmd, [with_value(item, 'extras', with_value(extras, key, value))])
        profile = item.get('profile') if isinstance(item.get('profile'), dict) else {}
        for key in ('userIdHash', 'nickname', 'userRoleCode', 'streamingProperty'):
            for value in VALUES:
                add(cmd, [with_value(item, 'profile', with_value(profile, key, value))])
        for value in VALUES:
            sub = with_value(profile, 'streamingProperty', with_value({}, 'subscription', value))
            add(cmd, [with_value(item, 'profile', sub)])
            if value is not MISSING and (isinstance(value, (int, str)) or value is None):
                add(cmd, [with_value(item, 'profile', with_value(profile, 'streamingProperty',
                                                                  {'subscription': {'accumulativeMonth': value, 'tier': value}}))])
        add(cmd, [with_value(item, 'profile', json.dumps(profile))])
        add(cmd, [with_value(item, 'extras', json.dumps(extras))])
        add(cmd, [item, None, 'x', 1, {}, item])
        add(cmd, item)
    for body in (MISSING, None, 'x', 1, [], {}, [None], [[]]):
        payload = {'cmd': 93101} if body is MISSING else {'cmd': 93101, 'bdy': body}
        add(93101, None, payload_json=json.dumps(payload))
    for cmd in (MISSING, None, True, '93101', '93102', ' 93101', 93101.0, 94010, 2**40, 'x'):
        payload = {'bdy': [message()]} if cmd is MISSING else {'cmd': cmd, 'bdy': [message()]}
        add(93101, None, payload_json=json.dumps(payload))
    for raw in (None, '', '{broken', 'null', '[]', '"s"', '1', '{}', '{"cmd":93101,"bdy":[{"msgTypeCode":1}],"cmd":93102}',
                '{"cmd":93101,"bdy":[{"msgTypeCode":1,"msgTime":1790089200000,"uid":"a","uid":"b","msg":"m"}]}',
                '{"cmd":93101,"bdy":"[]"}', ' {"cmd":93101,"bdy":[]} '):
        add(93101, None, payload_json=raw)
    for channel in (None, '', ' ', '　', 'other'):
        add(93101, [message()], channel_id=channel)
        add(93102, [message(10, extras={'donationType': 'CHAT', 'donationId': 'd', 'payType': 'CURRENCY', 'payAmount': 1,
                                        'isAnonymous': True, 'streamingChannelId': None})], channel_id=channel)
    for stamp in (None, 0, -1, -62135596800000, -62135596800001, 253402300799999, 253402300800000, 2**62, MS + 300000, MS - 300001):
        add(93101, [message()], kafka_timestamp_ms=stamp)
        add(93101, [message()], ingested_at_ms=stamp)
    for stamp in (0, 1, MS + 300000, MS + 300001, 253402268399999, 253402268400000, 253402300799999, 253402300800000):
        add(93101, [message(msgTime=stamp)], kafka_timestamp_ms=253402300799999)
    return payloads


EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def normalize(value):
    if isinstance(value, datetime):  # reference side; Spark timestamps arrive as micros (see collectable)
        return (value - EPOCH) // timedelta(microseconds=1)
    if isinstance(value, list):
        return tuple(normalize(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((k, normalize(v)) for k, v in value.items()))
    if hasattr(value, 'asDict'):
        return normalize(value.asDict(recursive=True))
    return value


def collectable(df):
    """Timestamps as epoch micros: PySpark cannot turn year-1 instants into Python datetimes."""
    from pyspark.sql import functions as F
    return df.select(*[F.unix_micros(c).alias(c) if t == 'timestamp' else F.col(c) for c, t in df.dtypes])


def check_parse(spark):
    from spark.silver_native import parse, parse_quarantine, table_rows
    from spark.silver_parser import TABLES, parse_frame
    from spark.silver_store import schemas
    frames = corpus()
    expected = {}
    for f in frames:
        for record in parse_frame(f, NOW):
            if record['table'] == '_metric':
                key = (f['offset'], 'metric', record['metric'])
                expected[key] = expected.get(key, 0) + 1
                continue
            fields = schemas()[record['table']].names
            key = (f['offset'], record['body_index'], record['table'])
            expected[key] = tuple((k, normalize(record.get(k))) for k in fields if k not in EXCLUDED)
    df = spark.createDataFrame(frames, FRAME_SCHEMA)
    parsed = parse(df, NOW).persist()
    actual = {}
    for row in parsed.filter("kind = 'metric'").groupBy('offset', 'metric').count().collect():
        actual[(row['offset'], 'metric', row['metric'])] = row['count']
    for table in TABLES:
        for row in collectable(table_rows(parsed, table)).collect():
            record = row.asDict(recursive=True)
            actual[(record['offset'], record['body_index'], table)] = tuple(
                (k, normalize(record[k])) for k in schemas()[table].names if k not in EXCLUDED)
    for row in collectable(parse_quarantine(parsed, NOW)).collect():
        record = row.asDict(recursive=True)
        actual[(record['offset'], record['body_index'], 'quarantine')] = tuple(
            (k, normalize(record[k])) for k in schemas()['quarantine'].names if k not in EXCLUDED)
    parsed.unpersist()
    report('parse', expected, actual, frames=len(frames))


def report(name, expected, actual, **context):
    missing = sorted(set(expected) - set(actual), key=str)
    extra = sorted(set(actual) - set(expected), key=str)
    different = sorted((k for k in set(expected) & set(actual) if expected[k] != actual[k]), key=str)
    if missing or extra or different:
        for key in missing[:5]:
            print('  only in reference:', key, expected[key])
        for key in extra[:5]:
            print('  only in native:', key, actual[key])
        for key in different[:5]:
            want, got = dict(expected[key]) if isinstance(expected[key], tuple) else expected[key], \
                dict(actual[key]) if isinstance(actual[key], tuple) else actual[key]
            if isinstance(want, dict):
                print('  differs:', key, {k: (want[k], got.get(k)) for k in want if want[k] != got.get(k)})
            else:
                print('  differs:', key, want, got)
        raise AssertionError(f'{name}: {len(missing)} missing, {len(extra)} extra, {len(different)} different')
    print(f'{name}: {len(expected)} records identical', ', '.join(f'{k}={v}' for k, v in context.items()), flush=True)


def tables(spark, path, exclude=EXCLUDED):
    from spark.silver_parser import TABLES
    result = {}
    for table in (*TABLES, 'quarantine'):
        df = spark.read.format('delta').load(f'{path}/{table}')
        columns = [c for c in df.columns if c not in exclude]
        for row in collectable(df.select(*columns)).collect():
            key = (table,) + tuple(row[k] for k in ('topic', 'partition', 'offset', 'body_index'))
            if key in result:
                raise AssertionError(f'duplicate {key} in {path}')
            result[key] = tuple((k, normalize(row[k])) for k in columns)
    return result


def scenarios():
    """Batch sequences that exercise every reconcile path; each batch stays under the reference's 10k chunk."""
    gift = lambda r: message(12, extras={'giftType': 'SUBSCRIPTION_GIFT_RECEIVER', 'giftId': 'gift', 'receiverUserIdHash': r})
    yield 'check_silver', [
        [frame([message(), None, message()], offset=0), frame([donation(msg_unused='ignored')], cmd=93102, offset=1),
         frame([message(11, extras={'month': 8})], cmd=93102, offset=2), frame([message(30, profile='{}')], cmd=93102, offset=3),
         frame([gift('recipient')], cmd=93102, offset=4)],
        [frame([message(), None, message()], offset=0)],
        [frame([donation(2000)], cmd=93102, offset=5)],
        [frame([donation()], cmd=93102, offset=6)],
    ]
    yield 'same_batch', [[frame([donation()], cmd=93102, offset=0), frame([donation()], cmd=93102, offset=1),
                          frame([donation()], cmd=93102, offset=1), frame([gift('one'), gift('two')], cmd=93102, offset=2),
                          frame([message(11, extras={'month': 8})] * 2, cmd=93102, offset=3),
                          frame([donation(-1)], cmd=93102, offset=4), frame([donation(-1)], cmd=93102, offset=4)]]
    yield 'source_conflict', [[frame(offset=0)], [frame([donation()], cmd=93102, offset=0)],
                              [frame([donation()], cmd=93102, offset=9)], [frame(offset=0)]]
    yield 'chained_conflict', [
        [frame([donation(id='a')], cmd=93102, offset=0), frame([donation(id='b')], cmd=93102, offset=1)],
        [frame([donation(2000, id='a'), donation(id='b')], cmd=93102, offset=2)],
        [frame([donation(id='b')], cmd=93102, offset=3), frame([donation(id='c')], cmd=93102, offset=4)],
    ]
    rng = random.Random(7)
    batches, offset, history = [], 0, []
    for _ in range(12):
        batch = []
        for _ in range(rng.randint(5, 40)):
            if history and rng.random() < 0.2:
                batch.append(copy.deepcopy(rng.choice(history)))
                continue
            body = []
            for _ in range(rng.randint(1, 3)):
                pick = rng.random()
                if pick < 0.45:
                    body.append(donation(rng.choice([1000, 1000, 1000, 2000]), id=f'd{rng.randint(0, 25)}'))
                elif pick < 0.6:
                    body.append(gift(f'r{rng.randint(0, 5)}'))
                elif pick < 0.7:
                    body.append(donation('bad'))
                else:
                    body.append(message(10, extras={'donationType': 'CHAT', 'donationId': f'd{rng.randint(0, 25)}',
                                                    'payType': 'CURRENCY', 'payAmount': 1000, 'isAnonymous': True}))
            f = frame(body, cmd=93102, offset=offset, channel_id=rng.choice(['channel', 'other']))
            offset += 1
            history.append(f)
            batch.append(f)
        batches.append(batch)
    yield 'random', batches


def check_write(spark):
    from spark import silver_native, silver_store
    held = len(silver_native.local_checkpoints(spark))
    for name, batches in scenarios():
        with TemporaryDirectory() as root:
            for implementation, output in ((silver_store, root + '/reference'), (silver_native, root + '/native')):
                for batch_id, batch in enumerate(batches):
                    implementation.write_batch(spark.createDataFrame(batch, FRAME_SCHEMA), batch_id, output)
            expected, actual = tables(spark, root + '/reference'), tables(spark, root + '/native')
            report(f'write:{name}', expected, actual, batches=len(batches))
            leaked = len(silver_native.local_checkpoints(spark)) - held
            assert leaked == 0, f'write:{name}: {leaked} checkpointed RDDs left in the block manager'


def pinned_snapshot(frames_root):
    """Bronze version and Silver time of the last committed Silver batch, so a running stream cannot skew the diff.

    The batch's end offset names the first Bronze version it did not read; its commit file is written after the
    batch's table writes and before the next batch starts, so Silver as of that file's time is that batch's result.
    """
    checkpoint = Path(frames_root) / 'silver_checkpoint'
    batch = max(int(p.name) for p in (checkpoint / 'commits').iterdir() if p.name.isdigit())
    offset = json.loads((checkpoint / 'offsets' / str(batch)).read_text().splitlines()[-1])
    version = offset['reservoirVersion'] - (1 if offset['index'] == -1 else 0)
    if offset['index'] != -1:
        raise AssertionError('Silver stopped inside a Bronze version; rerun after its next batch')
    committed = datetime.fromtimestamp((checkpoint / 'commits' / str(batch)).stat().st_mtime, timezone.utc)
    return version, committed.strftime('%Y-%m-%d %H:%M:%S.%f')


def version_at(spark, path, timestamp):
    """The table as of `timestamp`; timestampAsOf refuses a time after the last commit of a quiet table."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    version = (DeltaTable.forPath(spark, path).history().filter(F.col('timestamp') <= F.lit(timestamp).cast('timestamp'))
               .agg(F.max('version')).first()[0])
    return spark.read.format('delta').option('versionAsOf', version).load(path)


def check_real(spark, frames_root):
    """Native over the whole local Bronze must reproduce the Silver the reference built batch by batch.

    Compared inside Spark (both directions of exceptAll) because the tables are too large to collect.
    """
    from spark import silver_native
    from spark.silver_parser import TABLES
    bronze_version, silver_as_of = pinned_snapshot(frames_root)
    bronze = spark.read.format('delta').option('versionAsOf', bronze_version).load(f'{frames_root}/bronze')
    print(f'real: Bronze version {bronze_version}, Silver as of {silver_as_of}', flush=True)
    with TemporaryDirectory() as root:
        started = time.time()
        totals = silver_native.write_batch(bronze, 0, root + '/native', f'{frames_root}/bronze')
        print(f'real: native processed {totals["frames"]} frames in {time.time() - started:.0f}s', flush=True)
        problems = 0
        for table in (*TABLES, 'quarantine'):
            expected = version_at(spark, f'{frames_root}/silver/{table}', silver_as_of)
            actual = spark.read.format('delta').load(f'{root}/native/{table}')
            columns = [c for c in expected.columns if c not in EXCLUDED]
            expected, actual = expected.select(*columns), actual.select(*columns)
            missing, extra = expected.exceptAll(actual), actual.exceptAll(expected)
            counts = (expected.count(), actual.count(), missing.count(), extra.count())
            print(f'real:{table}: reference={counts[0]} native={counts[1]} only_reference={counts[2]} only_native={counts[3]}', flush=True)
            for row in missing.limit(3).collect():
                print('  only in reference:', row)
            for row in extra.limit(3).collect():
                print('  only in native:', row)
            problems += counts[2] + counts[3]
        if problems:
            raise AssertionError(f'real: {problems} rows differ')
        print('real: every table identical', flush=True)
        # Steady state: a small replayed batch on top of the full tables, as a 10-second trigger would see it.
        from pyspark.sql import functions as F
        from spark import silver_store
        replay = bronze.orderBy(F.col('offset').desc()).limit(200).localCheckpoint()
        for name, implementation in (('native', silver_native), ('reference', silver_store)):
            started = time.time()
            totals = implementation.write_batch(replay, 1, root + '/native', f'{frames_root}/bronze')
            print(f'real: {name} replay of {replay.count()} frames took {time.time() - started:.1f}s, '
                  f'inserted={totals.get("inserted")}', flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--real', action='store_true', help='also compare against the local Silver')
    parser.add_argument('--real-only', action='store_true', help='skip the synthetic checks')
    parser.add_argument('--frames', default='data/output/local/frames')
    args = parser.parse_args()
    spark = spark_session()
    try:
        if not args.real_only:
            check_parse(spark)
            check_write(spark)
        if args.real or args.real_only:
            check_real(spark, str(Path(args.frames).resolve()))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
