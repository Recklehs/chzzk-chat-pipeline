"""Reconcile source/business keys, then commit recoverable Delta table writes."""
import json
from collections import Counter, defaultdict, deque
from datetime import date, datetime, timezone

from spark.silver_parser import PARSER_VERSION, SOURCE_KEY, TABLES, quarantine, source_key

BUSINESS_KEYS = {
    'donations': ('channel_id', 'donation_id'),
    'subscription_gifts': ('channel_id', 'gift_type', 'gift_id', 'recipient_user_id'),
}
CRITICAL = {
    'donations': ('msg_time_ms', 'actor_user_id', 'is_anonymous', 'donation_type', 'pay_type', 'pay_amount', 'message', 'message_status'),
    'subscription_gifts': ('msg_time_ms', 'actor_user_id', 'gift_type', 'gift_id', 'recipient_user_id', 'gift_tier_no', 'message', 'message_status'),
}


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'),
                      default=lambda v: v.isoformat() if isinstance(v, (datetime, date)) else str(v))


def keys(row):
    result = [encode(['source', *source_key(row)])]
    if row['table'] in BUSINESS_KEYS:
        result.append(encode(['business', row['table'], *[row[k] for k in BUSINESS_KEYS[row['table']]]]))
    return result


def signature(row):
    return encode({k: sorted(v) if k == 'quality_flags' else v for k, v in row.items()
                   if k not in ('silver_processed_at', 'bronze_ingested_at', 'detected_at')})


def combine_quarantine(rows):
    combined = {}
    for row in rows:
        key = (*source_key(row), row['parser_version'])
        if key not in combined:
            combined[key] = dict(row)
            continue
        previous = combined[key]
        previous['detected_at'] = min(previous['detected_at'], row['detected_at'])
        for field in ('error_codes', 'error_fields', 'conflict_keys'):
            previous[field] = sorted(set(previous[field]) | set(row[field]))
        sources = {source_key(r) for r in previous['related_sources'] + row['related_sources']}
        previous['related_sources'] = [dict(zip(SOURCE_KEY, k)) for k in sorted(sources)]
    return list(combined.values())


def reconcile(incoming, existing, quarantined, now):
    """Pure reconciliation; only matched existing normal rows are required."""
    metrics = Counter(r['metric'] for r in incoming if r['table'] == '_metric')
    incoming = [r for r in incoming if r['table'] != '_metric']
    records = incoming + list(existing)
    groups = defaultdict(list)
    row_keys = []
    for i, row in enumerate(records):
        row_keys.append(keys(row))
        for key in row_keys[-1]:
            groups[key].append(i)
    seeds = []
    for row in quarantined:
        for key in keys(row) + row['conflict_keys']:
            for code in row['error_codes']:
                seeds.append((key, code))
    for key, indices in groups.items():
        normal = [records[i] for i in indices if records[i]['table'] != 'quarantine']
        if not normal:
            continue
        if json.loads(key)[0] == 'source':
            if len({signature(records[i]) for i in indices}) > 1:
                seeds.append((key, 'SOURCE_KEY_CONFLICT'))
        else:
            fields = CRITICAL[normal[0]['table']]
            if len({encode([r.get(k) for k in fields]) for r in normal}) > 1:
                seeds.append((key, 'BUSINESS_KEY_CONFLICT'))
    blocked = defaultdict(set)
    visited = set()
    pending = deque(seeds)
    while pending:
        key, code = pending.popleft()
        if (key, code) in visited:
            continue
        visited.add((key, code))
        for index in groups.get(key, ()):
            blocked[index].add(code)
            pending.extend((k, code) for k in row_keys[index])
    quarantines = [r for r in incoming if r['table'] == 'quarantine']
    handled = set()
    for index in blocked:
        if index in handled:
            continue
        component, tokens, codes, references = set(), set(), set(), set()
        pending_indices = [index]
        while pending_indices:
            current = pending_indices.pop()
            if current in component:
                continue
            component.add(current)
            references.add(source_key(records[current]))
            codes.update(blocked[current])
            for key in row_keys[current]:
                tokens.add(key)
                pending_indices.extend(groups[key])
        handled.update(component)
        for current in component:
            # Keep representative references, not a quadratic copy of the entire conflict group.
            relevant = {k for k in tokens if json.loads(k)[0] == 'business'} | {row_keys[current][0]}
            examples = set(sorted(references)[:2]) | {source_key(records[current])}
            quarantines.append(quarantine(records[current], codes, [], now, relevant, examples))
    inserts = []
    survivors = defaultdict(list)
    for index, row in enumerate(records):
        if index not in blocked and row['table'] in TABLES:
            survivors[row_keys[index][-1]].append(index)
    for indices in survivors.values():
        candidates = [i for i in indices if i < len(incoming)]
        if not candidates:
            continue
        if any(i >= len(incoming) for i in indices):
            metrics['duplicate_events'] += len(candidates)
            continue
        winner = min(candidates, key=lambda i: source_key(records[i]))
        inserts.append(records[winner])
        metrics['duplicate_events'] += len(candidates) - 1
    deletes = [r for i, r in enumerate(records) if i >= len(incoming) and i in blocked]
    return {'inserts': inserts, 'deletes': deletes, 'quarantine': combine_quarantine(quarantines),
            'metrics': dict(metrics)}


def schemas():
    from pyspark.sql.types import (ArrayType, BooleanType, DateType, IntegerType, LongType,
                                  StringType, StructField, StructType, TimestampType)
    s, i, l, t = StringType(), IntegerType(), LongType(), TimestampType()
    coordinate = [StructField(k, typ, False) for k, typ in zip(SOURCE_KEY, (s, i, l, i))]
    common = coordinate + [StructField(k, typ, False) for k, typ in (
        ('channel_id', s), ('source_cmd', i), ('cmd', i), ('message_type_code', i),
        ('msg_time_ms', l), ('event_time', t), ('event_date_kst', DateType()),
        ('kafka_timestamp', t), ('bronze_ingested_at', t), ('silver_processed_at', t),
        ('quality_flags', ArrayType(s, False)))]
    common += [StructField(k, typ, True) for k, typ in (
        ('chat_channel_id', s), ('message_status', s), ('actor_user_id', s), ('nickname', s),
        ('actor_user_role', s), ('message', s), ('sender_subscription_months', i), ('sender_subscription_tier', i))]
    extra = {
        'chat_messages': [],
        'donations': [('donation_id', s, False), ('donation_type', s, False), ('pay_type', s, False),
                      ('pay_amount', l, False), ('is_anonymous', BooleanType(), False)],
        'subscription_gifts': [('gift_id', s, False), ('gift_type', s, False), ('recipient_user_id', s, False), ('gift_tier_no', i, True)],
        'subscription_notifications': [('subscription_months', i, False), ('subscription_tier_no', i, True), ('subscription_tier_name', s, True)],
        'unclassified_events': [('classification_reason', s, False), ('observed_donation_type', s, True), ('observed_gift_type', s, True)],
    }
    result = {name: StructType([StructField.fromJson(f.jsonValue()) for f in common] + [StructField(*f) for f in fields]) for name, fields in extra.items()}
    result['chat_messages']['message'].nullable = False
    result['quarantine'] = StructType(coordinate + [StructField(k, typ, optional) for k, typ, optional in (
        ('channel_id', s, True), ('source_cmd', i, True), ('event_type', s, True),
        ('error_codes', ArrayType(s, False), False), ('error_fields', ArrayType(s, False), False),
        ('parser_version', s, False), ('detected_at', t, False), ('conflict_keys', ArrayType(s, False), False),
        ('related_sources', ArrayType(StructType(coordinate), False), False))])
    return result


def initialize_tables(spark, output_path):
    from delta.tables import DeltaTable
    for table, schema in schemas().items():
        path = f'{output_path}/{table}'
        if not DeltaTable.isDeltaTable(spark, path):
            spark.createDataFrame([], schema).write.format('delta').mode('errorifexists').save(path)
        else:
            actual = spark.read.format('delta').load(path).schema
            # Delta reads relax array/struct nullability; compare names and types, not that read metadata.
            if actual.simpleString() != schema.simpleString():
                raise ValueError(f'Silver schema changed: {table}; use a new output and checkpoint')


def read_records(df, table=None):
    from pyspark.sql import functions as F
    options = {'timeZone': 'UTC', 'ignoreNullFields': 'false', 'timestampFormat': "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"}
    if table:
        df = df.select(F.lit(table).alias('_table'), F.to_json(F.struct('*'), options).alias('_record'))
    records = []
    for value in df.collect():
        row = json.loads(value['_record'])
        row['table'] = value['_table']
        for key in ('event_time', 'kafka_timestamp', 'bronze_ingested_at', 'silver_processed_at', 'detected_at'):
            if row.get(key) is not None:
                row[key] = datetime.fromisoformat(row[key].replace('Z', '+00:00'))
        if row.get('event_date_kst'):
            row['event_date_kst'] = date.fromisoformat(row['event_date_kst'])
        records.append(row)
    return records


def load_matches(spark, output_path, incoming):
    from functools import reduce
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType
    incoming = [r for r in incoming if r['table'] != '_metric']
    coordinate_schema = StructType(schemas()['quarantine'].fields[:4])
    coordinates = spark.createDataFrame(sorted({source_key(r) for r in incoming}), coordinate_schema)
    options = {'timeZone': 'UTC', 'ignoreNullFields': 'false', 'timestampFormat': "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"}
    sources = []
    for table in TABLES:
        df = spark.read.format('delta').load(f'{output_path}/{table}')
        business = F.to_json(F.array(F.lit('business'), F.lit(table), *[F.col(k) for k in BUSINESS_KEYS[table]])) if table in BUSINESS_KEYS else F.lit(None).cast('string')
        sources.append(df.select(*SOURCE_KEY, F.lit(table).alias('_table'), business.alias('_key'),
                                 F.to_json(F.struct('*'), options).alias('_record')))
    stored = reduce(lambda a, b: a.unionByName(b), sources)
    matched = stored.join(F.broadcast(coordinates), list(SOURCE_KEY), 'left_semi')
    business = sorted({keys(r)[-1] for r in incoming if r['table'] in BUSINESS_KEYS})
    if business:
        candidates = spark.createDataFrame([(k,) for k in business], '_key string')
        matched = matched.unionByName(stored.join(F.broadcast(candidates), '_key', 'left_semi'))
    existing = read_records(matched.dropDuplicates(['_table', *SOURCE_KEY]))
    quarantined = spark.read.format('delta').load(f'{output_path}/quarantine')
    candidates = spark.createDataFrame([(k,) for k in sorted({k for r in incoming + existing for k in keys(r)})], '_key string')
    direct = quarantined.join(F.broadcast(coordinates), list(SOURCE_KEY), 'left_semi')
    blocked = (quarantined.withColumn('_key', F.explode('conflict_keys'))
               .join(F.broadcast(candidates), '_key', 'left_semi').select(*quarantined.columns))
    prior_errors = read_records(direct.unionByName(blocked).dropDuplicates([*SOURCE_KEY, 'parser_version']), 'quarantine')
    return existing, prior_errors


def match_expression(fields):
    return ' AND '.join(f't.`{k}` = s.`{k}`' for k in fields)


def merge_quarantine(spark, output_path, records):
    from delta.tables import DeltaTable
    if not records:
        return
    df = spark.createDataFrame(records, schemas()['quarantine'])
    updates = {k: f'array_sort(array_distinct(concat(t.{k}, s.{k})))'
               for k in ('error_codes', 'error_fields', 'conflict_keys', 'related_sources')}
    updates['detected_at'] = 'least(t.detected_at, s.detected_at)'
    (DeltaTable.forPath(spark, f'{output_path}/quarantine').alias('t')
     .merge(df.alias('s'), match_expression((*SOURCE_KEY, 'parser_version')))
     .whenMatchedUpdate(set=updates).whenNotMatchedInsertAll().execute())


def apply_changes(spark, output_path, changes):
    from delta.tables import DeltaTable
    schema = schemas()
    # Commit the durable block before removing accepted rows. A retry completes the removal.
    merge_quarantine(spark, output_path, changes['quarantine'])
    for table in TABLES:
        target = DeltaTable.forPath(spark, f'{output_path}/{table}')
        removed = [r for r in changes['deletes'] if r['table'] == table]
        if removed:
            df = spark.createDataFrame(removed, schema[table])
            target.alias('t').merge(df.alias('s'), match_expression(SOURCE_KEY)).whenMatchedDelete().execute()
        added = [r for r in changes['inserts'] if r['table'] == table]
        if added:
            df = spark.createDataFrame(added, schema[table])
            (target.alias('t').merge(df.alias('s'), match_expression(BUSINESS_KEYS.get(table, SOURCE_KEY)))
             .whenNotMatchedInsertAll().execute())


def validate_bronze_source(batch, bronze_path=None):
    """Check immutable Kafka content, including sources discarded by business dedup."""
    from pyspark.sql import functions as F
    coordinates = list(SOURCE_KEY[:3])
    source = batch
    if bronze_path is not None:
        source = (batch.sparkSession.read.format('delta').load(bronze_path)
                  .join(F.broadcast(batch.select(*coordinates).distinct()), coordinates, 'left_semi'))
    timestamp = 'kafka_timestamp' if 'kafka_timestamp' in source.columns else 'kafka_timestamp_ms'
    # DISTINCT includes NULL and omits ingested_at: replay may change ingestion time, never Kafka content.
    conflicts = (source.select(*coordinates, 'payload_json', 'channel_id', timestamp).distinct()
                 .groupBy(*coordinates).count().filter(F.col('count') > 1).limit(1).collect())
    if conflicts:
        location = tuple(conflicts[0][k] for k in coordinates)
        raise ValueError(f'Bronze source content conflict at {location}; repair the source before retrying')


def write_batch(batch, batch_id, output_path, bronze_path=None):
    from pyspark.sql import functions as F
    from spark.silver_parser import parse_frame
    spark = batch.sparkSession
    validate_bronze_source(batch, bronze_path)
    initialize_tables(spark, output_path)
    now = datetime.now(timezone.utc)
    totals = Counter()
    def commit(records):
        events = [r for r in records if r['table'] != '_metric']
        if events:
            existing, errors = load_matches(spark, output_path, records)
        else:
            existing, errors = [], []
        changes = reconcile(records, existing, errors, now)
        apply_changes(spark, output_path, changes)
        totals.update(changes['metrics'])
        totals.update({f'input:{name}': sum(r['table'] == name for r in events) for name in (*TABLES, 'quarantine')})
        totals['inserted'] += len(changes['inserts'])
        totals['removed'] += len(changes['deletes'])
        totals['quarantine_upserts'] += len(changes['quarantine'])
    if 'kafka_timestamp' in batch.columns:
        batch = batch.withColumn('kafka_timestamp_us', F.unix_micros('kafka_timestamp'))
    if 'ingested_at' in batch.columns:
        batch = batch.withColumn('ingested_at_us', F.unix_micros('ingested_at'))
    pending = []
    # ponytail: local driver parses/reconciles 10k items at a time; move to executor joins if CPU/volume outgrows this.
    for frame in batch.toLocalIterator():
        totals['frames'] += 1
        for record in parse_frame(frame, now):
            pending.append(record)
            if len(pending) >= 10000:
                commit(pending)
                pending = []
    if pending:
        commit(pending)
    print('[spark-silver] ' + encode({'batch_id': batch_id, 'status': 'completed', **totals}), flush=True)
    return dict(totals)
