"""Silver as Spark expressions: parse, reconcile and write a Bronze micro-batch without collecting rows to the driver.

The rules are the ones in silver_parser.parse_frame and silver_store.reconcile, which stay as the executable
reference: tests/check_silver_native.py runs both on the same input and requires identical tables. JSON values are
read as VARIANT so a number, a numeric string, a boolean and a decimal stay distinguishable, as they are in Python.
"""
from functools import reduce

from spark.silver_parser import PARSER_VERSION, SOURCE_KEY, TABLES
from spark.silver_store import (BUSINESS_KEYS, CRITICAL, initialize_tables, match_expression, merge_quarantine_frame,
                                schemas, validate_bronze_source)

# Python's datetime range and str.isspace(), so validity matches the reference exactly.
TS_MIN, TS_MAX = -62135596800000000, 253402300799999999
KST_OFFSET_US = 9 * 3600 * 1_000_000
FUTURE_LIMIT_US = 5 * 60 * 1_000_000
BLANK = r'\A[' + ''.join(f'\\x{{{c:X}}}' for c in range(0x110000) if chr(c).isspace()) + r']*\z'
DIGITS = r'\A[+-]?[0-9]+\z'
EXCLUDED_FROM_SIGNATURE = ('silver_processed_at', 'bronze_ingested_at', 'detected_at')


def _F():
    from pyspark.sql import functions as F
    return F


# --- JSON value helpers. Each mirrors one Python check on a VARIANT value. ---

def field(value, name):
    """item.get(name): SQL NULL when the key is absent, a variant null when it holds JSON null."""
    return _F().try_variant_get(value, f'$.{name}', 'variant')


def present(value):
    """`value is not None` in Python: the key exists and is not JSON null."""
    F = _F()
    return value.isNotNull() & ~F.coalesce(F.is_variant_null(value), F.lit(False))


def kind(value):
    return _F().schema_of_variant(value)


def is_object(value):
    F = _F()
    return F.coalesce(F.try_variant_get(value, '$', 'map<string,variant>').isNotNull(), F.lit(False))


def is_string(value):
    F = _F()
    return F.coalesce(kind(value) == 'STRING', F.lit(False))


def string_of(value):
    F = _F()
    return F.when(is_string(value), F.try_variant_get(value, '$', 'string'))


def is_blank(text):
    return text.rlike(BLANK)


def integer(value, bits=64):
    """silver_parser.integer: a JSON integer, or a string of ASCII digits, inside the signed range."""
    F = _F()
    text = string_of(value)
    target = 'int' if bits == 32 else 'bigint'
    from_text = F.when(text.rlike(DIGITS), text.try_cast(target)).cast('bigint')
    number = F.when(kind(value) == 'BIGINT', F.try_variant_get(value, '$', 'bigint'))
    if bits == 32:
        number = F.when(number.between(-2**31, 2**31 - 1), number)
    return F.coalesce(from_text, number)


def integer_column(df, name):
    """integer() on a Bronze column, whose Spark type stands in for the Python value's type."""
    F = _F()
    if name not in df.columns:
        return F.lit(None).cast('bigint')
    column, dtype = F.col(name), dict(df.dtypes)[name]
    if dtype == 'string':
        return F.when(column.rlike(DIGITS), column.try_cast('bigint'))
    if dtype in ('tinyint', 'smallint', 'int', 'bigint'):
        return column.cast('bigint')
    return F.lit(None).cast('bigint')


def object_value(value):
    """silver_parser.object_value: (object or NULL, invalid flag); a string holding JSON is parsed first."""
    F = _F()
    parsed = F.when(is_string(value), F.try_parse_json(string_of(value))).otherwise(value)
    valid = is_object(parsed)
    bad = F.when(is_string(value), parsed.isNull() | (present(parsed) & ~valid)).otherwise(present(value) & ~valid)
    return F.when(valid, parsed), bad


def micros(value, unit):
    """EPOCH + timedelta(...) without OverflowError: NULL outside Python's datetime range."""
    F = _F()
    if unit == 'ms':
        value = F.when(value.between(TS_MIN // 1000, TS_MAX // 1000), F.try_multiply(value, F.lit(1000)))
    return F.when(value.between(TS_MIN, TS_MAX), value)


class Item:
    """Collects the errors and quality flags parse_item would raise, as conditional array entries."""

    def __init__(self):
        self.errors, self.flags = [], []

    def fail(self, condition, field_name, code='INVALID_REQUIRED_FIELD'):
        F = _F()
        self.errors.append(F.when(condition, F.struct(F.lit(code).alias('code'), F.lit(field_name).alias('field'))))

    def flag(self, condition, name):
        F = _F()
        self.flags.append(F.when(condition, F.lit(name)))

    def string(self, value, field_name, required=False, preserve_empty=False, when=None):
        F = _F()
        valid = is_string(value)
        if not preserve_empty:
            valid = valid & ~F.coalesce(is_blank(string_of(value)), F.lit(True))
        scope = F.lit(True) if when is None else when
        if isinstance(required, bool):
            required = F.lit(required)
        self.fail(scope & required & ~valid, field_name)
        self.flag(scope & ~required & ~valid & present(value), 'INVALID_OPTIONAL_FIELD')
        return F.when(valid, string_of(value))

    def number(self, value, field_name, bits=32, minimum=0, required=False, when=None):
        F = _F()
        n = integer(value, bits)
        valid = n.isNotNull() & (n >= minimum)
        scope = F.lit(True) if when is None else when
        if required:
            self.fail(scope & ~valid, field_name)
        else:
            self.flag(scope & ~valid & present(value), 'INVALID_OPTIONAL_FIELD')
        return F.when(valid, n)


def parse(batch, now):
    """parse_frame for every frame: one row per output record with `kind` row, quarantine or metric."""
    F = _F()
    frames = batch
    for unit_column in ('kafka_timestamp', 'ingested_at'):
        if unit_column in frames.columns:
            frames = frames.withColumn(f'{unit_column}_us', F.unix_micros(unit_column))
    kafka = ('us', integer_column(frames, 'kafka_timestamp_us')) if 'kafka_timestamp_us' in frames.columns \
        else ('ms', integer_column(frames, 'kafka_timestamp_ms'))
    ingested = ('us', integer_column(frames, 'ingested_at_us')) if 'ingested_at_us' in frames.columns \
        else ('ms', integer_column(frames, 'ingested_at_ms'))
    payload = F.col('payload_json') if 'payload_json' in frames.columns else F.lit(None).cast('string')
    channel = F.col('channel_id') if 'channel_id' in frames.columns else F.lit(None).cast('string')

    df = frames.select(
        'topic', F.col('partition').cast('int').alias('partition'), F.col('offset').cast('bigint').alias('offset'),
        channel.alias('_channel'), F.try_parse_json(payload).alias('_payload'),
        micros(kafka[1], kafka[0]).alias('_kafka_us'), micros(ingested[1], ingested[0]).alias('_ingested_us'))
    body = field(F.col('_payload'), 'bdy')
    body_array = F.try_variant_get(body, '$', 'array<variant>')
    df = df.withColumn('source_cmd', integer(field(F.col('_payload'), 'cmd'), 32).cast('int'))
    status = (F.when(F.col('_payload').isNull(), 'INVALID_JSON')
              .when(~is_object(F.col('_payload')), 'INVALID_FRAME')
              .when(F.col('source_cmd').isNull(), 'INVALID_CMD')
              .when(~F.col('source_cmd').isin(93101, 93102), 'OUT_OF_SCOPE')
              .when(body_array.isNull() & ~is_object(body), 'INVALID_BODY')
              .when(F.size(body_array) == 0, 'EMPTY')
              .otherwise('ITEMS'))
    items = F.when(status == 'ITEMS', F.coalesce(body_array, F.array(body))).otherwise(F.array(F.col('_payload')))
    df = df.select('*', status.alias('_status'), F.posexplode(items).alias('_pos', '_item'))
    df = df.withColumn('body_index', F.when(F.col('_status') == 'ITEMS', F.col('_pos')).otherwise(F.lit(-1)))

    item = F.col('_item')
    has_cmd = field(item, 'cmd').isNotNull()
    df = df.withColumn('cmd', F.when(has_cmd, integer(field(item, 'cmd'), 32)).otherwise(F.col('source_cmd')).cast('int'))
    cmd = F.col('cmd')
    early = (F.when(F.col('_status') == 'INVALID_JSON', F.struct(F.lit('INVALID_JSON').alias('code'), F.lit('payload_json').alias('field')))
             .when(F.col('_status') == 'INVALID_FRAME', F.struct(F.lit('INVALID_FRAME').alias('code'), F.lit('payload_json').alias('field')))
             .when(F.col('_status') == 'INVALID_CMD', F.struct(F.lit('INVALID_CMD').alias('code'), F.lit('cmd').alias('field')))
             .when(F.col('_status') == 'INVALID_BODY', F.struct(F.lit('INVALID_BODY').alias('code'), F.lit('bdy').alias('field')))
             .when((F.col('_status') == 'ITEMS') & ~is_object(item),
                   F.struct(F.lit('INVALID_BODY_ITEM').alias('code'), F.concat(F.lit('bdy['), F.col('_pos').cast('string'), F.lit(']')).alias('field')))
             .when((F.col('_status') == 'ITEMS') & has_cmd & cmd.isNull(),
                   F.struct(F.lit('INVALID_CMD').alias('code'), F.lit('bdy.cmd').alias('field'))))
    metric = (F.when(F.col('_status') == 'OUT_OF_SCOPE', F.concat(F.lit('out_of_scope:'), F.col('source_cmd').cast('string')))
              .when(F.col('_status') == 'EMPTY', F.lit('empty_body_frames'))
              .when((F.col('_status') == 'ITEMS') & is_object(item) & cmd.isNotNull() & ~cmd.isin(93101, 93102),
                    F.concat(F.lit('out_of_scope:'), cmd.cast('string'))))
    df = df.withColumn('_early', early).withColumn('metric', metric)

    # parse_item, evaluated for every row; only rows without an early outcome use it.
    p = Item()
    code = p.number(field(item, 'msgTypeCode'), 'bdy.msgTypeCode', minimum=-(2**31), required=True)
    df = df.withColumn('message_type_code', code.cast('int'))
    code = F.col('message_type_code')
    df = df.withColumn('channel_id', F.when(F.col('_channel').isNotNull() & ~is_blank(F.col('_channel')), F.col('_channel')))
    p.fail(F.col('channel_id').isNull(), 'channel_id')
    df = df.withColumn('msg_time_ms', p.number(field(item, 'msgTime'), 'bdy.msgTime', bits=64, required=True))
    df = df.withColumn('_event_us', micros(F.col('msg_time_ms'), 'ms'))
    for target, source, name in (('kafka_timestamp', '_kafka_us', 'kafka_timestamp_ms'),
                                 ('bronze_ingested_at', '_ingested_us', 'ingested_at_ms'),
                                 ('event_time', '_event_us', 'bdy.msgTime')):
        p.fail(F.col(source).isNull(), name)
        df = df.withColumn(target, F.timestamp_micros(F.col(source)))
    kst_ok = F.col('_event_us') <= TS_MAX - KST_OFFSET_US
    p.fail(F.col('_event_us').isNotNull() & ~kst_ok, 'bdy.msgTime')
    df = df.withColumn('event_date_kst', F.when(kst_ok, F.to_date(F.from_utc_timestamp('event_time', 'Asia/Seoul'))))
    p.fail(F.col('_event_us') - F.col('_kafka_us') > FUTURE_LIMIT_US, 'bdy.msgTime', 'EVENT_TIME_TOO_FAR_IN_FUTURE')

    profile, bad_profile = object_value(field(item, 'profile'))
    extras, bad_extras = object_value(field(item, 'extras'))
    df = df.withColumn('_profile', profile).withColumn('_extras', extras)
    profile, extras = F.col('_profile'), F.col('_extras')
    p.flag(bad_profile, 'INVALID_OPTIONAL_PROFILE')
    needs_extras = (cmd == 93102) & code.isin(10, 11, 12)
    p.fail(needs_extras & extras.isNull(), 'bdy.extras')
    p.flag(~(needs_extras & extras.isNull()) & bad_extras, 'INVALID_OPTIONAL_EXTRAS')

    donation_branch = (cmd == 93102) & (code == 10)
    gift_branch = (cmd == 93102) & (code == 12)
    subscription = (cmd == 93102) & (code == 11)
    donation_kind = p.string(field(extras, 'donationType'), 'extras.donationType', required=True, when=donation_branch)
    gift_kind = p.string(field(extras, 'giftType'), 'extras.giftType', required=True, when=gift_branch)
    donation = donation_branch & F.coalesce(donation_kind == 'CHAT', F.lit(False))
    gift = gift_branch & F.coalesce(gift_kind == 'SUBSCRIPTION_GIFT_RECEIVER', F.lit(False))
    table = (F.when((cmd == 93101) & (code == 1), 'chat_messages').when(donation, 'donations')
             .when(gift, 'subscription_gifts').when(subscription, 'subscription_notifications')
             .otherwise('unclassified_events'))
    df = df.withColumn('table', table)
    table = F.col('table')

    df = df.withColumn('donation_type', F.when(donation, donation_kind))
    df = df.withColumn('donation_id', F.when(donation, p.string(field(extras, 'donationId'), 'extras.donationId', required=True, when=donation)))
    df = df.withColumn('pay_type', F.when(donation, p.string(field(extras, 'payType'), 'extras.payType', required=True, when=donation)))
    df = df.withColumn('pay_amount', F.when(donation, p.number(field(extras, 'payAmount'), 'extras.payAmount', bits=64, required=True, when=donation)))
    anonymous_value = field(extras, 'isAnonymous')
    is_boolean = F.coalesce(kind(anonymous_value) == 'BOOLEAN', F.lit(False))
    df = df.withColumn('is_anonymous', F.when(donation & is_boolean, F.try_variant_get(anonymous_value, '$', 'boolean')))
    p.fail(donation & ~is_boolean, 'extras.isAnonymous')
    p.flag(donation & (F.col('pay_amount') == 0), 'ZERO_PAY_AMOUNT')
    p.flag(donation & (F.col('pay_type') != 'CURRENCY'), 'UNRECOGNIZED_PAY_TYPE')
    uid = field(item, 'uid')
    p.fail(donation & (F.col('is_anonymous') == F.lit(False)) & F.coalesce(string_of(uid) == 'anonymous', F.lit(False)),
           'bdy.uid', 'ANONYMITY_CONFLICT')

    df = df.withColumn('gift_type', F.when(gift, gift_kind))
    df = df.withColumn('gift_id', F.when(gift, p.string(field(extras, 'giftId'), 'extras.giftId', required=True, when=gift)))
    df = df.withColumn('recipient_user_id', F.when(gift, p.string(field(extras, 'receiverUserIdHash'), 'extras.receiverUserIdHash', required=True, when=gift)))
    df = df.withColumn('gift_tier_no', F.when(gift, p.number(field(extras, 'giftTierNo'), 'extras.giftTierNo', minimum=1, when=gift)).cast('int'))

    df = df.withColumn('subscription_months', F.when(subscription, p.number(field(extras, 'month'), 'extras.month', required=True, when=subscription)).cast('int'))
    df = df.withColumn('subscription_tier_no', F.when(subscription, p.number(field(extras, 'tierNo'), 'extras.tierNo', minimum=1, when=subscription)).cast('int'))
    df = df.withColumn('subscription_tier_name', F.when(subscription, p.string(field(extras, 'tierName'), 'extras.tierName', when=subscription)))
    p.flag(subscription & (F.col('subscription_months') == 0), 'ZERO_SUBSCRIPTION_MONTHS')

    unclassified = table == 'unclassified_events'
    df = df.withColumn('classification_reason', F.when(unclassified, F.when((cmd == 93102) & (code == 30), 'SYSTEM_MESSAGE_NOT_MODELED')
                                                       .otherwise('UNSUPPORTED_EVENT_COMBINATION')))
    df = df.withColumn('observed_donation_type', F.when(unclassified, p.string(field(extras, 'donationType'), 'extras.donationType', when=unclassified)))
    df = df.withColumn('observed_gift_type', F.when(unclassified, p.string(field(extras, 'giftType'), 'extras.giftType', when=unclassified)))
    p.flag(unclassified, 'UNCLASSIFIED_EVENT')

    df = df.withColumn('chat_channel_id', p.string(field(item, 'cid'), 'bdy.cid'))
    df = df.withColumn('message_status', p.string(field(item, 'msgStatusType'), 'bdy.msgStatusType'))
    p.flag(F.col('message_status').isNull(), 'MISSING_MESSAGE_STATUS')
    df = df.withColumn('message', p.string(field(item, 'msg'), 'bdy.msg', required=table == 'chat_messages', preserve_empty=True))
    p.flag(F.col('message') == '', 'EMPTY_MESSAGE')
    actor = p.string(uid, 'bdy.uid')
    anonymous = F.coalesce(is_boolean & F.try_variant_get(anonymous_value, '$', 'boolean'), F.lit(False)) | F.coalesce(actor == 'anonymous', F.lit(False))
    df = df.withColumn('_anonymous', anonymous).withColumn('actor_user_id', F.when(~anonymous, actor))
    p.flag(~F.col('_anonymous') & F.col('actor_user_id').isNull(), 'MISSING_ACTOR_USER_ID')
    profile = F.when(~F.col('_anonymous'), profile)
    owner = string_of(field(profile, 'userIdHash'))
    mismatch = F.col('actor_user_id').isNotNull() & owner.isNotNull() & ~is_blank(owner) & (owner != F.col('actor_user_id'))
    df = df.withColumn('_mismatch', F.coalesce(mismatch, F.lit(False)))
    p.flag(F.col('_mismatch'), 'PROFILE_USER_MISMATCH')
    df = df.withColumn('_owner_profile', F.when(~F.col('_mismatch'), profile))
    profile = F.col('_owner_profile')
    df = df.withColumn('nickname', p.string(field(profile, 'nickname'), 'profile.nickname'))
    df = df.withColumn('actor_user_role', p.string(field(profile, 'userRoleCode'), 'profile.userRoleCode'))
    streaming = field(profile, 'streamingProperty')
    p.flag(present(streaming) & ~is_object(streaming), 'INVALID_OPTIONAL_FIELD')
    sub = F.when(is_object(streaming), field(streaming, 'subscription'))
    p.flag(present(sub) & ~is_object(sub), 'INVALID_OPTIONAL_FIELD')
    sub = F.when(is_object(sub), sub)
    df = df.withColumn('sender_subscription_months', p.number(field(sub, 'accumulativeMonth'), 'profile.subscription.accumulativeMonth').cast('int'))
    df = df.withColumn('sender_subscription_tier', p.number(field(sub, 'tier'), 'profile.subscription.tier', minimum=1).cast('int'))
    streaming_channel = field(extras, 'streamingChannelId')
    same_channel = (F.coalesce(string_of(streaming_channel) == F.col('channel_id'), F.lit(False))
                    | (F.coalesce(F.is_variant_null(streaming_channel), F.lit(False)) & F.col('channel_id').isNull()))
    p.fail(streaming_channel.isNotNull() & ~same_channel, 'extras.streamingChannelId', 'CHANNEL_ID_CONFLICT')

    df = df.withColumn('_errors', F.array_compact(F.array(*p.errors)))
    df = df.withColumn('quality_flags', F.array_sort(F.array_distinct(F.array_compact(F.array(*p.flags)))))
    errors = F.when(F.col('_early').isNotNull(), F.array(F.col('_early'))).otherwise(F.col('_errors'))
    kind_column = (F.when(F.col('metric').isNotNull(), 'metric')
                   .when(F.size(errors) > 0, 'quarantine').otherwise('row'))
    return df.select(
        kind_column.alias('kind'), 'metric', 'topic', 'partition', 'offset', F.col('body_index').cast('int').alias('body_index'),
        'source_cmd', 'cmd', 'message_type_code', 'table', 'channel_id', F.col('_channel').alias('frame_channel_id'),
        'msg_time_ms', 'event_time', 'event_date_kst', 'kafka_timestamp', 'bronze_ingested_at',
        F.lit(now).cast('timestamp').alias('silver_processed_at'), 'quality_flags',
        'chat_channel_id', 'message_status', 'actor_user_id', 'nickname', 'actor_user_role', 'message',
        'sender_subscription_months', 'sender_subscription_tier',
        'donation_id', 'donation_type', 'pay_type', 'pay_amount', 'is_anonymous',
        'gift_id', 'gift_type', 'recipient_user_id', 'gift_tier_no',
        'subscription_months', 'subscription_tier_no', 'subscription_tier_name',
        'classification_reason', 'observed_donation_type', 'observed_gift_type',
        F.array_sort(F.array_distinct(F.transform(errors, lambda e: e['code']))).alias('error_codes'),
        F.array_sort(F.array_distinct(F.transform(errors, lambda e: e['field']))).alias('error_fields'),
        F.col('_early').isNotNull().alias('early'))


def conform(df, schema):
    """Select `schema`'s columns in order, casting only real type differences.

    Nested nullability is left alone: Spark refuses to cast a nullable array element to a non-null one, and Delta
    does not enforce nested nullability on write. The arrays built here never hold NULL.
    """
    F = _F()
    types = dict(df.dtypes)
    return df.select(*[F.col(f.name) if types[f.name] == f.dataType.simpleString() else F.col(f.name).cast(f.dataType).alias(f.name)
                       for f in schema.fields])


def table_rows(parsed, table):
    """Parsed rows of one Silver table, in that table's schema."""
    F = _F()
    return conform(parsed.filter((F.col('kind') == 'row') & (F.col('table') == table)), schemas()[table])


def parse_quarantine(parsed, now):
    """quarantine() for parse failures: an early failure has no event type, a parse_item failure keeps its table."""
    F = _F()
    channel = F.when(F.col('frame_channel_id').isNotNull() & ~is_blank(F.col('frame_channel_id')), F.col('frame_channel_id'))
    return parsed.filter(F.col('kind') == 'quarantine').select(
        *SOURCE_KEY, channel.alias('channel_id'), 'source_cmd',
        F.when(~F.col('early'), F.col('table')).alias('event_type'),
        'error_codes', 'error_fields', F.lit(PARSER_VERSION).alias('parser_version'),
        F.lit(now).cast('timestamp').alias('detected_at'),
        F.array().cast('array<string>').alias('conflict_keys'),
        F.array().cast(schemas()['quarantine']['related_sources'].dataType).alias('related_sources'))


# --- Reconciliation: silver_store.reconcile on DataFrames. ---

def source_token():
    """encode(['source', *source_key]) exactly: JSON text that also appears in quarantine.conflict_keys."""
    F = _F()
    topic = F.regexp_extract(F.to_json(F.array(F.col('topic'))), r'(?s)^\[(.*)\]$', 1)
    return F.concat(F.lit('["source",'), topic, F.lit(','), F.col('partition').cast('string'), F.lit(','),
                    F.col('offset').cast('string'), F.lit(','), F.col('body_index').cast('string'), F.lit(']'))


def business_token(table):
    F = _F()
    if table not in BUSINESS_KEYS:
        return F.lit(None).cast('string')
    return F.to_json(F.array(F.lit('business'), F.lit(table), *[F.col(k) for k in BUSINESS_KEYS[table]]))


def signature(df, table):
    """reconcile's signature(): every value except processing times; equal text means equal rows."""
    F = _F()
    columns = [c for c in df.columns if c not in EXCLUDED_FROM_SIGNATURE]
    return F.to_json(F.struct(F.lit(table).alias('table'), *columns))


def records(df, table, origin):
    """One node per (origin, table, source key) with its keys, signature and critical values."""
    F = _F()
    critical = F.to_json(F.struct(*CRITICAL[table])) if table in CRITICAL else F.lit(None).cast('string')
    return df.select(
        F.lit(origin).alias('origin'), F.lit(table).alias('table'), *SOURCE_KEY,
        source_token().alias('src_token'), business_token(table).alias('biz_token'),
        signature(df, table).alias('sig'), critical.alias('crit'),
        'channel_id', 'source_cmd')


def stable(df, step):
    """Iterate `step` until the node labels stop changing; each round is a join, so lineage is cut per round."""
    F = _F()
    while True:
        updated = step(df).localCheckpoint()
        changed = updated.join(df.withColumnRenamed('label', 'previous'), ['node']).filter(F.col('label') != F.col('previous'))
        if changed.isEmpty():
            return updated
        df = updated


def components(edges):
    """Connected components of nodes linked by shared keys (reconcile's poisoning walk), as node -> label."""
    F = _F()
    labels = edges.groupBy('node').agg(F.min('token').alias('label')).localCheckpoint()

    def step(current):
        per_token = edges.join(current, 'node').groupBy('token').agg(F.min('label').alias('label'))
        return edges.join(per_token, 'token').groupBy('node').agg(F.min('label').alias('label'))
    return stable(labels, step)


def write_batch(batch, batch_id, output_path, bronze_path=None):
    from datetime import datetime, timezone
    F = _F()
    spark = batch.sparkSession
    if spark.conf.get('spark.sql.session.timeZone') != 'UTC':
        raise ValueError('Silver needs spark.sql.session.timeZone=UTC for event_date_kst')
    # Python's json keeps the last of duplicate keys; VARIANT rejects them unless told the same.
    spark.conf.set('spark.sql.variant.allowDuplicateKeys', 'true')
    validate_bronze_source(batch, bronze_path)
    initialize_tables(spark, output_path)
    now = datetime.now(timezone.utc)
    totals = {'frames': 0, 'inserted': 0, 'removed': 0, 'quarantine_upserts': 0, 'duplicate_events': 0}
    if batch.isEmpty():
        print(f'[spark-silver] {{"batch_id":{batch_id},"status":"completed","frames":0}}', flush=True)
        return totals
    bad = batch.filter(F.col('topic').isNull() | is_blank(F.col('topic')) | F.col('partition').isNull()
                       | ~F.col('partition').between(0, 2**31 - 1) | F.col('offset').isNull() | (F.col('offset') < 0))
    if not bad.isEmpty():
        raise ValueError('Invalid Bronze source coordinates; cannot safely retry this batch')

    # Checkpoints keep each later plan small: without them every lookup below would re-embed the whole parse
    # plan, and analyzing the unions of those copies exhausts the driver heap even for a one-row batch.
    parsed = parse(batch, now).localCheckpoint()
    return _reconcile_and_write(spark, parsed, batch, batch_id, output_path, now, totals)


def _reconcile_and_write(spark, parsed, batch, batch_id, output_path, now, totals):
    F = _F()
    totals['frames'] = batch.count()
    for row in parsed.groupBy('kind', 'metric', 'table').count().collect():
        if row['kind'] == 'metric':
            totals[row['metric']] = totals.get(row['metric'], 0) + row['count']
        else:
            name = row['table'] if row['kind'] == 'row' else 'quarantine'
            totals[f'input:{name}'] = totals.get(f'input:{name}', 0) + row['count']
    for name in (*TABLES, 'quarantine'):
        totals.setdefault(f'input:{name}', 0)

    incoming_rows = {t: table_rows(parsed, t) for t in TABLES}
    incoming_quarantine = parse_quarantine(parsed, now)
    nodes_in = [records(incoming_rows[t], t, 0) for t in TABLES] + [records(incoming_quarantine, 'quarantine', 0)]
    incoming = reduce(lambda a, b: a.unionByName(b), nodes_in).localCheckpoint()

    # Existing rows that share a source key or business key with the batch (load_matches).
    stored = {t: spark.read.format('delta').load(f'{output_path}/{t}') for t in TABLES}
    source_keys = incoming.select(*SOURCE_KEY).distinct()
    business_keys = incoming.filter(F.col('biz_token').isNotNull()).select(F.col('biz_token').alias('_key')).distinct()
    existing_parts = []
    for t in TABLES:
        with_keys = stored[t].withColumn('_key', business_token(t))
        matched = with_keys.join(F.broadcast(source_keys), list(SOURCE_KEY), 'left_semi')
        if t in BUSINESS_KEYS:
            matched = matched.unionByName(with_keys.join(F.broadcast(business_keys), '_key', 'left_semi'))
        existing_parts.append(records(matched.drop('_key').dropDuplicates(list(SOURCE_KEY)), t, 1))
    existing = reduce(lambda a, b: a.unionByName(b), existing_parts)
    all_records = incoming.unionByName(existing).localCheckpoint()
    return _decide(spark, parsed, incoming_rows, incoming_quarantine, incoming, all_records, stored,
                   batch_id, output_path, now, totals)


def _decide(spark, parsed, incoming_rows, incoming_quarantine, incoming, all_records, stored, batch_id, output_path, now, totals):
    import json
    from delta.tables import DeltaTable
    F = _F()
    quarantine_path = f'{output_path}/quarantine'
    prior = spark.read.format('delta').load(quarantine_path)
    candidate_keys = (all_records.select(F.col('src_token').alias('_key'))
                      .unionByName(all_records.filter(F.col('biz_token').isNotNull()).select(F.col('biz_token').alias('_key'))).distinct())
    direct = prior.join(F.broadcast(all_records.filter(F.col('origin') == 0).select(*SOURCE_KEY).distinct()), list(SOURCE_KEY), 'left_semi')
    linked = (prior.withColumn('_key', F.explode('conflict_keys'))
              .join(F.broadcast(candidate_keys), '_key', 'left_semi').drop('_key'))
    prior_matched = direct.unionByName(linked).dropDuplicates([*SOURCE_KEY, 'parser_version'])

    # Seeds: (key, code) pairs that start the poisoning walk.
    normal = F.col('table') != 'quarantine'
    source_conflicts = (all_records.groupBy('src_token')
                        .agg(F.max(normal.cast('int')).alias('has_normal'), F.countDistinct('sig').alias('n'))
                        .filter((F.col('has_normal') == 1) & (F.col('n') > 1))
                        .select(F.col('src_token').alias('token'), F.lit('SOURCE_KEY_CONFLICT').alias('code')))
    business_conflicts = (all_records.filter(F.col('biz_token').isNotNull()).groupBy('biz_token')
                          .agg(F.countDistinct('crit').alias('n')).filter(F.col('n') > 1)
                          .select(F.col('biz_token').alias('token'), F.lit('BUSINESS_KEY_CONFLICT').alias('code')))
    prior_seeds = (prior_matched.select(F.explode(F.array_union(F.array(source_token()), F.col('conflict_keys'))).alias('token'),
                                        F.col('error_codes'))
                   .select('token', F.explode('error_codes').alias('code')))
    seeds = source_conflicts.unionByName(business_conflicts).unionByName(prior_seeds).distinct().localCheckpoint()

    node = F.concat_ws('|', F.col('origin').cast('string'), F.col('table'), F.col('src_token'))
    nodes = all_records.select(node.alias('node'), 'origin', 'table', *SOURCE_KEY, 'src_token', 'biz_token',
                               'channel_id', 'source_cmd').dropDuplicates(['node'])
    blocked = None
    if not seeds.isEmpty():
        edges = (nodes.select('node', F.col('src_token').alias('token'))
                 .unionByName(nodes.filter(F.col('biz_token').isNotNull()).select('node', F.col('biz_token').alias('token'))))
        labels = components(edges)
        token_labels = edges.join(labels, 'node').select('token', 'label').distinct()
        codes = seeds.join(token_labels, 'token').groupBy('label').agg(F.array_sort(F.collect_set('code')).alias('codes'))
        members = nodes.join(labels, 'node')
        summary = (members.groupBy('label').agg(
            F.array_sort(F.collect_set('biz_token')).alias('business_tokens'),
            F.slice(F.array_sort(F.collect_set(F.struct(*SOURCE_KEY))), 1, 2).alias('examples')))
        blocked = members.join(codes, 'label').join(summary, 'label').localCheckpoint()

    # Quarantine records, then combine_quarantine: first record's scalars, arrays unioned, earliest detection.
    q_schema = schemas()['quarantine']
    parts = [incoming_quarantine.withColumn('_order', F.struct(F.lit(0).alias('stage'), F.lit(0).alias('origin'), *SOURCE_KEY))]
    if blocked is not None:
        channel = F.when(F.col('channel_id').isNotNull() & ~is_blank(F.col('channel_id')), F.col('channel_id'))
        parts.append(blocked.select(
            *SOURCE_KEY, channel.alias('channel_id'), 'source_cmd',
            F.when(F.col('table').isin(*TABLES), F.col('table')).alias('event_type'),
            F.col('codes').alias('error_codes'), F.array().cast('array<string>').alias('error_fields'),
            F.lit(PARSER_VERSION).alias('parser_version'), F.lit(now).cast('timestamp').alias('detected_at'),
            F.array_sort(F.array_distinct(F.concat(F.col('business_tokens'), F.array(F.col('src_token'))))).alias('conflict_keys'),
            F.array_sort(F.array_distinct(F.concat(F.col('examples'), F.array(F.struct(*SOURCE_KEY))))).alias('related_sources'),
            F.struct(F.lit(1).alias('stage'), F.col('origin'), *SOURCE_KEY).alias('_order')))
    quarantine_all = reduce(lambda a, b: a.unionByName(b), parts)
    first = F.min_by(F.struct('channel_id', 'source_cmd', 'event_type'), '_order')
    combined = (quarantine_all.groupBy(*SOURCE_KEY, 'parser_version').agg(
        first.alias('_first'), F.min('detected_at').alias('detected_at'),
        *[F.array_sort(F.array_distinct(F.flatten(F.collect_list(k)))).alias(k)
          for k in ('error_codes', 'error_fields', 'conflict_keys', 'related_sources')])
        .select(*SOURCE_KEY, '_first.channel_id', '_first.source_cmd', '_first.event_type', 'error_codes', 'error_fields',
                'parser_version', 'detected_at', 'conflict_keys', 'related_sources')
        ).localCheckpoint()
    combined = conform(combined, q_schema)
    totals['quarantine_upserts'] = combined.count()

    # Survivors: unblocked incoming rows; one per business key (or source key), none when a stored row has it.
    survivors = all_records.filter(normal)
    if blocked is not None:
        survivors = survivors.join(blocked.select('origin', 'table', 'src_token'), ['origin', 'table', 'src_token'], 'left_anti')
    survivors = survivors.withColumn('_group', F.coalesce('biz_token', 'src_token'))
    groups = survivors.groupBy('table', '_group').agg(
        F.max(F.col('origin')).alias('_stored'), F.sum((F.col('origin') == 0).cast('int')).alias('_incoming'),
        F.min_by(F.struct(*SOURCE_KEY), F.when(F.col('origin') == 0, F.struct(*SOURCE_KEY))).alias('_winner'))
    groups = groups.filter(F.col('_incoming') > 0).localCheckpoint()
    duplicate_count = groups.select(F.sum(F.when(F.col('_stored') == 1, F.col('_incoming')).otherwise(F.col('_incoming') - 1))).first()[0]
    totals['duplicate_events'] = int(duplicate_count or 0)
    winners = groups.filter(F.col('_stored') == 0).select('table', *[F.col(f'_winner.{k}').alias(k) for k in SOURCE_KEY])

    # Same order as apply_changes: the durable block first, so a retry finishes the removals.
    if totals['quarantine_upserts']:
        merge_quarantine_frame(spark, output_path, combined)
    for t in TABLES:
        target = DeltaTable.forPath(spark, f'{output_path}/{t}')
        if blocked is not None:
            removed = blocked.filter((F.col('origin') == 1) & (F.col('table') == t)).select(*SOURCE_KEY).distinct().localCheckpoint()
            count = removed.count()
            if count:
                target.alias('t').merge(removed.alias('s'), match_expression(SOURCE_KEY)).whenMatchedDelete().execute()
                totals['removed'] += count
        added = (incoming_rows[t].join(winners.filter(F.col('table') == t).drop('table'), list(SOURCE_KEY), 'left_semi')
                 .dropDuplicates(list(SOURCE_KEY)).localCheckpoint())
        count = added.count()
        if count:
            (target.alias('t').merge(added.alias('s'), match_expression(BUSINESS_KEYS.get(t, SOURCE_KEY)))
             .whenNotMatchedInsertAll().execute())
            totals['inserted'] += count
    print('[spark-silver] ' + json.dumps({'batch_id': batch_id, 'status': 'completed', **totals},
                                         ensure_ascii=False, sort_keys=True, separators=(',', ':')), flush=True)
    return totals
