"""Strict, token-free Bronze frame normalization; independent of Spark."""
import json
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

PARSER_VERSION = 'silver-v3.1'
SOURCE_KEY = ('topic', 'partition', 'offset', 'body_index')
TABLES = ('chat_messages', 'donations', 'subscription_gifts',
          'subscription_notifications', 'unclassified_events')
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
KST = ZoneInfo('Asia/Seoul')


def integer(value, bits=64):
    if isinstance(value, str) and re.fullmatch(r'[+-]?[0-9]+', value):
        try:
            value = int(value)
        except ValueError:
            return None
    if type(value) is int and -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
        return value
    return None


def text(value):
    return value if isinstance(value, str) and value.strip() else None


def object_value(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, RecursionError):
            return None, True
    return (value, False) if isinstance(value, dict) else (None, value is not None)


def source_key(row):
    return tuple(row[k] for k in SOURCE_KEY)


def quarantine(row, codes, fields, now, conflict_keys=(), related_sources=()):
    return {'table': 'quarantine', **{k: row.get(k) for k in SOURCE_KEY},
            'channel_id': text(row.get('channel_id')), 'source_cmd': row.get('source_cmd'),
            'event_type': row.get('table') if row.get('table') in TABLES else None,
            'error_codes': sorted(set(codes)), 'error_fields': sorted(set(fields)),
            'parser_version': PARSER_VERSION, 'detected_at': now,
            'conflict_keys': sorted(set(conflict_keys)),
            'related_sources': [dict(zip(SOURCE_KEY, k)) for k in sorted(set(related_sources))]}


def parse_frame(frame, processed_at):
    """Return normalized items, quarantined items, or internal metric records."""
    if hasattr(frame, 'asDict'):
        frame = frame.asDict(recursive=True)
    if not text(frame.get('topic')) or any(
        type(frame.get(k)) is not int or not 0 <= frame[k] < 2**bits
        for k, bits in (('partition', 31), ('offset', 63))
    ):
        raise ValueError('Invalid Bronze source coordinates; cannot safely retry this batch')
    base = {k: frame.get(k) for k in ('topic', 'partition', 'offset', 'channel_id')}
    base.update(body_index=-1, source_cmd=None)
    try:
        payload = json.loads(frame['payload_json'])
    except (KeyError, TypeError, ValueError, RecursionError):
        return [quarantine(base, ['INVALID_JSON'], ['payload_json'], processed_at)]
    if not isinstance(payload, dict):
        return [quarantine(base, ['INVALID_FRAME'], ['payload_json'], processed_at)]
    source_cmd = integer(payload.get('cmd'), 32)
    base['source_cmd'] = source_cmd
    if source_cmd is None:
        return [quarantine(base, ['INVALID_CMD'], ['cmd'], processed_at)]
    if source_cmd not in (93101, 93102):
        return [{'table': '_metric', 'metric': f'out_of_scope:{source_cmd}'}]
    body = payload.get('bdy')
    if not isinstance(body, (dict, list)):
        return [quarantine(base, ['INVALID_BODY'], ['bdy'], processed_at)]
    if body == []:
        return [{'table': '_metric', 'metric': 'empty_body_frames'}]
    result = []
    for index, item in enumerate(body if isinstance(body, list) else [body]):
        row = {**base, 'body_index': index}
        if not isinstance(item, dict):
            result.append(quarantine(row, ['INVALID_BODY_ITEM'], [f'bdy[{index}]'], processed_at))
            continue
        result.append(parse_item(row, item, frame, processed_at))
    return result


def parse_item(row, item, frame, now):
    errors, fields, flags = set(), set(), set()

    def fail(field, code='INVALID_REQUIRED_FIELD'):
        errors.add(code)
        fields.add(field)

    def string(value, field, required=False, preserve_empty=False):
        valid = isinstance(value, str) and (preserve_empty or bool(value.strip()))
        if not valid:
            if required:
                fail(field)
            elif value is not None:
                flags.add('INVALID_OPTIONAL_FIELD')
            return None
        return value

    def number(value, field, bits=32, minimum=0, required=False):
        n = integer(value, bits)
        if n is None or n < minimum:
            if required:
                fail(field)
            elif value is not None:
                flags.add('INVALID_OPTIONAL_FIELD')
            return None
        return n

    cmd = integer(item['cmd'], 32) if 'cmd' in item else row['source_cmd']
    if cmd is None:
        return quarantine(row, ['INVALID_CMD'], ['bdy.cmd'], now)
    if cmd not in (93101, 93102):
        return {'table': '_metric', 'metric': f'out_of_scope:{cmd}'}
    code = number(item.get('msgTypeCode'), 'bdy.msgTypeCode', minimum=-(2**31), required=True)
    row.update(cmd=cmd, message_type_code=code, table='unclassified_events', silver_processed_at=now)
    row['channel_id'] = string(row['channel_id'], 'channel_id', required=True)
    row['msg_time_ms'] = number(item.get('msgTime'), 'bdy.msgTime', bits=64, required=True)
    for target, field in (('kafka_timestamp', 'kafka_timestamp_ms'), ('bronze_ingested_at', 'ingested_at_ms'),
                          ('event_time', 'msg_time_ms')):
        microseconds = field != 'msg_time_ms' and field.replace('_ms', '_us') in frame
        value = row.get(field) if field == 'msg_time_ms' else frame.get(field.replace('_ms', '_us') if microseconds else field)
        try:
            if integer(value) is None:
                raise ValueError
            row[target] = EPOCH + timedelta(microseconds=value if microseconds else value * 1000)
        except (ValueError, OverflowError, TypeError):
            row[target] = None
            fail(field)
    if row['event_time'] is not None:
        try:
            row['event_date_kst'] = row['event_time'].astimezone(KST).date()
        except (ValueError, OverflowError):
            fail('bdy.msgTime')
    if row['event_time'] and row['kafka_timestamp'] and row['event_time'] - row['kafka_timestamp'] > timedelta(minutes=5):
        fail('bdy.msgTime', 'EVENT_TIME_TOO_FAR_IN_FUTURE')
    profile, bad_profile = object_value(item.get('profile'))
    extras, bad_extras = object_value(item.get('extras'))
    if bad_profile:
        flags.add('INVALID_OPTIONAL_PROFILE')
    profile = profile or {}
    if cmd == 93102 and code in (10, 11, 12) and extras is None:
        fail('bdy.extras')
    elif bad_extras:
        flags.add('INVALID_OPTIONAL_EXTRAS')
    extras = extras or {}
    if cmd == 93101 and code == 1:
        row['table'] = 'chat_messages'
    elif cmd == 93102 and code == 10:
        discriminator = string(extras.get('donationType'), 'extras.donationType', required=True)
        if discriminator == 'CHAT':
            row['table'] = 'donations'
            row['donation_type'] = discriminator
            row['donation_id'] = string(extras.get('donationId'), 'extras.donationId', required=True)
            row['pay_type'] = string(extras.get('payType'), 'extras.payType', required=True)
            row['pay_amount'] = number(extras.get('payAmount'), 'extras.payAmount', bits=64, required=True)
            row['is_anonymous'] = extras.get('isAnonymous')
            if type(row['is_anonymous']) is not bool:
                fail('extras.isAnonymous')
            if row['pay_amount'] == 0:
                flags.add('ZERO_PAY_AMOUNT')
            if row['pay_type'] and row['pay_type'] != 'CURRENCY':
                flags.add('UNRECOGNIZED_PAY_TYPE')
            if row['is_anonymous'] is False and item.get('uid') == 'anonymous':
                fail('bdy.uid', 'ANONYMITY_CONFLICT')
    elif cmd == 93102 and code == 12:
        discriminator = string(extras.get('giftType'), 'extras.giftType', required=True)
        if discriminator == 'SUBSCRIPTION_GIFT_RECEIVER':
            row.update(table='subscription_gifts', gift_type=discriminator)
            row['gift_id'] = string(extras.get('giftId'), 'extras.giftId', required=True)
            row['recipient_user_id'] = string(extras.get('receiverUserIdHash'), 'extras.receiverUserIdHash', required=True)
            row['gift_tier_no'] = number(extras.get('giftTierNo'), 'extras.giftTierNo', minimum=1)
    elif cmd == 93102 and code == 11:
        row['table'] = 'subscription_notifications'
        row['subscription_months'] = number(extras.get('month'), 'extras.month', required=True)
        row['subscription_tier_no'] = number(extras.get('tierNo'), 'extras.tierNo', minimum=1)
        row['subscription_tier_name'] = string(extras.get('tierName'), 'extras.tierName')
        if row['subscription_months'] == 0:
            flags.add('ZERO_SUBSCRIPTION_MONTHS')
    if row['table'] == 'unclassified_events':
        row['classification_reason'] = 'SYSTEM_MESSAGE_NOT_MODELED' if cmd == 93102 and code == 30 else 'UNSUPPORTED_EVENT_COMBINATION'
        row['observed_donation_type'] = string(extras.get('donationType'), 'extras.donationType')
        row['observed_gift_type'] = string(extras.get('giftType'), 'extras.giftType')
        flags.add('UNCLASSIFIED_EVENT')
    row['chat_channel_id'] = string(item.get('cid'), 'bdy.cid')
    row['message_status'] = string(item.get('msgStatusType'), 'bdy.msgStatusType')
    if row['message_status'] is None:
        flags.add('MISSING_MESSAGE_STATUS')
    row['message'] = string(item.get('msg'), 'bdy.msg', required=row['table'] == 'chat_messages', preserve_empty=True)
    if row['message'] == '':
        flags.add('EMPTY_MESSAGE')
    row['actor_user_id'] = string(item.get('uid'), 'bdy.uid')
    anonymous = extras.get('isAnonymous') is True or row['actor_user_id'] == 'anonymous'
    if anonymous:
        row['actor_user_id'] = None
        profile = {}
    elif row['actor_user_id'] is None:
        flags.add('MISSING_ACTOR_USER_ID')
    if row['actor_user_id'] and text(profile.get('userIdHash')) and profile['userIdHash'] != row['actor_user_id']:
        profile = {}
        flags.add('PROFILE_USER_MISMATCH')
    row['nickname'] = string(profile.get('nickname'), 'profile.nickname')
    row['actor_user_role'] = string(profile.get('userRoleCode'), 'profile.userRoleCode')
    streaming = profile.get('streamingProperty')
    if streaming is not None and not isinstance(streaming, dict):
        flags.add('INVALID_OPTIONAL_FIELD')
    subscription = streaming.get('subscription') if isinstance(streaming, dict) else None
    if subscription is not None and not isinstance(subscription, dict):
        flags.add('INVALID_OPTIONAL_FIELD')
    subscription = subscription if isinstance(subscription, dict) else {}
    row['sender_subscription_months'] = number(subscription.get('accumulativeMonth'), 'profile.subscription.accumulativeMonth')
    row['sender_subscription_tier'] = number(subscription.get('tier'), 'profile.subscription.tier', minimum=1)
    if 'streamingChannelId' in extras and extras['streamingChannelId'] != row['channel_id']:
        fail('extras.streamingChannelId', 'CHANNEL_ID_CONFLICT')
    if errors:
        return quarantine(row, errors, fields, now)
    row['quality_flags'] = sorted(flags)
    return row
