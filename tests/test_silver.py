"""Silver's observable contracts, using synthetic events without personal data."""
import copy
import json
from datetime import datetime, timezone

import pytest

NOW = datetime(2026, 9, 22, 15, 0, tzinfo=timezone.utc)
MS = 1790089200000  # 2026-09-23 00:00:00 KST


def message(code=1, **changes):
    return {'msgTypeCode': code, 'msgTime': MS, 'uid': 'actor', 'msg': 'hello',
            'msgStatusType': 'NORMAL', 'profile': {'userIdHash': 'actor', 'nickname': 'name',
            'userRoleCode': 'common_user', 'streamingProperty': {'subscription': {'accumulativeMonth': 4, 'tier': 1}}},
            'extras': {}, **changes}


def frame(body=None, cmd=93101, offset=0, **changes):
    return {'topic': 'synthetic', 'partition': 0, 'offset': offset, 'channel_id': 'channel',
            'payload_json': json.dumps({'cmd': cmd, 'bdy': [message()] if body is None else body}),
            'kafka_timestamp_ms': MS, 'ingested_at_ms': MS+1000, **changes}


def parse(value):
    from spark.silver_parser import parse_frame
    return parse_frame(value, NOW)


def donation(amount=1000, id='donation', **extras):
    return message(10, uid='anonymous', profile=None, extras={'donationId': id,
        'donationType': 'CHAT', 'payType': 'CURRENCY', 'payAmount': amount, 'isAnonymous': True, **extras})


def test_frame_expansion_keeps_indices_and_neighbours():
    rows=parse(frame([message(),None,message(cmd='bad'),message()]))
    assert [(r['table'],r['body_index']) for r in rows]==[
        ('chat_messages',0),('quarantine',1),('quarantine',2),('chat_messages',3)]
    assert parse(frame([]))[0]['metric']=='empty_body_frames'
    assert parse(frame(message()))[0]['body_index']==0
    assert parse(frame(cmd=94010))[0]['metric']=='out_of_scope:94010'
    assert parse(frame(payload_json='{broken'))[0]['body_index']==-1
    with pytest.raises(ValueError, match='source'):
        parse(frame(offset=None))


def test_types_keep_their_own_semantics_and_privacy():
    d=parse(frame([donation()],cmd=93102))[0]
    assert d['table']=='donations' and d['pay_amount']==1000
    assert d['actor_user_id'] is None and d['nickname'] is None and d['actor_user_role'] is None
    g=parse(frame([message(12,extras={'giftId':'gift','giftType':'SUBSCRIPTION_GIFT_RECEIVER',
        'receiverUserIdHash':'recipient','giftTierNo':1})],cmd=93102))[0]
    assert g['table']=='subscription_gifts' and g['actor_user_id']!=g['recipient_user_id']
    s=parse(frame([message(11,msg='8',extras={'month':8,'tierNo':1,'tierName':'fan'})],cmd=93102))[0]
    assert s['table']=='subscription_notifications' and s['subscription_months']==8
    c=parse(frame([message(extras={'month':0,'tierNo':0,'extraToken':'secret'},msgStatusType='CBOTBLIND')]))[0]
    assert c['sender_subscription_months']==4 and c['message_status']=='CBOTBLIND'
    assert not any(k in c for k in ('pay_amount','gift_id','extraToken','profile','extras'))
    u=parse(frame([message(30,profile='{}')],cmd=93102))[0]
    assert u['table']=='unclassified_events' and u['classification_reason']=='SYSTEM_MESSAGE_NOT_MODELED'
    assert parse(frame([donation(donationType='FUTURE')],cmd=93102))[0]['table']=='unclassified_events'
    assert parse(frame([message(11,extras={})],cmd=93102))[0]['table']=='quarantine'


@pytest.mark.parametrize('amount',[True,1.5,'1e3','bad',-1,2**63,None])
def test_invalid_amounts_are_quarantined_not_zero_or_unknown(amount):
    r=parse(frame([donation(amount)],cmd=93102))[0]
    assert r['table']=='quarantine' and 'extras.payAmount' in r['error_fields']
    assert 'secret' not in json.dumps(r,default=str)


def test_optional_fields_warn_but_required_conflicts_fail():
    r=parse(frame([message(profile='{broken',extras='{broken')]))[0]
    assert r['table']=='chat_messages'
    assert {'INVALID_OPTIONAL_PROFILE','INVALID_OPTIONAL_EXTRAS'}<=set(r['quality_flags'])
    r=parse(frame([message(profile={'userIdHash':'different','nickname':'wrong','userRoleCode':'manager'})]))[0]
    assert r['actor_user_id']=='actor' and r['nickname'] is None and r['actor_user_role'] is None
    assert 'PROFILE_USER_MISMATCH' in r['quality_flags']
    assert parse(frame([message(extras={'streamingChannelId':'wrong'})]))[0]['table']=='quarantine'
    r=parse(frame([donation('0')],cmd=93102))[0]
    assert r['pay_amount']==0 and 'ZERO_PAY_AMOUNT' in r['quality_flags']
    assert parse(frame([donation(isAnonymous=False)],cmd=93102))[0]['table']=='quarantine'


def test_event_time_uses_milliseconds_and_kst_without_dropping_old_data():
    r=parse(frame([message(msgTime=str(MS))]))[0]
    assert r['event_time']==NOW and str(r['event_date_kst'])=='2026-09-23'
    assert str(parse(frame([message(msgTime=MS-1)]))[0]['event_date_kst'])=='2026-09-22'
    assert parse(frame([message(msgTime=0)]))[0]['msg_time_ms']==0
    assert parse(frame([message(msgTime=MS+300001)]))[0]['table']=='quarantine'
    assert parse(frame([message(msgTime=True)]))[0]['table']=='quarantine'
    assert parse(frame(kafka_timestamp_ms=253402300799999))[0]['table']=='chat_messages'


def resolve(incoming, existing=(), quarantined=()):
    from spark.silver_store import reconcile
    return reconcile(incoming, existing, quarantined, NOW)


def test_business_duplicates_conflicts_and_recipient_grain():
    a=parse(frame([donation()],cmd=93102))[0]
    repeated=parse(frame([donation()],cmd=93102,offset=1))[0]
    out=resolve([a,repeated])
    assert len(out['inserts'])==1 and out['inserts'][0]['offset']==0
    assert resolve([repeated],[a])['inserts']==[]
    changed=parse(frame([donation(2000)],cmd=93102,offset=2))[0]
    out=resolve([changed],[a])
    assert not out['inserts'] and len(out['quarantine'])==2 and out['deletes']==[a]
    assert all('BUSINESS_KEY_CONFLICT' in r['error_codes'] for r in out['quarantine'])
    again=resolve([repeated],[],out['quarantine'])
    assert not again['inserts'] and len(again['quarantine'])==1
    gift=message(12,extras={'giftId':'gift','giftType':'SUBSCRIPTION_GIFT_RECEIVER','receiverUserIdHash':'one'})
    other=copy.deepcopy(gift);other['extras']['receiverUserIdHash']='two'
    out=resolve(parse(frame([gift,other],cmd=93102)))
    assert len(out['inserts'])==2 and not out['quarantine']


def test_source_conflict_across_routes_poisons_all_affected_keys():
    chat=parse(frame())[0]
    donation_row=parse(frame([donation()],cmd=93102))[0]
    out=resolve([donation_row],[chat])
    assert not out['inserts'] and out['deletes']==[chat]
    assert len(out['quarantine'])==1
    assert 'SOURCE_KEY_CONFLICT' in out['quarantine'][0]['error_codes']
    later=parse(frame([donation()],cmd=93102,offset=9))[0]
    assert not resolve([later],[],out['quarantine'])['inserts']
    assert not resolve([chat],[],out['quarantine'])['inserts']


def test_replay_preserves_same_text_and_subscription_notifications():
    c=parse(frame())[0]
    assert resolve([c],[c])['inserts']==[]
    reingested=parse(frame(ingested_at_ms=MS+2000))[0]
    assert resolve([reingested],[c])['quarantine']==[]
    assert resolve([reingested],[c])['inserts']==[]
    c2=parse(frame(offset=1))[0]
    assert len(resolve([c,c2])['inserts'])==2
    s=message(11,extras={'month':8})
    assert len(resolve(parse(frame([s,s],cmd=93102)))['inserts'])==2
    bad=parse(frame([donation(-1)],cmd=93102))[0]
    assert len(resolve([bad,bad])['quarantine'])==1


def test_only_chat_requires_a_message_in_storage_schema():
    from spark.silver_store import schemas
    s=schemas()
    assert not s['chat_messages']['message'].nullable
    assert s['donations']['message'].nullable


def test_bronze_timestamp_microseconds_are_preserved():
    row=parse(frame(kafka_timestamp_us=MS*1000+123,ingested_at_us=MS*1000+456))[0]
    assert row['kafka_timestamp'].microsecond==123
    assert row['bronze_ingested_at'].microsecond==456


def test_runtime_paths_and_lock_prevent_mixed_or_concurrent_writers(tmp_path):
    from spark.bronze_to_silver import load_settings, writer_lock
    props=tmp_path/'local.properties'
    base=tmp_path/'data'
    props.write_text(f'app.bucket.uri={base}\napp.kafka.bootstrap.servers=localhost:9092\napp.kafka.topic=raw\napp.kafka.startingOffsets=earliest\n')
    settings=load_settings(['--properties-file',str(props),'--available-now'])
    assert settings.output_path==str(base/'silver') and settings.checkpoint_path==str(base/'silver_checkpoint')
    with writer_lock(settings):
        with pytest.raises(RuntimeError,match='writer'):
            with writer_lock(settings):pass
    props.write_text(props.read_text()+f'app.silver.checkpoint.path={base}/other_checkpoint\n')
    with pytest.raises(ValueError,match='contract'):
        with writer_lock(load_settings(['--properties-file',str(props)])):pass
    props.write_text(props.read_text()+f'app.silver.path={base}/bronze\n')
    with pytest.raises(ValueError,match='overlap'):
        load_settings(['--properties-file',str(props)])
