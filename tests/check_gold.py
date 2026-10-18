"""Real Delta Silver -> Gold check: JAVA_HOME=<JDK17> python tests/check_gold.py."""
import os
import sqlite3
import sys
import time
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

os.environ['TZ']='UTC'
time.tzset()
os.environ['PYSPARK_PYTHON']=sys.executable
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from test_gold import control_db
from test_silver import MS, donation, frame, message

MINUTE=60_000
NOW=datetime(2026,9,22,16,0,tzinfo=timezone.utc)  # MS + 1h


def at(ms, body, cmd=93101, offset=0):
    return frame([body],cmd=cmd,offset=offset,kafka_timestamp_ms=ms,ingested_at_ms=ms+1000)


def paid(ms, amount, id, **extras):
    body=donation(amount,id=id,**extras)
    body['msgTime']=ms
    return at(ms,body,cmd=93102)


def utc(ms):
    return datetime(1970,1,1)+timedelta(milliseconds=ms)


def main():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from spark import silver_store
    from spark.silver_to_gold import GoldSettings, create_query, refresh, run
    builder=(SparkSession.builder.master('local[2]').appName('check-gold')
             .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2')
             .config('spark.sql.session.timeZone','UTC')
             .config('spark.databricks.delta.snapshotPartitions','2')
             .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
             .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog'))
    spark=configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    schema='topic string, partition int, offset long, channel_id string, payload_json string, kafka_timestamp_ms long, ingested_at_ms long'
    def df(records):return spark.createDataFrame([{**r,'offset':i} for i,r in enumerate(records,len(seen))],schema)
    seen=[]
    def silver(records,batch_id):
        silver_store.write_batch(df(records),batch_id,silver_path)
        seen.extend(records)
    try:
        with TemporaryDirectory() as root:
            silver_path,gold_path=root+'/silver',root+'/gold'
            # Session 1 starts at 23:59:30 KST on 9/22, mid-minute, and has a 19-minute silence.
            # Session 2 is ongoing without data; session 3 on another channel has an event every minute.
            db=control_db(root+'/control.db',[
                (1,'channel','one','2026-09-22T23:59:30+09:00','2026-09-23T00:25:00+09:00','BROADCAST_ENDED'),
                (2,'channel','one','2026-09-23T00:30:00+09:00',None,None),
                (3,'other','two','2026-09-23T00:00:00+09:00','2026-09-23T00:05:30+09:00','BROADCAST_ENDED')])
            first=[at(MS-40_000,message(msgTime=MS-40_000,uid='u0',profile=None)),
                   at(MS-10_000,message(msgTime=MS-10_000,uid='u1',profile=None)),
                   at(MS+5_000,message(msgTime=MS+5_000,uid='u1',profile=None,msgStatusType='CBOTBLIND')),
                   at(MS+20_000,message(msgTime=MS+20_000,uid='u2',profile=None)),
                   paid(MS+30_000,1000,'d1'),
                   paid(MS+40_000,500,'d2',payType='COIN'),
                   at(MS+20*MINUTE,message(msgTime=MS+20*MINUTE,uid='u1',profile=None)),
                   *[frame([message(msgTime=MS+k*MINUTE+1000,uid='u9',profile=None)],channel_id='other',
                           kafka_timestamp_ms=MS+k*MINUTE+1000) for k in range(6)]]
            silver(first,0)
            manifest=run(spark,silver_path,gold_path,db,now=NOW)
            assert set(manifest['silver_versions'])=={'chat_messages','donations','subscription_notifications','subscription_gifts'}
            def table(name):return spark.read.format('delta').load(f'{gold_path}/{name}')
            minutes={(r.channel_id,r.minute_start,r.session_id):r for r in table('channel_minute_activity').collect()}
            outside=minutes[('channel',utc(MS-MINUTE),None)]
            assert outside.chat_count==1 and outside.broadcast_date_kst is None, 'event before session start was assigned'
            first_minute=minutes[('channel',utc(MS-MINUTE),1)]
            assert first_minute.chat_count==1 and first_minute.broadcast_date_kst==date(2026,9,22), first_minute
            busy=minutes[('channel',utc(MS),1)]
            assert (busy.chat_count,busy.blinded_chat_count,busy.unique_chatters)==(2,1,2), busy
            assert (busy.donation_count,busy.anonymous_donation_count,busy.donation_amount_currency,busy.other_pay_type_donation_count)==(2,2,1000,1), busy
            sessions={r.session_id:r for r in table('broadcast_sessions').collect()}
            one,two,three=sessions[1],sessions[2],sessions[3]
            assert (one.chat_count,one.unique_chatters,one.donation_amount_currency)==(4,2,1000), one
            assert one.data_status=='CHECK_GAP' and 19<one.max_gap_minutes<20, one
            assert (one.peak_minute_start,one.peak_minute_chat_count,one.active_minutes)==(utc(MS),2,3), one
            assert one.broadcast_date_kst==date(2026,9,22)
            assert two.data_status=='NO_DATA' and two.ended_at is None and two.duration_minutes==30, two
            assert three.data_status=='OK' and three.chat_count==6 and three.unique_chatters==1, three
            print('Gold session boundaries, metric definitions and data status passed.',flush=True)
            def snapshot(path):
                return {name:sorted(map(repr,spark.read.format('delta').load(f'{path}/{name}').collect()))
                        for name in ('channel_minute_activity','broadcast_sessions')}
            references=iter(range(100))
            def matches_full(label):
                reference=f'{root}/reference-{next(references)}'
                run(spark,silver_path,reference,db,now=NOW)
                assert snapshot(gold_path)==snapshot(reference), f'incremental Gold differs from a full recompute after {label}'
            # The stream's first batch is the whole Silver snapshot; later batches carry only new rows.
            settings=GoldSettings(SimpleNamespace(output_path=silver_path),gold_path,root+'/gold_checkpoint',db,'1 second')
            query=create_query(spark,settings,lambda batch:refresh(spark,silver_path,gold_path,db,events=batch,now=NOW))
            try:
                query.processAllAvailable()
                matches_full('the first batch')
                # A late chat for ended session 1, one more chat in session 3, a chat between sessions 1 and 2,
                # and a chat in ongoing session 2 that a later Collector end time will leave outside the session.
                silver([at(MS+5*MINUTE,message(msgTime=MS+5*MINUTE,uid='u3',profile=None)),
                        at(MS+45*MINUTE,message(msgTime=MS+45*MINUTE,uid='u5',profile=None)),
                        frame([message(msgTime=MS+2*MINUTE+5000,uid='u8',profile=None)],channel_id='other',
                              kafka_timestamp_ms=MS+2*MINUTE+5000),
                        at(MS+27*MINUTE,message(msgTime=MS+27*MINUTE,uid='u4',profile=None))],1)
                query.processAllAvailable()
                matches_full('late, cross-channel and session-less events')
                print('Gold stream refresh of touched sessions matches a full recompute.',flush=True)
                # Session 2 ends at 00:40 in the Collector record with no new event; the timer path picks it up.
                # Its 00:45 chat now falls outside every session and must move to a session-less row, not vanish.
                with closing(sqlite3.connect(db)) as con:
                    con.execute("UPDATE monitoring_sessions SET ended_at='2026-09-23T00:40:00+09:00', stop_reason='BROADCAST_ENDED' WHERE id=2")
                    con.commit()
                refresh(spark,silver_path,gold_path,db,now=NOW)
                two=table('broadcast_sessions').filter('session_id = 2').first()
                assert two.ended_at is not None and two.duration_minutes==10 and two.data_status=='NO_DATA', two
                matches_full('a session end that leaves an event outside the session')
                # A session recorded later covers the 00:27 chat that was session-less; it moves, not doubles.
                with closing(sqlite3.connect(db)) as con:
                    con.execute("INSERT INTO monitoring_sessions VALUES (4,'channel','one','2026-09-23T00:26:00+09:00','2026-09-23T00:28:00+09:00','BROADCAST_ENDED')")
                    con.commit()
                refresh(spark,silver_path,gold_path,db,now=NOW)
                assert table('broadcast_sessions').filter('session_id = 4').first().chat_count==1
                matches_full('a new session covering a session-less event')
                # A corrected channel moves session 3 away from channel 'other'; its chats there become session-less.
                with closing(sqlite3.connect(db)) as con:
                    con.execute("UPDATE monitoring_sessions SET channel_id='third' WHERE id=3")
                    con.commit()
                refresh(spark,silver_path,gold_path,db,now=NOW)
                assert table('broadcast_sessions').filter('session_id = 3').first().data_status=='NO_DATA'
                matches_full('a corrected session channel')
                print('Gold timer refresh closes a session that received no events.',flush=True)
                # A Silver business-key conflict deletes d1. The stream skips that commit instead of failing,
                # so ended session 1 keeps the stale donation until --full reconciles it.
                silver([paid(MS+30_000,2000,'d1')],2)
                query.processAllAvailable()
                assert query.exception() is None
                one=table('broadcast_sessions').filter('session_id = 1').first()
                assert (one.donation_count,one.donation_amount_currency)==(2,1000), one
            finally:
                query.stop()
            run(spark,silver_path,gold_path,db,now=NOW)
            one=table('broadcast_sessions').filter('session_id = 1').first()
            assert (one.donation_count,one.donation_amount_currency)==(1,0), one
            assert table('channel_minute_activity').groupBy('channel_id','minute_start','session_id').count().filter('count > 1').count()==0
            print('Silver deletes do not stop the stream, and --full reconciles them without duplicate rows.',flush=True)
    finally:spark.stop()


if __name__=='__main__':main()
