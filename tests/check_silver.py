"""Real Delta recovery and streaming restart check: JAVA_HOME=<JDK17> python tests/check_silver.py."""
import os
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

os.environ['TZ']='UTC'
time.tzset()
os.environ['PYSPARK_PYTHON']=sys.executable
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from test_silver import MS, donation, frame, message


def main():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from spark import silver_store
    builder=(SparkSession.builder.master('local[2]').appName('check-silver')
             .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2')
             .config('spark.sql.session.timeZone','UTC')
             .config('spark.databricks.delta.snapshotPartitions','2')
             .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
             .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog'))
    spark=configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    schema='topic string, partition int, offset long, channel_id string, payload_json string, kafka_timestamp_ms long, ingested_at_ms long'
    def df(records):return spark.createDataFrame(records,schema)
    try:
        with TemporaryDirectory() as root:
            # A discarded business duplicate, an empty body and an out-of-scope frame
            # must not hide changed content at an existing Kafka coordinate.
            source_path=root+'/integrity-bronze'
            original=df([frame([donation()],cmd=93102,offset=0),
                         frame([donation()],cmd=93102,offset=1)])
            original.write.format('delta').save(source_path)
            for changed in (frame([donation(id='other')],cmd=93102,offset=1),
                            frame([],offset=1),frame(cmd=94010,offset=1),
                            frame([donation()],cmd=93102,offset=1,channel_id=None),
                            frame([donation()],cmd=93102,offset=1,kafka_timestamp_ms=MS+1)):
                attempt=df([changed])
                attempt.write.format('delta').mode('append').save(source_path)
                rejected_output=root+'/rejected-silver'
                try:silver_store.write_batch(attempt,0,rejected_output,source_path)
                except ValueError as exc:assert 'Bronze source content conflict' in str(exc)
                else:raise AssertionError('changed source was accepted')
                assert not Path(rejected_output).exists(), 'wrote output before input validation'
                original.write.format('delta').mode('overwrite').save(source_path)
            reingested=original.withColumn('ingested_at_ms',original.ingested_at_ms+1000)
            reingested.write.format('delta').mode('append').save(source_path)
            silver_store.validate_bronze_source(reingested,source_path)
            print('Bronze source integrity guard passed before any Silver writes.',flush=True)
            output=root+'/silver'
            first=df([frame([message(),None,message()],offset=0),
                      frame([donation(msg_unused='ignored')],cmd=93102,offset=1),
                      frame([message(11,extras={'month':8})],cmd=93102,offset=2),
                      frame([message(30,profile='{}')],cmd=93102,offset=3),
                      frame([message(12,extras={'giftType':'SUBSCRIPTION_GIFT_RECEIVER','giftId':'gift','receiverUserIdHash':'recipient'})],cmd=93102,offset=4)])
            silver_store.write_batch(first,0,output)
            silver_store.write_batch(first,0,output)
            def rows(table):return spark.read.format('delta').load(output+'/'+table).collect()
            assert len(rows('chat_messages'))==2
            assert len(rows('donations'))==len(rows('subscription_gifts'))==len(rows('subscription_notifications'))==len(rows('unclassified_events'))==1
            assert len(rows('quarantine'))==1
            conflicting=df([frame([donation(2000)],cmd=93102,offset=5)])
            original=silver_store.merge_quarantine
            def write_then_crash(*args,**kwargs):
                original(*args,**kwargs)
                raise RuntimeError('synthetic interruption after quarantine commit')
            with patch.object(silver_store,'merge_quarantine',side_effect=write_then_crash):
                try:silver_store.write_batch(conflicting,1,output)
                except RuntimeError as exc:assert 'synthetic interruption' in str(exc)
                else:raise AssertionError('failure not injected')
            assert len(rows('donations'))==1
            silver_store.write_batch(conflicting,1,output)
            assert not rows('donations')
            assert len(rows('quarantine'))==3
            silver_store.write_batch(df([frame([donation()],cmd=93102,offset=6)]),2,output)
            assert not rows('donations') and len(rows('quarantine'))==4
            print('Delta replay and quarantine-first crash recovery passed.',flush=True)
            from spark.bronze_to_silver import create_query, load_settings, writer_lock
            from pyspark.sql import functions as F
            base=Path(root)/'streaming'
            bronze=base/'bronze'
            def append_bronze(offset):
                source=(df([frame(offset=offset)])
                        .withColumn('kafka_timestamp',F.timestamp_millis('kafka_timestamp_ms'))
                        .withColumn('ingested_at',F.timestamp_millis('ingested_at_ms'))
                        .drop('kafka_timestamp_ms','ingested_at_ms'))
                source.write.format('delta').mode('append').save(str(bronze))
            append_bronze(10)
            props=Path(root)/'runtime.properties'
            props.write_text(f'app.bucket.uri={base}\napp.kafka.bootstrap.servers=localhost:9092\napp.kafka.topic=raw\napp.kafka.startingOffsets=earliest\n')
            settings=load_settings(['--properties-file',str(props),'--available-now'])
            for attempt in range(3):
                if attempt==2:
                    append_bronze(11)
                    append_bronze(12)
                with writer_lock(settings):
                    query=create_query(spark,settings)
                    try:assert query.awaitTermination(120), 'stream did not finish'
                    finally:query.stop()
                    if attempt==2:
                        assert len([p for p in query.recentProgress if p['numInputRows']])==1, 'backlog was limited to one file per trigger'
                count=spark.read.format('delta').load(settings.output_path+'/chat_messages').count()
                assert count==(3 if attempt==2 else 1), count
            print('Real Bronze stream checkpoint restart and new-input continuation passed.',flush=True)
    finally:spark.stop()


if __name__=='__main__':main()
