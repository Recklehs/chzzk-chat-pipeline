"""Run with a Spark-compatible JAVA_HOME: python tests/check_bronze_schema.py."""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark.job import transform_raw_messages
from spark.kafka_raw_to_bronze import RuntimeSettings, create_streaming_query


def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("check-bronze-schema")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        with TemporaryDirectory() as directory:
            settings = RuntimeSettings(
                output_path=f"{directory}/bronze",
                dead_letter_path=f"{directory}/dead_letter",
                checkpoint_path=f"{directory}/checkpoint",
                processing_time="1 second",
            )
            payloads = [
                '{ "cmd":93101, "bdy":[{"msg":"hello"},{"msg":"world"}], "futureField":true }',
                '{"cmd":94008,"bdy":{"blindType":"CBOTBLIND","message":null}}',
                '{"cmd":99999,"bdy":{"future":"value"}}',
                '{"cmd":93101,"bdy":[]}',
                '{}',
                '{"cmd":"not-an-integer","bdy":null}',
                '{broken json',
                '{"cmd":2147483648}',
                '{"cmd":10100,"retCode":0,"bdy":{"sid":"old-kafka-session"}}',
            ]
            batch = spark.createDataFrame(
                [("chat", 0, offset, datetime(2026, 9, 19), "channel-1", payload)
                 for offset, payload in enumerate(payloads)],
                "topic string, partition int, offset long, timestamp timestamp, key string, value string",
            )
            transformed = transform_raw_messages(batch)
            expected_columns = {
                "channel_id", "cmd", "payload_json", "topic", "partition", "offset",
                "kafka_timestamp", "ingested_at", "event_date",
            }
            assert set(transformed.columns) == expected_columns, transformed.columns
            rows = transformed.orderBy("offset").collect()
            assert [row.payload_json for row in rows] == payloads[:-1]
            assert [row.cmd for row in rows] == [93101, 94008, 99999, 93101, None, None, None, None]
            assert all(row.channel_id == "channel-1" and row.ingested_at is not None for row in rows)
            assert json.loads(rows[0].payload_json)["bdy"] == [{"msg": "hello"}, {"msg": "world"}]

            # Exercise the actual streaming sink, using local input instead of a Kafka broker.
            input_path = f"{directory}/input"
            batch.write.json(input_path)
            source = spark.readStream.schema(batch.schema).json(input_path)
            with patch("spark.kafka_raw_to_bronze.create_stage_df", return_value=source):
                for _ in range(2):
                    query = create_streaming_query(spark, settings)
                    try:
                        query.processAllAvailable()
                    finally:
                        query.stop()
            stored = spark.read.format("delta").load(settings.output_path).orderBy("offset").collect()
            assert [row.payload_json for row in stored] == payloads[:-1]
            assert not Path(settings.dead_letter_path).exists()
    finally:
        spark.stop()
    print("Bronze frame check passed: payloads preserved, 10100 excluded, checkpoint restart without duplicates.")


if __name__ == "__main__":
    main()
