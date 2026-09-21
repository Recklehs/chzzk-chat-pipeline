from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType, StructField, StructType

from spark.session import create_spark_session

BODY_ITEM_SCHEMA = StructType(
    [
        StructField("svcid", StringType(), True),
        StructField("cid", StringType(), True),
        StructField("mbrCnt", IntegerType(), True),
        StructField("uid", StringType(), True),
        StructField("profile", StringType(), True),
        StructField("msg", StringType(), True),
        StructField("msgTypeCode", IntegerType(), True),
        StructField("msgStatusType", StringType(), True),
        StructField("extras", StringType(), True),
        StructField("ctime", LongType(), True),
        StructField("utime", LongType(), True),
        StructField("msgTid", StringType(), True),
        StructField("cuid", StringType(), True),
        StructField("msgTime", LongType(), True),
        StructField("cmd", IntegerType(), True),
    ]
)

RAW_MESSAGE_SCHEMA = StructType(
    [
        StructField("svcid", StringType(), True),
        StructField("ver", StringType(), True),
        StructField("cmd", IntegerType(), True),
        StructField("tid", StringType(), True),
        StructField("cid", StringType(), True),
        StructField("bdy", ArrayType(BODY_ITEM_SCHEMA), True),
    ]
)


def build_source_dataframe(spark, job_config):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", job_config.kafka_bootstrap_servers)
        .option("subscribe", job_config.kafka_topic)
        .option("startingOffsets", job_config.kafka_starting_offsets)
        .load()
    )


def transform_raw_messages(raw_kafka_df):
    return raw_kafka_df.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.from_json(F.col("value").cast("string"), RAW_MESSAGE_SCHEMA).alias("parsed"),
        F.current_timestamp().alias("ingested_at"),
    )


def write_bronze_stream(message_df, job_config):
    return (
        message_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", job_config.checkpoint_path)
        .start(job_config.bronze_path)
    )


def run(job_config):
    spark = create_spark_session(job_config)
    spark.sparkContext.setLogLevel("WARN")

    raw_kafka_df = build_source_dataframe(spark, job_config)
    message_df = transform_raw_messages(raw_kafka_df)

    query = write_bronze_stream(message_df, job_config)
    query.awaitTermination()

