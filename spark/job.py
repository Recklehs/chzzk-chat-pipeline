from pyspark.sql import functions as F

from spark.session import create_spark_session


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
    """Keep one original frame per row; event-specific parsing belongs in Silver."""
    frames = raw_kafka_df.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("channel_id"),
        F.col("value").cast("string").alias("payload_json"),
        F.current_timestamp().alias("ingested_at"),
        F.to_date("timestamp").alias("event_date"),
    ).withColumn("cmd", F.get_json_object("payload_json", "$.cmd").try_cast("int"))
    # Old Kafka records can still contain connection acknowledgements.
    return frames.filter(F.col("cmd").isNull() | (F.col("cmd") != 10100))


def write_bronze_stream(message_df, job_config):
    return (
        message_df.writeStream
        .format("delta")
        .outputMode("append")
        .partitionBy("event_date")
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

