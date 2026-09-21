import os
import signal
import sys
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path

try:
    from py4j.protocol import Py4JError, Py4JNetworkError
except ModuleNotFoundError:  # pragma: no cover
    class Py4JError(Exception):
        pass

    class Py4JNetworkError(Exception):
        pass


READY_LINE = "SPARK_BRONZE_READY"
DEFAULT_APP_NAME = "chzzk-kafka-raw-to-bronze"
DEFAULT_OUTPUT_PATH = "data/output/chat_bdy_stream"
DEFAULT_DEAD_LETTER_PATH = "data/output/dead_letter/chat_bdy_stream"
DEFAULT_CHECKPOINT_PATH = "data/checkpoints/chat_bdy_stream"
GCS_CONNECTOR_PACKAGE = "com.google.cloud.bigdataoss:gcs-connector:4.0.1:shaded"
GCS_FILESYSTEM_IMPL = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
GCS_ABSTRACT_FILESYSTEM_IMPL = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
GCS_AUTH_TYPE_APPLICATION_DEFAULT = "APPLICATION_DEFAULT"

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from spark.config import derive_dead_letter_path, derive_storage_paths, load_job_config, resolve_properties_path


@dataclass(frozen=True)
class RuntimeSettings:
    spark_app_name: str = DEFAULT_APP_NAME
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "chzzk.events.raw"
    output_path: str = DEFAULT_OUTPUT_PATH
    dead_letter_path: str = DEFAULT_DEAD_LETTER_PATH
    checkpoint_path: str = DEFAULT_CHECKPOINT_PATH
    google_application_credentials: str | None = None
    processing_time: str = "30 seconds"
    auto_compact_min_num_files: int = 4
    auto_compact_max_file_size_bytes: int = 16 * 1024 * 1024
    spark_properties: dict[str, str] = field(default_factory=dict)

    def uses_gcs(self) -> bool:
        return any(
            path.startswith("gs://")
            for path in (self.output_path, self.dead_letter_path, self.checkpoint_path)
        )


def _get_first_env(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return None


def _resolve_storage_paths_from_env() -> tuple[str, str, str]:
    bucket_uri = _get_first_env("SPARK_BUCKET_URI", "GCS_BUCKET_URI")
    explicit_output_path = _get_first_env("SPARK_BRONZE_OUTPUT_PATH", "BRONZE_OUTPUT_PATH")
    explicit_dead_letter_path = _get_first_env("SPARK_BRONZE_DEAD_LETTER_PATH", "DEAD_LETTER_PATH")
    explicit_checkpoint_path = _get_first_env("SPARK_BRONZE_CHECKPOINT_PATH", "CHECKPOINT_PATH")

    derived_paths = derive_storage_paths(bucket_uri) if bucket_uri else {}

    output_path = explicit_output_path or derived_paths.get("bronze_path")
    checkpoint_path = explicit_checkpoint_path or derived_paths.get("checkpoint_path")
    dead_letter_path = explicit_dead_letter_path or derived_paths.get("dead_letter_path")

    if output_path is None:
        raise RuntimeError(
            "SPARK_BUCKET_URI or GCS_BUCKET_URI or SPARK_BRONZE_OUTPUT_PATH or BRONZE_OUTPUT_PATH is required."
        )
    if checkpoint_path is None:
        raise RuntimeError(
            "SPARK_BUCKET_URI or GCS_BUCKET_URI or SPARK_BRONZE_CHECKPOINT_PATH or CHECKPOINT_PATH is required."
        )
    if dead_letter_path is None:
        dead_letter_path = derive_dead_letter_path(output_path)

    return output_path, dead_letter_path, checkpoint_path


def _load_runtime_settings_from_properties(properties_path: str | Path) -> RuntimeSettings:
    job_config = load_job_config(properties_path)
    return RuntimeSettings(
        spark_app_name=job_config.app_name or DEFAULT_APP_NAME,
        kafka_bootstrap_servers=job_config.kafka_bootstrap_servers,
        kafka_topic=job_config.kafka_topic,
        output_path=job_config.bronze_path,
        dead_letter_path=job_config.dead_letter_path,
        checkpoint_path=job_config.checkpoint_path,
        google_application_credentials=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        processing_time=job_config.spark_properties.get("spark.streaming.trigger.interval", "30 seconds"),
        spark_properties=job_config.spark_properties,
    )


def load_runtime_settings(argv: list[str] | None = None) -> RuntimeSettings:
    args = list(argv if argv is not None else sys.argv[1:])
    try:
        properties_path = resolve_properties_path(args)
    except ValueError:
        properties_path = None

    if properties_path is not None:
        return _load_runtime_settings_from_properties(properties_path)

    output_path, dead_letter_path, checkpoint_path = _resolve_storage_paths_from_env()

    return RuntimeSettings(
        spark_app_name=os.environ.get("SPARK_APP_NAME", DEFAULT_APP_NAME),
        kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=os.environ.get("KAFKA_TOPIC", "chzzk.events.raw"),
        output_path=output_path,
        dead_letter_path=dead_letter_path,
        checkpoint_path=checkpoint_path,
        google_application_credentials=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        processing_time=os.environ.get("SPARK_BRONZE_PROCESSING_TIME", "30 seconds"),
    )


def build_schema():
    from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField("svcid", StringType(), True),
            StructField("ver", StringType(), True),
            StructField("cmd", IntegerType(), True),
            StructField("tid", StringType(), True),
            StructField("cid", StringType(), True),
            StructField(
                "bdy",
                ArrayType(
                    StructType(
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
                        ]
                    )
                ),
                True,
            ),
        ]
    )


def create_spark_session(settings: RuntimeSettings | None = None):
    import pyspark
    from delta import configure_spark_with_delta_pip

    resolved_settings = settings or load_runtime_settings()
    builder = pyspark.sql.SparkSession.builder.appName(resolved_settings.spark_app_name)

    for key, value in resolved_settings.spark_properties.items():
        if key.startswith("spark.") and key != "spark.jars.packages":
            builder = builder.config(key, value)

    builder = (
        builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.databricks.delta.autoCompact.minNumFiles", str(resolved_settings.auto_compact_min_num_files))
        .config(
            "spark.databricks.delta.autoCompact.maxFileSize",
            str(resolved_settings.auto_compact_max_file_size_bytes),
        )
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    )
    extra_packages = [f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"]

    if resolved_settings.uses_gcs():
        builder = (
            builder.config("spark.hadoop.fs.gs.impl", GCS_FILESYSTEM_IMPL)
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", GCS_ABSTRACT_FILESYSTEM_IMPL)
            .config("spark.hadoop.fs.gs.auth.type", GCS_AUTH_TYPE_APPLICATION_DEFAULT)
        )
        if resolved_settings.google_application_credentials:
            builder = builder.config(
                "spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS",
                resolved_settings.google_application_credentials,
            )
        extra_packages.append(GCS_CONNECTOR_PACKAGE)

    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_stage_df(spark, settings: RuntimeSettings):
    from pyspark.sql import functions as F

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "latest")
        .load()
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("raw_key"),
            F.col("value").cast("string").alias("raw_json"),
        )
    )


def write_main_and_dead_letter(batch_df, batch_id, *, schema, settings: RuntimeSettings):
    from pyspark.sql import functions as F

    parsed_batch = (
        batch_df.withColumn("parsed", F.from_json(F.col("raw_json"), schema))
        .withColumn("event_date", F.to_date("kafka_timestamp"))
        .withColumn("batch_id", F.lit(batch_id))
        .persist()
    )

    try:
        good_base = (
            parsed_batch.filter(F.col("parsed").isNotNull())
            .filter(F.col("parsed.bdy").isNotNull())
            .filter(F.size(F.col("parsed.bdy")) > 0)
        )
        good_batch = (
            good_base.select(
                "topic",
                "partition",
                "offset",
                "kafka_timestamp",
                "event_date",
                "batch_id",
                "raw_key",
                "raw_json",
                F.col("parsed.svcid").alias("svcid"),
                F.col("parsed.ver").alias("ver"),
                F.col("parsed.cmd").alias("cmd"),
                F.col("parsed.tid").alias("tid"),
                F.col("parsed.cid").alias("cid"),
                F.explode(F.col("parsed.bdy")).alias("body"),
            ).select(
                "topic",
                "partition",
                "offset",
                "kafka_timestamp",
                "event_date",
                "batch_id",
                "raw_json",
                F.col("raw_key").alias("channel_id"),
                "svcid",
                "ver",
                "cmd",
                "tid",
                "cid",
                F.col("body.svcid").alias("body_svcid"),
                F.col("body.cid").alias("body_cid"),
                F.col("body.mbrCnt").alias("mbr_cnt"),
                F.col("body.uid").alias("uid"),
                F.col("body.profile").alias("profile_json"),
                F.col("body.msg").alias("msg"),
                F.col("body.msgTypeCode").alias("msg_type_code"),
                F.col("body.msgStatusType").alias("msg_status_type"),
                F.col("body.extras").alias("extras_json"),
                F.col("body.ctime").alias("ctime"),
                F.col("body.utime").alias("utime"),
                F.col("body.msgTid").alias("msg_tid"),
                F.col("body.cuid").alias("cuid"),
                F.col("body.msgTime").alias("msg_time"),
            )
        )

        dead_json_batch = parsed_batch.filter(F.col("parsed").isNull()).select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "event_date",
            "batch_id",
            "raw_key",
            "raw_json",
            F.lit("json_parse_failed").alias("dead_letter_reason"),
            F.current_timestamp().alias("dead_letter_at"),
        )

        dead_bdy_batch = (
            parsed_batch.filter(F.col("parsed").isNotNull())
            .filter(F.col("parsed.bdy").isNull() | (F.size(F.col("parsed.bdy")) == 0))
            .select(
                "topic",
                "partition",
                "offset",
                "kafka_timestamp",
                "event_date",
                "batch_id",
                "raw_key",
                "raw_json",
                F.lit("bdy_missing_or_empty").alias("dead_letter_reason"),
                F.current_timestamp().alias("dead_letter_at"),
            )
        )

        good_batch.write.format("delta").mode("append").partitionBy("event_date").save(settings.output_path)
        dead_json_batch.write.format("delta").mode("append").partitionBy("event_date").save(settings.dead_letter_path)
        dead_bdy_batch.write.format("delta").mode("append").partitionBy("event_date").save(settings.dead_letter_path)
    finally:
        parsed_batch.unpersist()


def create_streaming_query(spark, settings: RuntimeSettings):
    schema = build_schema()
    stage_df = create_stage_df(spark, settings)
    batch_writer = partial(write_main_and_dead_letter, schema=schema, settings=settings)
    return (
        stage_df.writeStream.foreachBatch(batch_writer)
        .option("checkpointLocation", settings.checkpoint_path)
        .trigger(processingTime=settings.processing_time)
        .start()
    )


def install_signal_handlers():
    shutdown_requested = {"value": False}

    def request_shutdown(_signum=None, _frame=None):
        if shutdown_requested["value"]:
            return
        shutdown_requested["value"] = True
        print("[spark-bronze] Stopping streaming query...", flush=True)

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    return shutdown_requested


def build_stop_runtime(query, spark):
    stopped = False

    def stop_runtime():
        nonlocal stopped
        if stopped:
            return
        stopped = True
        try:
            query.stop()
        except Exception as exc:
            print(f"[spark-bronze] query.stop() failed: {exc}", flush=True)
        try:
            spark.stop()
        except Exception as exc:
            print(f"[spark-bronze] spark.stop() failed: {exc}", flush=True)

    return stop_runtime


def wait_for_query_termination(query, shutdown_requested):
    while True:
        if shutdown_requested["value"]:
            return False
        try:
            terminated = query.awaitTermination(1)
        except (Py4JError, Py4JNetworkError) as exc:
            if shutdown_requested["value"]:
                print(f"[spark-bronze] Ignoring shutdown-time streaming error: {exc}", flush=True)
                return False
            raise
        if terminated:
            return True


def main(argv: list[str] | None = None):
    settings = load_runtime_settings(argv) if argv is not None else load_runtime_settings()
    spark = None
    stop_runtime = None
    shutdown_requested = None
    try:
        spark = create_spark_session(settings)
        query = create_streaming_query(spark, settings)
        stop_runtime = build_stop_runtime(query, spark)
        shutdown_requested = install_signal_handlers()
        print(READY_LINE, flush=True)
        wait_for_query_termination(query, shutdown_requested)
    finally:
        if stop_runtime is not None:
            if shutdown_requested is not None and shutdown_requested["value"]:
                stop_runtime()
            elif spark is not None:
                try:
                    spark.stop()
                except Exception as exc:
                    print(f"[spark-bronze] spark.stop() failed during cleanup: {exc}", flush=True)
        elif spark is not None:
            try:
                spark.stop()
            except Exception as exc:
                print(f"[spark-bronze] spark.stop() failed during cleanup: {exc}", flush=True)


if __name__ == "__main__":
    main()
