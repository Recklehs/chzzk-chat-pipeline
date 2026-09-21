import os
import signal
import sys
from dataclasses import dataclass, field
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
DEFAULT_OUTPUT_PATH = "data/output/frames/bronze"
DEFAULT_DEAD_LETTER_PATH = "data/output/frames/dead_letter"
DEFAULT_CHECKPOINT_PATH = "data/output/frames/checkpoint"
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
    kafka_starting_offsets: str = "latest"
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
        kafka_starting_offsets=job_config.kafka_starting_offsets,
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
        kafka_starting_offsets=os.environ.get("KAFKA_STARTING_OFFSETS", "latest"),
        output_path=output_path,
        dead_letter_path=dead_letter_path,
        checkpoint_path=checkpoint_path,
        google_application_credentials=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        processing_time=os.environ.get("SPARK_BRONZE_PROCESSING_TIME", "30 seconds"),
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
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", settings.kafka_starting_offsets)
        .load()
    )


def create_streaming_query(spark, settings: RuntimeSettings):
    from spark.job import transform_raw_messages

    frames = transform_raw_messages(create_stage_df(spark, settings))
    return (
        frames.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("event_date")
        .option("checkpointLocation", settings.checkpoint_path)
        .trigger(processingTime=settings.processing_time)
        .start(settings.output_path)
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
