from collections.abc import Mapping

from pyspark.sql import SparkSession

DELTA_COORDINATE = "io.delta:delta-spark_2.13:4.0.1"
GCS_CONNECTOR_COORDINATE = "com.google.cloud.bigdataoss:gcs-connector:4.0.1:shaded"

DEFAULT_SPARK_SETTINGS = {
    "spark.master": "local[*]",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    "spark.hadoop.fs.gs.auth.type": "APPLICATION_DEFAULT",
}


def _parse_package_list(package_string: str | None) -> list[str]:
    if not package_string:
        return []
    return [item.strip() for item in package_string.split(",") if item.strip()]


def _dedupe_packages(packages: list[str]) -> list[str]:
    deduped: list[str] = []
    for package in packages:
        if package not in deduped:
            deduped.append(package)
    return deduped


def build_extra_packages(overrides: Mapping[str, str] | None = None) -> list[str]:
    override_packages = _parse_package_list((overrides or {}).get("spark.jars.packages"))
    merged = override_packages + [DELTA_COORDINATE, GCS_CONNECTOR_COORDINATE]
    return _dedupe_packages(merged)


def build_spark_settings(overrides: Mapping[str, str] | None = None) -> dict[str, str]:
    settings = dict(DEFAULT_SPARK_SETTINGS)
    for key, value in (overrides or {}).items():
        if key.startswith("spark.") and key != "spark.jars.packages":
            settings[key] = value
    settings["spark.jars.packages"] = ",".join(build_extra_packages(overrides))
    return settings


def create_spark_session(job_config) -> SparkSession:
    builder = SparkSession.builder.appName(job_config.app_name)
    for key, value in build_spark_settings(job_config.spark_properties).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()

