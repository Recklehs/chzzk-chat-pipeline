from spark.session import (
    DELTA_COORDINATE,
    GCS_CONNECTOR_COORDINATE,
    build_extra_packages,
    build_spark_settings,
)


def test_build_spark_settings_includes_required_defaults():
    settings = build_spark_settings({})

    assert settings["spark.master"] == "local[*]"
    assert settings["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    assert settings["spark.sql.catalog.spark_catalog"] == "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    assert settings["spark.hadoop.fs.gs.auth.type"] == "APPLICATION_DEFAULT"


def test_build_extra_packages_merges_required_coordinates_without_duplicates():
    packages = build_extra_packages(
        {
            "spark.jars.packages": ",".join(
                [
                    GCS_CONNECTOR_COORDINATE,
                    "org.example:demo:1.0.0",
                ]
            )
        }
    )

    assert packages[0] == GCS_CONNECTOR_COORDINATE
    assert "org.example:demo:1.0.0" in packages
    assert packages.count(GCS_CONNECTOR_COORDINATE) == 1
    assert DELTA_COORDINATE in packages


def test_build_spark_settings_preserves_explicit_overrides():
    settings = build_spark_settings(
        {
            "spark.master": "local[2]",
            "spark.databricks.delta.autoCompact.enabled": "false",
        }
    )

    assert settings["spark.master"] == "local[2]"
    assert settings["spark.databricks.delta.autoCompact.enabled"] == "false"
