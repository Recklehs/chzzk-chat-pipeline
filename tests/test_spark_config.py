from pathlib import Path

import pytest

from spark.config import derive_storage_paths, load_job_config, parse_properties_file, resolve_properties_path


def test_parse_properties_file_skips_comments_and_blank_lines(tmp_path):
    properties_file = tmp_path / "local.properties"
    properties_file.write_text(
        "\n".join(
            [
                "# comment",
                "",
                "spark.master=local[*]",
                "app.kafka.topic=chzzk.events.raw",
            ]
        ),
        encoding="utf-8",
    )

    properties = parse_properties_file(properties_file)

    assert properties == {
        "spark.master": "local[*]",
        "app.kafka.topic": "chzzk.events.raw",
    }


def test_resolve_properties_path_supports_separate_flag_value(tmp_path):
    properties_file = tmp_path / "dev.properties"
    properties_file.write_text("", encoding="utf-8")

    resolved = resolve_properties_path(["--properties-file", str(properties_file)])

    assert resolved == properties_file.resolve()


def test_load_job_config_requires_all_app_keys(tmp_path):
    properties_file = tmp_path / "broken.properties"
    properties_file.write_text("spark.master=local[*]\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required properties"):
        load_job_config(properties_file)


def test_derive_storage_paths_creates_sibling_directories():
    paths = derive_storage_paths("gs://bucket/chzzk/local/")

    assert paths == {
        "bronze_path": "gs://bucket/chzzk/local/bronze",
        "dead_letter_path": "gs://bucket/chzzk/local/dead_letter",
        "checkpoint_path": "gs://bucket/chzzk/local/checkpoint",
    }


def test_load_job_config_rejects_placeholder_bucket_uri(tmp_path):
    properties_file = tmp_path / "placeholder.properties"
    properties_file.write_text(
        "\n".join(
            [
                "spark.master=local[*]",
                "app.kafka.bootstrap.servers=localhost:9092",
                "app.kafka.topic=chzzk.events.raw",
                "app.kafka.startingOffsets=latest",
                "app.bucket.uri=gs://your-bucket/chzzk/local",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="placeholder value"):
        load_job_config(properties_file)


def test_load_job_config_derives_storage_paths_from_bucket_uri(tmp_path):
    properties_file = tmp_path / "valid.properties"
    properties_file.write_text(
        "\n".join(
            [
                "spark.app.name=Kafka Raw to Bronze",
                "spark.master=local[*]",
                "app.kafka.bootstrap.servers=localhost:9092",
                "app.kafka.topic=chzzk.events.raw",
                "app.kafka.startingOffsets=latest",
                "app.bucket.uri=gs://bucket/chzzk/local",
            ]
        ),
        encoding="utf-8",
    )

    config = load_job_config(properties_file)

    assert config.properties_path == Path(properties_file).resolve()
    assert config.spark_properties["spark.master"] == "local[*]"
    assert config.app_properties["app.kafka.topic"] == "chzzk.events.raw"
    assert config.bucket_uri == "gs://bucket/chzzk/local"
    assert config.bronze_path == "gs://bucket/chzzk/local/bronze"
    assert config.dead_letter_path == "gs://bucket/chzzk/local/dead_letter"
    assert config.checkpoint_path == "gs://bucket/chzzk/local/checkpoint"


def test_load_job_config_still_accepts_explicit_paths(tmp_path):
    properties_file = tmp_path / "explicit.properties"
    properties_file.write_text(
        "\n".join(
            [
                "spark.master=local[*]",
                "app.kafka.bootstrap.servers=localhost:9092",
                "app.kafka.topic=chzzk.events.raw",
                "app.kafka.startingOffsets=latest",
                "app.bronze.path=gs://bucket/chzzk/custom/bronze",
                "app.checkpoint.path=gs://bucket/chzzk/custom/checkpoint",
            ]
        ),
        encoding="utf-8",
    )

    config = load_job_config(properties_file)

    assert config.bucket_uri is None
    assert config.bronze_path == "gs://bucket/chzzk/custom/bronze"
    assert config.dead_letter_path == "gs://bucket/chzzk/custom/dead_letter"
    assert config.checkpoint_path == "gs://bucket/chzzk/custom/checkpoint"
