from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_collector_dockerfile_uses_collector_only_requirements():
    dockerfile_text = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "requirements-collector.txt" in dockerfile_text
    assert "requirements.txt" not in dockerfile_text
    assert "openjdk-17" not in dockerfile_text
    assert "JAVA_HOME" not in dockerfile_text


def test_requirements_umbrella_includes_collector_and_spark_files():
    requirements_text = (PROJECT_ROOT / "requirements.txt").read_text(encoding="utf-8")
    collector_requirements = (PROJECT_ROOT / "requirements-collector.txt").read_text(encoding="utf-8")
    spark_requirements = (PROJECT_ROOT / "requirements-spark.txt").read_text(encoding="utf-8")

    assert "-r requirements-collector.txt" in requirements_text
    assert "-r requirements-spark.txt" in requirements_text
    assert "google-cloud-pubsub" in collector_requirements
    assert "pyspark==4.0.1" in spark_requirements
