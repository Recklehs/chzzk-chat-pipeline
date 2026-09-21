from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUN_SERVER = PROJECT_ROOT / "run_server.sh"


def test_run_server_bootstraps_collector_without_spark_dependencies():
    script_text = RUN_SERVER.read_text(encoding="utf-8")

    assert "uvicorn chzzk_collector_server:app" in script_text
    assert "requirements-collector.txt" in script_text
    assert "collector.bootstrap_pubsub_raw_topic" in script_text
    assert "collect_runtime_environment" in script_text
    assert "APP_ENV" in script_text
    assert "Spark 호환 JDK" not in script_text
    assert "JAVA_HOME" not in script_text
