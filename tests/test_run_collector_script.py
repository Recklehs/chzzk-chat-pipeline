from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUN_COLLECTOR = PROJECT_ROOT / "run_collector.ps1"


def test_run_collector_uses_profile_resolution_and_bootstrap():
    script_text = RUN_COLLECTOR.read_text(encoding="utf-8")

    assert "collect_runtime_environment" in script_text
    assert "collector.bootstrap_pubsub_raw_topic" in script_text
    assert "APP_ENV" in script_text
    assert "ConvertFrom-Json" in script_text
