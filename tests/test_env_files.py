import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_public_export_excludes_local_data_and_credentials():
    environment = json.loads((PROJECT_ROOT / "postman/chzzk_local.postman_environment.json").read_text())
    assert all(item["value"] == "" for item in environment["values"] if item["key"] == "apiKey")
    assert "/data/" in (PROJECT_ROOT / ".gitignore").read_text().splitlines()
    assert {"postman/", "manual/", "data/"} <= set((PROJECT_ROOT / ".dockerignore").read_text().splitlines())


def test_gitignore_ignores_local_env_profiles():
    gitignore_text = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")

    assert ".env" in gitignore_text
    assert ".env.test" in gitignore_text
    assert ".env.prod" in gitignore_text


def test_example_env_files_exist():
    assert (PROJECT_ROOT / ".env.example").is_file()
    assert (PROJECT_ROOT / ".env.test.example").is_file()
    assert (PROJECT_ROOT / ".env.prod.example").is_file()


def test_root_env_example_contains_event_bus_selector():
    env_example_text = (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")

    assert "EVENT_BUS_BACKEND=pubsub" in env_example_text
    assert "EVENT_BUS_BACKEND=kafka" in env_example_text


def test_profile_examples_only_define_backend_connection_values():
    test_example_text = (PROJECT_ROOT / ".env.test.example").read_text(encoding="utf-8")
    prod_example_text = (PROJECT_ROOT / ".env.prod.example").read_text(encoding="utf-8")

    for text in (test_example_text, prod_example_text):
        assert "EVENT_BUS_BACKEND" not in text
        assert "PUBSUB_RAW_TOPIC" in text
        assert "KAFKA_BOOTSTRAP_SERVERS" in text
        assert "KAFKA_TOPIC" in text
        assert "KAFKA_CLIENT_ID" in text
