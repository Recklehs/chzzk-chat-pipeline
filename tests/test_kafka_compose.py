from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_kafka_compose_exists_with_expected_services_and_ports():
    compose_path = PROJECT_ROOT / "kafka" / "compose.yaml"

    assert compose_path.is_file()

    compose_text = compose_path.read_text(encoding="utf-8")

    assert "services:" in compose_text
    assert "  kafka:" in compose_text
    assert "  kafka-ui:" in compose_text
    assert "  collector:" not in compose_text
    assert "image: apache/kafka:4.2.0" in compose_text
    assert '- "9092:9092"' in compose_text
    assert '- "18080:8080"' in compose_text
    assert "KAFKA_ADVERTISED_LISTENERS: \"PLAINTEXT://localhost:9092,INTERNAL://kafka:29092\"" in compose_text
    assert "KAFKA_AUTO_CREATE_TOPICS_ENABLE: \"true\"" in compose_text
    assert "healthcheck:" in compose_text
    assert "condition: service_healthy" in compose_text
    assert "KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: \"kafka:29092\"" in compose_text
    assert "volumes:" in compose_text
    assert "kafka-data:" in compose_text
