from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

REQUIRED_COMMON_KEYS = (
    "app.kafka.bootstrap.servers",
    "app.kafka.topic",
    "app.kafka.startingOffsets",
)
PLACEHOLDER_MARKERS = (
    "your-bucket",
    "replace-me",
    "__required__",
    "<bucket>",
)


@dataclass(frozen=True)
class SparkJobConfig:
    properties_path: Path
    raw_properties: dict[str, str]
    spark_properties: dict[str, str]
    app_properties: dict[str, str]
    app_name: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_starting_offsets: str
    bucket_uri: str | None
    bronze_path: str
    dead_letter_path: str
    checkpoint_path: str


def parse_properties_file(path: str | Path) -> dict[str, str]:
    properties_path = Path(path).expanduser().resolve()
    if not properties_path.exists():
        raise FileNotFoundError(f"Spark properties file not found: {properties_path}")

    properties: dict[str, str] = {}
    for raw_line in properties_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("!"):
            continue
        if "=" in line:
            key, value = line.split("=", 1)
        elif ":" in line:
            key, value = line.split(":", 1)
        else:
            raise ValueError(f"Invalid properties line in {properties_path}: {raw_line}")
        properties[key.strip()] = value.strip()
    return properties


def resolve_properties_path(argv: Sequence[str] | None = None) -> Path:
    args = list(argv or [])
    for index, arg in enumerate(args):
        if arg == "--properties-file":
            try:
                return Path(args[index + 1]).expanduser().resolve()
            except IndexError as exc:
                raise ValueError("--properties-file requires a path") from exc
        if arg.startswith("--properties-file="):
            return Path(arg.split("=", 1)[1]).expanduser().resolve()

    raise ValueError("Spark job requires --properties-file <path>.")


def normalize_storage_base_uri(base_uri: str) -> str:
    normalized = str(base_uri).strip().rstrip("/")
    if not normalized:
        raise ValueError("Storage base URI is required.")
    return normalized


def derive_storage_paths(base_uri: str) -> dict[str, str]:
    normalized_base_uri = normalize_storage_base_uri(base_uri)
    return {
        "bronze_path": f"{normalized_base_uri}/bronze",
        "dead_letter_path": f"{normalized_base_uri}/dead_letter",
        "checkpoint_path": f"{normalized_base_uri}/checkpoint",
    }


def derive_dead_letter_path(bronze_path: str) -> str:
    normalized_bronze_path = normalize_storage_base_uri(bronze_path)
    if "/bronze/" in normalized_bronze_path:
        return normalized_bronze_path.replace("/bronze/", "/dead_letter/", 1)
    if normalized_bronze_path.endswith("/bronze"):
        return f"{normalized_bronze_path[:-len('/bronze')]}/dead_letter"
    return f"{normalized_bronze_path}_dead_letter"


def _contains_placeholder(value: str) -> bool:
    lowered = value.lower()
    return any(marker in lowered for marker in PLACEHOLDER_MARKERS)


def _validate_required_properties(properties: dict[str, str], properties_path: Path):
    missing = [key for key in REQUIRED_COMMON_KEYS if not properties.get(key)]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(f"Missing required properties in {properties_path}: {missing_list}")

    has_bucket_uri = bool(properties.get("app.bucket.uri"))
    has_explicit_paths = bool(properties.get("app.bronze.path")) and bool(properties.get("app.checkpoint.path"))
    if not has_bucket_uri and not has_explicit_paths:
        raise ValueError(
            f"Missing required properties in {properties_path}: app.bucket.uri or both "
            "app.bronze.path and app.checkpoint.path"
        )


def _validate_placeholder_paths(properties: dict[str, str], properties_path: Path):
    keys_to_validate = [
        key
        for key in ("app.bucket.uri", "app.bronze.path", "app.dead_letter.path", "app.checkpoint.path")
        if properties.get(key)
    ]
    for key in keys_to_validate:
        value = properties[key]
        if _contains_placeholder(value):
            raise ValueError(f"{key} in {properties_path} still contains a placeholder value: {value}")


def load_job_config(path: str | Path) -> SparkJobConfig:
    properties_path = Path(path).expanduser().resolve()
    properties = parse_properties_file(properties_path)
    _validate_required_properties(properties, properties_path)
    _validate_placeholder_paths(properties, properties_path)

    spark_properties = {
        key: value
        for key, value in properties.items()
        if key.startswith("spark.")
    }
    app_properties = {
        key: value
        for key, value in properties.items()
        if key.startswith("app.")
    }

    bucket_uri = app_properties.get("app.bucket.uri")
    derived_paths = derive_storage_paths(bucket_uri) if bucket_uri else {}
    bronze_path = app_properties.get("app.bronze.path") or derived_paths["bronze_path"]
    dead_letter_path = app_properties.get("app.dead_letter.path") or derived_paths.get("dead_letter_path")
    checkpoint_path = app_properties.get("app.checkpoint.path") or derived_paths["checkpoint_path"]

    if dead_letter_path is None:
        dead_letter_path = derive_dead_letter_path(bronze_path)

    return SparkJobConfig(
        properties_path=properties_path,
        raw_properties=properties,
        spark_properties=spark_properties,
        app_properties=app_properties,
        app_name=spark_properties.get("spark.app.name", "Kafka Raw to Bronze"),
        kafka_bootstrap_servers=properties["app.kafka.bootstrap.servers"],
        kafka_topic=properties["app.kafka.topic"],
        kafka_starting_offsets=properties["app.kafka.startingOffsets"],
        bucket_uri=bucket_uri,
        bronze_path=bronze_path,
        dead_letter_path=dead_letter_path,
        checkpoint_path=checkpoint_path,
    )
