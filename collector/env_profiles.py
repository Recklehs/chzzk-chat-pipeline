import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from collector.event_bus import normalize_event_bus_backend

DEFAULT_APP_ENV = "test"
SUPPORTED_APP_ENVS = {"test", "prod"}


@dataclass(frozen=True)
class LoadedEnvProfile:
    app_env: str
    root_dir: Path
    root_env_path: Path
    profile_env_path: Path


def resolve_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def normalize_app_env(value: str | None) -> str:
    normalized = (value or DEFAULT_APP_ENV).strip().lower()
    if not normalized:
        normalized = DEFAULT_APP_ENV

    if normalized not in SUPPORTED_APP_ENVS:
        choices = ", ".join(sorted(SUPPORTED_APP_ENVS))
        raise ValueError(f"Unsupported APP_ENV '{value}'. Expected one of: {choices}")

    return normalized


def load_runtime_env(*, root_dir: Path | None = None, override: bool = False) -> LoadedEnvProfile:
    resolved_root_dir = Path(root_dir) if root_dir is not None else resolve_project_root()
    root_env_path = resolved_root_dir / ".env"

    if root_env_path.is_file():
        load_dotenv(root_env_path, override=override)

    app_env = normalize_app_env(os.environ.get("APP_ENV"))
    profile_env_path = resolved_root_dir / f".env.{app_env}"
    if not profile_env_path.is_file():
        raise FileNotFoundError(
            f"Required env profile file not found for APP_ENV={app_env}: {profile_env_path}"
        )

    load_dotenv(profile_env_path, override=override)
    return LoadedEnvProfile(
        app_env=app_env,
        root_dir=resolved_root_dir,
        root_env_path=root_env_path,
        profile_env_path=profile_env_path,
    )


def collect_runtime_environment(*, root_dir: Path | None = None) -> dict[str, str]:
    profile = load_runtime_env(root_dir=root_dir)
    host = os.environ.get("HOST", "0.0.0.0").strip() or "0.0.0.0"
    port = str(int(os.environ.get("PORT", "8000").strip() or "8000"))
    event_bus_backend = normalize_event_bus_backend(os.environ.get("EVENT_BUS_BACKEND", "pubsub"))

    return {
        "app_env": profile.app_env,
        "root_env_path": str(profile.root_env_path),
        "profile_env_path": str(profile.profile_env_path),
        "host": host,
        "port": port,
        "event_bus_backend": event_bus_backend,
    }


def main() -> None:
    print(json.dumps(collect_runtime_environment(), ensure_ascii=False))


if __name__ == "__main__":
    main()
