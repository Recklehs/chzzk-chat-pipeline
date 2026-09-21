#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
STAMP_FILE="${VENV_DIR}/.requirements.sha256"
ENV_FILE="${ROOT_DIR}/.env"

choose_python() {
  if [[ -n "${PYTHON_BIN:-}" ]]; then
    echo "${PYTHON_BIN}"
    return 0
  fi

  if command -v python3.13 >/dev/null 2>&1; then
    echo "python3.13"
    return 0
  fi

  if command -v python3 >/dev/null 2>&1; then
    echo "python3"
    return 0
  fi

  echo "[ERROR] python3.13 또는 python3 실행 파일을 찾을 수 없습니다." >&2
  exit 1
}

current_requirements_hash() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${ROOT_DIR}/requirements-collector.txt" "${ROOT_DIR}/requirements-dev.txt" | shasum -a 256 | awk '{print $1}'
    return 0
  fi

  "${PYTHON_CMD}" - <<'PY'
from hashlib import sha256
from pathlib import Path

root = Path.cwd()
hasher = sha256()
for name in ("requirements-collector.txt", "requirements-dev.txt"):
    hasher.update((root / name).read_bytes())
print(hasher.hexdigest())
PY
}

PYTHON_CMD="$(choose_python)"

cd "${ROOT_DIR}"

if [[ ! -d "${VENV_DIR}" ]]; then
  echo "[INFO] 가상환경 생성: ${VENV_DIR}"
  "${PYTHON_CMD}" -m venv "${VENV_DIR}"
fi

# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

REQUIREMENTS_HASH="$(current_requirements_hash)"
INSTALLED_HASH=""
if [[ -f "${STAMP_FILE}" ]]; then
  INSTALLED_HASH="$(cat "${STAMP_FILE}")"
fi

if [[ "${REQUIREMENTS_HASH}" != "${INSTALLED_HASH}" ]]; then
  echo "[INFO] 의존성 설치 또는 갱신"
  python -m pip install -r requirements-collector.txt
  python -m pip install -r requirements-dev.txt
  printf '%s\n' "${REQUIREMENTS_HASH}" > "${STAMP_FILE}"
fi

if [[ ! -f "${ENV_FILE}" ]] && [[ -z "${APP_ENV:-}" ]]; then
  echo "[ERROR] .env 파일이 없고 APP_ENV 환경변수도 설정되지 않았습니다." >&2
  exit 1
fi

eval "$(python - <<'PY'
from collector.env_profiles import collect_runtime_environment
import shlex

runtime = collect_runtime_environment()
print(f'APP_ENV_RESOLVED={shlex.quote(runtime["app_env"])}')
print(f'HOST={shlex.quote(runtime["host"])}')
print(f'PORT={shlex.quote(runtime["port"])}')
print(f'EVENT_BUS_BACKEND={shlex.quote(runtime["event_bus_backend"])}')
PY
)"

if [[ "${APP_ENV_RESOLVED}" == "test" ]] && [[ "${EVENT_BUS_BACKEND}" == "pubsub" ]]; then
  echo "[INFO] test 프로필 Pub/Sub raw topic 확인"
  python -m collector.bootstrap_pubsub_raw_topic
fi

echo "[INFO] 서버 시작: http://${HOST}:${PORT}"
exec python -m uvicorn chzzk_collector_server:app --host "${HOST}" --port "${PORT}" "$@"
