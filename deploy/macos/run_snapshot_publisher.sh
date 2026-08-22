#!/bin/bash
set -euo pipefail
umask 077

fail() {
  echo "Dcar snapshot publisher preflight failed: $*" >&2
  exit 78
}

project_root="${DCAR_PROJECT_ROOT:-}"
[[ -n "$project_root" && "$project_root" = /* ]] || \
  fail "DCAR_PROJECT_ROOT must be an absolute path"
[[ -d "$project_root" ]] || fail "project root does not exist"
project_root="$(cd "$project_root" && pwd -P)"

publisher_env="${DCAR_PUBLISHER_ENV_FILE:-}"
[[ -n "$publisher_env" && "$publisher_env" = /* ]] || \
  fail "DCAR_PUBLISHER_ENV_FILE must be an absolute path"
[[ -f "$publisher_env" && ! -L "$publisher_env" ]] || \
  fail "publisher environment must be a regular non-symlink file"

python_bin="$project_root/.venv/bin/python"
[[ -x "$python_bin" ]] || fail "project virtualenv Python is missing"
[[ -s "${DCAR_V8_DB:-}" && ! -L "${DCAR_V8_DB:-}" ]] || \
  fail "formal writer database is missing or unsafe"
[[ -z "${TIKHUB_API_KEY:-}" ]] || \
  fail "the snapshot publisher must not receive a provider key"
[[ -z "${TIKHUB_API_KEY_FILE:-}" ]] || \
  fail "the snapshot publisher must not receive a provider key file"
[[ "${DCAR_SCHEDULER_ENABLED:-}" == "0" ]] || \
  fail "the publisher LaunchAgent must not enable a scheduler"
[[ "${DCAR_STARTUP_CATCHUP_ENABLED:-}" == "0" ]] || \
  fail "the publisher LaunchAgent must keep catch-up disabled"

arguments=(
  "$project_root/deploy/macos/publish_snapshot.py"
  --project-root "$project_root"
  --env-file "$publisher_env"
  --db "$DCAR_V8_DB"
)
if [[ -n "${DCAR_LEGACY_DB:-}" ]]; then
  arguments+=(--legacy-db "$DCAR_LEGACY_DB")
fi
if [[ "${1:-}" == "--check" ]]; then
  arguments+=(--check)
elif [[ $# -eq 0 ]]; then
  arguments+=(--automatic)
else
  fail "only the optional --check argument is supported"
fi

echo "Dcar snapshot publisher starting; provider_calls=0 catchup=0"
exec /usr/bin/caffeinate -i "$python_bin" "${arguments[@]}"
