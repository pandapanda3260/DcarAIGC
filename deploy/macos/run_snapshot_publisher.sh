#!/bin/bash
set -euo pipefail
umask 077
unset SSH_AUTH_SOCK

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
[[ -z "${TIKHUB_API_KEY:-}" ]] || \
  fail "the snapshot publisher must not receive a provider key"
[[ -z "${TIKHUB_API_KEY_FILE:-}" ]] || \
  fail "the snapshot publisher must not receive a provider key file"
[[ "${DCAR_READ_ONLY:-}" == "1" ]] || \
  fail "the publisher runtime must remain read-only"
[[ "${DCAR_SCHEDULER_ENABLED:-}" == "0" ]] || \
  fail "the publisher LaunchAgent must not enable a scheduler"
[[ "${DCAR_STARTUP_CATCHUP_ENABLED:-}" == "0" ]] || \
  fail "the publisher LaunchAgent must keep catch-up disabled"

arguments=(
  "$project_root/deploy/macos/publish_snapshot.py"
  --project-root "$project_root"
  --env-file "$publisher_env"
)
publisher_intent=""
if [[ "${1:-}" == "--check" && $# -eq 1 ]]; then
  publisher_intent="formal_read"
  arguments+=(--check)
elif [[ "${1:-}" == "--remote-check" && $# -eq 1 ]]; then
  publisher_intent="remote_only"
  arguments+=(--remote-check)
elif [[ "${1:-}" == "--resume-local-snapshot" && $# -eq 2 ]]; then
  publisher_intent="sealed_resume"
  snapshot_id="$2"
  [[ "$snapshot_id" =~ ^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$ ]] || \
    fail "resume snapshot ID is invalid"
  arguments+=(--resume-local-snapshot "$snapshot_id")
elif [[ "${1:-}" == "--resume-staged-snapshot" && $# -eq 2 ]]; then
  publisher_intent="sealed_resume"
  snapshot_id="$2"
  [[ "$snapshot_id" =~ ^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$ ]] || \
    fail "resume snapshot ID is invalid"
  arguments+=(--resume-staged-snapshot "$snapshot_id")
elif [[ $# -eq 0 ]]; then
  publisher_intent="formal_read"
  arguments+=(--automatic)
else
  fail "only --check, --remote-check, or --resume-staged-snapshot SNAPSHOT_ID or --resume-local-snapshot SNAPSHOT_ID is supported"
fi

if [[ "$publisher_intent" == "formal_read" ]]; then
  [[ -s "${DCAR_V8_DB:-}" && ! -L "${DCAR_V8_DB:-}" ]] || \
    fail "formal writer database is missing or unsafe"
  writer_database_dir="$(cd "$(dirname "$DCAR_V8_DB")" && pwd -P)"
  writer_database_path="$writer_database_dir/$(basename "$DCAR_V8_DB")"
  case "$writer_database_path" in
    "$project_root"|"$project_root"/*)
      fail "writer database must stay outside the repository"
      ;;
  esac
  arguments+=(--db "$writer_database_path")
  if [[ -n "${DCAR_LEGACY_DB:-}" ]]; then
    [[ -s "$DCAR_LEGACY_DB" && ! -L "$DCAR_LEGACY_DB" ]] || \
      fail "legacy database is missing or unsafe"
    legacy_database_dir="$(cd "$(dirname "$DCAR_LEGACY_DB")" && pwd -P)"
    legacy_database_path="$legacy_database_dir/$(basename "$DCAR_LEGACY_DB")"
    case "$legacy_database_path" in
      "$project_root"|"$project_root"/*)
        fail "legacy database must stay outside the repository"
        ;;
    esac
    arguments+=(--legacy-db "$legacy_database_path")
  fi
else
  unset DCAR_V8_DB DCAR_LEGACY_DB DCAR_WRITER_LOCK
fi

echo "Dcar snapshot publisher starting; intent=$publisher_intent provider_calls=0 catchup=0"
exec /usr/bin/caffeinate -i "$python_bin" "${arguments[@]}"
