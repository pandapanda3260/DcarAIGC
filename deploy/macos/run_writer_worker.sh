#!/bin/bash
set -euo pipefail
umask 077

fail() {
  echo "Dcar writer worker preflight failed: $*" >&2
  exit 78
}

project_root="${DCAR_PROJECT_ROOT:-}"
[[ -n "$project_root" ]] || fail "DCAR_PROJECT_ROOT is missing"
[[ "$project_root" = /* ]] || fail "DCAR_PROJECT_ROOT must be absolute"
[[ -d "$project_root" ]] || fail "project root does not exist"
project_root="$(cd "$project_root" && pwd -P)"

[[ "${DCAR_WORKER_HOST:-}" == "127.0.0.1" ]] || \
  fail "worker host must be 127.0.0.1"
[[ "${DCAR_WORKER_PORT:-}" == "8766" ]] || \
  fail "worker port must be 8766; port 8765 belongs to the UI/API process"
[[ -z "${TIKHUB_API_KEY:-}" ]] || \
  fail "direct TIKHUB_API_KEY values are forbidden; use an external key file"

writer_env="${DCAR_WRITER_ENV_FILE:-}"
[[ -n "$writer_env" ]] || fail "DCAR_WRITER_ENV_FILE is missing"
[[ "$writer_env" = /* ]] || fail "DCAR_WRITER_ENV_FILE must be absolute"
[[ -f "$writer_env" && ! -L "$writer_env" ]] || \
  fail "writer environment file must be a regular, non-symlink file"

writer_env_mode="$(/usr/bin/stat -f '%Lp' "$writer_env")"
case "$writer_env_mode" in
  400|600) ;;
  *) fail "writer environment file must have mode 0400 or 0600" ;;
esac

tikhub_key_file=""
cost_authorization=""
while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
  line="${raw_line%$'\r'}"
  case "$line" in
    ""|'#'*) continue ;;
    TIKHUB_API_KEY_FILE=*)
      [[ -z "$tikhub_key_file" ]] || fail "duplicate TIKHUB_API_KEY_FILE entry"
      tikhub_key_file="${line#TIKHUB_API_KEY_FILE=}"
      ;;
    DCAR_DAILY_COST_AUTHORIZATION=*)
      [[ -z "$cost_authorization" ]] || \
        fail "duplicate DCAR_DAILY_COST_AUTHORIZATION entry"
      cost_authorization="${line#DCAR_DAILY_COST_AUTHORIZATION=}"
      ;;
    TIKHUB_API_KEY=*)
      fail "the writer environment file must not contain the API key"
      ;;
    *) fail "unsupported writer environment entry" ;;
  esac
done < "$writer_env"

[[ "$cost_authorization" == "I_ACKNOWLEDGE_DAILY_PROVIDER_LIMIT_USD_8" ]] || \
  fail "daily USD 8 provider budget has not been explicitly authorized"
[[ -n "$tikhub_key_file" && "$tikhub_key_file" = /* ]] || \
  fail "TIKHUB_API_KEY_FILE must be an absolute external path"
[[ -f "$tikhub_key_file" && ! -L "$tikhub_key_file" ]] || \
  fail "TikHub key file must be a regular, non-symlink file"

key_file_mode="$(/usr/bin/stat -f '%Lp' "$tikhub_key_file")"
case "$key_file_mode" in
  400|600) ;;
  *) fail "TikHub key file must have mode 0400 or 0600" ;;
esac

key_file_dir="$(cd "$(dirname "$tikhub_key_file")" && pwd -P)"
key_file_path="$key_file_dir/$(basename "$tikhub_key_file")"
case "$key_file_path" in
  "$project_root"|"$project_root"/*)
    fail "TikHub key file must stay outside the repository"
    ;;
esac
/usr/bin/grep -Eq '^[[:space:]]*TIKHUB_API_KEY=' "$key_file_path" || \
  fail "TikHub key file does not contain TIKHUB_API_KEY"

python_bin="$project_root/.venv/bin/python"
[[ -x "$python_bin" ]] || fail "project virtualenv Python is missing"
for required_command in ffmpeg ffprobe swiftc; do
  command -v "$required_command" >/dev/null 2>&1 || \
    fail "$required_command is missing from the LaunchAgent PATH"
done
"$python_bin" -c 'import mlx_whisper' >/dev/null 2>&1 || \
  fail "mlx-whisper is not importable from the project virtualenv"
[[ -s "${DCAR_V8_DB:-}" ]] || \
  fail "formal writer database is missing or empty; refusing to create a new one"

export TIKHUB_API_KEY_FILE="$key_file_path"
export DCAR_READ_ONLY=0
export DCAR_SCHEDULER_ENABLED=1
export DCAR_STARTUP_CATCHUP_ENABLED=0

echo "Dcar writer worker starting on 127.0.0.1:8766; scheduler=1 catchup=0"
exec /usr/bin/caffeinate -s \
  "$python_bin" -m uvicorn v8.api:app \
  --app-dir "$project_root/src/dcar_eval" \
  --host 127.0.0.1 \
  --port 8766 \
  --workers 1 \
  --no-access-log
