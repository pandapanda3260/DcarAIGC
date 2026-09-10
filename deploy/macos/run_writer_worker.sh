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
source_root="${DCAR_WRITER_SOURCE_ROOT-$project_root}"
[[ "$source_root" = /* && -d "$source_root" && ! -L "$source_root" ]] || \
  fail "writer source root must be an absolute, non-symlink directory"
source_root="$(cd "$source_root" && pwd -P)"

scheduler_start_paused="${DCAR_SCHEDULER_START_PAUSED:-0}"
case "$scheduler_start_paused" in
  0|1) ;;
  *) fail "DCAR_SCHEDULER_START_PAUSED must be 0 or 1" ;;
esac

[[ "${DCAR_WORKER_HOST:-}" == "127.0.0.1" ]] || \
  fail "worker host must be 127.0.0.1"
[[ "${DCAR_WORKER_PORT:-}" == "8766" ]] || \
  fail "worker port must be 8766; port 8765 is reserved for the freeze read-only viewer"
[[ -z "${TIKHUB_API_KEY:-}" ]] || \
  fail "direct TIKHUB_API_KEY values are forbidden; use an external key file"
[[ -z "${TIKHUB_API_BASE:-}" ]] || \
  fail "direct TIKHUB_API_BASE values are forbidden; use the external TikHub config file"
[[ -z "${DCAR_LOADED_BUILD_ID:-}" ]] || \
  fail "DCAR_LOADED_BUILD_ID must be derived from the loaded build receipt"

reconcile_from="${DCAR_DAILY_CAPTURE_RECONCILE_FROM:-}"
unset DCAR_DAILY_CAPTURE_RECONCILE_FROM
[[ "$reconcile_from" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || \
  fail "DCAR_DAILY_CAPTURE_RECONCILE_FROM must be exactly YYYY-MM-DD"
normalized_reconcile_from="$(
  /bin/date -j -f '%Y-%m-%d' "$reconcile_from" '+%Y-%m-%d' 2>/dev/null
)" || fail "DCAR_DAILY_CAPTURE_RECONCILE_FROM is not a valid calendar date"
[[ "$normalized_reconcile_from" == "$reconcile_from" ]] || \
  fail "DCAR_DAILY_CAPTURE_RECONCILE_FROM is not canonical"

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

loaded_build_receipt="${DCAR_LOADED_BUILD_RECEIPT:-}"
[[ -n "$loaded_build_receipt" ]] || fail "DCAR_LOADED_BUILD_RECEIPT is missing"
[[ "$loaded_build_receipt" = /* ]] || \
  fail "DCAR_LOADED_BUILD_RECEIPT must be absolute"
[[ -f "$loaded_build_receipt" && ! -L "$loaded_build_receipt" ]] || \
  fail "loaded build receipt must be a regular, non-symlink file"
[[ "$(/usr/bin/stat -f '%l' "$loaded_build_receipt")" == "1" ]] || \
  fail "loaded build receipt must be a single-link file"
[[ "$(/usr/bin/stat -f '%u' "$loaded_build_receipt")" == "$(/usr/bin/id -u)" ]] || \
  fail "loaded build receipt must be owned by the current user"
[[ "$(/usr/bin/stat -f '%Lp' "$loaded_build_receipt")" == "600" ]] || \
  fail "loaded build receipt must have mode 0600"
loaded_build_receipt_dir="$(cd "$(dirname "$loaded_build_receipt")" && pwd -P)" || \
  fail "loaded build receipt parent is unavailable"
loaded_build_receipt_path="$loaded_build_receipt_dir/$(basename "$loaded_build_receipt")"
case "$loaded_build_receipt_path" in
  "$project_root"|"$project_root"/*)
    fail "loaded build receipt must stay outside the repository"
    ;;
esac
loaded_build_sha256="$(
  /usr/bin/shasum -a 256 "$loaded_build_receipt_path" | /usr/bin/awk '{print $1}'
)" || fail "loaded build receipt SHA-256 could not be calculated"
[[ "$loaded_build_sha256" =~ ^[0-9a-f]{64}$ ]] || \
  fail "loaded build receipt SHA-256 is invalid"

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

[[ "$cost_authorization" == "I_ACKNOWLEDGE_DAILY_PROVIDER_LIMIT_USD_100" ]] || \
  fail "daily USD 100 provider budget has not been explicitly authorized"
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
[[ "$(/usr/bin/grep -Ec '^[[:space:]]*TIKHUB_API_KEY=' "$key_file_path")" == "1" ]] || \
  fail "TikHub config file must contain exactly one TIKHUB_API_KEY"
[[ "$(/usr/bin/grep -Ec '^[[:space:]]*TIKHUB_API_BASE=' "$key_file_path")" == "1" ]] || \
  fail "TikHub config file must contain exactly one TIKHUB_API_BASE"
if ! /usr/bin/grep -Fxq 'TIKHUB_API_BASE=https://api.tikhub.dev' "$key_file_path" && \
  ! /usr/bin/grep -Fxq 'TIKHUB_API_BASE=https://api.tikhub.io' "$key_file_path"; then
  fail "TIKHUB_API_BASE must be exactly https://api.tikhub.dev or https://api.tikhub.io"
fi

python_bin="$project_root/.venv/bin/python"
[[ -x "$python_bin" ]] || fail "project virtualenv Python is missing"
if [[ -n "${DCAR_WRITER_SOURCE_ROOT:-}" ]]; then
  export DCAR_WRITER_SOURCE_ROOT="$source_root"
  export PYTHONDONTWRITEBYTECODE=1
  export PYTHONSAFEPATH=1
  source_verifier="$source_root/src/dcar_eval/v8/runtime_paths.py"
  [[ -f "$source_verifier" && ! -L "$source_verifier" ]] || \
    fail "writer source verifier is missing or unsafe"
  "$python_bin" -I -B - "$project_root" "$source_root" "$loaded_build_receipt_path" <<'WRITER_SOURCE_ANCHOR' || \
    fail "sealed source entrypoint verification failed"
import hashlib
import json
import os
import pwd
import stat
import sys
from pathlib import Path

def reject(message):
    raise SystemExit("writer source entrypoint: " + message)

def raw(path, private=False):
    if not path.is_absolute() or path.resolve(strict=True) != path:
        reject("path is not canonical")
    before = path.lstat()
    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
            or before.st_uid != os.geteuid() or before.st_mode & 0o022
            or (private and stat.S_IMODE(before.st_mode) != 0o600)
            or not 0 <= before.st_size <= 16 * 1024 * 1024):
        reject("file metadata is unsafe")
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        body = stream.read(16 * 1024 * 1024 + 1)
        opened = os.fstat(stream.fileno())
    after = path.lstat()
    def identity(value):
        return (value.st_dev, value.st_ino, value.st_mode, value.st_uid,
                value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
    if identity(before) != identity(opened) or identity(before) != identity(after) or len(body) != before.st_size:
        reject("file changed while reading")
    return body, stat.S_IMODE(before.st_mode)

def object_json(body):
    def unique(pairs):
        result = dict(pairs)
        if len(result) != len(pairs):
            reject("duplicate receipt field")
        return result
    def invalid_constant(value):
        reject("invalid receipt constant")
    result = json.loads(body, object_pairs_hook=unique, parse_constant=invalid_constant)
    if not isinstance(result, dict):
        reject("receipt is not an object")
    return result

def reference(value):
    if not isinstance(value, dict) or not isinstance(value.get("path"), str):
        reject("invalid private reference")
    body, _ = raw(Path(value["path"]), private=True)
    if (hashlib.sha256(body).hexdigest() != value.get("sha256")
            or len(body) != value.get("byte_size")):
        reject("private reference SHA or size differs")
    return object_json(body)

data, source = Path(sys.argv[1]), Path(sys.argv[2])
home = Path(pwd.getpwuid(os.geteuid()).pw_dir).resolve(strict=True)
import plistlib
installed = plistlib.loads(raw(home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist")[0])
if not isinstance(installed, dict):
    reject("installed writer is invalid")
environment = installed.get("EnvironmentVariables", {})
if (not isinstance(environment, dict)
        or installed.get("Label") != "cn.tj.dcar.writer-worker"
        or installed.get("WorkingDirectory") != str(data)
        or installed.get("ProgramArguments") != [str(source / "deploy/macos/run_writer_worker.sh")]
        or environment.get("DCAR_PROJECT_ROOT") != str(data)
        or environment.get("DCAR_WRITER_SOURCE_ROOT") != str(source)):
    reject("installed writer source differs")
selected = environment.get("DCAR_LOADED_BUILD_RECEIPT")
if not isinstance(selected, str) or (sys.argv[3] and sys.argv[3] != selected):
    reject("installed build differs")
envelope = object_json(raw(Path(selected), private=True)[0])
payload = envelope.get("payload")
digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
if (not isinstance(payload, dict) or envelope.get("contract_version") != "sealed-build-receipt-v1"
        or envelope.get("payload_sha256") != digest or payload.get("status") != "succeeded"):
    reject("build envelope differs")
cleanup = isinstance(payload.get("account_cleanup_generation"), dict)
plan = reference(payload["code_successor_plan"])
expected_contract = "account-cleanup-source-plan-v1" if cleanup else "writer-source-isolation-successor-plan-v1"
expected_transition = "account-cleanup-0907-v1" if cleanup else "writer-source-isolation-20260907-v1"
if (plan.get("contract") != expected_contract
        or plan.get("transition") != expected_transition
        or plan.get("project_root") != str(data) or plan.get("source_root") != str(source)
        or plan.get("git") != payload.get("git")):
    reject("source plan differs")
manifest = reference(plan["source_tree"])
if (manifest.get("contract") != "writer-source-tree-v1"
        or manifest.get("source_root") != str(source) or manifest.get("git") != plan.get("git")
        or not isinstance(manifest.get("files"), list)):
    reject("source manifest differs")
name = "src/dcar_eval/v8/runtime_paths.py"
rows = [row for row in manifest["files"] if isinstance(row, dict) and row.get("path") == name]
if len(rows) != 1:
    reject("entrypoint is not an exact manifest member")
body, mode = raw(source / name)
if (hashlib.sha256(body).hexdigest() != rows[0].get("sha256")
        or len(body) != rows[0].get("byte_size") or mode != rows[0].get("mode")):
    reject("entrypoint SHA, size or mode differs")
WRITER_SOURCE_ANCHOR
  "$python_bin" -I -B "$source_verifier" --verify-source \
    --project-root "$project_root" --source-root "$source_root" \
    --mode writer --build-receipt "$loaded_build_receipt_path" >/dev/null || \
    fail "sealed writer source receipt mismatch"
fi
export PYTHONPATH="$source_root/src/dcar_eval:$source_root/scripts"
writer_database="${DCAR_V8_DB:-}"
[[ "$writer_database" = /* && -s "$writer_database" && ! -L "$writer_database" ]] || \
  fail "formal writer database is missing or unsafe; refusing to create a new one"
writer_lock="${DCAR_WRITER_LOCK:-}"
[[ "$writer_lock" = /* && -f "$writer_lock" && ! -L "$writer_lock" ]] || \
  fail "installed writer lock is missing or unsafe"
PYTHONPATH="$source_root/src/dcar_eval:$source_root/scripts" \
  "$python_bin" -m v8.runtime_database \
  --access writer \
  --db "$writer_database" \
  --project-root "$project_root" \
  --check >/dev/null || fail "installed writer database contract mismatch"

for required_command in ffmpeg ffprobe swiftc; do
  command -v "$required_command" >/dev/null 2>&1 || \
    fail "$required_command is missing from the LaunchAgent PATH"
done
"$python_bin" -c 'import mlx_whisper' >/dev/null 2>&1 || \
  fail "mlx-whisper is not importable from the project virtualenv"
writer_database_dir="$(cd "$(dirname "$writer_database")" && pwd -P)"
writer_database_path="$writer_database_dir/$(basename "$writer_database")"
case "$writer_database_path" in
  "$project_root"|"$project_root"/*)
    fail "writer database must stay outside the repository"
    ;;
esac
legacy_database="${DCAR_LEGACY_DB:-}"
[[ "$legacy_database" = /* && -s "$legacy_database" && ! -L "$legacy_database" ]] || \
  fail "legacy database is missing or unsafe"
legacy_database_dir="$(cd "$(dirname "$legacy_database")" && pwd -P)"
legacy_database_path="$legacy_database_dir/$(basename "$legacy_database")"
case "$legacy_database_path" in
  "$project_root"|"$project_root"/*)
    fail "legacy database must stay outside the repository"
    ;;
esac

export TIKHUB_API_KEY_FILE="$key_file_path"
export DCAR_LOADED_BUILD_RECEIPT="$loaded_build_receipt_path"
export DCAR_LOADED_BUILD_ID="sha256:$loaded_build_sha256"
export DCAR_READ_ONLY=0
export DCAR_SCHEDULER_ENABLED=1
export DCAR_SCHEDULER_START_PAUSED="$scheduler_start_paused"
if [[ "$scheduler_start_paused" == "1" ]]; then
  export DCAR_STARTUP_CATCHUP_ENABLED=0
else
  export DCAR_STARTUP_CATCHUP_ENABLED=1
fi
export DCAR_DAILY_CAPTURE_RECONCILE_FROM="$reconcile_from"

if [[ "$scheduler_start_paused" == "1" ]]; then
  echo "Dcar writer worker starting on 127.0.0.1:8766; scheduler=paused catchup=disabled"
else
  echo "Dcar writer worker starting on 127.0.0.1:8766; scheduler=1 catchup=report_only"
fi
exec /usr/bin/caffeinate -s \
  "$python_bin" -m uvicorn v8.api:app \
  --app-dir "$source_root/src/dcar_eval" \
  --host 127.0.0.1 \
  --port 8766 \
  --workers 1 \
  --no-access-log
