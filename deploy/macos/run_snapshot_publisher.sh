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
source_root="${DCAR_WRITER_SOURCE_ROOT-$project_root}"
[[ "$source_root" = /* && -d "$source_root" && ! -L "$source_root" ]] || \
  fail "writer source root must be an absolute, non-symlink directory"
source_root="$(cd "$source_root" && pwd -P)"

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
  "$source_root/deploy/macos/publish_snapshot.py"
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

if [[ -n "${DCAR_WRITER_SOURCE_ROOT:-}" ]]; then
  export DCAR_WRITER_SOURCE_ROOT="$source_root"
  export PYTHONDONTWRITEBYTECODE=1
  export PYTHONSAFEPATH=1
  source_verifier="$source_root/src/dcar_eval/v8/runtime_paths.py"
  [[ -f "$source_verifier" && ! -L "$source_verifier" ]] || \
    fail "writer source verifier is missing or unsafe"
  "$python_bin" -I -B - "$project_root" "$source_root" "" <<'WRITER_SOURCE_ANCHOR' || \
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
schema_contract = payload.get("schema_contract", {})
if schema_contract.get("formal_schema") == 21 or schema_contract.get("code_schema") == 21:
    successor = payload.get("account_classification_successor")
    if (schema_contract.get("formal_schema") != 21 or schema_contract.get("code_schema") != 21
            or not cleanup or not isinstance(successor, dict)
            or successor.get("contract") != "account-classification-schema-successor-v1"
            or successor.get("transition") != "account-classification-20260908-v1"):
        reject("schema21 classification successor is missing or mismatched")
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
    --mode publisher >/dev/null || \
    fail "publisher source differs from the installed sealed writer"
else
  # A legacy publisher must not load mutable code after the Writer is frozen.
  # Read only LaunchAgent metadata here; no project module, DB, or lock access.
  "$python_bin" -I -B - <<'WRITER_SOURCE_METADATA' || \
    fail "publisher must configure the installed writer source root"
import os
import plistlib
import pwd
import stat
from pathlib import Path

home = Path(pwd.getpwuid(os.getuid()).pw_dir).resolve(strict=True)
path = home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
if path.exists() or path.is_symlink():
    if any(item.is_symlink() for item in (path, *path.parents)) or not path.is_file():
        raise SystemExit("installed writer LaunchAgent path is unsafe")
    metadata = path.stat()
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o022:
        raise SystemExit("installed writer LaunchAgent ownership or mode is unsafe")
    payload = plistlib.loads(path.read_bytes())
    if not isinstance(payload, dict):
        raise SystemExit("installed writer LaunchAgent is invalid")
    environment = payload.get("EnvironmentVariables", {})
    if not isinstance(environment, dict):
        raise SystemExit("installed writer LaunchAgent environment is invalid")
    if "DCAR_WRITER_SOURCE_ROOT" in environment:
        raise SystemExit("installed writer requires an explicit frozen source root")
WRITER_SOURCE_METADATA
fi
export PYTHONPATH="$source_root/src/dcar_eval:$source_root/scripts"

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
