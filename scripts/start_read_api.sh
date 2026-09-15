#!/bin/bash
# Dedicated reader only. Never source writer/provider environment files.
set -euo pipefail

read_code_root="$(cd "$(dirname "$0")/.." && pwd)"
read_project_root="${DCAR_PROJECT_ROOT:?Set the installed writer project root}"
read_database="${DCAR_V8_DB:?Set the installed formal SQLite database}"
read_key_file="${DCAR_READ_API_KEY_FILE:?Set a private reader/gateway key file}"
read_python="${DCAR_READ_PYTHON:-$read_project_root/.venv/bin/python}"
read_port="${DCAR_READ_API_PORT:-8768}"
read_check="${1:-}"
if [[ $# -gt 1 || ( -n "$read_check" && "$read_check" != "--check" ) ]]; then
  echo "Usage: scripts/start_read_api.sh [--check]" >&2
  exit 64
fi
if [[ ! -x "$read_python" ]]; then
  echo "Reader Python runtime is missing; set DCAR_READ_PYTHON" >&2
  exit 78
fi

# Explicit allowlist: no supplier credentials, scheduler activation fields,
# writer lock, proxy variables, or arbitrary PYTHONPATH reach the child.
exec /usr/bin/env -i \
  PATH=/usr/bin:/bin:/usr/sbin:/sbin \
  LANG=en_US.UTF-8 \
  PYTHONPATH="$read_code_root/src/dcar_eval" \
  PYTHONUNBUFFERED=1 \
  PYTHONDONTWRITEBYTECODE=1 \
  DCAR_PROJECT_ROOT="$read_project_root" \
  DCAR_V8_DB="$read_database" \
  DCAR_READ_API_KEY_FILE="$read_key_file" \
  DCAR_READ_ONLY=1 \
  DCAR_SCHEDULER_ENABLED=0 \
  DCAR_STARTUP_CATCHUP_ENABLED=0 \
  DCAR_LLM_DISABLED=1 \
  "$read_python" - "$read_port" "$read_check" <<'PY'
import json
import os
from pathlib import Path
import secrets
import stat
import sys

from v8.read_api import ReadApiConfig
from v8.storage import connect, live_wal_read_only_connections, require_schema_compatibility

port = int(sys.argv[1])
if not 1024 <= port <= 65535:
    raise SystemExit("Reader port must be between 1024 and 65535")
config = ReadApiConfig.from_env()
# Validate installed FORMAL_READ lineage before creating a key or opening DB.
config.validate()
with live_wal_read_only_connections(), connect(config.db_path, read_only=True) as connection:
    require_schema_compatibility(connection, supported_versions=frozenset({19, 20, 21, 22, 23}))

key_path = config.key_path
if not key_path.is_absolute() or key_path.is_symlink():
    raise SystemExit("Reader key must be an absolute regular file, not a symlink")
key_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
created = False
try:
    descriptor = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
except FileExistsError:
    pass
else:
    with os.fdopen(descriptor, "w", encoding="ascii") as stream:
        stream.write(secrets.token_urlsafe(48) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    created = True
metadata = key_path.lstat()
if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077):
    raise SystemExit("Reader key must be owned by this user with mode 0600")
key = key_path.read_text(encoding="ascii").strip()
if not 32 <= len(key) <= 512 or any(ord(character) < 33 for character in key):
    raise SystemExit("Reader key has an invalid format")
if sys.argv[2] == "--check":
    print(json.dumps({"status": "validated", "read_only": True,
                      "scheduler_enabled": False, "key_initialized": created, "port": port}))
    raise SystemExit(0)
os.execvpe(sys.executable, [sys.executable, "-m", "v8.read_api", "--port", str(port)], os.environ)
PY
