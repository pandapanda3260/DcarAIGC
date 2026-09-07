#!/bin/bash
# Real Web + real auth gateway + log SMS + disposable API upstream.
# All state and ports are temporary; no existing Dcar process or database is used.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
web_root="$repo_root/app/web"
python_bin="${PYTHON:-$repo_root/.venv/bin/python}"
base_path="${DCAR_SMOKE_BASE_PATH:-}"
if [[ -n "$base_path" && ( "$base_path" != /* || "$base_path" == */ ) ]]; then
  echo "DCAR_SMOKE_BASE_PATH must be empty or start with / without a trailing /" >&2
  exit 2
fi

if [[ ! -x "$python_bin" ]]; then
  echo "Python environment not found: $python_bin (run: uv sync --frozen)" >&2
  exit 2
fi

state_dir="$(mktemp -d "${TMPDIR:-/tmp}/dcar-auth-integration-smoke.XXXXXX")"
gateway_pid=""
api_pid=""
web_pid=""

cleanup() {
  status=$?
  trap - EXIT INT TERM
  for pid in "$gateway_pid" "$web_pid" "$api_pid"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  for pid in "$gateway_pid" "$web_pid" "$api_pid"; do
    if [[ -n "$pid" ]]; then
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ "$status" -ne 0 ]]; then
    for log in gateway web api; do
      if [[ -s "$state_dir/$log.log" ]]; then
        echo "--- $log log (tail) ---" >&2
        tail -80 "$state_dir/$log.log" >&2
      fi
    done
  fi
  rm -rf "$state_dir"
  exit "$status"
}
trap cleanup EXIT INT TERM

read -r gateway_port web_port api_port < <(
  "$python_bin" - <<'PY'
import socket

sockets = []
try:
    for _ in range(3):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        sockets.append(sock)
    print(*(sock.getsockname()[1] for sock in sockets))
finally:
    for sock in sockets:
        sock.close()
PY
)

umask 077
"$python_bin" -c 'import secrets; print(secrets.token_hex(32))' > "$state_dir/pepper"

SMOKE_DB="$state_dir/sessions.sqlite3" PYTHONPATH="$repo_root/src/dcar_eval" \
  "$python_bin" - <<'PY'
import os
from pathlib import Path

from dcar_auth.store import AuthStore, hash_password

store = AuthStore(Path(os.environ["SMOKE_DB"]))
store.initialize()
password_hash = hash_password("T3mp-Smoke-Passphrase!")
for username, phone, role, status in (
    ("super_smoke", "13800138101", "superadmin", "active"),
    ("manager_smoke", "13800138102", "operator", "active"),
    ("operator_smoke", "13800138103", "operator", "active"),
    ("code_smoke", "13800138104", "operator", "active"),
    ("reset_smoke", "13800138105", "operator", "active"),
    ("disabled_smoke", "13800138106", "operator", "disabled"),
    ("delete_smoke", "13800138107", "operator", "active"),
):
    store.create_user(
        username,
        password_hash,
        phone=phone,
        role=role,
        status=status,
        actor="browser-smoke",
    )
store.allow_phone("13800138108", "browser-smoke", actor="browser-smoke")
PY

"$python_bin" -m uvicorn auth_integration_smoke_upstream:app \
  --app-dir "$repo_root/tests" --host 127.0.0.1 --port "$api_port" \
  --log-level warning >"$state_dir/api.log" 2>&1 &
api_pid=$!

if [[ "${DCAR_SMOKE_SKIP_WEB_BUILD:-0}" != "1" ]]; then
  DCAR_WEB_BASE_PATH="$base_path" NEXT_PUBLIC_DCAR_API_BASE="$base_path" \
    npm --prefix "$web_root" run build
fi
npm --prefix "$web_root" run start -- --hostname 127.0.0.1 --port "$web_port" \
  >"$state_dir/web.log" 2>&1 &
web_pid=$!

DCAR_AUTH_BASE_PATH="$base_path" \
DCAR_AUTH_WEB_UPSTREAM="http://127.0.0.1:$web_port" \
DCAR_AUTH_API_UPSTREAM="http://127.0.0.1:$api_port" \
DCAR_AUTH_SESSION_DB="$state_dir/sessions.sqlite3" \
DCAR_AUTH_LOGIN_TEMPLATE="$repo_root/deploy/server/nginx/login.html" \
DCAR_AUTH_SECURE_COOKIE=0 \
DCAR_AUTH_SMS_PROVIDER=log \
DCAR_AUTH_PEPPER_FILE="$state_dir/pepper" \
DCAR_AUTH_CHANGE_LOG="$state_dir/auth-changes.log" \
DCAR_AUTH_FAILURE_DELAY_SECONDS=0 \
PYTHONPATH="$repo_root/src/dcar_eval" \
  "$python_bin" -m uvicorn dcar_auth.gateway:app --host 127.0.0.1 \
  --port "$gateway_port" --workers 1 --no-access-log --log-level warning \
  >"$state_dir/gateway.log" 2>&1 &
gateway_pid=$!

wait_for_url() {
  url=$1
  label=$2
  for _ in $(seq 1 160); do
    if curl -fsS -o /dev/null "$url" 2>/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  echo "$label did not become ready: $url" >&2
  return 1
}

wait_for_url "http://127.0.0.1:$api_port/api/v8/health" "stub API"
wait_for_url "http://127.0.0.1:$web_port$base_path/users" "Web"
wait_for_url "http://127.0.0.1:$gateway_port$base_path/auth/health" "auth gateway"

DCAR_SMOKE_BASE_URL="http://127.0.0.1:$gateway_port$base_path" \
DCAR_SMOKE_GATEWAY_LOG="$state_dir/gateway.log" \
  npm --prefix "$web_root" exec --yes --package=node@22.13.1 -- \
  node "$repo_root/tests/auth_integration_browser_smoke.mjs"
