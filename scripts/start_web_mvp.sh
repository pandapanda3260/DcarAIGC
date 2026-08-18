#!/bin/bash
set -euo pipefail

project_root="$(cd "$(dirname "$0")/.." && pwd)"
freeze_lock="${DCAR_OPERATOR_FREEZE_LOCK:-${DCAR_FREEZE_LOCK:-$project_root/runtime/operator-freeze.lock}}"
scheduler_enabled="${DCAR_SCHEDULER_ENABLED:-0}"
startup_catchup_enabled="${DCAR_STARTUP_CATCHUP_ENABLED:-0}"
daily_capture_reconcile_from="${DCAR_DAILY_CAPTURE_RECONCILE_FROM:-}"
auth_root="${DCAR_AUTH_RUNTIME_ROOT:-$project_root/runtime/auth}"
htpasswd_file="${DCAR_AUTH_HTPASSWD:-$auth_root/users.htpasswd}"
session_db="${DCAR_AUTH_SESSION_DB:-$auth_root/sessions.sqlite3}"
reuse_api="${DCAR_REUSE_EXISTING_READ_ONLY_API:-0}"

if [[ "$scheduler_enabled" != "0" ]]; then
  echo "DCar Insight 8765 启动失败：DCAR_SCHEDULER_ENABLED 必须为 0；调度仅允许在 8766 writer 运行" >&2
  exit 78
fi
if [[ "$startup_catchup_enabled" != "0" ]]; then
  echo "DCar Insight 8765 启动失败：DCAR_STARTUP_CATCHUP_ENABLED 必须为 0" >&2
  exit 78
fi
if [[ -n "$daily_capture_reconcile_from" ]]; then
  echo "DCar Insight 8765 启动失败：DCAR_DAILY_CAPTURE_RECONCILE_FROM 仅允许由 8766 writer 使用" >&2
  exit 78
fi
unset DCAR_DAILY_CAPTURE_RECONCILE_FROM

if [[ -e "$freeze_lock" ]]; then
  if [[ "$reuse_api" != "1" ]]; then
    echo "DCar Insight 启动已被运维冻结锁阻止：$freeze_lock" >&2
    exit 75
  fi
  echo "检测到运维冻结锁；仅复用现有只读 API，不启动或修改 API。"
fi

ports=(4173 4174)
if [[ "$reuse_api" != "1" ]]; then
  ports+=(8765)
fi
for port in "${ports[@]}"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "DCar Insight 启动失败：127.0.0.1:${port} 已被占用" >&2
    exit 1
  fi
done

if [[ ! -s "$htpasswd_file" ]]; then
  if [[ ! -t 0 ]]; then
    echo "首次启动需要在终端创建本地登录账号：scripts/start_web_mvp.sh" >&2
    exit 78
  fi
  cd "$project_root"
  python3 -m uv run --frozen python scripts/create_local_auth_user.py \
    --output "$htpasswd_file"
fi

cd "$project_root"
echo "DCar 自动调度：${scheduler_enabled}；启动补跑：${startup_catchup_enabled}"
api_pid=""
if [[ "$reuse_api" == "1" ]]; then
  api_health="$(curl -fsS http://127.0.0.1:8765/api/v8/health)" || {
    echo "DCar Insight 启动失败：现有 127.0.0.1:8765 API 不可用" >&2
    exit 1
  }
  API_HEALTH="$api_health" python3 - <<'PY'
import json
import os

health = json.loads(os.environ["API_HEALTH"])
if health.get("mode") != "read_only_replica" or health.get("read_only") is not True:
    raise SystemExit("现有 127.0.0.1:8765 不是只读副本，拒绝复用")
PY
  echo "复用现有只读 API：127.0.0.1:8765"
else
  DCAR_SCHEDULER_ENABLED=0 \
  DCAR_STARTUP_CATCHUP_ENABLED=0 \
  python3 -m uv run --frozen uvicorn v8.api:app \
    --app-dir "$project_root/src/dcar_eval" \
    --host 127.0.0.1 \
    --port 8765 \
    --no-access-log &
  api_pid=$!
fi

cd "$project_root/app/web"
WRANGLER_LOG_PATH=.wrangler/wrangler.log npm exec --yes --package=node@22.13.1 -- \
  node ./node_modules/vinext/dist/cli.js dev --hostname 127.0.0.1 --port 4174 &
web_pid=$!

cleanup() {
  if [[ -n "$api_pid" ]]; then
    kill "$api_pid" 2>/dev/null || true
  fi
  kill "$web_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for _ in {1..60}; do
  if curl -fsS -o /dev/null http://127.0.0.1:8765/api/v8/health \
    && curl -fsS -o /dev/null http://127.0.0.1:4174/; then
    break
  fi
  sleep 0.25
done
if ! curl -fsS -o /dev/null http://127.0.0.1:8765/api/v8/health \
  || ! curl -fsS -o /dev/null http://127.0.0.1:4174/; then
  echo "DCar Insight 启动失败：API 或 Web 上游未就绪" >&2
  exit 1
fi

cd "$project_root"
DCAR_AUTH_BASE_PATH="" \
DCAR_AUTH_WEB_UPSTREAM=http://127.0.0.1:4174 \
DCAR_AUTH_API_UPSTREAM=http://127.0.0.1:8765 \
DCAR_AUTH_HTPASSWD="$htpasswd_file" \
DCAR_AUTH_SESSION_DB="$session_db" \
DCAR_AUTH_LOGIN_TEMPLATE="$project_root/deploy/server/nginx/login.html" \
DCAR_AUTH_SECURE_COOKIE=0 \
python3 -m uv run --frozen uvicorn dcar_auth.gateway:app \
  --app-dir "$project_root/src/dcar_eval" \
  --host 127.0.0.1 \
  --port 4173 \
  --workers 1 \
  --no-access-log
