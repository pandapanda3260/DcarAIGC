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
auth_bypass="${DCAR_AUTH_BYPASS:-0}"
reuse_api="${DCAR_REUSE_EXISTING_READ_ONLY_API:-0}"
api_upstream=""

case "$auth_bypass" in
  1|true|TRUE|yes|YES|on|ON) auth_bypass=1 ;;
  *) auth_bypass=0 ;;
esac

if [[ "$scheduler_enabled" != "0" ]]; then
  echo "DCar Insight 本地操作台启动失败：DCAR_SCHEDULER_ENABLED 必须为 0；调度仅允许在 8766 writer 运行" >&2
  exit 78
fi
if [[ "$startup_catchup_enabled" != "0" ]]; then
  echo "DCar Insight 本地操作台启动失败：DCAR_STARTUP_CATCHUP_ENABLED 必须为 0" >&2
  exit 78
fi
if [[ -n "$daily_capture_reconcile_from" ]]; then
  echo "DCar Insight 本地操作台启动失败：DCAR_DAILY_CAPTURE_RECONCILE_FROM 仅允许由 8766 writer 使用" >&2
  exit 78
fi
unset DCAR_DAILY_CAPTURE_RECONCILE_FROM

if [[ -e "$freeze_lock" ]]; then
  if [[ "$reuse_api" != "1" ]]; then
    echo "DCar Insight 启动已被运维冻结锁阻止：$freeze_lock" >&2
    exit 75
  fi
  api_upstream="http://127.0.0.1:8765"
  echo "检测到运维冻结锁；仅复用现有只读 API，不启动或修改正式 API。"
elif [[ "$reuse_api" == "1" ]]; then
  echo "DCar Insight 启动失败：只有 operator freeze 生效时才能复用 8765 只读副本" >&2
  exit 78
else
  api_upstream="http://127.0.0.1:8766"
fi

ports=(4173 4174)
for port in "${ports[@]}"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "DCar Insight 启动失败：127.0.0.1:${port} 已被占用" >&2
    exit 1
  fi
done

validate_read_only_replica() {
  API_HEALTH="$1" python3 - <<'PY'
import json
import os

health = json.loads(os.environ["API_HEALTH"])
if health.get("mode") != "read_only_replica" or health.get("read_only") is not True:
    raise SystemExit("8765 不是只读副本")
PY
}

if [[ "$reuse_api" == "1" ]]; then
  api_health="$(curl -fsS "$api_upstream/api/v8/health")" || {
    echo "DCar Insight 启动失败：现有 127.0.0.1:8765 API 不可用" >&2
    exit 78
  }
  if ! validate_read_only_replica "$api_health"; then
    echo "DCar Insight 启动失败：现有 127.0.0.1:8765 不是只读副本" >&2
    exit 78
  fi
  echo "复用 freeze 只读 API：127.0.0.1:8765"
else
  if lsof -nP -iTCP:8765 -sTCP:LISTEN >/dev/null 2>&1; then
    residual_health="$(curl -fsS http://127.0.0.1:8765/api/v8/health)" || {
      echo "DCar Insight 启动失败：8765 被未知进程占用，且无法验证其只读身份" >&2
      exit 78
    }
    if validate_read_only_replica "$residual_health"; then
      echo "警告：检测到残留的 8765 只读副本；本地操作台仍将使用 8766。" >&2
    else
      echo "DCar Insight 启动失败：8765 存在非只读或身份不明的 API" >&2
      exit 78
    fi
  fi

  api_health="$(curl -fsS "$api_upstream/api/v8/health")" || {
    echo "DCar Insight 启动失败：正式 API 127.0.0.1:8766 不可用" >&2
    exit 78
  }
  scheduler_health="$(curl -fsS "$api_upstream/api/v8/scheduler")" || {
    echo "DCar Insight 启动失败：无法读取 8766 调度状态" >&2
    exit 78
  }
  if ! python3 - 3<<<"$api_health" 4<<<"$scheduler_health" <<'PY'
import json
import os

with os.fdopen(3, encoding="utf-8") as health_stream:
    health = json.load(health_stream)
with os.fdopen(4, encoding="utf-8") as scheduler_stream:
    scheduler = json.load(scheduler_stream)
database_state = health.get("database_state") or {}
schema = database_state.get("schema_compatibility") or {}
checks = (
    (health.get("status") == "ok", "8766 health status 不是 ok"),
    (health.get("mode") == "local_v8", "8766 不是 local_v8 正式 API"),
    (health.get("read_only") is False, "8766 仍处于只读模式"),
    (health.get("database") == "dcar_insight.sqlite3", "8766 未连接正式数据库"),
    (schema.get("compatible") is True, "8766 数据库 schema 不兼容"),
    (scheduler.get("read_only") is False, "8766 调度端点处于只读模式"),
    (scheduler.get("requested") is True, "8766 scheduler 未请求启用"),
    (scheduler.get("enabled") is True, "8766 scheduler 未启用"),
    ((scheduler.get("writer_lock") or {}).get("held") is True, "8766 未持有 writer lock"),
    (
        (scheduler.get("daily_capture_reconcile") or {}).get("enabled") is True,
        "8766 daily capture reconcile 未启用",
    ),
    (
        (scheduler.get("report_runtime") or {}).get("ready") is True,
        "8766 report runtime 未就绪",
    ),
)
for passed, message in checks:
    if not passed:
        raise SystemExit(message)
PY
  then
    echo "DCar Insight 启动失败：8766 未满足正式本地操作台运行合同" >&2
    exit 78
  fi
fi

if [[ "$auth_bypass" != "1" && ! -s "$htpasswd_file" ]]; then
  if [[ ! -t 0 ]]; then
    echo "首次启动需要在终端创建本地登录账号：scripts/start_web_mvp.sh" >&2
    exit 78
  fi
  cd "$project_root"
  python3 -m uv run --frozen python scripts/create_local_auth_user.py \
    --output "$htpasswd_file"
fi

cd "$project_root"
if [[ "$reuse_api" == "1" ]]; then
  echo "本地页面已连接 freeze 只读副本；页面写操作已禁用。"
else
  echo "本地操作台已连接正式数据库；页面写操作会立即生效。"
  echo "自动调度由 8766 writer 提供；本地网页进程不启动调度。"
fi
if [[ "$auth_bypass" == "1" ]]; then
  echo "账号密码登录：已临时关闭（当前可免登录访问）"
else
  echo "账号密码登录：已启用"
fi
cd "$project_root/app/web"
WRANGLER_LOG_PATH=.wrangler/wrangler.log npm exec --yes --package=node@22.13.1 -- \
  node ./node_modules/vinext/dist/cli.js dev --hostname 127.0.0.1 --port 4174 &
web_pid=$!

cleanup() {
  kill "$web_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for _ in {1..60}; do
  if curl -fsS -o /dev/null "$api_upstream/api/v8/health" \
    && curl -fsS -o /dev/null http://127.0.0.1:4174/; then
    break
  fi
  sleep 0.25
done
if ! curl -fsS -o /dev/null "$api_upstream/api/v8/health" \
  || ! curl -fsS -o /dev/null http://127.0.0.1:4174/; then
  echo "DCar Insight 启动失败：API 或 Web 上游未就绪" >&2
  exit 1
fi

cd "$project_root"
DCAR_AUTH_BASE_PATH="" \
DCAR_AUTH_WEB_UPSTREAM=http://127.0.0.1:4174 \
DCAR_AUTH_API_UPSTREAM="$api_upstream" \
DCAR_AUTH_HTPASSWD="$htpasswd_file" \
DCAR_AUTH_SESSION_DB="$session_db" \
DCAR_AUTH_LOGIN_TEMPLATE="$project_root/deploy/server/nginx/login.html" \
DCAR_AUTH_SECURE_COOKIE=0 \
DCAR_AUTH_BYPASS="$auth_bypass" \
python3 -m uv run --frozen uvicorn dcar_auth.gateway:app \
  --app-dir "$project_root/src/dcar_eval" \
  --host 127.0.0.1 \
  --port 4173 \
  --workers 1 \
  --no-access-log
