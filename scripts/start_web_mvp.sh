#!/bin/bash
set -euo pipefail

project_root="$(cd "$(dirname "$0")/.." && pwd)"

if lsof -nP -iTCP:4173 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "DCar Insight 启动失败：127.0.0.1:4173 已被占用" >&2
  exit 1
fi
if lsof -nP -iTCP:8765 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "DCar Insight 启动失败：127.0.0.1:8765 已被占用" >&2
  exit 1
fi

cd "$project_root"
DCAR_SCHEDULER_ENABLED="${DCAR_SCHEDULER_ENABLED:-1}" python3 -m uv run --frozen uvicorn v8.api:app \
  --app-dir "$project_root/src/dcar_eval" \
  --host 127.0.0.1 \
  --port 8765 \
  --no-access-log &
api_pid=$!

cleanup() {
  kill "$api_pid" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

cd "$project_root/app/web"
WRANGLER_LOG_PATH=.wrangler/wrangler.log npm exec --yes --package=node@22.13.1 -- \
  node ./node_modules/vinext/dist/cli.js dev --hostname 127.0.0.1 --port 4173
