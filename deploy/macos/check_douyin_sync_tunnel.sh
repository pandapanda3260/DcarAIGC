#!/bin/bash
set -euo pipefail
umask 077

script_dir="$(cd "$(dirname "$0")" && pwd -P)"
# shellcheck source=deploy/macos/douyin_sync_common.sh
source "$script_dir/douyin_sync_common.sh"

dcar_sync_load_env "${DCAR_DOUYIN_SYNC_ENV_FILE:-}"
control_socket="$(dcar_sync_control_socket)"
[[ -S "$control_socket" ]] || dcar_sync_fail "SSH control socket is not active"
/usr/bin/ssh -S "$control_socket" -O check \
  "$DCAR_DOUYIN_SSH_ALIAS_VALUE" >/dev/null 2>&1 || \
  dcar_sync_fail "SSH tunnel process is not healthy"

machine_key="$(/bin/cat "$DCAR_DOUYIN_MACHINE_KEY_FILE_VALUE")"
[[ "$machine_key" =~ ^[A-Za-z0-9._~+/=-]{32,512}$ ]] || \
  dcar_sync_fail "Machine credential format is invalid"

{
  printf 'silent\nshow-error\nfail\nconnect-timeout = 2\nmax-time = 5\n'
  printf 'header = "X-Dcar-Machine-Key: %s"\n' "$machine_key"
} | /usr/bin/curl --config - \
  "http://127.0.0.1:${DCAR_DOUYIN_LOCAL_PORT_VALUE}/internal/v1/health"
printf '\nDcar Douyin sync tunnel health passed\n'
