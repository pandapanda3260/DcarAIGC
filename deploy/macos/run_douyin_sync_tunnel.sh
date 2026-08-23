#!/bin/bash
set -euo pipefail
umask 077

script_dir="$(cd "$(dirname "$0")" && pwd -P)"
# shellcheck source=deploy/macos/douyin_sync_common.sh
source "$script_dir/douyin_sync_common.sh"

project_root="${DCAR_PROJECT_ROOT:-}"
[[ -n "$project_root" && "$project_root" = /* && -d "$project_root" ]] || \
  dcar_sync_fail "DCAR_PROJECT_ROOT must be an existing absolute directory"
project_root="$(cd "$project_root" && pwd -P)"
[[ "$script_dir" == "$project_root/deploy/macos" ]] || \
  dcar_sync_fail "tunnel wrapper does not belong to DCAR_PROJECT_ROOT"

dcar_sync_load_env "${DCAR_DOUYIN_SYNC_ENV_FILE:-}"
known_hosts="$(dcar_sync_known_hosts)"
dcar_sync_validate_alias "$DCAR_DOUYIN_SSH_ALIAS_VALUE"

runtime_dir="$HOME/Library/Application Support/DcarAIGC/runtime"
/bin/mkdir -p "$runtime_dir"
/bin/chmod 0700 "$runtime_dir"
control_socket="$(dcar_sync_control_socket)"

echo "Dcar Douyin sync tunnel starting on 127.0.0.1:14175 -> 127.0.0.1:4175"
exec /usr/bin/ssh \
  -NT \
  -M \
  -S "$control_socket" \
  -o BatchMode=yes \
  -o PasswordAuthentication=no \
  -o KbdInteractiveAuthentication=no \
  -o IdentitiesOnly=yes \
  -o StrictHostKeyChecking=yes \
  -o "UserKnownHostsFile=$known_hosts" \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -o TCPKeepAlive=yes \
  -o ControlMaster=yes \
  -o ControlPersist=no \
  -L "127.0.0.1:${DCAR_DOUYIN_LOCAL_PORT_VALUE}:127.0.0.1:4175" \
  "$DCAR_DOUYIN_SSH_ALIAS_VALUE"
