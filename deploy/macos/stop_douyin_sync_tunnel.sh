#!/bin/bash
set -euo pipefail

fail() {
  echo "Dcar Douyin sync tunnel stop failed: $*" >&2
  exit 78
}

label="cn.tj.dcar.douyin-sync-tunnel"
domain="gui/$(id -u)"
plist="${DCAR_DOUYIN_TUNNEL_PLIST:-$HOME/Library/LaunchAgents/$label.plist}"
[[ "$plist" = /* && -f "$plist" && ! -L "$plist" ]] || \
  fail "rendered LaunchAgent plist is missing or unsafe"

if /bin/launchctl print "$domain/$label" >/dev/null 2>&1; then
  /bin/launchctl bootout "$domain" "$plist"
fi
/bin/launchctl disable "$domain/$label"

control_socket="$HOME/Library/Application Support/DcarAIGC/runtime/douyin-sync-tunnel.sock"
for _attempt in $(seq 1 10); do
  [[ ! -S "$control_socket" ]] && break
  /bin/sleep 1
done
[[ ! -S "$control_socket" ]] || fail "SSH control socket remained active"
echo "Dcar Douyin sync tunnel stopped and disabled"
