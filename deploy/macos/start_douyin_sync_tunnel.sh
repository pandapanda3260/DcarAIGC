#!/bin/bash
set -euo pipefail

fail() {
  echo "Dcar Douyin sync tunnel start failed: $*" >&2
  exit 78
}

label="cn.tj.dcar.douyin-sync-tunnel"
domain="gui/$(id -u)"
plist="${DCAR_DOUYIN_TUNNEL_PLIST:-$HOME/Library/LaunchAgents/$label.plist}"
[[ "$plist" = /* && -f "$plist" && ! -L "$plist" ]] || \
  fail "rendered LaunchAgent plist is missing or unsafe"
/usr/bin/plutil -lint "$plist" >/dev/null || fail "LaunchAgent plist is invalid"

script_dir="$(cd "$(dirname "$0")" && pwd -P)"
if /bin/launchctl print "$domain/$label" >/dev/null 2>&1; then
  exec "$script_dir/check_douyin_sync_tunnel.sh"
fi

/bin/launchctl enable "$domain/$label"
/bin/launchctl bootstrap "$domain" "$plist"
for _attempt in $(seq 1 30); do
  if "$script_dir/check_douyin_sync_tunnel.sh" >/dev/null 2>&1; then
    echo "Dcar Douyin sync tunnel started and passed health"
    exit 0
  fi
  /bin/sleep 1
done

/bin/launchctl bootout "$domain" "$plist" >/dev/null 2>&1 || true
fail "tunnel did not become healthy within 30 seconds"
