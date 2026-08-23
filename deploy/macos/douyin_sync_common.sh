#!/bin/bash

dcar_sync_fail() {
  echo "Dcar Douyin sync tunnel preflight failed: $*" >&2
  exit 78
}

dcar_sync_mode() {
  local path="$1"
  local value=""
  if value="$(/usr/bin/stat -f '%Lp' "$path" 2>/dev/null)"; then
    printf '%s\n' "$value"
    return
  fi
  /usr/bin/stat -c '%a' "$path" 2>/dev/null || return 1
}

dcar_sync_uid() {
  local path="$1"
  local value=""
  if value="$(/usr/bin/stat -f '%u' "$path" 2>/dev/null)"; then
    printf '%s\n' "$value"
    return
  fi
  /usr/bin/stat -c '%u' "$path" 2>/dev/null || return 1
}

dcar_sync_load_env() {
  local sync_env="$1"
  [[ -n "$sync_env" && "$sync_env" = /* ]] || \
    dcar_sync_fail "DCAR_DOUYIN_SYNC_ENV_FILE must be an absolute path"
  [[ -f "$sync_env" && ! -L "$sync_env" ]] || \
    dcar_sync_fail "sync environment must be a regular non-symlink file"
  local mode
  mode="$(dcar_sync_mode "$sync_env")" || \
    dcar_sync_fail "cannot inspect sync environment mode"
  case "$mode" in
    400|600) ;;
    *) dcar_sync_fail "sync environment must have mode 0400 or 0600" ;;
  esac

  DCAR_DOUYIN_SSH_ALIAS_VALUE=""
  DCAR_DOUYIN_LOCAL_PORT_VALUE=""
  DCAR_DOUYIN_MACHINE_KEY_FILE_VALUE=""
  local raw_line line
  while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
    line="${raw_line%$'\r'}"
    case "$line" in
      ""|'#'*) continue ;;
      DCAR_DOUYIN_SSH_ALIAS=*)
        [[ -z "$DCAR_DOUYIN_SSH_ALIAS_VALUE" ]] || \
          dcar_sync_fail "duplicate DCAR_DOUYIN_SSH_ALIAS entry"
        DCAR_DOUYIN_SSH_ALIAS_VALUE="${line#DCAR_DOUYIN_SSH_ALIAS=}"
        ;;
      DCAR_DOUYIN_LOCAL_PORT=*)
        [[ -z "$DCAR_DOUYIN_LOCAL_PORT_VALUE" ]] || \
          dcar_sync_fail "duplicate DCAR_DOUYIN_LOCAL_PORT entry"
        DCAR_DOUYIN_LOCAL_PORT_VALUE="${line#DCAR_DOUYIN_LOCAL_PORT=}"
        ;;
      DCAR_DOUYIN_MACHINE_KEY_FILE=*)
        [[ -z "$DCAR_DOUYIN_MACHINE_KEY_FILE_VALUE" ]] || \
          dcar_sync_fail "duplicate DCAR_DOUYIN_MACHINE_KEY_FILE entry"
        DCAR_DOUYIN_MACHINE_KEY_FILE_VALUE="${line#DCAR_DOUYIN_MACHINE_KEY_FILE=}"
        ;;
      *) dcar_sync_fail "unsupported sync environment entry" ;;
    esac
  done < "$sync_env"

  [[ "$DCAR_DOUYIN_SSH_ALIAS_VALUE" =~ ^[A-Za-z0-9._-]+$ ]] || \
    dcar_sync_fail "DCAR_DOUYIN_SSH_ALIAS must be a dedicated SSH alias"
  [[ "$DCAR_DOUYIN_LOCAL_PORT_VALUE" == "14175" ]] || \
    dcar_sync_fail "DCAR_DOUYIN_LOCAL_PORT must be 14175"
  [[ -n "$DCAR_DOUYIN_MACHINE_KEY_FILE_VALUE" && \
     "$DCAR_DOUYIN_MACHINE_KEY_FILE_VALUE" = /* ]] || \
    dcar_sync_fail "DCAR_DOUYIN_MACHINE_KEY_FILE must be absolute"
  [[ -f "$DCAR_DOUYIN_MACHINE_KEY_FILE_VALUE" && \
     ! -L "$DCAR_DOUYIN_MACHINE_KEY_FILE_VALUE" ]] || \
    dcar_sync_fail "Machine credential must be a regular non-symlink file"
  mode="$(dcar_sync_mode "$DCAR_DOUYIN_MACHINE_KEY_FILE_VALUE")" || \
    dcar_sync_fail "cannot inspect Machine credential mode"
  case "$mode" in
    400|600) ;;
    *) dcar_sync_fail "Machine credential must have mode 0400 or 0600" ;;
  esac
}

dcar_sync_control_socket() {
  printf '%s\n' "$HOME/Library/Application Support/DcarAIGC/runtime/douyin-sync-tunnel.sock"
}

dcar_sync_expand_identity() {
  local identity="$1"
  case "$identity" in
    '~/'*) printf '%s\n' "$HOME/${identity#'~/'}" ;;
    /*) printf '%s\n' "$identity" ;;
    *) return 1 ;;
  esac
}

dcar_sync_known_hosts() {
  local known_hosts="$HOME/.ssh/known_hosts"
  [[ -f "$known_hosts" && ! -L "$known_hosts" ]] || \
    dcar_sync_fail "standard SSH known_hosts is missing or unsafe"
  local mode owner
  mode="$(dcar_sync_mode "$known_hosts")" || \
    dcar_sync_fail "cannot inspect known_hosts mode"
  owner="$(dcar_sync_uid "$known_hosts")" || \
    dcar_sync_fail "cannot inspect known_hosts owner"
  [[ "$owner" == "$(id -u)" ]] || dcar_sync_fail "known_hosts owner is unsafe"
  case "$mode" in
    400|600|644) ;;
    *) dcar_sync_fail "known_hosts mode is unsafe" ;;
  esac
  printf '%s\n' "$known_hosts"
}

dcar_sync_validate_alias() {
  local alias="$1"
  local resolved
  resolved="$(/usr/bin/ssh -G "$alias" 2>/dev/null)" || \
    dcar_sync_fail "SSH alias cannot be resolved"
  local user
  user="$(printf '%s\n' "$resolved" | /usr/bin/awk '$1 == "user" {print $2; exit}')"
  [[ "$user" == "dcar-douyin-sync" ]] || \
    dcar_sync_fail "SSH alias must resolve to User dcar-douyin-sync"
  if printf '%s\n' "$resolved" | \
      /usr/bin/awk '$1 == "localforward" || $1 == "remoteforward" || $1 == "dynamicforward" {found=1} END {exit !found}'; then
    dcar_sync_fail "SSH alias must not declare additional forwards"
  fi

  local identity candidate mode owner found=""
  while IFS= read -r identity; do
    candidate="$(dcar_sync_expand_identity "$identity")" || continue
    [[ -f "$candidate" && ! -L "$candidate" ]] || continue
    mode="$(dcar_sync_mode "$candidate")" || continue
    owner="$(dcar_sync_uid "$candidate")" || continue
    if [[ "$owner" == "$(id -u)" && ( "$mode" == "400" || "$mode" == "600" ) ]]; then
      found="$candidate"
      break
    fi
  done < <(printf '%s\n' "$resolved" | /usr/bin/awk '$1 == "identityfile" {print $2}')
  [[ -n "$found" ]] || \
    dcar_sync_fail "SSH alias has no safe dedicated IdentityFile"
}
