#!/usr/bin/env bash
set -euo pipefail

_log_rank() {
  case "${1:-info}" in
    error) echo 0 ;;
    warn) echo 1 ;;
    info) echo 2 ;;
    debug) echo 3 ;;
    *) echo 2 ;;
  esac
}

log_ts() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log_emit() {
  local level="$1"; shift
  local msg="$*"
  local cfg_level="${LOG_LEVEL:-info}"
  if [ "$(_log_rank "$level")" -le "$(_log_rank "$cfg_level")" ]; then
    printf "[%s] [%s] %s\n" "$(log_ts)" "$level" "$msg" >&2
  fi
}

log_info(){ log_emit info "$*"; }
log_warn(){ log_emit warn "$*"; }
log_error(){ log_emit error "$*"; }
log_debug(){ log_emit debug "$*"; }
