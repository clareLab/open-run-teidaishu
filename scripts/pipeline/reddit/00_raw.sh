#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

source "$ROOT_DIR/scripts/lib/yaml.sh"
source "$ROOT_DIR/scripts/lib/log.sh"
source "$ROOT_DIR/scripts/lib/progress.sh"

CFG="${CFG:-$ROOT_DIR/config/pipeline/reddit/00_raw.yaml}"

: "${REDDIT_CLIENT_ID:?REDDIT_CLIENT_ID not set}"
: "${REDDIT_CLIENT_SECRET:?REDDIT_CLIENT_SECRET not set}"
: "${REDDIT_USER_AGENT:?REDDIT_USER_AGENT not set}"

if [[ ! -f "$CFG" ]]; then
  log_error "config not found: $CFG"
  exit 1
fi

RAW_ROOT="$(yaml_get "$CFG" "paths.raw_root")"
APPS_ROOT="$(yaml_get "$CFG" "paths.apps_root")"
DAYS="$(yaml_get "$CFG" "reddit.days")"
LIMIT="$(yaml_get "$CFG" "reddit.limit")"
SLEEP_SEC="$(yaml_get "$CFG" "runtime.sleep_sec")"
LOG_LEVEL="$(yaml_get "$CFG" "logging.level")"
PROGRESS="$(yaml_get "$CFG" "logging.progress")"

RAW_ROOT="${RAW_ROOT:-data/reddit/00_raw}"
APPS_ROOT="${APPS_ROOT:-apps/reddit/harvest}"
DAYS="${DAYS:-7}"
LIMIT="${LIMIT:-100}"
SLEEP_SEC="${SLEEP_SEC:-0}"
LOG_LEVEL="${LOG_LEVEL:-info}"
PROGRESS="${PROGRESS:-true}"

mkdir -p "$ROOT_DIR/$RAW_ROOT"

mapfile -t subs < <(yaml_list "$CFG" "reddit.subreddits")
TOTAL="${#subs[@]}"
if [[ "$TOTAL" -eq 0 ]]; then
  log_error "no subreddits found in $CFG"
  exit 1
fi

run_submissions() {
  local sub="$1"
  (cd "$ROOT_DIR/$APPS_ROOT" && go run ./cmd/submissions -sub "$sub" -days "$DAYS" -root "$ROOT_DIR/$RAW_ROOT" -limit "$LIMIT")
}

run_comments() {
  local sub="$1"
  (cd "$ROOT_DIR/$APPS_ROOT" && go run ./cmd/comments -sub "$sub" -days "$DAYS" -root "$ROOT_DIR/$RAW_ROOT")
}

errs=0
idx=0

log_info "start cfg=$CFG raw_root=$RAW_ROOT days=$DAYS limit=$LIMIT subs=$TOTAL"

for s in "${subs[@]}"; do
  idx=$((idx+1))
  sub="${s#r/}"
  sub="${sub#r_}"

  if [[ "$PROGRESS" == "true" ]]; then progress_render "$idx" "$TOTAL"; fi

  log_info "sub=$sub phase=submissions"
  if ! run_submissions "$sub"; then
    log_error "sub=$sub phase=submissions failed"
    errs=$((errs+1))
    continue
  fi

  log_info "sub=$sub phase=comments"
  if ! run_comments "$sub"; then
    log_error "sub=$sub phase=comments failed"
    errs=$((errs+1))
    continue
  fi

  if [[ "$SLEEP_SEC" != "0" ]]; then
    sleep "$SLEEP_SEC"
  fi
done

if [[ "$PROGRESS" == "true" ]]; then progress_done; fi

if [[ "$errs" -gt 0 ]]; then
  log_error "done errors=$errs"
  exit 2
fi

log_info "done ok"
