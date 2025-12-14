#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CFG="${CFG:-$ROOT_DIR/config/pipeline/reddit/02_staged.yaml}"

log() { printf '[%s] %s\n' "$1" "$2" >&2; }
utc_now() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
sec_now() { date -u +%s; }

cfg_scalar() {
  local key="$1"
  local line
  line="$(grep -E "^[[:space:]]*${key}:" "$CFG" | head -n 1 || true)"
  [[ -z "$line" ]] && return 0
  printf '%s' "$line" \
    | sed -E "s/^[[:space:]]*${key}:[[:space:]]*//" \
    | sed -E 's/[[:space:]]*#.*$//' \
    | sed -E 's/[[:space:]]*$//'
}

cfg_list() {
  local key="$1"
  awk -v k="$key" '
    BEGIN{inside=0}
    $0 ~ ("^" k ":[[:space:]]*$") { inside=1; next }
    inside && $0 ~ "^[[:space:]]*-[[:space:]]+" {
      sub(/^[[:space:]]*-[[:space:]]+/, "", $0)
      sub(/[[:space:]]*#.*$/, "", $0)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0)
      if ($0 != "") print
      next
    }
    inside && $0 ~ "^[^[:space:]]" { inside=0 }
  ' "$CFG"
}

main() {
  [[ -f "$CFG" ]] || { log ERROR "config not found: $CFG"; exit 1; }

  local PARQUET_ROOT STAGED_ROOT
  PARQUET_ROOT="$(cfg_scalar parquet_root)"
  STAGED_ROOT="$(cfg_scalar staged_root)"
  PARQUET_ROOT="${PARQUET_ROOT:-data/reddit/01_parquet}"
  STAGED_ROOT="${STAGED_ROOT:-data/reddit/02_staged}"

  local lookback_days cutoff_key cutoff_iso
  lookback_days="$(cfg_scalar lookback_days || true)"
  lookback_days="${LOOKBACK_DAYS:-$lookback_days}"
  lookback_days="${lookback_days:-0}"
  [[ "$lookback_days" =~ ^[0-9]+$ ]] || { log ERROR "bad lookback_days=$lookback_days"; exit 1; }

  if [[ "$lookback_days" -le 0 ]]; then
    cutoff_key=""
    cutoff_iso="1970-01-01T00:00:00Z"
  else
    cutoff_key="$(date -u -d "-${lookback_days} days" +%y%m%d%H%M%S)"
    cutoff_iso="$(date -u -d "-${lookback_days} days" +%Y-%m-%dT%H:%M:%SZ)"
  fi

  mapfile -t subs < <(cfg_list subreddits)
  [[ ${#subs[@]} -gt 0 ]] || { log ERROR "no subreddits in $CFG"; exit 1; }

  local t0 t1
  t0="$(sec_now)"
  log INFO "start ts=$(utc_now) task=pl-reddit-02 cfg=$CFG lookback_days=$lookback_days cutoff_iso=$cutoff_iso cutoff_key=${cutoff_key:-none}"

  local g_dirs_scan=0 g_dirs_skip=0 g_write=0 g_skip_exist=0 g_skip_empty=0

  for sub in "${subs[@]}"; do
    log INFO "subreddit=$sub begin"

    local base_in="$ROOT_DIR/$PARQUET_ROOT/r_${sub}"
    if [[ ! -d "$base_in" ]]; then
      log WARN "subreddit=$sub skip reason=no_01_dir path=$base_in"
      continue
    fi

    local kind
    for kind in submissions comments; do
      local in_root="$base_in/$kind"
      if [[ ! -d "$in_root" ]]; then
        log WARN "subreddit=$sub kind=$kind missing_dir path=$in_root"
        continue
      fi

      local dir_scan=0 dir_skip=0 wrote=0 skip_exist=0 skip_empty=0

      while IFS= read -r d; do
        dir_scan=$((dir_scan+1)); g_dirs_scan=$((g_dirs_scan+1))

        local created_id created_key
        created_id="$(basename "$d")"
        created_key="${created_id%%_*}"

        if [[ "$lookback_days" -gt 0 ]]; then
          if [[ ${#created_key} -ne 12 || "$created_key" < "$cutoff_key" ]]; then
            dir_skip=$((dir_skip+1)); g_dirs_skip=$((g_dirs_skip+1))
            log INFO "subreddit=$sub kind=$kind action=skip scope=dir created_id=$created_id reason=prune created_key=$created_key cutoff_key=$cutoff_key"
            continue
          fi
        fi

        local latest
        latest="$(find "$d" -maxdepth 1 -type f -name '*.parquet' | sort | tail -n 1 || true)"
        if [[ -z "$latest" ]]; then
          skip_empty=$((skip_empty+1)); g_skip_empty=$((g_skip_empty+1))
          log INFO "subreddit=$sub kind=$kind action=skip scope=dir created_id=$created_id reason=no_parquet"
          continue
        fi

        local fname stem capture_ts hash out_dir out
        fname="$(basename "$latest")"
        stem="${fname%.parquet}"
        capture_ts="${stem%%_*}"
        hash="${stem#*_}"

        out_dir="$ROOT_DIR/$STAGED_ROOT/r_${sub}/${kind}"
        out="$out_dir/${created_id}_${capture_ts}_${hash}.parquet"

        if [[ -f "$out" ]]; then
          skip_exist=$((skip_exist+1)); g_skip_exist=$((g_skip_exist+1))
          log INFO "subreddit=$sub kind=$kind action=skip scope=dir created_id=$created_id reason=exists out=$(basename "$out") src=$fname"
          continue
        fi

        mkdir -p "$out_dir"
        cp -f "$latest" "$out"
        wrote=$((wrote+1)); g_write=$((g_write+1))
        log INFO "subreddit=$sub kind=$kind action=write created_id=$created_id latest=$fname out=$(basename "$out")"
      done < <(find "$in_root" -maxdepth 1 -mindepth 1 -type d | sort)

      log INFO "subreddit=$sub kind=$kind stats dirs_scanned=$dir_scan dirs_skipped=$dir_skip wrote=$wrote skip_exists=$skip_exist skip_empty=$skip_empty"
    done

    log INFO "subreddit=$sub end"
  done

  t1="$(sec_now)"
  log INFO "done ts=$(utc_now) task=pl-reddit-02 elapsed=$((t1 - t0))s total_dirs_scanned=$g_dirs_scan total_dirs_skipped=$g_dirs_skip total_wrote=$g_write total_skip_exists=$g_skip_exist total_skip_empty=$g_skip_empty"
}

main "$@"
