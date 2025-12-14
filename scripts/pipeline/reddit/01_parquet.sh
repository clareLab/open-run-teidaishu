#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CFG="${CFG:-$ROOT_DIR/config/pipeline/reddit/01_parquet.yaml}"

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

esc_sql() { printf "%s" "$1" | sed "s/'/''/g"; }

main() {
  command -v duckdb >/dev/null 2>&1 || { log ERROR "missing binary: duckdb"; exit 1; }
  [[ -f "$CFG" ]] || { log ERROR "config not found: $CFG"; exit 1; }

  local RAW_ROOT PARQUET_ROOT COMPRESSION
  RAW_ROOT="$(cfg_scalar raw_root)"
  PARQUET_ROOT="$(cfg_scalar parquet_root)"
  COMPRESSION="$(cfg_scalar compression)"
  RAW_ROOT="${RAW_ROOT:-data/reddit/00_raw}"
  PARQUET_ROOT="${PARQUET_ROOT:-data/reddit/01_parquet}"
  COMPRESSION="${COMPRESSION:-zstd}"

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
  log INFO "start ts=$(utc_now) task=pl-reddit-01 cfg=$CFG lookback_days=$lookback_days cutoff_iso=$cutoff_iso cutoff_key=${cutoff_key:-none} compression=$COMPRESSION"

  local g_dirs_scan=0 g_dirs_skip=0 g_files_scan=0 g_write=0 g_skip_exist=0

  for sub in "${subs[@]}"; do
    log INFO "subreddit=$sub begin"

    local raw_base="$ROOT_DIR/$RAW_ROOT/r_${sub}"
    if [[ ! -d "$raw_base" ]]; then
      log WARN "subreddit=$sub skip reason=no_raw_dir path=$raw_base"
      continue
    fi

    local kind
    for kind in submissions comments; do
      local in_root="$raw_base/$kind"
      if [[ ! -d "$in_root" ]]; then
        log WARN "subreddit=$sub kind=$kind missing_dir path=$in_root"
        continue
      fi

      local dir_scan=0 dir_skip=0 file_scan=0 wrote=0 skip_exist=0

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

        while IFS= read -r f; do
          file_scan=$((file_scan+1)); g_files_scan=$((g_files_scan+1))

          local base capture_ts hash out_dir out
          base="$(basename "$f" .jsonl)"
          capture_ts="${base%%_*}"
          hash="${base#*_}"

          out_dir="$ROOT_DIR/$PARQUET_ROOT/r_${sub}/${kind}/${created_id}"
          out="$out_dir/${capture_ts}_${hash}.parquet"

          if [[ -f "$out" ]]; then
            skip_exist=$((skip_exist+1)); g_skip_exist=$((g_skip_exist+1))
            log INFO "subreddit=$sub kind=$kind action=skip scope=file created_id=$created_id file=$(basename "$f") reason=exists out=$out"
            continue
          fi

          mkdir -p "$out_dir"

          local in_esc out_esc sid
          in_esc="$(esc_sql "$f")"
          out_esc="$(esc_sql "$out")"
          sid="${created_id#*_}"

          if [[ "$kind" == "submissions" ]]; then
            duckdb :memory: -c "
              COPY (
                SELECT
                  coalesce(author, '') AS author,
                  '${sid}' AS submission_id,
                  CAST(created_utc AS BIGINT) AS created_utc,
                  CAST(epoch(strptime('${capture_ts}', '%y%m%d%H%M%S')) AS BIGINT) AS capture_utc,
                  coalesce(title, '') AS title,
                  coalesce(selftext, '') AS body
                FROM read_json('${in_esc}', format='newline_delimited')
                LIMIT 1
              ) TO '${out_esc}' (FORMAT parquet, COMPRESSION '${COMPRESSION}');
            " >/dev/null
          else
            duckdb :memory: -c "
              COPY (
                SELECT
                  coalesce(author, '') AS author,
                  '${sid}' AS submission_id,
                  coalesce(id, '') AS comment_id,
                  coalesce(parent_id, '') AS parent_id,
                  CAST(created_utc AS BIGINT) AS created_utc,
                  CAST(epoch(strptime('${capture_ts}', '%y%m%d%H%M%S')) AS BIGINT) AS capture_utc,
                  coalesce(body, '') AS body
                FROM read_json('${in_esc}', format='newline_delimited')
                WHERE id IS NOT NULL
              ) TO '${out_esc}' (FORMAT parquet, COMPRESSION '${COMPRESSION}');
            " >/dev/null
          fi

          wrote=$((wrote+1)); g_write=$((g_write+1))
          log INFO "subreddit=$sub kind=$kind action=write created_id=$created_id file=$(basename "$f") out=$out"
        done < <(find "$d" -maxdepth 1 -type f -name '*.jsonl' | sort)
      done < <(find "$in_root" -maxdepth 1 -mindepth 1 -type d | sort)

      log INFO "subreddit=$sub kind=$kind stats dirs_scanned=$dir_scan dirs_skipped=$dir_skip files_scanned=$file_scan wrote=$wrote skip_exists=$skip_exist"
    done

    log INFO "subreddit=$sub end"
  done

  t1="$(sec_now)"
  log INFO "done ts=$(utc_now) task=pl-reddit-01 elapsed=$((t1 - t0))s total_dirs_scanned=$g_dirs_scan total_dirs_skipped=$g_dirs_skip total_files_scanned=$g_files_scan total_wrote=$g_write total_skip_exists=$g_skip_exist"
}

main "$@"
