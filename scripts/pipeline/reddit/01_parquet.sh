#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CFG="${CFG:-$ROOT_DIR/config/pipeline/reddit/01_parquet.yaml}"

log() { printf '[%s] %s\n' "$1" "$2" >&2; }
utc_now() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
sec_now() { date -u +%s; }

yaml_get() {
  local k="$1"
  awk -v k="$k" -F':' '$1==k {sub(/^[[:space:]]+/, "", $2); sub(/^[[:space:]]+/, "", $2); print $2; exit}' "$CFG" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'
}

yaml_list() {
  local k="$1"
  awk -v k="$k" '
    $1==k":" {inside=1; next}
    inside && /^[[:space:]]*-[[:space:]]/ {sub(/^[[:space:]]*-[[:space:]]*/, "", $0); print; next}
    inside && /^[^[:space:]]/ {inside=0}
  ' "$CFG"
}

require_file() {
  [[ -f "$1" ]] || { log ERROR "config not found: $1"; exit 1; }
}

require_bin() {
  command -v "$1" >/dev/null 2>&1 || { log ERROR "missing binary: $1"; exit 1; }
}

ensure_dir() {
  mkdir -p "$1"
}

move_parquet_16hex() {
  local src_dir="$1"
  local dst_dir="$2"
  ensure_dir "$dst_dir"
  shopt -s nullglob
  local files=("$src_dir"/*.parquet)
  if [[ ${#files[@]} -eq 0 ]]; then
    return 0
  fi
  for f in "${files[@]}"; do
    local sum
    sum="$(sha256sum "$f" | awk '{print $1}')"
    local moved=0
    for off in 0 16 32 48; do
      local h="${sum:$off:16}"
      if [[ ! -e "$dst_dir/$h.parquet" ]]; then
        mv -f "$f" "$dst_dir/$h.parquet"
        moved=1
        break
      fi
    done
    if [[ $moved -ne 1 ]]; then
      log ERROR "hash collision (extremely unlikely) for $f"
      exit 1
    fi
  done
}

main() {
  require_file "$CFG"
  require_bin duckdb
  require_bin sha256sum

  local RAW_ROOT PARQUET_ROOT COMPRESSION FILE_SIZE_BYTES
  RAW_ROOT="$(yaml_get raw_root)"
  PARQUET_ROOT="$(yaml_get parquet_root)"
  COMPRESSION="$(yaml_get compression)"
  FILE_SIZE_BYTES="$(yaml_get file_size_bytes)"

  ensure_dir "$PARQUET_ROOT/submissions"
  ensure_dir "$PARQUET_ROOT/comments"

  local t0
  t0="$(sec_now)"
  log INFO "start ts=$(utc_now) task=pl-reddit-01"

  mapfile -t subs < <(yaml_list subreddits | sed '/^\s*$/d')
  if [[ ${#subs[@]} -eq 0 ]]; then
    log ERROR "no subreddits in config: $CFG"
    exit 1
  fi

  for sub in "${subs[@]}"; do
    local subdir_raw="$RAW_ROOT/r_${sub}"
    if [[ ! -d "$subdir_raw" ]]; then
      log WARN "skip subreddit=$sub reason=no_raw_dir dir=$subdir_raw"
      continue
    fi

    local sub_glob="$subdir_raw/submissions/*/*.jsonl"
    local cmt_glob="$subdir_raw/comments/*/*.jsonl"

    if compgen -G "$sub_glob" >/dev/null; then
      local tmp_out
      tmp_out="$(mktemp -d)"
      duckdb :memory: <<SQL
COPY (
  SELECT
    regexp_extract(filename, '/r_([^/]+)/', 1) AS subreddit,
    coalesce(author, '') AS author,
    id AS submission_id,
    cast(created_utc as BIGINT) AS created_utc,
    cast(epoch(strptime(regexp_extract(filename, '/([0-9]{12})_[^/]+\\.jsonl$', 1), '%y%m%d%H%M%S')) as BIGINT) AS capture_utc,
    coalesce(title, '') AS title,
    coalesce(selftext, '') AS body,
    coalesce(permalink, '') AS permalink
  FROM read_json_auto('$sub_glob', format='newline_delimited')
  WHERE id IS NOT NULL
) TO '$tmp_out'
(FORMAT parquet, COMPRESSION '$COMPRESSION', FILE_SIZE_BYTES $FILE_SIZE_BYTES, PER_THREAD_OUTPUT);
SQL
      move_parquet_16hex "$tmp_out" "$PARQUET_ROOT/submissions/r_${sub}"
      rm -rf "$tmp_out"
      log INFO "submissions ok subreddit=$sub"
    else
      log WARN "submissions skip subreddit=$sub reason=no_files"
    fi

    if compgen -G "$cmt_glob" >/dev/null; then
      local tmp_out
      tmp_out="$(mktemp -d)"
      duckdb :memory: <<SQL
COPY (
  SELECT
    regexp_extract(filename, '/r_([^/]+)/', 1) AS subreddit,
    coalesce(author, '') AS author,
    regexp_extract(link_id, '^t3_(.+)$', 1) AS submission_id,
    id AS comment_id,
    parent_id AS parent_id,
    cast(created_utc as BIGINT) AS created_utc,
    cast(epoch(strptime(regexp_extract(filename, '/([0-9]{12})_[^/]+\\.jsonl$', 1), '%y%m%d%H%M%S')) as BIGINT) AS capture_utc,
    coalesce(body, '') AS body,
    coalesce(permalink, '') AS permalink
  FROM read_json_auto('$cmt_glob', format='newline_delimited')
  WHERE id IS NOT NULL AND link_id LIKE 't3_%'
) TO '$tmp_out'
(FORMAT parquet, COMPRESSION '$COMPRESSION', FILE_SIZE_BYTES $FILE_SIZE_BYTES, PER_THREAD_OUTPUT);
SQL
      move_parquet_16hex "$tmp_out" "$PARQUET_ROOT/comments/r_${sub}"
      rm -rf "$tmp_out"
      log INFO "comments ok subreddit=$sub"
    else
      log WARN "comments skip subreddit=$sub reason=no_files"
    fi
  done

  local t1
  t1="$(sec_now)"
  log INFO "end ts=$(utc_now) task=pl-reddit-01 elapsed=$((t1 - t0))s"
}

main "$@"
