#!/usr/bin/env bash
set -euo pipefail

progress_render() {
  local cur="$1"
  local total="$2"
  local width="${3:-28}"

  if [ "$total" -le 0 ]; then
    printf "\r[----------------------------] 0/0\n" >&2
    return
  fi

  local pct=$((cur * 100 / total))
  local fill=$((cur * width / total))
  local empty=$((width - fill))

  printf "\r[" >&2
  if [ "$fill" -gt 0 ]; then printf "%0.s#" $(seq 1 "$fill") >&2; fi
  if [ "$empty" -gt 0 ]; then printf "%0.s-" $(seq 1 "$empty") >&2; fi
  printf "] %d%% %d/%d" "$pct" "$cur" "$total" >&2
}

progress_done() {
  printf "\n" >&2
}
