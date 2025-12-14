default:
    just --list

init:

pl-reddit-00:
    bash scripts/pipeline/reddit/00_raw.sh
