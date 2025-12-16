default:
    just --list

init:
    just init-py

init-py:
    just py-venv && \
    just py-lock && \
    just py-deps

py-venv:
    uv venv --clear

py-lock:
    uv pip compile requirements.in -o requirements.txt

py-deps:
    VIRTUAL_ENV=.venv uv pip sync requirements.txt

import-arctic:
    python3 scripts/tools/import_arctic.py --root data/reddit/00_raw data/import/arctic/*_posts.jsonl data/import/arctic/*_comments.jsonl

pl-reddit:
    just pl-reddit-00 && \
    just pl-reddit-01 && \
    just pl-reddit-02 && \
    just pl-reddit-03

pl-reddit-00:
    bash scripts/pipeline/reddit/00_raw.sh

pl-reddit-01:
    bash scripts/pipeline/reddit/01_parquet.sh

pl-reddit-02:
    bash scripts/pipeline/reddit/02_staged.sh

pl-reddit-03:
    bash scripts/pipeline/reddit/03_index.sh
