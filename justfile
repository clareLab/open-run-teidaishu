default:
    just --list

init:

import-arctic:
    python3 scripts/tools/import_arctic.py --root data/reddit/00_raw data/import/arctic/*_posts.jsonl data/import/arctic/*_comments.jsonl

pl-reddit-00:
    bash scripts/pipeline/reddit/00_raw.sh

pl-reddit-01:
    bash scripts/pipeline/reddit/01_parquet.sh

pl-reddit-02:
    bash scripts/pipeline/reddit/02_staged.sh
