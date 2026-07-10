"""One-time backfill of the staged FBG newsletter items into news_items.

Reads the 652 items parsed from FBG's daily emails (staging/fbg-daily-news/
news_items.jsonl, 2026-04-27 → 2026-07-05), resolves each to a player, classifies
news_type, and upserts into news_items (insert-only on source_url). Unresolved
items (no matchable player — player_id is NOT NULL) are skipped and logged.

The daily /updates scraper (fetch_fbg_news.py) picks up from here forward.

Usage:
    python3 scripts/news/backfill_staged_news.py --dry-run
    python3 scripts/news/backfill_staged_news.py
    python3 scripts/news/backfill_staged_news.py --file /path/to/news_items.jsonl
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from news_shared import (  # noqa: E402
    fetch_players, build_name_index, resolve_player_id,
    classify_news_type, to_row, upsert_news,
)

DEFAULT_JSONL = os.path.expanduser("~/dev/nfl-db/staging/fbg-daily-news/news_items.jsonl")
UNRESOLVED_LOG = os.path.expanduser("~/dev/nfl-db/data/logs/fbg_news_unresolved.jsonl")


def main():
    ap = argparse.ArgumentParser(description="Backfill staged FBG news into news_items.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--file", default=DEFAULT_JSONL)
    args = ap.parse_args()

    with open(args.file) as f:
        items = [json.loads(line) for line in f if line.strip()]
    print(f"Loaded {len(items)} staged items from {args.file}")

    print("Building player name index...")
    name_index = build_name_index(fetch_players())
    print(f"  {len(name_index)} indexed names")

    rows, unresolved = [], []
    from collections import Counter
    types = Counter()
    for it in items:
        if not it.get("source_url"):
            continue
        pid = resolve_player_id(it, name_index)
        if not pid:
            unresolved.append(it)
            continue
        nt = classify_news_type(it.get("headline"), it.get("fact_text"))
        types[nt] += 1
        rows.append(to_row(it, pid, nt))

    print(f"\n  Resolved: {len(rows)}  |  Unresolved (skipped): {len(unresolved)}")
    print(f"  news_type mix: {dict(types)}")
    if unresolved[:8]:
        print("  Sample unresolved:")
        for it in unresolved[:8]:
            print(f"    [{it.get('team')}] {it.get('headline')}")

    if args.dry_run:
        print(f"\n[dry-run] would upsert {len(rows)} rows to news_items.")
        return

    os.makedirs(os.path.dirname(UNRESOLVED_LOG), exist_ok=True)
    with open(UNRESOLVED_LOG, "a") as f:
        for it in unresolved:
            f.write(json.dumps(it) + "\n")
    print(f"\nUpserting {len(rows)} rows to news_items (insert-only on source_url)...")
    ok, errors = upsert_news(rows)
    print(f"  Sent: {ok}  Errors: {errors}  (existing source_urls silently ignored)")
    print(f"  {len(unresolved)} unresolved logged to {UNRESOLVED_LOG}")


if __name__ == "__main__":
    main()
