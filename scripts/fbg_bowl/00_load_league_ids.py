"""Load FBG Bowl league IDs into fbg_bowl_leagues (sleeper_id + year only).

Usage:
  python3 scripts/fbg_bowl/00_load_league_ids.py [--year 2025] [--dry-run]

2025: reads local CSV at /Users/dan/Desktop/r2024/fbg_bowl_standings_new/
2024: reads local CSV (deduplicated from standings league_id column)

Only inserts sleeper_id + year. Script 01 fills name/scoring_type/roster_count.
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from shared import supa_insert, supa_get, save_checkpoint

import pandas as pd

CSV_2025 = "/Users/dan/Desktop/r2024/fbg_bowl_standings_new/FBG Bowl 2025 League IDs - Sheet1.csv"
CSV_2024 = "/Users/dan/Desktop/r2024/fbg_bowl_standings_new/FBG Bowl 2024 League IDs.csv"


def load_2025_ids():
    df = pd.read_csv(CSV_2025, dtype=str)
    col = df.columns[0]  # "league_id"
    ids = df[col].dropna().str.strip().tolist()
    print(f"  Loaded {len(ids)} league IDs from CSV")
    return ids


def load_2024_ids():
    df = pd.read_csv(CSV_2024, dtype=str)
    col = df.columns[0]  # "league_id"
    ids = df[col].dropna().str.strip().unique().tolist()
    print(f"  Loaded {len(ids)} unique league IDs from CSV")
    return ids


def main():
    dry_run = "--dry-run" in sys.argv
    year = 2025
    for i, arg in enumerate(sys.argv):
        if arg == "--year" and i + 1 < len(sys.argv):
            year = int(sys.argv[i + 1])

    print(f"Loading {year} league IDs...")

    if year == 2025:
        ids = load_2025_ids()
    elif year == 2024:
        ids = load_2024_ids()
    else:
        print(f"No source configured for year {year}")
        return

    if not ids:
        print("No IDs to load.")
        return

    # Check which are already in DB
    existing = supa_get("fbg_bowl_leagues", select="sleeper_id", params=f"year=eq.{year}")
    existing_ids = {r["sleeper_id"] for r in existing}
    new_ids = [i for i in ids if i not in existing_ids]
    print(f"  Already in DB: {len(existing_ids)}")
    print(f"  New to insert: {len(new_ids)}")

    if not new_ids:
        print("All IDs already loaded.")
        save_checkpoint(f"league_ids_{year}", ids)
        return

    rows = [{"sleeper_id": lid, "year": year} for lid in new_ids]

    if dry_run:
        print(f"\n[DRY RUN] Would insert {len(rows)} rows. Sample: {rows[:3]}")
        return

    inserted = supa_insert("fbg_bowl_leagues", rows)
    print(f"  Inserted: {len(inserted)} leagues")

    # Save full list for downstream scripts
    all_ids = [r["sleeper_id"] for r in supa_get("fbg_bowl_leagues", select="sleeper_id", params=f"year=eq.{year}")]
    save_checkpoint(f"league_ids_{year}", all_ids)
    print(f"  Checkpoint saved: league_ids_{year}.json ({len(all_ids)} total)")


if __name__ == "__main__":
    main()
