#!/usr/bin/env python3
"""Match nflreadr ff_playerids to existing players by sportradar_id.

This is the easiest match — sportradar_id in nflreadr = player_id in our DB.
Outputs data/matched/nflreadr_ids.json with new ID columns for each matched player.
"""

import json
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))
from shared import NFLREADR_DIR, MATCHED_DIR, read_csv, ensure_dir

# Columns to extract from ff_playerids (maps CSV column -> our DB column)
ID_COLUMNS = {
    "pff_id": "pff_id",
    "fantasypros_id": "fantasypros_id",
    "mfl_id": "mfl_id",
    "stats_id": "stats_id",
    "stats_global_id": "stats_global_id",
    "fantasy_data_id": "fantasy_data_id",
    "cbs_id": "cbs_id",
    "fleaflicker_id": "fleaflicker_id",
    "swish_id": "swish_id",
    "ktc_id": "ktc_id",
    "cfbref_id": "cfbref_id",
    "rotoworld_id": "rotoworld_id",
}


def main():
    csv_path = os.path.join(NFLREADR_DIR, "ff_playerids.csv")
    if not os.path.exists(csv_path):
        print(f"ERROR: {csv_path} not found. Download from nflverse first.")
        sys.exit(1)

    rows = read_csv(csv_path)
    print(f"Loaded {len(rows)} rows from ff_playerids.csv")

    # Build lookup by sportradar_id
    nflreadr_by_sr = {}
    for row in rows:
        sr_id = row.get("sportradar_id", "").strip()
        if sr_id:
            nflreadr_by_sr[sr_id] = row

    # Fetch existing players from Supabase
    from shared import get_all_players
    players = get_all_players()
    print(f"Fetched {len(players)} players from Supabase")

    # Match
    matched = {}
    for p in players:
        pid = p["player_id"]
        if pid in nflreadr_by_sr:
            nr = nflreadr_by_sr[pid]
            updates = {}
            for csv_col, db_col in ID_COLUMNS.items():
                val = nr.get(csv_col, "").strip()
                if val and val != "NA":
                    updates[db_col] = val
            if updates:
                matched[pid] = updates

    print(f"\nMatched {len(matched)} / {len(players)} players to nflreadr IDs")

    # Show coverage per ID
    for db_col in ID_COLUMNS.values():
        count = sum(1 for u in matched.values() if db_col in u)
        print(f"  {db_col}: {count}")

    # Save
    ensure_dir(MATCHED_DIR)
    out_path = os.path.join(MATCHED_DIR, "nflreadr_ids.json")
    with open(out_path, "w") as f:
        json.dump(matched, f, indent=2)
    print(f"\nSaved to {out_path}")


if __name__ == "__main__":
    main()
