#!/usr/bin/env python3
"""Match Footballguys player IDs to existing DB players.

Uses the FBG crosswalk CSV (from Google Sheet) which maps FBG IDs to SportsDataIO IDs.
Chains through our SportsData matches to link FBG IDs to our player_id.
Also falls back to name matching for any remaining.
Outputs data/matched/fbg_ids.json.
"""

import json
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    IMPORTS_DIR, MATCHED_DIR, ensure_dir,
    get_all_players, build_player_lookup, normalize_name,
    read_csv
)


def main():
    # Load FBG crosswalk
    crosswalk_path = os.path.join(IMPORTS_DIR, "fbg_crosswalk.csv")
    crosswalk = read_csv(crosswalk_path)
    print(f"Loaded {len(crosswalk)} rows from FBG crosswalk")

    # Load raw FBG IDs (from projections API)
    raw_ids_path = os.path.join(MATCHED_DIR, "fbg_raw_ids.json")
    if os.path.exists(raw_ids_path):
        with open(raw_ids_path) as f:
            raw_fbg_ids = set(json.load(f))
        print(f"Have {len(raw_fbg_ids)} FBG IDs from projections API")

    # Load SportsData matched IDs (to chain: FBG -> SD -> player_id)
    sd_matched_path = os.path.join(MATCHED_DIR, "sportsdata_ids.json")
    sd_to_pid = {}
    if os.path.exists(sd_matched_path):
        with open(sd_matched_path) as f:
            sd_matched = json.load(f)
        for pid, ids in sd_matched.items():
            sd_id = ids.get("sportsdata_id", "")
            if sd_id:
                sd_to_pid[sd_id] = pid
    print(f"Have {len(sd_to_pid)} SportsData→player_id mappings")

    # Fetch DB players for name-based fallback
    db_players = get_all_players()
    by_name_pos, by_name = build_player_lookup(db_players)

    matched = {}
    matched_via_sd = 0
    matched_via_name = 0
    unmatched = []

    for row in crosswalk:
        fbg_id = row.get("ID", "").strip()
        name = row.get("Name", "").strip()
        sd_id = row.get("SportsDataIO ID", "").strip()
        pos = row.get("Position", "").upper().strip()

        if not fbg_id:
            continue

        player_id = None

        # Strategy 1: Chain through SportsDataIO ID
        if sd_id and sd_id != "-":
            player_id = sd_to_pid.get(sd_id)
            if player_id:
                matched_via_sd += 1

        # Strategy 2: Name matching fallback
        if not player_id and name:
            norm = normalize_name(name)
            player_id = by_name_pos.get((norm, pos))
            if not player_id:
                player_id = by_name.get(norm)
            if player_id:
                matched_via_name += 1

        if player_id:
            matched[player_id] = {"footballguys_id": fbg_id}
        else:
            unmatched.append({"fbg_id": fbg_id, "name": name, "pos": pos})

    print(f"\nMatched {len(matched)} / {len(crosswalk)} FBG players")
    print(f"  via SportsDataIO crosswalk: {matched_via_sd}")
    print(f"  via name matching: {matched_via_name}")

    if unmatched:
        print(f"\nUnmatched FBG players: {len(unmatched)} (showing first 10)")
        for u in unmatched[:10]:
            print(f"  {u['name']} ({u['pos']}) - FBG ID: {u['fbg_id']}")

    ensure_dir(MATCHED_DIR)
    out_path = os.path.join(MATCHED_DIR, "fbg_ids.json")
    with open(out_path, "w") as f:
        json.dump(matched, f, indent=2)
    print(f"\nSaved to {out_path}")


if __name__ == "__main__":
    main()
