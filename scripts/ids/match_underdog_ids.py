#!/usr/bin/env python3
"""Match Underdog Fantasy players to existing DB players.

Parses data/imports/underdog_ADP.csv and matches by firstName+lastName+position.
Outputs data/matched/underdog_ids.json.
"""

import json
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    IMPORTS_DIR, MATCHED_DIR, ensure_dir,
    get_all_players, build_player_lookup, normalize_name,
    read_csv, TEAM_FULLNAME_TO_ABBR
)


def main():
    csv_path = os.path.join(IMPORTS_DIR, "underdog_ADP.csv")
    ud_rows = read_csv(csv_path)
    print(f"Loaded {len(ud_rows)} players from underdog_ADP.csv")

    db_players = get_all_players()
    print(f"Got {len(db_players)} players from Supabase")

    by_name_pos, by_name = build_player_lookup(db_players)

    matched = {}
    unmatched = []
    for row in ud_rows:
        ud_id = row.get("id", "").strip()
        first = row.get("firstName", "").strip()
        last = row.get("lastName", "").strip()
        full_name = f"{first} {last}"
        norm = normalize_name(full_name)
        pos = row.get("slotName", "").upper().strip()

        # Try name+position
        player_id = by_name_pos.get((norm, pos))
        if not player_id:
            player_id = by_name.get(norm)

        if player_id and ud_id:
            matched[player_id] = {"underdog_id": ud_id}
        else:
            adp = row.get("adp", "999")
            try:
                adp_val = float(adp) if adp else 999
            except ValueError:
                adp_val = 999
            team_full = row.get("teamName", "")
            team_abbr = TEAM_FULLNAME_TO_ABBR.get(team_full.lower(), "")
            unmatched.append({
                "name": full_name, "pos": pos, "team": team_abbr,
                "adp": adp_val, "underdog_id": ud_id,
            })

    print(f"\nMatched {len(matched)} / {len(ud_rows)} Underdog players")

    # Show unmatched by ADP (most important ones first)
    unmatched.sort(key=lambda x: x["adp"])
    top_unmatched = [u for u in unmatched if u["adp"] < 300]
    if top_unmatched:
        print(f"\nUnmatched Underdog players with ADP < 300: {len(top_unmatched)}")
        for u in top_unmatched[:30]:
            print(f"  ADP {u['adp']:>6.1f}: {u['name']} ({u['pos']}, {u['team']}) UD ID: {u['underdog_id']}")

    # Save
    ensure_dir(MATCHED_DIR)
    out_path = os.path.join(MATCHED_DIR, "underdog_ids.json")
    with open(out_path, "w") as f:
        json.dump(matched, f, indent=2)
    print(f"\nSaved to {out_path}")

    # Also save unmatched for later insertion (Phase 2)
    unmatched_path = os.path.join(MATCHED_DIR, "underdog_unmatched.json")
    with open(unmatched_path, "w") as f:
        json.dump(unmatched, f, indent=2)
    print(f"Saved {len(unmatched)} unmatched to {unmatched_path}")


if __name__ == "__main__":
    main()
