#!/usr/bin/env python3
"""Match DraftKings players to existing DB players.

Parses data/imports/DkPreDraftRankings.csv and matches by name+position.
Outputs data/matched/dk_ids.json.
"""

import json
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    IMPORTS_DIR, MATCHED_DIR, ensure_dir,
    get_all_players, build_player_lookup, normalize_name, normalize_team,
    read_csv
)


def main():
    csv_path = os.path.join(IMPORTS_DIR, "DkPreDraftRankings.csv")
    dk_rows = read_csv(csv_path)
    print(f"Loaded {len(dk_rows)} players from DkPreDraftRankings.csv")

    db_players = get_all_players()
    print(f"Got {len(db_players)} players from Supabase")

    by_name_pos, by_name = build_player_lookup(db_players)

    matched = {}
    unmatched = []
    for row in dk_rows:
        dk_id = row.get("ID", "").strip()
        name = row.get("Name", "").strip()
        norm = normalize_name(name)
        pos = row.get("Position", "").upper().strip()

        player_id = by_name_pos.get((norm, pos))
        if not player_id:
            player_id = by_name.get(norm)

        if player_id and dk_id:
            matched[player_id] = {"draftkings_id": dk_id}
        else:
            team = normalize_team(row.get("Team", ""))
            adp = row.get("ADP", "999")
            try:
                adp_val = float(adp) if adp else 999
            except ValueError:
                adp_val = 999
            unmatched.append({
                "name": name, "pos": pos, "team": team,
                "adp": adp_val, "dk_id": dk_id,
            })

    print(f"\nMatched {len(matched)} / {len(dk_rows)} DraftKings players")

    unmatched.sort(key=lambda x: x["adp"])
    top_unmatched = [u for u in unmatched if u["adp"] < 200]
    if top_unmatched:
        print(f"\nUnmatched DK players with ADP < 200: {len(top_unmatched)}")
        for u in top_unmatched[:20]:
            print(f"  ADP {u['adp']:>6.1f}: {u['name']} ({u['pos']}, {u['team']}) DK ID: {u['dk_id']}")

    ensure_dir(MATCHED_DIR)
    out_path = os.path.join(MATCHED_DIR, "dk_ids.json")
    with open(out_path, "w") as f:
        json.dump(matched, f, indent=2)
    print(f"\nSaved to {out_path}")


if __name__ == "__main__":
    main()
