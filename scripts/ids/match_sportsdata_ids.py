#!/usr/bin/env python3
"""Match SportsData.io players to existing players.

Pulls the full player list from SportsData.io API, matches to our DB
by name+team, and extracts sportsdata_id, fanduel_id, draftkings_id.
Outputs data/matched/sportsdata_ids.json.
"""

import json
import os
import sys
import urllib.request
sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    SPORTSDATA_KEY, MATCHED_DIR, ensure_dir,
    get_all_players, build_player_lookup, normalize_name, normalize_team
)


def fetch_sportsdata_players():
    """Fetch all players from SportsData.io API."""
    url = "https://api.sportsdata.io/v3/nfl/scores/json/Players"
    req = urllib.request.Request(url, headers={
        "Ocp-Apim-Subscription-Key": SPORTSDATA_KEY,
    })
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read().decode("utf-8"))


def main():
    print("Fetching players from SportsData.io API...")
    sd_players = fetch_sportsdata_players()
    print(f"Got {len(sd_players)} players from SportsData.io")

    print("Fetching players from Supabase...")
    db_players = get_all_players()
    print(f"Got {len(db_players)} players from Supabase")

    by_name_pos, by_name = build_player_lookup(db_players)

    matched = {}
    unmatched = []
    for sd in sd_players:
        sd_name = f"{sd.get('FirstName', '')} {sd.get('LastName', '')}".strip()
        norm = normalize_name(sd_name)
        pos = (sd.get("Position") or "").upper()
        # Normalize position for matching
        if pos in ("DEF", "DST"):
            continue  # Skip team defenses

        # Try name+position match first
        player_id = by_name_pos.get((norm, pos))
        if not player_id:
            # Try name-only match
            player_id = by_name.get(norm)

        if player_id:
            updates = {}
            sd_id = sd.get("PlayerID")
            if sd_id:
                updates["sportsdata_id"] = str(sd_id)
            fd_id = sd.get("FanDuelPlayerID")
            if fd_id:
                updates["fanduel_id"] = str(fd_id)
            dk_id = sd.get("DraftKingsPlayerID")
            if dk_id:
                updates["draftkings_id"] = str(dk_id)
            if updates:
                # Merge with existing matches (don't overwrite)
                if player_id in matched:
                    matched[player_id].update(updates)
                else:
                    matched[player_id] = updates
        else:
            if pos in ("QB", "RB", "WR", "TE", "K"):
                unmatched.append({"name": sd_name, "pos": pos, "team": sd.get("Team"),
                                  "sportsdata_id": sd.get("PlayerID")})

    print(f"\nMatched {len(matched)} / {len(db_players)} DB players to SportsData.io")
    for col in ["sportsdata_id", "fanduel_id", "draftkings_id"]:
        count = sum(1 for u in matched.values() if col in u)
        print(f"  {col}: {count}")

    if unmatched:
        print(f"\nUnmatched skill players from SportsData.io: {len(unmatched)} (showing first 20)")
        for u in unmatched[:20]:
            print(f"  {u['name']} ({u['pos']}, {u['team']}) - SD ID: {u['sportsdata_id']}")

    # Save
    ensure_dir(MATCHED_DIR)
    out_path = os.path.join(MATCHED_DIR, "sportsdata_ids.json")
    with open(out_path, "w") as f:
        json.dump(matched, f, indent=2)
    print(f"\nSaved to {out_path}")

    # Also save the full SportsData player list for later use
    sd_cache = os.path.join(MATCHED_DIR, "sportsdata_players_cache.json")
    with open(sd_cache, "w") as f:
        json.dump(sd_players, f)
    print(f"Cached full SportsData player list to {sd_cache}")


if __name__ == "__main__":
    main()
