#!/usr/bin/env python3
"""Fetch 2026 rookies from SportsData.io and match to our DB.

Uses the Rookies/{season} endpoint to get all 2026 draft picks,
matches to existing players, and identifies missing ones for insertion.
Merges results into sportsdata_ids.json.
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

SEASON = 2026


def fetch_rookies(season):
    """Fetch rookies from SportsData.io API."""
    url = f"https://api.sportsdata.io/v3/nfl/scores/json/Rookies/{season}"
    req = urllib.request.Request(url, headers={
        "Ocp-Apim-Subscription-Key": SPORTSDATA_KEY,
    })
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read().decode("utf-8"))


def main():
    print(f"Fetching {SEASON} rookies from SportsData.io...")
    rookies = fetch_rookies(SEASON)
    print(f"Got {len(rookies)} rookies from SportsData.io")

    # Filter to skill positions
    skill_positions = {"QB", "RB", "WR", "TE", "K"}
    skill_rookies = [r for r in rookies if (r.get("Position") or "").upper() in skill_positions]
    print(f"Skill-position rookies: {len(skill_rookies)}")

    print("Fetching players from Supabase...")
    db_players = get_all_players()
    print(f"Got {len(db_players)} players from Supabase")

    db_by_pid = {p["player_id"] for p in db_players}
    by_name_pos, by_name = build_player_lookup(db_players)

    # Load existing sportsdata_ids.json to merge into
    sd_ids_path = os.path.join(MATCHED_DIR, "sportsdata_ids.json")
    if os.path.exists(sd_ids_path):
        with open(sd_ids_path) as f:
            existing_sd = json.load(f)
    else:
        existing_sd = {}

    matched = 0
    new_ids = 0
    unmatched = []

    for r in skill_rookies:
        first = r.get("FirstName") or ""
        last = r.get("LastName") or ""
        full_name = f"{first} {last}".strip()
        norm = normalize_name(full_name)
        pos = (r.get("Position") or "").upper()
        team = normalize_team(r.get("Team") or "")
        sr_id = (r.get("SportRadarPlayerID") or "").strip()

        # Try sportradar match first
        player_id = None
        if sr_id and sr_id in db_by_pid:
            player_id = sr_id
        else:
            player_id = by_name_pos.get((norm, pos))
            if not player_id:
                player_id = by_name.get(norm)

        if player_id:
            matched += 1
            updates = existing_sd.get(player_id, {})
            added = 0
            sd_id = r.get("PlayerID")
            if sd_id and "sportsdata_id" not in updates:
                updates["sportsdata_id"] = str(sd_id)
                added += 1
            fd_id = r.get("FanDuelPlayerID")
            if fd_id and "fanduel_id" not in updates:
                updates["fanduel_id"] = str(fd_id)
                added += 1
            dk_id = r.get("DraftKingsPlayerID")
            if dk_id and "draftkings_id" not in updates:
                updates["draftkings_id"] = str(dk_id)
                added += 1
            if updates:
                existing_sd[player_id] = updates
                new_ids += added
        else:
            unmatched.append({
                "name": full_name,
                "pos": pos,
                "team": team,
                "sportradar_id": sr_id,
                "sportsdata_id": r.get("PlayerID"),
                "fanduel_id": r.get("FanDuelPlayerID"),
                "draftkings_id": r.get("DraftKingsPlayerID"),
                "college": r.get("College"),
                "draft_round": r.get("DraftRound"),
                "draft_pick": r.get("DraftPick"),
                "height": r.get("Height"),
                "weight": r.get("Weight"),
            })

    print(f"\nMatched {matched} / {len(skill_rookies)} rookies to DB")
    print(f"New ID values added: {new_ids}")
    print(f"Unmatched rookies: {len(unmatched)}")

    if unmatched:
        print("\nUnmatched rookies (potential additions):")
        for u in unmatched:
            print(f"  {u['name']:30s} {u['pos']:3s} {u['team']:4s}  "
                  f"SR={u.get('sportradar_id', '')[:8]}  "
                  f"SD={u.get('sportsdata_id', '')}  "
                  f"College={u.get('college', '')}")

    # Save merged sportsdata_ids
    ensure_dir(MATCHED_DIR)
    with open(sd_ids_path, "w") as f:
        json.dump(existing_sd, f, indent=2)
    print(f"\nSaved merged sportsdata_ids to {sd_ids_path}")

    # Save unmatched rookies for add_missing_rookies.py
    unmatched_path = os.path.join(MATCHED_DIR, "sportsdata_unmatched_rookies.json")
    with open(unmatched_path, "w") as f:
        json.dump(unmatched, f, indent=2)
    print(f"Saved {len(unmatched)} unmatched rookies to {unmatched_path}")


if __name__ == "__main__":
    main()
