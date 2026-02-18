"""Enrich players with data from the FBG NFLPlayers.json endpoint.

Fetches https://appdata.footballguys.com/tn/NFLPlayers.json and matches
to our players table using multiple strategies:
  1. footballguys_id (exact match on FBG id)
  2. fantasy_data_id (exact match on FBG fd_id)
  3. mfl_id (exact match on FBG mfl_id)
  4. name + position fallback

Updates: footballguys_id, fantasy_data_id, height, weight (only fills gaps).

Usage:
    python3 scripts/ids/enrich_from_fbg.py
"""

import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    normalize_name,
    normalize_team,
    supabase_rest_patch,
    PLAYER_ALIASES,
)

FBG_URL = "https://appdata.footballguys.com/tn/NFLPlayers.json"

# FBG pos -> our pos
POS_MAP = {
    "qb": "QB", "rb": "RB", "wr": "WR", "te": "TE", "pk": "K",
    "fb": "RB",  # fullbacks grouped with RB
}


def fetch_fbg_players():
    """Download the FBG NFLPlayers.json."""
    req = urllib.request.Request(FBG_URL, headers={"User-Agent": "Mozilla/5.0"})
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read().decode("utf-8"))


def fetch_our_players():
    """Fetch all players with ID columns needed for matching."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    limit = 1000
    select = "player_id,first_name,last_name,position,footballguys_id,fantasy_data_id,mfl_id,height,weight"
    while True:
        url = f"{SUPABASE_URL}/rest/v1/players?select={select}&offset={offset}&limit={limit}"
        req = urllib.request.Request(url, headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
        })
        resp = urllib.request.urlopen(req)
        batch = json.loads(resp.read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += limit
    return players


def build_lookups(players):
    """Build multiple lookup dicts for matching."""
    by_fbg_id = {}       # footballguys_id -> player
    by_fd_id = {}         # fantasy_data_id -> player
    by_mfl_id = {}        # mfl_id -> player
    by_name_pos = {}      # (norm_name, pos) -> player
    by_name = {}          # norm_name -> player

    for p in players:
        if p.get("footballguys_id"):
            by_fbg_id[p["footballguys_id"]] = p
        if p.get("fantasy_data_id"):
            by_fd_id[p["fantasy_data_id"]] = p
        if p.get("mfl_id"):
            by_mfl_id[p["mfl_id"]] = p

        full_name = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
        norm = normalize_name(full_name)
        pos = (p.get("position") or "").upper()
        by_name_pos[(norm, pos)] = p
        by_name[norm] = p
        # Also index aliases
        alias = PLAYER_ALIASES.get(norm)
        if alias:
            by_name_pos[(alias, pos)] = p
            by_name[alias] = p

    return by_fbg_id, by_fd_id, by_mfl_id, by_name_pos, by_name


def main():
    # ── 1. Fetch data ────────────────────────────────────────────────────────
    print("Fetching FBG NFLPlayers.json...")
    fbg_players = fetch_fbg_players()
    print(f"  {len(fbg_players)} players from FBG")

    # Filter to fantasy-relevant positions
    fbg_fantasy = [p for p in fbg_players if p.get("pos", "").lower() in POS_MAP]
    print(f"  {len(fbg_fantasy)} fantasy-relevant (QB/RB/WR/TE/K)")

    print("Fetching our players from Supabase...")
    our_players = fetch_our_players()
    print(f"  {len(our_players)} players in DB")

    by_fbg_id, by_fd_id, by_mfl_id, by_name_pos, by_name = build_lookups(our_players)

    # ── 2. Match and collect updates ─────────────────────────────────────────
    matched = 0
    unmatched = 0
    updates_to_apply = []  # (player_id, updates_dict, match_method, fbg_name)

    for fbg in fbg_fantasy:
        fbg_id = fbg.get("id", "")
        fd_id = str(fbg.get("fd_id", "")) if fbg.get("fd_id") else ""
        mfl_id = str(fbg.get("mfl_id", "")) if fbg.get("mfl_id") else ""
        fbg_name = f"{fbg.get('first', '')} {fbg.get('last', '')}".strip()
        fbg_pos = POS_MAP.get(fbg.get("pos", "").lower(), "")

        # Try matching in priority order
        our = None
        method = ""

        if fbg_id and fbg_id in by_fbg_id:
            our = by_fbg_id[fbg_id]
            method = "fbg_id"
        elif fd_id and fd_id in by_fd_id:
            our = by_fd_id[fd_id]
            method = "fd_id"
        elif mfl_id and mfl_id != "0" and mfl_id in by_mfl_id:
            our = by_mfl_id[mfl_id]
            method = "mfl_id"
        else:
            norm = normalize_name(fbg_name)
            our = by_name_pos.get((norm, fbg_pos)) or by_name.get(norm)
            if our:
                method = "name"

        if not our:
            unmatched += 1
            continue

        matched += 1
        player_id = our["player_id"]
        updates = {}

        # Fill footballguys_id if missing
        if not our.get("footballguys_id") and fbg_id:
            updates["footballguys_id"] = fbg_id

        # Fill fantasy_data_id if missing
        if not our.get("fantasy_data_id") and fd_id:
            updates["fantasy_data_id"] = fd_id

        # Fill height if missing
        if not our.get("height") and fbg.get("height"):
            updates["height"] = fbg["height"]

        # Fill weight if missing
        if not our.get("weight") and fbg.get("weight"):
            updates["weight"] = int(fbg["weight"])

        if updates:
            updates_to_apply.append((player_id, updates, method, fbg_name))

    print(f"\nMatched: {matched}, Unmatched: {unmatched}")
    print(f"Players with updates to apply: {len(updates_to_apply)}")

    # Break down what's being updated
    fill_counts = {"footballguys_id": 0, "fantasy_data_id": 0, "height": 0, "weight": 0}
    for _, updates, _, _ in updates_to_apply:
        for key in updates:
            fill_counts[key] += 1

    print("\nGap fills:")
    for col, count in fill_counts.items():
        if count > 0:
            print(f"  {col}: {count} new values")

    # ── 3. Apply updates ─────────────────────────────────────────────────────
    print(f"\nApplying {len(updates_to_apply)} updates...")
    applied = 0
    errors = 0

    for i, (player_id, updates, method, fbg_name) in enumerate(updates_to_apply):
        try:
            supabase_rest_patch("players", "player_id", player_id, updates)
            applied += 1
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"  ERROR updating {fbg_name}: {e.code} {body}")
            errors += 1

        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(updates_to_apply)}...")

    # ── 4. Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"SUMMARY")
    print(f"{'='*50}")
    print(f"FBG fantasy players:  {len(fbg_fantasy)}")
    print(f"Matched to DB:        {matched}")
    print(f"Unmatched:            {unmatched}")
    print(f"Updates applied:      {applied}")
    print(f"Errors:               {errors}")
    print()
    for col, count in fill_counts.items():
        if count > 0:
            print(f"  {col}: +{count}")


if __name__ == "__main__":
    main()
