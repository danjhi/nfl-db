#!/usr/bin/env python3
"""Match Sleeper API players to existing players.

Fetches the full Sleeper player database (no auth required), matches to our
DB by sportradar_id (direct PK match) and name+position fallback.
Extracts sleeper_id and fills in other ID gaps (espn, yahoo, etc.).
Outputs data/matched/sleeper_ids.json.
"""

import json
import os
import sys
import urllib.request
sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    MATCHED_DIR, ensure_dir,
    get_all_players, build_player_lookup, normalize_name, normalize_team
)

SLEEPER_URL = "https://api.sleeper.app/v1/players/nfl"

# Sleeper field → our DB column
SLEEPER_ID_MAP = {
    "player_id": "sleeper_id",
    "espn_id": "espn_id",
    "yahoo_id": "yahoo_id",
    "fantasy_data_id": "fantasy_data_id",
    "stats_id": "stats_id",
    "rotowire_id": "rotowire_id",
    "rotoworld_id": "rotoworld_id",
}


def fetch_sleeper_players():
    """Fetch all NFL players from the Sleeper API."""
    req = urllib.request.Request(SLEEPER_URL, headers={
        "User-Agent": "nfl-db/1.0",
    })
    resp = urllib.request.urlopen(req, timeout=60)
    return json.loads(resp.read().decode("utf-8"))


def main():
    print("Fetching players from Sleeper API...")
    sleeper_data = fetch_sleeper_players()
    print(f"Got {len(sleeper_data)} entries from Sleeper")

    # Filter to real NFL players (not teams/defenses)
    skill_positions = {"QB", "RB", "WR", "TE", "K"}
    sleeper_players = []
    for sid, p in sleeper_data.items():
        pos = (p.get("position") or "").upper()
        if pos not in skill_positions:
            continue
        # Must have a name
        first = p.get("first_name") or ""
        last = p.get("last_name") or ""
        if not (first or last):
            continue
        p["_sleeper_id"] = sid
        sleeper_players.append(p)

    print(f"Filtered to {len(sleeper_players)} skill-position players")

    print("Fetching players from Supabase...")
    db_players = get_all_players()
    print(f"Got {len(db_players)} players from Supabase")

    # Build lookup by player_id (for sportradar match)
    db_by_pid = {p["player_id"] for p in db_players}
    by_name_pos, by_name = build_player_lookup(db_players)

    matched = {}          # player_id → {col: val, ...}
    match_method = {}      # player_id → "sportradar" | "name"
    unmatched = []
    match_by_sr = 0
    match_by_name = 0

    # Build reverse lookup: player_id → position (for name-only fallback check)
    db_pos = {p["player_id"]: (p.get("position") or "").upper() for p in db_players}

    for p in sleeper_players:
        sr_id = (p.get("sportradar_id") or "").strip()
        sleeper_id = p["_sleeper_id"]
        first = p.get("first_name") or ""
        last = p.get("last_name") or ""
        full_name = f"{first} {last}".strip()
        norm = normalize_name(full_name)
        pos = (p.get("position") or "").upper()
        team = normalize_team(p.get("team") or "")

        # Strategy 1: Direct match by sportradar_id → our player_id PK
        player_id = None
        method = None
        if sr_id and sr_id in db_by_pid:
            player_id = sr_id
            method = "sportradar"
            match_by_sr += 1
        else:
            # Strategy 2: Name+position match (most reliable name fallback)
            player_id = by_name_pos.get((norm, pos))
            if not player_id:
                # Strategy 3: Name-only match — verify position compatibility
                candidate = by_name.get(norm)
                if candidate:
                    db_player_pos = db_pos.get(candidate, "")
                    if db_player_pos == pos:
                        player_id = candidate
                    # Skip if positions don't match — likely a different player
            if player_id:
                method = "name"
                match_by_name += 1

        if player_id:
            updates = {}
            for sl_field, db_col in SLEEPER_ID_MAP.items():
                val = p.get(sl_field)
                if val:
                    updates[db_col] = str(val)
            if updates:
                prev_method = match_method.get(player_id)
                if player_id not in matched:
                    # First match for this player
                    matched[player_id] = updates
                    match_method[player_id] = method
                elif method == "sportradar" and prev_method == "name":
                    # Sportradar match overrides a previous name-based match
                    matched[player_id] = updates
                    match_method[player_id] = method
                else:
                    # Don't overwrite existing values (same quality or lower)
                    for k, v in updates.items():
                        if k not in matched[player_id]:
                            matched[player_id][k] = v
        else:
            # Track unmatched for potential additions
            years_exp = p.get("years_exp")
            status = (p.get("status") or "").lower()
            # Only care about active/rookie players at skill positions
            if status in ("active", "inactive", ""):
                unmatched.append({
                    "sleeper_id": sleeper_id,
                    "name": full_name,
                    "pos": pos,
                    "team": team,
                    "sportradar_id": sr_id,
                    "years_exp": years_exp,
                    "status": p.get("status", ""),
                    "search_rank": p.get("search_rank"),
                })

    print(f"\nMatched {len(matched)} / {len(db_players)} DB players")
    print(f"  By sportradar_id: {match_by_sr}")
    print(f"  By name: {match_by_name}")

    # Report per-column coverage
    for col in sorted(set(v for u in matched.values() for v in u.keys())):
        count = sum(1 for u in matched.values() if col in u)
        print(f"  {col}: {count}")

    # Filter unmatched to interesting ones (have a search_rank or are rookies)
    unmatched_rookies = [u for u in unmatched if u.get("years_exp") == 0 and u.get("team")]
    unmatched_rookies.sort(key=lambda x: x.get("search_rank") or 99999)

    print(f"\nUnmatched total: {len(unmatched)}")
    print(f"Unmatched 2026 rookies with NFL teams: {len(unmatched_rookies)}")
    if unmatched_rookies:
        print("Top unmatched rookies (by search rank):")
        for u in unmatched_rookies[:25]:
            print(f"  {u['name']:30s} {u['pos']:3s} {u['team']:4s}  SR={u.get('sportradar_id', '')[:8]}...  rank={u.get('search_rank', 'N/A')}")

    # Save matched
    ensure_dir(MATCHED_DIR)
    out_path = os.path.join(MATCHED_DIR, "sleeper_ids.json")
    with open(out_path, "w") as f:
        json.dump(matched, f, indent=2)
    print(f"\nSaved {len(matched)} matched players to {out_path}")

    # Save unmatched rookies for potential addition
    rookies_path = os.path.join(MATCHED_DIR, "sleeper_unmatched_rookies.json")
    with open(rookies_path, "w") as f:
        json.dump(unmatched_rookies, f, indent=2)
    print(f"Saved {len(unmatched_rookies)} unmatched rookies to {rookies_path}")

    # Save full Sleeper cache for other scripts
    cache_path = os.path.join(MATCHED_DIR, "sleeper_players_cache.json")
    with open(cache_path, "w") as f:
        json.dump({p["_sleeper_id"]: {k: v for k, v in p.items() if k != "_sleeper_id"}
                    for p in sleeper_players}, f)
    print(f"Cached full Sleeper player list to {cache_path}")


if __name__ == "__main__":
    main()
