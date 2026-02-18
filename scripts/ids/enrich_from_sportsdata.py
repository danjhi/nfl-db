"""Enrich players with data from the SportsData.io Players endpoint.

Fetches https://api.sportsdata.io/v3/nfl/scores/json/Players and matches
to our players table using SportRadarPlayerID (= our player_id PK), then
sportsdata_id, then name+position fallback.

Fills gaps in: height, weight, headshot_url, college, birth_date,
draft_year, draft_round, draft_pick, sportsdata_id, fanduel_id,
draftkings_id, status, latest_team.

Usage:
    python3 scripts/ids/enrich_from_sportsdata.py
"""

import json
import os
import re
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    SPORTSDATA_KEY,
    normalize_name,
    normalize_team,
    supabase_rest_patch,
    PLAYER_ALIASES,
)

SPORTSDATA_URL = "https://api.sportsdata.io/v3/nfl/scores/json/Players"


def convert_height(height_str):
    """Convert SportsData height '6\\'0\"' to our format '6-0'."""
    if not height_str:
        return None
    m = re.match(r"(\d+)'(\d+)", height_str)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return None


def fetch_sportsdata_players():
    """Download all players from SportsData.io."""
    req = urllib.request.Request(SPORTSDATA_URL, headers={
        "Ocp-Apim-Subscription-Key": SPORTSDATA_KEY,
    })
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read().decode("utf-8"))


def fetch_our_players():
    """Fetch all players with columns needed for gap detection."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    limit = 1000
    select = ",".join([
        "player_id", "first_name", "last_name", "position",
        "height", "weight", "headshot_url", "college", "birth_date",
        "draft_year", "draft_round", "draft_pick",
        "sportsdata_id", "fanduel_id", "draftkings_id",
        "status", "latest_team",
    ])
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
    """Build lookup dicts for our players."""
    by_player_id = {}     # player_id (sportradar) -> player
    by_sportsdata_id = {} # sportsdata_id -> player
    by_name_pos = {}      # (norm_name, pos) -> player
    by_name = {}          # norm_name -> player

    for p in players:
        by_player_id[p["player_id"]] = p
        if p.get("sportsdata_id"):
            by_sportsdata_id[p["sportsdata_id"]] = p

        full_name = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
        norm = normalize_name(full_name)
        pos = (p.get("position") or "").upper()
        by_name_pos[(norm, pos)] = p
        by_name[norm] = p
        alias = PLAYER_ALIASES.get(norm)
        if alias:
            by_name_pos[(alias, pos)] = p
            by_name[alias] = p

    return by_player_id, by_sportsdata_id, by_name_pos, by_name


def main():
    if not SPORTSDATA_KEY:
        print("ERROR: SPORTSDATA_API_KEY not set in .env")
        sys.exit(1)

    # ── 1. Fetch data ────────────────────────────────────────────────────────
    print("Fetching SportsData.io Players...")
    sd_players = fetch_sportsdata_players()
    print(f"  {len(sd_players)} total players from SportsData.io")

    # Filter to fantasy-relevant
    fantasy_pos = {"QB", "RB", "WR", "TE", "K"}
    sd_fantasy = [p for p in sd_players if p.get("Position") in fantasy_pos]
    print(f"  {len(sd_fantasy)} fantasy-relevant")

    print("Fetching our players from Supabase...")
    our_players = fetch_our_players()
    print(f"  {len(our_players)} players in DB")

    by_pid, by_sdid, by_name_pos, by_name = build_lookups(our_players)

    # ── 2. Match and collect updates ─────────────────────────────────────────
    matched = 0
    unmatched = 0
    updates_to_apply = []

    for sd in sd_fantasy:
        sportradar_id = sd.get("SportRadarPlayerID", "")
        sd_id = str(sd.get("PlayerID", "")) if sd.get("PlayerID") else ""
        sd_name = f"{sd.get('FirstName', '')} {sd.get('LastName', '')}".strip()
        sd_pos = sd.get("Position", "")

        # Match priority: sportradar_id -> sportsdata_id -> name+pos
        our = None
        if sportradar_id and sportradar_id in by_pid:
            our = by_pid[sportradar_id]
        elif sd_id and sd_id in by_sdid:
            our = by_sdid[sd_id]
        else:
            norm = normalize_name(sd_name)
            sd_team = normalize_team(sd.get("Team") or "")
            our = by_name_pos.get((norm, sd_pos)) or by_name.get(norm)

        if not our:
            unmatched += 1
            continue

        matched += 1
        player_id = our["player_id"]
        updates = {}

        # Height
        if not our.get("height") and sd.get("Height"):
            h = convert_height(sd["Height"])
            if h:
                updates["height"] = h

        # Weight
        if not our.get("weight") and sd.get("Weight"):
            updates["weight"] = int(sd["Weight"])

        # Headshot URL (prefer UsaTodayHeadshotNoBackgroundUrl, fallback to PhotoUrl)
        if not our.get("headshot_url"):
            url = sd.get("UsaTodayHeadshotNoBackgroundUrl") or sd.get("PhotoUrl")
            if url:
                updates["headshot_url"] = url

        # College
        if not our.get("college") and sd.get("College"):
            updates["college"] = sd["College"]

        # Birth date
        if not our.get("birth_date") and sd.get("BirthDate"):
            bd = sd["BirthDate"][:10]  # "2000-03-01T00:00:00" -> "2000-03-01"
            if bd and bd != "0000-00-00":
                updates["birth_date"] = bd

        # Draft info
        if not our.get("draft_year") and sd.get("CollegeDraftYear"):
            updates["draft_year"] = int(sd["CollegeDraftYear"])
        if not our.get("draft_round") and sd.get("CollegeDraftRound"):
            updates["draft_round"] = int(sd["CollegeDraftRound"])
        if not our.get("draft_pick") and sd.get("CollegeDraftPick"):
            updates["draft_pick"] = int(sd["CollegeDraftPick"])

        # IDs
        if not our.get("sportsdata_id") and sd_id:
            updates["sportsdata_id"] = sd_id
        if not our.get("fanduel_id") and sd.get("FanDuelPlayerID"):
            updates["fanduel_id"] = str(sd["FanDuelPlayerID"])
        if not our.get("draftkings_id") and sd.get("DraftKingsPlayerID"):
            updates["draftkings_id"] = str(sd["DraftKingsPlayerID"])

        # Status and team
        if sd.get("Status"):
            updates["status"] = sd["Status"]
        if sd.get("Team"):
            team = normalize_team(sd["Team"])
            if team:
                updates["latest_team"] = team

        if updates:
            updates_to_apply.append((player_id, updates, sd_name))

    print(f"\nMatched: {matched}, Unmatched: {unmatched}")
    print(f"Players with updates: {len(updates_to_apply)}")

    # Tally what's being filled
    fill_counts = {}
    for _, updates, _ in updates_to_apply:
        for key in updates:
            fill_counts[key] = fill_counts.get(key, 0) + 1

    print("\nGap fills:")
    for col in sorted(fill_counts, key=fill_counts.get, reverse=True):
        print(f"  {col}: {fill_counts[col]}")

    # ── 3. Apply updates ─────────────────────────────────────────────────────
    print(f"\nApplying {len(updates_to_apply)} updates...")
    applied = 0
    errors = 0

    for i, (player_id, updates, sd_name) in enumerate(updates_to_apply):
        try:
            supabase_rest_patch("players", "player_id", player_id, updates)
            applied += 1
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"  ERROR updating {sd_name}: {e.code} {body}")
            errors += 1

        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(updates_to_apply)}...")

    # ── 4. Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    print(f"SportsData fantasy players: {len(sd_fantasy)}")
    print(f"Matched to DB:             {matched}")
    print(f"Unmatched:                 {unmatched}")
    print(f"Updates applied:           {applied}")
    print(f"Errors:                    {errors}")
    print()
    for col in sorted(fill_counts, key=fill_counts.get, reverse=True):
        print(f"  {col}: +{fill_counts[col]}")


if __name__ == "__main__":
    main()
