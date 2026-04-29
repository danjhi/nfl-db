#!/usr/bin/env python3
"""Add 2026 NFL Draft late-round rookies to the players table.

26 players from Dan's dynasty trade value chart Google Sheet.
For each: lookup Sleeper API for IDs, insert into players table with dan_id.

Dynasty values are NOT inserted here — they sync from the Sheet via Apps Script
once the dan_id is on the player row.

Usage:
    python3 scripts/ids/add_2026_late_round_rookies.py [--dry-run]
"""

import json
import os
import sys
import urllib.request
import urllib.error
import uuid
sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_KEY,
    normalize_name, normalize_team,
    get_all_players, build_player_lookup,
)

# (dan_id, name, team, position) — values handled separately via Sheet sync
ROOKIES = [
    ("2026300", "Nate Boerkircher",   "JAX", "TE"),
    ("2026301", "Marlin Klein",       "HOU", "TE"),
    ("2026302", "Will Kacmarek",      "MIA", "TE"),
    ("2026303", "Eli Raridon",        "NE",  "TE"),
    ("2026304", "Zavion Thomas",      "CHI", "WR"),
    ("2026305", "Kaden Wetjen",       "PIT", "WR"),
    ("2026306", "Matthew Hibner",     "BAL", "TE"),
    ("2026307", "Colbie Young",       "CIN", "WR"),
    ("2026308", "Kendrick Law",       "DET", "WR"),
    ("2026309", "Riley Nowakowski",   "PIT", "TE"),
    ("2026310", "Joe Royer",          "CLE", "TE"),
    ("2026311", "Josh Cuevas",        "BAL", "TE"),
    ("2026312", "Cyrus Allen",        "KC",  "WR"),
    ("2026313", "Seydou Traore",      "MIA", "TE"),
    ("2026314", "Bauer Sharp",        "TB",  "TE"),
    ("2026315", "Barion Brown",       "NO",  "WR"),
    ("2026316", "Malik Benson",       "LV",  "WR"),
    ("2026317", "CJ Daniels",         "LAR", "WR"),
    ("2026318", "Emmanuel Henderson", "SEA", "WR"),
    ("2026319", "Lewis Bond",         "HOU", "WR"),
    ("2026320", "Anthony Smith",      "DAL", "WR"),
    ("2026321", "Jack Endries",       "CIN", "TE"),
    ("2026322", "Athan Kaliakmanis",  "WAS", "QB"),
    ("2026323", "Jaren Kanak",        "TEN", "TE"),
    ("2026324", "Eli Heidenreich",    "PIT", "RB"),
    ("2026325", "Dallen Bentley",     "DEN", "TE"),
]

DRY_RUN = "--dry-run" in sys.argv


def fetch_sleeper_db():
    url = "https://api.sleeper.app/v1/players/nfl"
    print(f"Fetching Sleeper player DB from {url}...")
    req = urllib.request.Request(url, headers={"User-Agent": "nfl-db/1.0"})
    resp = urllib.request.urlopen(req, timeout=60)
    data = json.loads(resp.read().decode("utf-8"))
    print(f"  Fetched {len(data)} entries from Sleeper")
    return data


def build_sleeper_index(sleeper_db):
    """Index by (normalized_name, position). Returns lists to handle duplicates."""
    by_name_pos = {}
    by_name = {}
    by_last_pos = {}
    for sleeper_id, p in sleeper_db.items():
        if not isinstance(p, dict):
            continue
        first = p.get("first_name") or ""
        last = p.get("last_name") or ""
        full = f"{first} {last}".strip()
        norm = normalize_name(full)
        norm_last = normalize_name(last)
        pos = (p.get("position") or "").upper()
        if not norm:
            continue
        # Use the API key as sleeper_id (top-level dict key) — that's authoritative
        p["_sleeper_id"] = sleeper_id
        by_name_pos.setdefault((norm, pos), []).append(p)
        by_name.setdefault(norm, []).append(p)
        by_last_pos.setdefault((norm_last, pos), []).append(p)
    return by_name_pos, by_name, by_last_pos


def find_sleeper_match(name, pos, team, by_name_pos, by_name, by_last_pos):
    """Try multiple match strategies. Returns matched Sleeper player dict or None."""
    norm = normalize_name(name)
    parts = name.split(" ", 1)
    last = parts[1] if len(parts) > 1 else parts[0]
    norm_last = normalize_name(last)

    def disambiguate(candidates):
        if len(candidates) == 1:
            return candidates[0]
        # Filter by team
        team_matches = [p for p in candidates if normalize_team(p.get("team") or "") == team]
        if len(team_matches) == 1:
            return team_matches[0]
        if len(team_matches) > 1:
            return team_matches[0]
        # No team match — return first
        return candidates[0]

    # 1. Exact name + position
    cands = by_name_pos.get((norm, pos))
    if cands:
        return disambiguate(cands), "name+pos"

    # 2. Name only (any position)
    cands = by_name.get(norm)
    if cands:
        return disambiguate(cands), "name-only"

    # 3. Last name + position + team match
    cands = by_last_pos.get((norm_last, pos))
    if cands:
        # Require team match to avoid false positives on common last names
        team_matches = [p for p in cands if normalize_team(p.get("team") or "") == team]
        if len(team_matches) == 1:
            return team_matches[0], "lastname+pos+team"

    return None, None


def supabase_post(table, rows):
    key = SUPABASE_SERVICE_KEY
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    data = json.dumps(rows).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }, method="POST")
    try:
        urllib.request.urlopen(req)
        return True, None
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        return False, f"{e.code}: {body[:300]}"


def supabase_patch(table, match_col, match_val, updates):
    key = SUPABASE_SERVICE_KEY
    url = f"{SUPABASE_URL}/rest/v1/{table}?{match_col}=eq.{match_val}"
    data = json.dumps(updates).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }, method="PATCH")
    try:
        urllib.request.urlopen(req)
        return True, None
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        return False, f"{e.code}: {body[:300]}"


def build_player_row(dan_id, name, team, pos, sleeper_p):
    """Build dict of columns to insert into players table."""
    parts = name.split(" ", 1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ""

    row = {
        "first_name": first_name,
        "last_name": last_name,
        "position": pos,
        "latest_team": team or None,
        "dan_id": dan_id,
    }

    if sleeper_p:
        row["sleeper_id"] = sleeper_p["_sleeper_id"]
        # Cross-source IDs from Sleeper
        for sleeper_key, db_key in [
            ("espn_id", "espn_id"),
            ("yahoo_id", "yahoo_id"),
            ("fantasy_data_id", "fantasy_data_id"),
            ("rotowire_id", "rotowire_id"),
            ("rotoworld_id", "rotoworld_id"),
            ("stats_id", "stats_id"),
            ("gsis_id", "gsis_id"),
            ("swish_id", "swish_id"),
            ("pff_id", "pff_id"),
        ]:
            v = sleeper_p.get(sleeper_key)
            if v:
                row[db_key] = str(v)

        # Bio
        bd = sleeper_p.get("birth_date")
        if bd and not str(bd).startswith("0000"):
            row["birth_date"] = bd
        col = sleeper_p.get("college")
        if col:
            row["college"] = col
        h = sleeper_p.get("height")
        if h:
            row["height"] = h
        w = sleeper_p.get("weight")
        if w:
            try:
                row["weight"] = int(w)
            except (ValueError, TypeError):
                pass
        st = sleeper_p.get("status")
        if st:
            row["status"] = st

    # Determine player_id: prefer sportradar_id, else generate UUID
    sportradar = (sleeper_p.get("sportradar_id") if sleeper_p else None) or ""
    if sportradar:
        row["player_id"] = sportradar
    else:
        row["player_id"] = str(uuid.uuid4())

    return row


def main():
    if DRY_RUN:
        print("=== DRY RUN — no DB writes ===\n")

    sleeper_db = fetch_sleeper_db()
    by_name_pos, by_name, by_last_pos = build_sleeper_index(sleeper_db)

    print("Fetching existing players from Supabase...")
    existing = get_all_players()
    db_by_name_pos, db_by_name = build_player_lookup(existing)
    print(f"  {len(existing)} players in DB\n")

    inserted = 0
    patched = 0
    failures = []

    for dan_id, name, team, pos in ROOKIES:
        team = normalize_team(team)
        norm = normalize_name(name)

        # Skip if already in DB
        existing_pid = db_by_name_pos.get((norm, pos)) or db_by_name.get(norm)
        if existing_pid:
            print(f"  EXISTS in DB: {name:25s} ({pos}, {team}) — patching dan_id={dan_id}, latest_team={team}")
            if not DRY_RUN:
                ok, err = supabase_patch("players", "player_id", existing_pid, {
                    "dan_id": dan_id,
                    "latest_team": team or None,
                })
                if not ok:
                    print(f"    PATCH failed: {err}")
                    failures.append((name, "patch failed"))
                    continue
            patched += 1
            continue

        # Find in Sleeper
        match, method = find_sleeper_match(name, pos, team, by_name_pos, by_name, by_last_pos)
        if not match:
            print(f"  NO SLEEPER MATCH: {name:25s} ({pos}, {team})")
            # Still insert with no Sleeper IDs (player_id = UUID)
            row = build_player_row(dan_id, name, team, pos, None)
        else:
            sleeper_team = normalize_team(match.get("team") or "")
            row = build_player_row(dan_id, name, team, pos, match)
            id_count = sum(1 for k in row if k.endswith("_id") and row[k])
            sportradar = match.get("sportradar_id") or ""
            pid_kind = "sportradar" if sportradar else "uuid"
            sleeper_name = f"{match.get('first_name','')} {match.get('last_name','')}".strip()
            note = ""
            if sleeper_team and sleeper_team != team:
                note = f"  (Sleeper team={sleeper_team}, Sheet={team})"
            print(f"  MATCH ({method}): {name:25s} ({pos}, {team}) → {sleeper_name} sleeper_id={match['_sleeper_id']} {id_count} IDs {pid_kind}{note}")

        if DRY_RUN:
            inserted += 1
            continue

        ok, err = supabase_post("players", [row])
        if ok:
            inserted += 1
        else:
            print(f"    INSERT failed: {err}")
            failures.append((name, err))

    print(f"\n=== Summary ===")
    print(f"Inserted: {inserted}")
    print(f"Patched (already in DB): {patched}")
    print(f"Failed: {len(failures)}")
    if failures:
        for name, err in failures:
            print(f"  - {name}: {err}")


if __name__ == "__main__":
    main()
