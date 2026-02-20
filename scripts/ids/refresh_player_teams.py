#!/usr/bin/env python3
"""Refresh players.latest_team from the Sleeper API.

Pulls the full Sleeper player database, matches to our DB via sleeper_id,
and PATCHes latest_team where it has changed. Prints a diff report.

Designed to run daily via cron — safe to run repeatedly (idempotent).

Usage:
    python3 scripts/ids/refresh_player_teams.py
    python3 scripts/ids/refresh_player_teams.py --dry-run   # show changes without applying
"""

import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_KEY,
    normalize_team,
)

SLEEPER_URL = "https://api.sleeper.app/v1/players/nfl"
LOG_DIR = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
    "data", "logs"
)

key = SUPABASE_SERVICE_KEY or SUPABASE_KEY


def fetch_sleeper_players():
    """Fetch all NFL players from the Sleeper API."""
    req = urllib.request.Request(SLEEPER_URL, headers={
        "User-Agent": "nfl-db/1.0",
    })
    resp = urllib.request.urlopen(req, timeout=60)
    return json.loads(resp.read().decode("utf-8"))


def fetch_db_players():
    """Fetch all players with sleeper_id from Supabase (paginated)."""
    players = []
    offset = 0
    limit = 1000
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,first_name,last_name,position,latest_team,sleeper_id"
            f"&sleeper_id=not.is.null"
            f"&offset={offset}&limit={limit}"
        )
        req = urllib.request.Request(url, headers={
            "apikey": key, "Authorization": f"Bearer {key}",
        })
        resp = urllib.request.urlopen(req)
        batch = json.loads(resp.read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += limit
    return players


def patch_player(player_id, updates):
    """PATCH a player row via REST API."""
    url = f"{SUPABASE_URL}/rest/v1/players?player_id=eq.{player_id}"
    data = json.dumps(updates).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }, method="PATCH")
    urllib.request.urlopen(req)


def main():
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        print("=== DRY RUN — no changes will be applied ===\n")

    # 1. Fetch Sleeper data
    print("Fetching Sleeper API...")
    sleeper_data = fetch_sleeper_players()
    print(f"  {len(sleeper_data)} entries from Sleeper")

    # Build sleeper_id → team mapping (skill positions only)
    skill_positions = {"QB", "RB", "WR", "TE", "K"}
    sleeper_teams = {}
    for sid, p in sleeper_data.items():
        pos = (p.get("position") or "").upper()
        if pos not in skill_positions:
            continue
        team = normalize_team(p.get("team") or "")
        sleeper_teams[sid] = team

    print(f"  {len(sleeper_teams)} skill-position players with team data")

    # 2. Fetch DB players
    print("Fetching DB players with sleeper_id...")
    db_players = fetch_db_players()
    print(f"  {len(db_players)} players with sleeper_id in DB")

    # 3. Compare and collect changes
    # Policy: only update when Sleeper shows a NEW team (non-empty).
    # Never null out latest_team — retired/FA players keep their last team.
    changes = []
    no_sleeper_match = 0
    same_team = 0
    newly_signed = 0  # was empty, now has team
    skipped_fa = 0  # Sleeper shows no team, we keep ours
    team_change = 0  # changed from one team to another

    for p in db_players:
        sid = p["sleeper_id"]
        db_team = (p.get("latest_team") or "").strip()
        sleeper_team = sleeper_teams.get(sid)

        if sleeper_team is None:
            no_sleeper_match += 1
            continue

        # Normalize both for comparison
        db_team_norm = normalize_team(db_team)
        sl_team_norm = sleeper_team  # already normalized

        if db_team_norm == sl_team_norm:
            same_team += 1
            continue

        # Skip if Sleeper shows no team — keep our latest_team
        if not sl_team_norm:
            skipped_fa += 1
            continue

        name = f"{p['first_name']} {p['last_name']}"
        pos = p.get("position", "")

        if not db_team_norm and sl_team_norm:
            change_type = "SIGNED"
            newly_signed += 1
        else:
            change_type = "TRADE/FA"
            team_change += 1

        changes.append({
            "player_id": p["player_id"],
            "name": name,
            "position": pos,
            "old_team": db_team_norm or "(none)",
            "new_team": sl_team_norm,
            "type": change_type,
        })

    # 4. Report
    print(f"\n{'='*60}")
    print(f"TEAM UPDATE SUMMARY")
    print(f"  Same team (no change):  {same_team}")
    print(f"  Not in Sleeper data:    {no_sleeper_match}")
    print(f"  Skipped (now FA/ret'd): {skipped_fa}")
    print(f"  ---")
    print(f"  Team changes:           {team_change}")
    print(f"  Newly signed:           {newly_signed}")
    print(f"  TOTAL TO UPDATE:        {len(changes)}")
    print(f"{'='*60}\n")

    if changes:
        # Sort: trades first (by name), then signed
        type_order = {"TRADE/FA": 0, "SIGNED": 1}
        changes.sort(key=lambda c: (type_order.get(c["type"], 9), c["name"]))

        for c in changes:
            print(f"  {c['type']:10s} {c['name']:30s} {c['position']:3s}  "
                  f"{c['old_team']:4s} -> {c['new_team']:4s}")

    # 5. Apply changes
    if changes and not dry_run:
        print(f"\nApplying {len(changes)} updates...")
        applied = 0
        errors = 0
        for c in changes:
            try:
                patch_player(c["player_id"], {"latest_team": c["new_team"]})
                applied += 1
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                print(f"  ERROR updating {c['name']}: {e.code} {body[:200]}")
                errors += 1
        print(f"  Applied: {applied}, Errors: {errors}")
    elif changes and dry_run:
        print(f"\n(dry run — {len(changes)} changes would be applied)")
    elif not changes:
        print("No team changes detected.")

    # 6. Log results
    os.makedirs(LOG_DIR, exist_ok=True)
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "dry_run": dry_run,
        "db_players": len(db_players),
        "same_team": same_team,
        "changes": len(changes),
        "team_changes": team_change,
        "newly_signed": newly_signed,
        "skipped_fa": skipped_fa,
        "details": changes,
    }
    log_path = os.path.join(LOG_DIR, "team_refresh.jsonl")
    with open(log_path, "a") as f:
        f.write(json.dumps(log_entry) + "\n")
    print(f"\nLogged to {log_path}")


if __name__ == "__main__":
    main()
