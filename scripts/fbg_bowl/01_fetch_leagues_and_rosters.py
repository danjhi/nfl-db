"""Fetch league metadata and roster/user info from Sleeper for each FBG Bowl league.

Usage:
  python3 scripts/fbg_bowl/01_fetch_leagues_and_rosters.py [--year 2025]

For each league_id:
  GET /v1/league/{id}         → fills fbg_bowl_leagues.name, scoring_type, roster_count
  GET /v1/league/{id}/users   → display_name, team_name
  GET /v1/league/{id}/rosters → roster_id, owner_id

Loads results into fbg_bowl_rosters.
Saves checkpoint: rosters_2025.json = [{sleeper_id, internal_id, roster_id, ...}, ...]
for use by downstream scripts.

Checkpointable: skips leagues whose rosters are already in fbg_bowl_rosters.
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    sleeper_get, supa_get, supa_upsert, supa_batch_insert,
    save_checkpoint, load_checkpoint, parse_roster_pts
)

import json

YEAR = 2025


def main():
    for i, arg in enumerate(sys.argv):
        if arg == "--year" and i + 1 < len(sys.argv):
            global YEAR
            YEAR = int(sys.argv[i + 1])

    print(f"Fetching leagues and rosters for {YEAR}...")

    # Load leagues from DB
    leagues_db = supa_get(
        "fbg_bowl_leagues",
        select="id,sleeper_id",
        params=f"year=eq.{YEAR}",
    )
    print(f"  {len(leagues_db)} leagues in DB for {YEAR}")

    # Check which already have rosters loaded
    existing_rosters = supa_get(
        "fbg_bowl_rosters",
        select="league_id",
        params="",
    )
    leagues_with_rosters = {r["league_id"] for r in existing_rosters}
    to_process = [lg for lg in leagues_db if lg["id"] not in leagues_with_rosters]
    print(f"  Already have rosters: {len(leagues_with_rosters)}")
    print(f"  Need to fetch:        {len(to_process)}")

    if not to_process:
        print("All rosters already loaded.")
        _save_roster_checkpoint(YEAR)
        return

    league_updates = []
    all_roster_rows = []
    errors = 0

    for idx, lg in enumerate(to_process, 1):
        sleeper_id = lg["sleeper_id"]
        internal_id = lg["id"]

        if idx % 50 == 0 or idx == len(to_process):
            print(f"  {idx}/{len(to_process)}  ({errors} errors so far)")

        # Fetch league metadata
        league_meta = sleeper_get(f"league/{sleeper_id}")
        if not league_meta:
            errors += 1
            continue

        scoring = (league_meta.get("scoring_settings") or {}).get("rec", None)
        scoring_type = "ppr" if scoring == 1 else ("half_ppr" if scoring == 0.5 else "standard")
        roster_count = (league_meta.get("settings") or {}).get("num_teams") or len(
            league_meta.get("roster_positions") or []
        )

        league_updates.append({
            "sleeper_id": sleeper_id,
            "year": YEAR,
            "name": league_meta.get("name"),
            "scoring_type": scoring_type,
            "roster_count": roster_count,
        })

        # Fetch users
        users_raw = sleeper_get(f"league/{sleeper_id}/users") or []
        user_map = {}
        for u in users_raw:
            uid = str(u.get("user_id", ""))
            display = u.get("display_name") or u.get("username") or f"User_{uid[:6]}"
            meta = u.get("metadata") or {}
            team_name = meta.get("team_name") or meta.get("nickname") or None
            user_map[uid] = {"display_name": display, "team_name": team_name}

        # Fetch rosters
        rosters_raw = sleeper_get(f"league/{sleeper_id}/rosters") or []
        for r in rosters_raw:
            owner_id = str(r.get("owner_id") or "")
            user_info = user_map.get(owner_id, {})
            display = user_info.get("display_name") or f"Owner_{owner_id[:6]}"
            team_name = user_info.get("team_name")

            all_roster_rows.append({
                "league_id": internal_id,
                "sleeper_user_id": owner_id,
                "sleeper_roster_id": int(r.get("roster_id", 0)),
                "display_name": display,
                "team_name": team_name,
            })

    print(f"\n  League metadata to update: {len(league_updates)}")
    print(f"  Roster rows to insert:     {len(all_roster_rows)}")

    # Upsert league metadata (fills name/scoring_type/roster_count)
    if league_updates:
        supa_upsert("fbg_bowl_leagues", league_updates, on_conflict="sleeper_id")

    # Insert rosters in batches
    if all_roster_rows:
        inserted = supa_batch_insert("fbg_bowl_rosters", all_roster_rows)
        print(f"  Inserted {len(inserted)} roster rows")

    _save_roster_checkpoint(YEAR)
    print(f"\nDone. Errors: {errors}")


def _save_roster_checkpoint(year):
    """Save (sleeper_league_id, sleeper_roster_id) → internal roster.id mapping."""
    leagues = supa_get("fbg_bowl_leagues", select="id,sleeper_id", params=f"year=eq.{year}")
    lid_map = {lg["id"]: lg["sleeper_id"] for lg in leagues}

    rosters = supa_get(
        "fbg_bowl_rosters",
        select="id,league_id,sleeper_roster_id,sleeper_user_id,display_name",
    )
    # Only include rosters for this year's leagues
    year_league_ids = set(lid_map.keys())
    roster_data = [r for r in rosters if r["league_id"] in year_league_ids]

    # Build lookup: (sleeper_league_id, sleeper_roster_id) → internal roster.id
    checkpoint = {
        f"{lid_map[r['league_id']]}:{r['sleeper_roster_id']}": r["id"]
        for r in roster_data
    }
    save_checkpoint(f"roster_map_{year}", checkpoint)
    print(f"  Roster checkpoint saved: {len(checkpoint)} entries")

    # Also save league internal id map
    league_map = {lg["sleeper_id"]: lg["id"] for lg in leagues}
    save_checkpoint(f"league_map_{year}", league_map)


if __name__ == "__main__":
    main()
