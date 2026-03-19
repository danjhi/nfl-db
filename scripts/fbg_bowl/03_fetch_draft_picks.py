"""Fetch draft picks from Sleeper for all FBG Bowl leagues.

Usage:
  python3 scripts/fbg_bowl/03_fetch_draft_picks.py [--year 2025]

For each league:
  GET /v1/league/{id}/drafts     → draft_id
  GET /v1/draft/{draft_id}/picks → all picks, unnest metadata

Loads into fbg_bowl_draft_picks.
Checkpointable: skips leagues already in fbg_bowl_draft_picks.

Volume: ~417 leagues × 2 calls = ~834 calls (~2 min)
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    sleeper_get, supa_get, supa_batch_insert, save_checkpoint
)

YEAR = 2025


def get_roster_map(year):
    leagues = supa_get("fbg_bowl_leagues", select="id,sleeper_id", params=f"year=eq.{year}")
    league_map = {lg["sleeper_id"]: lg["id"] for lg in leagues}
    lid_to_sleeper = {v: k for k, v in league_map.items()}

    rosters = supa_get("fbg_bowl_rosters", select="id,league_id,sleeper_roster_id")
    year_lid_set = set(league_map.values())
    roster_map = {}
    for r in rosters:
        if r["league_id"] in year_lid_set:
            sleeper_lid = lid_to_sleeper[r["league_id"]]
            roster_map[(sleeper_lid, r["sleeper_roster_id"])] = r["id"]
    return league_map, lid_to_sleeper, roster_map


def main():
    global YEAR
    for i, arg in enumerate(sys.argv):
        if arg == "--year" and i + 1 < len(sys.argv):
            YEAR = int(sys.argv[i + 1])

    print(f"Fetching draft picks for {YEAR}...")
    league_map, lid_to_sleeper, roster_map = get_roster_map(YEAR)
    print(f"  Leagues: {len(league_map)}")

    # Check which leagues already have picks loaded
    existing = supa_get("fbg_bowl_draft_picks", select="league_id")
    loaded_league_ids = {r["league_id"] for r in existing}
    to_process = {
        sleeper_lid: internal_lid
        for sleeper_lid, internal_lid in league_map.items()
        if internal_lid not in loaded_league_ids
    }
    print(f"  Already loaded: {len(loaded_league_ids)}")
    print(f"  Need to fetch:  {len(to_process)}")

    if not to_process:
        print("All draft picks already loaded.")
        return

    all_rows = []
    errors = 0
    no_draft = 0

    for idx, (sleeper_lid, internal_lid) in enumerate(to_process.items(), 1):
        if idx % 100 == 0:
            print(f"  {idx}/{len(to_process)}  ({errors} errors, {no_draft} no-draft)")

        # Get draft IDs for this league
        drafts = sleeper_get(f"league/{sleeper_lid}/drafts")
        if not drafts:
            no_draft += 1
            continue

        for draft in drafts:
            draft_id = draft.get("draft_id") or draft.get("draft_id")
            if not draft_id:
                continue

            picks = sleeper_get(f"draft/{draft_id}/picks")
            if not picks:
                continue

            for pick in picks:
                meta = pick.get("metadata") or {}

                # Resolve roster: picked_by (user_id) → need roster that belongs to that user
                # The pick has roster_id directly (which team the player was drafted to)
                sleeper_roster_id = int(pick.get("roster_id") or 0)
                internal_rid = roster_map.get((sleeper_lid, sleeper_roster_id))

                # player_id is in metadata (not top-level) per R code
                sleeper_player_id = (
                    str(meta.get("player_id") or "")
                    or str(pick.get("player_id") or "")
                )

                all_rows.append({
                    "league_id": internal_lid,
                    "roster_id": internal_rid,
                    "sleeper_draft_id": str(draft_id),
                    "pick_no": int(pick.get("pick_no") or 0),
                    "draft_slot": int(pick.get("draft_slot") or 0),
                    "sleeper_player_id": sleeper_player_id or None,
                    "first_name": str(meta.get("first_name") or ""),
                    "last_name": str(meta.get("last_name") or ""),
                    "position": str(meta.get("position") or ""),
                })

    print(f"\n  Total pick rows to insert: {len(all_rows)}")
    print(f"  Errors: {errors}, No-draft leagues: {no_draft}")

    if all_rows:
        supa_batch_insert("fbg_bowl_draft_picks", all_rows, batch_size=500)
        print(f"  Inserted {len(all_rows)} picks")

    print("Done.")


if __name__ == "__main__":
    main()
