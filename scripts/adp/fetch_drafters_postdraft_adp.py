"""Fetch Drafters POST-DRAFT ADP and upsert into adp_sources.

Drafters reuses the same `formatId=0` endpoint for both pre-draft and post-draft.
After the 2026 NFL Draft (~Apr 27, 2026) Drafters cut over to post-draft data on
that same endpoint AND changed their JSON field names from full names
(`first_name`, `last_name`, `position`, `nfl_team`) to abbreviations
(`fn`, `ln`, `pn`, `tn`). This script targets the new shape.

The existing fetch_drafters_adp.py (pre-draft) is now broken on the same URL;
since the pre-draft date window (Feb 19 – Apr 22) is already over, we just leave
it untouched and use this new script going forward.

Usage:
    python3 scripts/adp/fetch_drafters_postdraft_adp.py [--dry-run]

ADP note: Drafters stores ADP in round.pick float format (e.g. mod_adp=1.089
means round 1, pick ~1). Stored as-is in adp_sources.adp.
"""

import datetime
import json
import os
import sys
import urllib.error
import urllib.request

_script_dir = os.path.dirname(os.path.abspath(__file__)) if os.path.exists(__file__) else os.path.join("scripts", "adp")
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import (
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    normalize_name,
    PLAYER_ALIASES,
)

# ── Config ────────────────────────────────────────────────────────────────────
YEAR = 2026
SOURCE = "drafters_postdraft"
TODAY = datetime.date.today().isoformat()

# Same URL as pre-draft — Drafters reuses formatId=0 for both contest types.
DRAFTERS_API_URL = "https://node.drafters.com/getUserRankData/28941/2/0"


def get_jwt():
    token = os.environ.get("DRAFTERS_JWT", "").strip()
    if not token:
        print("ERROR: DRAFTERS_JWT not set in .env")
        sys.exit(1)
    return token


def fetch_drafters_players(jwt):
    req = urllib.request.Request(
        DRAFTERS_API_URL,
        headers={
            "Authorization": f"Bearer {jwt}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
    )
    try:
        resp = urllib.request.urlopen(req)
        data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 401:
            print("ERROR: 401 Unauthorized — DRAFTERS_JWT has expired.")
            print("  Update DRAFTERS_JWT in .env with a fresh token from your browser's localStorage.")
        else:
            body = e.read().decode("utf-8", errors="replace")
            print(f"ERROR: HTTP {e.code}: {body[:200]}")
        sys.exit(1)

    players = data.get("entities", {}).get("players", [])
    if not players:
        print("ERROR: No players found in response. Response keys:", list(data.keys()))
        sys.exit(1)
    return players


def fetch_players_with_drafters_id():
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,drafters_id,first_name,last_name,position"
            f"&drafters_id=not.is.null"
            f"&offset={offset}&limit=1000"
        )
        req = urllib.request.Request(url, headers={"apikey": key, "Authorization": f"Bearer {key}"})
        batch = json.loads(urllib.request.urlopen(req).read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += 1000
    return players


def fetch_all_players_for_name_match():
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,first_name,last_name,position"
            f"&offset={offset}&limit=1000"
        )
        req = urllib.request.Request(url, headers={"apikey": key, "Authorization": f"Bearer {key}"})
        batch = json.loads(urllib.request.urlopen(req).read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += 1000
    return players


def batch_upsert(rows, batch_size=100):
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/adp_sources"
    inserted, errors = 0, 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        data = json.dumps(batch).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={
            "apikey": key, "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates",
        }, method="POST")
        try:
            urllib.request.urlopen(req)
            inserted += len(batch)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"  ERROR batch at row {i}: {e.code} {body}")
            errors += len(batch)
    return inserted, errors


def main():
    dry_run = "--dry-run" in sys.argv

    print("Fetching Drafters post-draft ADP...")
    jwt = get_jwt()
    dr_players = fetch_drafters_players(jwt)
    print(f"  {len(dr_players)} players in response")

    print("Fetching players from Supabase...")
    db_players = fetch_players_with_drafters_id()
    dr_to_pid = {p["drafters_id"]: p["player_id"] for p in db_players if p.get("drafters_id")}
    print(f"  {len(dr_to_pid)} players with drafters_id")

    all_players = fetch_all_players_for_name_match()
    by_name_pos, by_name = {}, {}
    for p in all_players:
        full_name = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
        norm = normalize_name(full_name)
        pos = (p.get("position") or "").upper()
        by_name_pos[(norm, pos)] = p["player_id"]
        by_name[norm] = p["player_id"]
        alias = PLAYER_ALIASES.get(norm)
        if alias:
            by_name_pos[(alias, pos)] = p["player_id"]
            by_name[alias] = p["player_id"]

    adp_rows = []
    not_found = []
    skipped_no_adp = 0
    matched_by_id = 0
    matched_by_name = 0

    for player in dr_players:
        # New abbreviated field names: fn, ln, pn (position), tn (nfl_team), mod_adp
        dr_id = str(player.get("id", "")).strip()
        mod_adp = player.get("mod_adp")
        if not mod_adp:
            skipped_no_adp += 1
            continue
        try:
            adp_val = float(mod_adp)
        except (ValueError, TypeError):
            skipped_no_adp += 1
            continue

        # Drafters uses 9999 as "no rank" sentinel
        if adp_val >= 9999:
            skipped_no_adp += 1
            continue

        first = (player.get("fn") or "").strip()
        last = (player.get("ln") or "").strip()
        name = f"{first} {last}".strip()
        pos = (player.get("pn") or "").strip().upper()

        # Match by drafters_id first, then name+pos fallback
        player_id = dr_to_pid.get(dr_id)
        if player_id:
            matched_by_id += 1
        else:
            norm = normalize_name(name)
            player_id = by_name_pos.get((norm, pos)) or by_name.get(norm)
            if player_id:
                matched_by_name += 1

        if not player_id:
            not_found.append(f"  {name} ({pos}) [dr_id={dr_id}] adp={adp_val}")
            continue

        adp_rows.append({
            "player_id": player_id,
            "source": SOURCE,
            "year": YEAR,
            "date": TODAY,
            "adp": adp_val,
            "projected_points": player.get("fpts"),
            "position_rank": None,
        })

    print(f"\n  Matched by drafters_id:  {matched_by_id}")
    print(f"  Matched by name+pos:     {matched_by_name}")
    print(f"  Skipped (no ADP):        {skipped_no_adp}")
    print(f"  Not found in DB:         {len(not_found)}")

    if adp_rows and not dry_run:
        print(f"\nUpserting {len(adp_rows)} rows to adp_sources...")
        inserted, errors = batch_upsert(adp_rows)
        print(f"  Inserted/updated: {inserted}")
        if errors:
            print(f"  Errors: {errors}")
    elif dry_run:
        print(f"\n[DRY RUN] Would upsert {len(adp_rows)} rows. Sample:")
        for row in adp_rows[:3]:
            print(f"  {row}")

    print(f"\n{'='*50}\nSUMMARY\n{'='*50}")
    print(f"Drafters players in response: {len(dr_players)}")
    print(f"Matched & upserted:           {len(adp_rows)}")
    print(f"Not found in DB:              {len(not_found)}")
    print(f"Skipped (no ADP):             {skipped_no_adp}")

    if not_found:
        print(f"\nUnmatched players (top 20):")
        for line in not_found[:20]:
            print(line)
        if len(not_found) > 20:
            print(f"  ... and {len(not_found) - 20} more")


if __name__ == "__main__":
    main()
