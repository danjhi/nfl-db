"""Fetch current Drafters ADP and upsert into adp_sources.

Calls the Drafters node API, matches players by drafters_id (with
name+position fallback), and upserts into adp_sources.

Designed to be run daily. Requires DRAFTERS_JWT in .env — when the JWT
expires you'll get a 401 error; update DRAFTERS_JWT in .env to refresh.

Usage:
    python3 scripts/adp/fetch_drafters_adp.py [--dry-run]

ADP note: Drafters stores ADP in round.pick format (e.g. mod_adp=1.089
means round 1, pick ~1). This is stored as-is in adp_sources.adp.
"""

import datetime
import json
import os
import sys
import urllib.error
import urllib.request

# Add ids dir so shared imports work
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
SOURCE = "drafters"
TODAY = datetime.date.today().isoformat()

# Drafters API endpoint
# getUserRankData/{userId}/{sportId}/{formatId}
#   28941 = your Drafters user ID
#   2     = NFL
#   0     = best ball format
DRAFTERS_API_URL = "https://node.drafters.com/getUserRankData/28941/2/0"


def get_jwt():
    """Read DRAFTERS_JWT from environment (loaded from .env by shared.py)."""
    token = os.environ.get("DRAFTERS_JWT", "").strip()
    if not token:
        print("ERROR: DRAFTERS_JWT not set in .env")
        sys.exit(1)
    return token


def fetch_drafters_players(jwt):
    """Call Drafters API and return the players array."""
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
    """Fetch players that have drafters_id set."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    limit = 1000
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,drafters_id,first_name,last_name,position"
            f"&drafters_id=not.is.null"
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


def fetch_all_players_for_name_match():
    """Fetch all players for name-based fallback matching."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    limit = 1000
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,first_name,last_name,position"
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


def batch_upsert(rows, batch_size=100):
    """POST rows to adp_sources in batches."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/adp_sources"
    inserted = 0
    errors = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        data = json.dumps(batch).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
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

    # ── 1. Fetch Drafters data ────────────────────────────────────────────────
    print("Fetching Drafters ADP...")
    jwt = get_jwt()
    dr_players = fetch_drafters_players(jwt)
    print(f"  {len(dr_players)} players in response")

    # ── 2. Build player lookups ───────────────────────────────────────────────
    print("Fetching players from Supabase...")
    dr_db_players = fetch_players_with_drafters_id()
    dr_to_pid = {p["drafters_id"]: p["player_id"] for p in dr_db_players if p.get("drafters_id")}
    print(f"  {len(dr_to_pid)} players with drafters_id")

    # Name fallback
    all_players = fetch_all_players_for_name_match()
    by_name_pos = {}
    by_name = {}
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

    # ── 3. Match and build adp_sources rows ──────────────────────────────────
    adp_rows = []
    skipped_no_adp = 0
    not_found = []

    for player in dr_players:
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

        first = (player.get("fn") or "").strip()
        last = (player.get("ln") or "").strip()
        name = f"{first} {last}".strip()
        pos = (player.get("pn") or "").upper().strip()

        # Match by drafters_id first
        player_id = dr_to_pid.get(dr_id)

        # Fallback: name + position
        if not player_id:
            norm = normalize_name(name)
            player_id = by_name_pos.get((norm, pos)) or by_name.get(norm)

        if not player_id:
            not_found.append(f"  {name} ({pos}) [dr_id={dr_id}] adp={adp_val}")
            continue

        adp_rows.append({
            "player_id": player_id,
            "source": SOURCE,
            "year": YEAR,
            "date": TODAY,
            "adp": adp_val,
            "projected_points": None,
            "position_rank": str(player.get("player_rank", "")) or None,
        })

    # Deduplicate by player_id — keep lowest ADP (most meaningful rank)
    seen = {}
    for row in adp_rows:
        pid = row["player_id"]
        if pid not in seen or row["adp"] < seen[pid]["adp"]:
            seen[pid] = row
    adp_rows = list(seen.values())

    print(f"\n  Rows with ADP: {len(adp_rows)}")
    print(f"  Skipped (no ADP): {skipped_no_adp}")
    print(f"  Not found in DB: {len(not_found)}")

    # ── 4. Upsert to Supabase ────────────────────────────────────────────────
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

    # ── 5. Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    print(f"Drafters players:    {len(dr_players)}")
    print(f"With ADP value:      {len(adp_rows) + len(not_found)}")
    print(f"Matched & upserted:  {len(adp_rows)}")
    print(f"Not found in DB:     {len(not_found)}")
    print(f"Skipped (no ADP):    {skipped_no_adp}")

    if not_found:
        print(f"\nUnmatched players (top 20):")
        for line in not_found[:20]:
            print(line)
        if len(not_found) > 20:
            print(f"  ... and {len(not_found) - 20} more")


if __name__ == "__main__":
    main()
