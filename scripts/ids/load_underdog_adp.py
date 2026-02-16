"""Load Underdog ADP data into the adp_sources table in Supabase.

Reads data/imports/underdog_ADP.csv, matches each player by underdog_id
to our players table, and inserts rows into adp_sources via REST API.

Usage:
    python3 scripts/ids/load_underdog_adp.py
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

# Add parent dir so shared imports work when run from repo root
sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    IMPORTS_DIR,
    read_csv,
)

# ── Config ───────────────────────────────────────────────────────────────────
YEAR = 2026
SOURCE = "underdog"
CSV_PATH = os.path.join(IMPORTS_DIR, "underdog_ADP.csv")


def get_players_with_underdog_id():
    """Fetch all players that have an underdog_id set, with pagination."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    limit = 1000
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,underdog_id,first_name,last_name"
            f"&underdog_id=not.is.null"
            f"&offset={offset}&limit={limit}"
        )
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


def insert_adp_row(row):
    """POST a single row to the adp_sources table via REST API.

    Uses Prefer: resolution=merge-duplicates with the on-conflict columns
    so re-runs upsert instead of failing on duplicates.
    """
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/adp_sources"
    data = json.dumps(row).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates",
    }, method="POST")
    urllib.request.urlopen(req)


def main():
    # ── 1. Read the CSV ──────────────────────────────────────────────────────
    if not os.path.exists(CSV_PATH):
        print(f"ERROR: CSV not found at {CSV_PATH}")
        sys.exit(1)

    rows = read_csv(CSV_PATH)
    print(f"Read {len(rows)} rows from Underdog CSV")

    # ── 2. Build underdog_id → player_id lookup ─────────────────────────────
    print("Fetching players with underdog_id from Supabase...")
    players = get_players_with_underdog_id()
    print(f"Found {len(players)} players with underdog_id set")

    ud_to_pid = {}
    for p in players:
        ud_id = p.get("underdog_id")
        if ud_id:
            ud_to_pid[ud_id] = p["player_id"]

    # ── 3. Match CSV rows and insert ─────────────────────────────────────────
    inserted = 0
    skipped = 0
    errors = 0
    not_found = []

    print(f"\nInserting ADP rows for year={YEAR}, source={SOURCE}...")
    for i, row in enumerate(rows):
        ud_id = row.get("id", "").strip()
        if not ud_id:
            skipped += 1
            continue

        player_id = ud_to_pid.get(ud_id)
        if not player_id:
            first = row.get("firstName", "")
            last = row.get("lastName", "")
            skipped += 1
            not_found.append(f"  {first} {last} (underdog_id={ud_id})")
            continue

        # Parse numeric fields
        adp_val = row.get("adp", "").strip()
        proj_pts = row.get("projectedPoints", "").strip()
        pos_rank = row.get("positionRank", "").strip()

        if not adp_val:
            skipped += 1
            continue

        adp_row = {
            "player_id": player_id,
            "source": SOURCE,
            "year": YEAR,
            "adp": float(adp_val),
        }

        # Optional fields — only include if present
        if proj_pts:
            adp_row["projected_points"] = float(proj_pts)
        if pos_rank:
            adp_row["position_rank"] = pos_rank

        try:
            insert_adp_row(adp_row)
            inserted += 1
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            first = row.get("firstName", "")
            last = row.get("lastName", "")
            print(f"  ERROR inserting {first} {last}: {e.code} {body}")
            errors += 1

        # Progress update every 100 rows
        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(rows)}...")

    # ── 4. Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"SUMMARY")
    print(f"{'='*50}")
    print(f"CSV rows:       {len(rows)}")
    print(f"Inserted:       {inserted}")
    print(f"Skipped:        {skipped}")
    print(f"Errors:         {errors}")
    print(f"Not found:      {len(not_found)}")

    if not_found:
        print(f"\nPlayers not found in DB (no matching underdog_id):")
        for name in not_found[:30]:
            print(name)
        if len(not_found) > 30:
            print(f"  ... and {len(not_found) - 30} more")


if __name__ == "__main__":
    main()
