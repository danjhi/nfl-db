"""Load dynasty value change log CSV into dynasty_value_history table.

Reads data/imports/Change Log DTVC - Sheet1.csv, matches Player names
to player_id via dan_id (from dynasty_values.csv) with name-matching fallback,
and inserts into dynasty_value_history.

Usage:
    python3 scripts/ids/load_dynasty_value_history.py
"""

import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    IMPORTS_DIR,
    normalize_name,
    read_csv,
)

CSV_PATH = os.path.join(IMPORTS_DIR, "Change Log DTVC - Sheet1.csv")
DYNASTY_VALUES_CSV = os.path.join(IMPORTS_DIR, "dynasty_values.csv")


def get_players_with_dan_id():
    """Fetch all players with dan_id set from Supabase."""
    players = []
    offset = 0
    limit = 1000
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,first_name,last_name,dan_id"
            f"&dan_id=not.is.null"
            f"&offset={offset}&limit={limit}"
        )
        req = urllib.request.Request(url, headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        })
        resp = urllib.request.urlopen(req)
        batch = json.loads(resp.read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += limit
    return players


def get_all_players():
    """Fetch all players from Supabase for name-based fallback."""
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
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        })
        resp = urllib.request.urlopen(req)
        batch = json.loads(resp.read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += limit
    return players


def insert_row(row):
    """POST a single row to dynasty_value_history with upsert."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/dynasty_value_history"
    data = json.dumps(row).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates",
    }, method="POST")
    urllib.request.urlopen(req)


def main():
    if not os.path.exists(CSV_PATH):
        print(f"ERROR: CSV not found at {CSV_PATH}")
        sys.exit(1)

    # ── 1. Build Player name → dan_id mapping from dynasty values CSV ──────
    if not os.path.exists(DYNASTY_VALUES_CSV):
        print(f"WARNING: {DYNASTY_VALUES_CSV} not found, skipping dan_id mapping")
        name_to_dan_id = {}
    else:
        dv_rows = read_csv(DYNASTY_VALUES_CSV)
        name_to_dan_id = {}
        for r in dv_rows:
            player = r.get("Player", "").strip()
            dan_id = r.get("dan_id", "").strip()
            if player and dan_id:
                name_to_dan_id[normalize_name(player)] = dan_id
        print(f"Loaded {len(name_to_dan_id)} Player→dan_id mappings from dynasty_values.csv")

    # ── 2. Build dan_id → player_id from Supabase ─────────────────────────
    print("Fetching players with dan_id from Supabase...")
    dan_players = get_players_with_dan_id()
    dan_id_to_player_id = {p["dan_id"]: p["player_id"] for p in dan_players}
    print(f"Found {len(dan_id_to_player_id)} players with dan_id in DB")

    # ── 3. Build name → player_id fallback from all players ───────────────
    print("Fetching all players for name fallback...")
    all_players = get_all_players()
    name_to_player_id = {}
    for p in all_players:
        full = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
        name_to_player_id[normalize_name(full)] = p["player_id"]
    print(f"Built name lookup for {len(name_to_player_id)} players")

    # ── 4. Read change log and insert ──────────────────────────────────────
    rows = read_csv(CSV_PATH)
    print(f"\nRead {len(rows)} change log rows")

    inserted = 0
    skipped_no_match = []
    errors = 0

    for i, row in enumerate(rows):
        player_name = row.get("Player", "").strip()
        date_str = row.get("Date", "").strip()
        old_val = row.get("Old", "").strip()
        new_val = row.get("New", "").strip()
        comment = row.get("Comment", "").strip()

        if not player_name or not date_str:
            continue

        # Parse date from M/D/YYYY to YYYY-MM-DD
        try:
            parsed_date = datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            print(f"  WARNING: Bad date '{date_str}' for {player_name}, skipping")
            continue

        # Resolve player_id: Player name → dan_id → player_id, fallback to name match
        norm = normalize_name(player_name)
        player_id = None

        dan_id = name_to_dan_id.get(norm)
        if dan_id:
            player_id = dan_id_to_player_id.get(dan_id)

        if not player_id:
            player_id = name_to_player_id.get(norm)

        if not player_id:
            skipped_no_match.append(player_name)
            continue

        db_row = {
            "player_id": player_id,
            "date": parsed_date,
        }
        if old_val:
            db_row["old_value"] = float(old_val)
        if new_val:
            db_row["new_value"] = float(new_val)
        if comment:
            db_row["comment"] = comment

        try:
            insert_row(db_row)
            inserted += 1
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"  ERROR inserting {player_name} ({parsed_date}): {e.code} {body}")
            errors += 1

        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(rows)}...")

    # ── 5. Summary ─────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"SUMMARY")
    print(f"{'='*50}")
    print(f"CSV rows:       {len(rows)}")
    print(f"Inserted:       {inserted}")
    print(f"Errors:         {errors}")
    print(f"No match:       {len(skipped_no_match)}")

    if skipped_no_match:
        unique = sorted(set(skipped_no_match))
        print(f"\nUNMATCHED PLAYERS ({len(unique)} unique):")
        for name in unique:
            print(f"  {name}")


if __name__ == "__main__":
    main()
