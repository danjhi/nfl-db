"""Bootstrap dan_id on the players table and load initial dynasty values.

Reads data/imports/dan_dynasty_values.csv (exported from Google Sheet),
matches each row by name + position to our players table, sets dan_id
on the player, and inserts the dynasty value into dynasty_values.

Usage:
    python3 scripts/ids/match_dan_ids.py
"""

import json
import os
import sys
import urllib.error
import urllib.request

# Add parent dir so shared imports work when run from repo root
sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    IMPORTS_DIR,
    get_all_players,
    build_player_lookup,
    normalize_name,
    normalize_team,
    read_csv,
    supabase_rest_patch,
)

CSV_PATH = os.path.join(IMPORTS_DIR, "dan_tradevalues_with_rookies.csv")


def upsert_dynasty_value(row):
    """POST a single row to dynasty_values with upsert on conflict."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/dynasty_values"
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
        print(f"Export your Google Sheet to: {CSV_PATH}")
        sys.exit(1)

    rows = read_csv(CSV_PATH)
    print(f"Read {len(rows)} rows from dynasty values CSV")

    # ── 2. Build player lookup from Supabase ─────────────────────────────────
    print("Fetching players from Supabase...")
    players = get_all_players()
    print(f"Found {len(players)} players in DB")

    by_name_pos, by_name = build_player_lookup(players)

    # ── 3. Match rows and update ─────────────────────────────────────────────
    matched = 0
    unmatched = []
    dan_id_set = 0
    values_inserted = 0
    errors = 0

    print("\nMatching and loading...")
    for i, row in enumerate(rows):
        dan_id = row.get("dan_id", "").strip()
        player_name = row.get("Player", "").strip()
        position = row.get("Position", "").upper().strip()
        value = row.get("Value", "").strip()
        sf_value = row.get("SF_Value", "").strip()

        if not dan_id or not player_name:
            continue

        norm = normalize_name(player_name)

        # Try name+position first, then name only
        player_id = by_name_pos.get((norm, position)) or by_name.get(norm)

        if not player_id:
            val = float(value) if value else 0
            unmatched.append((player_name, position, dan_id, val))
            continue

        matched += 1

        # PATCH dan_id onto the player
        try:
            supabase_rest_patch("players", "player_id", player_id, {"dan_id": dan_id})
            dan_id_set += 1
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"  ERROR setting dan_id for {player_name}: {e.code} {body}")
            errors += 1

        # Insert dynasty value
        if value:
            dv_row = {
                "player_id": player_id,
                "value": float(value),
            }
            if sf_value:
                dv_row["sf_value"] = float(sf_value)

            try:
                upsert_dynasty_value(dv_row)
                values_inserted += 1
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                print(f"  ERROR inserting value for {player_name}: {e.code} {body}")
                errors += 1

        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(rows)}...")

    # ── 4. Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"SUMMARY")
    print(f"{'='*50}")
    print(f"CSV rows:          {len(rows)}")
    print(f"Matched:           {matched}")
    print(f"dan_id set:        {dan_id_set}")
    print(f"Values inserted:   {values_inserted}")
    print(f"Errors:            {errors}")
    print(f"Unmatched:         {len(unmatched)}")

    if unmatched:
        # Sort by value descending so high-value misses are obvious
        unmatched.sort(key=lambda x: x[3], reverse=True)
        high_value = [u for u in unmatched if u[3] >= 1]
        zero_value = [u for u in unmatched if u[3] < 1]

        if high_value:
            print(f"\nUNMATCHED — Value >= 1 ({len(high_value)} players, NEEDS ATTENTION):")
            for name, pos, did, val in high_value:
                print(f"  {name} ({pos}) [dan_id={did}] Value={val}")

        if zero_value:
            print(f"\nUnmatched — Value 0 ({len(zero_value)} players, likely legacy):")
            for name, pos, did, val in zero_value[:20]:
                print(f"  {name} ({pos}) [dan_id={did}] Value={val}")
            if len(zero_value) > 20:
                print(f"  ... and {len(zero_value) - 20} more")


if __name__ == "__main__":
    main()
