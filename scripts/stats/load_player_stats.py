"""Load player_stats CSV into Supabase via REST API.

Reads data/nflreadr/player_stats.csv (exported from build_player_stats.R)
and upserts all rows into the player_stats table. Generated columns
(PPR variants) are excluded from the payload — Postgres computes them.

Usage:
    python3 scripts/stats/load_player_stats.py
"""

import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ids"))
from shared import SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_KEY, read_csv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CSV_PATH = os.path.join(ROOT, "data", "nflreadr", "player_stats.csv")

key = SUPABASE_SERVICE_KEY or SUPABASE_KEY

# Generated columns must NOT be in the POST payload
GENERATED_COLS = {"fantasy_points_hppr", "fantasy_points_ppr"}

# Integer columns
INT_COLS = {
    "season", "week",
    "pass_att", "pass_cmp", "pass_yds", "pass_td", "pass_int",
    "sacks", "sack_yds", "sack_fumbles_lost",
    "pass_air_yds", "pass_yac", "pass_first_downs", "pass_2pt",
    "rush_att", "rush_yds", "rush_td", "rush_fumbles_lost",
    "rush_first_downs", "rush_2pt",
    "targets", "receptions", "rec_yds", "rec_td", "rec_fumbles_lost",
    "rec_air_yds", "rec_yac", "rec_first_downs", "rec_2pt",
    "special_teams_tds",
}

# Float columns
FLOAT_COLS = {"fantasy_points"}


def transform_row(row):
    """Convert a CSV row dict to proper types, excluding generated columns."""
    out = {}
    for k, v in row.items():
        if k in GENERATED_COLS:
            continue
        if v == "" or v is None:
            out[k] = None
        elif k in INT_COLS:
            out[k] = int(float(v))
        elif k in FLOAT_COLS:
            out[k] = float(v)
        else:
            out[k] = v
    return out


def batch_upsert(rows, batch_size=500):
    """POST rows in batches with upsert."""
    url = f"{SUPABASE_URL}/rest/v1/player_stats"
    inserted = 0
    errors = 0

    # Ensure all rows have identical keys
    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())
    padded = [{k: r.get(k) for k in sorted(all_keys)} for r in rows]

    for i in range(0, len(padded), batch_size):
        batch = padded[i:i + batch_size]
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
            print(f"  ERROR batch at row {i}: {e.code} {body[:500]}")
            errors += len(batch)

        if (i + batch_size) % 2000 == 0 or i + batch_size >= len(padded):
            print(f"  {min(inserted + errors, len(padded))}/{len(padded)}...")

    return inserted, errors


def get_valid_player_ids():
    """Fetch all player_ids from the players table."""
    ids = set()
    offset = 0
    limit = 1000
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/players"
               f"?select=player_id&offset={offset}&limit={limit}")
        req = urllib.request.Request(url, headers={
            "apikey": key, "Authorization": f"Bearer {key}",
        })
        resp = urllib.request.urlopen(req)
        batch = json.loads(resp.read().decode("utf-8"))
        if not batch:
            break
        ids.update(r["player_id"] for r in batch)
        offset += limit
    return ids


def main():
    if not os.path.exists(CSV_PATH):
        print(f"ERROR: CSV not found at {CSV_PATH}")
        print("Run: Rscript scripts/stats/build_player_stats.R")
        sys.exit(1)

    rows = read_csv(CSV_PATH)
    print(f"Read {len(rows)} rows from {CSV_PATH}")

    # Filter to only players in our DB (FK constraint)
    print("Fetching valid player_ids from players table...")
    valid_ids = get_valid_player_ids()
    print(f"  {len(valid_ids)} players in DB")
    rows = [r for r in rows if r.get("player_id") in valid_ids]
    print(f"  {len(rows)} rows after filtering to DB players")

    transformed = [transform_row(r) for r in rows]
    print(f"Transformed {len(transformed)} rows "
          f"(excluded {len(GENERATED_COLS)} generated columns)")

    print(f"\nUpserting {len(transformed)} rows...")
    inserted, errors = batch_upsert(transformed)
    print(f"\nInserted/updated: {inserted}")
    if errors:
        print(f"Errors: {errors}")

    # Quick verify via content-range header
    url = (
        f"{SUPABASE_URL}/rest/v1/player_stats"
        f"?select=season,player_id&order=season,player_id&limit=1"
    )
    req = urllib.request.Request(url, headers={
        "apikey": key, "Authorization": f"Bearer {key}",
        "Prefer": "count=exact",
    })
    resp = urllib.request.urlopen(req)
    count = resp.headers.get("content-range", "").split("/")[-1]
    print(f"\nVerified: {count} rows in player_stats")


if __name__ == "__main__":
    main()
