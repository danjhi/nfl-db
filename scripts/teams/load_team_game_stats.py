"""Load team_game_stats CSV into Supabase via REST API.

Reads data/nflreadr/team_game_stats.csv (exported from build_team_game_stats.R)
and upserts all rows into the team_game_stats table. Generated columns
(PPR variants) are excluded from the payload — Postgres computes them.

Usage:
    python3 scripts/teams/load_team_game_stats.py
"""

import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ids"))
from shared import SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_KEY, read_csv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CSV_PATH = os.path.join(ROOT, "data", "nflreadr", "team_game_stats.csv")

key = SUPABASE_SERVICE_KEY or SUPABASE_KEY

# Generated columns must NOT be in the POST payload
GENERATED_COLS = {
    "off_recv_fp_hppr", "off_recv_fp_ppr",
    "off_total_fp_hppr", "off_total_fp_ppr",
    "qb_fp_hppr", "qb_fp_ppr",
    "rb_fp_hppr", "rb_fp_ppr",
    "wr_fp_hppr", "wr_fp_ppr",
    "te_fp_hppr", "te_fp_ppr",
    "def_recv_fp_hppr", "def_recv_fp_ppr",
    "def_total_fp_hppr", "def_total_fp_ppr",
    "def_qb_fp_hppr", "def_qb_fp_ppr",
    "def_rb_fp_hppr", "def_rb_fp_ppr",
    "def_wr_fp_hppr", "def_wr_fp_ppr",
    "def_te_fp_hppr", "def_te_fp_ppr",
}

# Integer columns
INT_COLS = {
    "season", "week", "team_score", "opp_score",
    "pass_att", "pass_cmp", "pass_yds", "pass_td", "pass_int",
    "rush_att", "rush_yds", "rush_td",
    "targets", "receptions", "rec_yds", "rec_td",
    "qb_rec", "rb_rec", "wr_rec", "te_rec",
    "def_receptions",
    "def_qb_rec", "def_rb_rec", "def_wr_rec", "def_te_rec",
}

# Float columns
FLOAT_COLS = {
    "spread", "total_line", "implied_total",
    "off_pass_fp", "off_rush_fp", "off_recv_fp", "off_total_fp",
    "qb_fp", "rb_fp", "wr_fp", "te_fp",
    "def_pass_fp", "def_rush_fp", "def_recv_fp", "def_total_fp",
    "def_qb_fp", "def_rb_fp", "def_wr_fp", "def_te_fp",
}


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
    url = f"{SUPABASE_URL}/rest/v1/team_game_stats"
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
            print(f"  ERROR batch at row {i}: {e.code} {body}")
            errors += len(batch)

        if (i + batch_size) % 1000 == 0 or i + batch_size >= len(padded):
            print(f"  {min(inserted + errors, len(padded))}/{len(padded)}...")

    return inserted, errors


def main():
    if not os.path.exists(CSV_PATH):
        print(f"ERROR: CSV not found at {CSV_PATH}")
        print("Run: Rscript scripts/teams/build_team_game_stats.R")
        sys.exit(1)

    rows = read_csv(CSV_PATH)
    print(f"Read {len(rows)} rows from {CSV_PATH}")

    # Transform
    transformed = [transform_row(r) for r in rows]
    print(f"Transformed {len(transformed)} rows (excluded {len(GENERATED_COLS)} generated columns)")

    # Upsert
    print(f"\nUpserting {len(transformed)} rows...")
    inserted, errors = batch_upsert(transformed)
    print(f"\nInserted/updated: {inserted}")
    if errors:
        print(f"Errors: {errors}")

    # Quick verify
    url = (
        f"{SUPABASE_URL}/rest/v1/team_game_stats"
        f"?select=season,team&order=season,team&limit=1"
    )
    req = urllib.request.Request(url, headers={
        "apikey": key, "Authorization": f"Bearer {key}",
        "Prefer": "count=exact",
    })
    resp = urllib.request.urlopen(req)
    count = resp.headers.get("content-range", "").split("/")[-1]
    print(f"\nVerified: {count} rows in team_game_stats")


if __name__ == "__main__":
    main()
