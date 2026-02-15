#!/usr/bin/env python3
"""Load clean CSV data into Supabase tables via the REST API."""

import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error

# ── Config ──────────────────────────────────────────────────────────────────
# Load .env
env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
with open(env_path) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

SUPABASE_URL = "https://twfzcrodldvhpfaykasj.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ["SUPABASE_ANON_KEY"]
CLEAN_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "clean")
BATCH_SIZE = 500  # rows per request

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}


def post_batch(table, rows):
    """POST a batch of rows to the Supabase REST API."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    data = json.dumps(rows).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=HEADERS, method="POST")
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  ERROR on {table} batch: {e.code} {body[:300]}")
        raise


def load_csv(table, csv_filename, transform=None):
    """Read a CSV, apply optional transform, and upload in batches."""
    path = os.path.join(CLEAN_DIR, csv_filename)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch = []
        total = 0
        for row in reader:
            if transform:
                row = transform(row)
            if row is None:
                continue
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                post_batch(table, batch)
                total += len(batch)
                batch = []
                sys.stdout.write(f"\r  {table}: {total} rows loaded")
                sys.stdout.flush()
        if batch:
            post_batch(table, batch)
            total += len(batch)
    print(f"\r  {table}: {total} rows loaded ✓")
    return total


# ── Transforms ──────────────────────────────────────────────────────────────
def nullable(val):
    """Convert empty strings and NA to None."""
    return None if not val or val in ("NA", "nan", "") else val


def nullable_date(val):
    """Convert to date string or None, rejecting invalid dates."""
    v = nullable(val)
    if not v or v.startswith("0000"):
        return None
    return v


def nullable_int(val):
    """Convert to int or None."""
    v = nullable(val)
    return int(float(v)) if v else None


def nullable_float(val):
    """Convert to float or None."""
    v = nullable(val)
    return float(v) if v else None


def transform_player(row):
    # Skip blank rows
    if not row.get("player_id"):
        return None
    return {
        "player_id": row["player_id"],
        "first_name": nullable(row["first_name"]),
        "last_name": nullable(row["last_name"]),
        "position": nullable(row["position"]),
        "birth_date": nullable_date(row["birth_date"]),
        "gsis_id": nullable(row["gsis_id"]),
        "espn_id": nullable(row["espn_id"]),
        "yahoo_id": nullable(row["yahoo_id"]),
        "sleeper_id": nullable(row["sleeper_id"]),
        "pfr_id": nullable(row["pfr_id"]),
        "rotowire_id": nullable(row["rotowire_id"]),
        "headshot_url": nullable(row["headshot_url"]),
        "college": nullable(row["college"]),
        "draft_year": nullable_int(row["draft_year"]),
        "draft_round": nullable_int(row["draft_round"]),
        "draft_pick": nullable_int(row["draft_pick"]),
        "latest_team": nullable(row["latest_team"]),
        "status": nullable(row["status"]),
    }


def transform_league(row):
    return {
        "league_id": int(row["league_id"]),
        "year": int(row["year"]),
        "name": nullable(row["name"]),
        "num_teams": nullable_int(row["roster_size"]),
        "third_round_reversal": row["third_round_reversal"] == "True",
        "draft_date": nullable(row["draft_date"]),
        "draft_completed_date": nullable(row["draft_completed_date"]),
    }


def transform_league_team(row):
    return {
        "league_id": int(row["league_id"]),
        "team_id": int(row["team_id"]),
        "year": int(row["year"]),
        "draft_order": nullable_int(row["draft_order"]),
        "league_rank": nullable_int(row["league_rank"]),
        "league_points": nullable_float(row["league_points"]),
        "overall_rank": nullable_int(row["overall_rank"]),
        "overall_points": nullable_float(row["overall_points"]),
    }


def transform_adp(row):
    return {
        "player_id": row["player_id"],
        "year": int(row["year"]),
        "adp": nullable_float(row["adp"]),
        "min_pick": nullable_int(row["min_pick"]),
        "max_pick": nullable_int(row["max_pick"]),
        "times_drafted": nullable_int(row["times_drafted"]),
    }


def transform_draft_pick(row):
    return {
        "league_id": int(row["league_id"]),
        "year": int(row["year"]),
        "round": int(row["round"]),
        "pick_in_round": int(row["pick_in_round"]),
        "overall_pick": int(row["overall_pick"]),
        "team_id": int(row["team_id"]),
        "player_id": row["player_id"],
        "picked_at": nullable(row["timestamp"]),
        "pick_duration": nullable_int(row["pick_duration"]),
    }


# ── Main ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Loading clean data into Supabase...\n")
    t0 = time.time()

    # Load order respects FK constraints
    load_csv("players", "players.csv", transform_player)
    load_csv("leagues", "leagues.csv", transform_league)
    load_csv("league_teams", "league_teams.csv", transform_league_team)
    load_csv("adp", "adp.csv", transform_adp)
    load_csv("draft_picks", "draft_picks.csv", transform_draft_pick)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s")
