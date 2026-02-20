"""Load teams CSV into Supabase teams table via REST API.

Reads data/nflreadr/teams.csv (exported from nflreadr load_teams())
and upserts all 32 teams into the teams table.

Usage:
    python3 scripts/teams/load_teams.py
"""

import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ids"))
from shared import SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_KEY, read_csv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CSV_PATH = os.path.join(ROOT, "data", "nflreadr", "teams.csv")

key = SUPABASE_SERVICE_KEY or SUPABASE_KEY


def main():
    if not os.path.exists(CSV_PATH):
        print(f"ERROR: CSV not found at {CSV_PATH}")
        print("Run: Rscript scripts/teams/export_teams.R")
        sys.exit(1)

    rows = read_csv(CSV_PATH)
    print(f"Read {len(rows)} teams from {CSV_PATH}")

    # POST all 32 rows in one batch
    url = f"{SUPABASE_URL}/rest/v1/teams"
    data = json.dumps(rows).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates",
    }, method="POST")

    try:
        urllib.request.urlopen(req)
        print(f"Upserted {len(rows)} teams")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"ERROR: {e.code} {body}")
        sys.exit(1)

    # Quick verify
    req = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/teams?select=team_abbr,team_name,team_conf&order=team_abbr",
        headers={"apikey": key, "Authorization": f"Bearer {key}"},
    )
    teams = json.loads(urllib.request.urlopen(req).read().decode())
    print(f"\nVerified {len(teams)} teams in DB:")
    for t in teams:
        print(f"  {t['team_abbr']:<4} {t['team_name']:<30} {t['team_conf']}")


if __name__ == "__main__":
    main()
