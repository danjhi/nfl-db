"""Load DraftKings Best Ball ADP from a downloaded CSV into adp_sources.

DraftKings auth uses a 5-minute token — fully automated fetching isn't
practical without Playwright. Instead, manually download the rankings CSV
from draftkings.com/draft/rankings/nfl (there's a download button on the
page), then run this script.

Usage:
    python3 scripts/adp/load_draftkings_adp.py [--dry-run] [path/to/csv]

CSV location (checked in order):
    1. Path passed as CLI argument
    2. ~/Downloads/DkPreDraftRankings*.csv  (most recently modified)
    3. data/imports/DkPreDraftRankings.csv  (manual copy)

CSV columns: ID, Name, Position, ADP, Team
  - ID  = DraftKings player ID (matches players.draftkings_id)
  - ADP = overall pick float (same scale as Underdog, e.g. 1.125)
"""

import csv
import datetime
import glob
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
SOURCE = "draftkings"
TODAY = datetime.date.today().isoformat()

DOWNLOADS_PATTERN = os.path.expanduser("~/Downloads/DkPreDraftRankings*.csv")
IMPORTS_FALLBACK = os.path.join(
    os.path.dirname(_script_dir), "..", "data", "imports", "DkPreDraftRankings.csv"
)


def find_csv(cli_arg=None):
    """Locate the DK rankings CSV."""
    # 1. Explicit CLI path
    if cli_arg and os.path.exists(cli_arg):
        return cli_arg

    # 2. Most recently modified file matching Downloads pattern
    matches = glob.glob(DOWNLOADS_PATTERN)
    if matches:
        path = max(matches, key=os.path.getmtime)
        print(f"  Found CSV in Downloads: {os.path.basename(path)}")
        return path

    # 3. data/imports fallback
    imports_path = os.path.normpath(IMPORTS_FALLBACK)
    if os.path.exists(imports_path):
        print(f"  Found CSV in data/imports/")
        return imports_path

    return None


def read_dk_csv(path):
    """Read DK rankings CSV, return list of dicts."""
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip instruction-only rows (no ID or ADP)
            if row.get("ID", "").strip() and row.get("ADP", "").strip():
                rows.append(row)
    return rows


def fetch_players_with_dk_id():
    """Fetch players that have draftkings_id set."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,draftkings_id,first_name,last_name,position"
            f"&draftkings_id=not.is.null"
            f"&offset={offset}&limit=1000"
        )
        req = urllib.request.Request(url, headers={
            "apikey": key, "Authorization": f"Bearer {key}",
        })
        batch = json.loads(urllib.request.urlopen(req).read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += 1000
    return players


def fetch_all_players_for_name_match():
    """Fetch all players for name-based fallback matching."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,first_name,last_name,position"
            f"&offset={offset}&limit=1000"
        )
        req = urllib.request.Request(url, headers={
            "apikey": key, "Authorization": f"Bearer {key}",
        })
        batch = json.loads(urllib.request.urlopen(req).read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += 1000
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
    cli_path = next((a for a in sys.argv[1:] if not a.startswith("--")), None)

    # ── 1. Find CSV ───────────────────────────────────────────────────────────
    csv_path = find_csv(cli_path)
    if not csv_path:
        print("ERROR: No DraftKings CSV found.")
        print("  Download from draftkings.com/draft/rankings/nfl and place in ~/Downloads/")
        print("  Or pass the path directly: python3 load_draftkings_adp.py path/to/file.csv")
        sys.exit(1)

    print(f"Loading DraftKings ADP from: {csv_path}")
    dk_rows = read_dk_csv(csv_path)
    print(f"  {len(dk_rows)} player rows in CSV")

    # ── 2. Build player lookups ───────────────────────────────────────────────
    print("Fetching players from Supabase...")
    dk_players = fetch_players_with_dk_id()
    dk_to_pid = {p["draftkings_id"]: p["player_id"] for p in dk_players if p.get("draftkings_id")}
    print(f"  {len(dk_to_pid)} players with draftkings_id")

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

    # ── 3. Match and build rows ───────────────────────────────────────────────
    adp_rows = []
    not_found = []

    for row in dk_rows:
        dk_id = row.get("ID", "").strip()
        adp_str = row.get("ADP", "").strip()
        name = row.get("Name", "").strip()
        pos = row.get("Position", "").strip().upper()

        try:
            adp_val = float(adp_str)
        except ValueError:
            continue

        # Match by draftkings_id first
        player_id = dk_to_pid.get(dk_id)

        # Fallback: name + position
        if not player_id:
            norm = normalize_name(name)
            player_id = by_name_pos.get((norm, pos)) or by_name.get(norm)

        if not player_id:
            not_found.append(f"  {name} ({pos}) [dk_id={dk_id}] adp={adp_val:.2f}")
            continue

        adp_rows.append({
            "player_id": player_id,
            "source": SOURCE,
            "year": YEAR,
            "date": TODAY,
            "adp": adp_val,
            "projected_points": None,
            "position_rank": None,
        })

    print(f"\n  Matched & ready: {len(adp_rows)}")
    print(f"  Not found in DB: {len(not_found)}")

    # ── 4. Upsert ─────────────────────────────────────────────────────────────
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

    # ── 5. Summary ────────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    print(f"DK CSV rows:         {len(dk_rows)}")
    print(f"Matched & upserted:  {len(adp_rows)}")
    print(f"Not found in DB:     {len(not_found)}")

    if not_found:
        print(f"\nUnmatched players (top 20):")
        for line in not_found[:20]:
            print(line)
        if len(not_found) > 20:
            print(f"  ... and {len(not_found) - 20} more")


if __name__ == "__main__":
    main()
