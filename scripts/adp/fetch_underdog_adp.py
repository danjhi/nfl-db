"""Fetch current Underdog ADP and upsert into adp_sources.

Downloads the Underdog rankings CSV directly from their download endpoint,
matches by underdog_id (with name+position fallback), and upserts into
the adp_sources table.

Designed to be run daily.

Usage:
    python3 scripts/adp/fetch_underdog_adp.py
"""

import csv
import datetime
import io
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
    normalize_team,
    PLAYER_ALIASES,
)

# ── Config ───────────────────────────────────────────────────────────────────
YEAR = 2026
SOURCE = "underdog"
TODAY = datetime.date.today().isoformat()  # e.g. "2026-02-18"

UNDERDOG_CSV_URL = (
    "https://app.underdogfantasy.com/rankings/download/"
    "8f9df7e5-d6ab-4a51-87e1-f91f5c806912/"
    "ccf300b0-9197-5951-bd96-cba84ad71e86/"
    "978b95dd-7c25-467c-83c9-332d90a557a4"
    "?product=fantasy"
    "&product_experience_id=018e1234-5678-9abc-def0-123456789002"
    "&state_config_id=7b937c4c-58ae-467c-90e7-c8dc2202a02a"
)

# Underdog slotName -> our position
SLOT_TO_POS = {
    "QB": "QB", "RB": "RB", "WR": "WR", "TE": "TE", "K": "K",
    "FLEX": None,  # skip flex-only rows
}


def fetch_underdog_csv():
    """Download the Underdog rankings CSV."""
    req = urllib.request.Request(UNDERDOG_CSV_URL, headers={"User-Agent": "Mozilla/5.0"})
    resp = urllib.request.urlopen(req)
    text = resp.read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def fetch_players_with_underdog_id():
    """Fetch players that have underdog_id set."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    limit = 1000
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,underdog_id,first_name,last_name,position"
            f"&underdog_id=not.is.null"
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
    # ── 1. Fetch Underdog CSV ────────────────────────────────────────────────
    print("Fetching Underdog ADP CSV...")
    ud_rows = fetch_underdog_csv()
    print(f"  {len(ud_rows)} rows downloaded")

    # ── 2. Build player lookups ──────────────────────────────────────────────
    print("Fetching players from Supabase...")
    ud_players = fetch_players_with_underdog_id()
    ud_to_pid = {p["underdog_id"]: p["player_id"] for p in ud_players if p.get("underdog_id")}
    print(f"  {len(ud_to_pid)} players with underdog_id")

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

    for row in ud_rows:
        ud_id = row.get("id", "").strip()
        adp_val = row.get("adp", "").strip()
        if not adp_val or adp_val == "-":
            skipped_no_adp += 1
            continue

        first = row.get("firstName", "").strip()
        last = row.get("lastName", "").strip()
        name = f"{first} {last}"
        slot = row.get("slotName", "").strip()

        # Match by underdog_id first
        player_id = ud_to_pid.get(ud_id)

        # Fallback: name + position
        if not player_id:
            norm = normalize_name(name)
            pos = SLOT_TO_POS.get(slot, slot)
            if pos:
                player_id = by_name_pos.get((norm, pos)) or by_name.get(norm)
            else:
                player_id = by_name.get(norm)

        if not player_id:
            not_found.append(f"  {name} ({slot}) [ud_id={ud_id}] adp={adp_val}")
            continue

        proj_pts = row.get("projectedPoints", "").strip()
        pos_rank = row.get("positionRank", "").strip()

        adp_row = {
            "player_id": player_id,
            "source": SOURCE,
            "year": YEAR,
            "date": TODAY,
            "adp": float(adp_val),
            "projected_points": float(proj_pts) if proj_pts and proj_pts != "0.0" else None,
            "position_rank": pos_rank if pos_rank else None,
        }

        adp_rows.append(adp_row)

    print(f"\n  Rows with ADP: {len(adp_rows)}")
    print(f"  Skipped (no ADP): {skipped_no_adp}")
    print(f"  Not found in DB: {len(not_found)}")

    # ── 4. Upsert to Supabase ────────────────────────────────────────────────
    if adp_rows:
        print(f"\nUpserting {len(adp_rows)} rows to adp_sources...")
        inserted, errors = batch_upsert(adp_rows)
        print(f"  Inserted/updated: {inserted}")
        if errors:
            print(f"  Errors: {errors}")

    # ── 5. Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    print(f"Underdog CSV rows:   {len(ud_rows)}")
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
