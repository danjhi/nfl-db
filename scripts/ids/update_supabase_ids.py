#!/usr/bin/env python3
"""Unified Supabase ID updater.

Reads all data/matched/*.json files and UPDATEs player rows in Supabase
via the REST API (using anon key with RLS UPDATE policy).
Reports coverage stats after completion.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
sys.path.insert(0, os.path.dirname(__file__))
from shared import SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_KEY, MATCHED_DIR

ALL_ID_COLUMNS = [
    # Original columns (from NFFC/nflreadr load)
    "gsis_id", "espn_id", "yahoo_id", "sleeper_id", "pfr_id", "rotowire_id",
    # Added in Phase 1
    "pff_id", "fantasypros_id", "mfl_id", "stats_id", "stats_global_id",
    "fantasy_data_id", "cbs_id", "fleaflicker_id", "swish_id", "ktc_id",
    "cfbref_id", "rotoworld_id", "sportsdata_id", "footballguys_id",
    "fanduel_id", "draftkings_id", "underdog_id", "drafters_id",
]

SOURCES = [
    "nflreadr_ids.json",
    "sportsdata_ids.json",
    "sleeper_ids.json",
    "underdog_ids.json",
    "dk_ids.json",
    "drafters_ids.json",
    "fbg_ids.json",
]

_WRITE_KEY = SUPABASE_SERVICE_KEY or SUPABASE_KEY
HEADERS = {
    "apikey": _WRITE_KEY,
    "Authorization": f"Bearer {_WRITE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}


def load_and_merge():
    """Load all matched JSON files and merge."""
    merged = {}
    for src_file in SOURCES:
        path = os.path.join(MATCHED_DIR, src_file)
        if not os.path.exists(path):
            print(f"  Skipping {src_file} (not found)")
            continue
        with open(path) as f:
            data = json.load(f)
        count = 0
        for player_id, updates in data.items():
            if player_id not in merged:
                merged[player_id] = {}
            for col, val in updates.items():
                if col in ALL_ID_COLUMNS and val:
                    if col not in merged[player_id]:
                        merged[player_id][col] = val
                        count += 1
        print(f"  {src_file}: {len(data)} players, {count} new ID values")
    return merged


def patch_player(player_id, updates):
    """PATCH a single player via REST API."""
    url = f"{SUPABASE_URL}/rest/v1/players?player_id=eq.{player_id}"
    data = json.dumps(updates).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=HEADERS, method="PATCH")
    urllib.request.urlopen(req)


def batch_update(merged):
    """Update all players via REST API PATCH requests."""
    items = [(pid, u) for pid, u in merged.items() if u]
    total = len(items)
    updated = 0
    errors = 0

    for player_id, updates in items:
        try:
            patch_player(player_id, updates)
            updated += 1
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            if errors < 5:
                print(f"\n  ERROR {player_id}: {e.code} {body[:200]}")
            errors += 1

        if updated % 100 == 0:
            sys.stdout.write(f"\r  Updated {updated}/{total} players...")
            sys.stdout.flush()

    print(f"\r  Updated {updated}/{total} players ({errors} errors)    ")
    return updated, errors


def report_coverage():
    """Query Supabase for ID coverage via REST API."""
    print("\n── ID Coverage Report ──")
    all_players = []
    offset = 0
    select = "player_id," + ",".join(ALL_ID_COLUMNS)
    while True:
        url = f"{SUPABASE_URL}/rest/v1/players?select={select}&offset={offset}&limit=1000"
        req = urllib.request.Request(url, headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        })
        resp = urllib.request.urlopen(req)
        batch = json.loads(resp.read().decode("utf-8"))
        if not batch:
            break
        all_players.extend(batch)
        offset += 1000

    total = len(all_players)
    results = []
    for col in ALL_ID_COLUMNS:
        count = sum(1 for p in all_players if p.get(col))
        results.append((col, count))

    results.sort(key=lambda x: -x[1])
    for col, count in results:
        pct = count / total * 100 if total else 0
        bar = "#" * int(pct / 2)
        print(f"  {col:20s}: {count:>5}  ({pct:4.1f}%) {bar}")
    print(f"\n  {'TOTAL PLAYERS':20s}: {total:>5}")


def main():
    print("Loading matched ID files...\n")
    merged = load_and_merge()

    players_with_updates = sum(1 for u in merged.values() if u)
    total_ids = sum(len(u) for u in merged.values())
    print(f"\nMerged: {players_with_updates} players with {total_ids} total ID values\n")

    if not players_with_updates:
        print("Nothing to update.")
        return

    print("Updating Supabase via REST API...")
    t0 = time.time()
    updated, errors = batch_update(merged)
    elapsed = time.time() - t0
    print(f"Done in {elapsed:.1f}s")

    report_coverage()


if __name__ == "__main__":
    main()
