#!/usr/bin/env python3
"""Refresh 2025 NFFC Rotowire OC league_teams outcomes in Supabase.

The January 2026 pull captured team rows but outcomes (league_rank, league_points,
overall_rank, overall_points) weren't populated yet because 2025 was the current
season. Outcomes are now populated on NFFC's side. This script re-pulls just the
2025 Rotowire OC league details and upserts the outcome columns.

Usage:
    python3 refresh_2025_outcomes.py --dry-run    # fetch + preview, no write
    python3 refresh_2025_outcomes.py              # fetch + upsert to Supabase
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(HERE, "..", "..", ".env")

# ── Config from .env ─────────────────────────────────────────────────────────
with open(ENV_PATH) as f:
    _env = {k.strip(): v.strip() for k, v in
            (line.strip().split("=", 1) for line in f
             if "=" in line and not line.strip().startswith("#"))}

NFFC_API_KEY = _env["NFFC_API_KEY"]
SB_URL = "https://twfzcrodldvhpfaykasj.supabase.co"
SB_KEY = _env.get("SUPABASE_SERVICE_ROLE_KEY") or _env["SUPABASE_ANON_KEY"]

NFFC_BASE = "https://nfc.shgn.com/api/public"
YEAR = 2025
MAX_WORKERS = 5
BATCH_SIZE = 500
NFFC_HEADERS = {"User-Agent": "nfl-db/refresh-2025-outcomes"}
SB_HEADERS = {
    "apikey": SB_KEY,
    "Authorization": f"Bearer {SB_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
}


def is_rotowire_oc(name: str) -> bool:
    n = name.lower()
    return "rotowire" in n and "online" in n


def fetch_json(url: str):
    req = urllib.request.Request(url, headers=NFFC_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"_http_error": e.code, "_body": (e.read() or b"").decode(errors="replace")[:200]}
    except Exception as e:
        return {"_error": str(e)}


def get_roc_leagues():
    data = fetch_json(f"{NFFC_BASE}/historicalleagues/football/{YEAR}?api_key={NFFC_API_KEY}")
    if not isinstance(data, list):
        raise SystemExit(f"Unexpected league-list response: {data}")
    return [l for l in data if is_rotowire_oc(l["name"])]


def fetch_detail(league_id: int):
    return fetch_json(f"{NFFC_BASE}/historicalleagues/football/{YEAR}/{league_id}?api_key={NFFC_API_KEY}")


def extract_team_rows(league_id: int, detail: dict) -> list[dict]:
    rows = []
    for team in detail.get("teams", []) or []:
        rows.append({
            "league_id": league_id,
            "team_id": int(team["id"]),
            "year": YEAR,
            "draft_order": team.get("draft_order"),
            "league_rank": team.get("league_rank"),
            "league_points": float(team["league_points"]) if team.get("league_points") not in (None, "") else None,
            "overall_rank": team.get("overall_rank"),
            "overall_points": float(team["overall_points"]) if team.get("overall_points") not in (None, "") else None,
        })
    return rows


def upsert_batch(rows: list[dict]):
    data = json.dumps(rows).encode()
    req = urllib.request.Request(f"{SB_URL}/rest/v1/league_teams", data=data,
                                 headers=SB_HEADERS, method="POST")
    try:
        urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as e:
        body = (e.read() or b"").decode(errors="replace")[:400]
        raise SystemExit(f"Supabase upsert failed: {e.code} {body}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch + summarize; do not write to Supabase")
    args = parser.parse_args()

    t0 = time.time()

    print(f"[1/3] Fetching {YEAR} Rotowire OC league list...")
    leagues = get_roc_leagues()
    print(f"      {len(leagues)} ROC leagues")

    print(f"[2/3] Fetching detail for each league ({MAX_WORKERS} concurrent)...")
    all_rows = []
    errors = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_detail, l["id"]): l for l in leagues}
        done = 0
        for fut in as_completed(futures):
            done += 1
            lg = futures[fut]
            detail = fut.result()
            if not isinstance(detail, dict) or "teams" not in detail:
                errors += 1
                continue
            all_rows.extend(extract_team_rows(lg["id"], detail))
            if done % 50 == 0:
                sys.stdout.write(f"\r      {done}/{len(leagues)} ({errors} errors)")
                sys.stdout.flush()
    print(f"\r      {done}/{len(leagues)} done ({errors} errors), {len(all_rows)} team rows")

    # Sanity stats on the outcome data
    rows_with_rank = sum(1 for r in all_rows if r["league_rank"] is not None)
    rows_with_points = sum(1 for r in all_rows if r["league_points"] is not None)
    print(f"      Outcomes populated: league_rank {rows_with_rank}/{len(all_rows)}, "
          f"league_points {rows_with_points}/{len(all_rows)}")

    if args.dry_run:
        print("\n[3/3] DRY RUN — no writes. Sample row:")
        if all_rows:
            print(json.dumps(all_rows[0], indent=2))
        return

    print(f"[3/3] Upserting to Supabase in batches of {BATCH_SIZE}...")
    for i in range(0, len(all_rows), BATCH_SIZE):
        batch = all_rows[i:i + BATCH_SIZE]
        upsert_batch(batch)
        sys.stdout.write(f"\r      {min(i + BATCH_SIZE, len(all_rows))}/{len(all_rows)}")
        sys.stdout.flush()
    print(f"\r      {len(all_rows)}/{len(all_rows)} upserted ✓")

    print(f"\nDone in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
