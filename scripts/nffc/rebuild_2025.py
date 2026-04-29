#!/usr/bin/env python3
"""Rebuild 2025 NFFC Rotowire OC data in Supabase from the historical endpoint,
preserving draft_date / draft_completed_date / picked_at / pick_duration from
the old Supabase rows via the '#N' suffix bridge.

Background: January 2026 pull used the current-season endpoint, which used
different league ids than the historical endpoint returns now. All 363 existing
2025 league rows in Supabase are orphaned from NFFC's current id space. But the
'#N' suffix in each league name is stable across the transition, so we can
merge preserved timestamps from the old rows onto the new rows.

- 356/363 leagues bridge cleanly via '#N'; 7 Live venue leagues don't bridge
  (names truncated in the historical endpoint) — those rows rebuild with
  NULL timestamps.

Usage:
    python3 rebuild_2025.py --dry-run    # fetch + build + summarize, no writes
    python3 rebuild_2025.py              # full rebuild (destructive)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

HERE = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(HERE, "..", "..", ".env")

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
PAGE_SIZE = 1000
NFFC_HEADERS = {"User-Agent": "nfl-db/rebuild-2025"}
SB_HEADERS = {
    "apikey": SB_KEY,
    "Authorization": f"Bearer {SB_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}


def extract_hash_n(name: str):
    m = re.search(r'#(\d+)\s*$', (name or "").strip())
    return int(m.group(1)) if m else None


def live_match_key(name: str):
    """For Live venue leagues (no #N), return everything after 'Live '
    with any trailing '#N' stripped. Used to bridge old↔new Live leagues where
    NFFC truncated the new name (e.g. 'Las Vegas (2) 9/4 @ 2:00 pm PT'
    → 'Las Vegas (2) 9/4 @ 2:0'). Match rule: new_key is a prefix of old_key."""
    if not name:
        return None
    m = re.search(r'Live\s+(.+?)(\s*#\d+)?$', name)
    if not m:
        return None
    return m.group(1).strip()


def is_rotowire_oc(name: str) -> bool:
    n = (name or "").lower()
    return "rotowire" in n and "online" in n


# ─── HTTP ───────────────────────────────────────────────────────────────────
def fetch_json(url: str):
    req = urllib.request.Request(url, headers=NFFC_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"_http_error": e.code}
    except Exception as e:
        return {"_error": str(e)}


def sb_get_all(table: str, select: str, filter_str: str = ""):
    """Paginate a Supabase GET past the 1000-row default."""
    rows = []
    offset = 0
    while True:
        url = f"{SB_URL}/rest/v1/{table}?select={select}{filter_str}&limit={PAGE_SIZE}&offset={offset}"
        req = urllib.request.Request(url, headers={
            "apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}"
        })
        batch = json.loads(urllib.request.urlopen(req, timeout=60).read())
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def sb_delete_year(table: str):
    url = f"{SB_URL}/rest/v1/{table}?year=eq.{YEAR}"
    req = urllib.request.Request(
        url,
        headers={**SB_HEADERS, "Prefer": "return=representation,count=exact"},
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            cr = r.headers.get("Content-Range", "")
            return int(cr.split("/")[-1]) if "/" in cr else 0
    except urllib.error.HTTPError as e:
        body = (e.read() or b"").decode(errors="replace")[:300]
        raise SystemExit(f"Delete from {table} failed: {e.code} {body}")


def sb_insert_batch(table: str, rows: list[dict]):
    data = json.dumps(rows).encode()
    req = urllib.request.Request(f"{SB_URL}/rest/v1/{table}", data=data,
                                 headers=SB_HEADERS, method="POST")
    try:
        urllib.request.urlopen(req, timeout=60)
    except urllib.error.HTTPError as e:
        body = (e.read() or b"").decode(errors="replace")[:400]
        raise SystemExit(f"Insert to {table} failed: {e.code} {body}")


def sb_insert_all(table: str, rows: list[dict]):
    for i in range(0, len(rows), BATCH_SIZE):
        sb_insert_batch(table, rows[i:i + BATCH_SIZE])
        sys.stdout.write(f"\r      {min(i + BATCH_SIZE, len(rows))}/{len(rows)}")
        sys.stdout.flush()
    print(f"\r      {len(rows)}/{len(rows)} ✓")


# ─── NFFC fetches ───────────────────────────────────────────────────────────
def get_roc_leagues():
    data = fetch_json(f"{NFFC_BASE}/historicalleagues/football/{YEAR}?api_key={NFFC_API_KEY}")
    if not isinstance(data, list):
        raise SystemExit(f"Unexpected league-list response: {data}")
    return [l for l in data if is_rotowire_oc(l["name"])]


def fetch_detail(league_id: int):
    return fetch_json(f"{NFFC_BASE}/historicalleagues/football/{YEAR}/{league_id}?api_key={NFFC_API_KEY}")


def fetch_draft(league_id: int):
    return fetch_json(f"{NFFC_BASE}/historicaldraftresults/football/{YEAR}/{league_id}?api_key={NFFC_API_KEY}")


# ─── Build ──────────────────────────────────────────────────────────────────
def build_leagues_row(league_id: int, detail: dict, carry: Optional[dict]) -> dict:
    info = detail.get("league", {}) or {}
    return {
        "league_id": league_id,
        "year": YEAR,
        "name": info.get("name", ""),
        "num_teams": int(info.get("rosterSize", 20)) if info.get("rosterSize") else 20,
        "third_round_reversal": bool(info.get("3rr", 0)),
        "draft_date": carry.get("draft_date") if carry else None,
        "draft_completed_date": carry.get("draft_completed_date") if carry else None,
    }


def build_team_rows(league_id: int, detail: dict) -> list[dict]:
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


def build_pick_rows(league_id: int, draft_payload: dict, num_teams: int,
                    pick_carry: Optional[dict]) -> list[dict]:
    rows = []
    picks = draft_payload.get("draft_results", []) or []
    for p in picks:
        rd = int(p["round"])
        overall = int(p["pick"])
        pick_in_round = overall - (rd - 1) * num_teams
        carry = (pick_carry or {}).get(overall, {})
        rows.append({
            "league_id": league_id,
            "year": YEAR,
            "round": rd,
            "pick_in_round": pick_in_round,
            "overall_pick": overall,
            "team_id": int(p["team"]),
            "player_id": p["player"],
            "picked_at": carry.get("picked_at"),
            "pick_duration": carry.get("pick_duration"),
        })
    return rows


# ─── Main ───────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    t0 = time.time()

    print(f"[1/6] Fetching old Supabase {YEAR} data for merge bridge...")
    old_leagues = sb_get_all("leagues", "league_id,name,draft_date,draft_completed_date",
                              f"&year=eq.{YEAR}")
    print(f"      old leagues: {len(old_leagues)}")
    old_picks = sb_get_all("draft_picks",
                            "league_id,overall_pick,picked_at,pick_duration",
                            f"&year=eq.{YEAR}")
    print(f"      old draft_picks: {len(old_picks)}")

    # Build old_#N → league carry (primary bridge)
    old_league_by_n: dict = {}
    for r in old_leagues:
        n = extract_hash_n(r["name"])
        if n is None:
            continue
        old_league_by_n[n] = {
            "old_league_id": r["league_id"],
            "draft_date": r["draft_date"],
            "draft_completed_date": r["draft_completed_date"],
        }

    # Build Live-venue bridge (secondary, for the 7 Live leagues whose new names
    # were truncated by NFFC and have no #N suffix)
    old_live_by_key: list = []
    for r in old_leagues:
        key = live_match_key(r["name"])
        if key:
            old_live_by_key.append((key, r))

    # Build (old_league_id, overall_pick) → pick carry
    old_pick_by_lid_overall: dict[tuple[int, int], dict] = {}
    for p in old_picks:
        old_pick_by_lid_overall[(p["league_id"], p["overall_pick"])] = {
            "picked_at": p.get("picked_at"),
            "pick_duration": p.get("pick_duration"),
        }

    print(f"\n[2/6] Fetching {YEAR} ROC league list from NFFC...")
    leagues = get_roc_leagues()
    print(f"      {len(leagues)} leagues")

    print(f"\n[3/6] Fetching detail + draft for each league ({MAX_WORKERS} concurrent)...")
    details, drafts, errors = {}, {}, 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {}
        for lg in leagues:
            futures[ex.submit(fetch_detail, lg["id"])] = ("detail", lg["id"])
            futures[ex.submit(fetch_draft, lg["id"])] = ("draft", lg["id"])
        done = 0
        for fut in as_completed(futures):
            done += 1
            kind, lid = futures[fut]
            res = fut.result()
            if not isinstance(res, dict) or ("_error" in res) or ("_http_error" in res):
                errors += 1
                continue
            if kind == "detail" and "teams" in res:
                details[lid] = res
            elif kind == "draft" and "draft_results" in res:
                drafts[lid] = res
            if done % 100 == 0:
                sys.stdout.write(f"\r      {done}/{len(futures)} ({errors} errors)")
                sys.stdout.flush()
    print(f"\r      {done}/{len(futures)} done ({errors} errors) — details:{len(details)}, drafts:{len(drafts)}")

    print(f"\n[4/6] Building rows with merge-bridge...")
    leagues_rows, team_rows, pick_rows = [], [], []
    skipped = 0
    bridged_leagues = 0
    bridged_pick_total = 0
    pick_total = 0
    for lg in leagues:
        lid = lg["id"]
        detail = details.get(lid)
        draft = drafts.get(lid)
        if not detail or not draft:
            skipped += 1
            continue

        # Primary bridge via #N
        n = extract_hash_n(lg["name"])
        carry = old_league_by_n.get(n) if n is not None else None

        # Secondary bridge via Live-venue name prefix (for the 7 truncated Live leagues)
        via_live = False
        if not carry:
            new_key = live_match_key(lg["name"])
            if new_key:
                matches = [old for old_key, old in old_live_by_key
                           if old_key.startswith(new_key)]
                if len(matches) == 1:
                    r = matches[0]
                    carry = {
                        "old_league_id": r["league_id"],
                        "draft_date": r["draft_date"],
                        "draft_completed_date": r["draft_completed_date"],
                    }
                    via_live = True

        if carry:
            bridged_leagues += 1
            old_lid = carry["old_league_id"]
            pick_carry = {
                k[1]: v for k, v in old_pick_by_lid_overall.items()
                if k[0] == old_lid
            }
        else:
            pick_carry = None

        leagues_rows.append(build_leagues_row(lid, detail, carry))
        team_count = len(detail.get("teams", [])) or 12
        team_rows.extend(build_team_rows(lid, detail))
        new_picks = build_pick_rows(lid, draft, team_count, pick_carry)
        pick_rows.extend(new_picks)

        pick_total += len(new_picks)
        if pick_carry:
            bridged_pick_total += sum(1 for p in new_picks if p["picked_at"] is not None)

    print(f"      leagues: {len(leagues_rows)} (bridged={bridged_leagues}, fresh={len(leagues_rows)-bridged_leagues})")
    print(f"      league_teams: {len(team_rows)}")
    print(f"      draft_picks: {len(pick_rows)} (picked_at preserved on {bridged_pick_total}, fresh {pick_total - bridged_pick_total})")
    if skipped:
        print(f"      ⚠ skipped {skipped} leagues (missing detail or draft)")

    n_rank = sum(1 for r in team_rows if r["league_rank"] is not None)
    print(f"      outcomes populated: {n_rank}/{len(team_rows)} rows have league_rank")

    if args.dry_run:
        print(f"\n[5/6] DRY RUN — skipping deletes and writes")
        print(f"[6/6] Sample rows:")
        # Show one bridged and one fresh, if we have both
        bridged_sample = next((r for r in leagues_rows if r["draft_date"]), None)
        fresh_sample = next((r for r in leagues_rows if not r["draft_date"]), None)
        if bridged_sample:
            print("leagues (bridged):", json.dumps(bridged_sample, indent=2))
        if fresh_sample:
            print("leagues (fresh):", json.dumps(fresh_sample, indent=2))
        print("league_teams[0]:", json.dumps(team_rows[0], indent=2))
        pick_w_ts = next((p for p in pick_rows if p["picked_at"]), None)
        pick_w_no_ts = next((p for p in pick_rows if not p["picked_at"]), None)
        if pick_w_ts:
            print("draft_picks (with picked_at):", json.dumps(pick_w_ts, indent=2))
        if pick_w_no_ts:
            print("draft_picks (no picked_at):", json.dumps(pick_w_no_ts, indent=2))
        return

    print(f"\n[5/6] Deleting existing {YEAR} rows (FK order: draft_picks → league_teams → leagues)...")
    for table in ("draft_picks", "league_teams", "leagues"):
        n = sb_delete_year(table)
        print(f"      {table}: deleted {n} rows")

    print(f"\n[6/6] Inserting fresh {YEAR} rows...")
    print(f"      leagues ({len(leagues_rows)})...")
    sb_insert_all("leagues", leagues_rows)
    print(f"      league_teams ({len(team_rows)})...")
    sb_insert_all("league_teams", team_rows)
    print(f"      draft_picks ({len(pick_rows)})...")
    sb_insert_all("draft_picks", pick_rows)

    print(f"\nDone in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
