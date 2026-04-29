#!/usr/bin/env python3
"""End-to-end verification of the 2026 NFFC scraper pipeline using LIVE data.

Runs fetch → filter → transform against today's Footballguys Online Championship
(FOC, formerly Rotowire OC) leagues. Does NOT write to Supabase or disk.

Purpose: validate that the existing scraper code still works end-to-end after
the 2026 rebrand, before packaging the scripts for handoff to FBG.

Usage: python3 test_2026_pipeline.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(HERE, "..", "..", ".env")

with open(ENV_PATH) as f:
    _env = {k.strip(): v.strip() for k, v in
            (line.strip().split("=", 1) for line in f
             if "=" in line and not line.strip().startswith("#"))}

API_KEY = _env["NFFC_API_KEY"]
BASE = "https://nfc.shgn.com/api/public"
YEAR = 2026
HEADERS = {"User-Agent": "nfl-db/test-2026-pipeline"}


def is_main_oc(name: str) -> bool:
    """Match the main NFFC OC contest across the 2026 rebrand.

    - 2018–2025: 'Rotowire Online Championship'
    - 2026+:     'Footballguys Online Championship'
    """
    n = (name or "").lower()
    return "online championship" in n and ("rotowire" in n or "footballguys" in n)


def fetch_json(url: str):
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"_http_error": e.code}
    except Exception as e:
        return {"_error": str(e)}


def main():
    t0 = time.time()

    # --- 1. List leagues (current season uses /publicleagues) ---
    print(f"[1/4] Fetching current-season league list...")
    data = fetch_json(f"{BASE}/publicleagues/football?api_key={API_KEY}")
    if not isinstance(data, list):
        raise SystemExit(f"Unexpected response: {data}")
    all_leagues = data
    print(f"      {len(all_leagues)} leagues visible in the current season")

    # --- 2. Filter via the updated (brand-agnostic) matcher ---
    foc = [l for l in all_leagues if is_main_oc(l.get("name", ""))]
    print(f"[2/4] Main-OC filter (Rotowire OR Footballguys 'Online Championship'):")
    print(f"      {len(foc)} leagues match")
    for l in foc:
        print(f"      - id={l['id']}: {l['name']}")

    if not foc:
        print("\nNo FOC leagues live yet — nothing to test end-to-end.")
        return

    # --- 3. Fetch detail + draft for each matched league ---
    print(f"[3/4] Fetching detail + draft for each...")
    details, drafts = {}, {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {}
        for lg in foc:
            futures[ex.submit(fetch_json,
                              f"{BASE}/publicleagues/football/{lg['id']}?api_key={API_KEY}")] = ("detail", lg["id"])
            futures[ex.submit(fetch_json,
                              f"{BASE}/publicdraftresults/football/{lg['id']}?api_key={API_KEY}")] = ("draft", lg["id"])
        for fut in as_completed(futures):
            kind, lid = futures[fut]
            res = fut.result()
            if not isinstance(res, dict):
                continue
            if kind == "detail" and "teams" in res:
                details[lid] = res
            elif kind == "draft" and "draft_results" in res:
                drafts[lid] = res
    print(f"      got {len(details)} details, {len(drafts)} draft results")

    # --- 4. Transform into the same shape load_to_supabase expects ---
    print(f"[4/4] Transforming to scraper output rows...")
    leagues_rows, team_rows, pick_rows = [], [], []
    for lg in foc:
        lid = lg["id"]
        detail = details.get(lid)
        draft = drafts.get(lid)
        if not (detail and draft):
            continue
        info = detail.get("league", {}) or {}
        leagues_rows.append({
            "league_id": lid,
            "year": YEAR,
            "name": info.get("name", ""),
            "num_teams": int(info.get("rosterSize", 20)),
            "third_round_reversal": bool(info.get("3rr", 0)),
            "draft_date": info.get("draft_date") or None,
            "draft_completed_date": info.get("draft_completed_date") or None,
        })
        team_count = len(detail.get("teams", [])) or 12
        for team in detail.get("teams", []) or []:
            team_rows.append({
                "league_id": lid,
                "team_id": int(team["id"]),
                "year": YEAR,
                "draft_order": team.get("draft_order"),
                "league_rank": team.get("league_rank"),
                "league_points": float(team["league_points"]) if team.get("league_points") not in (None, "") else None,
                "overall_rank": team.get("overall_rank"),
                "overall_points": float(team["overall_points"]) if team.get("overall_points") not in (None, "") else None,
            })
        picks = draft.get("draft_results", []) or []
        for p in picks:
            rd = int(p["round"])
            overall = int(p["pick"])
            pick_rows.append({
                "league_id": lid,
                "year": YEAR,
                "round": rd,
                "pick_in_round": overall - (rd - 1) * team_count,
                "overall_pick": overall,
                "team_id": int(p["team"]),
                "player_id": p["player"],
                "picked_at": p.get("timestamp") or None,
                "pick_duration": int(p["pick_duration"]) if p.get("pick_duration") not in (None, "") else None,
            })

    print(f"\n=== Transform summary ===")
    print(f"  leagues:       {len(leagues_rows)}")
    print(f"  league_teams:  {len(team_rows)}")
    print(f"  draft_picks:   {len(pick_rows)}")

    # Field population stats
    if team_rows:
        n_rank = sum(1 for r in team_rows if r["league_rank"] is not None)
        print(f"  team rows with league_rank populated: {n_rank}/{len(team_rows)}  (expected 0 this early — drafts just completed)")
    if pick_rows:
        n_ts = sum(1 for p in pick_rows if p["picked_at"])
        n_dur = sum(1 for p in pick_rows if p["pick_duration"] is not None)
        print(f"  picks with picked_at:     {n_ts}/{len(pick_rows)}")
        print(f"  picks with pick_duration: {n_dur}/{len(pick_rows)}")

    print(f"\n--- Sample rows ---")
    if leagues_rows:
        print("leagues[0]:", json.dumps(leagues_rows[0], indent=2))
    if team_rows:
        print("league_teams[0]:", json.dumps(team_rows[0], indent=2))
    if pick_rows:
        print("draft_picks[0]:", json.dumps(pick_rows[0], indent=2))

    print(f"\nDone in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
