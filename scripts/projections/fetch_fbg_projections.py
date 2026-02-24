"""Fetch FBG projections (preseason or weekly) and load into player_projections.

Matches by footballguys_id (with name fallback via NFLPlayers.json),
calculates half-PPR points, and upserts into player_projections.

Usage:
    # Single preseason year
    python3 scripts/projections/fetch_fbg_projections.py --year 2025

    # Single weekly
    python3 scripts/projections/fetch_fbg_projections.py --year 2024 --week 4

    # All available preseason (2023-2026)
    python3 scripts/projections/fetch_fbg_projections.py --all-preseason

    # All available weekly (2023-2025, weeks 1-18)
    python3 scripts/projections/fetch_fbg_projections.py --all-weekly

    # Everything
    python3 scripts/projections/fetch_fbg_projections.py --all
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ids"))
from shared import (
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    FBG_KEY,
    normalize_name,
    PLAYER_ALIASES,
)

SOURCE = "fbg"
FBG_BASE = "https://www.footballguys.com/api/projections"
FBG_PLAYERS_URL = "https://appdata.footballguys.com/tn/NFLPlayers.json"

# Years available in the FBG API (tested Feb 2026)
PRESEASON_YEARS = [2023, 2024, 2025, 2026]
WEEKLY_YEARS = [2023, 2024, 2025]
MAX_WEEK = 18

key = SUPABASE_SERVICE_KEY or SUPABASE_KEY

# FBG pos mapping
POS_MAP = {"qb": "QB", "rb": "RB", "wr": "WR", "te": "TE", "pk": "K", "fb": "RB"}


# ── API fetchers ──────────────────────────────────────────────────────────────

def fetch_fbg_data(year, week=None):
    """Download FBG projections. week=None for preseason, 1-18 for weekly."""
    if week is None:
        url = f"{FBG_BASE}/preseason?year={year}&apikey={FBG_KEY}"
    else:
        url = f"{FBG_BASE}/weekly?year={year}&week={week}&apikey={FBG_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        resp = urllib.request.urlopen(req)
        return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 400:
            return None  # "No projections found"
        raise


def fetch_fbg_player_names():
    """Download FBG NFLPlayers.json for name lookup."""
    req = urllib.request.Request(FBG_PLAYERS_URL, headers={"User-Agent": "Mozilla/5.0"})
    data = json.loads(urllib.request.urlopen(req).read().decode())
    return {p.get("id", ""): p for p in data}


# ── Supabase lookups ──────────────────────────────────────────────────────────

def fetch_players_by_fbg_id():
    """Fetch players that have footballguys_id set."""
    players = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,footballguys_id"
            f"&footballguys_id=not.is.null"
            f"&offset={offset}&limit=1000"
        )
        req = urllib.request.Request(url, headers={
            "apikey": key, "Authorization": f"Bearer {key}",
        })
        batch = json.loads(urllib.request.urlopen(req).read().decode())
        if not batch:
            break
        players.extend(batch)
        offset += 1000
    return {p["footballguys_id"]: p["player_id"] for p in players}


def fetch_all_players_for_name_match():
    """Fetch all players for name-based fallback."""
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
        batch = json.loads(urllib.request.urlopen(req).read().decode())
        if not batch:
            break
        players.extend(batch)
        offset += 1000

    by_name_pos = {}
    by_name = {}
    for p in players:
        full = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
        norm = normalize_name(full)
        pos = (p.get("position") or "").upper()
        by_name_pos[(norm, pos)] = p["player_id"]
        by_name[norm] = p["player_id"]
        alias = PLAYER_ALIASES.get(norm)
        if alias:
            by_name_pos[(alias, pos)] = p["player_id"]
            by_name[alias] = p["player_id"]
    return by_name_pos, by_name


# ── Row building ──────────────────────────────────────────────────────────────

def calc_half_ppr(stats):
    """Calculate half-PPR fantasy points from stat projections."""
    def val(k):
        v = stats.get(k)
        return v if v is not None else 0

    pts = 0.0
    pts += val("pass_yds") * 0.04
    pts += val("pass_td") * 4
    pts += val("pass_int") * -2
    pts += val("rush_yds") * 0.1
    pts += val("rush_td") * 6
    pts += val("rec_yds") * 0.1
    pts += val("rec_td") * 6
    pts += val("receptions") * 0.5
    pts += val("fumbles_lost") * -2
    return round(pts, 1)


def build_projection_row(player_id, fbg_stats, year, week=0):
    """Convert FBG stat dict to our schema.

    For weekly projections (week > 0), skip rec-tgt because the weekly API
    has the targets/receptions fields swapped relative to preseason.
    """
    is_weekly = week > 0
    row = {
        "player_id": player_id,
        "source": SOURCE,
        "year": year,
        "season_type": "regular",
        "week": week,
        "pass_att": fbg_stats.get("pass-att"),
        "pass_cmp": fbg_stats.get("pass-cmp"),
        "pass_yds": fbg_stats.get("pass-yds"),
        "pass_td": fbg_stats.get("pass-td"),
        "pass_int": fbg_stats.get("pass-int"),
        "pass_sck": fbg_stats.get("pass-sck"),
        "pass_first_downs": fbg_stats.get("pass-1d"),
        "rush_att": fbg_stats.get("rush-car"),
        "rush_yds": fbg_stats.get("rush-yds"),
        "rush_td": fbg_stats.get("rush-td"),
        "rush_first_downs": fbg_stats.get("rush-1d"),
        "receptions": fbg_stats.get("rec-rec"),
        "rec_yds": fbg_stats.get("rec-yds"),
        "rec_td": fbg_stats.get("rec-td"),
        "rec_first_downs": fbg_stats.get("rec-1d"),
        "fumbles_lost": fbg_stats.get("fum-lost"),
    }

    # Only include targets and games for preseason (reliable there)
    if not is_weekly:
        row["targets"] = fbg_stats.get("rec-tgt")
        row["games"] = fbg_stats.get("ssn-gms")

    row["half_ppr_pts"] = calc_half_ppr(row)

    # Remove None values so all rows have identical keys (for batch POST)
    return {k: v for k, v in row.items() if v is not None}


# ── Upsert ────────────────────────────────────────────────────────────────────

def batch_upsert(rows, batch_size=50):
    """POST rows to player_projections in batches."""
    url = f"{SUPABASE_URL}/rest/v1/player_projections"
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

    return inserted, errors


# ── Matching + loading ────────────────────────────────────────────────────────

def match_and_load(fbg_proj, year, week, fbg_to_pid, fbg_players, by_name_pos, by_name):
    """Match FBG projections to player_ids and upsert."""
    wk_label = f"week {week}" if week else "preseason"
    proj_rows = []
    not_found = []

    for fbg_id, stats in fbg_proj.items():
        player_id = fbg_to_pid.get(fbg_id)

        if not player_id and fbg_id in fbg_players:
            fp = fbg_players[fbg_id]
            name = f"{fp.get('first', '')} {fp.get('last', '')}".strip()
            norm = normalize_name(name)
            pos = POS_MAP.get(fp.get("pos", "").lower(), "")
            player_id = by_name_pos.get((norm, pos)) or by_name.get(norm)

        if not player_id:
            name = "unknown"
            if fbg_id in fbg_players:
                fp = fbg_players[fbg_id]
                name = f"{fp.get('first', '')} {fp.get('last', '')}".strip()
            not_found.append((fbg_id, name))
            continue

        proj_rows.append(build_projection_row(player_id, stats, year, week or 0))

    # Deduplicate by player_id (multiple FBG IDs can map to same player)
    seen = {}
    for row in proj_rows:
        seen[row["player_id"]] = row
    proj_rows = list(seen.values())

    if proj_rows:
        inserted, errors = batch_upsert(proj_rows)
        print(f"  {year} {wk_label}: {inserted} upserted, {len(not_found)} unmatched", end="")
        if errors:
            print(f", {errors} errors", end="")
        print()
    else:
        print(f"  {year} {wk_label}: no matched rows ({len(not_found)} unmatched)")

    return len(proj_rows), len(not_found)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fetch FBG projections")
    parser.add_argument("--year", type=int, help="Season year")
    parser.add_argument("--week", type=int, help="Week number (1-18). Omit for preseason.")
    parser.add_argument("--all-preseason", action="store_true",
                        help=f"Fetch preseason for all available years ({PRESEASON_YEARS})")
    parser.add_argument("--all-weekly", action="store_true",
                        help=f"Fetch weekly for all available years ({WEEKLY_YEARS}), weeks 1-{MAX_WEEK}")
    parser.add_argument("--all", action="store_true", help="Fetch everything (preseason + weekly)")
    args = parser.parse_args()

    if not FBG_KEY:
        print("ERROR: FBG_API_KEY not set in .env")
        sys.exit(1)

    # Determine what to fetch
    jobs = []  # list of (year, week_or_None)

    if args.all:
        for y in PRESEASON_YEARS:
            jobs.append((y, None))
        for y in WEEKLY_YEARS:
            for w in range(1, MAX_WEEK + 1):
                jobs.append((y, w))
    elif args.all_preseason:
        for y in PRESEASON_YEARS:
            jobs.append((y, None))
    elif args.all_weekly:
        for y in WEEKLY_YEARS:
            for w in range(1, MAX_WEEK + 1):
                jobs.append((y, w))
    elif args.year:
        jobs.append((args.year, args.week))
    else:
        parser.print_help()
        sys.exit(1)

    # ── 1. Fetch lookup data (once) ───────────────────────────────────────────
    print("Fetching FBG player names...")
    fbg_players = fetch_fbg_player_names()
    print(f"  {len(fbg_players)} in NFLPlayers.json")

    print("Fetching player lookups from Supabase...")
    fbg_to_pid = fetch_players_by_fbg_id()
    print(f"  {len(fbg_to_pid)} players with footballguys_id")
    by_name_pos, by_name = fetch_all_players_for_name_match()
    print()

    # ── 2. Process each job ───────────────────────────────────────────────────
    total_loaded = 0
    total_unmatched = 0

    for i, (year, week) in enumerate(jobs):
        wk_label = f"week {week}" if week else "preseason"

        fbg_proj = fetch_fbg_data(year, week)
        if fbg_proj is None:
            print(f"  {year} {wk_label}: no data available")
            continue

        loaded, unmatched = match_and_load(
            fbg_proj, year, week,
            fbg_to_pid, fbg_players, by_name_pos, by_name,
        )
        total_loaded += loaded
        total_unmatched += unmatched

        # Small delay between API calls to be respectful
        if i < len(jobs) - 1:
            time.sleep(0.5)

    # ── 3. Summary ────────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    print(f"Jobs processed:      {len(jobs)}")
    print(f"Total loaded:        {total_loaded}")
    print(f"Total unmatched:     {total_unmatched}")


if __name__ == "__main__":
    main()
