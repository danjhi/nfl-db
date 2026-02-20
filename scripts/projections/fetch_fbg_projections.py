"""Fetch FBG preseason projections and load into player_projections.

Fetches from https://www.footballguys.com/api/projections/preseason,
matches by footballguys_id (with name fallback via NFLPlayers.json),
calculates half-PPR points, and upserts into player_projections.

Usage:
    python3 scripts/projections/fetch_fbg_projections.py
"""

import json
import os
import sys
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

YEAR = 2026
SOURCE = "fbg"
FBG_PROJ_URL = f"https://www.footballguys.com/api/projections/preseason?year={YEAR}"
FBG_PLAYERS_URL = "https://appdata.footballguys.com/tn/NFLPlayers.json"

key = SUPABASE_SERVICE_KEY or SUPABASE_KEY


def fetch_fbg_projections():
    """Download FBG preseason projections."""
    url = f"{FBG_PROJ_URL}&apikey={FBG_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    return json.loads(urllib.request.urlopen(req).read().decode())


def fetch_fbg_player_names():
    """Download FBG NFLPlayers.json for name lookup."""
    req = urllib.request.Request(FBG_PLAYERS_URL, headers={"User-Agent": "Mozilla/5.0"})
    data = json.loads(urllib.request.urlopen(req).read().decode())
    return {p.get("id", ""): p for p in data}


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


def calc_half_ppr(stats):
    """Calculate half-PPR fantasy points from stat projections."""
    def val(key):
        v = stats.get(key)
        return v if v is not None else 0

    pts = 0.0
    # Passing: 0.04 per yard, 4 per TD, -2 per INT
    pts += val("pass_yds") * 0.04
    pts += val("pass_td") * 4
    pts += val("pass_int") * -2
    # Rushing: 0.1 per yard, 6 per TD
    pts += val("rush_yds") * 0.1
    pts += val("rush_td") * 6
    # Receiving: 0.1 per yard, 6 per TD, 0.5 per reception
    pts += val("rec_yds") * 0.1
    pts += val("rec_td") * 6
    pts += val("receptions") * 0.5
    # Fumbles lost: -2
    pts += val("fumbles_lost") * -2
    return round(pts, 1)


def build_projection_row(player_id, fbg_stats):
    """Convert FBG stat dict to our schema."""
    row = {
        "player_id": player_id,
        "source": SOURCE,
        "year": YEAR,
        "season_type": "regular",
        "games": fbg_stats.get("ssn-gms"),
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
        "targets": fbg_stats.get("rec-tgt"),
        "receptions": fbg_stats.get("rec-rec"),
        "rec_yds": fbg_stats.get("rec-yds"),
        "rec_td": fbg_stats.get("rec-td"),
        "rec_first_downs": fbg_stats.get("rec-1d"),
        "fumbles_lost": fbg_stats.get("fum-lost"),
    }

    # Calculate half-PPR points
    row["half_ppr_pts"] = calc_half_ppr(row)

    # Remove None values so all rows have identical keys (for batch POST)
    return {k: v for k, v in row.items() if v is not None}


def batch_upsert(rows, batch_size=50):
    """POST rows to player_projections in batches."""
    url = f"{SUPABASE_URL}/rest/v1/player_projections"
    inserted = 0
    errors = 0

    # Ensure all rows have identical keys (pad with None)
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


def main():
    if not FBG_KEY:
        print("ERROR: FBG_API_KEY not set in .env")
        sys.exit(1)

    # ── 1. Fetch data ────────────────────────────────────────────────────────
    print("Fetching FBG preseason projections...")
    fbg_proj = fetch_fbg_projections()
    print(f"  {len(fbg_proj)} players")

    print("Fetching FBG player names...")
    fbg_players = fetch_fbg_player_names()
    print(f"  {len(fbg_players)} in NFLPlayers.json")

    print("Fetching player lookups from Supabase...")
    fbg_to_pid = fetch_players_by_fbg_id()
    print(f"  {len(fbg_to_pid)} players with footballguys_id")

    by_name_pos, by_name = fetch_all_players_for_name_match()

    # FBG pos mapping
    pos_map = {"qb": "QB", "rb": "RB", "wr": "WR", "te": "TE", "pk": "K", "fb": "RB"}

    # ── 2. Match and build rows ──────────────────────────────────────────────
    proj_rows = []
    not_found = []

    for fbg_id, stats in fbg_proj.items():
        # Match by footballguys_id
        player_id = fbg_to_pid.get(fbg_id)

        # Fallback: name from NFLPlayers.json
        if not player_id and fbg_id in fbg_players:
            fp = fbg_players[fbg_id]
            name = f"{fp.get('first', '')} {fp.get('last', '')}".strip()
            norm = normalize_name(name)
            pos = pos_map.get(fp.get("pos", "").lower(), "")
            player_id = by_name_pos.get((norm, pos)) or by_name.get(norm)

        if not player_id:
            name = "unknown"
            if fbg_id in fbg_players:
                fp = fbg_players[fbg_id]
                name = f"{fp.get('first', '')} {fp.get('last', '')}".strip()
            not_found.append((fbg_id, name))
            continue

        proj_rows.append(build_projection_row(player_id, stats))

    print(f"\n  Matched: {len(proj_rows)}")
    print(f"  Not found: {len(not_found)}")

    # ── 3. Upsert ────────────────────────────────────────────────────────────
    if proj_rows:
        print(f"\nUpserting {len(proj_rows)} projections...")
        inserted, errors = batch_upsert(proj_rows)
        print(f"  Inserted/updated: {inserted}")
        if errors:
            print(f"  Errors: {errors}")

    # ── 4. Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    print(f"FBG projections:     {len(fbg_proj)}")
    print(f"Matched & loaded:    {len(proj_rows)}")
    print(f"Not found in DB:     {len(not_found)}")

    if not_found:
        print(f"\nUnmatched (all {len(not_found)}):")
        for fid, name in not_found:
            print(f"  {fid:<15} {name}")


if __name__ == "__main__":
    main()
