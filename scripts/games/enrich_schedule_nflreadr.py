"""Enrich the `games` table with nflreadr metadata.

Reads `data/nflreadr/schedule_{year}.csv` (produced by export_schedule_nflreadr.R)
and PATCHes each existing `games` row with: kickoff (UTC), stadium, roof, surface,
is_international, is_primetime, plus home_score/away_score (NULL until played).

Match key: (season, week, home_team, away_team) — game_id is unreliable because
nflreadr uses 'LAR' inside game_id but 'LA' in team columns for the Rams.

Primetime heuristic: ET kickoff hour ∈ {19, 20, 21}.
International heuristic: location == 'Neutral' (covers London/Madrid/Brazil/etc.).

Usage:
    python3 scripts/games/enrich_schedule_nflreadr.py [--year 2026] [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import sys
import urllib.error
import urllib.request
from zoneinfo import ZoneInfo

_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_KEY  # noqa: E402

ET = ZoneInfo("America/New_York")
UTC = datetime.timezone.utc


def parse_kickoff(kickoff_et_text: str, game_date: str) -> str | None:
    """Convert 'YYYY-MM-DD HH:MM' (ET, naive) to UTC ISO string."""
    if not kickoff_et_text:
        return None
    try:
        dt_naive = datetime.datetime.strptime(kickoff_et_text, "%Y-%m-%d %H:%M")
    except ValueError:
        return None
    dt_et = dt_naive.replace(tzinfo=ET)
    return dt_et.astimezone(UTC).isoformat()


def fetch_games(season: int) -> list[dict]:
    """Pull existing games for the season (paginate to be safe)."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    rows: list[dict] = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/games"
            f"?select=game_id,season,week,home_team,away_team"
            f"&season=eq.{season}"
            f"&offset={offset}&limit=1000"
        )
        req = urllib.request.Request(url, headers={"apikey": key, "Authorization": f"Bearer {key}"})
        with urllib.request.urlopen(req) as r:
            batch = json.loads(r.read().decode("utf-8"))
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def patch_game(game_id: str, updates: dict) -> None:
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/games?game_id=eq.{urllib.request.quote(game_id, safe='')}"
    data = json.dumps(updates).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        method="PATCH",
    )
    urllib.request.urlopen(req)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    csv_path = os.path.join("data", "nflreadr", f"schedule_{args.year}.csv")
    if not os.path.exists(csv_path):
        print(f"Missing CSV: {csv_path}")
        print(f"Run: Rscript scripts/games/export_schedule_nflreadr.R {args.year}")
        sys.exit(1)

    # R's write_csv emits cp1252 for non-ASCII (e.g. Levi's® Stadium)
    with open(csv_path, newline="", encoding="cp1252") as f:
        rows = list(csv.DictReader(f))
    print(f"Read {len(rows)} nflreadr rows from {csv_path}")

    db_games = fetch_games(args.year)
    by_key = {(int(g["season"]), int(g["week"]), g["home_team"], g["away_team"]): g["game_id"] for g in db_games}
    print(f"Loaded {len(db_games)} existing games for {args.year}")

    matched = unmatched = patched = 0
    unmatched_examples: list[str] = []

    for r in rows:
        try:
            key = (int(r["season"]), int(r["week"]), r["home_team"], r["away_team"])
        except (ValueError, KeyError):
            continue
        game_id = by_key.get(key)
        if not game_id:
            unmatched += 1
            if len(unmatched_examples) < 10:
                unmatched_examples.append(f"  wk{r['week']} {r['away_team']} @ {r['home_team']}")
            continue
        matched += 1

        updates: dict = {}
        kickoff = parse_kickoff(r.get("kickoff_et_text", ""), r.get("game_date", ""))
        if kickoff:
            updates["kickoff"] = kickoff
            hour_et = int(r["kickoff_et_text"].split(" ")[1].split(":")[0])
            updates["is_primetime"] = hour_et in (19, 20, 21)
        for col_csv, col_db in [
            ("stadium", "stadium"),
            ("roof", "roof"),
            ("surface", "surface"),
        ]:
            v = r.get(col_csv, "").strip()
            if v:
                updates[col_db] = v
        loc = r.get("location", "").strip()
        if loc:
            updates["is_international"] = (loc == "Neutral")
            if loc == "Neutral":
                stadium = r.get("stadium", "").strip()
                if stadium:
                    updates["location_override"] = stadium
        # Scores: only set if present (avoid clobbering NULLs into 0)
        for col_csv, col_db in [("home_score", "home_score"), ("away_score", "away_score")]:
            v = r.get(col_csv, "").strip()
            if v:
                try:
                    updates[col_db] = int(v)
                except ValueError:
                    pass

        if not updates:
            continue

        if args.dry_run:
            if patched < 3:
                print(f"  [dry] {game_id}: {updates}")
        else:
            try:
                patch_game(game_id, updates)
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                print(f"  ERROR {game_id}: {e.code} {body}")
                continue
        patched += 1

    print(f"\nMatched:   {matched}")
    print(f"Patched:   {patched}")
    print(f"Unmatched: {unmatched}")
    if unmatched_examples:
        print("Unmatched examples:")
        for e in unmatched_examples:
            print(e)


if __name__ == "__main__":
    main()
