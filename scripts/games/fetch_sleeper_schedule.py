"""Fetch the NFL schedule from Sleeper and upsert into the `games` table.

Sleeper exposes the schedule at:
    GET https://api.sleeper.app/schedule/nfl/regular/{year}
    GET https://api.sleeper.app/schedule/nfl/post/{year}

Each game has: status, date, home, week, game_id (Sleeper's), away.
Sleeper uses LAR for the Rams — normalize to LA via shared.normalize_team().
Games with status='canceled' are skipped (e.g. moved-then-relisted matchups).

The game_id we store is in nflreadr format: "{season}_{week:02d}_{AWAY}_{HOME}",
matching team_game_stats.game_id for future joins.

Usage:
    python3 scripts/games/fetch_sleeper_schedule.py [--year 2026] [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import urllib.error
import urllib.request

_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import (  # noqa: E402
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    normalize_team,
)

SLEEPER_BASE = "https://api.sleeper.app/schedule/nfl"


def fetch_sleeper(season_type: str, year: int) -> list[dict]:
    url = f"{SLEEPER_BASE}/{season_type}/{year}"
    req = urllib.request.Request(url, headers={"User-Agent": "nfl-db/1.0"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def build_row(g: dict, season: int, season_type_short: str) -> dict | None:
    """Convert a Sleeper game dict to a `games` row, or None if it should be skipped."""
    if g.get("status") == "canceled":
        return None
    week = g.get("week")
    home = normalize_team(g.get("home", ""))
    away = normalize_team(g.get("away", ""))
    date_str = g.get("date")
    if not (week and home and away and date_str):
        return None
    game_id = f"{season}_{week:02d}_{away}_{home}"
    return {
        "game_id": game_id,
        "season": season,
        "week": int(week),
        "season_type": season_type_short,
        "game_date": date_str,
        "home_team": home,
        "away_team": away,
        "sleeper_game_id": g.get("game_id"),
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


def batch_upsert(rows: list[dict], batch_size: int = 200) -> tuple[int, int]:
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/games"
    inserted = errors = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        data = json.dumps(batch).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal,resolution=merge-duplicates",
            },
            method="POST",
        )
        try:
            urllib.request.urlopen(req)
            inserted += len(batch)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"  ERROR batch at row {i}: {e.code} {body}")
            errors += len(batch)
    return inserted, errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    year = args.year
    all_rows: list[dict] = []
    skipped_canceled = 0

    for season_type, short in [("regular", "reg"), ("post", "post")]:
        print(f"Fetching Sleeper schedule: {season_type} {year} ...")
        try:
            games = fetch_sleeper(season_type, year)
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code} — skipping")
            continue
        print(f"  {len(games)} raw games")
        for g in games:
            if g.get("status") == "canceled":
                skipped_canceled += 1
                continue
            row = build_row(g, year, short)
            if row is not None:
                all_rows.append(row)

    # Dedupe by game_id (defensive — Sleeper has been seen to repeat moved games)
    by_id: dict[str, dict] = {}
    for r in all_rows:
        by_id[r["game_id"]] = r
    rows = list(by_id.values())

    print(f"\nRows to upsert: {len(rows)} (skipped canceled: {skipped_canceled})")
    if args.dry_run:
        print("[dry-run] Sample rows:")
        for r in rows[:5]:
            print(f"  {r}")
        return

    inserted, errors = batch_upsert(rows)
    print(f"\nInserted/updated: {inserted}")
    if errors:
        print(f"Errors: {errors}")
        sys.exit(1)


if __name__ == "__main__":
    main()
