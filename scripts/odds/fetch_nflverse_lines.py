"""Fetch NFL game lines from nflverse's games.csv and upsert into
`game_odds_snapshots` (bookmaker="nflverse").

Source: https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv
— the exact file behind nflreadr::load_schedules(). No auth, no API key, no
Playwright; pure stdlib. Lines are nflverse's single consensus line (spread,
total, moneylines + odds), updated by their automation through the season.

Replaces the Odds API snapshot (`fetch_odds_snapshot.py`, dormant since the
key was deactivated) as the daily scheduled odds source. For a true live
multi-market board, run `fetch_draftkings_lines.py` manually (headful).

Conventions:
  - games.csv `spread_line` is POSITIVE when the home team is favored
    (nflreadr convention). Our `home_spread` uses standard betting
    convention (negative = home favored), so we flip the sign.
  - Completed games (non-empty `result`) are skipped by default — snapshots
    are pre-game market states. `--include-completed` overrides.
  - Match to `games.game_id` by (season, week, home, away) through
    `normalize_team()` — never by games.csv's own game_id (it uses LAR
    while team columns use LA; see CLAUDE.md gotchas).

Usage:
    python3 scripts/odds/fetch_nflverse_lines.py [--season 2026] [--dry-run]
        [--include-completed]
"""
from __future__ import annotations

import argparse
import csv
import datetime
import io
import json
import logging
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

GAMES_CSV_URL = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"

LOG_DIR = os.path.join(os.path.dirname(_script_dir), "..", "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, "nflverse_lines.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
)
log = logging.getLogger("nflverse_lines")


def _num(v: str | None) -> float | None:
    if v is None or v == "" or v == "NA":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _int(v: str | None) -> int | None:
    f = _num(v)
    return int(f) if f is not None else None


def fetch_games_csv() -> list[dict]:
    req = urllib.request.Request(GAMES_CSV_URL, headers={"User-Agent": "nfl-db/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        text = r.read().decode("utf-8", errors="replace")
    return list(csv.DictReader(io.StringIO(text)))


def fetch_db_games(season: int) -> dict[tuple, str]:
    """(season, week, home, away) → game_id for our games table."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    rows: list[dict] = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/games"
            f"?select=game_id,season,week,home_team,away_team"
            f"&season=eq.{season}&offset={offset}&limit=1000"
        )
        req = urllib.request.Request(url, headers={"apikey": key, "Authorization": f"Bearer {key}"})
        with urllib.request.urlopen(req, timeout=30) as r:
            batch = json.loads(r.read().decode("utf-8"))
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return {
        (int(g["season"]), int(g["week"]), g["home_team"], g["away_team"]): g["game_id"]
        for g in rows
    }


def build_row(r: dict, game_id: str, today: str) -> dict | None:
    spread_line = _num(r.get("spread_line"))
    total = _num(r.get("total_line"))
    if spread_line is None and total is None:
        return None

    # nflreadr: positive = home favored → standard: negative = home favored
    home_spread = -spread_line if spread_line is not None else None

    implied_home = implied_away = None
    if home_spread is not None and total is not None:
        implied_home = round(total / 2.0 - home_spread / 2.0, 3)
        implied_away = round(total / 2.0 + home_spread / 2.0, 3)

    return {
        "game_id": game_id,
        "bookmaker": "nflverse",
        "date": today,
        "home_spread": home_spread,
        "home_spread_price": _int(r.get("home_spread_odds")),
        "away_spread_price": _int(r.get("away_spread_odds")),
        "total": total,
        "over_price": _int(r.get("over_odds")),
        "under_price": _int(r.get("under_odds")),
        "home_moneyline": _int(r.get("home_moneyline")),
        "away_moneyline": _int(r.get("away_moneyline")),
        "home_implied_total": implied_home,
        "away_implied_total": implied_away,
    }


def batch_upsert(rows: list[dict], batch_size: int = 500) -> tuple[int, int]:
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/game_odds_snapshots"
    inserted = errors = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        req = urllib.request.Request(
            url,
            data=json.dumps(batch).encode("utf-8"),
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
            log.error(f"batch at row {i}: {e.code} {body[:200]}")
            errors += len(batch)
    return inserted, errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--include-completed", action="store_true")
    args = parser.parse_args()

    today = datetime.date.today().isoformat()
    log.info(f"=== nflverse lines snapshot {today} (season={args.season}) ===")

    csv_rows = fetch_games_csv()
    season_rows = [r for r in csv_rows if _int(r.get("season")) == args.season]
    log.info(f"games.csv: {len(csv_rows)} total rows, {len(season_rows)} for {args.season}")
    if not season_rows:
        log.error("ERROR: no rows for season — games.csv format may have changed")
        sys.exit(1)

    db_lookup = fetch_db_games(args.season)
    log.info(f"DB games loaded: {len(db_lookup)}")

    out: list[dict] = []
    completed = unmatched = no_lines = 0
    unmatched_examples: list[str] = []
    for r in season_rows:
        if not args.include_completed and (r.get("result") or "").strip() != "":
            completed += 1
            continue
        try:
            k = (
                int(r["season"]),
                int(r["week"]),
                normalize_team(r["home_team"]),
                normalize_team(r["away_team"]),
            )
        except (KeyError, ValueError):
            continue
        game_id = db_lookup.get(k)
        if not game_id:
            unmatched += 1
            if len(unmatched_examples) < 8:
                unmatched_examples.append(f"  wk{r.get('week')} {r.get('away_team')} @ {r.get('home_team')}")
            continue
        row = build_row(r, game_id, today)
        if row is None:
            no_lines += 1
            continue
        out.append(row)

    log.info(
        f"Rows ready: {len(out)} (skipped: {completed} completed, "
        f"{no_lines} without lines, {unmatched} unmatched)"
    )
    if unmatched_examples:
        log.info("Unmatched examples:\n" + "\n".join(unmatched_examples))

    if args.dry_run:
        for row in out[:5]:
            log.info(f"  DRY {row['game_id']}: spread {row['home_spread']}, total {row['total']}")
        log.info("Dry run — nothing written.")
        return

    if not out:
        log.error("ERROR: zero rows to upsert")
        sys.exit(1)

    inserted, errors = batch_upsert(out)
    log.info(f"Upserted: {inserted}, errors: {errors}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
