"""Fetch a live NFL odds snapshot from The Odds API and upsert into
`game_odds_snapshots`.

Endpoint:
    GET https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds
        ?regions=us&markets=h2h,spreads,totals&oddsFormat=american

Cost: ~3 credits per call (1 base × 3 markets × 1 region). Daily snapshot
= ~90 credits/month against a 20K/month tier (≈0.45%).

Snapshots are keyed by (game_id, bookmaker, date). Re-running on the same
day upserts in-place. The Odds API has its own opaque event IDs; we match
to our `games.game_id` by (game_date_et, home_team_abbr, away_team_abbr).

Usage:
    python3 scripts/odds/fetch_odds_snapshot.py [--season 2026] [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import sys
import urllib.error
import urllib.request
from zoneinfo import ZoneInfo

_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import (  # noqa: E402
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    TEAM_FULLNAME_TO_ABBR,
)

ET = ZoneInfo("America/New_York")
UTC = datetime.timezone.utc

ODDS_BASE = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds"
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

LOG_DIR = os.path.join(os.path.dirname(_script_dir), "..", "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, "odds_snapshot.log")
JSONL_PATH = os.path.join(LOG_DIR, "odds_snapshot.jsonl")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
)
log = logging.getLogger("odds")


def name_to_abbr(name: str) -> str | None:
    if not name:
        return None
    return TEAM_FULLNAME_TO_ABBR.get(name.lower().strip())


def fetch_odds() -> tuple[list[dict], dict]:
    if not ODDS_API_KEY:
        raise RuntimeError("ODDS_API_KEY not set in env")
    url = (
        f"{ODDS_BASE}?regions=us&markets=h2h,spreads,totals"
        f"&oddsFormat=american&apiKey={ODDS_API_KEY}"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "nfl-db/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        body = json.loads(r.read().decode("utf-8"))
        headers = {
            "x-requests-used": r.headers.get("x-requests-used"),
            "x-requests-remaining": r.headers.get("x-requests-remaining"),
            "x-requests-last": r.headers.get("x-requests-last"),
        }
    return body, headers


def fetch_games(season: int) -> list[dict]:
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    rows: list[dict] = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/games"
            f"?select=game_id,game_date,kickoff,home_team,away_team"
            f"&season=eq.{season}&offset={offset}&limit=1000"
        )
        req = urllib.request.Request(url, headers={"apikey": key, "Authorization": f"Bearer {key}"})
        with urllib.request.urlopen(req) as r:
            batch = json.loads(r.read().decode("utf-8"))
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def build_game_lookup(games: list[dict]) -> dict[tuple, str]:
    """Map (date_et, home_abbr, away_abbr) → game_id.

    Indexes under both game_date (Sleeper's date) and the date derived from
    kickoff (in case ET kickoff lands on a different date than `game_date`).
    """
    lookup: dict[tuple, str] = {}
    for g in games:
        gid = g["game_id"]
        home, away = g["home_team"], g["away_team"]
        gd = g.get("game_date")
        if gd:
            lookup[(gd, home, away)] = gid
        ko = g.get("kickoff")
        if ko:
            try:
                dt_utc = datetime.datetime.fromisoformat(ko.replace("Z", "+00:00"))
                date_et = dt_utc.astimezone(ET).date().isoformat()
                lookup[(date_et, home, away)] = gid
            except ValueError:
                pass
    return lookup


def _outcome(market: dict | None, name: str) -> dict | None:
    if not market:
        return None
    for o in market.get("outcomes", []):
        if o.get("name") == name:
            return o
    return None


def flatten_event(event: dict, game_id: str, today: str) -> list[dict]:
    """Convert one Odds API event → one row per bookmaker."""
    home_full = event.get("home_team", "")
    away_full = event.get("away_team", "")
    rows: list[dict] = []
    for bm in event.get("bookmakers", []):
        markets = {m["key"]: m for m in bm.get("markets", [])}
        h2h = markets.get("h2h")
        spreads = markets.get("spreads")
        totals = markets.get("totals")

        # Spreads: outcomes use full team name + 'point' = handicap from that side
        sp_home = _outcome(spreads, home_full)
        sp_away = _outcome(spreads, away_full)
        home_spread = sp_home.get("point") if sp_home else None

        totals_over = _outcome(totals, "Over")
        totals_under = _outcome(totals, "Under")
        total = totals_over.get("point") if totals_over else (
            totals_under.get("point") if totals_under else None
        )

        # Implied totals (when both spread + total present)
        implied_home = implied_away = None
        if home_spread is not None and total is not None:
            implied_home = total / 2.0 - home_spread / 2.0
            implied_away = total / 2.0 + home_spread / 2.0

        h2h_home = _outcome(h2h, home_full)
        h2h_away = _outcome(h2h, away_full)

        rows.append({
            "game_id": game_id,
            "bookmaker": bm["key"],
            "date": today,
            "home_spread": home_spread,
            "home_spread_price": int(sp_home["price"]) if sp_home and sp_home.get("price") is not None else None,
            "away_spread_price": int(sp_away["price"]) if sp_away and sp_away.get("price") is not None else None,
            "total": total,
            "over_price": int(totals_over["price"]) if totals_over and totals_over.get("price") is not None else None,
            "under_price": int(totals_under["price"]) if totals_under and totals_under.get("price") is not None else None,
            "home_moneyline": int(h2h_home["price"]) if h2h_home and h2h_home.get("price") is not None else None,
            "away_moneyline": int(h2h_away["price"]) if h2h_away and h2h_away.get("price") is not None else None,
            "home_implied_total": round(implied_home, 3) if implied_home is not None else None,
            "away_implied_total": round(implied_away, 3) if implied_away is not None else None,
        })
    return rows


def batch_upsert(rows: list[dict], batch_size: int = 500) -> tuple[int, int]:
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/game_odds_snapshots"
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
            log.error(f"batch at row {i}: {e.code} {body[:200]}")
            errors += len(batch)
    return inserted, errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    today = datetime.date.today().isoformat()
    log.info(f"=== Odds snapshot {today} (season={args.season}) ===")

    try:
        payload, hdrs = fetch_odds()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        log.error(f"Odds API HTTP {e.code}: {body[:300]}")
        sys.exit(1)
    except Exception as e:
        log.error(f"Odds API failed: {e!r}")
        sys.exit(1)

    log.info(
        f"Odds API: {len(payload)} events  "
        f"used={hdrs['x-requests-used']}  remaining={hdrs['x-requests-remaining']}  "
        f"last={hdrs['x-requests-last']}"
    )

    games = fetch_games(args.season)
    lookup = build_game_lookup(games)
    log.info(f"Games table: {len(games)} rows for {args.season}")

    rows: list[dict] = []
    unmatched: list[str] = []
    unknown_names: set[str] = set()

    for event in payload:
        commence = event.get("commence_time", "")
        try:
            dt_utc = datetime.datetime.fromisoformat(commence.replace("Z", "+00:00"))
            date_et = dt_utc.astimezone(ET).date().isoformat()
        except ValueError:
            continue

        home_abbr = name_to_abbr(event.get("home_team", ""))
        away_abbr = name_to_abbr(event.get("away_team", ""))
        if not home_abbr:
            unknown_names.add(event.get("home_team", ""))
        if not away_abbr:
            unknown_names.add(event.get("away_team", ""))
        if not (home_abbr and away_abbr):
            unmatched.append(f"  {date_et}  {event.get('away_team')} @ {event.get('home_team')}  (unknown name)")
            continue

        game_id = lookup.get((date_et, home_abbr, away_abbr))
        if not game_id:
            # Try ±1 day (sometimes commence_time lands on adjacent date in ET)
            for delta in (-1, 1):
                d = (datetime.date.fromisoformat(date_et) + datetime.timedelta(days=delta)).isoformat()
                if (d, home_abbr, away_abbr) in lookup:
                    game_id = lookup[(d, home_abbr, away_abbr)]
                    break
        if not game_id:
            unmatched.append(f"  {date_et}  {away_abbr} @ {home_abbr}  (no matching game)")
            continue

        rows.extend(flatten_event(event, game_id, today))

    log.info(f"Flattened rows: {len(rows)}  (unmatched events: {len(unmatched)})")
    if unknown_names:
        log.warning(f"Unknown team names from Odds API: {sorted(unknown_names)}")
    for line in unmatched[:10]:
        log.warning(line)

    if args.dry_run:
        log.info(f"[dry-run] sample row: {rows[0] if rows else 'none'}")
        return

    inserted, errors = batch_upsert(rows)
    log.info(f"Inserted/updated: {inserted}   Errors: {errors}")

    # JSONL run log (for trend analysis)
    with open(JSONL_PATH, "a") as f:
        f.write(json.dumps({
            "date": today,
            "events_api": len(payload),
            "rows_upserted": inserted,
            "errors": errors,
            "unmatched": len(unmatched),
            "credits_used": hdrs["x-requests-used"],
            "credits_remaining": hdrs["x-requests-remaining"],
            "credits_last": hdrs["x-requests-last"],
        }) + "\n")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
