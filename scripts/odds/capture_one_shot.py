"""One-shot capture of high-value Odds API data before the 20K credit tier
drops back to 90/month on 2026-05-17.

Pulls and dumps raw JSON to data/imports/odds_api/ for later loading into
Supabase (if/when needed). Nothing here is loaded automatically — files are
parking-lot snapshots.

What this captures:

1. **Super Bowl futures snapshot** — outrights endpoint on
   `americanfootball_nfl_super_bowl_winner`. ~1 credit. 32 teams × ~5 books.
   Cheap, locked-in season-long market value.

2. **Player prop probe** — hit `/events/{id}/odds` for every currently-listed
   event (~75) with a slate of common prop markets. The Odds API only charges
   for markets *returned*, so games with no props posted cost 0 credits. We
   log which events return prop data and save the raw JSON for those.

3. **Player season-long futures** — if the SB winner sport has additional
   sub-markets (best record, etc.) we'll pick them up via the outrights call.

Idempotent: re-running with the same date overwrites the day's JSON files.

Usage:
    python3 scripts/odds/capture_one_shot.py
"""
from __future__ import annotations

import datetime
import json
import os
import sys
import time
import urllib.error
import urllib.request

_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import *  # noqa: F401,F403  triggers .env load

API_KEY = os.environ["ODDS_API_KEY"]
OUT_DIR = os.path.join(os.path.dirname(_script_dir), "..", "data", "imports", "odds_api")
os.makedirs(OUT_DIR, exist_ok=True)

TODAY = datetime.date.today().isoformat()

PROP_MARKETS = ",".join([
    "player_pass_yds",
    "player_pass_tds",
    "player_pass_completions",
    "player_pass_attempts",
    "player_pass_interceptions",
    "player_rush_yds",
    "player_rush_tds",
    "player_receptions",
    "player_reception_yds",
    "player_reception_tds",
    "player_anytime_td",
])


def fetch(url: str) -> tuple[object, dict]:
    req = urllib.request.Request(url, headers={"User-Agent": "nfl-db/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        body = json.loads(r.read().decode())
        hdrs = {
            "used": r.headers.get("x-requests-used"),
            "remaining": r.headers.get("x-requests-remaining"),
            "last": r.headers.get("x-requests-last"),
        }
    return body, hdrs


def capture_sb_winner() -> None:
    print("→ Super Bowl winner futures")
    url = (
        f"https://api.the-odds-api.com/v4/sports/americanfootball_nfl_super_bowl_winner/odds"
        f"?regions=us&markets=outrights&oddsFormat=american&apiKey={API_KEY}"
    )
    data, hdrs = fetch(url)
    path = os.path.join(OUT_DIR, f"sb_winner_{TODAY}.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  saved {path}  used={hdrs['used']} remaining={hdrs['remaining']} last={hdrs['last']}")


def capture_props() -> None:
    print("→ Player prop probe across all priced events")
    # List events first (free)
    url = f"https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events?apiKey={API_KEY}"
    events, hdrs = fetch(url)
    print(f"  events available: {len(events)}  remaining={hdrs['remaining']}")

    all_props = []  # events that returned at least one prop market
    no_props = 0
    total_credits_before = int(hdrs["used"] or 0)

    for i, e in enumerate(events):
        evt_url = (
            f"https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events/{e['id']}/odds"
            f"?regions=us&markets={PROP_MARKETS}&oddsFormat=american&apiKey={API_KEY}"
        )
        try:
            data, hdrs = fetch(evt_url)
        except urllib.error.HTTPError as ex:
            body = ex.read().decode("utf-8", errors="replace")
            print(f"  [{i+1:2d}/{len(events)}]  HTTP {ex.code}  {body[:120]}")
            continue
        n_books_with_props = sum(1 for b in data.get("bookmakers", []) if b.get("markets"))
        if n_books_with_props > 0:
            all_props.append(data)
            print(f"  [{i+1:2d}/{len(events)}]  {e['away_team']} @ {e['home_team']}  →  {n_books_with_props} books with props  remaining={hdrs['remaining']}")
        else:
            no_props += 1
        time.sleep(0.2)

    total_credits_after = int(hdrs["used"] or 0)
    print(f"\n  events with prop data:  {len(all_props)}")
    print(f"  events with no props:    {no_props}")
    print(f"  credits used by probe:   {total_credits_after - total_credits_before}")
    print(f"  credits remaining:       {hdrs['remaining']}")

    if all_props:
        path = os.path.join(OUT_DIR, f"props_{TODAY}.json")
        with open(path, "w") as f:
            json.dump(all_props, f, indent=2)
        print(f"  saved {path}")
    else:
        # Save a marker so we know we ran and got nothing
        path = os.path.join(OUT_DIR, f"props_{TODAY}_empty.json")
        with open(path, "w") as f:
            json.dump({"date": TODAY, "events_checked": len(events), "events_with_props": 0}, f, indent=2)
        print(f"  no props anywhere — saved marker file at {path}")


def main():
    print(f"=== One-shot Odds API capture {TODAY} ===\n")
    capture_sb_winner()
    print()
    capture_props()
    print("\nDone.")


if __name__ == "__main__":
    main()
