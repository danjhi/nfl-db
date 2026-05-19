"""Scrape full-season DraftKings NFL game lines and upsert into game_odds_snapshots.

The Odds API only shows games once major US books expose them publicly via their
main odds feed. DraftKings sometimes posts season-long look-ahead lines on their
sportsbook page before they appear in The Odds API. This script reads DK's own
internal sportscontent API (the same XHR the DK NFL page itself uses), so we get
whatever DK has on the board right now.

Endpoint (captured from the DK NFL page):
    GET https://sportsbook-nash.draftkings.com/sites/US-OH-SB/api/sportscontent/
        controldata/league/leagueSubcategory/v1/markets?isBatchable=false
        &templateVars=88808,4

Behind Akamai bot mgmt — plain HTTP fails. Must drive a headless browser so that
the page-level Akamai cookies (`_abck`, `bm_sz`) attach to our XHRs.

Strategy:
1. Open the DK NFL page in headless Chromium.
2. Listen for every `sportscontent/controldata/.../markets` XHR.
3. Scroll the page aggressively to trigger client-side pagination
   (DK lazy-loads further weeks as the user scrolls).
4. Parse the captured JSON: each response has events + markets + selections.
5. Match each event to our `games.game_id` by (date_ET, home_abbr, away_abbr).
6. Upsert into `game_odds_snapshots` with bookmaker='draftkings'.

Usage:
    python3 scripts/odds/fetch_draftkings_lines.py [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import sys
import urllib.request
from zoneinfo import ZoneInfo

_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import (  # noqa: E402
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
)

ET = ZoneInfo("America/New_York")
DK_NFL_URL = "https://sportsbook.draftkings.com/leagues/football/nfl"
BOOKMAKER = "draftkings"

# DK uses the Unicode minus sign − in displayOdds.american
def _to_int_price(s):
    if s is None:
        return None
    s = str(s).replace("−", "-").replace("+", "")
    try:
        return int(s)
    except ValueError:
        return None


# DK team metadata.shortName uses sportsbook abbrs. Map to our team_abbr.
DK_SHORT_TO_TEAM = {
    "ARI": "ARI", "ATL": "ATL", "BAL": "BAL", "BUF": "BUF", "CAR": "CAR",
    "CHI": "CHI", "CIN": "CIN", "CLE": "CLE", "DAL": "DAL", "DEN": "DEN",
    "DET": "DET", "GB":  "GB",  "HOU": "HOU", "IND": "IND", "JAX": "JAX",
    "KC":  "KC",  "LAC": "LAC", "LAR": "LA",  # Rams = LA in nfl-db
    "LV":  "LV",  "MIA": "MIA", "MIN": "MIN", "NE":  "NE",  "NO":  "NO",
    "NYG": "NYG", "NYJ": "NYJ", "PHI": "PHI", "PIT": "PIT", "SEA": "SEA",
    "SF":  "SF",  "TB":  "TB",  "TEN": "TEN", "WAS": "WAS",
}


def setup_logging():
    log_dir = os.path.join(os.path.dirname(_script_dir), "..", "data", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "draftkings_lines.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
    )
    return logging.getLogger("dk-lines")


def scrape_dk_payloads(log) -> list[dict]:
    """Drive a headless browser, scroll DK's NFL page, return all captured payloads."""
    from playwright.sync_api import sync_playwright

    payloads: list[dict] = []
    seen_event_ids: set[str] = set()

    def on_response(resp):
        url = resp.url
        if "sportscontent/controldata" not in url:
            return
        if "league" not in url or "markets" not in url:
            return
        if resp.status != 200:
            return
        try:
            data = json.loads(resp.text())
        except Exception as e:
            log.warning(f"  parse failed for {url}: {e}")
            return
        events = data.get("events", []) or []
        new = [e for e in events if e["id"] not in seen_event_ids]
        for e in new:
            seen_event_ids.add(e["id"])
        if new:
            payloads.append(data)
            log.info(f"  XHR captured: +{len(new)} new events (total {len(seen_event_ids)})")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        )
        page = ctx.new_page()
        page.on("response", on_response)

        log.info("Loading DK NFL page...")
        page.goto(DK_NFL_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)

        # DK paginates 100 events per page. The "View More" button
        # (class: cms-market-selector-load-more-button) triggers the next page.
        # Click until it disappears.
        for round_i in range(20):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(500)
            clicked = page.evaluate("""() => {
                const b = document.querySelector('.cms-market-selector-load-more-button');
                if (!b) return null;
                b.scrollIntoView({block:'center'});
                b.click();
                return true;
            }""")
            if not clicked:
                log.info(f"  no more View More button — captured {len(seen_event_ids)} events total")
                break
            page.wait_for_timeout(2500)
            log.info(f"  clicked View More (round {round_i+1}): {len(seen_event_ids)} events so far")
        page.wait_for_timeout(1000)
        browser.close()

    return payloads


def flatten_dk(payloads: list[dict], today: str, log) -> list[dict]:
    """Convert captured payloads into game_odds_snapshots rows."""
    # Deduplicate across payloads — keep latest version of each (event, market, selection)
    events_by_id: dict[str, dict] = {}
    markets_by_id: dict[str, dict] = {}
    sels_by_id: dict[str, dict] = {}
    for d in payloads:
        for e in d.get("events", []) or []:
            events_by_id[e["id"]] = e
        for m in d.get("markets", []) or []:
            markets_by_id[m["id"]] = m
        for s in d.get("selections", []) or []:
            sels_by_id[s["id"]] = s

    # Index selections by marketId
    sels_by_mkt: dict[str, list[dict]] = {}
    for s in sels_by_id.values():
        sels_by_mkt.setdefault(s["marketId"], []).append(s)

    # Index markets by eventId
    mkts_by_evt: dict[str, list[dict]] = {}
    for m in markets_by_id.values():
        mkts_by_evt.setdefault(m["eventId"], []).append(m)

    rows: list[dict] = []
    skipped_no_team = 0
    skipped_partial = 0

    for evt_id, e in events_by_id.items():
        # Pull team short names + roles from participants
        home_short = away_short = None
        for pt in e.get("participants", []):
            short = (pt.get("metadata") or {}).get("shortName")
            role = pt.get("venueRole")
            if role == "Home":
                home_short = short
            elif role == "Away":
                away_short = short
        if not (home_short and away_short):
            skipped_no_team += 1
            continue
        home = DK_SHORT_TO_TEAM.get(home_short)
        away = DK_SHORT_TO_TEAM.get(away_short)
        if not (home and away):
            log.warning(f"  unknown team abbrs: {away_short}@{home_short} in event {evt_id}")
            skipped_no_team += 1
            continue

        # Parse start date → ET local date for matching
        try:
            start = e["startEventDate"].replace("Z", "+00:00").split(".")[0] + "+00:00"
            dt_utc = datetime.datetime.fromisoformat(start)
        except Exception:
            log.warning(f"  bad date for event {evt_id}: {e.get('startEventDate')}")
            continue

        # Walk this event's markets
        home_spread = home_spread_price = away_spread_price = None
        total = over_price = under_price = None
        home_ml = away_ml = None
        for m in mkts_by_evt.get(evt_id, []):
            name = m.get("name")
            for s in sels_by_mkt.get(m["id"], []):
                price = _to_int_price((s.get("displayOdds") or {}).get("american"))
                otype = s.get("outcomeType")
                pts = s.get("points")
                if name == "Moneyline":
                    if otype == "Home":
                        home_ml = price
                    elif otype == "Away":
                        away_ml = price
                elif name == "Spread":
                    if otype == "Home":
                        home_spread = pts
                        home_spread_price = price
                    elif otype == "Away":
                        away_spread_price = price
                elif name == "Total":
                    if otype == "Over":
                        total = pts
                        over_price = price
                    elif otype == "Under":
                        under_price = price

        if home_spread is None and total is None and home_ml is None:
            skipped_partial += 1
            continue

        implied_home = implied_away = None
        if home_spread is not None and total is not None:
            implied_home = round(total / 2.0 - home_spread / 2.0, 3)
            implied_away = round(total / 2.0 + home_spread / 2.0, 3)

        rows.append({
            "_date_et": dt_utc.astimezone(ET).date().isoformat(),
            "_home": home,
            "_away": away,
            "row": {
                "game_id": None,  # filled in after we look up
                "bookmaker": BOOKMAKER,
                "date": today,
                "home_spread": home_spread,
                "home_spread_price": home_spread_price,
                "away_spread_price": away_spread_price,
                "total": total,
                "over_price": over_price,
                "under_price": under_price,
                "home_moneyline": home_ml,
                "away_moneyline": away_ml,
                "home_implied_total": implied_home,
                "away_implied_total": implied_away,
            },
        })

    log.info(
        f"Parsed {len(rows)} candidate rows  "
        f"(skipped_no_team={skipped_no_team}  skipped_no_lines={skipped_partial})"
    )
    return rows


def fetch_games() -> list[dict]:
    """Pull all 2026 games for matching."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    rows = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/games"
            f"?select=game_id,game_date,kickoff,home_team,away_team"
            f"&season=eq.2026&offset={offset}&limit=1000"
        )
        req = urllib.request.Request(url, headers={"apikey": key, "Authorization": f"Bearer {key}"})
        with urllib.request.urlopen(req) as r:
            batch = json.loads(r.read().decode())
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def build_lookup(games: list[dict]) -> dict[tuple, str]:
    """Map (date_ET, home_abbr, away_abbr) → game_id, indexing both game_date and ET-of-kickoff."""
    lookup = {}
    for g in games:
        gid = g["game_id"]
        home, away = g["home_team"], g["away_team"]
        if g.get("game_date"):
            lookup[(g["game_date"], home, away)] = gid
        if g.get("kickoff"):
            try:
                dt_utc = datetime.datetime.fromisoformat(g["kickoff"].replace("Z", "+00:00"))
                d_et = dt_utc.astimezone(ET).date().isoformat()
                lookup[(d_et, home, away)] = gid
            except ValueError:
                pass
    return lookup


def batch_upsert(rows: list[dict], log) -> tuple[int, int]:
    import urllib.error
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/game_odds_snapshots"
    inserted = errors = 0
    for i in range(0, len(rows), 500):
        batch = rows[i:i + 500]
        data = json.dumps(batch).encode()
        req = urllib.request.Request(
            url, data=data,
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
            log.error(f"  batch {i}: {e.code} {body[:200]}")
            errors += len(batch)
    return inserted, errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log = setup_logging()
    today = datetime.date.today().isoformat()
    log.info(f"=== DraftKings lines scrape {today} ===")

    payloads = scrape_dk_payloads(log)
    log.info(f"Captured {len(payloads)} payloads across {sum(len(p.get('events',[])) for p in payloads)} raw event entries")
    if not payloads:
        log.error("No payloads captured. Akamai blocked us or DK changed paths.")
        sys.exit(1)

    candidates = flatten_dk(payloads, today, log)

    games = fetch_games()
    lookup = build_lookup(games)
    log.info(f"games table: {len(games)} rows; lookup keys: {len(lookup)}")

    rows = []
    unmatched: list[str] = []
    for c in candidates:
        key = (c["_date_et"], c["_home"], c["_away"])
        gid = lookup.get(key)
        if not gid:
            # try ±1 day in ET (international games or Thursday/Friday boundary)
            for delta in (-1, 1):
                d = (datetime.date.fromisoformat(c["_date_et"]) + datetime.timedelta(days=delta)).isoformat()
                if (d, c["_home"], c["_away"]) in lookup:
                    gid = lookup[(d, c["_home"], c["_away"])]
                    break
        if not gid:
            unmatched.append(f"  {c['_date_et']} {c['_away']}@{c['_home']}")
            continue
        r = c["row"]
        r["game_id"] = gid
        rows.append(r)

    log.info(f"Matched: {len(rows)}  Unmatched: {len(unmatched)}")
    for u in unmatched[:10]:
        log.warning(u)

    if args.dry_run:
        log.info(f"[dry-run] sample row: {rows[0] if rows else 'none'}")
        return

    inserted, errors = batch_upsert(rows, log)
    log.info(f"Inserted/updated: {inserted}  Errors: {errors}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
