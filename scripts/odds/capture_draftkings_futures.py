"""One-shot capture of DraftKings NFL season-long futures + player props.

Goes beyond the daily game-line scrape (fetch_draftkings_lines.py) to grab
everything that lives in the futures/awards categories of DK's NFL page:

  * Super Bowl winner odds (32 teams)
  * AFC + NFC winners
  * 8 division winners
  * 6 award futures: MVP, OPOY, DPOY, OROY, DROY, Comeback Player of the Year
  * Player season-long futures (the user's main interest):
    - Regular season passing yards O/U (~21 QBs)
    - Regular season rushing yards O/U (~16 RBs)
    - Regular season receiving yards O/U (~44 WRs/TEs)
    - Regular season receiving TDs O/U (~handful of players)
  * Player matchup H2H futures (e.g. Most Sacks: Crosby vs Hendrickson)
  * Playoff / rookie futures

Strategy: drive headless Chromium, navigate to each known category/subcategory
URL on DK's NFL page, click "View More" until it disappears, capture every
`sportscontent/controldata/.../markets` XHR, and dump the raw JSON to
`data/imports/odds_api/draftkings_futures/{YYYY-MM-DD}/{slug}.json`.

Loaders into Supabase are TBD — this is a parking-lot capture.

Usage:
    python3 scripts/odds/capture_draftkings_futures.py
"""
from __future__ import annotations

import datetime
import json
import os
import sys
import time
from typing import Iterable

DK_NFL_URL = "https://sportsbook.draftkings.com/leagues/football/nfl"

# (slug, query string suffix)  — slug becomes filename
TARGETS: list[tuple[str, str]] = [
    ("super_bowl_winner",          "?category=futures&subcategory=super-bowl"),
    ("conferences",                "?category=futures&subcategory=conferences"),
    ("divisions",                  "?category=futures&subcategory=divisions"),
    ("playoffs",                   "?category=futures&subcategory=playoffs"),
    ("rookie_futures",             "?category=futures&subcategory=rookie-futures"),
    ("player_matchups",            "?category=futures&subcategory=player-matchups"),
    ("wins",                       "?category=futures&subcategory=wins"),
    ("player_pass_yards",          "?category=futures&subcategory=player-futures&nav_1=pass-yards"),
    ("player_rush_yards",          "?category=futures&subcategory=player-futures&nav_1=rush-yards"),
    ("player_rec_yards",           "?category=futures&subcategory=player-futures&nav_1=rec-yards"),
    ("player_rec_tds",             "?category=futures&subcategory=player-futures&nav_1=rec-tds"),
    ("award_mvp",                  "?category=awards&subcategory=mvp"),
    ("award_opoy",                 "?category=awards&subcategory=opoy"),
    ("award_dpoy",                 "?category=awards&subcategory=dpoy"),
    ("award_oroy",                 "?category=awards&subcategory=oroy"),
    ("award_droy",                 "?category=awards&subcategory=droy"),
    ("award_comeback",             "?category=awards&subcategory=comeback"),
]


def _setup_ssl():
    """python.org Python 3.11 lacks SSL certs — use certifi's bundle."""
    try:
        import certifi
        os.environ.setdefault("SSL_CERT_FILE", certifi.where())
    except ImportError:
        pass


def capture_one(page, slug: str, qs: str, out_dir: str) -> dict:
    """Navigate to a single DK category URL, click 'View More' until done,
    collect every sportscontent XHR, and dump to disk."""
    captured: list[dict] = []
    seen_resp_ids = set()  # dedup multiple XHRs with same payload

    def on_response(resp):
        if "sportscontent" not in resp.url or "markets" not in resp.url or resp.status != 200:
            return
        try:
            body = resp.text()
        except Exception:
            return
        key = hash(body)
        if key in seen_resp_ids:
            return
        seen_resp_ids.add(key)
        try:
            data = json.loads(body)
        except Exception:
            return
        captured.append({"url": resp.url, "data": data})

    page.on("response", on_response)
    print(f"\n→ {slug}")
    print(f"  navigating {DK_NFL_URL}{qs}")
    page.goto(f"{DK_NFL_URL}{qs}", wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(5000)
    # Scroll to surface the load-more button if it exists
    for _ in range(3):
        page.mouse.wheel(0, 3000)
        page.wait_for_timeout(500)
    # Click View More until it disappears
    for round_i in range(20):
        clicked = page.evaluate("""() => {
            const b = document.querySelector('.cms-market-selector-load-more-button');
            if (!b) return false;
            b.scrollIntoView({block:'center'});
            b.click();
            return true;
        }""")
        if not clicked:
            break
        page.wait_for_timeout(2500)
    page.wait_for_timeout(1500)
    page.remove_listener("response", on_response)

    # Aggregate counts
    n_events = sum(len(p["data"].get("events", []) or []) for p in captured)
    n_markets = sum(len(p["data"].get("markets", []) or []) for p in captured)
    n_sels = sum(len(p["data"].get("selections", []) or []) for p in captured)
    print(f"  captured {len(captured)} XHR payloads  → events={n_events} markets={n_markets} selections={n_sels}")

    out_path = os.path.join(out_dir, f"{slug}.json")
    with open(out_path, "w") as f:
        json.dump({"slug": slug, "qs": qs, "captured_at": datetime.datetime.utcnow().isoformat() + "Z", "payloads": captured}, f)
    print(f"  saved {out_path}")
    return {"slug": slug, "events": n_events, "markets": n_markets, "selections": n_sels, "path": out_path}


def main() -> None:
    _setup_ssl()
    today = datetime.date.today().isoformat()
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    out_dir = os.path.join(repo_root, "data", "imports", "odds_api", "draftkings_futures", today)
    os.makedirs(out_dir, exist_ok=True)
    print(f"=== DraftKings NFL futures + player season-props capture {today} ===")
    print(f"out: {out_dir}")

    from playwright.sync_api import sync_playwright
    summary: list[dict] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        )
        # Warm up Akamai cookies once on the main NFL page before sweeping
        warm = ctx.new_page()
        warm.goto(DK_NFL_URL, wait_until="domcontentloaded", timeout=60000)
        warm.wait_for_timeout(3000)
        warm.close()

        page = ctx.new_page()
        for slug, qs in TARGETS:
            try:
                summary.append(capture_one(page, slug, qs, out_dir))
            except Exception as e:
                print(f"  ERROR for {slug}: {e!r}")
                summary.append({"slug": slug, "error": str(e)})
            time.sleep(1)
        browser.close()

    # Write summary
    summ_path = os.path.join(out_dir, "_summary.json")
    with open(summ_path, "w") as f:
        json.dump({"date": today, "results": summary}, f, indent=2)

    print(f"\n=== Summary ===")
    for s in summary:
        if "error" in s:
            print(f"  {s['slug']:25s}  ERROR  {s['error']}")
        else:
            print(f"  {s['slug']:25s}  events={s['events']:>4d}  markets={s['markets']:>4d}  selections={s['selections']:>4d}")
    print(f"\nSummary written to {summ_path}")


if __name__ == "__main__":
    main()
