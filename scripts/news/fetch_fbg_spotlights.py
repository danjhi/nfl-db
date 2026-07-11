"""Scrape footballguys.com Player Spotlight articles into spotlight_articles.

The articles listing (articles?category=15) is server-rendered; each card
carries everything we store — no per-article fetches needed:

    <div class="card card-ps content-item article-item">
      <a class="article-link ..." href="https://.../article/2026-player-spotlights-{slug}">
        <div class="thumb-bg" style="background-image: url({photo});"></div>
        <div class="content-text">
          <h5>{Player Name}: {Hook}</h5>
          <span class="content-date">{Author}, {Mon} {D}, {YYYY}</span>
        </div>
      </a>
    </div>

The year comes from the URL slug ({year}-player-spotlights-...); only
--year (default 2026) rows are kept. The featured player is resolved from
the title's "{Player Name}:" prefix against the players table (news_shared
name index). Insert-only on url (ignore-duplicates), mirroring news_items,
so re-runs are free and later human edits survive.

Usage:
    python3 scripts/news/fetch_fbg_spotlights.py --dry-run
    python3 scripts/news/fetch_fbg_spotlights.py
    python3 scripts/news/fetch_fbg_spotlights.py --year 2026
"""

import argparse
import datetime
import html as htmlmod
import json
import os
import re
import ssl
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from news_shared import fetch_players, build_name_index, resolve_player_id  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ids"))
from shared import SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_KEY  # noqa: E402

LISTING = "https://www.footballguys.com/articles?category=15"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"

_CARD = re.compile(
    r'href="(?P<url>https://www\.footballguys\.com/article/(?P<year>\d{4})-player-spotlights[^"]*)".*?'
    r'background-image:\s*url\((?P<photo>[^)]+)\).*?'
    r"<h5>(?P<title>.*?)</h5>.*?"
    r'<span class="content-date">(?P<byline>.*?)</span>',
    re.S,
)
_MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul",
     "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}

try:
    import certifi

    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()


def fetch_listing():
    req = urllib.request.Request(LISTING, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as r:
        return r.read().decode("utf-8", errors="replace")


def parse_byline(byline):
    """'Jason Wood, Jul 10, 2026' -> (author, date). Author keeps any commas."""
    parts = [p.strip() for p in byline.rsplit(",", 2)]
    if len(parts) == 3:
        author, monthday, year = parts
        m = re.match(r"([A-Za-z]{3})[a-z]*\s+(\d{1,2})", monthday)
        if m and m.group(1) in _MONTHS and year.isdigit():
            try:
                date = datetime.date(int(year), _MONTHS[m.group(1)], int(m.group(2)))
                return author, date.isoformat()
            except ValueError:
                pass
    return byline.strip() or None, None


def insert_rows(rows, batch_size=50):
    """Insert-only on url; existing rows untouched."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/spotlight_articles?on_conflict=url"
    ok = errors = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        req = urllib.request.Request(url, data=json.dumps(batch).encode(), headers={
            "apikey": key, "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=ignore-duplicates",
        }, method="POST")
        try:
            urllib.request.urlopen(req, timeout=60, context=SSL_CTX)
            ok += len(batch)
        except urllib.error.HTTPError as e:
            print(f"  ERROR batch at {i}: {e.code} {e.read().decode(errors='replace')[:300]}")
            errors += len(batch)
    return ok, errors


def main():
    parser = argparse.ArgumentParser(description="Scrape FBG Player Spotlights.")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    html = fetch_listing()
    cards = list(_CARD.finditer(html))
    print(f"{len(cards)} spotlight cards on the listing")

    print("Building player name index…")
    name_index = build_name_index(fetch_players())

    rows = []
    skipped_year = 0
    unresolved = []
    seen = set()
    for m in cards:
        if int(m.group("year")) != args.year:
            skipped_year += 1
            continue
        url = m.group("url")
        if url in seen:
            continue
        seen.add(url)
        title = htmlmod.unescape(re.sub(r"<[^>]+>", "", m.group("title"))).strip()
        author, published = parse_byline(htmlmod.unescape(m.group("byline")))
        # The featured player is the title's "{Name}:" prefix; resolve that
        # alone so a second player mentioned in the hook can't win.
        prefix = title.split(":", 1)[0]
        pid = resolve_player_id({"headline": prefix, "fact_text": "", "team": None}, name_index)
        if not pid:
            unresolved.append(f"  {title} [{url.rsplit('/', 1)[-1]}]")
        rows.append({
            "player_id": pid,
            "title": title,
            "url": url,
            "photo_url": htmlmod.unescape(m.group("photo")).strip("'\" "),
            "author": author,
            "published_at": published,
            "year": args.year,
        })

    print(f"{len(rows)} articles for {args.year} ({skipped_year} other-year skipped)")
    print(f"Resolved players: {len(rows) - len(unresolved)}/{len(rows)}")
    if unresolved:
        print("Unresolved (stored with player_id NULL):")
        for line in unresolved:
            print(line)

    if args.dry_run:
        print("\n[dry-run] not writing; sample row:")
        if rows:
            print(json.dumps(rows[0], indent=2)[:500])
        return
    ok, errors = insert_rows(rows)
    print(f"Inserted-or-ignored: {ok}, errors: {errors}")


if __name__ == "__main__":
    main()
