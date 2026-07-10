"""Daily scrape of footballguys.com/updates 'News and Notes' into news_items.

The /updates listing ("Past Issues") maps each edition's date to a ?view={id}
sub-page. Each sub-page embeds the newsletter HTML (escaped once) with the
numbered News-and-Notes stories: an <h3> "{n}. {TEAM}: {headline}", a
"Source: {outlet} - {author}" link, a fact paragraph, and an "Our view:"
paragraph. We parse those, resolve the subject player, classify news_type, and
upsert into news_items (insert-only on source_url, so a human's later status
changes survive re-runs).

This is the forward-going daily job; the one-time email backfill is
backfill_staged_news.py. Both share news_shared.py. Default --limit 7 covers the
newest ~week each run (dedup makes overlap free), so a missed day self-heals.

Usage:
    python3 scripts/news/fetch_fbg_news.py --dry-run
    python3 scripts/news/fetch_fbg_news.py                 # newest 7 editions
    python3 scripts/news/fetch_fbg_news.py --limit 12      # wider (gap fill)
"""

import argparse
import datetime
import html as htmlmod
import os
import re
import ssl
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
from news_shared import (  # noqa: E402
    fetch_players, build_name_index, resolve_player_id,
    classify_news_type, to_row, upsert_news,
)

BASE = "https://www.footballguys.com/updates"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
UNRESOLVED_LOG = os.path.expanduser("~/dev/nfl-db/data/logs/fbg_news_unresolved.jsonl")

_MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], 1)}

# listing: "{Month} {day}(st|nd|rd|th) • Vol. NN, #NN • ... view={id}"
_LISTING = re.compile(
    r'([A-Z][a-z]+)\s+(\d{1,2})(?:st|nd|rd|th)\s*[•·].*?Vol\.\s*\d+,\s*#\d+\s*[•·].*?'
    r'updates\?view=(\d+)', re.S)
_STORY_SPLIT = re.compile(r'<a name="story-\d+"')
_H3 = re.compile(r'<h3[^>]*>\s*\d+\.\s*(?:([A-Z]{2,4}):\s*)?(.*?)</h3>', re.S)
_SOURCE = re.compile(r'href="([^"]+)"[^>]*>\s*Source:\s*(.*?)\s*</a>', re.S)
_P = re.compile(r'<p[^>]*>(.*?)</p>', re.S)
_TAGS = re.compile(r'<[^>]+>')
_WS = re.compile(r'\s+')
_FOOTER = re.compile(r'footballguys|sign up|unsubscribe|copyright|all rights reserved', re.I)

try:
    import certifi

    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=45, context=SSL_CTX) as r:
        return htmlmod.unescape(r.read().decode("utf-8", "replace"))


def _clean(s):
    return _WS.sub(" ", _TAGS.sub(" ", s)).strip()


def _to_iso(month_name, day):
    m = _MONTHS.get(month_name)
    if not m:
        return None
    today = datetime.date.today()
    year = today.year
    if m > today.month + 1:      # e.g. scraping in Jan, edition dated December
        year -= 1
    return f"{year}-{m:02d}-{int(day):02d}"


def list_editions(limit):
    """Return newest `limit` (iso_date, view_id) pairs from the /updates listing."""
    listing = _get(BASE)
    seen, out = set(), []
    for month, day, vid in _LISTING.findall(listing):
        if vid in seen:
            continue
        seen.add(vid)
        iso = _to_iso(month, day)
        if iso:
            out.append((iso, vid))
        if len(out) >= limit:
            break
    return out


def parse_edition(iso_date, view_id):
    """Return list of news item dicts from one ?view= page."""
    page = _get(f"{BASE}?view={view_id}")
    items = []
    for block in _STORY_SPLIT.split(page)[1:]:
        h3 = _H3.search(block)
        src = _SOURCE.search(block)
        if not h3 or not src:
            continue
        team, headline = h3.group(1), _clean(h3.group(2))
        source_url = src.group(1).strip()
        outlet_author = _clean(src.group(2))
        outlet, author = (outlet_author.split(" - ", 1) + [""])[:2] if " - " in outlet_author else (outlet_author, "")

        # fact = paragraphs before "Our view:"; our_view = the "Our view:" para(s),
        # stopping before page footer/nav that can trail the last story.
        paras = [_clean(p) for p in _P.findall(block)]
        paras = [p for p in paras if p and not p.startswith("Source:")]
        fact, view, in_view = [], [], False
        for p in paras:
            if p.startswith("Our view:"):
                in_view = True
                rest = p[len("Our view:"):].strip()
                if rest:
                    view.append(rest)
            elif in_view:
                if _FOOTER.search(p) or len(view) >= 2:
                    break
                view.append(p)
            else:
                if _FOOTER.search(p):
                    break
                fact.append(p)

        items.append({
            "date": iso_date, "team": team, "headline": headline,
            "source_outlet": outlet.strip(), "source_author": author.strip(),
            "source_url": source_url, "fact_text": " ".join(fact),
            "our_view_text": " ".join(view),
        })
    return items


def main():
    ap = argparse.ArgumentParser(description="Scrape footballguys.com/updates news into news_items.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=7, help="how many recent editions to process")
    args = ap.parse_args()

    print(f"Fetching /updates listing (newest {args.limit} editions)...")
    editions = list_editions(args.limit)
    for iso, vid in editions:
        print(f"  {iso}  view={vid}")

    all_items = []
    for iso, vid in editions:
        items = parse_edition(iso, vid)
        print(f"  parsed {len(items):>2} items from {iso} (view={vid})")
        all_items.extend(items)

    print(f"\nTotal items scraped: {len(all_items)}")
    print("Building player name index...")
    name_index = build_name_index(fetch_players())

    rows, unresolved = [], []
    from collections import Counter
    types = Counter()
    for it in all_items:
        if not it.get("source_url"):
            continue
        pid = resolve_player_id(it, name_index)
        if not pid:
            unresolved.append(it)
            continue
        nt = classify_news_type(it.get("headline"), it.get("fact_text"))
        types[nt] += 1
        rows.append(to_row(it, pid, nt))

    print(f"  Resolved: {len(rows)}  |  Unresolved (skipped): {len(unresolved)}")
    print(f"  news_type mix: {dict(types)}")

    if args.dry_run:
        print(f"\n[dry-run] would upsert {len(rows)} rows (new source_urls insert; existing ignored).")
        for it in all_items[:3]:
            print(f"    e.g. [{it['date']} {it['team']}] {it['headline']}")
            print(f"         fact: {it['fact_text'][:90]}...  view: {it['our_view_text'][:60]}...")
        return

    os.makedirs(os.path.dirname(UNRESOLVED_LOG), exist_ok=True)
    with open(UNRESOLVED_LOG, "a") as f:
        for it in unresolved:
            import json
            f.write(json.dumps(it) + "\n")
    print(f"\nUpserting {len(rows)} rows to news_items...")
    ok, errors = upsert_news(rows)
    print(f"  Sent: {ok}  Errors: {errors}  (existing source_urls ignored)")


if __name__ == "__main__":
    main()
