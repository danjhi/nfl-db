"""Fetch CBS Sports redraft ADP and upsert into adp_sources (source="cbs").

CBS publishes draft "average position" as static server-rendered HTML (no login,
no JS). The pages carry a CBS player id in each row's href
(/nfl/players/{cbs_id}/...), which joins directly to players.cbs_id.

CBS splits the board by scoring/position, so we fetch three pages:
  - offense (PPR):  /ppr/both/h2h/all/  — QB/RB/WR/TE (K/DST aren't PPR-scored,
                    so they're absent from the "all" PPR view)
  - kickers:        /both/h2h/K/        — individual kickers
  - team defense:   /both/h2h/DST/      — one row per team ("Broncos", team=DEN)

Columns per row: [Rank, Player, Trend, Avg Pos (ADP), Hi/Lo, Pct%]. ADP is cell 3.

Join:
  - Offense + kickers: cbs_id (from the href) -> players.cbs_id; name+pos fallback
    (name-only catches the new kickers, which have no cbs_id yet).
  - Team defense: remap -> DEF_{TEAM} via the team abbr in the row.

`source="cbs"` matches the key FBG's feed carries (the consumer contract).

NOTE: this parses CBS's HTML with regexes keyed on their design-system classes
(TableBase-bodyTr, CellPlayerName--long/-position/-team). Stable, but re-check if
CBS restyles the draft-averages page.

Usage:
    python3 scripts/adp/fetch_cbs_adp.py            # fetch + upsert
    python3 scripts/adp/fetch_cbs_adp.py --dry-run  # fetch + match, no write
"""

import argparse
import datetime
import json
import os
import re
import ssl
import sys
import urllib.error
import urllib.request

_script_dir = os.path.dirname(os.path.abspath(__file__)) if os.path.exists(__file__) else os.path.join("scripts", "adp")
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import (  # noqa: E402
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    normalize_name,
    normalize_team,
    build_player_lookup,
)

# ── Config ───────────────────────────────────────────────────────────────────
YEAR = 2026
SOURCE = "cbs"
TODAY = datetime.date.today().isoformat()

CBS_BASE = "https://www.cbssports.com/fantasy/football/draft/averages"
# (url, is_dst) — offense uses the PPR view; K/DST live on the non-PPR paths.
CBS_PAGES = [
    (f"{CBS_BASE}/ppr/both/h2h/all/", False),
    (f"{CBS_BASE}/both/h2h/K/", False),
    (f"{CBS_BASE}/both/h2h/DST/", True),
]
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"

_ROW = re.compile(r'<tr class="TableBase-bodyTr">(.*?)</tr>', re.S)
_ID = re.compile(r'/nfl/players/(\d+)/')
_LONG = re.compile(
    r'CellPlayerName--long.*?<a[^>]*>(.*?)</a>.*?'
    r'CellPlayerName-position">\s*(.*?)\s*</span>.*?'
    r'CellPlayerName-team">\s*(.*?)\s*</span>', re.S)
_TD = re.compile(r'<td[^>]*>(.*?)</td>', re.S)
_TAGS = re.compile(r'<[^>]+>')
_WS = re.compile(r'\s+')


def _clean(s):
    return _WS.sub(" ", _TAGS.sub(" ", s)).strip()


try:
    import certifi

    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()


# ── Fetch + parse ────────────────────────────────────────────────────────────
def fetch_cbs_page(url):
    """Return list of (cbs_id, name, pos, team, adp) for one CBS page."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=45, context=SSL_CTX) as resp:
        html = resp.read().decode("utf-8", "replace")
    out = []
    for row in _ROW.findall(html):
        idm = _ID.search(row)
        lm = _LONG.search(row)
        cells = _TD.findall(row)
        if not lm or len(cells) < 4:
            continue
        try:
            adp = float(_clean(cells[3]))
        except ValueError:
            continue
        out.append((idm.group(1) if idm else None,
                    _clean(lm.group(1)), _clean(lm.group(2)), _clean(lm.group(3)), adp))
    return out


def fetch_all_players():
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players, offset = [], 0
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/players"
               f"?select=player_id,first_name,last_name,position,latest_team,cbs_id"
               f"&offset={offset}&limit=1000")
        req = urllib.request.Request(url, headers={"apikey": key, "Authorization": f"Bearer {key}"})
        with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as resp:
            batch = json.loads(resp.read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += 1000
    return players


# ── Upsert ───────────────────────────────────────────────────────────────────
def batch_upsert(rows, batch_size=100):
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/adp_sources"
    inserted = errors = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        req = urllib.request.Request(url, data=json.dumps(batch).encode("utf-8"), headers={
            "apikey": key, "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates",
        }, method="POST")
        try:
            urllib.request.urlopen(req, timeout=60, context=SSL_CTX)
            inserted += len(batch)
        except urllib.error.HTTPError as e:
            print(f"  ERROR batch at {i}: {e.code} {e.read().decode('utf-8','replace')}")
            errors += len(batch)
    return inserted, errors


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Fetch CBS ADP into adp_sources.")
    ap.add_argument("--dry-run", action="store_true", help="fetch + match, no write")
    args = ap.parse_args()

    print("Fetching CBS draft-averages pages...")
    entries = []
    for url, is_dst in CBS_PAGES:
        page = fetch_cbs_page(url)
        print(f"  {len(page):>3} rows  {url}")
        entries.extend((cid, nm, pos, tm, adp, is_dst) for cid, nm, pos, tm, adp in page)

    print("Fetching players from Supabase...")
    all_players = fetch_all_players()
    by_cbs_id = {str(p["cbs_id"]): p["player_id"] for p in all_players if p.get("cbs_id")}
    def_by_team = {normalize_team(p["latest_team"] or ""): p["player_id"]
                   for p in all_players if p["position"] == "DEF"}
    by_name_pos, by_name = build_player_lookup(all_players)
    print(f"  {len(all_players)} players ({len(by_cbs_id)} with cbs_id, {len(def_by_team)} defenses)")

    adp_rows = []
    counts = {"offense": [0, 0], "k": [0, 0], "def": [0, 0]}
    unmatched = []
    seen = set()
    for cid, name, pos, team, adp, is_dst in entries:
        if is_dst or pos == "DST":
            target, category = def_by_team.get(normalize_team(team)), "def"
        else:
            target = by_cbs_id.get(cid) if cid else None
            if not target:
                norm = normalize_name(name)
                target = by_name_pos.get((norm, pos)) or by_name.get(norm)
            category = "k" if pos == "K" else "offense"

        if not target:
            counts[category][1] += 1
            unmatched.append(f"  {name} ({pos}, {team}) cbs_id={cid} adp={adp}")
            continue
        if target in seen:          # a player can't appear twice across pages
            continue
        seen.add(target)
        counts[category][0] += 1
        adp_rows.append({"player_id": target, "source": SOURCE, "year": YEAR, "date": TODAY,
                         "adp": adp, "projected_points": None, "position_rank": None})

    matched = len(adp_rows)
    print(f"\n  Matched: {matched}  "
          f"[offense {counts['offense'][0]}, k {counts['k'][0]}, def {counts['def'][0]}]")
    print(f"  Unmatched: {sum(c[1] for c in counts.values())}  "
          f"[offense {counts['offense'][1]}, k {counts['k'][1]}, def {counts['def'][1]}]")
    for line in unmatched[:20]:
        print(line)

    if args.dry_run:
        print(f"\n[dry-run] would upsert {matched} rows (source={SOURCE}).")
    elif adp_rows:
        print(f"\nUpserting {matched} rows (source={SOURCE})...")
        inserted, errors = batch_upsert(adp_rows)
        print(f"  Inserted/updated: {inserted}" + (f"  Errors: {errors}" if errors else ""))

    print(f"\n{'=' * 50}\nDONE ({'dry-run' if args.dry_run else 'wrote'}) — {matched} rows, {TODAY}\n{'=' * 50}")


if __name__ == "__main__":
    main()
