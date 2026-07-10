"""Fetch Yahoo redraft ADP and upsert into adp_sources (source="yahoo").

Yahoo exposes ADP through its PUBLIC read-only fantasy API
(pub-api-ro.fantasysports.yahoo.com) — no OAuth, no login. The players resource
with the draft_analysis subresource returns average_pick per player, and the
`player_id` field IS our `yahoo_id`. `format=json_f` gives clean flattened JSON;
`sort=AR` orders by average round (ADP order); `count=300` returns the whole
drafted board in one request.

Join:
  - Offense (QB/RB/WR/TE) + kickers: player_id -> players.yahoo_id; name+pos
    fallback (covers the new kickers, which lack yahoo_id).
  - Team defense (display_position "DEF"): remap -> DEF_{TEAM} via
    editorial_team_abbr, so Yahoo defense shares the clean defense row with every
    other source (same treatment as RTSports/NFFC/ESPN/CBS).

`source="yahoo"` matches the key FBG's feed carries (the consumer contract).

Usage:
    python3 scripts/adp/fetch_yahoo_adp.py            # fetch + upsert
    python3 scripts/adp/fetch_yahoo_adp.py --dry-run  # fetch + match, no write
"""

import argparse
import datetime
import json
import os
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
SOURCE = "yahoo"
TODAY = datetime.date.today().isoformat()

YAHOO_URL = (
    "https://pub-api-ro.fantasysports.yahoo.com/fantasy/v2/game/nfl/"
    "players;sort=AR;out=draft_analysis;count=300?format=json_f"
)
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"

try:
    import certifi

    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()


# ── Fetch ────────────────────────────────────────────────────────────────────
def fetch_yahoo_players():
    req = urllib.request.Request(YAHOO_URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=45, context=SSL_CTX) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data.get("fantasy_content", {}).get("game", {}).get("players", [])


def fetch_all_players():
    """player_id, name, position, latest_team, yahoo_id (paginated)."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players, offset = [], 0
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/players"
               f"?select=player_id,first_name,last_name,position,latest_team,yahoo_id"
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
    ap = argparse.ArgumentParser(description="Fetch Yahoo ADP into adp_sources.")
    ap.add_argument("--dry-run", action="store_true", help="fetch + match, no write")
    args = ap.parse_args()

    print("Fetching Yahoo public draft_analysis API...")
    yahoo = fetch_yahoo_players()
    print(f"  {len(yahoo)} Yahoo players")

    print("Fetching players from Supabase...")
    all_players = fetch_all_players()
    by_yahoo_id = {str(p["yahoo_id"]): p["player_id"] for p in all_players if p.get("yahoo_id")}
    def_by_team = {normalize_team(p["latest_team"] or ""): p["player_id"]
                   for p in all_players if p["position"] == "DEF"}
    by_name_pos, by_name = build_player_lookup(all_players)
    print(f"  {len(all_players)} players ({len(by_yahoo_id)} with yahoo_id, {len(def_by_team)} defenses)")

    adp_rows = []
    counts = {"offense": [0, 0], "k": [0, 0], "def": [0, 0]}
    unmatched = []
    for entry in yahoo:
        pl = entry.get("player") or {}
        da = pl.get("draft_analysis") or {}
        try:
            adp = float(da.get("average_pick"))
        except (TypeError, ValueError):
            continue
        pos = pl.get("display_position") or ""

        if pos == "DEF":
            abbr = normalize_team((pl.get("editorial_team_abbr") or "").upper())
            target, category = def_by_team.get(abbr), "def"
        else:
            target = by_yahoo_id.get(str(pl.get("player_id")))
            if not target:
                norm = normalize_name((pl.get("name") or {}).get("full") or "")
                target = by_name_pos.get((norm, pos)) or by_name.get(norm)
            category = "k" if pos == "K" else "offense"

        if target:
            counts[category][0] += 1
            adp_rows.append({"player_id": target, "source": SOURCE, "year": YEAR, "date": TODAY,
                             "adp": adp, "projected_points": None, "position_rank": None})
        else:
            counts[category][1] += 1
            unmatched.append(f"  {(pl.get('name') or {}).get('full','?')} ({pos}, "
                             f"{pl.get('editorial_team_abbr')}) yahoo_id={pl.get('player_id')} adp={adp}")

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
