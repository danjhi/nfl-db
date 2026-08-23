"""Fetch FFPC Main Event ADP and upsert into adp_sources (source="ffpc").

FFPC's public ADP page (myffpc.com/cms/public/ffpc-league-and-tournament-adp)
is a thin client over an XML endpoint:

    https://myffpc.com/FFPCADPReport.ashx
        ?leagueTypeID=1                (Main Event)
        &draftStartDateFrom=16Aug2026  (ddMonYYYY)
        &draftStartDateTo=23Aug2026
        &superflexFilter=0&slimRostersFilter=0   (locked-lineup league types)

Each <player> row carries name, nflTeam, position, adp, min/max, and
sportsDataForeignKey, which IS our players.player_id UUID, so the join is a
direct key match (name+position fallback for the rare miss; DST rows remap
to DEF_{TEAM} by team abbr like the CBS scraper).

Window: trailing WINDOW_DAYS days of draft starts (Dan, 2026-08-23: one
week), so the column tracks the live market instead of the season blend.

The source key "ffpc" matches the key FBG's feed already carries; once the
labs backfill posts our daily sets, the feed serves these instead of the
one-time August 1 manual upload that had gone stale.

Usage:
    python3 scripts/adp/fetch_ffpc_adp.py            # fetch + upsert
    python3 scripts/adp/fetch_ffpc_adp.py --dry-run  # fetch + match, no write
"""

import argparse
import datetime
import os
import re
import ssl
import sys
import urllib.request
import xml.etree.ElementTree as ET

_script_dir = os.path.dirname(os.path.abspath(__file__)) if os.path.exists(__file__) else os.path.join("scripts", "adp")
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import (  # noqa: E402
    normalize_name,
    normalize_team,
    build_player_lookup,
)
from fetch_cbs_adp import fetch_all_players, batch_upsert  # noqa: E402

# ── Config ───────────────────────────────────────────────────────────────────
YEAR = 2026
SOURCE = "ffpc"
TODAY = datetime.date.today().isoformat()
WINDOW_DAYS = 7
LEAGUE_TYPE_ID = 1  # FFPC Main Event
ADP_MAX = 350  # FFPC returns every rosterable player; undrafted pins at 350
               # (their page applies the same adp < 350 filter)
# Out for the season: excluded from redraft ADP (Dan, 2026-08-23). Window
# drafts still contain their pre-injury picks. players.player_id UUIDs.
OUT_FOR_SEASON_PLAYER_IDS = {
    "f55444a9-ccad-4343-bb03-caca6ea3de99",  # Jayden Higgins, ACL 2026-08-18
    "aca4c1b8-915f-4295-92e4-05b51feee6b1",  # Ricky Pearsall, out for season
}

FFPC_URL = "https://myffpc.com/FFPCADPReport.ashx"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
SSL_CTX = ssl.create_default_context()
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def api_date(d: datetime.date) -> str:
    return f"{d.day:02d}{MONTHS[d.month - 1]}{d.year}"


def fetch_ffpc_xml() -> str:
    today = datetime.date.today()
    params = (
        f"?leagueTypeID={LEAGUE_TYPE_ID}"
        f"&draftStartDateFrom={api_date(today - datetime.timedelta(days=WINDOW_DAYS))}"
        f"&draftStartDateTo={api_date(today)}"
        f"&superflexFilter=0&slimRostersFilter=0"
    )
    req = urllib.request.Request(FFPC_URL + params, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=45, context=SSL_CTX) as resp:
        return resp.read().decode("utf-8", errors="replace")


def main():
    ap = argparse.ArgumentParser(description="Fetch FFPC Main Event ADP into adp_sources.")
    ap.add_argument("--dry-run", action="store_true", help="fetch + match, no write")
    args = ap.parse_args()

    print(f"Fetching FFPC Main Event ADP (trailing {WINDOW_DAYS} days)...")
    root = ET.fromstring(fetch_ffpc_xml())
    players_el = root.find("players")
    league_count = (players_el.get("leagueCount") if players_el is not None else "0") or "0"
    rows = players_el.findall("player") if players_el is not None else []
    print(f"  {len(rows)} players from {league_count} leagues")

    print("Fetching players from Supabase...")
    all_players = fetch_all_players()
    known_ids = {p["player_id"] for p in all_players}
    def_by_team = {normalize_team(p["latest_team"] or ""): p["player_id"]
                   for p in all_players if p["position"] == "DEF"}
    by_name_pos, by_name = build_player_lookup(all_players)
    print(f"  {len(all_players)} players ({len(def_by_team)} defenses)")

    adp_rows = []
    matched_key = matched_fallback = 0
    unmatched = []
    seen = set()
    for el in rows:
        name = el.get("name") or ""
        team = el.get("nflTeam") or ""
        pos = (el.get("position") or "").upper()
        sdio = (el.get("sportsDataForeignKey") or "").strip().lower()
        try:
            adp = float(el.get("adp"))
        except (TypeError, ValueError):
            continue
        if adp >= ADP_MAX:
            continue

        if pos in ("DF", "DST", "DEF", "D/ST"):
            target = def_by_team.get(normalize_team(team))
        elif sdio and sdio in known_ids:
            target = sdio
            matched_key += 1
        else:
            norm = normalize_name(name)
            target = by_name_pos.get((norm, pos)) or by_name.get(norm)
            if target:
                matched_fallback += 1

        if not target:
            unmatched.append(f"  {name} ({pos}, {team}) adp={adp}")
            continue
        if target in OUT_FOR_SEASON_PLAYER_IDS:
            continue
        if target in seen:
            continue
        seen.add(target)
        adp_rows.append({"player_id": target, "source": SOURCE, "year": YEAR, "date": TODAY,
                         "adp": adp, "projected_points": None, "position_rank": None})

    print(f"\n  Matched: {len(adp_rows)}  [by key {matched_key}, fallback {matched_fallback}, "
          f"def {len(adp_rows) - matched_key - matched_fallback}]")
    print(f"  Unmatched: {len(unmatched)}")
    for line in unmatched[:15]:
        print(line)

    if args.dry_run:
        print(f"\n[dry-run] would upsert {len(adp_rows)} rows (source={SOURCE}).")
    elif adp_rows:
        print(f"\nUpserting {len(adp_rows)} rows (source={SOURCE})...")
        inserted, errors = batch_upsert(adp_rows)
        print(f"  Inserted/updated: {inserted}" + (f"  Errors: {errors}" if errors else ""))

    print(f"\n{'=' * 50}\nDONE ({'dry-run' if args.dry_run else 'wrote'}) — {len(adp_rows)} rows, {TODAY}\n{'=' * 50}")


if __name__ == "__main__":
    main()
