"""Fetch current RTSports ADP and upsert into adp_sources.

RealTime Fantasy Sports publishes a public ADP XML feed. We pull the PPR
leagues feed, match players by name+position (RTSports exposes no id that maps
to our players table), and upsert into adp_sources under source="rtsports".

Feed shape (one flat <player> per row):
    <average-draft-position sport="football" title="PPR Leagues" updated="...">
      <player rtfs-id="18232" stats-id="0" adp="1.45"
              last="Gibbs" first="Jahmyr" position="RB" team="DET"/>
      ...
    </average-draft-position>

Notes:
  - Host: use www.rtsports.com (valid TLS cert). The api.rtsports.com host
    serves the identical feed but presents a cert valid only for rtsports.com,
    so it fails hostname verification. www is the drop-in fix.
  - Positions: QB/RB/WR/TE/K map straight through. DEF rows are team defenses
    (first="Houston" last="Texans"); they don't name-match our offense-oriented
    players table and are reported as an unmatched gap for a later team-DST map.
  - No projected_points or position_rank in the feed; both stored as NULL.
  - RTSports requires a live link back to www.rtsports.com wherever this data
    is displayed. Attribution belongs on the consuming page, not here.

Designed to be run daily.

Usage:
    python3 scripts/adp/fetch_rtsports_adp.py            # fetch + upsert
    python3 scripts/adp/fetch_rtsports_adp.py --dry-run  # fetch + match, no write
    python3 scripts/adp/fetch_rtsports_adp.py --type STD # standard (non-PPR) feed
"""

import argparse
import datetime
import json
import os
import ssl
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

# Add ids dir so shared imports work (mirrors the other adp scrapers).
_script_dir = os.path.dirname(os.path.abspath(__file__)) if os.path.exists(__file__) else os.path.join("scripts", "adp")
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import (  # noqa: E402
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    normalize_name,
    build_player_lookup,
)

# ── Config ───────────────────────────────────────────────────────────────────
YEAR = 2026
SOURCE = "rtsports"
TODAY = datetime.date.today().isoformat()

# RTSports TYPE param -> feed. PPR is the FBG default; STD kept for parity.
RTSPORTS_URL = "https://www.rtsports.com/api-adp-xml?TYPE={type}"
USER_AGENT = "Mozilla/5.0 (nfl-db adp fetcher)"

# macOS system Python often lacks a usable trust store; prefer certifi.
try:
    import certifi

    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()


# ── Fetch ────────────────────────────────────────────────────────────────────
def fetch_rtsports_xml(feed_type="PPR"):
    """Download and parse the RTSports ADP XML. Returns (players, updated)."""
    url = RTSPORTS_URL.format(type=feed_type)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as resp:
        raw = resp.read()
    root = ET.fromstring(raw)
    adp_el = root.find("average-draft-position")
    if adp_el is None:
        raise RuntimeError("no <average-draft-position> element in feed")
    updated = adp_el.get("updated")
    players = [dict(p.attrib) for p in adp_el.findall("player")]
    return players, updated


def fetch_all_players():
    """Fetch all players (paginated) for name-based matching."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    limit = 1000
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,first_name,last_name,position"
            f"&offset={offset}&limit={limit}"
        )
        req = urllib.request.Request(url, headers={
            "apikey": key, "Authorization": f"Bearer {key}",
        })
        with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as resp:
            batch = json.loads(resp.read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += limit
    return players


# ── Upsert ───────────────────────────────────────────────────────────────────
def batch_upsert(rows, batch_size=100):
    """POST rows to adp_sources in batches (idempotent merge-duplicates)."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/adp_sources"
    inserted = 0
    errors = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        data = json.dumps(batch).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates",
        }, method="POST")
        try:
            urllib.request.urlopen(req, timeout=60, context=SSL_CTX)
            inserted += len(batch)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"  ERROR batch at row {i}: {e.code} {body}")
            errors += len(batch)
    return inserted, errors


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Fetch RTSports ADP into adp_sources.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch + match + report, but do not write to Supabase.")
    parser.add_argument("--type", default="PPR",
                        help="RTSports feed TYPE param (default PPR).")
    parser.add_argument("--limit", type=int,
                        help="Cap RTSports rows processed (testing only).")
    args = parser.parse_args()

    # ── 1. Fetch RTSports feed ───────────────────────────────────────────────
    print(f"Fetching RTSports {args.type} ADP feed...")
    rts_rows, updated = fetch_rtsports_xml(args.type)
    if args.limit:
        rts_rows = rts_rows[:args.limit]
    print(f"  {len(rts_rows)} players (feed updated {updated})")

    # ── 2. Build player lookups ──────────────────────────────────────────────
    print("Fetching players from Supabase...")
    all_players = fetch_all_players()
    by_name_pos, by_name = build_player_lookup(all_players)
    print(f"  {len(all_players)} players loaded")

    # ── 3. Match and build adp_sources rows ──────────────────────────────────
    adp_rows = []
    not_found = []          # offense/K we expected to match but didn't
    unmatched_def = []      # team defenses (known gap, matched later)

    for row in rts_rows:
        adp_val = (row.get("adp") or "").strip()
        pos = (row.get("position") or "").strip().upper()
        first = (row.get("first") or "").strip()
        last = (row.get("last") or "").strip()
        name = f"{first} {last}".strip()

        if not adp_val:
            continue
        try:
            adp_num = float(adp_val)
        except ValueError:
            continue

        norm = normalize_name(name)
        player_id = by_name_pos.get((norm, pos)) or by_name.get(norm)

        if not player_id:
            label = f"  {name} ({pos}, {row.get('team', '?')}) adp={adp_val}"
            if pos == "DEF":
                unmatched_def.append(label)
            else:
                not_found.append(label)
            continue

        adp_rows.append({
            "player_id": player_id,
            "source": SOURCE,
            "year": YEAR,
            "date": TODAY,
            "adp": adp_num,
            "projected_points": None,
            "position_rank": None,
        })

    matched = len(adp_rows)
    print(f"\n  Matched & ready: {matched}")
    print(f"  Unmatched offense/K: {len(not_found)}")
    print(f"  Unmatched team DEF (known gap): {len(unmatched_def)}")

    # ── 4. Upsert ────────────────────────────────────────────────────────────
    if args.dry_run:
        print("\n[dry-run] Skipping write to adp_sources.")
    elif adp_rows:
        print(f"\nUpserting {matched} rows to adp_sources (source={SOURCE})...")
        inserted, errors = batch_upsert(adp_rows)
        print(f"  Inserted/updated: {inserted}")
        if errors:
            print(f"  Errors: {errors}")

    # ── 5. Summary ───────────────────────────────────────────────────────────
    print(f"\n{'=' * 50}\nSUMMARY (rtsports {args.type}, {TODAY})\n{'=' * 50}")
    print(f"Feed players:          {len(rts_rows)}")
    print(f"Matched & upserted:    {matched}")
    print(f"Unmatched offense/K:   {len(not_found)}")
    print(f"Unmatched team DEF:    {len(unmatched_def)}")

    if not_found:
        print("\nUnmatched offense/K (need alias or DB add):")
        for line in not_found[:25]:
            print(line)
        if len(not_found) > 25:
            print(f"  ... and {len(not_found) - 25} more")


if __name__ == "__main__":
    main()
