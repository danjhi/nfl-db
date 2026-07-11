"""One-time snapshot of the FBG feed's MFL ADP column into adp_sources.

The MFL scrape was abandoned (the export API sample was too thin to trust:
a handful of mock drafts). Until MFL is dropped from the ADP page entirely,
we freeze the last-known-good values from FBG's own feed so the column
keeps rendering after the page stops reading FBG's feed for owned sources.

Reads https://www.footballguys.com/api/nfl/{year}/adp-sources, takes the
`mfl` key per player, resolves FBG ids to nfl-db ids via
players.footballguys_id, and upserts source="mfl" rows dated today.
Idempotent; re-run on purpose only (a re-run on a later date creates a new
snapshot date, which is fine and intended if MFL values ever refresh).

Usage:
    python3 scripts/adp/snapshot_mfl_adp.py --dry-run
    python3 scripts/adp/snapshot_mfl_adp.py
"""

import argparse
import datetime
import json
import os
import ssl
import sys
import urllib.request

_script_dir = os.path.dirname(os.path.abspath(__file__)) if os.path.exists(__file__) else os.path.join("scripts", "adp")
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_KEY  # noqa: E402

YEAR = 2026
SOURCE = "mfl"
TODAY = datetime.date.today().isoformat()
FEED = f"https://www.footballguys.com/api/nfl/{YEAR}/adp-sources"

try:
    import certifi

    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()

KEY = SUPABASE_SERVICE_KEY or SUPABASE_KEY
HDR = {"apikey": KEY, "Authorization": f"Bearer {KEY}"}


def get_json(url, headers=None):
    # NOTE: no browser User-Agent on Supabase calls — the service-role secret
    # is rejected (401) when the request looks like a browser. Only the FBG
    # feed fetch gets a UA.
    h = dict(headers or {})
    if "supabase.co" not in url:
        h.setdefault("User-Agent", "Mozilla/5.0")
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as r:
        return json.loads(r.read().decode())


def fetch_fbg_id_map():
    """players with footballguys_id set: fbg id -> player_id."""
    out = {}
    offset = 0
    while True:
        batch = get_json(
            f"{SUPABASE_URL}/rest/v1/players?select=player_id,footballguys_id"
            f"&footballguys_id=not.is.null&offset={offset}&limit=1000",
            HDR,
        )
        if not batch:
            break
        for p in batch:
            out[p["footballguys_id"]] = p["player_id"]
        offset += 1000
    return out


def main():
    parser = argparse.ArgumentParser(description="Snapshot FBG feed MFL ADP into adp_sources.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    feed = get_json(FEED)
    mfl_rows = [(r["id"], float(r["mfl"])) for r in feed if r.get("mfl") is not None]
    print(f"FBG feed: {len(feed)} rows, {len(mfl_rows)} with an mfl value")

    fbg_to_pid = fetch_fbg_id_map()
    print(f"{len(fbg_to_pid)} players carry footballguys_id")

    rows, unmatched = [], []
    for fbg_id, adp in mfl_rows:
        pid = fbg_to_pid.get(fbg_id)
        if not pid:
            unmatched.append(fbg_id)
            continue
        rows.append({
            "player_id": pid,
            "source": SOURCE,
            "year": YEAR,
            "date": TODAY,
            "adp": adp,
            "projected_points": None,
            "position_rank": None,
        })
    print(f"Matched {len(rows)}, unmatched fbg ids: {len(unmatched)} {unmatched[:8]}")

    if args.dry_run:
        print("[dry-run] not writing")
        return
    data = json.dumps(rows).encode()
    req = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/adp_sources",
        data=data,
        headers={**HDR, "Content-Type": "application/json",
                 "Prefer": "return=minimal,resolution=merge-duplicates"},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=60, context=SSL_CTX)
    print(f"Upserted {len(rows)} rows as source={SOURCE}, date={TODAY}")


if __name__ == "__main__":
    main()
