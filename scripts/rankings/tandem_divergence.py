"""Divergence report: Dan's DK master order vs each site's live market ADP.

The "talk to Claude" input for the tandem loop: for Underdog and Drafters,
line up Dan's rank (DK drag-and-drop order) against the site's latest market
rank (today's adp_sources snapshot) and surface the biggest gaps both ways.
Rank-space on both sides, so Drafters' round.pick ADP encoding never matters.

Usage:
    python3 scripts/rankings/tandem_divergence.py [--top 250] [--dk FILE]
        [--min-gap 15] [--show 12]
"""

import argparse
import json
import os
import sys
import urllib.request

_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
sys.path.insert(0, _script_dir)
from shared import SUPABASE_URL, SUPABASE_SERVICE_KEY  # noqa: E402
from tandem_from_dk import read_dk, newest, name_keys, DOWNLOADS  # noqa: E402

SITES = {"underdog": "underdog_postdraft", "drafters": "drafters_postdraft"}
YEAR = 2026


def _get(path):
    req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/{path}", headers={
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def latest_site_ranks(source):
    """{player_id: market_rank} for the source's most recent snapshot."""
    latest = _get(f"adp_sources?select=date&source=eq.{source}&year=eq.{YEAR}"
                  f"&order=date.desc&limit=1")
    if not latest:
        sys.exit(f"ERROR: no adp_sources rows for {source}")
    date = latest[0]["date"]
    rows = _get(f"adp_sources?select=player_id,adp&source=eq.{source}"
                f"&year=eq.{YEAR}&date=eq.{date}&order=adp.asc&limit=1000")
    return date, {r["player_id"]: i + 1 for i, r in enumerate(rows)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=250)
    ap.add_argument("--dk", default=None)
    ap.add_argument("--min-gap", type=int, default=15)
    ap.add_argument("--show", type=int, default=12)
    args = ap.parse_args()

    dk_path = args.dk or newest(os.path.join(DOWNLOADS, "DkPreDraftRankings*.csv"), "DK rankings")
    master = read_dk(dk_path, args.top)
    print(f"Master: {os.path.basename(dk_path)} · top {len(master)}")

    players = _get("players?select=player_id,draftkings_id,first_name,last_name,position&limit=3000")
    by_dk_id = {str(p["draftkings_id"]): p for p in players if p.get("draftkings_id")}
    by_name = {}
    for p in players:
        for k in name_keys(f"{p['first_name']} {p['last_name']}"):
            by_name.setdefault((k, p["position"]), p)

    resolved = []
    for i, row in enumerate(master):
        p = by_dk_id.get(row["ID"])
        if not p:
            for k in name_keys(row["Name"]):
                p = by_name.get((k, row["Position"]))
                if p:
                    break
        resolved.append((i + 1, row, p["player_id"] if p else None))

    for site, source in SITES.items():
        date, ranks = latest_site_ranks(source)
        print(f"\n{'=' * 62}\n{site.upper()} · market snapshot {date} · {len(ranks)} players ranked")

        gaps = []
        missing = []
        for my_rank, row, pid in resolved:
            mkt = ranks.get(pid) if pid else None
            if mkt is None:
                if my_rank <= 150:
                    missing.append(f"{row['Name']} ({row['Position']}, you {my_rank})")
                continue
            gaps.append((mkt - my_rank, my_rank, mkt, row))

        higher = sorted([g for g in gaps if g[0] >= args.min_gap], reverse=True)[:args.show]
        lower = sorted([g for g in gaps if g[0] <= -args.min_gap])[:args.show]

        print(f"\n  YOU'RE HIGHER than the {site} room (they'll fall to you):")
        for d, mine, mkt, row in higher:
            print(f"    {row['Name']:<24} {row['Position']:<3} you {mine:>3} · market {mkt:>3}  (+{d})")
        print(f"\n  THE ROOM IS HIGHER than you (you won't see them at your number):")
        for d, mine, mkt, row in lower:
            print(f"    {row['Name']:<24} {row['Position']:<3} you {mine:>3} · market {mkt:>3}  ({d})")
        if missing:
            print(f"\n  IN YOUR TOP 150, NOT IN THE {site} MARKET SNAPSHOT:")
            for m in missing[:8]:
                print(f"    - {m}")


if __name__ == "__main__":
    main()
