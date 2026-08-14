"""Pull DK's MARKET order (rank field = ADP-ordered default board) via the API.

WARNING — NOT Dan's rankings. The playerpool `rank` field tracks DK's default
(ADP) order, not the user's saved drag-and-drop board: on 2026-08-14 it put
Pitts at 89 (his ADP 89.7, Dan's rank 78) and Henderson at 61 (ADP 61.8,
Dan's rank 74). Dan caught this before drafting on CSVs built from it. The
diff-vs-his-CSV "verification" that day was fooled because personal rankings
correlate with ADP. His board is only available via the manual CSV download
(DkPreDraftRankings) — which stays step 1 of the tandem loop.

Kept for market-order pulls only. Output filename deliberately does NOT match
tandem_from_dk.py's DkPreDraftRankings* glob.

Usage:
    python3 scripts/rankings/pull_dk_master.py
"""

import csv
import datetime
import json
import os
import sys
import urllib.request

_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
sys.path.insert(0, os.path.join(_script_dir, "..", "adp"))
from shared import SUPABASE_URL, SUPABASE_SERVICE_KEY  # noqa: E402
import fetch_draftkings_postdraft_adp as dk  # noqa: E402

OUT = os.path.expanduser(
    f"~/Downloads/DkMarketOrder_{datetime.date.today().isoformat()}.csv")


def main():
    players = dk.fetch_dk_players()
    players.sort(key=lambda p: p["rank"])
    print(f"  {len(players)} players in DK MARKET order (rank field = ADP order,")
    print("  NOT your saved rankings — use the DkPreDraftRankings download for those)")

    req = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/players?draftkings_id=not.is.null"
        f"&select=draftkings_id,position,latest_team&limit=3000",
        headers={"apikey": SUPABASE_SERVICE_KEY,
                 "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        xwalk = {str(r["draftkings_id"]): r for r in json.loads(resp.read())}

    with open(OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ID", "Name", "Position", "ADP", "Team"])
        for p in players:
            pid = str(p["playerId"])
            x = xwalk.get(pid, {})
            slots = p.get("draftableRosterPositions") or []
            slot_pos = ""
            if slots:
                s0 = slots[0]
                # API returns dicts ({draftableId, teamPositionId, ...}), not strings
                slot_pos = s0 if isinstance(s0, str) else dk.POSITION_MAP.get(s0.get("teamPositionId"), "")
            pos = x.get("position") or slot_pos
            w.writerow([pid, p["displayName"], pos,
                        p.get("averageDraftPosition") or "", x.get("latest_team") or ""])
    print(f"  Wrote {OUT}")


if __name__ == "__main__":
    main()
