"""Export a full-join CSV of dynasty values + today's Underdog ADP.

Output: data/dynasty_values_with_adp.csv
"""

import csv
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ids"))
from shared import SUPABASE_URL, SUPABASE_SERVICE_KEY, SUPABASE_KEY

key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def fetch_all(url_base):
    rows = []
    offset = 0
    while True:
        url = f"{url_base}&offset={offset}&limit=1000"
        req = urllib.request.Request(url, headers={
            "apikey": key, "Authorization": f"Bearer {key}",
        })
        batch = json.loads(urllib.request.urlopen(req).read().decode())
        if not batch:
            break
        rows.extend(batch)
        offset += 1000
    return rows


def main():
    # Fetch dynasty values with player info
    dv_rows = fetch_all(
        f"{SUPABASE_URL}/rest/v1/dynasty_values"
        f"?select=player_id,value,sf_value,"
        f"players(dan_id,first_name,last_name,position,latest_team,draft_year)"
    )
    print(f"Dynasty values: {len(dv_rows)}")

    # Fetch today's Underdog ADP
    import datetime
    today = datetime.date.today().isoformat()
    adp_rows = fetch_all(
        f"{SUPABASE_URL}/rest/v1/adp_sources"
        f"?select=player_id,adp,"
        f"players(first_name,last_name,position,latest_team,draft_year)"
        f"&source=eq.underdog&date=eq.{today}"
    )
    print(f"Underdog ADP rows ({today}): {len(adp_rows)}")

    # Build lookups
    dv_by_pid = {}
    for r in dv_rows:
        p = r["players"]
        dv_by_pid[r["player_id"]] = {
            "dan_id": p.get("dan_id") or "",
            "Player": f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
            "Team": p.get("latest_team") or "",
            "Position": p.get("position") or "",
            "Rookie": "TRUE" if (p.get("draft_year") or 0) >= 2025 else "FALSE",
            "Value": r["value"],
            "SF_Value": r["sf_value"] or "",
        }

    adp_by_pid = {}
    for r in adp_rows:
        p = r["players"]
        adp_by_pid[r["player_id"]] = {
            "adp": r["adp"],
            "Player": f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
            "Team": p.get("latest_team") or "",
            "Position": p.get("position") or "",
            "Rookie": "TRUE" if (p.get("draft_year") or 0) >= 2025 else "FALSE",
        }

    # Full outer join
    all_pids = set(dv_by_pid.keys()) | set(adp_by_pid.keys())
    out_rows = []

    for pid in all_pids:
        dv = dv_by_pid.get(pid, {})
        adp = adp_by_pid.get(pid, {})
        out_rows.append({
            "dan_id": dv.get("dan_id", ""),
            "Player": dv.get("Player") or adp.get("Player", ""),
            "Team": dv.get("Team") or adp.get("Team", ""),
            "Position": dv.get("Position") or adp.get("Position", ""),
            "Rookie": dv.get("Rookie") or adp.get("Rookie", ""),
            "Value": dv.get("Value", ""),
            "SF_Value": dv.get("SF_Value", ""),
            "Underdog_ADP": adp.get("adp", ""),
        })

    # Sort: Value desc (blanks last), then ADP asc (blanks last)
    def sort_key(r):
        v = float(r["Value"]) if r["Value"] not in ("", None) else -1
        a = float(r["Underdog_ADP"]) if r["Underdog_ADP"] not in ("", None) else 9999
        return (-v, a)

    out_rows.sort(key=sort_key)

    # Write CSV
    out_path = os.path.join(ROOT, "data", "dynasty_values_with_adp.csv")
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "dan_id", "Player", "Team", "Position", "Rookie",
            "Value", "SF_Value", "Underdog_ADP",
        ])
        w.writeheader()
        w.writerows(out_rows)

    both = sum(1 for r in out_rows if r["Value"] not in ("", None) and r["Underdog_ADP"] not in ("", None))
    dv_only = sum(1 for r in out_rows if r["Value"] not in ("", None) and r["Underdog_ADP"] in ("", None))
    adp_only = sum(1 for r in out_rows if r["Value"] in ("", None) and r["Underdog_ADP"] not in ("", None))

    print(f"\nWritten {len(out_rows)} rows to {out_path}")
    print(f"  Both dynasty value + ADP: {both}")
    print(f"  Dynasty value only (no ADP): {dv_only}")
    print(f"  ADP only (new for sheet):  {adp_only}")


if __name__ == "__main__":
    main()
