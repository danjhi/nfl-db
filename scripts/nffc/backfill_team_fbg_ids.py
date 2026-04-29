#!/usr/bin/env python3
"""Backfill footballguys_id for NFL team-level entities (TDSP, TK) in players.

FBG uses PFR-style 3-character team codes followed by 'xxx99' for team defenses
and team kickers (e.g. 'minxxx99' for Minnesota's DST). Same prefix works for
both TDSP and TK. The mapping below was derived from existing rows in
nfl-db.players that already carry footballguys_id.

Usage:
    python3 backfill_team_fbg_ids.py --dry-run    # preview, no writes
    python3 backfill_team_fbg_ids.py              # apply updates
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(HERE, "..", "..", ".env")

with open(ENV_PATH) as f:
    _env = {k.strip(): v.strip() for k, v in
            (line.strip().split("=", 1) for line in f
             if "=" in line and not line.strip().startswith("#"))}

SB_URL = "https://twfzcrodldvhpfaykasj.supabase.co"
SB_KEY = _env.get("SUPABASE_SERVICE_ROLE_KEY") or _env["SUPABASE_ANON_KEY"]
SB_HEADERS = {
    "apikey": SB_KEY,
    "Authorization": f"Bearer {SB_KEY}",
    "Content-Type": "application/json",
}

# team_abbr (nflreadr standard) → FBG's PFR-style 3-char prefix
PFR = {
    "ARI": "crd",  # Cardinals
    "ATL": "atl",
    "BAL": "rav",  # Ravens
    "BUF": "buf",
    "CAR": "car",
    "CHI": "chi",
    "CIN": "cin",
    "CLE": "cle",
    "DAL": "dal",
    "DEN": "den",
    "DET": "det",
    "GB":  "gnb",
    "HOU": "htx",
    "IND": "clt",  # Colts
    "JAX": "jax",
    "KC":  "kan",
    "LAC": "sdg",  # legacy San Diego Chargers
    "LAR": "ram",  # legacy Rams
    "LV":  "rai",  # legacy Oakland Raiders
    "MIA": "mia",
    "MIN": "min",
    "NE":  "nwe",
    "NO":  "nor",
    "NYG": "nyg",
    "NYJ": "nyj",
    "PHI": "phi",
    "PIT": "pit",
    "SEA": "sea",
    "SF":  "sfo",
    "TB":  "tam",
    "TEN": "oti",  # legacy Oilers/Titans
    "WAS": "was",
}


def sb(path: str):
    req = urllib.request.Request(f"{SB_URL}/rest/v1/{path}",
                                  headers={"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}"})
    return json.loads(urllib.request.urlopen(req, timeout=30).read())


def sb_patch(path: str, body: dict):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{SB_URL}/rest/v1/{path}",
        data=data,
        headers={**SB_HEADERS, "Prefer": "return=minimal"},
        method="PATCH",
    )
    try:
        urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as e:
        body = (e.read() or b"").decode(errors="replace")[:400]
        raise SystemExit(f"PATCH failed: {e.code} {body}")


def resolve_team_abbr(name: str, name_to_abbr: dict) -> str | None:
    """Extract NFL team_abbr from a player 'name' like 'Los Angeles (LAR) Rams'
    or 'New Orleans Saints' or 'Kansas City Chiefs'.

    Returns None for junk entries like 'Holder Defense1', 'None Ghost', etc.
    """
    name = name.strip()

    # Parenthetical disambiguation: 'Los Angeles (LAR) Rams' → LAR
    m = re.search(r"\(([A-Z]{2,3})\)", name)
    if m:
        abbr = m.group(1)
        if abbr in PFR:
            return abbr

    # Try direct name match against teams.team_name
    for team_name, abbr in name_to_abbr.items():
        if team_name.lower() in name.lower():
            return abbr

    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # Load team_name → team_abbr map
    teams = sb("teams?select=team_abbr,team_name&limit=50")
    name_to_abbr = {t["team_name"]: t["team_abbr"] for t in teams}

    # Pull all TDSP + TK entries without fbg_id
    targets = []
    for pos in ("TDSP", "TK"):
        rows = sb(f"players?select=player_id,first_name,last_name,position,footballguys_id&"
                  f"position=eq.{pos}&footballguys_id=is.null&limit=100")
        targets.extend(rows)

    print(f"Found {len(targets)} TDSP/TK entries missing footballguys_id")

    resolved = []
    skipped = []
    for r in targets:
        full = f"{r['first_name']} {r['last_name']}".strip()
        abbr = resolve_team_abbr(full, name_to_abbr)
        if abbr:
            prefix = PFR[abbr]
            fbg_id = f"{prefix}xxx99"
            resolved.append((r, abbr, fbg_id))
        else:
            skipped.append(r)

    print(f"\nResolved: {len(resolved)}")
    for r, abbr, fbg_id in resolved:
        full = f"{r['first_name']} {r['last_name']}"
        print(f"  {r['position']:<4} {abbr:<4} → {fbg_id}   ({full})")

    print(f"\nSkipped (junk / unrecognized): {len(skipped)}")
    for r in skipped:
        full = f"{r['first_name']} {r['last_name']}"
        print(f"  {r['position']:<4} {full}")

    if args.dry_run:
        print(f"\nDRY RUN — no writes. Re-run without --dry-run to apply.")
        return

    print(f"\nApplying {len(resolved)} updates...")
    for r, abbr, fbg_id in resolved:
        pid_escaped = urllib.parse.quote(r['player_id'])
        sb_patch(f"players?player_id=eq.{pid_escaped}", {"footballguys_id": fbg_id})
    print(f"Done.")


if __name__ == "__main__":
    import urllib.parse
    main()
