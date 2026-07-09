#!/usr/bin/env python3
"""Backfill footballguys_id for NFL team-level entities in players.

FBG uses PFR-style 3-character team codes followed by 'xxx99' for team defenses
and team kickers (e.g. 'minxxx99' for Minnesota's DST). Same prefix works for
both TDSP and TK. The mapping below was derived from existing rows in
nfl-db.players that already carry footballguys_id, and cross-checked against
FBG's live `api/nfl/2026/players` pos="td" feed (all 32 match exactly).

Two row sets get patched:
  - TDSP / TK  — NFFC holder/team artifact rows; team resolved from the display
    name (e.g. "Los Angeles (LAR) Rams" -> LAR).
  - DEF_{TEAM} — the clean team-defense rows the ADP page joins on (player_id
    like "DEF_HOU"); team resolved from the player_id suffix. Backfilling these
    is what makes the footballguys.com/adp page's defenses join (see
    docs/adp-kicker-defense-join.md).

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
import urllib.parse
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


def backfill_def_rows(dry_run: bool):
    """Make each clean DEF_{TEAM} row the UNIQUE owner of its FBG defense id.

    Team abbr comes straight off the player_id suffix ("DEF_HOU" -> "HOU"),
    which matches the PFR dict keys (and FBG's own team abbr, incl. LAR for the
    Rams). FBG defense id = "{pfr3}xxx99".

    Two steps, both idempotent:
      1. Set footballguys_id on every DEF_{TEAM} row whose value differs.
      2. Clear footballguys_id from any NON-DEF row that still carries one of
         those ids (legacy NFFC TDSP artifact rows were backfilled with the same
         "{pfr3}xxx99" scheme). The ADP fixture maps footballguys_id ->
         player_id last-write-wins, so a colliding artifact could hijack a real
         defense's id (htxxxx99 -> a "Houston Texans" TDSP row instead of
         DEF_HOU). Those artifact rows are kept (they're referenced by historical
         adp / draft_picks / player_projections) — only the duplicate id clears.
    """
    rows = sb("players?select=player_id,first_name,last_name,footballguys_id&"
              "position=eq.DEF&limit=100")
    print(f"\n=== DEF_{{TEAM}} team-defense rows: {len(rows)} ===")

    resolved = []
    skipped = []
    for r in rows:
        pid = r["player_id"]
        abbr = pid[4:] if pid.startswith("DEF_") else None
        prefix = PFR.get(abbr) if abbr else None
        if prefix:
            resolved.append((r, abbr, f"{prefix}xxx99"))
        else:
            skipped.append(r)

    to_set = [(r, abbr, fbg_id) for r, abbr, fbg_id in resolved
              if r["footballguys_id"] != fbg_id]
    print(f"  Need footballguys_id set/changed: {len(to_set)} "
          f"(already correct: {len(resolved) - len(to_set)})")
    for r, abbr, fbg_id in to_set:
        name = f"{r['first_name']} {r['last_name']}"
        print(f"    {r['player_id']:<10} {abbr:<4} {r['footballguys_id']} → {fbg_id}   ({name})")
    if skipped:
        print(f"  Skipped (no PFR mapping): {len(skipped)}")
        for r in skipped:
            print(f"    {r['player_id']}  {r['first_name']} {r['last_name']}")

    # Step 2: find non-DEF rows colliding on any DEF id.
    def_ids = sorted({fbg_id for _, _, fbg_id in resolved})
    collisions = []
    if def_ids:
        holders = sb("players?select=player_id,first_name,last_name,position,footballguys_id&"
                     "footballguys_id=in.(%s)&limit=500" % ",".join(def_ids))
        collisions = [h for h in holders if not h["player_id"].startswith("DEF_")]
    print(f"  Non-DEF rows colliding on a DEF id (will clear): {len(collisions)}")
    for h in collisions:
        print(f"    {h['position']:<5} {h['footballguys_id']} ✗  "
              f"{h['first_name']} {h['last_name']} ({h['player_id']})")

    if dry_run:
        return
    print(f"\n  Applying {len(to_set)} DEF sets + {len(collisions)} collision clears...")
    for r, abbr, fbg_id in to_set:
        pid_escaped = urllib.parse.quote(r["player_id"])
        sb_patch(f"players?player_id=eq.{pid_escaped}", {"footballguys_id": fbg_id})
    for h in collisions:
        pid_escaped = urllib.parse.quote(h["player_id"])
        sb_patch(f"players?player_id=eq.{pid_escaped}", {"footballguys_id": None})
    print("  Done.")


def backfill_artifact_rows(dry_run: bool):
    """Patch footballguys_id on the NFFC TDSP / TK artifact rows.

    WARNING: these resolve to the same "{pfr3}xxx99" ids as the clean DEF_{TEAM}
    rows, so patching them makes footballguys_id non-unique. The ADP fixture
    builder maps footballguys_id -> player_id last-write-wins, so a colliding
    artifact row could hijack a real defense's id (e.g. htxxxx99 -> a "Holder"
    row instead of DEF_HOU). Off by default; only enable with --include-artifacts
    if you know downstream consumers don't key on footballguys_id.
    """
    # Load team_name → team_abbr map
    teams = sb("teams?select=team_abbr,team_name&limit=50")
    name_to_abbr = {t["team_name"]: t["team_abbr"] for t in teams}

    targets = []
    for pos in ("TDSP", "TK"):
        rows = sb(f"players?select=player_id,first_name,last_name,position,footballguys_id&"
                  f"position=eq.{pos}&footballguys_id=is.null&limit=100")
        targets.extend(rows)

    print(f"\n=== TDSP/TK artifact rows missing footballguys_id: {len(targets)} ===")

    resolved = []
    skipped = []
    for r in targets:
        full = f"{r['first_name']} {r['last_name']}".strip()
        abbr = resolve_team_abbr(full, name_to_abbr)
        if abbr:
            resolved.append((r, abbr, f"{PFR[abbr]}xxx99"))
        else:
            skipped.append(r)

    print(f"  Resolved: {len(resolved)}  (collide with DEF_{{TEAM}} ids)")
    for r, abbr, fbg_id in resolved:
        full = f"{r['first_name']} {r['last_name']}"
        print(f"    {r['position']:<4} {abbr:<4} → {fbg_id}   ({full})")
    print(f"  Skipped (junk / unrecognized): {len(skipped)}")
    for r in skipped:
        print(f"    {r['position']:<4} {r['first_name']} {r['last_name']}")

    if dry_run:
        return
    print(f"\n  Applying {len(resolved)} artifact updates...")
    for r, abbr, fbg_id in resolved:
        pid_escaped = urllib.parse.quote(r["player_id"])
        sb_patch(f"players?player_id=eq.{pid_escaped}", {"footballguys_id": fbg_id})
    print("  Done.")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="preview, no writes")
    ap.add_argument("--include-artifacts", action="store_true",
                    help="also patch NFFC TDSP/TK artifact rows (UNSAFE: collides "
                         "with DEF_{TEAM} footballguys_id — see docstring)")
    args = ap.parse_args()

    # Clean DEF_{TEAM} rows — the ADP-page join target (highest leverage).
    backfill_def_rows(dry_run=args.dry_run)

    if args.include_artifacts:
        backfill_artifact_rows(dry_run=args.dry_run)

    if args.dry_run:
        print("\nDRY RUN — no writes. Re-run without --dry-run to apply.")


if __name__ == "__main__":
    main()
