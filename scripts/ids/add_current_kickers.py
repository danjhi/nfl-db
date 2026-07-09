#!/usr/bin/env python3
"""Add current starting kickers to the players table with footballguys_id.

The players table was built for best ball / dynasty, where kickers barely
matter, so its PK set is stale: it carries retired kickers but is missing the
current starters that show up in every ADP feed (Aubrey, McPherson, Dicker…).
Those orphan on the footballguys.com/adp page because there's no players row to
match the scraper's adp_sources value, and no footballguys_id to join FBG's
feed. This script adds them.

For each kicker: look it up in the Sleeper player DB (authoritative for current
team + cross-source IDs), insert a players row keyed on the Sportradar id
(canonical player_id), position "PK", latest_team from Sleeper, and
footballguys_id from FBG's `api/nfl/2026/players` pos="pk" list.

Notes:
  - Sleeper stores kickers as position "K"; we store "PK" (nfl-db convention).
    Matching is done among Sleeper's K-position players only.
  - The DB display name matches the ADP feeds (e.g. RTSports "Andres
    Borregales"); Sleeper/FBG call him "Andy". See scripts/ids/shared.py
    PLAYER_ALIASES for the name-only-match bridge.
  - footballguys_id is the load-bearing column for the ADP fixture join — see
    docs/adp-kicker-defense-join.md.

Usage:
    python3 scripts/ids/add_current_kickers.py --dry-run   # preview, no writes
    python3 scripts/ids/add_current_kickers.py             # insert
"""

import json
import os
import sys
import urllib.error
import urllib.request
import uuid

sys.path.insert(0, os.path.dirname(__file__))
from shared import (  # noqa: E402
    SUPABASE_URL, SUPABASE_SERVICE_KEY,
    normalize_name, normalize_team,
    get_all_players, build_player_lookup,
)

# name (as ADP feeds spell it) → FBG pk id + expected team (for a sanity log).
# sleeper_name overrides the Sleeper lookup key when it differs from `name`.
KICKERS = [
    {"name": "Brandon Aubrey",    "fbg": "AubrBr00", "team": "DAL"},
    {"name": "Cameron Dicker",    "fbg": "DickCa44", "team": "LAC"},
    {"name": "Cam Little",        "fbg": "LittCa00", "team": "JAX"},
    {"name": "Jake Bates",        "fbg": "BateJa00", "team": "DET"},
    {"name": "Tyler Loop",        "fbg": "LoopTy00", "team": "BAL"},
    {"name": "Harrison Mevis",    "fbg": "MeviHa44", "team": "LA"},
    {"name": "Andres Borregales", "fbg": "BorrAn00", "team": "NE",
     "sleeper_name": "Andy Borregales"},
    {"name": "Will Reichard",     "fbg": "ReicWi44", "team": "MIN"},
    {"name": "Evan McPherson",    "fbg": "McPhEv44", "team": "CIN"},
]

DRY_RUN = "--dry-run" in sys.argv


def fetch_sleeper_db():
    url = "https://api.sleeper.app/v1/players/nfl"
    print(f"Fetching Sleeper player DB from {url}...")
    req = urllib.request.Request(url, headers={"User-Agent": "nfl-db/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    print(f"  Fetched {len(data)} entries from Sleeper")
    return data


def build_kicker_index(sleeper_db):
    """Index Sleeper kickers (position K/PK) by normalized name."""
    by_name = {}
    for sleeper_id, p in sleeper_db.items():
        if not isinstance(p, dict):
            continue
        if (p.get("position") or "").upper() not in ("K", "PK"):
            continue
        full = f"{p.get('first_name') or ''} {p.get('last_name') or ''}".strip()
        norm = normalize_name(full)
        if not norm:
            continue
        p["_sleeper_id"] = sleeper_id
        by_name.setdefault(norm, []).append(p)
    return by_name


def build_player_row(kicker, sleeper_p):
    """Build the players insert dict for a kicker."""
    name = kicker["name"]
    parts = name.split(" ", 1)
    row = {
        "first_name": parts[0],
        "last_name": parts[1] if len(parts) > 1 else "",
        "position": "PK",
        "footballguys_id": kicker["fbg"],
    }

    if sleeper_p:
        row["sleeper_id"] = sleeper_p["_sleeper_id"]
        row["latest_team"] = normalize_team(sleeper_p.get("team") or "") or None
        for sk, dk in [
            ("espn_id", "espn_id"), ("yahoo_id", "yahoo_id"),
            ("fantasy_data_id", "fantasy_data_id"), ("rotowire_id", "rotowire_id"),
            ("rotoworld_id", "rotoworld_id"), ("stats_id", "stats_id"),
            ("gsis_id", "gsis_id"), ("swish_id", "swish_id"), ("pff_id", "pff_id"),
        ]:
            v = sleeper_p.get(sk)
            if v:
                row[dk] = str(v)
        bd = sleeper_p.get("birth_date")
        if bd and not str(bd).startswith("0000"):
            row["birth_date"] = bd
        for sk, dk in [("college", "college"), ("height", "height"), ("status", "status")]:
            if sleeper_p.get(sk):
                row[dk] = sleeper_p[sk]
        w = sleeper_p.get("weight")
        if w:
            try:
                row["weight"] = int(w)
            except (ValueError, TypeError):
                pass
        row["player_id"] = sleeper_p.get("sportradar_id") or str(uuid.uuid4())
    else:
        row["latest_team"] = normalize_team(kicker["team"]) or None
        row["player_id"] = str(uuid.uuid4())

    return row


def supabase_post(rows):
    url = f"{SUPABASE_URL}/rest/v1/players"
    data = json.dumps(rows).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }, method="POST")
    try:
        urllib.request.urlopen(req, timeout=30)
        return True, None
    except urllib.error.HTTPError as e:
        return False, f"{e.code}: {e.read().decode()[:300]}"


def main():
    if DRY_RUN:
        print("=== DRY RUN — no DB writes ===\n")

    sleeper_db = fetch_sleeper_db()
    kicker_idx = build_kicker_index(sleeper_db)

    print("Fetching existing players from Supabase...")
    existing = get_all_players()
    db_by_name_pos, db_by_name = build_player_lookup(existing)
    print(f"  {len(existing)} players in DB\n")

    inserted = 0
    skipped_existing = 0
    failures = []

    for k in KICKERS:
        name = k["name"]
        norm = normalize_name(name)

        # Skip if already present (by name, any position).
        if db_by_name.get(norm) or db_by_name_pos.get((norm, "PK")):
            print(f"  EXISTS: {name:20s} — skipping")
            skipped_existing += 1
            continue

        # Match Sleeper among kickers.
        lookup = normalize_name(k.get("sleeper_name", name))
        cands = kicker_idx.get(lookup, [])
        match = None
        if len(cands) == 1:
            match = cands[0]
        elif len(cands) > 1:
            tm = normalize_team(k["team"])
            tmatch = [c for c in cands if normalize_team(c.get("team") or "") == tm]
            match = tmatch[0] if tmatch else cands[0]

        row = build_player_row(k, match)
        pid_kind = "sportradar" if (match and match.get("sportradar_id")) else "uuid"
        if match:
            st = normalize_team(match.get("team") or "")
            note = f"  (Sleeper team={st}, expected={normalize_team(k['team'])})" if st != normalize_team(k["team"]) else ""
            print(f"  ADD: {name:20s} PK {row['latest_team']:<4} fbg={k['fbg']} "
                  f"sleeper_id={match['_sleeper_id']} pid={pid_kind}{note}")
        else:
            print(f"  ADD (NO SLEEPER MATCH): {name:20s} PK {row['latest_team']} fbg={k['fbg']} pid=uuid")

        if DRY_RUN:
            inserted += 1
            continue

        ok, err = supabase_post([row])
        if ok:
            inserted += 1
        else:
            print(f"    INSERT failed: {err}")
            failures.append((name, err))

    print("\n=== Summary ===")
    print(f"{'Would insert' if DRY_RUN else 'Inserted'}: {inserted}")
    print(f"Already existed (skipped): {skipped_existing}")
    print(f"Failed: {len(failures)}")
    for name, err in failures:
        print(f"  - {name}: {err}")


if __name__ == "__main__":
    main()
