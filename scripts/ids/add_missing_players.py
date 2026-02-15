#!/usr/bin/env python3
"""Add missing players from Underdog top 500 to the database.

Reads unmatched Underdog players, looks them up in nflreadr and SportsData.io,
and inserts them into the players table with all available IDs.
"""

import csv
import json
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    IMPORTS_DIR, MATCHED_DIR, NFLREADR_DIR, SUPABASE_PAT, PROJECT_REF,
    normalize_name, normalize_team, TEAM_FULLNAME_TO_ABBR,
    read_csv, ensure_dir
)

import urllib.request
import urllib.error
import time


def mgmt_query(sql):
    """Run SQL via Management API with retry."""
    url = f"https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query"
    data = json.dumps({"query": sql}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "Authorization": f"Bearer {SUPABASE_PAT}",
        "Content-Type": "application/json",
    })
    for attempt in range(5):
        try:
            resp = urllib.request.urlopen(req)
            return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 403):
                wait = 10 * (attempt + 1)
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                body = e.read().decode()
                print(f"    ERROR: {e.code} {body[:200]}")
                raise
    raise Exception("Rate limit exceeded")


def escape_sql(val):
    if val is None:
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"


def main():
    # Load Underdog CSV (full file, to get all data for unmatched)
    ud_csv = read_csv(os.path.join(IMPORTS_DIR, "underdog_ADP.csv"))
    ud_by_id = {r["id"]: r for r in ud_csv}
    print(f"Loaded {len(ud_csv)} Underdog players")

    # Load unmatched list
    unmatched_path = os.path.join(MATCHED_DIR, "underdog_unmatched.json")
    with open(unmatched_path) as f:
        unmatched = json.load(f)

    # Filter to top 500 by ADP
    top_unmatched = [u for u in unmatched if u.get("adp", 999) <= 500]
    top_unmatched.sort(key=lambda x: x.get("adp", 999))
    print(f"Unmatched Underdog players with ADP <= 500: {len(top_unmatched)}")

    # Load nflreadr for lookups
    nflreadr_rows = read_csv(os.path.join(NFLREADR_DIR, "ff_playerids.csv"))
    nr_by_name = {}
    for row in nflreadr_rows:
        norm = normalize_name(row.get("name", ""))
        pos = row.get("position", "").upper()
        key = (norm, pos)
        if norm:
            nr_by_name[key] = row
            nr_by_name[norm] = row  # fallback by name only
    print(f"Loaded {len(nflreadr_rows)} nflreadr player IDs")

    # Load SportsData cache
    sd_cache_path = os.path.join(MATCHED_DIR, "sportsdata_players_cache.json")
    sd_by_name = {}
    if os.path.exists(sd_cache_path):
        with open(sd_cache_path) as f:
            sd_players = json.load(f)
        for p in sd_players:
            name = f"{p.get('FirstName', '')} {p.get('LastName', '')}".strip()
            norm = normalize_name(name)
            pos = (p.get("Position") or "").upper()
            sd_by_name[(norm, pos)] = p
            sd_by_name[norm] = p
    print(f"Loaded {len(sd_players)} SportsData.io players")

    # Load DK and Drafters CSVs for additional IDs
    dk_rows = read_csv(os.path.join(IMPORTS_DIR, "DkPreDraftRankings.csv"))
    dk_by_name = {}
    for r in dk_rows:
        norm = normalize_name(r.get("Name", ""))
        pos = r.get("Position", "").upper()
        if norm:
            dk_by_name[(norm, pos)] = r.get("ID", "")
            dk_by_name[norm] = r.get("ID", "")

    dr_rows = read_csv(os.path.join(IMPORTS_DIR, "drafters_players.csv"))
    dr_by_name = {}
    for r in dr_rows:
        norm = normalize_name(r.get("name", ""))
        pos = r.get("position", "").upper()
        if norm:
            dr_by_name[(norm, pos)] = r.get("id", "").strip('"')
            dr_by_name[norm] = r.get("id", "").strip('"')

    # Build INSERT statements for missing players
    inserts = []
    for u in top_unmatched:
        ud_id = u.get("underdog_id", "")
        name = u.get("name", "")
        pos = u.get("pos", "")
        norm = normalize_name(name)

        # Try to find in nflreadr (gives us sportradar_id as player_id)
        nr = nr_by_name.get((norm, pos)) or nr_by_name.get(norm)
        sd = sd_by_name.get((norm, pos)) or sd_by_name.get(norm)

        # player_id: prefer sportradar_id from nflreadr
        player_id = None
        if nr:
            player_id = nr.get("sportradar_id", "").strip()

        if not player_id:
            # Use the Underdog ID as fallback player_id
            player_id = ud_id

        if not player_id:
            print(f"  SKIP: No player_id for {name}")
            continue

        # Split name
        parts = name.split(" ", 1)
        first_name = parts[0] if parts else ""
        last_name = parts[1] if len(parts) > 1 else ""

        # Gather team
        team = u.get("team", "")
        if not team and nr:
            team = normalize_team(nr.get("team", ""))
        if not team and sd:
            team = normalize_team(sd.get("Team") or "")

        # Gather IDs from all sources
        ids = {
            "player_id": player_id,
            "first_name": first_name,
            "last_name": last_name,
            "position": pos,
            "latest_team": team or None,
            "underdog_id": ud_id,
        }

        # nflreadr IDs
        if nr:
            id_map = {
                "gsis_id": "gsis_id", "espn_id": "espn_id", "yahoo_id": "yahoo_id",
                "sleeper_id": "sleeper_id", "pfr_id": "pfr_id", "rotowire_id": "rotowire_id",
                "pff_id": "pff_id", "fantasypros_id": "fantasypros_id", "mfl_id": "mfl_id",
                "stats_id": "stats_id", "stats_global_id": "stats_global_id",
                "fantasy_data_id": "fantasy_data_id", "cbs_id": "cbs_id",
                "fleaflicker_id": "fleaflicker_id", "swish_id": "swish_id",
                "ktc_id": "ktc_id", "cfbref_id": "cfbref_id", "rotoworld_id": "rotoworld_id",
            }
            for csv_col, db_col in id_map.items():
                val = nr.get(csv_col, "").strip()
                if val and val != "NA":
                    ids[db_col] = val

            # Bio data from nflreadr
            bd = nr.get("birthdate", "").strip()
            if bd and bd != "NA" and not bd.startswith("0000"):
                ids["birth_date"] = bd
            col = nr.get("college", "").strip()
            if col and col != "NA":
                ids["college"] = col
            for field in ["draft_year", "draft_round", "draft_ovr"]:
                val = nr.get(field, "").strip()
                if val and val != "NA":
                    db_field = "draft_pick" if field == "draft_ovr" else field
                    ids[db_field] = val

        # SportsData IDs
        if sd:
            sd_id = sd.get("PlayerID")
            if sd_id:
                ids["sportsdata_id"] = str(sd_id)
            fd_id = sd.get("FanDuelPlayerID")
            if fd_id:
                ids["fanduel_id"] = str(fd_id)
            dk_id_sd = sd.get("DraftKingsPlayerID")
            if dk_id_sd:
                ids["draftkings_id"] = str(dk_id_sd)

        # DK CSV ID
        dk_id = dk_by_name.get((norm, pos)) or dk_by_name.get(norm)
        if dk_id and "draftkings_id" not in ids:
            ids["draftkings_id"] = dk_id

        # Drafters CSV ID
        dr_id = dr_by_name.get((norm, pos)) or dr_by_name.get(norm)
        if dr_id:
            ids["drafters_id"] = dr_id

        inserts.append(ids)

    print(f"\nPrepared {len(inserts)} players for insertion")

    # Generate SQL INSERT statements
    if not inserts:
        print("Nothing to insert.")
        return

    # All possible columns
    all_cols = sorted(set(col for ins in inserts for col in ins.keys()))

    stmts = []
    for ins in inserts:
        cols = []
        vals = []
        for col in all_cols:
            if col in ins and ins[col] is not None:
                cols.append(col)
                val = ins[col]
                if col in ("draft_year", "draft_round", "draft_pick"):
                    vals.append(str(int(float(val))))
                else:
                    vals.append(escape_sql(val))

        stmt = f"INSERT INTO players ({', '.join(cols)}) VALUES ({', '.join(vals)}) ON CONFLICT (player_id) DO NOTHING;"
        stmts.append(stmt)

    # Save SQL
    sql_path = os.path.join(MATCHED_DIR, "insert_missing_players.sql")
    with open(sql_path, "w") as f:
        f.write("\n".join(stmts))
    print(f"Saved {len(stmts)} INSERT statements to {sql_path}")

    # Also save a summary
    print("\nFirst 20 players to be added:")
    for ins in inserts[:20]:
        name = f"{ins.get('first_name', '')} {ins.get('last_name', '')}"
        pos = ins.get('position', '') or ''
        team = ins.get('latest_team', '') or ''
        id_count = sum(1 for k, v in ins.items() if k.endswith('_id') and v)
        print(f"  {name:30s} {pos:3s} {team:4s} - {id_count} IDs")


if __name__ == "__main__":
    main()
