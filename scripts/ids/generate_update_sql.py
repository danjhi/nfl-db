#!/usr/bin/env python3
"""Generate a SQL file with all player ID updates.

Reads matched JSON files and outputs a single .sql file that can be
executed in one Management API call or via the Supabase SQL editor.
"""

import json
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))
from shared import MATCHED_DIR, ROOT_DIR

ALL_ID_COLUMNS = [
    "gsis_id", "espn_id", "yahoo_id", "sleeper_id", "pfr_id", "rotowire_id",
    "pff_id", "fantasypros_id", "mfl_id", "stats_id", "stats_global_id",
    "fantasy_data_id", "cbs_id", "fleaflicker_id", "swish_id", "ktc_id",
    "cfbref_id", "rotoworld_id", "sportsdata_id", "footballguys_id",
    "fanduel_id", "draftkings_id", "underdog_id", "drafters_id",
]

SOURCES = [
    "nflreadr_ids.json",
    "sportsdata_ids.json",
    "sleeper_ids.json",
    "underdog_ids.json",
    "dk_ids.json",
    "drafters_ids.json",
    "fbg_ids.json",
]


def escape_sql(val):
    return val.replace("'", "''")


def main():
    merged = {}
    for src_file in SOURCES:
        path = os.path.join(MATCHED_DIR, src_file)
        if not os.path.exists(path):
            continue
        with open(path) as f:
            data = json.load(f)
        for player_id, updates in data.items():
            if player_id not in merged:
                merged[player_id] = {}
            for col, val in updates.items():
                if col in ALL_ID_COLUMNS and val and col not in merged[player_id]:
                    merged[player_id][col] = val

    # Generate SQL
    stmts = []
    for player_id, updates in merged.items():
        if not updates:
            continue
        set_clauses = ", ".join(
            f"{col} = '{escape_sql(str(val))}'" for col, val in updates.items()
        )
        stmts.append(
            f"UPDATE players SET {set_clauses} WHERE player_id = '{escape_sql(player_id)}';"
        )

    sql = "\n".join(stmts)
    out_path = os.path.join(ROOT_DIR, "data", "matched", "update_ids.sql")
    with open(out_path, "w") as f:
        f.write(sql)

    print(f"Generated {len(stmts)} UPDATE statements")
    print(f"SQL file size: {len(sql):,} bytes")
    print(f"Saved to {out_path}")


if __name__ == "__main__":
    main()
