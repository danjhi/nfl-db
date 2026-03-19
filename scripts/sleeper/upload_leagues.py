"""Upload 2026 dynasty leagues from sleeper.db → Supabase sleeper_leagues."""

import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from shared import SLEEPER_DB_PATH, supa_upsert

def main():
    print(f"Reading leagues from {SLEEPER_DB_PATH}")
    conn = sqlite3.connect(SLEEPER_DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT * FROM sleeper_leagues WHERE is_dynasty = 1 AND season = 2026"
    ).fetchall()
    conn.close()

    print(f"  Found {len(rows):,} dynasty 2026 leagues")

    # Map SQLite rows → Supabase payload (skip columns not in Supabase schema)
    payload = []
    for r in rows:
        payload.append({
            "league_id": r["league_id"],
            "season": r["season"],
            "name": r["name"],
            "total_rosters": r["total_rosters"],
            "status": r["status"],
            "is_dynasty": bool(r["is_dynasty"]),
            "is_superflex": bool(r["is_superflex"]),
            "is_tep": bool(r["is_tep"]),
            "is_idp": bool(r["is_idp"]),
            "ppr_type": r["ppr_type"],
            "rec_ppr": r["rec_ppr"],
            "te_premium": r["te_premium"],
            "pass_td_pts": r["pass_td_pts"],
            "starter_qb": r["starter_qb"],
            "starter_rb": r["starter_rb"],
            "starter_wr": r["starter_wr"],
            "starter_te": r["starter_te"],
            "starter_flex": r["starter_flex"],
            "starter_super_flex": r["starter_super_flex"],
            "bench_count": r["bench_count"],
            "taxi_slots": r["taxi_slots"],
            "draft_rounds": r["draft_rounds"],
            "pick_trading": bool(r["pick_trading"]),
        })

    print(f"Upserting {len(payload):,} leagues to Supabase...")
    n = supa_upsert("sleeper_leagues", payload, on_conflict="league_id")
    print(f"  Done: {n:,} leagues upserted")


if __name__ == "__main__":
    main()
