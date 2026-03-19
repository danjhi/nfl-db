"""Upload trades + trade assets from sleeper.db → Supabase."""

import json
import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from shared import SLEEPER_DB_PATH, supa_upsert, supa_delete, supa_batch_insert

BATCH_SIZE = 500


def main():
    print(f"Reading trades from {SLEEPER_DB_PATH}")
    conn = sqlite3.connect(SLEEPER_DB_PATH)
    conn.row_factory = sqlite3.Row

    trades = conn.execute("SELECT * FROM sleeper_trades").fetchall()
    assets = conn.execute(
        "SELECT * FROM sleeper_trade_assets ORDER BY id"
    ).fetchall()
    conn.close()

    print(f"  Found {len(trades):,} trades, {len(assets):,} trade assets")

    # ── Trades ────────────────────────────────────────────────────────────
    trade_payload = []
    for t in trades:
        trade_payload.append({
            "transaction_id": t["transaction_id"],
            "league_id": t["league_id"],
            "season": t["season"],
            "week": t["week"],
            "created_ms": t["created_ms"],
            "roster_ids": json.loads(t["roster_ids"]) if isinstance(t["roster_ids"], str) else t["roster_ids"],
            "consenter_ids": json.loads(t["consenter_ids"]) if isinstance(t["consenter_ids"], str) else t["consenter_ids"],
        })

    print(f"Upserting {len(trade_payload):,} trades...")
    n = supa_upsert("sleeper_trades", trade_payload, on_conflict="transaction_id")
    print(f"  Done: {n:,} trades upserted")

    # ── Trade assets ──────────────────────────────────────────────────────
    # Delete all existing assets then re-insert (no natural PK for upsert)
    print(f"Deleting existing trade assets...")
    supa_delete("sleeper_trade_assets", "id=gt.0")

    asset_payload = []
    for a in assets:
        asset_payload.append({
            "transaction_id": a["transaction_id"],
            "receiving_roster_id": a["receiving_roster_id"],
            "asset_type": a["asset_type"],
            "sleeper_player_id": a["sleeper_player_id"],
            "pick_season": a["pick_season"],
            "pick_round": a["pick_round"],
            "pick_original_roster_id": a["pick_original_roster_id"],
            "pick_slot": a["pick_slot"],
        })

    print(f"Inserting {len(asset_payload):,} trade assets...")
    n = supa_batch_insert("sleeper_trade_assets", asset_payload)
    print(f"  Done: {n:,} trade assets inserted")


if __name__ == "__main__":
    main()
