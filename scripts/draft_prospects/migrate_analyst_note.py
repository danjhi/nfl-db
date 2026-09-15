"""Add Mike Kashuba's report columns to draft_prospects (2026-09-15).

analyst_note / analyst_note_date / analyst_name hold the dated note from his
shared prospect sheet, written by sync_kashuba_sheet.py and shown on the DTVC
Plus prospect card. Additive and idempotent (ADD COLUMN IF NOT EXISTS); the
DTVC API falls back to its old column list while these are missing, so the
order of migration vs deploy does not matter.

Same connection recipe as scripts/dfs/apply_schema.py: the Management API is
Cloudflare-blocked, so DDL goes straight to Postgres (direct host, then the
IPv4 session pooler).

    .venv/bin/python3 scripts/draft_prospects/migrate_analyst_note.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import socket
import sys

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_here, "..", "ids"))
from shared import PROJECT_REF  # noqa: E402  (loads .env)

import pg8000.dbapi  # noqa: E402

DDL = """
ALTER TABLE draft_prospects
  ADD COLUMN IF NOT EXISTS analyst_note      TEXT,
  ADD COLUMN IF NOT EXISTS analyst_note_date DATE,
  ADD COLUMN IF NOT EXISTS analyst_name      TEXT;
"""
DB_PASSWORD = os.environ.get("SUPABASE_DB_PASSWORD", "")


def connect():
    attempts = [
        dict(host=f"db.{PROJECT_REF}.supabase.co", port=5432, user="postgres"),
        dict(host="aws-0-us-west-2.pooler.supabase.com", port=5432, user=f"postgres.{PROJECT_REF}"),
    ]
    last = None
    for a in attempts:
        try:
            return pg8000.dbapi.connect(database="postgres", password=DB_PASSWORD, timeout=20, **a)
        except (OSError, socket.gaierror, Exception) as e:  # noqa: BLE001
            last = e
            print(f"  connect {a['host']} failed: {e}")
    raise SystemExit(f"could not connect: {last}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if args.dry_run:
        print(DDL)
        return
    if not DB_PASSWORD:
        raise SystemExit("SUPABASE_DB_PASSWORD missing from .env")
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(DDL)
        conn.commit()
        cur.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_name = 'draft_prospects' and column_name like 'analyst%' order by 1"
        )
        print("columns:", cur.fetchall())
        cur.execute("NOTIFY pgrst, 'reload schema'")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
