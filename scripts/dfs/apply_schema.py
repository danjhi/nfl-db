"""Apply scripts/dfs/schema.sql over a direct Postgres connection (pg8000).

Same recipe as scripts/ids/migrate_rookie_player_ids.py: the Supabase Management
API is Cloudflare-blocked, so DDL goes straight to the database. IPv6-only direct
host first, session pooler (IPv4) as the fallback.

    .venv/bin/python3 scripts/dfs/apply_schema.py [--dry-run]
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

SCHEMA = os.path.join(_here, "schema.sql")
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
    sql = open(SCHEMA).read()
    if args.dry_run:
        print(sql)
        return
    if not DB_PASSWORD:
        raise SystemExit("SUPABASE_DB_PASSWORD missing from .env")
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.execute("select table_name from information_schema.tables where table_name in ('dfs_slates','dfs_salaries') order by 1")
        print("tables:", [r[0] for r in cur.fetchall()])
        cur.execute("NOTIFY pgrst, 'reload schema'")
        conn.commit()
    finally:
        conn.close()
    print("schema applied")


if __name__ == "__main__":
    main()
