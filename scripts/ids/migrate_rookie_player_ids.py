#!/usr/bin/env python3
"""Migrate 2026 rookie player_ids to canonical sportradar_id (from Sleeper).

For each rookie whose player_id is currently an Underdog UUID or a generated
UUID but whose Sleeper record now exposes a sportradar_id, swap the player_id
to that sportradar UUID — same player, canonical key.

Why: NFFC and Sportradar use sportradar_id as their player key. Mixed keys
break joins on the eventual NFFC integration.

Per-player transactional steps (pg8000):
  1. UPDATE players SET dan_id = NULL WHERE player_id = old_pid
     (frees the unique partial index on dan_id so step 2 doesn't violate)
  2. INSERT INTO players (...) VALUES (...) — full copy with new player_id
  3. UPDATE FK refs in dynasty_values, player_notes, adp_sources, etc.
  4. DELETE old players row
  5. ROLLBACK on any error inside the transaction

After DB migration succeeds for a player:
  6. Replace old player_id in vault YAML frontmatter (~/obsidian-vault/.../Players/*.md)
  7. Replace old player_id in data/writeups/player_writeups.yaml
  8. Append to data/migrations/player_id_remap_<date>.json (audit trail)

Usage:
  python3 scripts/ids/migrate_rookie_player_ids.py --dry-run
  python3 scripts/ids/migrate_rookie_player_ids.py
"""

import json
import os
import re
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from shared import ROOT_DIR, SUPABASE_KEY, SUPABASE_URL  # noqa: E402

import pg8000.dbapi  # noqa: E402

DB_PASSWORD = os.environ.get("SUPABASE_DB_PASSWORD", "")
DB_HOST = "db.twfzcrodldvhpfaykasj.supabase.co"
DB_PORT = 5432
DB_USER = "postgres"
DB_NAME = "postgres"

VAULT_PLAYERS_DIR = "/Users/dan/obsidian-vault/Fantasy Football/Players"
WRITEUPS_YAML = os.path.join(ROOT_DIR, "data", "writeups", "player_writeups.yaml")
MIGRATIONS_DIR = os.path.join(ROOT_DIR, "data", "migrations")

FK_TABLES = [
    "dynasty_values",
    "dynasty_value_history",
    "player_projections",
    "adp_sources",
    "player_notes",
    "news_items",
    "adp",
    "draft_picks",
    "player_seasons",
    "player_stats",
]

DRY_RUN = "--dry-run" in sys.argv


def fetch_sleeper_db():
    print("Fetching Sleeper player DB...")
    req = urllib.request.Request(
        "https://api.sleeper.app/v1/players/nfl",
        headers={"User-Agent": "nfl-db/1.0"},
    )
    return json.loads(urllib.request.urlopen(req, timeout=60).read())


def fetch_2026_rookies():
    url = (
        f"{SUPABASE_URL}/rest/v1/players?select=*"
        f"&dan_id=gte.2026000&dan_id=lte.2026999&order=dan_id"
    )
    req = urllib.request.Request(
        url,
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
    )
    return json.loads(urllib.request.urlopen(req).read())


def build_mapping(rookies, sleeper):
    mapping = []
    skipped = []
    for r in rookies:
        old_pid = r["player_id"]
        sid = r.get("sleeper_id")
        sp = sleeper.get(sid, {}) if sid else {}
        sr = sp.get("sportradar_id") if sp else None
        name = f"{r['first_name']} {r['last_name']}"
        if not sr:
            reason = "no sleeper_id" if not sid else "no sportradar_id in Sleeper"
            skipped.append((r["dan_id"], name, old_pid, reason))
            continue
        if old_pid == sr:
            continue
        mapping.append({
            "dan_id": r["dan_id"],
            "name": name,
            "old_pid": old_pid,
            "new_pid": sr,
            "row": r,
        })
    return mapping, skipped


def migrate_one(cur, m):
    old_pid = m["old_pid"]
    new_pid = m["new_pid"]
    row = m["row"]

    cur.execute("UPDATE players SET dan_id = NULL WHERE player_id = %s", (old_pid,))

    cols = list(row.keys())
    new_row = dict(row)
    new_row["player_id"] = new_pid
    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO players ({col_list}) VALUES ({placeholders})"
    cur.execute(sql, tuple(new_row[c] for c in cols))

    fk_counts = {}
    for tbl in FK_TABLES:
        cur.execute(
            f"UPDATE {tbl} SET player_id = %s WHERE player_id = %s",
            (new_pid, old_pid),
        )
        fk_counts[tbl] = cur.rowcount

    cur.execute("DELETE FROM players WHERE player_id = %s", (old_pid,))
    if cur.rowcount != 1:
        raise RuntimeError(f"Expected to delete 1 row, deleted {cur.rowcount}")

    return fk_counts


def update_vault_yaml(mapping, dry_run):
    pid_to_new = {m["old_pid"]: m["new_pid"] for m in mapping}
    pdir = Path(VAULT_PLAYERS_DIR)
    if not pdir.exists():
        return []
    updated = []
    for md in pdir.glob("*.md"):
        content = md.read_text()
        m = re.search(r'^player_id:\s*["\']?([0-9a-f-]{36})["\']?', content, re.MULTILINE)
        if not m:
            continue
        old = m.group(1)
        if old not in pid_to_new:
            continue
        new = pid_to_new[old]
        new_content = re.sub(
            r'^(player_id:\s*["\']?)' + re.escape(old) + r'(["\']?)',
            r"\g<1>" + new + r"\g<2>",
            content, count=1, flags=re.MULTILINE,
        )
        updated.append((md.name, old, new))
        if not dry_run:
            md.write_text(new_content)
    return updated


def update_writeups_yaml(mapping, dry_run):
    if not os.path.exists(WRITEUPS_YAML):
        return []
    pid_to_new = {m["old_pid"]: m["new_pid"] for m in mapping}
    with open(WRITEUPS_YAML) as f:
        content = f.read()
    updated = []
    new_content = content
    for old, new in pid_to_new.items():
        if old in new_content:
            new_content = new_content.replace(old, new)
            updated.append((old, new))
    if not dry_run and updated:
        with open(WRITEUPS_YAML, "w") as f:
            f.write(new_content)
    return updated


def main():
    if DRY_RUN:
        print("=== DRY RUN — no DB writes, no file writes ===\n")

    sleeper = fetch_sleeper_db()
    rookies = fetch_2026_rookies()
    print(f"Fetched {len(rookies)} 2026 rookies from Supabase")

    mapping, skipped = build_mapping(rookies, sleeper)
    print(f"\n{len(mapping)} need migration, {len(skipped)} unfixable")

    if skipped:
        print("\nSkipped (unfixable, leaving as-is):")
        for dan, name, pid, reason in skipped:
            print(f"  {dan} {name:25s} — {reason}")

    if not mapping:
        print("\nNothing to migrate.")
        return

    print("\n--- Plan (first 10) ---")
    for m in mapping[:10]:
        print(f"  {m['dan_id']} {m['name']:25s}  {m['old_pid']} → {m['new_pid']}")
    if len(mapping) > 10:
        print(f"  ... and {len(mapping) - 10} more")

    if DRY_RUN:
        vault_changes = update_vault_yaml(mapping, dry_run=True)
        yaml_changes = update_writeups_yaml(mapping, dry_run=True)
        print(f"\n[dry-run] Vault notes that would update: {len(vault_changes)}")
        for name, old, new in vault_changes[:10]:
            print(f"  {name}  {old[:8]}... → {new[:8]}...")
        if len(vault_changes) > 10:
            print(f"  ... and {len(vault_changes) - 10} more")
        print(f"\n[dry-run] writeups YAML replacements: {len(yaml_changes)}")
        return

    print(f"\nConnecting to Postgres at {DB_HOST}...")
    conn = pg8000.dbapi.connect(
        user=DB_USER, host=DB_HOST, port=DB_PORT,
        database=DB_NAME, password=DB_PASSWORD,
    )
    success = []
    failures = []
    print(f"\nMigrating {len(mapping)} players...\n")
    for m in mapping:
        try:
            cur = conn.cursor()
            fk_counts = migrate_one(cur, m)
            conn.commit()
            success.append(m)
            nonzero = {k: v for k, v in fk_counts.items() if v}
            print(f"  ✓ {m['dan_id']} {m['name']:25s}  FK updates: {nonzero}")
        except Exception as e:
            conn.rollback()
            failures.append((m, str(e)))
            print(f"  ✗ {m['dan_id']} {m['name']:25s}  ERROR: {str(e)[:200]}")
    conn.close()

    print(f"\nUpdating vault YAML frontmatter...")
    vault_changes = update_vault_yaml(success, dry_run=False)
    print(f"  Updated {len(vault_changes)} vault notes")

    print(f"\nUpdating data/writeups/player_writeups.yaml...")
    yaml_changes = update_writeups_yaml(success, dry_run=False)
    print(f"  Replaced {len(yaml_changes)} player_ids")

    os.makedirs(MIGRATIONS_DIR, exist_ok=True)
    date_str = datetime.now().strftime("%Y_%m_%d")
    map_path = os.path.join(MIGRATIONS_DIR, f"player_id_remap_{date_str}.json")
    map_data = {
        "date": datetime.now().isoformat(),
        "rationale": "Migrate 2026 rookies to canonical sportradar_id (NFFC/Sportradar key parity)",
        "successes": [
            {"dan_id": m["dan_id"], "name": m["name"], "old": m["old_pid"], "new": m["new_pid"]}
            for m in success
        ],
        "failures": [
            {"dan_id": m["dan_id"], "name": m["name"], "error": err}
            for m, err in failures
        ],
        "skipped": [
            {"dan_id": d, "name": n, "old_pid": pid, "reason": r}
            for d, n, pid, r in skipped
        ],
    }
    with open(map_path, "w") as f:
        json.dump(map_data, f, indent=2)
    print(f"\nSaved audit trail to {map_path}")

    print(f"\n=== Summary ===")
    print(f"  Migrated:           {len(success)}/{len(mapping)}")
    print(f"  Failed:             {len(failures)}")
    print(f"  Vault notes:        {len(vault_changes)}")
    print(f"  Writeups replaced:  {len(yaml_changes)}")
    print(f"  Skipped (unfixable): {len(skipped)}")


if __name__ == "__main__":
    main()
