"""Push player writeups from YAML to Supabase player_notes table.

Usage: python3 scripts/notes/push_writeups.py [--dry-run]

Reads data/writeups/player_writeups.yaml, filters to non-empty writeups,
and upserts them into the player_notes table via REST API (service role key).
"""

import json
import os
import sys
import urllib.request
import urllib.error

# ── Config ───────────────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

env_path = os.path.join(ROOT_DIR, ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip()

SUPABASE_URL = "https://twfzcrodldvhpfaykasj.supabase.co"
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
YAML_PATH = os.path.join(ROOT_DIR, "data", "writeups", "player_writeups.yaml")


def parse_yaml_simple(path):
    """Parse the writeups YAML without PyYAML dependency.

    Handles multi-line writeups using YAML literal/folded blocks or quoted strings.
    """
    players = []
    current = {}

    with open(path) as f:
        for line in f:
            stripped = line.rstrip("\n")

            # Skip comments and blank lines outside of a player entry
            if stripped.startswith("#") or (not stripped.strip() and not current):
                continue

            # New player entry
            if stripped.strip().startswith("- player_id:"):
                if current and current.get("player_id"):
                    players.append(current)
                val = stripped.split("player_id:", 1)[1].strip().strip('"')
                current = {"player_id": val, "writeup": ""}
            elif current and stripped.strip().startswith("name:"):
                pass  # reference only
            elif current and stripped.strip().startswith("position:"):
                pass  # reference only
            elif current and stripped.strip().startswith("team:"):
                pass  # reference only
            elif current and stripped.strip().startswith("writeup:"):
                val = stripped.split("writeup:", 1)[1].strip()
                # Strip surrounding quotes
                if val.startswith('"') and val.endswith('"'):
                    val = val[1:-1].replace('\\"', '"')
                elif val.startswith("'") and val.endswith("'"):
                    val = val[1:-1]
                # Handle YAML block scalar indicators
                if val in ("|", ">", "|-", ">-"):
                    # Read indented continuation lines
                    val = ""
                    # Will be handled by continuation logic below
                    current["_block"] = True
                current["writeup"] = val
            elif current and current.get("_block") and stripped.startswith("      "):
                # Continuation of block scalar (indented under writeup)
                line_text = stripped.strip()
                if current["writeup"]:
                    current["writeup"] += " " + line_text
                else:
                    current["writeup"] = line_text

    # Don't forget the last entry
    if current and current.get("player_id"):
        players.append(current)

    # Clean up internal flags
    for p in players:
        p.pop("_block", None)

    return players


def upsert_writeups(rows, dry_run=False):
    """Upsert writeups to player_notes via REST API."""
    url = f"{SUPABASE_URL}/rest/v1/player_notes"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal,count=exact",
    }

    if dry_run:
        print(f"\n[DRY RUN] Would upsert {len(rows)} writeups:")
        for r in rows[:10]:
            preview = r["writeup"][:80] + "..." if len(r["writeup"]) > 80 else r["writeup"]
            print(f"  {r['player_id']}: {preview}")
        if len(rows) > 10:
            print(f"  ... and {len(rows) - 10} more")
        return

    # Batch in groups of 100
    batch_size = 100
    total_upserted = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        payload = json.dumps(batch).encode()

        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req) as resp:
                content_range = resp.getheader("content-range", "")
                total_upserted += len(batch)
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"  ERROR batch {i // batch_size + 1}: {e.code} {body}")
            return

    print(f"  Upserted: {total_upserted}")


def main():
    dry_run = "--dry-run" in sys.argv

    print(f"Reading {YAML_PATH}...")
    players = parse_yaml_simple(YAML_PATH)
    print(f"  Total entries: {len(players)}")

    # Filter to non-empty writeups
    rows = [
        {"player_id": p["player_id"], "writeup": p["writeup"]}
        for p in players
        if p.get("writeup", "").strip()
    ]
    print(f"  With writeups: {len(rows)}")
    skipped = len(players) - len(rows)
    if skipped:
        print(f"  Skipped (empty): {skipped}")

    if not rows:
        print("\nNo writeups to push.")
        return

    print(f"\nUpserting to player_notes...")
    upsert_writeups(rows, dry_run=dry_run)
    print("\nDone.")


if __name__ == "__main__":
    main()
