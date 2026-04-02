"""One-time migration: copy YAML writeups into Obsidian vault player files.

Usage:
  python3 scripts/notes/migrate_yaml_to_vault.py [--dry-run]

Reads data/writeups/player_writeups.yaml, matches each entry to a vault file
by player_id (stored in frontmatter), and inserts the writeup text below the
<!-- Dynasty writeup --> comment.

Behavior:
  - Matched + empty stub: inserts writeup
  - Matched + already has writeup: skips (preserves any edits)
  - No vault file for player_id: reports as unmatched
  - --dry-run: shows what would be written without touching files

After running, use sync_player_notes.py to push to Supabase.
"""

import os
import re
import sys

# ── Config ────────────────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
YAML_PATH = os.path.join(ROOT_DIR, "data", "writeups", "player_writeups.yaml")
VAULT_PLAYERS_DIR = "/Users/dan/obsidian-vault/Fantasy Football/Players"

COMMENT_LINE = "<!-- Dynasty writeup below. 3-5 sentences. Updated as news develops. -->"


# ── YAML parsing (reuses push_writeups.py approach — no PyYAML dependency) ───
def parse_yaml(path):
    """Parse player_writeups.yaml into list of {player_id, writeup} dicts."""
    players = []
    current = {}

    with open(path, encoding="utf-8") as f:
        for line in f:
            stripped = line.rstrip("\n")

            if stripped.startswith("#") or (not stripped.strip() and not current):
                continue

            if stripped.strip().startswith("- player_id:"):
                if current and current.get("player_id"):
                    players.append(current)
                val = stripped.split("player_id:", 1)[1].strip().strip('"\'')
                current = {"player_id": val, "writeup": "", "name": ""}

            elif current and stripped.strip().startswith("name:"):
                val = stripped.split("name:", 1)[1].strip().strip('"\'')
                current["name"] = val

            elif current and stripped.strip().startswith("writeup:"):
                val = stripped.split("writeup:", 1)[1].strip()
                if val.startswith('"') and val.endswith('"'):
                    val = val[1:-1].replace('\\"', '"')
                elif val.startswith("'") and val.endswith("'"):
                    val = val[1:-1]
                if val in ("|", ">", "|-", ">-"):
                    val = ""
                    current["_block"] = True
                current["writeup"] = val

            elif current and current.get("_block") and stripped.startswith("      "):
                line_text = stripped.strip()
                if current["writeup"]:
                    current["writeup"] += " " + line_text
                else:
                    current["writeup"] = line_text

    if current and current.get("player_id"):
        players.append(current)

    for p in players:
        p.pop("_block", None)

    return players


# ── Vault index ───────────────────────────────────────────────────────────────
def build_vault_index():
    """Scan vault files and return {player_id: file_path}."""
    index = {}
    for fname in os.listdir(VAULT_PLAYERS_DIR):
        if not fname.endswith(".md"):
            continue
        path = os.path.join(VAULT_PLAYERS_DIR, fname)
        with open(path, encoding="utf-8") as f:
            content = f.read()
        m = re.search(r'^player_id:\s*["\']?([^"\']+)["\']?\s*$', content, re.MULTILINE)
        if m:
            index[m.group(1).strip()] = path
    return index


# ── File update ───────────────────────────────────────────────────────────────
def has_writeup(path):
    """Return True if the file already has non-empty content below the comment."""
    with open(path, encoding="utf-8") as f:
        content = f.read()
    # Find closing frontmatter
    close = content.find("\n---", 3)
    if close == -1:
        return False
    body = content[close + 4:]
    # Strip headings, wikilinks, comments, blank lines
    for line in body.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("#") or re.match(r'^\[\[.*\]\]$', s) or s.startswith("<!--"):
            continue
        return True
    return False


def insert_writeup(path, writeup, dry_run=False):
    """Insert writeup text into the vault file below the comment line."""
    with open(path, encoding="utf-8") as f:
        content = f.read()

    if COMMENT_LINE not in content:
        # Comment not found — append at end of file
        new_content = content.rstrip() + "\n\n" + writeup + "\n"
    else:
        idx = content.index(COMMENT_LINE)
        after_comment = content[idx + len(COMMENT_LINE):]
        new_content = content[:idx + len(COMMENT_LINE)] + "\n\n" + writeup + "\n"

    if not dry_run:
        with open(path, "w", encoding="utf-8") as f:
            f.write(new_content)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        print("[DRY RUN] No files will be modified.\n")

    print(f"Parsing {YAML_PATH}...")
    entries = parse_yaml(YAML_PATH)
    entries_with_writeup = [e for e in entries if e["writeup"].strip()]
    print(f"  Total entries:       {len(entries)}")
    print(f"  With writeups:       {len(entries_with_writeup)}")

    print(f"\nIndexing vault files...")
    vault_index = build_vault_index()
    print(f"  Vault files indexed: {len(vault_index)}")

    inserted = []
    skipped_has_writeup = []
    unmatched = []

    for entry in entries_with_writeup:
        pid = entry["player_id"]
        writeup = entry["writeup"].strip()

        vault_path = vault_index.get(pid)
        if not vault_path:
            unmatched.append(entry)
            continue

        if has_writeup(vault_path):
            skipped_has_writeup.append(os.path.basename(vault_path))
            continue

        insert_writeup(vault_path, writeup, dry_run=dry_run)
        inserted.append(os.path.basename(vault_path))
        if dry_run:
            preview = writeup[:80] + "..." if len(writeup) > 80 else writeup
            print(f"  WOULD INSERT  {os.path.basename(vault_path)}: {preview}")

    print(f"\nSummary:")
    print(f"  Inserted:            {len(inserted)}")
    print(f"  Skipped (has text):  {len(skipped_has_writeup)}")
    print(f"  Unmatched (no stub): {len(unmatched)}")

    if unmatched:
        print(f"\nUnmatched players (no vault stub — outside top 400 ADP):")
        for e in unmatched:
            print(f"  {e['player_id']}  ({e.get('name', '?')})")

    if not dry_run and inserted:
        print(f"\nNext step: run sync_player_notes.py to push to Supabase.")


if __name__ == "__main__":
    main()
