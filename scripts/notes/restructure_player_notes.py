"""Restructure vault player notes to add ## Dynasty and ## Best Ball section headers.

Usage:
  python3 scripts/notes/restructure_player_notes.py [--dry-run]

For each .md file in the vault Players directory:
  - Files WITH writeup content: replaces the <!-- comment --> with ## Dynasty header,
    preserves existing writeup below it, appends ## Best Ball section
  - Files WITHOUT writeup (empty stubs): replaces <!-- comment --> with both sections
  - Files already restructured (contain ## Dynasty): skipped

This is idempotent — safe to run multiple times.
"""

import os
import re
import sys

VAULT_PLAYERS_DIR = "/Users/dan/obsidian-vault/Fantasy Football/Players"
COMMENT_PATTERN = re.compile(r'^<!-- Dynasty writeup below\..*?-->\s*$', re.MULTILINE)
BESTBALL_COMMENT = "<!-- Best ball writeup. Weekly variance, stacking, correlation, exposure. -->"


def restructure_file(path, dry_run=False):
    """Add section headers to a single player file. Returns status string."""
    with open(path, encoding="utf-8") as f:
        content = f.read()

    # Already restructured
    if "\n## Dynasty" in content:
        return "skipped (already has ## Dynasty)"

    # Find the HTML comment
    match = COMMENT_PATTERN.search(content)
    if not match:
        # No comment line — check if there's content after frontmatter
        if "---" not in content[3:]:
            return "skipped (no frontmatter)"
        # Insert sections after the last structural line (team wikilink or heading)
        return "skipped (no comment marker)"

    comment_start = match.start()
    comment_end = match.end()

    # Everything before the comment (frontmatter + heading + wikilink)
    before = content[:comment_start].rstrip("\n")

    # Everything after the comment (the actual writeup, if any)
    after = content[comment_end:].strip()

    if after:
        # Has writeup content — put it under ## Dynasty
        new_content = f"{before}\n\n## Dynasty\n\n{after}\n\n## Best Ball\n\n{BESTBALL_COMMENT}\n"
    else:
        # Empty stub — add both sections empty
        new_content = f"{before}\n\n## Dynasty\n\n\n\n## Best Ball\n\n{BESTBALL_COMMENT}\n"

    if dry_run:
        return f"would restructure ({'with content' if after else 'empty stub'})"

    with open(path, "w", encoding="utf-8") as f:
        f.write(new_content)

    return f"restructured ({'with content' if after else 'empty stub'})"


def main():
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        print("[DRY RUN] No files will be modified.\n")

    md_files = sorted(f for f in os.listdir(VAULT_PLAYERS_DIR) if f.endswith(".md"))
    print(f"Scanning {len(md_files)} player files...\n")

    counts = {"restructured": 0, "skipped": 0, "error": 0}

    for fname in md_files:
        path = os.path.join(VAULT_PLAYERS_DIR, fname)
        try:
            status = restructure_file(path, dry_run)
            if "restructure" in status:
                counts["restructured"] += 1
            else:
                counts["skipped"] += 1
            if dry_run or "restructure" in status:
                print(f"  {fname}: {status}")
        except Exception as e:
            counts["error"] += 1
            print(f"  {fname}: ERROR — {e}")

    print(f"\nDone: {counts['restructured']} restructured, {counts['skipped']} skipped, {counts['error']} errors")


if __name__ == "__main__":
    main()
