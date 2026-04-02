"""Push Obsidian vault player writeups to Supabase player_notes table.

Usage:
  python3 scripts/notes/sync_player_notes.py [--dry-run]

Reads all .md files in the vault Players directory, extracts writeup content
from ## Dynasty and ## Best Ball sections, and upserts into player_notes
(player_id, context, writeup, updated_at).

Section extraction:
  - Splits body on ## Dynasty and ## Best Ball headers
  - Strips: [[Wikilinks]], <!-- HTML comments -->, blank lines
  - Skips sections with empty content (no upsert for empty sections)
  - Fallback: if no section headers, treats entire body as dynasty

Authentication: SUPABASE_SERVICE_ROLE_KEY (write access required)
"""

import json
import os
import re
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
VAULT_PLAYERS_DIR = "/Users/dan/obsidian-vault/Fantasy Football/Players"

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

SECTION_HEADERS = {"## Dynasty": "dynasty", "## Best Ball": "bestball"}


# ── Parsing ───────────────────────────────────────────────────────────────────
def parse_player_file(path):
    """Parse a player .md file. Returns (player_id, sections_dict) or (None, None).

    sections_dict: {"dynasty": "text...", "bestball": "text..."} — only non-empty sections included.
    """
    with open(path, encoding="utf-8") as f:
        content = f.read()

    if not content.startswith("---"):
        return None, None

    close = content.find("\n---", 3)
    if close == -1:
        return None, None

    frontmatter = content[3:close]
    body = content[close + 4:]  # skip "\n---"

    # Extract player_id from frontmatter
    player_id = None
    for line in frontmatter.splitlines():
        m = re.match(r'^player_id:\s*["\']?([^"\']+)["\']?\s*$', line)
        if m:
            player_id = m.group(1).strip()
            break

    if not player_id:
        return None, None

    # Extract sections
    sections = extract_sections(body)
    return player_id, sections


def extract_sections(body):
    """Split body into named sections based on ## headers.

    Returns dict with non-empty sections only, e.g. {"dynasty": "writeup text"}.
    If no section headers found, treats entire body as dynasty (backward compat).
    """
    # Check if body has any section headers
    has_sections = any(h in body for h in SECTION_HEADERS)

    if not has_sections:
        # Fallback: treat entire body as dynasty
        cleaned = clean_text(body)
        if cleaned:
            return {"dynasty": cleaned}
        return {}

    # Split body into sections
    sections = {}
    current_context = None
    current_lines = []

    for line in body.splitlines():
        stripped = line.strip()
        # Check if this line is a section header
        matched_header = None
        for header, context in SECTION_HEADERS.items():
            if stripped == header.strip():
                matched_header = context
                break

        if matched_header:
            # Save previous section
            if current_context is not None:
                cleaned = clean_text("\n".join(current_lines))
                if cleaned:
                    sections[current_context] = cleaned
            current_context = matched_header
            current_lines = []
        elif stripped.startswith("## ") and current_context is not None:
            # Unrecognized ## header ends the current section
            cleaned = clean_text("\n".join(current_lines))
            if cleaned:
                sections[current_context] = cleaned
            current_context = None
            current_lines = []
        elif current_context is not None:
            current_lines.append(line)

    # Save last section
    if current_context is not None:
        cleaned = clean_text("\n".join(current_lines))
        if cleaned:
            sections[current_context] = cleaned

    return sections


def clean_text(text):
    """Strip structural elements from text; return the actual writeup content."""
    lines = text.splitlines()
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            cleaned.append("")
            continue
        # Skip [[Wikilinks]]
        if re.match(r'^\[\[.*\]\]$', stripped):
            continue
        # Skip <!-- HTML comments --> (single-line)
        if re.match(r'^<!--.*-->$', stripped):
            continue
        # Skip multi-line comment start/end lines
        if stripped.startswith("<!--") or stripped.endswith("-->"):
            continue
        # Skip # Headings
        if stripped.startswith("#"):
            continue
        cleaned.append(line)

    # Rejoin and strip outer blank lines
    return "\n".join(cleaned).strip()


# ── Supabase ──────────────────────────────────────────────────────────────────
def upsert_notes(rows, dry_run=False):
    """Upsert writeups to player_notes via REST API. Returns True on success."""
    if dry_run:
        print(f"\n[DRY RUN] Would upsert {len(rows)} notes:")
        for r in rows[:10]:
            preview = r["writeup"][:80] + "..." if len(r["writeup"]) > 80 else r["writeup"]
            print(f"  [{r['context']}] {r['player_id']}: {preview}")
        if len(rows) > 10:
            print(f"  ... and {len(rows) - 10} more")
        return True

    url = f"{SUPABASE_URL}/rest/v1/player_notes"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    batch_size = 100
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i: i + batch_size]
        req = urllib.request.Request(
            url,
            data=json.dumps(batch).encode(),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req):
                total += len(batch)
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"  ERROR batch {i // batch_size + 1}: {e.code} {body}")
            return False

    print(f"  Upserted: {total}")
    return True


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    dry_run = "--dry-run" in sys.argv

    if not SUPABASE_SERVICE_KEY:
        print("ERROR: SUPABASE_SERVICE_ROLE_KEY not set. Check .env.")
        sys.exit(1)

    if dry_run:
        print("[DRY RUN] No changes will be written to Supabase.\n")

    md_files = sorted(
        f for f in os.listdir(VAULT_PLAYERS_DIR) if f.endswith(".md")
    )
    print(f"Scanning {len(md_files)} player files in vault...")

    rows = []
    skipped_no_id = 0
    skipped_empty = 0
    errors = []
    context_counts = {"dynasty": 0, "bestball": 0}
    now = datetime.now(timezone.utc).isoformat()

    for fname in md_files:
        path = os.path.join(VAULT_PLAYERS_DIR, fname)
        try:
            player_id, sections = parse_player_file(path)
        except Exception as e:
            errors.append(f"{fname}: {e}")
            continue

        if not player_id:
            skipped_no_id += 1
            continue

        if not sections:
            skipped_empty += 1
            continue

        for context, writeup in sections.items():
            rows.append({
                "player_id": player_id,
                "context": context,
                "writeup": writeup,
                "updated_at": now,
            })
            context_counts[context] = context_counts.get(context, 0) + 1

    print(f"  Dynasty writeups:   {context_counts.get('dynasty', 0)}")
    print(f"  Best ball writeups: {context_counts.get('bestball', 0)}")
    print(f"  Empty (no content): {skipped_empty}")
    if skipped_no_id:
        print(f"  Missing player_id:  {skipped_no_id}")
    if errors:
        print(f"  Parse errors:       {len(errors)}")
        for e in errors:
            print(f"    {e}")

    if not rows:
        print("\nNo writeups to push.")
        return

    print(f"\nUpserting {len(rows)} rows to player_notes...")
    upsert_notes(rows, dry_run=dry_run)
    print("Done.")


if __name__ == "__main__":
    main()
