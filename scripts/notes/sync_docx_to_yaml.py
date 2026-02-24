#!/usr/bin/env python3
"""Sync writeups from a DOCX back into player_writeups.yaml.

Parses Heading 2 entries from the DOCX, normalizes smart quotes,
and replaces the writeup lines in the YAML (matched by position order).
"""

import re
import sys
from docx import Document

YAML_PATH = "data/writeups/player_writeups.yaml"


def normalize_text(s):
    """Normalize smart quotes and unicode characters for YAML storage."""
    # Smart double quotes -> escaped straight quotes
    s = s.replace("\u201c", '\\"')  # left curly double
    s = s.replace("\u201d", '\\"')  # right curly double
    # Smart single quotes -> straight apostrophe
    s = s.replace("\u2018", "'")    # left curly single
    s = s.replace("\u2019", "'")    # right curly single
    # Em dash, en dash -> hyphen
    s = s.replace("\u2014", "-")
    s = s.replace("\u2013", "-")
    # Ellipsis
    s = s.replace("\u2026", "...")
    # Escape any remaining straight double quotes
    # (but don't double-escape ones we already escaped)
    # Split on already-escaped quotes, escape unescaped ones, rejoin
    parts = s.split('\\"')
    parts = [p.replace('"', '\\"') for p in parts]
    s = '\\"'.join(parts)
    return s


def parse_docx(docx_path):
    """Parse DOCX into list of writeup strings (one per player)."""
    doc = Document(docx_path)
    writeups = []
    current_name = None
    current_text = []

    for para in doc.paragraphs:
        if para.style and para.style.name == "Heading 2":
            if current_name:
                writeups.append(" ".join(current_text).strip())
            current_name = para.text.strip()
            current_text = []
        elif para.style and para.style.name in ("normal", "Normal", "Body Text"):
            if current_name and para.text.strip():
                current_text.append(para.text.strip())

    if current_name:
        writeups.append(" ".join(current_text).strip())

    return writeups


def update_yaml(yaml_path, writeups):
    """Replace writeup lines in YAML with new text."""
    with open(yaml_path) as f:
        yaml_lines = f.readlines()

    entry_idx = -1
    new_lines = []
    for line in yaml_lines:
        if line.strip().startswith("- player_id:"):
            entry_idx += 1
            new_lines.append(line)
        elif line.strip().startswith("writeup:"):
            new_writeup = normalize_text(writeups[entry_idx])
            new_lines.append(f'  writeup: "{new_writeup}"\n')
        else:
            new_lines.append(line)

    with open(yaml_path, "w") as f:
        f.writelines(new_lines)

    return entry_idx + 1


if __name__ == "__main__":
    docx_path = sys.argv[1] if len(sys.argv) > 1 else "data/writeups/player_writeups (1).docx"

    print(f"Reading {docx_path}...")
    writeups = parse_docx(docx_path)
    print(f"  {len(writeups)} players found in DOCX")

    print(f"Updating {YAML_PATH}...")
    count = update_yaml(YAML_PATH, writeups)
    print(f"  {count} writeups replaced")
    print("Done.")
