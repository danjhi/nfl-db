"""Shared utilities for Sleeper trade upload scripts."""

import json
import os
import urllib.request
import urllib.error

# ── Config ────────────────────────────────────────────────────────────────────
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

SLEEPER_DB_PATH = os.environ.get(
    "SLEEPER_DB_PATH",
    os.path.expanduser("~/Desktop/sleeper scrape/sleeper.db"),
)


# ── Supabase REST helpers ─────────────────────────────────────────────────────
def _supa_headers():
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def supa_upsert(table, rows, on_conflict, batch_size=500):
    """POST rows with upsert in batches."""
    if not rows:
        return 0
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}"
        headers = {
            **_supa_headers(),
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }
        req = urllib.request.Request(
            url, data=json.dumps(batch).encode(), headers=headers, method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            resp.read()
        total += len(batch)
    return total


def supa_batch_insert(table, rows, batch_size=500):
    """Insert rows in batches. Returns total inserted."""
    if not rows:
        return 0
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        headers = {**_supa_headers(), "Prefer": "return=minimal"}
        req = urllib.request.Request(
            url, data=json.dumps(batch).encode(), headers=headers, method="POST"
        )
        with urllib.request.urlopen(req) as resp:
            resp.read()
        total += len(batch)
    return total


def supa_delete(table, params):
    """DELETE rows matching params (e.g. 'year=eq.2025')."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params}"
    req = urllib.request.Request(
        url,
        headers={**_supa_headers(), "Prefer": "return=minimal"},
        method="DELETE",
    )
    with urllib.request.urlopen(req) as resp:
        resp.read()
