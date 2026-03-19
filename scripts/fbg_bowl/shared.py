"""Shared utilities for FBG Bowl ETL scripts."""

import json
import os
import time
import urllib.request
import urllib.error

# ── Config ────────────────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data", "fbg_bowl")

env_path = os.path.join(ROOT_DIR, ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip()

SUPABASE_URL = "https://twfzcrodldvhpfaykasj.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

SLEEPER_BASE = "https://api.sleeper.app/v1"
SLEEP_BETWEEN_CALLS = 0.15  # seconds


# ── Filesystem ────────────────────────────────────────────────────────────────
def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def checkpoint_path(name):
    return os.path.join(DATA_DIR, f"{name}.json")


def save_checkpoint(name, data):
    ensure_data_dir()
    with open(checkpoint_path(name), "w") as f:
        json.dump(data, f)


def load_checkpoint(name):
    p = checkpoint_path(name)
    if os.path.exists(p):
        with open(p) as f:
            return json.load(f)
    return None


# ── Sleeper API ───────────────────────────────────────────────────────────────
def sleeper_get(path, retries=3, pause=SLEEP_BETWEEN_CALLS):
    """GET a Sleeper API endpoint. Returns parsed JSON or None on failure."""
    url = f"{SLEEPER_BASE}/{path.lstrip('/')}"
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "fbg-bowl-etl/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
                time.sleep(pause)
                return data
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if attempt < retries:
                time.sleep(0.5 * attempt)
            else:
                print(f"  WARN sleeper {url}: HTTP {e.code}")
                return None
        except Exception as e:
            if attempt < retries:
                time.sleep(0.5 * attempt)
            else:
                print(f"  WARN sleeper {url}: {e}")
                return None
    return None


# ── Supabase REST helpers ─────────────────────────────────────────────────────
def _supa_headers(service=True):
    key = SUPABASE_SERVICE_KEY if service else SUPABASE_KEY
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


def supa_insert(table, rows, service=True):
    """POST rows to Supabase table. Returns inserted rows (with IDs) or raises."""
    if not rows:
        return []
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {**_supa_headers(service), "Prefer": "return=representation"}
    req = urllib.request.Request(
        url,
        data=json.dumps(rows).encode(),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def supa_upsert(table, rows, on_conflict, service=True):
    """POST rows with upsert (merge-duplicates on the given conflict column(s))."""
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}"
    headers = {
        **_supa_headers(service),
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(rows).encode(),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        resp.read()


def supa_get(table, select="*", params="", service=False):
    """GET rows from Supabase with pagination."""
    all_rows = []
    offset = 0
    limit = 1000
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}&offset={offset}&limit={limit}"
        if params:
            url += f"&{params}"
        req = urllib.request.Request(url, headers=_supa_headers(service))
        with urllib.request.urlopen(req) as resp:
            batch = json.loads(resp.read())
        all_rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return all_rows


def supa_batch_insert(table, rows, batch_size=500, service=True):
    """Insert rows in batches; return all inserted rows."""
    inserted = []
    for i in range(0, len(rows), batch_size):
        batch = rows[i: i + batch_size]
        result = supa_insert(table, batch, service=service)
        inserted.extend(result)
    return inserted


def supa_delete(table, params, service=True):
    """DELETE rows matching params (e.g. 'year=eq.2025')."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params}"
    req = urllib.request.Request(
        url,
        headers={**_supa_headers(service), "Prefer": "return=minimal"},
        method="DELETE",
    )
    with urllib.request.urlopen(req) as resp:
        resp.read()


# ── Sleeper data helpers ──────────────────────────────────────────────────────
def parse_roster_pts(settings):
    """Parse Sleeper roster settings → (pts_for, wins, losses, ties)."""
    if not settings:
        return 0.0, 0, 0, 0
    fpts = settings.get("fpts") or 0
    fpts_dec = settings.get("fpts_decimal") or 0
    pts_for = float(fpts) + float(fpts_dec) / 100
    wins = int(settings.get("wins") or 0)
    losses = int(settings.get("losses") or 0)
    ties = int(settings.get("ties") or 0)
    return round(pts_for, 2), wins, losses, ties


def compute_week_results(matchups):
    """
    Given a list of Sleeper matchup dicts for one week,
    return list of {roster_id, pts_for, pts_against, win, loss, tie}.
    """
    if not matchups:
        return []

    # Group by matchup_id
    by_matchup = {}
    for m in matchups:
        mid = m.get("matchup_id")
        if mid is None:
            continue
        rid = int(m.get("roster_id", 0))
        pts = float(m.get("points") or 0)
        if mid not in by_matchup:
            by_matchup[mid] = []
        by_matchup[mid].append({"roster_id": rid, "pts": pts})

    results = []
    for mid, teams in by_matchup.items():
        if len(teams) == 2:
            a, b = teams
            a_win = a["pts"] > b["pts"]
            tie = a["pts"] == b["pts"]
            results.append({
                "roster_id": a["roster_id"],
                "pts_for": round(a["pts"], 2),
                "pts_against": round(b["pts"], 2),
                "win": a_win and not tie,
                "loss": not a_win and not tie,
                "tie": tie,
            })
            results.append({
                "roster_id": b["roster_id"],
                "pts_for": round(b["pts"], 2),
                "pts_against": round(a["pts"], 2),
                "win": not a_win and not tie,
                "loss": a_win and not tie,
                "tie": tie,
            })
        else:
            # Bye or solo matchup
            for t in teams:
                results.append({
                    "roster_id": t["roster_id"],
                    "pts_for": round(t["pts"], 2),
                    "pts_against": None,
                    "win": False,
                    "loss": False,
                    "tie": False,
                })
    return results
