"""Shared utilities for player ID matching scripts."""

import csv
import json
import os
import re
import urllib.request
import urllib.error

# ── Config ──────────────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Load .env
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
SUPABASE_PAT = os.environ.get("SUPABASE_ACCESS_TOKEN", "")
SPORTSDATA_KEY = os.environ.get("SPORTSDATA_API_KEY", "")
FBG_KEY = os.environ.get("FBG_API_KEY", "")
PROJECT_REF = "twfzcrodldvhpfaykasj"

IMPORTS_DIR = os.path.join(ROOT_DIR, "data", "imports")
NFLREADR_DIR = os.path.join(ROOT_DIR, "data", "nflreadr")
MATCHED_DIR = os.path.join(ROOT_DIR, "data", "matched")


# ── Name normalization ──────────────────────────────────────────────────────
def normalize_name(name):
    """Normalize a player name for matching: lowercase, strip suffixes, punctuation."""
    if not name:
        return ""
    name = name.lower().strip()
    # Remove common suffixes
    name = re.sub(r'\s+(jr\.?|sr\.?|ii|iii|iv|v)$', '', name)
    # Remove punctuation except hyphens (strip apostrophes, periods, quotes)
    name = re.sub(r"[.\"']", "", name)
    # Normalize whitespace
    name = re.sub(r'\s+', ' ', name)
    return name


def normalize_team(team):
    """Normalize team abbreviation."""
    if not team:
        return ""
    team = team.upper().strip()
    # Common mappings
    mapping = {
        "LA": "LAR", "LAR": "LAR",
        "JAC": "JAX", "JAX": "JAX",
        "WSH": "WAS", "WAS": "WAS",
        "LV": "LV", "OAK": "LV",
        "FA": "",  # Free agent = no team
    }
    return mapping.get(team, team)


TEAM_FULLNAME_TO_ABBR = {
    "arizona cardinals": "ARI",
    "atlanta falcons": "ATL",
    "baltimore ravens": "BAL",
    "buffalo bills": "BUF",
    "carolina panthers": "CAR",
    "chicago bears": "CHI",
    "cincinnati bengals": "CIN",
    "cleveland browns": "CLE",
    "dallas cowboys": "DAL",
    "denver broncos": "DEN",
    "detroit lions": "DET",
    "green bay packers": "GB",
    "houston texans": "HOU",
    "indianapolis colts": "IND",
    "jacksonville jaguars": "JAX",
    "kansas city chiefs": "KC",
    "las vegas raiders": "LV",
    "los angeles chargers": "LAC",
    "los angeles rams": "LAR",
    "miami dolphins": "MIA",
    "minnesota vikings": "MIN",
    "new england patriots": "NE",
    "new orleans saints": "NO",
    "new york giants": "NYG",
    "new york jets": "NYJ",
    "philadelphia eagles": "PHI",
    "pittsburgh steelers": "PIT",
    "san francisco 49ers": "SF",
    "seattle seahawks": "SEA",
    "tampa bay buccaneers": "TB",
    "tennessee titans": "TEN",
    "washington commanders": "WAS",
}


# ── Player name aliases (nickname → canonical) ────────────────────────────
# Maps normalized alternate names to normalized canonical names.
# Used by build_player_lookup to index players under both names.
PLAYER_ALIASES = {
    "hollywood brown": "marquise brown",
    "chig okonkwo": "chigoziem okonkwo",
    "chigoziem okonkwo": "chig okonkwo",
    "gabe davis": "gabriel davis",
    "gabriel davis": "gabe davis",
    "scotty miller": "scott miller",
    "scott miller": "scotty miller",
    "robbie chosen anderson": "robbie anderson",
    "chosen anderson": "robbie anderson",
    "robbie anderson": "chosen anderson",
    "keandre lambert": "keandre lambert-smith",
    "keandre lambert-smith": "keandre lambert",
}


# ── Supabase helpers ────────────────────────────────────────────────────────
def supabase_query(sql):
    """Run a SQL query via the Supabase Management API."""
    url = f"https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query"
    data = json.dumps({"query": sql}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "Authorization": f"Bearer {SUPABASE_PAT}",
        "Content-Type": "application/json",
    })
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read().decode("utf-8"))


def supabase_rest_get(table, select="*", params=""):
    """GET rows from Supabase REST API."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}"
    if params:
        url += f"&{params}"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    })
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read().decode("utf-8"))


def supabase_rest_patch(table, match_col, match_val, updates):
    """PATCH (update) a row in Supabase REST API using service role key."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/{table}?{match_col}=eq.{match_val}"
    data = json.dumps(updates).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }, method="PATCH")
    urllib.request.urlopen(req)


def get_all_players():
    """Fetch all players from Supabase (handles pagination)."""
    players = []
    offset = 0
    limit = 1000
    while True:
        url = f"{SUPABASE_URL}/rest/v1/players?select=player_id,first_name,last_name,position,latest_team&offset={offset}&limit={limit}"
        req = urllib.request.Request(url, headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        })
        resp = urllib.request.urlopen(req)
        batch = json.loads(resp.read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += limit
    return players


def build_player_lookup(players):
    """Build lookup dictionaries for matching: by name+pos, by name only.

    Also indexes players under known aliases from PLAYER_ALIASES so that
    nickname-based lookups (e.g., "Hollywood Brown") resolve correctly.
    """
    by_name_pos = {}  # (normalized_name, pos) -> player_id
    by_name = {}      # normalized_name -> player_id (last one wins)
    for p in players:
        full_name = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
        norm = normalize_name(full_name)
        pos = (p.get("position") or "").upper()
        by_name_pos[(norm, pos)] = p["player_id"]
        by_name[norm] = p["player_id"]
        # Also index under known aliases
        alias = PLAYER_ALIASES.get(norm)
        if alias:
            by_name_pos[(alias, pos)] = p["player_id"]
            by_name[alias] = p["player_id"]
    return by_name_pos, by_name


def read_csv(path):
    """Read a CSV file and return list of dicts."""
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def ensure_dir(path):
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)
