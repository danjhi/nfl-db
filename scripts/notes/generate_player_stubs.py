"""Generate Obsidian player note stubs for top 400 dynasty-relevant players.

Usage:
  python3 scripts/notes/generate_player_stubs.py [--dry-run] [--update-adp] [--limit N]

Vault path (hardcoded):
  /Users/dan/Documents/ObsidianVault/Fantasy Football/Players/

Behavior:
  - New players: creates {slug}.md with pre-populated frontmatter + empty body
  - Existing files: skipped by default
  - With --update-adp: updates adp, adp_rank, team in frontmatter (never touches body)
  - With --limit N: process only top N players (useful for testing)
  - Safe to re-run at any time

Data source: Supabase adp_sources (latest Underdog 2026 ADP) joined to players.
"""

import json
import os
import re
import sys
import urllib.request
import urllib.error
from datetime import date

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
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY", "")

ABBR_TO_TEAM = {
    "ARI": "Arizona Cardinals",   "ATL": "Atlanta Falcons",     "BAL": "Baltimore Ravens",
    "BUF": "Buffalo Bills",       "CAR": "Carolina Panthers",   "CHI": "Chicago Bears",
    "CIN": "Cincinnati Bengals",  "CLE": "Cleveland Browns",    "DAL": "Dallas Cowboys",
    "DEN": "Denver Broncos",      "DET": "Detroit Lions",       "GB":  "Green Bay Packers",
    "HOU": "Houston Texans",      "IND": "Indianapolis Colts",  "JAX": "Jacksonville Jaguars",
    "KC":  "Kansas City Chiefs",  "LV":  "Las Vegas Raiders",   "LAC": "Los Angeles Chargers",
    "LA":  "Los Angeles Rams",    "MIA": "Miami Dolphins",      "MIN": "Minnesota Vikings",
    "NE":  "New England Patriots","NO":  "New Orleans Saints",  "NYG": "New York Giants",
    "NYJ": "New York Jets",       "PHI": "Philadelphia Eagles", "PIT": "Pittsburgh Steelers",
    "SF":  "San Francisco 49ers", "SEA": "Seattle Seahawks",    "TB":  "Tampa Bay Buccaneers",
    "TEN": "Tennessee Titans",    "WAS": "Washington Commanders",
}


# ── Helpers ───────────────────────────────────────────────────────────────────
def slugify(first_name, last_name):
    """Generate slug from player name: lowercase, remove apostrophes/periods, spaces→hyphens.

    Examples:
      Ja'Marr Chase  → jamarr-chase
      A.J. Brown     → aj-brown
      D.K. Metcalf   → dk-metcalf
    """
    s = f"{first_name} {last_name}".lower()
    s = re.sub(r"['.]+", "", s)                        # remove apostrophes and periods
    s = re.sub(r"\s+(jr|sr|ii|iii|iv|v)$", "", s)     # strip suffixes
    s = re.sub(r"\s+", "-", s.strip())                 # spaces → hyphens
    s = re.sub(r"[^a-z0-9-]", "", s)                  # strip any remaining odd chars
    return s


def rest_get_paginated(path, params=""):
    """Fetch all rows from a Supabase REST endpoint, handling 1000-row pagination."""
    all_rows = []
    offset = 0
    limit = 1000
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{path}?{params}&offset={offset}&limit={limit}"
        req = urllib.request.Request(url, headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        })
        with urllib.request.urlopen(req) as resp:
            batch = json.loads(resp.read().decode())
        all_rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return all_rows


# ── Data fetching ─────────────────────────────────────────────────────────────
def fetch_latest_adp():
    """Fetch Underdog 2026 ADP rows and return latest snapshot per player.

    Returns dict: {player_id: {"adp": float, "date": str}}
    """
    rows = rest_get_paginated(
        "adp_sources",
        "source=eq.underdog&year=eq.2026&select=player_id,adp,date&order=date.desc",
    )
    latest = {}
    for row in rows:
        pid = row["player_id"]
        if pid not in latest or row["date"] > latest[pid]["date"]:
            latest[pid] = row
    return latest


def fetch_players():
    """Fetch all QB/RB/WR/TE players from Supabase."""
    return rest_get_paginated(
        "players",
        "position=in.(QB,RB,WR,TE)&select=player_id,dan_id,first_name,last_name,position,latest_team",
    )


def build_player_list(limit=400):
    """Join ADP + players, sort by ADP, return top N with ranks."""
    print("  Fetching ADP data...")
    adp_data = fetch_latest_adp()
    print(f"  Found {len(adp_data)} players with Underdog 2026 ADP")

    print("  Fetching player records...")
    players = fetch_players()
    player_map = {p["player_id"]: p for p in players}

    merged = []
    for pid, adp_row in adp_data.items():
        p = player_map.get(pid)
        if p is None:
            continue
        if adp_row["adp"] is None:
            continue
        merged.append({
            "player_id": p["player_id"],
            "dan_id": p.get("dan_id"),
            "first_name": p["first_name"] or "",
            "last_name": p["last_name"] or "",
            "position": p["position"] or "",
            "latest_team": p.get("latest_team") or "",
            "adp": float(adp_row["adp"]),
        })

    merged.sort(key=lambda x: x["adp"])
    top = merged[:limit]
    for i, row in enumerate(top, 1):
        row["adp_rank"] = i
    return top


# ── Stub generation ───────────────────────────────────────────────────────────
def make_stub(player, today):
    """Return markdown content for a new player stub."""
    full_team = ABBR_TO_TEAM.get(player["latest_team"], player["latest_team"])
    full_name = f"{player['first_name']} {player['last_name']}"

    lines = ["---", f'player_id: "{player["player_id"]}"']
    if player["dan_id"] is not None:
        lines.append(f'dan_id: {player["dan_id"]}')
    lines += [
        f'position: {player["position"]}',
        f'team: {full_team}',
        f'adp: {player["adp"]:.1f}',
        f'adp_rank: {player["adp_rank"]}',
        f'created: {today}',
        f'modified: {today}',
        'status: active',
        '---',
        '',
        f'# {full_name}',
        '',
    ]
    if full_team:
        lines.append(f'[[{full_team}]]')
        lines.append('')
    lines += [
        '<!-- Dynasty writeup below. 3-5 sentences. Updated as news develops. -->',
        '',
    ]
    return "\n".join(lines)


def update_frontmatter(content, player):
    """Update adp, adp_rank, and team fields within the frontmatter block only.

    Splits on the opening/closing --- delimiters so the body is never touched.
    """
    if not content.startswith("---"):
        return content
    # Find the closing --- (search from char 3 onward to skip the opening ---)
    close = content.find("\n---", 3)
    if close == -1:
        return content

    fm = content[3:close]       # frontmatter text (between the two ---)
    rest = content[close:]      # "\n---\n..." and everything after

    full_team = ABBR_TO_TEAM.get(player["latest_team"], player["latest_team"])

    def replace_field(text, field, new_val):
        return re.sub(
            rf"^({re.escape(field)}:\s*).*$",
            rf"\g<1>{new_val}",
            text,
            flags=re.MULTILINE,
        )

    fm = replace_field(fm, "adp", f"{player['adp']:.1f}")
    fm = replace_field(fm, "adp_rank", str(player["adp_rank"]))
    if full_team:
        fm = replace_field(fm, "team", full_team)

    return "---" + fm + rest


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    dry_run = "--dry-run" in sys.argv
    update_adp = "--update-adp" in sys.argv

    limit = 400
    for i, arg in enumerate(sys.argv):
        if arg == "--limit" and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])

    if not SUPABASE_KEY:
        print("ERROR: SUPABASE_ANON_KEY not set. Check .env.")
        sys.exit(1)

    if dry_run:
        print("[DRY RUN] No files will be written.\n")

    print(f"Querying Supabase for top {limit} players by Underdog 2026 ADP...")
    players = build_player_list(limit=limit)
    print(f"  Returning {len(players)} players after join\n")

    os.makedirs(VAULT_PLAYERS_DIR, exist_ok=True)

    today = date.today().isoformat()
    created = 0
    updated = 0
    skipped = 0

    for p in players:
        slug = slugify(p["first_name"], p["last_name"])
        file_path = os.path.join(VAULT_PLAYERS_DIR, f"{slug}.md")

        if os.path.exists(file_path):
            if update_adp:
                with open(file_path, "r", encoding="utf-8") as f:
                    original = f.read()
                updated_content = update_frontmatter(original, p)
                if updated_content != original:
                    if not dry_run:
                        with open(file_path, "w", encoding="utf-8") as f:
                            f.write(updated_content)
                    print(f"  UPDATED  {slug}.md  (adp={p['adp']:.1f}, rank={p['adp_rank']})")
                    updated += 1
                else:
                    skipped += 1
            else:
                skipped += 1
        else:
            stub = make_stub(p, today)
            if not dry_run:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(stub)
            print(f"  CREATED  {slug}.md  ({p['position']}, adp={p['adp']:.1f})")
            created += 1

    print(f"\nSummary:")
    print(f"  Created:  {created}")
    if update_adp:
        print(f"  Updated:  {updated}")
    print(f"  Skipped:  {skipped}")
    print(f"\nVault path: {VAULT_PLAYERS_DIR}")


if __name__ == "__main__":
    main()
