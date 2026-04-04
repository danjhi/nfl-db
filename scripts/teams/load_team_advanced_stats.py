"""
Load nflverse advanced team stats into Supabase team_advanced_stats table.
Merges 5 CSV files, normalizes team abbreviations, computes league ranks.

Source CSVs (from ~/dev/2026-nfl-projections/data/nflverse/):
  - team_efficiency.csv     (EPA, completion %, YPA, sack rate, etc.)
  - red_zone_stats.csv      (RZ trips, TD rate, pass rate, inside-10)
  - scheme_stats.csv        (play action, RPO, screen, motion, formation)
  - neutral_pass_rate.csv   (PROE: neutral, leading, trailing)
  - sack_pressure_stats.csv (pressure rate, blitz rate, pocket time)

Usage:
  cd ~/dev/nfl-db
  python3 scripts/teams/load_team_advanced_stats.py          # load to Supabase
  python3 scripts/teams/load_team_advanced_stats.py --dry-run # preview without loading
  python3 scripts/teams/load_team_advanced_stats.py --csv-only # write merged CSV with ranks
"""

import csv
import json
import os
import sys
from pathlib import Path
from collections import defaultdict

# Add shared utilities
sys.path.insert(0, str(Path(__file__).parent.parent / "ids"))
from shared import SUPABASE_URL, SUPABASE_SERVICE_KEY

# Load env from nfl-db .env
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
except ImportError:
    # Manual .env loading if python-dotenv not installed
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

NFLVERSE_DIR = Path.home() / "dev" / "2026-nfl-projections" / "data" / "nflverse"
OUTPUT_CSV = Path.home() / "dev" / "2026-nfl-projections" / "data" / "team_advanced_stats.csv"

# Normalize team abbreviations to nflreadr standard
TEAM_NORMALIZE = {
    "LA": "LAR",
    "LAR": "LAR",
    "LVR": "LV",
    "WAS": "WSH",
    "OAK": "LV",
    "SD": "LAC",
    "STL": "LAR",
}

VALID_TEAMS = {
    "ARI", "ATL", "BAL", "BUF", "CAR", "CHI", "CIN", "CLE",
    "DAL", "DEN", "DET", "GB", "HOU", "IND", "JAX", "KC",
    "LAC", "LAR", "LV", "MIA", "MIN", "NE", "NO", "NYG",
    "NYJ", "PHI", "PIT", "SEA", "SF", "TB", "TEN", "WSH",
}

def normalize_team(t: str) -> str:
    t = t.strip().upper()
    return TEAM_NORMALIZE.get(t, t)

def read_csv(filename: str) -> list[dict]:
    path = NFLVERSE_DIR / filename
    if not path.exists():
        print(f"  WARNING: {path} not found, skipping")
        return []
    with open(path) as f:
        rows = list(csv.DictReader(f))
    # Normalize teams, filter to valid NFL teams
    for r in rows:
        r["team"] = normalize_team(r["team"])
    return [r for r in rows if r["team"] in VALID_TEAMS]

def safe_float(val, default=None):
    try:
        v = float(val)
        return v if v == v else default  # NaN check
    except (ValueError, TypeError):
        return default

def safe_int(val, default=None):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


# --- Columns to extract from each CSV ---

EFFICIENCY_COLS = {
    "pass_att_g": float, "rush_att_g": float, "sacks_g": float, "plays_g": float,
    "cmp_pct": float, "ypa": float, "ypc": float, "sack_rate": float,
    "pass_epa_play": float, "rush_epa_play": float,
    "air_yds_pct": float, "yac_pct": float,
    "pass_td_rate": float, "rush_td_rate": float, "int_rate": float,
    "cpoe": float,
    # Raw totals for context
    "pass_att": int, "completions": int, "pass_yds": int, "pass_td": int, "pass_int": int,
    "rush_att": int, "rush_yds": int, "rush_td": int,
}

RED_ZONE_COLS = {
    "rz_trips": int, "rz_pass_rate": float, "rz_rush_rate": float,
    "rz_td_rate": float, "rz_pass_td": int, "rz_rush_td": int,
    "rz_epa_play": float,
    "i10_plays": int, "i10_td": int, "i10_td_rate": float,
    "rz_trip_td_rate": float,
}

SCHEME_COLS = {
    "play_action_rate": float, "rpo_rate": float, "screen_rate": float,
    "no_huddle_rate": float, "motion_rate": float,
    "under_center_rate": float, "shotgun_rate": float, "pistol_rate": float,
}

PROE_COLS = {
    "neutral_pass_rate": float, "leading_pass_rate": float, "trailing_pass_rate": float,
    "total_pass_rate": float,
}

PRESSURE_COLS = {
    "pressure_rate": float, "blitz_rate_faced": float,
    "hurry_rate": float, "hit_rate": float,
    "drop_rate": float, "bad_throw_rate": float, "throwaway_rate": float,
    "pocket_time": float,
}

# All stat columns that should get a rank computed
ALL_RANKED_COLS = list(EFFICIENCY_COLS.keys()) + list(RED_ZONE_COLS.keys()) + \
                  list(SCHEME_COLS.keys()) + list(PROE_COLS.keys()) + list(PRESSURE_COLS.keys())

# Higher is better for these columns (rank 1 = highest value)
HIGHER_IS_BETTER = {
    "pass_att_g", "rush_att_g", "plays_g", "cmp_pct", "ypa", "ypc",
    "pass_epa_play", "rush_epa_play", "yac_pct",
    "pass_td_rate", "rush_td_rate", "cpoe",
    "pass_att", "completions", "pass_yds", "pass_td", "rush_att", "rush_yds", "rush_td",
    "rz_trips", "rz_td_rate", "rz_pass_td", "rz_rush_td", "rz_epa_play",
    "i10_td", "i10_td_rate", "rz_trip_td_rate",
    "play_action_rate", "motion_rate",
    "neutral_pass_rate", "leading_pass_rate", "trailing_pass_rate", "total_pass_rate",
    "pocket_time",
}

# Lower is better for these (rank 1 = lowest value)
LOWER_IS_BETTER = {
    "sacks_g", "sack_rate", "int_rate",
    "pressure_rate", "blitz_rate_faced", "hurry_rate", "hit_rate",
    "drop_rate", "bad_throw_rate", "throwaway_rate",
}

# Neutral (rank by value descending but no good/bad judgment)
# Everything else gets ranked higher = rank 1 by default


def merge_data() -> dict[tuple[str, int], dict]:
    """Merge all CSV files into a single dict keyed by (team, season)."""
    merged: dict[tuple[str, int], dict] = {}

    def ensure_key(team, season):
        key = (team, int(season))
        if key not in merged:
            merged[key] = {"team": team, "season": int(season)}
        return key

    # 1. Efficiency
    for r in read_csv("team_efficiency.csv"):
        key = ensure_key(r["team"], r["season"])
        for col, typ in EFFICIENCY_COLS.items():
            val = safe_float(r.get(col)) if typ == float else safe_int(r.get(col))
            merged[key][col] = val

    # 2. Red zone
    for r in read_csv("red_zone_stats.csv"):
        key = ensure_key(r["team"], r["season"])
        for col, typ in RED_ZONE_COLS.items():
            val = safe_float(r.get(col)) if typ == float else safe_int(r.get(col))
            merged[key][col] = val

    # 3. Scheme
    for r in read_csv("scheme_stats.csv"):
        key = ensure_key(r["team"], r["season"])
        for col, typ in SCHEME_COLS.items():
            val = safe_float(r.get(col)) if typ == float else safe_int(r.get(col))
            merged[key][col] = val

    # 4. PROE
    for r in read_csv("neutral_pass_rate.csv"):
        key = ensure_key(r["team"], r["season"])
        for col, typ in PROE_COLS.items():
            val = safe_float(r.get(col)) if typ == float else safe_int(r.get(col))
            merged[key][col] = val

    # 5. Pressure
    for r in read_csv("sack_pressure_stats.csv"):
        key = ensure_key(r["team"], r["season"])
        for col, typ in PRESSURE_COLS.items():
            val = safe_float(r.get(col)) if typ == float else safe_int(r.get(col))
            merged[key][col] = val

    return merged


def compute_ranks(merged: dict[tuple[str, int], dict]) -> None:
    """Add rank columns (1-32) for each stat within each season."""
    # Group by season
    by_season: dict[int, list[dict]] = defaultdict(list)
    for (team, season), row in merged.items():
        by_season[season].append(row)

    for season, rows in by_season.items():
        for col in ALL_RANKED_COLS:
            # Get values for this column, filtering None
            vals = [(i, r.get(col)) for i, r in enumerate(rows) if r.get(col) is not None]
            if not vals:
                continue

            # Sort: higher is better = descending, lower is better = ascending
            if col in LOWER_IS_BETTER:
                vals.sort(key=lambda x: x[1])  # ascending (lowest = rank 1)
            else:
                vals.sort(key=lambda x: x[1], reverse=True)  # descending (highest = rank 1)

            rank_col = f"{col}_rank"
            for rank, (idx, _) in enumerate(vals, 1):
                rows[idx][rank_col] = rank


def main():
    dry_run = "--dry-run" in sys.argv
    csv_only = "--csv-only" in sys.argv

    print("Merging nflverse CSVs...")
    merged = merge_data()
    print(f"  {len(merged)} team-season rows")

    print("Computing league ranks...")
    compute_ranks(merged)

    # Build flat list sorted by season desc, team asc
    rows = sorted(merged.values(), key=lambda r: (-r["season"], r["team"]))

    # Verify a sample
    cin_2025 = next((r for r in rows if r["team"] == "CIN" and r["season"] == 2025), None)
    if cin_2025:
        print(f"\n  CIN 2025 sample:")
        print(f"    pass_epa_play={cin_2025.get('pass_epa_play')} (rank {cin_2025.get('pass_epa_play_rank')})")
        print(f"    pressure_rate={cin_2025.get('pressure_rate')} (rank {cin_2025.get('pressure_rate_rank')})")
        print(f"    motion_rate={cin_2025.get('motion_rate')} (rank {cin_2025.get('motion_rate_rank')})")
        print(f"    rz_trip_td_rate={cin_2025.get('rz_trip_td_rate')} (rank {cin_2025.get('rz_trip_td_rate_rank')})")

    # Write CSV
    if rows:
        all_cols = sorted(set().union(*(r.keys() for r in rows)))
        # Put team, season first
        all_cols = ["team", "season"] + [c for c in all_cols if c not in ("team", "season")]

        with open(OUTPUT_CSV, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=all_cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"\nWrote {len(rows)} rows to {OUTPUT_CSV}")
        print(f"  Columns: {len(all_cols)} ({sum(1 for c in all_cols if c.endswith('_rank'))} rank columns)")

    if csv_only:
        print("\n--csv-only mode, skipping Supabase upload")
        return

    if dry_run:
        print(f"\n--dry-run mode, would upload {len(rows)} rows to team_advanced_stats")
        return

    # Upload to Supabase
    import requests as req

    print("\nUploading to Supabase team_advanced_stats...")
    service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", SUPABASE_SERVICE_KEY)
    if not service_key:
        print("ERROR: SUPABASE_SERVICE_ROLE_KEY not set. Cannot upload.")
        sys.exit(1)

    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    # Ensure all rows have the same keys (REST API requires uniform objects per batch)
    all_keys = sorted(set().union(*(r.keys() for r in rows)))
    rows = [{k: r.get(k) for k in all_keys} for r in rows]

    batch_size = 50
    uploaded = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]

        resp = req.post(
            f"{SUPABASE_URL}/rest/v1/team_advanced_stats",
            headers=headers,
            json=batch,
        )
        if resp.status_code not in (200, 201):
            print(f"  ERROR {resp.status_code}: {resp.text[:200]}")
            sys.exit(1)

        uploaded += len(batch)
        print(f"  {uploaded}/{len(rows)} rows uploaded")

    print(f"\nDone! {uploaded} rows loaded to team_advanced_stats")


if __name__ == "__main__":
    main()
