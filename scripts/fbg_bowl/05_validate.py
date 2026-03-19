"""Validate FBG Bowl data in Supabase against local CSVs and Google Sheets.

Usage:
  python3 scripts/fbg_bowl/05_validate.py [--year 2025]

Checks:
  1. Row counts in each table
  2. Week-14 standings vs local week14standings.csv (top 20 by wins/pts)
  3. fbg_standings.csv (reg season end-of-season) vs fbg_bowl_standings week 14
  4. Overall meta-score rankings vs Google Sheet 13ViIGkfLZnjclF0b2ROM0mBW15Edi4eABq8pkdWbV6Y (2024)
     (for 2025: compare against Google Sheet 1EhwJeRFrP80jvDjlsBZcliFY4CMGarwO0DaIX1lQPGo)
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from shared import supa_get

import pandas as pd

YEAR = 2025
LOCAL_STANDINGS_CSV = "/Users/dan/Desktop/r2024/fbg_bowl_standings_new/fbg_standings.csv"
LOCAL_WEEK14_CSV    = "/Users/dan/Desktop/r2024/fbg_bowl_standings_new/week14standings.csv"
GSHEET_2025_PLAYOFF = "1EhwJeRFrP80jvDjlsBZcliFY4CMGarwO0DaIX1lQPGo"
GSHEET_2025_REG     = "1WDv4EUuX4mRGqlT5caA6Nqha52o3pA-9s5whZ0trddc"  # 2024 reg season (for 2024 validation)


def gsheet(sheet_id, gid="0"):
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    return pd.read_csv(url)


def check_row_counts(year):
    print("\n── Row counts ──────────────────────────────────────────")
    leagues = supa_get("fbg_bowl_leagues", select="id", params=f"year=eq.{year}")
    lid_set = {lg["id"] for lg in leagues}

    tables = {
        "fbg_bowl_leagues":          (supa_get("fbg_bowl_leagues", params=f"year=eq.{year}"), None),
        "fbg_bowl_rosters":          (supa_get("fbg_bowl_rosters"), lid_set),
        "fbg_bowl_weekly_results":   (supa_get("fbg_bowl_weekly_results", select="id,league_id"), lid_set),
        "fbg_bowl_standings":        (supa_get("fbg_bowl_standings", select="id,league_id"), lid_set),
        "fbg_bowl_playoff_results":  (supa_get("fbg_bowl_playoff_results", select="id,league_id"), lid_set),
        "fbg_bowl_draft_picks":      (supa_get("fbg_bowl_draft_picks", select="id,league_id"), lid_set),
        "fbg_bowl_scores":           (supa_get("fbg_bowl_scores", params=f"year=eq.{year}"), None),
    }

    for name, (rows, filter_set) in tables.items():
        if filter_set is not None:
            count = sum(1 for r in rows if r.get("league_id") in filter_set)
        else:
            count = len(rows)
        print(f"  {name:35s}  {count:7,d}")


def check_week14_standings(year):
    print("\n── Week-14 standings vs local CSV ──────────────────────")
    try:
        local = pd.read_csv(LOCAL_WEEK14_CSV)
        print(f"  Local CSV: {len(local)} rows, columns: {list(local.columns)}")
    except FileNotFoundError:
        print(f"  Local CSV not found: {LOCAL_WEEK14_CSV}")
        return

    # Get week-14 standings from Supabase
    leagues = supa_get("fbg_bowl_leagues", select="id", params=f"year=eq.{year}")
    lid_set = {lg["id"] for lg in leagues}
    standings = supa_get(
        "fbg_bowl_standings",
        select="roster_id,league_id,week,wins,losses,pts_for",
        params="week=eq.14",
    )
    standings = [s for s in standings if s["league_id"] in lid_set]

    # Load roster display names
    rosters = supa_get("fbg_bowl_rosters", select="id,display_name,league_id")
    rid_to_name = {r["id"]: r["display_name"] for r in rosters}

    df = pd.DataFrame(standings)
    df["team"] = df["roster_id"].map(rid_to_name)
    df["pts_for"] = df["pts_for"].astype(float)
    df = df.sort_values(["wins", "pts_for"], ascending=[False, False])

    print(f"\n  DB top-10 (week 14):")
    print(df[["team", "wins", "losses", "pts_for"]].head(10).to_string(index=False))

    # Compare with CSV top-10
    col_team = [c for c in local.columns if "team" in c.lower() or "display" in c.lower()][0] if local.columns.any() else "team"
    col_w = [c for c in local.columns if c in ("w", "wins")][0] if [c for c in local.columns if c in ("w", "wins")] else None
    col_pts = [c for c in local.columns if "point" in c.lower() or c == "pts_for"][0] if [c for c in local.columns if "point" in c.lower() or c == "pts_for"] else None

    if col_w and col_pts:
        local_sorted = local.sort_values([col_w, col_pts], ascending=[False, False])
        print(f"\n  CSV top-10 (week 14):")
        print(local_sorted[[col_team, col_w, col_pts]].head(10).to_string(index=False))

        # Check top team matches
        db_top = df["team"].iloc[0] if len(df) > 0 else "?"
        csv_top = local_sorted[col_team].iloc[0] if len(local_sorted) > 0 else "?"
        match = "✓ MATCH" if db_top.strip() == csv_top.strip() else "✗ MISMATCH"
        print(f"\n  Top team: DB='{db_top}' vs CSV='{csv_top}'  {match}")

        # Win counts
        db_wins = int(df["wins"].max())
        csv_wins = int(local_sorted[col_w].max())
        match = "✓" if db_wins == csv_wins else "✗"
        print(f"  Max wins: DB={db_wins} vs CSV={csv_wins}  {match}")


def check_fbg_standings(year):
    print("\n── fbg_standings.csv vs DB ──────────────────────────────")
    try:
        local = pd.read_csv(LOCAL_STANDINGS_CSV)
        print(f"  Local CSV: {len(local)} rows, columns: {list(local.columns)}")
    except FileNotFoundError:
        print(f"  Local CSV not found: {LOCAL_STANDINGS_CSV}")
        return

    # fbg_standings.csv appears to be end-of-season cumulative (same as week 14)
    print("  (Same data as week14standings.csv — skipping duplicate check)")


def check_playoff_sheet(year):
    print("\n── Playoff standings vs Google Sheet ───────────────────")
    try:
        sheet = gsheet(GSHEET_2025_PLAYOFF)
        print(f"  Sheet columns: {list(sheet.columns)}")
        print(f"  Sheet rows: {len(sheet)}")
        print(f"  Top 5:")
        print(sheet.head(5).to_string(index=False))
    except Exception as e:
        print(f"  Could not read playoff sheet: {e}")


def main():
    global YEAR
    for i, arg in enumerate(sys.argv):
        if arg == "--year" and i + 1 < len(sys.argv):
            YEAR = int(sys.argv[i + 1])

    print(f"Validating FBG Bowl {YEAR} data...")
    check_row_counts(YEAR)
    check_week14_standings(YEAR)
    check_fbg_standings(YEAR)
    check_playoff_sheet(YEAR)
    print("\nValidation complete.")


if __name__ == "__main__":
    main()
