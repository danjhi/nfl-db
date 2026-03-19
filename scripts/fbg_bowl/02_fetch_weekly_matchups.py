"""Fetch weekly matchup results (weeks 1–17) and compute standings.

Usage:
  python3 scripts/fbg_bowl/02_fetch_weekly_matchups.py [--year 2025] [--weeks 1-14]
  python3 scripts/fbg_bowl/02_fetch_weekly_matchups.py --year 2025 --playoff

Modes:
  Default (no --playoff): fetches weeks 1–14 (or --weeks range), loads:
    - fbg_bowl_weekly_results (per-week W/L/pts)
    - fbg_bowl_standings (cumulative, including qualified_playoffs flag after week 14)

  --playoff: fetches weeks 15–17 for leagues with qualified playoff teams only,
    loads fbg_bowl_playoff_results.

Checkpointable by league×week: skips leagues already loaded for a given week.
Volume: ~417 leagues × 14 weeks = ~5,800 calls (~15 min at 0.15s)
        ~250 playoff leagues × 3 weeks = ~750 calls  (~2 min)
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from shared import (
    sleeper_get, supa_get, supa_batch_insert, save_checkpoint, load_checkpoint,
    compute_week_results
)

YEAR = 2025
REG_SEASON_WEEKS = list(range(1, 15))
PLAYOFF_WEEKS = [15, 16, 17]

# Playoff qualification threshold (points over full regular season)
PLAYOFF_PTS_THRESHOLD = 1920


def get_roster_map(year):
    """Return {(sleeper_league_id, sleeper_roster_id): internal_roster_id} and
       {sleeper_league_id: internal_league_id}."""
    leagues = supa_get("fbg_bowl_leagues", select="id,sleeper_id", params=f"year=eq.{year}")
    league_map = {lg["sleeper_id"]: lg["id"] for lg in leagues}
    lid_to_sleeper = {lg["id"]: lg["sleeper_id"] for lg in leagues}

    rosters = supa_get(
        "fbg_bowl_rosters",
        select="id,league_id,sleeper_roster_id",
    )
    # Only for this year
    year_lid_set = set(league_map.values())
    roster_map = {}
    for r in rosters:
        if r["league_id"] in year_lid_set:
            sleeper_lid = lid_to_sleeper[r["league_id"]]
            roster_map[(sleeper_lid, r["sleeper_roster_id"])] = r["id"]

    return league_map, roster_map


def fetch_regular_season(year, weeks):
    print(f"Fetching regular season weeks {weeks[0]}–{weeks[-1]} for {year}...")
    league_map, roster_map = get_roster_map(year)
    print(f"  Leagues: {len(league_map)}, Rosters: {len(roster_map)}")

    # Check which (league_id, week) already loaded
    existing = supa_get("fbg_bowl_weekly_results", select="league_id,week")
    loaded = {(r["league_id"], r["week"]) for r in existing}
    print(f"  Already loaded: {len(loaded)} league×week combos")

    all_weekly_rows = []
    errors = 0
    total_calls = 0

    for wk in weeks:
        week_rows = []
        for sleeper_lid, internal_lid in league_map.items():
            if (internal_lid, wk) in loaded:
                continue
            matchups = sleeper_get(f"league/{sleeper_lid}/matchups/{wk}")
            total_calls += 1
            if not matchups:
                errors += 1
                continue
            results = compute_week_results(matchups)
            for res in results:
                rid_key = (sleeper_lid, res["roster_id"])
                internal_rid = roster_map.get(rid_key)
                if internal_rid is None:
                    continue
                week_rows.append({
                    "roster_id": internal_rid,
                    "league_id": internal_lid,
                    "week": wk,
                    "pts_for": res["pts_for"],
                    "pts_against": res["pts_against"],
                    "win": res["win"],
                    "loss": res["loss"],
                    "tie": res["tie"],
                })

        if week_rows:
            supa_batch_insert("fbg_bowl_weekly_results", week_rows)
            all_weekly_rows.extend(week_rows)

        print(f"  Week {wk:2d}: {len(week_rows):5d} rows inserted  ({errors} errors, {total_calls} calls)")

    print(f"\n  Total weekly rows inserted: {len(all_weekly_rows)}")
    return all_weekly_rows


def compute_and_load_standings(year, final_week=14):
    """Pull all weekly results, compute cumulative standings, load fbg_bowl_standings."""
    print(f"\nComputing cumulative standings through week {final_week}...")
    league_map, roster_map = get_roster_map(year)
    internal_to_sleeper_lid = {v: k for k, v in league_map.items()}

    # Pull all weekly results for this year
    all_weekly = supa_get(
        "fbg_bowl_weekly_results",
        select="roster_id,league_id,week,pts_for,pts_against,win,loss,tie",
    )
    year_lid_set = set(league_map.values())
    weekly = [r for r in all_weekly if r["league_id"] in year_lid_set and r["week"] <= final_week]

    # Accumulate per roster
    from collections import defaultdict
    roster_totals = defaultdict(lambda: {
        "wins": 0, "losses": 0, "ties": 0, "pts_for": 0.0, "pts_against": 0.0, "league_id": None
    })
    roster_by_week = defaultdict(lambda: defaultdict(dict))  # roster_id → week → totals

    # Process week by week to compute cumulative
    for row in weekly:
        rid = row["roster_id"]
        wk = row["week"]
        roster_by_week[rid][wk] = row

    # For each roster, compute cumulative through each week
    standings_rows = []
    for rid, week_data in roster_by_week.items():
        cum_wins = cum_losses = cum_ties = 0
        cum_pf = cum_pa = 0.0
        league_id = None
        for wk in sorted(week_data.keys()):
            d = week_data[wk]
            league_id = d["league_id"]
            cum_wins += 1 if d["win"] else 0
            cum_losses += 1 if d["loss"] else 0
            cum_ties += 1 if d["tie"] else 0
            cum_pf += float(d["pts_for"] or 0)
            cum_pa += float(d["pts_against"] or 0)
            standings_rows.append({
                "roster_id": rid,
                "league_id": league_id,
                "week": wk,
                "wins": cum_wins,
                "losses": cum_losses,
                "pts_for": round(cum_pf, 2),
                "pts_against": round(cum_pa, 2),
                "league_rank": None,  # filled below for final week
                "qualified_playoffs": None,  # filled below for final week
            })

    # For the final week, compute league_rank and qualified_playoffs
    final_week_rows = {r["roster_id"]: r for r in standings_rows if r["week"] == final_week}
    # Group by league
    from collections import defaultdict
    by_league = defaultdict(list)
    for rid, row in final_week_rows.items():
        by_league[row["league_id"]].append(row)

    for lid, rows in by_league.items():
        sorted_rows = sorted(rows, key=lambda r: (-r["wins"], -r["pts_for"]))
        for rank, row in enumerate(sorted_rows, 1):
            row["league_rank"] = rank
            # Qualify: top 2 per league OR pts_for >= threshold
            row["qualified_playoffs"] = rank <= 2 or row["pts_for"] >= PLAYOFF_PTS_THRESHOLD

    # Upsert standings (unique on roster_id, week)
    print(f"  Upserting {len(standings_rows)} standings rows...")
    # Delete existing first to avoid duplicates (re-runnable)
    year_league_ids = list(league_map.values())
    # Batch upsert via insert with on_conflict
    from shared import supa_upsert
    supa_upsert("fbg_bowl_standings", standings_rows, on_conflict="roster_id,week")
    print(f"  Done.")

    # Save playoff-qualified rosters for script 02 --playoff mode
    qualified = [
        {"roster_id": row["roster_id"], "league_id": row["league_id"],
         "pts_for": row["pts_for"], "wins": row["wins"]}
        for row in final_week_rows.values()
        if row.get("qualified_playoffs")
    ]
    save_checkpoint(f"playoff_qualified_{year}", qualified)
    print(f"  Playoff qualifiers: {len(qualified)}")
    return qualified


def fetch_playoff_weeks(year):
    """Fetch weeks 15–17 for leagues with qualified playoff teams."""
    print(f"\nFetching playoff weeks 15–17 for {year}...")
    league_map, roster_map = get_roster_map(year)

    qualified = load_checkpoint(f"playoff_qualified_{year}")
    if not qualified:
        print("  No playoff qualifiers found. Run regular season mode first.")
        return

    # Get unique leagues with playoff qualifiers
    playoff_league_ids = {r["league_id"] for r in qualified}
    qualified_roster_ids = {r["roster_id"] for r in qualified}
    internal_to_sleeper = {v: k for k, v in league_map.items()}

    print(f"  Leagues with playoff teams: {len(playoff_league_ids)}")
    print(f"  Qualified rosters: {len(qualified_roster_ids)}")

    # Check already loaded
    existing = supa_get("fbg_bowl_playoff_results", select="league_id,week")
    loaded = {(r["league_id"], r["week"]) for r in existing}

    all_rows = []
    errors = 0

    for wk in PLAYOFF_WEEKS:
        week_rows = []
        for internal_lid in playoff_league_ids:
            if (internal_lid, wk) in loaded:
                continue
            sleeper_lid = internal_to_sleeper.get(internal_lid)
            if not sleeper_lid:
                continue
            matchups = sleeper_get(f"league/{sleeper_lid}/matchups/{wk}")
            if not matchups:
                errors += 1
                continue
            for m in matchups:
                rid_key = (sleeper_lid, int(m.get("roster_id", 0)))
                internal_rid = roster_map.get(rid_key)
                if internal_rid is None:
                    continue
                # Only include qualified playoff teams
                if internal_rid not in qualified_roster_ids:
                    continue
                pts = float(m.get("points") or 0)
                week_rows.append({
                    "roster_id": internal_rid,
                    "league_id": internal_lid,
                    "week": wk,
                    "pts_for": round(pts, 2),
                    "final_rank": None,  # computed in script 04
                })

        if week_rows:
            supa_batch_insert("fbg_bowl_playoff_results", week_rows)
            all_rows.extend(week_rows)
        print(f"  Week {wk}: {len(week_rows)} rows ({errors} errors)")

    print(f"\nTotal playoff rows inserted: {len(all_rows)}")


def main():
    global YEAR
    playoff_mode = "--playoff" in sys.argv
    weeks = REG_SEASON_WEEKS

    for i, arg in enumerate(sys.argv):
        if arg == "--year" and i + 1 < len(sys.argv):
            YEAR = int(sys.argv[i + 1])
        if arg == "--weeks" and i + 1 < len(sys.argv):
            parts = sys.argv[i + 1].split("-")
            weeks = list(range(int(parts[0]), int(parts[-1]) + 1))

    if playoff_mode:
        fetch_playoff_weeks(YEAR)
    else:
        fetch_regular_season(YEAR, weeks)
        if max(weeks) >= 14:
            compute_and_load_standings(YEAR, final_week=14)


if __name__ == "__main__":
    main()
