"""Compute FBG Bowl meta-scores from standings and playoff results.

Usage:
  python3 scripts/fbg_bowl/04_compute_fbg_scores.py [--year 2025]

Scoring system:
  +1   per regular-season win (wins through week 14)
  +35  for 1st place in your league (week 14 standings)
  +10  for 2nd place in your league (week 14 standings)
  +35  for making the semifinals (score in week 16)
  +35  for making the finals     (score in week 17)
  +300/200/150/125/100/85/70/55/45/35  for top 10 overall final rank

Final rank is based on: reg_season_ppg × 14 + week15_pts + week16_pts + week17_pts
(i.e., total season + playoff points, where reg_season contribution = total regular season pts)

Clears fbg_bowl_scores for the year before re-inserting (idempotent).
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from shared import supa_get, supa_batch_insert, supa_delete

YEAR = 2025

TOP10_BONUS = {1: 300, 2: 200, 3: 150, 4: 125, 5: 100,
               6: 85,  7: 70,  8: 55,  9: 45,  10: 35}


def main():
    global YEAR
    for i, arg in enumerate(sys.argv):
        if arg == "--year" and i + 1 < len(sys.argv):
            YEAR = int(sys.argv[i + 1])

    print(f"Computing FBG Bowl scores for {YEAR}...")

    # ── Load week-14 standings ─────────────────────────────────────────────────
    standings_14 = supa_get(
        "fbg_bowl_standings",
        select="roster_id,league_id,wins,pts_for,league_rank,qualified_playoffs",
        params="week=eq.14",
    )
    # Filter to this year's leagues
    leagues = supa_get("fbg_bowl_leagues", select="id", params=f"year=eq.{YEAR}")
    year_lid_set = {lg["id"] for lg in leagues}
    standings_14 = [s for s in standings_14 if s["league_id"] in year_lid_set]
    print(f"  Week-14 standings rows: {len(standings_14)}")

    # ── Load playoff results ───────────────────────────────────────────────────
    playoff_rows = supa_get(
        "fbg_bowl_playoff_results",
        select="roster_id,week,pts_for",
    )
    # Pivot: roster_id → {15: pts, 16: pts, 17: pts}
    playoff_pts = {}
    for row in playoff_rows:
        rid = row["roster_id"]
        wk = row["week"]
        pts = float(row["pts_for"] or 0)
        if rid not in playoff_pts:
            playoff_pts[rid] = {}
        playoff_pts[rid][wk] = pts

    made_w16 = {rid for rid, wks in playoff_pts.items() if 16 in wks and wks[16] > 0}
    made_w17 = {rid for rid, wks in playoff_pts.items() if 17 in wks and wks[17] > 0}
    print(f"  Rosters with week-16 score: {len(made_w16)}")
    print(f"  Rosters with week-17 score: {len(made_w17)}")

    # ── Compute total score for final rank ─────────────────────────────────────
    # Total score = reg_season_pts + w15 + w16 + w17
    roster_totals = {}
    for s in standings_14:
        rid = s["roster_id"]
        reg_pts = float(s["pts_for"] or 0)
        play_data = playoff_pts.get(rid, {})
        total = reg_pts + play_data.get(15, 0) + play_data.get(16, 0) + play_data.get(17, 0)
        roster_totals[rid] = total

    # Rank ALL rosters by total (including non-playoff teams, who just have reg pts)
    all_rids_ranked = sorted(roster_totals.keys(), key=lambda r: -roster_totals[r])
    final_rank_map = {rid: rank for rank, rid in enumerate(all_rids_ranked, 1)}

    # ── Compute meta-scores ────────────────────────────────────────────────────
    score_rows = []
    for s in standings_14:
        rid = s["roster_id"]
        wins = int(s["wins"] or 0)
        lrank = int(s["league_rank"] or 99)

        reg_wins = wins
        league_rank_bonus = 35 if lrank == 1 else (10 if lrank == 2 else 0)
        semi_bonus = 35 if rid in made_w16 else 0
        finals_bonus = 35 if rid in made_w17 else 0
        final_rank = final_rank_map.get(rid, 0)
        top10_bonus = TOP10_BONUS.get(final_rank, 0)

        total = reg_wins + league_rank_bonus + semi_bonus + finals_bonus + top10_bonus

        score_rows.append({
            "roster_id": rid,
            "year": YEAR,
            "reg_season_wins": reg_wins,
            "league_rank_bonus": league_rank_bonus,
            "semi_bonus": semi_bonus,
            "finals_bonus": finals_bonus,
            "top10_bonus": top10_bonus,
            "total_score": total,
            "overall_rank": None,  # filled after sorting
        })

    # Rank by total_score DESC
    score_rows.sort(key=lambda r: -r["total_score"])
    for i, row in enumerate(score_rows, 1):
        row["overall_rank"] = i

    print(f"\n  Score rows: {len(score_rows)}")
    print(f"  Top 10:")
    for row in score_rows[:10]:
        print(f"    Rank {row['overall_rank']:4d}: score={row['total_score']:4d}  "
              f"(wins={row['reg_season_wins']}, lg_bonus={row['league_rank_bonus']}, "
              f"semi={row['semi_bonus']}, finals={row['finals_bonus']}, top10={row['top10_bonus']})")

    # Clear old scores for this year, then insert
    print(f"\nClearing existing scores for {YEAR}...")
    # Need to filter by year - delete rows for this year's rosters
    existing = supa_get("fbg_bowl_scores", select="id", params=f"year=eq.{YEAR}")
    if existing:
        # Supabase REST DELETE with year filter
        from shared import SUPABASE_URL, SUPABASE_SERVICE_KEY
        import urllib.request, json
        url = f"{SUPABASE_URL}/rest/v1/fbg_bowl_scores?year=eq.{YEAR}"
        req = urllib.request.Request(
            url,
            headers={
                "apikey": SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "Prefer": "return=minimal",
            },
            method="DELETE",
        )
        with urllib.request.urlopen(req) as resp:
            resp.read()
        print(f"  Deleted {len(existing)} existing rows")

    inserted = supa_batch_insert("fbg_bowl_scores", score_rows)
    print(f"  Inserted {len(inserted)} score rows")
    print("Done.")


if __name__ == "__main__":
    main()
