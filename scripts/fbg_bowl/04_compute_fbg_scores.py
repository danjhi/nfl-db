"""Compute FBG Bowl meta-scores from standings and playoff advancement.

Usage:
  python3 scripts/fbg_bowl/04_compute_fbg_scores.py [--year 2025]

Scoring system:
  +1   per regular-season win (wins through week 14)
  +35  for 1st place in your league (week 14 standings)
  +10  for 2nd place in your league (week 14 standings)
  +35  for making the semifinals (surviving the week-15 cut)
  +35  for making the finals     (surviving the week-16 cut)
  +300/200/150/125/100/85/70/55/45/35  for top 10 overall final rank

The playoff has weekly CUTS — having a Sleeper score in week 16/17 does NOT
mean a team advanced (Sleeper scores everyone). Advancement and final rank
come from data/fbg_bowl/advancement_{year}.csv, built by
06_backfill_playoff_advancement.py from Dan's published sheets/R exports.
Final rank = reg-season PPG + wk15 + wk16 + wk17 points, ranked among
finalists only. (Historic bug fixed 2026-07-07: this script previously
awarded semi/finals bonuses to every qualifier and ranked everyone by TOTAL
season points + playoff points, which scrambled top-10 bonuses and ranks.)

Clears fbg_bowl_scores for the year before re-inserting (idempotent).
"""

import csv
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from shared import supa_get, supa_batch_insert, supa_delete

YEAR = 2025
DATA = os.path.join(os.path.dirname(__file__), "..", "..", "data", "fbg_bowl")

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

    # ── Load playoff advancement ground truth ──────────────────────────────────
    adv_path = os.path.join(DATA, f"advancement_{YEAR}.csv")
    if not os.path.exists(adv_path):
        sys.exit(f"Missing {adv_path} — run 06_backfill_playoff_advancement.py --year {YEAR} first")
    made_semi, made_final, final_rank_map, final_total_map = set(), set(), {}, {}
    with open(adv_path) as f:
        for row in csv.DictReader(f):
            rid = int(row["roster_id"])
            if row["made_semi"] == "1":
                made_semi.add(rid)
            if row["made_final"] == "1":
                made_final.add(rid)
                final_rank_map[rid] = int(row["final_rank"])
                final_total_map[rid] = float(row["final_total"])
    print(f"  Semifinalists: {len(made_semi)}, Finalists: {len(made_final)}")

    # ── Cross-validate ground truth against DB points ──────────────────────────
    # Final total should equal reg-season PPG + wk15 + wk16 + wk17 from our data.
    playoff_rows = supa_get("fbg_bowl_playoff_results", select="roster_id,week,pts_for")
    playoff_pts = {}
    for row in playoff_rows:
        playoff_pts.setdefault(row["roster_id"], {})[row["week"]] = float(row["pts_for"] or 0)
    pts14 = {s["roster_id"]: float(s["pts_for"] or 0) for s in standings_14}
    worst = (0.0, None)
    for rid in made_final:
        p = playoff_pts.get(rid, {})
        recomputed = pts14.get(rid, 0) / 14 + p.get(15, 0) + p.get(16, 0) + p.get(17, 0)
        diff = abs(recomputed - final_total_map[rid])
        if diff > worst[0]:
            worst = (diff, rid)
    print(f"  Cross-check vs DB (PPG + wk15-17): max |diff| = {worst[0]:.2f} pts (roster {worst[1]})")
    if worst[0] > 2.0:
        sys.exit("  Ground-truth totals disagree with DB recomputation beyond tolerance — investigate")

    # ── Compute meta-scores ────────────────────────────────────────────────────
    score_rows = []
    for s in standings_14:
        rid = s["roster_id"]
        wins = int(s["wins"] or 0)
        lrank = int(s["league_rank"] or 99)

        reg_wins = wins
        league_rank_bonus = 35 if lrank == 1 else (10 if lrank == 2 else 0)
        semi_bonus = 35 if rid in made_semi else 0
        finals_bonus = 35 if rid in made_final else 0
        final_rank = final_rank_map.get(rid)
        top10_bonus = TOP10_BONUS.get(final_rank, 0) if final_rank else 0

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

    # Rank by total_score DESC; break ties by playoff finish, then reg-season pts
    score_rows.sort(key=lambda r: (
        -r["total_score"],
        final_rank_map.get(r["roster_id"], 10**6),
        -pts14.get(r["roster_id"], 0),
    ))
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
