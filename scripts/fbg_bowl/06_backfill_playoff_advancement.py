"""Build advancement_{year}.csv from ground-truth playoff sheets/exports.

The FBG Bowl playoff had weekly cuts that were never loaded into Supabase:
  2024: 758 qualifiers -> 542 semifinalists (wk16) -> ~136 finalists (wk17)
  2025: 1457 qualifiers -> 729 semifinalists  -> 365 finalists
Final rank = reg-season PPG + wk15 + wk16 + wk17 points, ranked among finalists.

Ground truth staged in data/fbg_bowl/:
  semis_2024.csv / finalstandings_2024.csv       (from Dan's Google Sheets)
  semis_2025.csv / finalists_2025.csv / finalstandings_2025.csv  (from R .rds)

Output: data/fbg_bowl/advancement_{year}.csv with
  roster_id, made_semi, made_final, final_rank, final_total
(roster_id = internal fbg_bowl_rosters.id). Fails loudly on unmatched rows.

Usage:
  python3 scripts/fbg_bowl/06_backfill_playoff_advancement.py --year 2025
  python3 scripts/fbg_bowl/06_backfill_playoff_advancement.py --year 2025 --write-db
    (--write-db also PATCHes fbg_bowl_playoff_results.final_rank for finalists)
"""

import csv
import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))
from shared import supa_get

DATA = os.path.join(os.path.dirname(__file__), "..", "..", "data", "fbg_bowl")


def read_csv(name):
    with open(os.path.join(DATA, name)) as f:
        return list(csv.DictReader(f))


def norm_league(name):
    """Normalize 2024 league names: collapse whitespace, unify division padding."""
    n = re.sub(r"\s+", " ", (name or "").strip())
    m = re.match(r"FBG Bowl - Division (\d+)$", n)
    if m:
        return f"FBG Bowl - Division {int(m.group(1))}"
    return n


def load_rosters(year):
    leagues = supa_get("fbg_bowl_leagues", select="id,sleeper_id,name", params=f"year=eq.{year}")
    rosters = supa_get("fbg_bowl_rosters", select="id,league_id,sleeper_roster_id,display_name")
    lids = {lg["id"] for lg in leagues}
    rosters = [r for r in rosters if r["league_id"] in lids]
    return leagues, rosters


def match_2025():
    leagues, rosters = load_rosters(2025)
    league_by_sleeper = {lg["sleeper_id"]: lg["id"] for lg in leagues}
    roster_by_key = {(r["league_id"], r["sleeper_roster_id"]): r["id"] for r in rosters}

    def key(row):
        lid = league_by_sleeper.get(str(row["league_id"]))
        if lid is None:
            return None
        return roster_by_key.get((lid, int(row["roster_id"])))

    semis = read_csv("semis_2025.csv")
    finals = read_csv("finalists_2025.csv")
    standings = read_csv("finalstandings_2025.csv")

    semi_ids, missed = set(), []
    for row in semis:
        rid = key(row)
        (semi_ids.add(rid) if rid else missed.append(row))
    final_rows = {}
    for row in standings:
        rid = key(row)
        if rid:
            final_rows[rid] = {"final_rank": int(row["rank"]), "final_total": float(row["total"])}
        else:
            missed.append(row)
    final_ids_check = {key(r) for r in finals}
    assert final_ids_check - {None} == set(final_rows), "finalists_2025 != finalstandings_2025 membership"
    return semi_ids, final_rows, missed, len(semis), len(standings)


def match_2024():
    leagues, rosters = load_rosters(2024)
    league_by_name = {}
    unnamed = []
    for lg in leagues:
        n = norm_league(lg["name"])
        if n:
            league_by_name.setdefault(n, lg["id"])
        else:
            unnamed.append(lg["id"])
    # Sheet quirks vs DB: the staff league is "FBG Staffer League" in Sleeper,
    # and Division 0144's Sleeper metadata came back nameless (single unnamed league).
    league_by_name["FBG Bowl - Division Staff"] = league_by_name.get("FBG Staffer League")
    if len(unnamed) == 1:
        league_by_name["FBG Bowl - Division 144"] = unnamed[0]
    roster_by_key = {}
    for r in rosters:
        roster_by_key.setdefault((r["league_id"], (r["display_name"] or "").strip().lower()), r["id"])

    # Fallback for display names changed after the season: match on the
    # week-14 record + points the sheet carries.
    wk14 = supa_get("fbg_bowl_standings",
                    select="roster_id,league_id,wins,losses,pts_for", params="week=eq.14")
    roster_ids = {r["id"] for r in rosters}
    record_by_key = {}
    for s in wk14:
        if s["roster_id"] in roster_ids:
            k = (s["league_id"], int(s["wins"] or 0), int(s["losses"] or 0), round(float(s["pts_for"] or 0), 2))
            record_by_key.setdefault(k, []).append(s["roster_id"])

    def key(row):
        lid = league_by_name.get(norm_league(row["league_name"]))
        if lid is None:
            return None
        rid = roster_by_key.get((lid, row["display_name"].strip().lower()))
        if rid:
            return rid
        k = (lid, int(row["wins"]), int(row["losses"]), round(float(row["points"]), 2))
        hits = record_by_key.get(k, [])
        if len(hits) == 1:
            print(f"  record-fallback: {row['display_name']} ({row['league_name']}) -> roster {hits[0]}")
            return hits[0]
        return None

    semis = read_csv("semis_2024.csv")
    standings = read_csv("finalstandings_2024.csv")
    standings.sort(key=lambda r: -float(r["week17_total"]))

    semi_ids, missed = set(), []
    for row in semis:
        rid = key(row)
        (semi_ids.add(rid) if rid else missed.append(row))
    final_rows = {}
    for rank, row in enumerate(standings, 1):
        rid = key(row)
        if rid:
            final_rows[rid] = {"final_rank": rank, "final_total": float(row["week17_total"])}
        else:
            missed.append(row)
    return semi_ids, final_rows, missed, len(semis), len(standings)


def main():
    year = 2025
    for i, arg in enumerate(sys.argv):
        if arg == "--year" and i + 1 < len(sys.argv):
            year = int(sys.argv[i + 1])

    semi_ids, final_rows, missed, n_semis, n_finals = match_2025() if year == 2025 else match_2024()

    print(f"{year}: semis matched {len(semi_ids)}/{n_semis}, finalists matched {len(final_rows)}/{n_finals}")
    if missed:
        print("UNMATCHED ROWS:")
        for row in missed[:20]:
            print("  ", {k: row.get(k) for k in ("display_name", "league_name", "league_id", "roster_id", "user_id", "team") if row.get(k)})
        sys.exit(f"{len(missed)} unmatched rows — fix matching before proceeding")

    not_semi = set(final_rows) - semi_ids
    assert not not_semi, f"{len(not_semi)} finalists missing from semis list"

    out = os.path.join(DATA, f"advancement_{year}.csv")
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["roster_id", "made_semi", "made_final", "final_rank", "final_total"])
        for rid in sorted(semi_ids):
            fr = final_rows.get(rid)
            w.writerow([rid, 1, 1 if fr else 0,
                        fr["final_rank"] if fr else "", fr["final_total"] if fr else ""])
    print(f"wrote {out} ({len(semi_ids)} rows, {len(final_rows)} finalists)")

    if "--write-db" in sys.argv:
        import json
        import urllib.request
        from shared import SUPABASE_URL, SUPABASE_SERVICE_KEY
        print(f"writing final_rank to fbg_bowl_playoff_results for {len(final_rows)} finalists...")
        for i, (rid, fr) in enumerate(sorted(final_rows.items())):
            url = f"{SUPABASE_URL}/rest/v1/fbg_bowl_playoff_results?roster_id=eq.{rid}"
            req = urllib.request.Request(
                url,
                data=json.dumps({"final_rank": fr["final_rank"]}).encode(),
                method="PATCH",
                headers={"apikey": SUPABASE_SERVICE_KEY,
                         "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                         "Content-Type": "application/json", "Prefer": "return=minimal"},
            )
            urllib.request.urlopen(req).read()
            if (i + 1) % 100 == 0:
                print(f"  {i + 1}/{len(final_rows)}")
        print("  done")


if __name__ == "__main__":
    main()
