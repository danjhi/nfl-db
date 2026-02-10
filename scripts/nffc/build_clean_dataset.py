"""
Build clean Rotowire OC dataset from raw NFFC API data + nflreadr enrichment.
Outputs tidy CSV files ready for Supabase upload.

Usage: python3 scripts/build_clean_dataset.py
"""

import csv
import json
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
NFLREADR_DIR = Path(__file__).parent.parent / "data" / "nflreadr"
OUT_DIR = Path(__file__).parent.parent / "data" / "clean"
OUT_DIR.mkdir(parents=True, exist_ok=True)

YEARS = range(2018, 2026)


def is_rotowire_oc(name):
    name_lower = name.lower()
    return "rotowire" in name_lower and "online" in name_lower


def load_nflreadr_players():
    """Load nflreadr ff_playerids keyed by sportradar_id."""
    players = {}
    with open(NFLREADR_DIR / "ff_playerids.csv", newline="") as f:
        for row in csv.DictReader(f):
            sr_id = row.get("sportradar_id", "").strip()
            if sr_id:
                players[sr_id] = row
    return players


def load_nflreadr_details():
    """Load nflreadr player details keyed by gsis_id."""
    details = {}
    with open(NFLREADR_DIR / "players.csv", newline="") as f:
        for row in csv.DictReader(f):
            gsis = row.get("gsis_id", "").strip()
            if gsis:
                details[gsis] = row
    return details


def build_leagues_and_teams():
    """Build clean leagues and league_teams tables."""
    leagues = []
    league_teams = []

    for year in YEARS:
        # Get Rotowire OC league IDs from the league list
        with open(RAW_DIR / f"historical_leagues_{year}.json") as f:
            all_leagues = json.load(f)
        roto_ids = {l["id"] for l in all_leagues if is_rotowire_oc(l["name"])}

        # Load league details
        with open(RAW_DIR / "league_details" / f"league_details_{year}.json") as f:
            details = json.load(f)

        for lid_str, detail in details.items():
            lid = int(lid_str)
            if lid not in roto_ids:
                continue

            league_info = detail.get("league", {})
            leagues.append({
                "league_id": lid,
                "year": year,
                "name": league_info.get("name", ""),
                "roster_size": league_info.get("rosterSize", 20),
                "third_round_reversal": bool(league_info.get("3rr", 0)),
                "draft_date": league_info.get("draft_date", ""),
                "draft_completed_date": league_info.get("draft_completed_date", ""),
            })

            for team in detail.get("teams", []):
                league_teams.append({
                    "league_id": lid,
                    "year": year,
                    "team_id": team["id"],
                    "draft_order": team.get("draft_order"),
                    "league_rank": team.get("league_rank"),
                    "league_points": team.get("league_points"),
                    "overall_rank": team.get("overall_rank"),
                    "overall_points": team.get("overall_points"),
                })

    return leagues, league_teams


def build_draft_picks(leagues_by_year_id):
    """Build clean draft picks. The API 'pick' field is already overall pick (1-240)."""
    picks = []
    num_teams = 12  # All Rotowire OC leagues are 12-team

    for year in YEARS:
        with open(RAW_DIR / "drafts" / f"drafts_{year}.json") as f:
            drafts = json.load(f)

        for lid_str, draft_data in drafts.items():
            lid = int(lid_str)
            if (year, lid) not in leagues_by_year_id:
                continue

            for pick_data in draft_data["picks"]:
                rd = pick_data["round"]
                overall_pick = pick_data["pick"]
                # Derive the within-round pick number (1-12)
                pick_in_round = overall_pick - (rd - 1) * num_teams

                picks.append({
                    "league_id": lid,
                    "year": year,
                    "round": rd,
                    "pick_in_round": pick_in_round,
                    "overall_pick": overall_pick,
                    "team_id": int(pick_data["team"]),
                    "player_id": pick_data["player"],
                    "timestamp": pick_data.get("timestamp", ""),
                    "pick_duration": pick_data.get("pick_duration", ""),
                })

    return picks


def build_players(picks, adp_all_years):
    """Build enriched player table from ADP data + nflreadr."""
    nflreadr_ids = load_nflreadr_players()
    nflreadr_details = load_nflreadr_details()

    # Collect all player UUIDs that appear in our picks
    pick_uuids = {p["player_id"] for p in picks}

    # Also include all UUIDs from ADP data
    adp_uuids = set()
    for entries in adp_all_years.values():
        for e in entries:
            adp_uuids.add(e["player"])

    all_uuids = pick_uuids | adp_uuids

    players = []
    matched = 0
    unmatched = 0

    # Build from ADP data (has name/pos/team/dob)
    adp_lookup = {}
    for year, entries in adp_all_years.items():
        for e in entries:
            uuid = e["player"]
            if uuid not in adp_lookup:
                adp_lookup[uuid] = e

    for uuid in sorted(all_uuids):
        adp_info = adp_lookup.get(uuid, {}).get("player_info", {})
        nflreadr = nflreadr_ids.get(uuid, {})
        gsis = nflreadr.get("gsis_id", "")
        detail = nflreadr_details.get(gsis, {}) if gsis else {}

        player = {
            "player_id": uuid,
            # Name: prefer nflreadr (more standardized)
            "first_name": nflreadr.get("name", "").split(" ", 1)[0] if nflreadr.get("name") else adp_info.get("fname", ""),
            "last_name": " ".join(nflreadr.get("name", "").split(" ", 1)[1:]) if nflreadr.get("name") else adp_info.get("lname", ""),
            "position": nflreadr.get("position") or adp_info.get("pos", ""),
            "birth_date": nflreadr.get("birthdate") or adp_info.get("dob", ""),
            # nflreadr enrichment
            "gsis_id": gsis,
            "espn_id": nflreadr.get("espn_id", ""),
            "yahoo_id": nflreadr.get("yahoo_id", ""),
            "sleeper_id": nflreadr.get("sleeper_id", ""),
            "pfr_id": nflreadr.get("pfr_id", ""),
            "rotowire_id": nflreadr.get("rotowire_id", ""),
            "headshot_url": detail.get("headshot", ""),
            "college": nflreadr.get("college") or detail.get("college_name", ""),
            "draft_year": nflreadr.get("draft_year") or detail.get("draft_year", ""),
            "draft_round": nflreadr.get("draft_round") or detail.get("draft_round", ""),
            "draft_pick": nflreadr.get("draft_ovr") or detail.get("draft_pick", ""),
            "latest_team": detail.get("latest_team") or nflreadr.get("team", ""),
            "status": detail.get("status", ""),
        }
        players.append(player)

        if gsis:
            matched += 1
        else:
            unmatched += 1

    print(f"  Players: {len(players)} total, {matched} matched to nflreadr, {unmatched} unmatched")
    return players


def build_adp():
    """Build clean ADP table."""
    adp_all_years = {}
    adp_rows = []

    for year in YEARS:
        with open(RAW_DIR / "adp" / f"adp_{year}.json") as f:
            data = json.load(f)
        adp_all_years[year] = data

        for entry in data:
            adp_rows.append({
                "player_id": entry["player"],
                "year": year,
                "adp": float(entry["adp"]),
                "min_pick": entry["min_pick"],
                "max_pick": entry["max_pick"],
                "times_drafted": entry["number"],
            })

    return adp_rows, adp_all_years


def write_csv(path, rows, fieldnames=None):
    if not rows:
        print(f"  WARNING: No rows to write for {path.name}")
        return
    if fieldnames is None:
        fieldnames = list(rows[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    size_mb = path.stat().st_size / 1024 / 1024
    print(f"  {path.name}: {len(rows):,} rows ({size_mb:.1f}MB)")


def main():
    print("Building clean Rotowire OC dataset...\n")

    # 1. Leagues and teams
    print("1. Leagues & teams...")
    leagues, league_teams = build_leagues_and_teams()
    leagues_lookup = {(l["year"], l["league_id"]): l for l in leagues}
    print(f"  {len(leagues)} leagues, {len(league_teams)} team entries")

    # 2. ADP
    print("\n2. ADP data...")
    adp_rows, adp_all_years = build_adp()
    print(f"  {len(adp_rows)} ADP entries")

    # 3. Draft picks
    print("\n3. Draft picks...")
    picks = build_draft_picks(leagues_lookup)
    print(f"  {len(picks):,} picks")

    # 4. Players (enriched)
    print("\n4. Players (nflreadr enrichment)...")
    players = build_players(picks, adp_all_years)

    # 5. Write CSVs
    print("\n5. Writing CSVs...")
    write_csv(OUT_DIR / "leagues.csv", leagues)
    write_csv(OUT_DIR / "league_teams.csv", league_teams)
    write_csv(OUT_DIR / "draft_picks.csv", picks)
    write_csv(OUT_DIR / "adp.csv", adp_rows)
    write_csv(OUT_DIR / "players.csv", players)

    print("\nDone!")


if __name__ == "__main__":
    main()
