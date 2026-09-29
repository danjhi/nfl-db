"""Pull DraftKings and FanDuel DFS slates + salaries from SportsDataIO into Supabase.

    .venv/bin/python3 scripts/dfs/fetch_sportsdata_dfs.py                 # current week (Timeframes/current)
    .venv/bin/python3 scripts/dfs/fetch_sportsdata_dfs.py --season 2026 --week 1
    .venv/bin/python3 scripts/dfs/fetch_sportsdata_dfs.py --week 1 --dry-run   # fetch + match + report, no writes

Two tables (scripts/dfs/schema.sql):

  dfs_slates     one row per operator slate (name, game type, start, games, cap, roster slots)
  dfs_salaries   one row per slate per player (salary, operator ids, our player_id);
                 slate_id = 0 rows carry the operator-wide weekly salary from the
                 projections feeds (DK + FD), the fallback when slates are late.

Sources (all under https://api.sportsdata.io/v3/nfl/, header Ocp-Apim-Subscription-Key):
  projections/json/DfsSlatesByWeek/{season}/{week}               slates, salaries, operator ids
  projections/json/PlayerGameProjectionStatsByWeek/{season}/{week}  DraftKingsSalary / FanDuelSalary per player
  projections/json/FantasyDefenseProjectionsByGame/{season}/{week}  the same for team defenses
  scores/json/Players                                             PlayerID -> SportRadarPlayerID (= players.player_id)
  scores/json/Timeframes/current                                  season + week when not given

Player matching, in order: SportsDataIO's SportRadarPlayerID (our PK), players.sportsdata_id,
normalized name + team + position against players. Team defenses map to DEF_<TEAM>.
Every unmatched player with a salary above the operator minimum is listed in the report.
Idempotent: upserts on the primary keys (resolution=merge-duplicates), safe to re-run daily.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from zoneinfo import ZoneInfo

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_here, "..", "ids"))
from shared import (  # noqa: E402  (loads .env on import)
    SPORTSDATA_KEY,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    SUPABASE_URL,
    normalize_name,
    normalize_team,
)

BASE = "https://api.sportsdata.io/v3/nfl/"
ET = ZoneInfo("America/New_York")
OPERATORS = ("DraftKings", "FanDuel")
DEF_POS = {"DST", "D", "DEF", "DF"}
CLASSIC_FAMILY = {"Classic", "SuperFlex", "Tiers", "Snake"}   # one row per player; Showdown / Single Game price a player per slot
PK = ("season", "week", "operator", "slate_id", "sdio_player_id", "slot")
MIN_SALARY = {"DraftKings": 4000, "FanDuel": 4000}   # report threshold only


# ── HTTP ─────────────────────────────────────────────────────────────────────
def sdio(path: str):
    req = urllib.request.Request(BASE + path, headers={"Ocp-Apim-Subscription-Key": SPORTSDATA_KEY})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def sb_get(table: str, query: str) -> list[dict]:
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    rows: list[dict] = []
    offset = 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?{query}&limit=1000&offset={offset}"
        req = urllib.request.Request(url, headers={"apikey": key, "Authorization": f"Bearer {key}"})
        with urllib.request.urlopen(req, timeout=60) as r:
            page = json.loads(r.read().decode("utf-8"))
        rows.extend(page)
        if len(page) < 1000:
            return rows
        offset += 1000


def sb_upsert(table: str, rows: list[dict], batch: int = 500) -> tuple[int, int]:
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    ok = err = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        req = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/{table}",
            data=json.dumps(chunk).encode("utf-8"),
            headers={"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json",
                     "Prefer": "return=minimal,resolution=merge-duplicates"},
            method="POST",
        )
        try:
            urllib.request.urlopen(req, timeout=120)
            ok += len(chunk)
        except urllib.error.HTTPError as e:
            print(f"  ERROR {table} batch at {i}: {e.code} {e.read().decode('utf-8', 'replace')[:300]}")
            err += len(chunk)
    return ok, err


# ── helpers ──────────────────────────────────────────────────────────────────
def et_to_utc(s: str | None) -> str | None:
    """SDIO operator times are ET wall clock without an offset."""
    if not s:
        return None
    t = dt.datetime.fromisoformat(s.replace("Z", ""))
    if t.tzinfo is None:
        t = t.replace(tzinfo=ET)
    return t.astimezone(dt.timezone.utc).isoformat()


def current_week() -> tuple[int, int]:
    tf = sdio("scores/json/Timeframes/current")[0]
    season, week = int(tf["Season"]), int(tf["Week"])
    if tf.get("HasEnded"):
        week += 1
    return season, week


class Matcher:
    """SportsDataIO PlayerID -> our players.player_id."""

    def __init__(self):
        print("loading SportsDataIO player list ...")
        sd_players = sdio("scores/json/Players")
        self.sr = {p["PlayerID"]: p.get("SportRadarPlayerID") for p in sd_players if p.get("SportRadarPlayerID")}
        self.sd_meta = {p["PlayerID"]: p for p in sd_players}
        print("loading players table ...")
        ours = sb_get("players", "select=player_id,sportsdata_id,first_name,last_name,position,latest_team")
        self.known = {p["player_id"] for p in ours}
        self.by_sdio = {int(p["sportsdata_id"]): p["player_id"] for p in ours if p.get("sportsdata_id")}
        self.by_name: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for p in ours:
            nm = normalize_name(f"{p.get('first_name') or ''} {p.get('last_name') or ''}")
            self.by_name[(nm, normalize_team(p.get("latest_team")))].append(p)
            self.by_name[(nm, "")].append(p)
        self.hits = Counter()

    def match(self, sdio_id: int, name: str, team: str, position: str) -> tuple[str | None, str]:
        if position in DEF_POS:
            self.hits["team"] += 1
            return f"DEF_{team}", "team"
        sr = self.sr.get(sdio_id)
        if sr and sr in self.known:
            self.hits["sportradar"] += 1
            return sr, "sportradar"
        if sdio_id in self.by_sdio:
            self.hits["supabase"] += 1
            return self.by_sdio[sdio_id], "supabase"
        nm = normalize_name(name)
        cands = [p for p in self.by_name.get((nm, team), []) if (p.get("position") or "").upper() in (position, "PK" if position == "K" else position)]
        if not cands:
            cands = [p for p in self.by_name.get((nm, ""), []) if (p.get("position") or "").upper() == position]
        if len(cands) == 1:
            self.hits["name"] += 1
            return cands[0]["player_id"], "name"
        if sr:  # Sportradar id exists upstream but the player is not in our table yet
            self.hits["sportradar_new"] += 1
            return sr, "sportradar_new"
        self.hits["none"] += 1
        return None, "none"


# ── build rows ───────────────────────────────────────────────────────────────
def slate_rows(season: int, week: int, slates: list[dict], m: Matcher, retrieved: str):
    srows, prows = [], []
    for s in slates:
        if s["Operator"] not in OPERATORS:
            continue
        games = []
        for g in s.get("DfsSlateGames") or []:
            gm = g.get("Game") or {}
            away, home = normalize_team(gm.get("AwayTeam")), normalize_team(gm.get("HomeTeam"))
            games.append({"away": away, "home": home, "kickoff": et_to_utc(gm.get("DateTime")),
                          "game_id": f"{season}_{week:02d}_{away}_{home}" if away and home else None,
                          "removed": bool(g.get("RemovedByOperator"))})
        players = s.get("DfsSlatePlayers") or []
        srows.append({
            "slate_id": s["SlateID"], "season": season, "week": week, "operator": s["Operator"],
            "operator_slate_id": str(s.get("OperatorSlateID") or ""), "operator_name": s.get("OperatorName"),
            "operator_game_type": s.get("OperatorGameType"), "operator_day": (s.get("OperatorDay") or "")[:10] or None,
            "operator_start_time": et_to_utc(s.get("OperatorStartTime")), "number_of_games": s.get("NumberOfGames"),
            "is_multi_day": bool(s.get("IsMultiDaySlate")), "removed_by_operator": bool(s.get("RemovedByOperator")),
            "salary_cap": s.get("SalaryCap"), "roster_slots": s.get("SlateRosterSlots"), "games": games,
            "player_count": len(players), "retrieved_at": retrieved,
        })
        if s.get("RemovedByOperator"):
            continue
        per_slot = s.get("OperatorGameType") not in CLASSIC_FAMILY
        for p in players:
            slot = "/".join(p.get("OperatorRosterSlots") or []) if per_slot else ""
            pos = (p.get("OperatorPosition") or "").upper()
            team = normalize_team(p.get("Team"))
            name = (p.get("OperatorPlayerName") or "").strip()
            pid, src = m.match(int(p["PlayerID"]), name, team, pos)
            prows.append({
                "season": season, "week": week, "operator": s["Operator"], "slate_id": s["SlateID"],
                "sdio_player_id": int(p["PlayerID"]), "player_id": pid, "match_source": src,
                "operator_player_id": str(p.get("OperatorPlayerID") or ""), "name": name, "position": pos,
                "team": team, "salary": p.get("OperatorSalary"), "roster_slots": p.get("OperatorRosterSlots"), "slot": slot,
                "removed_by_operator": bool(p.get("RemovedByOperator")), "retrieved_at": retrieved,
            })
    return srows, prows


def dedupe(rows: list[dict]) -> list[dict]:
    """One row per primary key (last wins) so a batch upsert never hits the same key twice."""
    seen: dict[tuple, dict] = {}
    for r in rows:
        seen[tuple(r[k] for k in PK)] = r
    return list(seen.values())


def weekly_rows(season: int, week: int, m: Matcher, retrieved: str) -> list[dict]:
    """slate_id = 0: the operator-wide weekly salary from the projections feeds."""
    out = []
    feeds = [("projections/json/PlayerGameProjectionStatsByWeek/%d/%d", False),
             ("projections/json/FantasyDefenseProjectionsByGame/%d/%d", True)]
    for path, is_def in feeds:
        try:
            rows = sdio(path % (season, week))
        except urllib.error.HTTPError as e:
            print(f"  weekly feed {path} -> {e.code}, skipped")
            continue
        for r in rows:
            team = normalize_team(r.get("Team"))
            for operator, sal_key, pos_key in (("DraftKings", "DraftKingsSalary", "DraftKingsPosition"),
                                               ("FanDuel", "FanDuelSalary", "FanDuelPosition")):
                sal = r.get(sal_key)
                if sal is None:
                    continue
                pos = (r.get(pos_key) or r.get("FantasyPosition") or r.get("Position") or "").upper()
                if is_def:
                    pos = "DST"
                name = f"{team} D/ST" if is_def else (r.get("Name") or "")
                sdio_id = int(r.get("PlayerID") or r.get("TeamID"))
                pid, src = m.match(sdio_id, name, team, pos)
                meta = m.sd_meta.get(sdio_id, {})
                op_id = meta.get("DraftKingsPlayerID" if operator == "DraftKings" else "FanDuelPlayerID")
                out.append({
                    "season": season, "week": week, "operator": operator, "slate_id": 0,
                    "sdio_player_id": sdio_id, "player_id": pid, "match_source": src,
                    "operator_player_id": str(op_id) if op_id else "", "name": name, "position": pos,
                    "team": team, "salary": int(sal), "roster_slots": None, "slot": "", "removed_by_operator": False,
                    "retrieved_at": retrieved,
                })
    return out


def report(srows: list[dict], prows: list[dict], m: Matcher) -> None:
    print("\nslates loaded:")
    for s in sorted(srows, key=lambda s: (s["operator"], s["operator_start_time"] or "", s["slate_id"])):
        if s["removed_by_operator"] and not s["player_count"]:
            continue
        flag = " REMOVED" if s["removed_by_operator"] else ""
        print(f"  {s['operator']:<10} {s['slate_id']:>6}  {s['operator_name']:<32} {s['operator_game_type']:<26} "
              f"games {s['number_of_games']:>2}  players {s['player_count']:>4}  start {s['operator_start_time']}{flag}")
    print("\nmatch sources:", dict(m.hits))
    for operator in OPERATORS:
        mains = [s for s in srows if s["operator"] == operator and s["operator_name"] == "Main"
                 and s["operator_game_type"] == "Classic" and not s["removed_by_operator"]]
        for s in mains:
            rows = [p for p in prows if p["slate_id"] == s["slate_id"]]
            matched = sum(1 for p in rows if p["player_id"])
            miss = sorted((p for p in rows if not p["player_id"] and (p["salary"] or 0) > MIN_SALARY[operator]),
                          key=lambda p: -(p["salary"] or 0))
            print(f"\n{operator} Main {s['slate_id']} ({s['operator_start_time']}): {matched}/{len(rows)} players matched; "
                  f"unmatched above min salary: {len(miss)}")
            for p in miss[:15]:
                print(f"    {p['name']:<24} {p['position']:<3} {p['team']:<4} ${p['salary']}")
    weekly = [p for p in prows if p["slate_id"] == 0]
    if weekly:
        c = Counter(p["operator"] for p in weekly)
        print(f"\nweekly-feed rows (slate 0): {dict(c)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int)
    ap.add_argument("--week", type=int)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-weekly", action="store_true", help="skip the slate_id = 0 weekly-feed rows")
    args = ap.parse_args()
    if not SPORTSDATA_KEY:
        raise SystemExit("SPORTSDATA_API_KEY missing from .env")
    if args.season and args.week:
        season, week = args.season, args.week
    else:
        season, week = current_week()
        if args.week:
            week = args.week
        if args.season:
            season = args.season
    retrieved = dt.datetime.now(dt.timezone.utc).isoformat()
    print(f"SportsDataIO DFS pull: {season} week {week} ({retrieved})")
    slates = sdio(f"projections/json/DfsSlatesByWeek/{season}/{week}")
    print(f"  {len(slates)} slates in the feed")
    m = Matcher()
    srows, prows = slate_rows(season, week, slates, m, retrieved)
    if not args.no_weekly:
        prows += weekly_rows(season, week, m, retrieved)
    prows = dedupe(prows)
    report(srows, prows, m)
    if args.dry_run:
        print("\n--dry-run: nothing written")
        return
    ok, err = sb_upsert("dfs_slates", srows)
    print(f"\ndfs_slates: upserted {ok}, errors {err}")
    ok, err = sb_upsert("dfs_salaries", prows)
    print(f"dfs_salaries: upserted {ok}, errors {err}")
    if err:
        sys.exit(1)


if __name__ == "__main__":
    main()
