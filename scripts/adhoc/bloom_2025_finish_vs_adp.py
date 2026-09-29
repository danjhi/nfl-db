"""Sigmund Bloom request: 2025 fantasy finish mapped onto positional ADP.

The idea: if a player finished as QB4 in 2025, show him at the draft cost of
whoever was the QB4 by 2025 ADP. That isolates positional value from the
player himself, so you can see which positions actually paid off relative to
what they cost.

Method
------
ADP comes from the raw picks we store, not from a published average: 87,360
picks across 364 leagues in 2025 ($350 Online Championship). ADP for a player
is the mean of his overall_pick across every league he was drafted in.

Universe is the top 150 by that ADP. Finish is 2025 full PPR
(player_season_stats.fantasy_points_ppr), ranked within position across all
players, not just the top 150, so a finish rank of RB7 means genuinely 7th
among running backs.

Each row then carries two costs:
  adp             what THIS player actually cost
  finish_pos_adp  what his FINISH slot cost, i.e. the ADP of the player who
                  was drafted as the positional rank he ended up finishing at

value_gained = adp - finish_pos_adp. Positive means he returned a slot that
cost more than he did (a hit); negative means he cost more than his finish
was worth (a bust).

Usage:
    ~/dev/nfl-db/.venv/bin/python scripts/adhoc/bloom_2025_finish_vs_adp.py
"""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path

from supabase import create_client

YEAR = 2025
TOP_N = 150
OUT = Path(__file__).resolve().parents[2] / "data" / "bloom_2025_finish_vs_adp.csv"
# The board view: same universe, but presented the way Bloom described it,
# walking the ADP board slot by slot and showing the player who FINISHED at
# each slot's positional rank (pick 1 was the ADP WR1, so it shows the actual
# WR1 finisher). Dan's spec 2026-08-05.
OUT_BOARD = (
    Path(__file__).resolve().parents[2] / "data" / "bloom_2025_finishers_board.csv"
)
PAGE = 1000


def client():
    env_path = Path(__file__).resolve().parents[2] / ".env"
    # [A-Z0-9_], not [A-Z_]: a key with a digit (LABS_DATA_S3_URI) silently
    # fails to parse otherwise. See footballguysdotcom/fbg-labs#2.
    env = dict(re.findall(r"^([A-Z0-9_]+)=(.*)$", env_path.read_text(), re.M))
    return create_client(
        "https://twfzcrodldvhpfaykasj.supabase.co",
        env["SUPABASE_SERVICE_ROLE_KEY"].strip(),
    )


def fetch_all(sb, table: str, columns: str, **filters):
    """Page through a table; PostgREST caps a single response at 1000 rows."""
    rows, start = [], 0
    while True:
        q = sb.table(table).select(columns)
        for col, val in filters.items():
            q = q.eq(col, val)
        page = q.range(start, start + PAGE - 1).execute().data
        rows.extend(page)
        if len(page) < PAGE:
            return rows
        start += PAGE


def main() -> None:
    sb = client()

    print(f"Pulling {YEAR} draft picks...")
    picks = fetch_all(sb, "draft_picks", "player_id,overall_pick", year=YEAR)
    print(f"  {len(picks):,} picks")

    by_player = defaultdict(list)
    for p in picks:
        if p["player_id"] and p["overall_pick"] is not None:
            by_player[p["player_id"]].append(p["overall_pick"])

    adp = {
        pid: (sum(v) / len(v), len(v))
        for pid, v in by_player.items()
    }
    print(f"  {len(adp):,} distinct players drafted")

    print(f"Pulling {YEAR} season stats...")
    stats = {
        s["player_id"]: s
        for s in fetch_all(
            sb,
            "player_season_stats",
            "player_id,first_name,last_name,position,team,games,fantasy_points_ppr",
            season=YEAR,
        )
    }
    print(f"  {len(stats):,} player seasons")

    # Finish rank within position, computed over EVERY player with 2025 points,
    # so the rank means what it says even if the player went undrafted.
    finish_rank: dict[str, int] = {}
    by_pos = defaultdict(list)
    for pid, s in stats.items():
        if s["position"] and s["fantasy_points_ppr"] is not None:
            by_pos[s["position"]].append((s["fantasy_points_ppr"], pid))
    for pos, rows in by_pos.items():
        for i, (_, pid) in enumerate(sorted(rows, reverse=True), start=1):
            finish_rank[pid] = i

    # ADP rank within position, over everyone drafted. This is the lookup that
    # answers "what did the QB4 slot cost?".
    pos_adp_ladder = defaultdict(list)
    for pid, (a, _) in adp.items():
        s = stats.get(pid)
        if s and s["position"]:
            pos_adp_ladder[s["position"]].append((a, pid))
    # position -> {positional rank: that slot's ADP}
    slot_cost: dict[str, dict[int, float]] = {}
    adp_pos_rank: dict[str, int] = {}
    for pos, rows in pos_adp_ladder.items():
        ladder = sorted(rows)
        slot_cost[pos] = {i: a for i, (a, _) in enumerate(ladder, start=1)}
        for i, (_, pid) in enumerate(ladder, start=1):
            adp_pos_rank[pid] = i

    top = sorted(adp.items(), key=lambda kv: kv[1][0])[:TOP_N]
    print(f"Building the top {TOP_N} by ADP...")

    # A player who missed all of 2025 has no stat row, so fall back to the
    # players table for identity. These are not data gaps: Joe Mixon and
    # Brandon Aiyuk were drafted in essentially every league and played zero
    # games, which is exactly the outcome this exercise is meant to surface.
    missing = [pid for pid, _ in top if pid not in stats]
    identity = {}
    for pid in missing:
        row = (
            sb.table("players")
            .select("first_name,last_name,position")
            .eq("player_id", pid)
            .limit(1)
            .execute()
            .data
        )
        if row:
            identity[pid] = row[0]

    out_rows = []
    for overall_rank, (pid, (a, n_drafts)) in enumerate(top, start=1):
        s = stats.get(pid)
        if not s:
            ident = identity.get(pid, {})
            name = f"{ident.get('first_name','')} {ident.get('last_name','')}".strip()
            pos = ident.get("position", "")
            # No games played means no finish, so the whole draft cost was lost.
            out_rows.append({
                "adp_overall_rank": overall_rank,
                "player": name or "UNKNOWN",
                "position": pos, "team": "",
                "adp": round(a, 1), "times_drafted": n_drafts,
                "adp_pos_rank": "", "games": 0, "ppr_points": 0,
                "finish_pos_rank": "", "finish_pos_adp": "",
                "value_gained": "", "note": "did not play in 2025",
            })
            continue

        pos = s["position"]
        fin = finish_rank.get(pid)
        fin_adp = slot_cost.get(pos, {}).get(fin) if fin else None

        out_rows.append({
            "adp_overall_rank": overall_rank,
            "player": f"{s['first_name']} {s['last_name']}".strip(),
            "position": pos,
            "team": s["team"] or "",
            "adp": round(a, 1),
            "times_drafted": n_drafts,
            "adp_pos_rank": f"{pos}{adp_pos_rank.get(pid, '')}",
            "games": s["games"] or 0,
            "ppr_points": round(s["fantasy_points_ppr"] or 0, 1),
            "finish_pos_rank": f"{pos}{fin}" if fin else "",
            "finish_pos_adp": round(fin_adp, 1) if fin_adp is not None else "",
            "value_gained": round(a - fin_adp, 1) if fin_adp is not None else "",
            "note": "" if fin_adp is not None
                    else f"no {pos} was drafted at that finish slot",
        })

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)

    print(f"\nWrote {len(out_rows)} rows -> {OUT}")

    # ---- Board view: each ADP slot shows its FINISHER ------------------
    # Inverse of finish_rank: (position, rank) -> player. Reuses the exact
    # ladders computed above so the two views can never disagree.
    finisher_at = {}
    for pid, rank in finish_rank.items():
        pos = stats[pid]["position"]
        finisher_at[(pos, rank)] = pid

    board_rows = []
    for row in out_rows:
        pos_rank = row["adp_pos_rank"]
        base = {
            "slot_overall_rank": row["adp_overall_rank"],
            "slot_adp": row["adp"],
            "slot_pos_rank": pos_rank,
            "drafted_player": row["player"],
        }
        if not pos_rank:
            board_rows.append({
                **base,
                "finisher": "", "finisher_team": "",
                "finisher_ppr_points": "", "finisher_games": "",
                "finisher_own_adp": "",
                "note": row["note"],
            })
            continue
        pos = row["position"]
        rank = int(pos_rank[len(pos):])
        fin_pid = finisher_at.get((pos, rank))
        fs = stats.get(fin_pid) if fin_pid else None
        fin_adp = adp.get(fin_pid) if fin_pid else None
        board_rows.append({
            **base,
            "finisher": f"{fs['first_name']} {fs['last_name']}".strip() if fs else "",
            "finisher_team": (fs["team"] or "") if fs else "",
            "finisher_ppr_points": round(fs["fantasy_points_ppr"] or 0, 1) if fs else "",
            "finisher_games": (fs["games"] or 0) if fs else "",
            "finisher_own_adp": round(fin_adp[0], 1) if fin_adp else "",
            "note": "" if fin_adp else ("undrafted in NFFC" if fs else ""),
        })

    with OUT_BOARD.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(board_rows[0].keys()))
        w.writeheader()
        w.writerows(board_rows)

    print(f"Wrote {len(board_rows)} rows -> {OUT_BOARD}")
    hits = [r for r in out_rows if isinstance(r["value_gained"], float)]
    hits.sort(key=lambda r: r["value_gained"], reverse=True)
    print("\nBiggest value gained (finish slot cost more than they did):")
    for r in hits[:8]:
        print(f"  {r['player']:<24} {r['adp_pos_rank']:>5} -> {r['finish_pos_rank']:<5} "
              f"ADP {r['adp']:>5}  finish slot cost {r['finish_pos_adp']:>5}  "
              f"(+{r['value_gained']})")
    print("\nBiggest value lost:")
    for r in hits[-8:][::-1]:
        print(f"  {r['player']:<24} {r['adp_pos_rank']:>5} -> {r['finish_pos_rank']:<5} "
              f"ADP {r['adp']:>5}  finish slot cost {r['finish_pos_adp']:>5}  "
              f"({r['value_gained']})")


if __name__ == "__main__":
    main()
