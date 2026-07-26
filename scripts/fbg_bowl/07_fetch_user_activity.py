"""Pull Sleeper league activity for every FBG Bowl participant.

For each distinct sleeper_user_id in fbg_bowl_rosters, ask Sleeper how many NFL
leagues that user is in for each season, and classify those leagues (Bowl vs other
FBG product vs their own leagues; dynasty/redraft/best-ball/superflex/TEP).

Sleeper retains league history: a spot-check of 150 users from the 2024 Bowl found
100% still return their 2024 Bowl league, so backward-looking counts are real
rather than survivorship artifacts.

Writes one row per (user, season, snapshot_date) to fbg_bowl_user_activity.
Re-running on a later date appends a new snapshot (adp_sources pattern), which is
how in-season 2026 growth gets tracked.

Usage:
    python3 scripts/fbg_bowl/07_fetch_user_activity.py [--dry-run] [--seasons 2023,2024,2025,2026]
"""

import argparse
import asyncio
import collections
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import httpx  # noqa: E402
from shared import supa_get, supa_batch_insert, ensure_data_dir, checkpoint_path  # noqa: E402

SLEEPER_BASE = "https://api.sleeper.app/v1"
MAX_CONCURRENT = 10
FBG_COMPANY_ID = "1236407759822397440"  # FBG's Sleeper company account (2025 Bowl)

BOWL_RE = re.compile(r"(fbg|footballguys)\s*bowl", re.I)
FBG_RE = re.compile(r"\b(fbg|footballguys)\b", re.I)


def classify(lg, bowl_ids):
    """(is_bowl, is_fbg_other). Bowl naming changed between years, so match on
    the Supabase id list, the company account, and both name conventions."""
    name = lg.get("name") or ""
    if lg["league_id"] in bowl_ids or BOWL_RE.search(name):
        return True, False
    if str(lg.get("company_id")) == FBG_COMPANY_ID:
        return True, False
    if FBG_RE.search(name):
        return False, True
    return False, False


async def fetch(client, sem, user_id, season, stats):
    async with sem:
        for attempt in range(4):
            try:
                r = await client.get(
                    f"{SLEEPER_BASE}/user/{user_id}/leagues/nfl/{season}", timeout=25
                )
                if r.status_code == 429:
                    await asyncio.sleep(3 * (attempt + 1))
                    continue
                if r.status_code != 200:
                    await asyncio.sleep(1)
                    continue
                stats["done"] += 1
                if stats["done"] % 2000 == 0:
                    print(f"  {stats['done']} calls done", flush=True)
                return r.json() or []
            except Exception:
                await asyncio.sleep(1)
        stats["failed"] += 1
        return None


async def pull(users, seasons):
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    stats = {"done": 0, "failed": 0}
    async with httpx.AsyncClient() as client:
        tasks = [fetch(client, sem, u, s, stats) for u in users for s in seasons]
        results = await asyncio.gather(*tasks)
    print(f"  {stats['done']} ok, {stats['failed']} failed")
    out, i = {}, 0
    for u in users:
        out[u] = {}
        for s in seasons:
            out[u][s] = results[i] or []
            i += 1
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--seasons", default="2023,2024,2025,2026")
    args = ap.parse_args()
    seasons = [int(s) for s in args.seasons.split(",")]

    print("Reading cohort from Supabase...")
    rosters = supa_get("fbg_bowl_rosters", select="sleeper_user_id,league_id")
    leagues = supa_get("fbg_bowl_leagues", select="id,sleeper_id,year")
    bowl_ids = {l["sleeper_id"] for l in leagues}
    year_by_id = {l["id"]: l["year"] for l in leagues}

    played = collections.defaultdict(set)
    for r in rosters:
        if r["sleeper_user_id"]:
            y = year_by_id.get(r["league_id"])
            if y:
                played[r["sleeper_user_id"]].add(y)

    users = sorted(played.keys())
    print(f"  {len(users):,} distinct users x {len(seasons)} seasons "
          f"= {len(users)*len(seasons):,} calls")

    raw = asyncio.run(pull(users, seasons))

    ensure_data_dir()
    raw_path = checkpoint_path("user_activity_raw")
    with open(raw_path, "w") as f:
        json.dump(raw, f)
    print(f"  raw saved -> {raw_path}")

    rows = []
    for u in users:
        for s in seasons:
            c = collections.Counter()
            for lg in raw[u][s]:
                is_bowl, is_fbg_other = classify(lg, bowl_ids)
                c["total"] += 1
                c["bowl" if is_bowl else ("fbg_other" if is_fbg_other else "outside")] += 1
                st = lg.get("settings") or {}
                t = st.get("type")
                c[{0: "redraft", 1: "keeper", 2: "dynasty", 3: "elimination"}.get(t, "_unk")] += 1
                if st.get("best_ball") == 1:
                    c["best_ball"] += 1
                if "SUPER_FLEX" in (lg.get("roster_positions") or []):
                    c["superflex"] += 1
                bonus = (lg.get("scoring_settings") or {}).get("bonus_rec_te")
                if bonus and bonus > 0:
                    c["tep"] += 1
            rows.append({
                "sleeper_user_id": u,
                "season": s,
                "league_count": c["total"],
                "bowl_league_count": c["bowl"],
                "fbg_other_count": c["fbg_other"],
                "outside_league_count": c["outside"],
                "dynasty_count": c["dynasty"],
                "redraft_count": c["redraft"],
                "keeper_count": c["keeper"],
                "elimination_count": c["elimination"],
                "best_ball_count": c["best_ball"],
                "superflex_count": c["superflex"],
                "tep_count": c["tep"],
                "played_bowl_2024": 2024 in played[u],
                "played_bowl_2025": 2025 in played[u],
            })

    print(f"\nDerived {len(rows):,} rows")
    for s in seasons:
        sub = [r for r in rows if r["season"] == s]
        act = sum(1 for r in sub if r["league_count"] > 0)
        print(f"  {s}: {act:,} active users, {sum(r['league_count'] for r in sub):,} leagues")

    if args.dry_run:
        print("\n--dry-run: nothing written")
        return

    print(f"\nLoading into fbg_bowl_user_activity...")
    supa_batch_insert("fbg_bowl_user_activity", rows, batch_size=500)
    print(f"Done: {len(rows):,} rows")


if __name__ == "__main__":
    main()
