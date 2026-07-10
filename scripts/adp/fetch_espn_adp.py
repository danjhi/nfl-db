"""Fetch ESPN redraft ADP and upsert into adp_sources (source="espn").

ESPN publishes ADP through the public fantasy "kona_player_info" endpoint (no
login). ADP lives at player.ownership.averageDraftPosition on the PPR default
league (leaguedefaults/3). One request returns every player — the x-fantasy-filter
just needs a sort (ESPN rejects a `limit` without one: "Limit request must be
accompanied by a sort").

Join:
  - Offense (QB/RB/WR/TE) + kickers: match ESPN's player id -> players.espn_id;
    name+pos fallback for the rest (covers the new kickers, which have no espn_id
    yet — ESPN uses individual kickers like every redraft source).
  - Team defense (ESPN pos 16, id = -(16000+proTeamId)): remap -> DEF_{TEAM} via
    ESPN's proTeamId, so ESPN defense shares the clean defense row with every
    other source (same treatment as RTSports/NFFC).

`source="espn"` matches the key FBG's feed carries (the consumer contract) — this
replaces the FBG-feed value.

Usage:
    python3 scripts/adp/fetch_espn_adp.py            # fetch + upsert
    python3 scripts/adp/fetch_espn_adp.py --dry-run  # fetch + match, no write
"""

import argparse
import datetime
import json
import os
import ssl
import sys
import urllib.error
import urllib.request
from collections import Counter

_script_dir = os.path.dirname(os.path.abspath(__file__)) if os.path.exists(__file__) else os.path.join("scripts", "adp")
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import (  # noqa: E402
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    normalize_name,
    normalize_team,
    build_player_lookup,
)

# ── Config ───────────────────────────────────────────────────────────────────
YEAR = 2026
SOURCE = "espn"
TODAY = datetime.date.today().isoformat()

ESPN_URL = (
    "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/"
    f"seasons/{YEAR}/segments/0/leaguedefaults/3?view=kona_player_info"
)
# limit needs a sort; sortPercOwned (no limit) returns the full player list.
ESPN_FILTER = json.dumps({"players": {"sortPercOwned": {"sortPriority": 1, "sortAsc": False}}})
USER_AGENT = "Mozilla/5.0 (nfl-db adp fetcher)"

ESPN_POS = {1: "QB", 2: "RB", 3: "WR", 4: "TE", 5: "K", 16: "DST"}

# ESPN proTeamId -> nflreadr-standard abbreviation (for D/ST -> DEF_{TEAM}).
ESPN_TEAM = {
    1: "ATL", 2: "BUF", 3: "CHI", 4: "CIN", 5: "CLE", 6: "DAL", 7: "DEN",
    8: "DET", 9: "GB", 10: "TEN", 11: "IND", 12: "KC", 13: "LV", 14: "LA",
    15: "MIA", 16: "MIN", 17: "NE", 18: "NO", 19: "NYG", 20: "NYJ", 21: "PHI",
    22: "ARI", 23: "PIT", 24: "LAC", 25: "SF", 26: "SEA", 27: "TB", 28: "WAS",
    29: "CAR", 30: "JAX", 33: "BAL", 34: "HOU",
}

try:
    import certifi

    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()


# ── Fetch ────────────────────────────────────────────────────────────────────
def fetch_espn_players():
    req = urllib.request.Request(
        ESPN_URL, headers={"User-Agent": USER_AGENT, "x-fantasy-filter": ESPN_FILTER}
    )
    with urllib.request.urlopen(req, timeout=60, context=SSL_CTX) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data.get("players", [])


def undrafted_cutoff(espn):
    """ESPN reports ADP for *every* player, pinning the undrafted ones at a
    sentinel value (a huge cluster just past the last real pick — e.g. ~793
    players at ~170). Return an ADP cutoff: keep only players below the sentinel
    cluster, so we store the drafted board (like RTSports/NFFC's pre-filtered
    feeds), not ESPN's whole player universe. Self-adjusts each run."""
    adps = [((e.get("player") or {}).get("ownership") or {}).get("averageDraftPosition") or 0
            for e in espn]
    adps = [a for a in adps if a > 0]
    if not adps:
        return float("inf")
    bucket, count = Counter(round(a) for a in adps).most_common(1)[0]
    if count <= 50:                      # no sentinel cluster — keep everything
        return float("inf")
    return min(a for a in adps if round(a) == bucket)   # low edge of the cluster


def fetch_all_players():
    """player_id, name, position, latest_team, espn_id (paginated)."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,first_name,last_name,position,latest_team,espn_id"
            f"&offset={offset}&limit=1000"
        )
        req = urllib.request.Request(url, headers={"apikey": key, "Authorization": f"Bearer {key}"})
        with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as resp:
            batch = json.loads(resp.read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += 1000
    return players


# ── Upsert ───────────────────────────────────────────────────────────────────
def batch_upsert(rows, batch_size=100):
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/adp_sources"
    inserted = errors = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        req = urllib.request.Request(url, data=json.dumps(batch).encode("utf-8"), headers={
            "apikey": key, "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates",
        }, method="POST")
        try:
            urllib.request.urlopen(req, timeout=60, context=SSL_CTX)
            inserted += len(batch)
        except urllib.error.HTTPError as e:
            print(f"  ERROR batch at {i}: {e.code} {e.read().decode('utf-8','replace')}")
            errors += len(batch)
    return inserted, errors


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Fetch ESPN ADP into adp_sources.")
    ap.add_argument("--dry-run", action="store_true", help="fetch + match, no write")
    args = ap.parse_args()

    print("Fetching ESPN kona_player_info...")
    espn = fetch_espn_players()
    cutoff = undrafted_cutoff(espn)
    print(f"  {len(espn)} ESPN players (drafted-board cutoff: ADP < {cutoff:.1f})")

    print("Fetching players from Supabase...")
    all_players = fetch_all_players()
    by_espn_id = {str(p["espn_id"]): p["player_id"] for p in all_players if p.get("espn_id")}
    def_by_team = {
        normalize_team(p["latest_team"] or ""): p["player_id"]
        for p in all_players if p["position"] == "DEF"
    }
    by_name_pos, by_name = build_player_lookup(all_players)
    print(f"  {len(all_players)} players ({len(by_espn_id)} with espn_id, {len(def_by_team)} defenses)")

    adp_rows = []
    counts = {"offense": [0, 0], "k": [0, 0], "def": [0, 0]}  # [matched, unmatched]
    unmatched = []
    for e in espn:
        pl = e.get("player") or {}
        adp = (pl.get("ownership") or {}).get("averageDraftPosition") or 0
        if adp <= 0 or adp >= cutoff:   # skip undrafted sentinel cluster
            continue
        pos_id = pl.get("defaultPositionId")

        if pos_id == 16:  # team defense
            abbr = normalize_team(ESPN_TEAM.get(pl.get("proTeamId"), ""))
            target, category = def_by_team.get(abbr), "def"
        else:
            eid = str(pl.get("id"))
            target = by_espn_id.get(eid)
            if not target:
                norm = normalize_name(pl.get("fullName") or "")
                pos = ESPN_POS.get(pos_id, "")
                target = by_name_pos.get((norm, pos)) or by_name.get(norm)
            category = "k" if pos_id == 5 else "offense"

        if target:
            counts[category][0] += 1
            adp_rows.append({
                "player_id": target, "source": SOURCE, "year": YEAR, "date": TODAY,
                "adp": float(adp), "projected_points": None, "position_rank": None,
            })
        else:
            counts[category][1] += 1
            unmatched.append(f"  {pl.get('fullName','?')} ({ESPN_POS.get(pos_id,pos_id)}) "
                             f"espn_id={pl.get('id')} adp={adp}")

    matched = len(adp_rows)
    print(f"\n  Matched: {matched}  "
          f"[offense {counts['offense'][0]}, k {counts['k'][0]}, def {counts['def'][0]}]")
    print(f"  Unmatched: {sum(c[1] for c in counts.values())}  "
          f"[offense {counts['offense'][1]}, k {counts['k'][1]}, def {counts['def'][1]}]")
    if unmatched:
        print("  Unmatched (top 20 — mostly deep players not in our DB):")
        for line in unmatched[:20]:
            print(line)
        if len(unmatched) > 20:
            print(f"    ... and {len(unmatched) - 20} more")

    if args.dry_run:
        print(f"\n[dry-run] would upsert {matched} rows (source={SOURCE}).")
    elif adp_rows:
        print(f"\nUpserting {matched} rows (source={SOURCE})...")
        inserted, errors = batch_upsert(adp_rows)
        print(f"  Inserted/updated: {inserted}" + (f"  Errors: {errors}" if errors else ""))

    print(f"\n{'=' * 50}\nDONE ({'dry-run' if args.dry_run else 'wrote'}) — {matched} rows, {TODAY}\n{'=' * 50}")


if __name__ == "__main__":
    main()
