#!/usr/bin/env python3
"""
build_oc_local_fixture.py

Pulls 2026 NFFC OC drafts from NFFC's public API, resolves each pick's
NFFC UUID to a Footballguys playerId, and writes a JSON file shaped
exactly like FBG's GET /api/nfl/:year/adp/nffc/oc/data so the React app
in ~/dev/2026-nffc-tool can use it as a dev fixture until the
FBG-side cron's NFFC-id ↔ FBG-id mapping is fully populated
(see fbgsite#43 — Simon's last comment notes the gap).

Resolution chain (per pick):
  1. Hand-curated alias map (NFFC fname,lname → FBG id)
  2. Supabase players.player_id (=NFFC UUID) → players.footballguys_id
  3. NFFC ADP player_info → name+DOB join against FBG /players
  4. Same join with last-name suffix stripped ("Walker III" → "Walker")
  5. Name+position fallback (when DOB missing on NFFC side)
  6. Unique name-only fallback

Output:
  ~/dev/2026-nffc-tool/public/dev-data/nffc-oc-{YEAR}.json

Run:  python3 build_oc_local_fixture.py
Re-run any time NFFC has new picks or Supabase has been re-enriched.
"""

import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# macOS Python frequently lacks a usable system trust store; prefer certifi.
try:
    import certifi

    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CONTEXT = ssl.create_default_context()

# ── Config ───────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
NFL_DB_ROOT = SCRIPT_DIR.parent.parent
ENV_PATH = NFL_DB_ROOT / ".env"

# Load nfl-db .env (Supabase keys live here)
if ENV_PATH.exists():
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

NFFC_API_KEY = os.environ.get(
    "NFFC_API_KEY", "22c3eaf3f16842fda979d38c83880386"
)  # matches existing pull_draft_results.py default
SUPABASE_URL = "https://twfzcrodldvhpfaykasj.supabase.co"
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get(
    "SUPABASE_ANON_KEY"
)
if not SUPABASE_KEY:
    sys.exit("error: SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY) missing from nfl-db/.env")

NFFC_BASE = "https://nfc.shgn.com/api/public"
FBG_BASE = "https://www.footballguys.com"

YEAR = 2026
GAME_TYPE_ID_OC = 936  # 2026 Footballguys Online Championship; changes yearly
NUM_TEAMS = 12  # OC is always 12-team
USER_AGENT = "NFFC-OC-Fixture/1.0"

OUT = Path.home() / "dev/2026-nffc-tool/public/dev-data" / f"nffc-oc-{YEAR}.json"

# Hand-curated NFFC name → FBG id aliases for cases where Supabase lookup
# fails AND name+DOB heuristics fail. Add new entries here as discovered.
# Keys are (norm(fname), norm(lname)) — see norm() below.
ALIASES: dict[tuple[str, str], str] = {
    # Hollywood Brown is "Marquise Brown" in FBG
    ("hollywood", "brown"): "BrowMa05",
}

# NFFC team abbreviations that differ from FBG's. Extend as discovered.
NFFC_TO_FBG_TEAM = {
    "LA": "LAR",  # NFFC uses 'LA' for the Rams; FBG uses 'LAR'
}

# Common nicknames → formal names. Used in the name-based fallback when an
# initial first-name match fails (e.g. NFFC stores "Nick Singleton", FBG has
# "Nicholas Singleton"). One-way: we try the formal name when nickname misses.
NICKNAME_EXPANSIONS: dict[str, list[str]] = {
    "nick": ["nicholas"],
    "mike": ["michael"],
    "tom": ["thomas"],
    "tommy": ["thomas"],
    "rob": ["robert"],
    "bob": ["robert"],
    "robby": ["robert"],
    "matt": ["matthew"],
    "dan": ["daniel"],
    "danny": ["daniel"],
    "joe": ["joseph"],
    "joey": ["joseph"],
    "will": ["william"],
    "billy": ["william"],
    "bill": ["william"],
    "alex": ["alexander"],
    "chris": ["christopher"],
    "tony": ["anthony"],
    "ben": ["benjamin"],
    "dave": ["david"],
    "ted": ["theodore", "edward"],
    "sam": ["samuel"],
    "jake": ["jacob"],
    "zach": ["zachary"],
    "josh": ["joshua"],
    "andy": ["andrew"],
    "drew": ["andrew"],
    "tony": ["anthony"],
    "tim": ["timothy"],
    "ron": ["ronald"],
    "ricky": ["richard"],
    "rick": ["richard"],
    "jim": ["james"],
    "jimmy": ["james"],
    "jamie": ["james"],
}

# ── Network helpers ──────────────────────────────────────────────────────────


def fetch_json(url: str, headers: dict | None = None, retries: int = 2) -> object:
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)
    last_exc = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=30, context=SSL_CONTEXT) as resp:
                return json.loads(resp.read().decode())
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            last_exc = e
            if attempt < retries:
                time.sleep(1 + attempt)
    raise RuntimeError(f"fetch_json failed: {url[:120]} — {last_exc}")


def supabase_headers() -> dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }


# ── Name normalization (suffix-aware) ────────────────────────────────────────

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def norm(s: str | None) -> str:
    if not s:
        return ""
    s = s.lower().replace(".", "").replace("'", "").replace("’", "").strip()
    return re.sub(r"\s+", " ", s)


def strip_suffix(last: str) -> str:
    parts = last.split()
    if len(parts) > 1 and parts[-1] in SUFFIXES:
        return " ".join(parts[:-1])
    return last


# ── Step 1: pull NFFC leagues + picks + team draftOrder ──────────────────────


def pull_nffc_leagues() -> list[dict]:
    """Get the 5-or-so OC leagues for the current year."""
    url = f"{NFFC_BASE}/publicleagues/football?api_key={NFFC_API_KEY}&game_type_id={GAME_TYPE_ID_OC}"
    return fetch_json(url)


def pull_league_picks(league_id: int) -> list[dict]:
    url = f"{NFFC_BASE}/publicdraftresults/football/{league_id}?api_key={NFFC_API_KEY}"
    data = fetch_json(url)
    return data.get("draft_results", [])


def pull_league_detail(league_id: int) -> dict:
    url = f"{NFFC_BASE}/publicleagues/football/{league_id}?api_key={NFFC_API_KEY}"
    return fetch_json(url)


def pull_nffc_adp() -> dict[str, dict]:
    """UUID → player_info (fname, lname, pos, team, dob) for fallback resolution."""
    url = f"{NFFC_BASE}/adp/football?api_key={NFFC_API_KEY}&game_type_id={GAME_TYPE_ID_OC}"
    rows = fetch_json(url)
    return {e["player"]: e["player_info"] for e in rows if "player_info" in e}


# ── Step 2: Supabase batch lookup ────────────────────────────────────────────


def supabase_player_lookup(uuids: list[str]) -> dict[str, dict]:
    """Batch-query Supabase players by player_id (= NFFC UUID).

    Returns {uuid: {first_name, last_name, position, birth_date, footballguys_id}}.
    Ignores non-UUID ids (NFFC sometimes uses bare integer pids for late rookies).
    """
    out: dict[str, dict] = {}
    uuid_only = [u for u in uuids if isinstance(u, str) and "-" in u]
    BATCH = 100  # PostgREST URL-length safety
    for i in range(0, len(uuid_only), BATCH):
        batch = uuid_only[i : i + BATCH]
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?player_id=in.({','.join(batch)})"
            f"&select=player_id,first_name,last_name,position,birth_date,footballguys_id"
        )
        rows = fetch_json(url, headers=supabase_headers())
        for r in rows:
            out[r["player_id"]] = r
    return out


# ── Step 3: FBG players for fallback name+DOB join ───────────────────────────


def build_fbg_indexes(fbg_players: list[dict]) -> tuple[dict, dict, dict, dict]:
    """Returns (by_full, by_name_pos, by_name, team_pk) where team_pk maps FBG team abbr → team-PK id."""
    by_full: dict[tuple, str] = {}
    by_name_pos: dict[tuple, str] = {}
    by_name: dict[tuple, list[str]] = {}
    team_pk: dict[str, str] = {}
    for p in fbg_players:
        # Team-PK entries are isTeamPositionGroup=true with pos='pk'; one per team.
        if p.get("isTeamPositionGroup") and norm(p.get("pos")) == "pk":
            team_pk[(p.get("team") or "").upper()] = p["id"]

        last = norm(p.get("last"))
        first = norm(p.get("first"))
        if not last or not first:
            continue
        last_s = strip_suffix(last)
        dob = p.get("dob") or ""
        pos = norm(p.get("pos"))
        for ln in {last, last_s}:
            by_full[(ln, first, dob)] = p["id"]
            by_name_pos[(ln, first, pos)] = p["id"]
            by_name.setdefault((ln, first), []).append(p["id"])
    return by_full, by_name_pos, by_name, team_pk


def resolve_via_name(
    info: dict,
    by_full: dict,
    by_name_pos: dict,
    by_name: dict,
) -> tuple[str | None, str]:
    """Try to resolve {first, last, dob, pos} → FBG id. Returns (id_or_None, method)."""
    last = norm(info.get("last"))
    first = norm(info.get("first"))
    if not last or not first:
        return None, "no_name"
    dob = (info.get("dob") or "").strip()
    if dob == "0000-00-00":
        dob = ""
    pos = norm(info.get("pos"))
    last_s = strip_suffix(last)

    # Build first-name candidates: the original + any formal expansions.
    first_candidates = [first] + NICKNAME_EXPANSIONS.get(first, [])

    if dob:
        for fn in first_candidates:
            for ln in {last, last_s}:
                if (ln, fn, dob) in by_full:
                    return by_full[(ln, fn, dob)], "name_dob"
    for fn in first_candidates:
        for ln in {last, last_s}:
            if (ln, fn, pos) in by_name_pos:
                return by_name_pos[(ln, fn, pos)], "name_pos"
    for fn in first_candidates:
        cands = by_name.get((last, fn), []) or by_name.get((last_s, fn), [])
        if len(cands) == 1:
            return cands[0], "name_only"
    return None, "ambiguous_or_missing"


# ── Main ─────────────────────────────────────────────────────────────────────


_NFFC_TZ = ZoneInfo("America/New_York")  # NFFC is in NJ; FBG converts to UTC for storage
_UTC = ZoneInfo("UTC")


def to_iso_z(ts: str) -> str:
    """Convert NFFC's 'YYYY-MM-DD HH:MM:SS' (America/New_York) to FBG's UTC 'YYYY-MM-DDTHH:MM:SSZ'."""
    if not ts:
        return ts
    if "T" in ts:
        return ts if ts.endswith("Z") else ts + "Z"
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=_NFFC_TZ)
        return dt.astimezone(_UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return ts.replace(" ", "T") + "Z"  # fallback if format unexpected


def main() -> None:
    print(f"Building NFFC OC fixture for {YEAR}…\n")

    print("1. Pulling NFFC leagues…")
    leagues = pull_nffc_leagues()
    print(f"   {len(leagues)} OC leagues: {[l['id'] for l in leagues]}")

    print("\n2. Pulling picks + team detail per league…")
    all_picks: list[dict] = []
    all_teams: list[dict] = []
    for lg in leagues:
        league_id = lg["id"]
        picks = pull_league_picks(league_id)
        detail = pull_league_detail(league_id)
        for p in picks:
            rd = p["round"]
            overall = p["pick"]
            all_picks.append(
                {
                    "leagueId": league_id,
                    "round": rd,
                    "pickInRound": overall - (rd - 1) * NUM_TEAMS,
                    "overallPick": overall,
                    "teamId": int(p["team"]),
                    "playerId": None,  # filled below
                    "pickedAt": to_iso_z(p.get("timestamp", "")),
                    "pickDuration": p.get("pick_duration") or 0,
                    "_nffcPlayer": p["player"],  # internal, dropped before write
                }
            )
        for t in detail.get("teams", []):
            all_teams.append(
                {
                    "leagueId": league_id,
                    "teamId": int(t["id"]),
                    "draftOrder": t.get("draft_order"),
                    "leagueRank": None,
                    "leaguePoints": None,
                    "overallRank": None,
                    "overallPoints": None,
                }
            )
    print(f"   {len(all_picks)} picks, {len(all_teams)} teams")

    print("\n3. Pulling NFFC ADP for player_info fallback…")
    nffc_adp = pull_nffc_adp()
    print(f"   {len(nffc_adp)} ADP entries with player_info")

    print("\n4. Pulling FBG players for name/DOB fallback…")
    fbg_players = fetch_json(f"{FBG_BASE}/api/nfl/{YEAR}/players")
    print(f"   {len(fbg_players)} FBG players")
    by_full, by_name_pos, by_name, team_pk = build_fbg_indexes(fbg_players)
    print(f"   {len(team_pk)} team-PK entries indexed")

    print("\n5. Querying Supabase for NFFC UUID → footballguys_id…")
    unique_uuids = sorted({p["_nffcPlayer"] for p in all_picks})
    supa_lookup = supabase_player_lookup([u for u in unique_uuids if isinstance(u, str)])
    in_supa = sum(1 for u in unique_uuids if u in supa_lookup)
    fbg_in_supa = sum(1 for u in unique_uuids if supa_lookup.get(u, {}).get("footballguys_id"))
    print(
        f"   {in_supa}/{len(unique_uuids)} unique UUIDs in Supabase, "
        f"{fbg_in_supa} have footballguys_id set"
    )

    print("\n6. Resolving each pick…")
    stats = {
        "alias": 0,
        "supabase": 0,
        "team_pk": 0,
        "name_dob": 0,
        "name_pos": 0,
        "name_only": 0,
        "unresolved": 0,
    }
    unresolved_samples: list[dict] = []

    for p in all_picks:
        nffc_uuid = p["_nffcPlayer"]
        rec = supa_lookup.get(nffc_uuid)

        # Build a normalized info dict for alias / name fallbacks. NFFC's team
        # abbr lives only in player_info, so always read it from there.
        pi = nffc_adp.get(nffc_uuid, {})
        if rec:
            info = {
                "first": rec.get("first_name"),
                "last": rec.get("last_name"),
                "dob": rec.get("birth_date"),
                "pos": rec.get("position"),
                "team": pi.get("team"),
            }
        else:
            info = {
                "first": pi.get("fname"),
                "last": pi.get("lname"),
                "dob": pi.get("dob"),
                "pos": pi.get("pos"),
                "team": pi.get("team"),
            }

        # 1. Alias check (highest priority — covers known nicknames)
        alias_key = (norm(info.get("first")), norm(info.get("last")))
        if alias_key in ALIASES:
            p["playerId"] = ALIASES[alias_key]
            stats["alias"] += 1
            continue

        # 2. Supabase direct join
        if rec and rec.get("footballguys_id"):
            p["playerId"] = rec["footballguys_id"]
            stats["supabase"] += 1
            continue

        # 3. NFFC team-kicker pseudo-player → FBG team-PK entry by team abbr
        if norm(info.get("pos")) == "tk":
            nffc_team = (info.get("team") or "").upper()
            fbg_team = NFFC_TO_FBG_TEAM.get(nffc_team, nffc_team)
            if fbg_team in team_pk:
                p["playerId"] = team_pk[fbg_team]
                stats["team_pk"] += 1
                continue

        # 4-6. Name-based fallback
        fbg_id, method = resolve_via_name(info, by_full, by_name_pos, by_name)
        if fbg_id:
            p["playerId"] = fbg_id
            stats[method] = stats.get(method, 0) + 1
        else:
            stats["unresolved"] += 1
            if len(unresolved_samples) < 15:
                unresolved_samples.append(
                    {
                        "uuid": nffc_uuid,
                        "league": p["leagueId"],
                        "overall_pick": p["overallPick"],
                        "info": info,
                    }
                )

    # Drop internal field
    for p in all_picks:
        p.pop("_nffcPlayer", None)

    # ── Write output ─────────────────────────────────────────────────────────
    OUT.parent.mkdir(parents=True, exist_ok=True)
    output = {"year": YEAR, "teams": all_teams, "picks": all_picks}
    OUT.write_text(json.dumps(output, indent=2))

    total = len(all_picks)
    resolved = sum(1 for p in all_picks if p["playerId"])

    print()
    print("=" * 60)
    print(f"Resolution stats — {total} picks across {len(leagues)} leagues:")
    for k, v in sorted(stats.items(), key=lambda x: -x[1]):
        if v:
            print(f"  {k:18s} {v:5d}")
    print(f"  {'TOTAL RESOLVED':18s} {resolved:5d} ({resolved * 100 / total:.1f}%)")

    if unresolved_samples:
        print()
        print("Sample unresolved picks (first 15):")
        for s in unresolved_samples:
            print(f"  L{s['league']} pick {s['overall_pick']:3d} uuid={str(s['uuid'])[:20]} info={s['info']}")
        print()
        print(
            "→ For UUIDs that ARE in Supabase but missing footballguys_id: "
            "run nfl-db/scripts/players/enrich_from_fbg.py to enrich the players table."
        )
        print(
            "→ For NFFC names that need an alias (nicknames, team-D entries): "
            "add to ALIASES at the top of this script."
        )

    print()
    print(f"Wrote {OUT}")
    print(f"  ({OUT.stat().st_size / 1024:.1f} KB)")


if __name__ == "__main__":
    main()
