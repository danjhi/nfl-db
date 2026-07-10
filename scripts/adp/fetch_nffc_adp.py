"""Fetch NFFC ADP and upsert into adp_sources.

NFFC (nfc.shgn.com) publishes ADP through a public API. Its `player` id IS our
players.player_id (Sportradar UUID) — the same identity NFFC's draft feeds use —
so offense joins on the id directly, which is immune to the name-collision bug
that muddies FBG's stored NFFC data (e.g. Marquise vs A.J. Brown).

Three sources are published from the same endpoint, differing only by
game_type_id (all 2026-specific — bump yearly; see build_oc_local_fixture.py):
  - "nffc_oc"     — the $350 Footballguys Online Championship (game_type_id=936).
                    The number we display on the /adp page.
  - "nffc"        — all NFFC contests, un-broken-out (no game_type_id). Matches the
                    old footballguys.com/adp "NFFC" column. Kept in the DB as a
                    low-cost hedge / time series; may not be displayed.
  - "bestball10s" — NFFC BestBall10s (game_type_id=941), the best-ball-tab source.
                    Replaces the same key FBG's feed carries (confirmed by matching
                    FBG's `bestball10s` column values).

Position handling (NFFC drafts team K/DST, not individuals):
  - Offense (QB/RB/WR/TE): join on `player` == players.player_id (UUID). A few
    UDFA/late rookies carry a bare integer id instead of a UUID — name+pos
    fallback covers those.
  - Team defense (pos "TDSP"): NFFC's UUID matches our legacy TDSP artifact rows,
    but the /adp page renders defenses on the clean DEF_{TEAM} rows, so we remap
    TDSP -> DEF_{TEAM} by team. NFFC defense then shares the defense row with
    every other source (RTSports, FBG feed, ...).
  - Team kicker (pos "TK"): NFFC's integer id matches our team-kicker (TK) rows
    directly. Team kickers are an NFFC-only unit (other sources draft individual
    kickers), so they stay on their own TK entity — the consumer shows them only
    under the NFFC source and excludes them from the individual-kicker consensus.
    Deliberately NOT given a footballguys_id, so FBG's own feed can't pull a
    number onto them; team-kicker ADP is sourced purely from here.

See docs/adp-kicker-defense-join.md and docs/adp-source-scrapers.md.

Usage:
    python3 scripts/adp/fetch_nffc_adp.py            # fetch + upsert both sources
    python3 scripts/adp/fetch_nffc_adp.py --dry-run  # fetch + match, no write
    python3 scripts/adp/fetch_nffc_adp.py --source nffc_oc   # one source only
"""

import argparse
import datetime
import json
import os
import ssl
import sys
import urllib.error
import urllib.request

# Add ids dir so shared imports work (mirrors the other adp scrapers).
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
TODAY = datetime.date.today().isoformat()

NFFC_BASE = "https://nfc.shgn.com/api/public"
NFFC_API_KEY = os.environ.get("NFFC_API_KEY", "")
# NFFC returns 403 to a default urllib User-Agent — must set one.
USER_AGENT = "NFFC-Draft-Explorer/1.0"

# game_type_id=936 is the 2026 Footballguys Online Championship. It CHANGES
# YEARLY (see scripts/nffc/build_oc_local_fixture.py GAME_TYPE_ID_OC) — bump it
# each season. None = all contests (un-filtered), matching the old "NFFC" column.
SOURCES = {
    "nffc_oc": 936,       # displayed: FBG Online Championship
    "nffc": None,         # hedge: all NFFC contests blended
    "bestball10s": 941,   # NFFC BestBall10s product (best-ball tab; replaces FBG feed)
}

# macOS system Python often lacks a usable trust store; prefer certifi.
try:
    import certifi

    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()


# ── Fetch ────────────────────────────────────────────────────────────────────
def fetch_nffc_adp(game_type_id=None):
    """Download NFFC ADP rows. Returns a list of entries."""
    url = f"{NFFC_BASE}/adp/football?api_key={NFFC_API_KEY}"
    if game_type_id is not None:
        url += f"&game_type_id={game_type_id}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=45, context=SSL_CTX) as resp:
        data = json.loads(resp.read().decode())
    if not isinstance(data, list):
        raise RuntimeError(f"unexpected NFFC response: {str(data)[:200]}")
    return data


def fetch_all_players():
    """Fetch all players (paginated): player_id, name, position, latest_team."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    limit = 1000
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,first_name,last_name,position,latest_team"
            f"&offset={offset}&limit={limit}"
        )
        req = urllib.request.Request(url, headers={
            "apikey": key, "Authorization": f"Bearer {key}",
        })
        with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as resp:
            batch = json.loads(resp.read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += limit
    return players


# ── Upsert ───────────────────────────────────────────────────────────────────
def batch_upsert(rows, batch_size=100):
    """POST rows to adp_sources in batches (idempotent merge-duplicates)."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/adp_sources"
    inserted = 0
    errors = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        data = json.dumps(batch).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates",
        }, method="POST")
        try:
            urllib.request.urlopen(req, timeout=60, context=SSL_CTX)
            inserted += len(batch)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"  ERROR batch at row {i}: {e.code} {body}")
            errors += len(batch)
    return inserted, errors


# ── Resolve one NFFC entry to a players.player_id ────────────────────────────
def resolve_entry(entry, valid_pids, def_by_team, by_name_pos, by_name):
    """Return (player_id, category) or (None, category) if unmatched.

    category ∈ {offense, tk, def} — for reporting.
    """
    pi = entry.get("player_info") or {}
    pos = (pi.get("pos") or "").upper()
    raw = entry.get("player")
    pid = str(raw) if raw is not None else None

    if pos == "TDSP":
        # Team defense -> converge on the clean DEF_{TEAM} row (via team).
        team = normalize_team(pi.get("team") or "")
        return def_by_team.get(team), "def"

    if pos == "TK":
        # Team kicker -> its own TK entity (NFFC id == our player_id).
        return (pid if pid in valid_pids else None), "tk"

    # Offense: id join first, name+pos fallback for bare-integer UDFA/rookies.
    if pid and pid in valid_pids:
        return pid, "offense"
    norm = normalize_name(f"{pi.get('fname', '')} {pi.get('lname', '')}")
    target = by_name_pos.get((norm, pos)) or by_name.get(norm)
    return target, "offense"


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Fetch NFFC ADP into adp_sources.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch + match + report, but do not write to Supabase.")
    parser.add_argument("--source", choices=sorted(SOURCES),
                        help="Only fetch one source (default: both).")
    args = parser.parse_args()

    if not NFFC_API_KEY:
        sys.exit("NFFC_API_KEY not set (check .env).")

    # ── Build player lookups (shared across both sources) ────────────────────
    print("Fetching players from Supabase...")
    all_players = fetch_all_players()
    valid_pids = {str(p["player_id"]) for p in all_players}
    def_by_team = {
        normalize_team(p["latest_team"] or ""): p["player_id"]
        for p in all_players if p["position"] == "DEF"
    }
    by_name_pos, by_name = build_player_lookup(all_players)
    print(f"  {len(all_players)} players ({len(def_by_team)} team defenses)")

    sources = [args.source] if args.source else list(SOURCES)
    grand_matched = 0

    for source in sources:
        game_type_id = SOURCES[source]
        label = f"{source} (game_type_id={game_type_id})" if game_type_id else f"{source} (all contests)"
        print(f"\n{'=' * 60}\nSOURCE: {label}\n{'=' * 60}")
        entries = fetch_nffc_adp(game_type_id)
        print(f"  {len(entries)} NFFC entries")

        adp_rows = []
        counts = {"offense": [0, 0], "tk": [0, 0], "def": [0, 0]}  # [matched, unmatched]
        unmatched = []
        for entry in entries:
            adp_val = entry.get("adp")
            if adp_val is None:
                continue
            try:
                adp_num = float(adp_val)
            except (ValueError, TypeError):
                continue

            target, category = resolve_entry(entry, valid_pids, def_by_team, by_name_pos, by_name)
            if target:
                counts[category][0] += 1
                adp_rows.append({
                    "player_id": target,
                    "source": source,
                    "year": YEAR,
                    "date": TODAY,
                    "adp": adp_num,
                    "projected_points": None,
                    "position_rank": None,
                })
            else:
                counts[category][1] += 1
                pi = entry.get("player_info") or {}
                unmatched.append(
                    f"  {pi.get('fname', '?')} {pi.get('lname', '?')} "
                    f"({pi.get('pos', '?')}, {pi.get('team', '?')}) "
                    f"player={entry.get('player')} adp={adp_val}"
                )

        matched = len(adp_rows)
        grand_matched += matched
        print(f"  Matched: {matched}  "
              f"[offense {counts['offense'][0]}, tk {counts['tk'][0]}, def {counts['def'][0]}]")
        unmatched_total = sum(c[1] for c in counts.values())
        print(f"  Unmatched: {unmatched_total}  "
              f"[offense {counts['offense'][1]}, tk {counts['tk'][1]}, def {counts['def'][1]}]")
        if unmatched:
            print("  Unmatched entries:")
            for line in unmatched[:20]:
                print(line)
            if len(unmatched) > 20:
                print(f"    ... and {len(unmatched) - 20} more")

        if args.dry_run:
            print(f"  [dry-run] would upsert {matched} rows.")
        elif adp_rows:
            print(f"  Upserting {matched} rows (source={source})...")
            inserted, errors = batch_upsert(adp_rows)
            print(f"    Inserted/updated: {inserted}" + (f"  Errors: {errors}" if errors else ""))

    print(f"\n{'=' * 60}\nDONE ({'dry-run' if args.dry_run else 'wrote'}) — "
          f"{grand_matched} rows across {len(sources)} source(s), {TODAY}\n{'=' * 60}")


if __name__ == "__main__":
    main()
