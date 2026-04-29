"""Fetch DraftKings POST-DRAFT Best Ball ADP and upsert into adp_sources.

Mirrors fetch_draftkings_adp.py but for the post-draft draftgroup that
opened after the 2026 NFL Draft. Differences from pre-draft:
  - SOURCE = "draftkings_postdraft"
  - DK_API_URL points at draftgroup 146136 (vs 141336 pre-draft)
  - Reuses existing data/dk_session.json (same DK login, no separate setup)

Re-run setup_dk_session.py if you see auth errors (DK session ~2 weeks).

Usage:
    python3 scripts/adp/fetch_draftkings_postdraft_adp.py [--dry-run]
"""

import datetime
import json
import os
import sys
import urllib.error
import urllib.request

_script_dir = os.path.dirname(os.path.abspath(__file__)) if os.path.exists(__file__) else os.path.join("scripts", "adp")
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import (
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_SERVICE_KEY,
    normalize_name,
    PLAYER_ALIASES,
)

# ── Config ───────────────────────────────────────────────────────────────────
YEAR = 2026
SOURCE = "draftkings_postdraft"
TODAY = datetime.date.today().isoformat()

DK_API_URL = "https://api.draftkings.com/rankings/v1/draftgroups/146136/playerpool?format=json"
SESSION_FILE = os.path.normpath(os.path.join(_script_dir, "..", "..", "data", "dk_session.json"))

POSITION_MAP = {1: "QB", 2: "RB", 3: "WR", 4: "TE", 5: "K", 6: "DST"}


def fetch_dk_players():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: playwright not installed.")
        print("  Run: pip install playwright && python3 -m playwright install chromium")
        sys.exit(1)

    if not os.path.exists(SESSION_FILE):
        print("ERROR: No saved session found.")
        print(f"  Expected: {SESSION_FILE}")
        print("  Run setup first (with Chrome closed):")
        print("    python3 scripts/adp/setup_dk_session.py")
        sys.exit(1)

    print(f"  Loading session from {os.path.basename(SESSION_FILE)}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state=SESSION_FILE)
        page = context.new_page()
        print(f"  Fetching DK post-draft playerpool API (draftgroup 146136)…")
        resp = page.goto(DK_API_URL, wait_until="load", timeout=30000)

        if resp is None or resp.status != 200:
            status = resp.status if resp else "no response"
            print(f"ERROR: HTTP {status} from DK API")
            if resp and resp.status in (401, 403):
                print("  Session has expired. Re-run setup (with Chrome closed):")
                print("    python3 scripts/adp/setup_dk_session.py")
            context.close()
            browser.close()
            sys.exit(1)

        raw = page.evaluate("document.body.innerText")
        context.close()
        browser.close()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"ERROR: Could not parse JSON response: {e}")
        print(f"  Response preview: {raw[:200]}")
        sys.exit(1)

    players = data.get("playerPool", {}).get("draftablePlayers", [])
    if not players:
        print("ERROR: No players in response. Top-level keys:", list(data.keys()))
        sys.exit(1)

    return players


def fetch_players_with_dk_id():
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,draftkings_id,first_name,last_name,position"
            f"&draftkings_id=not.is.null"
            f"&offset={offset}&limit=1000"
        )
        req = urllib.request.Request(url, headers={"apikey": key, "Authorization": f"Bearer {key}"})
        batch = json.loads(urllib.request.urlopen(req).read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += 1000
    return players


def fetch_all_players_for_name_match():
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,first_name,last_name,position"
            f"&offset={offset}&limit=1000"
        )
        req = urllib.request.Request(url, headers={"apikey": key, "Authorization": f"Bearer {key}"})
        batch = json.loads(urllib.request.urlopen(req).read().decode("utf-8"))
        if not batch:
            break
        players.extend(batch)
        offset += 1000
    return players


def batch_upsert(rows, batch_size=100):
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/adp_sources"
    inserted, errors = 0, 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        data = json.dumps(batch).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={
            "apikey": key, "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates",
        }, method="POST")
        try:
            urllib.request.urlopen(req)
            inserted += len(batch)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            print(f"  ERROR batch at row {i}: {e.code} {body}")
            errors += len(batch)
    return inserted, errors


def main():
    dry_run = "--dry-run" in sys.argv

    print("Fetching DraftKings post-draft ADP...")
    dk_players = fetch_dk_players()
    print(f"  {len(dk_players)} players in response")

    print("Fetching players from Supabase...")
    db_players = fetch_players_with_dk_id()
    dk_to_pid = {p["draftkings_id"]: p["player_id"] for p in db_players if p.get("draftkings_id")}
    print(f"  {len(dk_to_pid)} players with draftkings_id")

    all_players = fetch_all_players_for_name_match()
    by_name_pos, by_name = {}, {}
    for p in all_players:
        full_name = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
        norm = normalize_name(full_name)
        pos = (p.get("position") or "").upper()
        by_name_pos[(norm, pos)] = p["player_id"]
        by_name[norm] = p["player_id"]
        alias = PLAYER_ALIASES.get(norm)
        if alias:
            by_name_pos[(alias, pos)] = p["player_id"]
            by_name[alias] = p["player_id"]

    adp_rows = []
    not_found = []
    skipped_no_adp = 0

    for player in dk_players:
        dk_id = str(player.get("playerId", "")).strip()
        adp_val = player.get("averageDraftPosition")

        if adp_val is None:
            skipped_no_adp += 1
            continue
        try:
            adp_val = float(adp_val)
        except (ValueError, TypeError):
            skipped_no_adp += 1
            continue

        name = (player.get("displayName") or "").strip()
        pos = ""
        slots = player.get("draftableRosterPositions") or []
        if slots:
            pos_id = slots[0].get("teamPositionId")
            pos = POSITION_MAP.get(pos_id, "")

        player_id = dk_to_pid.get(dk_id)
        if not player_id:
            norm = normalize_name(name)
            player_id = by_name_pos.get((norm, pos)) or by_name.get(norm)

        if not player_id:
            not_found.append(f"  {name} ({pos}) [dk_id={dk_id}] adp={adp_val:.2f}")
            continue

        adp_rows.append({
            "player_id": player_id,
            "source": SOURCE,
            "year": YEAR,
            "date": TODAY,
            "adp": adp_val,
            "projected_points": None,
            "position_rank": None,
        })

    print(f"\n  Matched & ready: {len(adp_rows)}")
    print(f"  Skipped (no ADP): {skipped_no_adp}")
    print(f"  Not found in DB: {len(not_found)}")

    if adp_rows and not dry_run:
        print(f"\nUpserting {len(adp_rows)} rows to adp_sources...")
        inserted, errors = batch_upsert(adp_rows)
        print(f"  Inserted/updated: {inserted}")
        if errors:
            print(f"  Errors: {errors}")
    elif dry_run:
        print(f"\n[DRY RUN] Would upsert {len(adp_rows)} rows. Sample:")
        for row in adp_rows[:3]:
            print(f"  {row}")

    print(f"\n{'='*50}\nSUMMARY\n{'='*50}")
    print(f"DK players in response: {len(dk_players)}")
    print(f"Matched & upserted:     {len(adp_rows)}")
    print(f"Not found in DB:        {len(not_found)}")
    print(f"Skipped (no ADP):       {skipped_no_adp}")

    if not_found:
        print(f"\nUnmatched players (top 20):")
        for line in not_found[:20]:
            print(line)
        if len(not_found) > 20:
            print(f"  ... and {len(not_found) - 20} more")


if __name__ == "__main__":
    main()
