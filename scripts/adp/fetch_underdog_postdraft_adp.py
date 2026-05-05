"""Fetch Underdog POST-DRAFT ADP and upsert into adp_sources.

Mirrors fetch_underdog_adp.py but for the post-draft best ball contest type
that opened after the 2026 NFL Draft. Differences from pre-draft:
  - SOURCE = "underdog_postdraft" (separate adp_sources rows)
  - Different CSV URL (3 different UUIDs in the path; domain migrated from
    underdogfantasy.com → underdogsports.com in May 2026)
  - Matches by `underdog_postdraft_id` first, falls back to name+pos
  - On name-fallback match, writes the new underdog id back to
    `players.underdog_postdraft_id` so future runs match by ID

Cloudflare bot management blocks plain urllib + curl on the Underdog endpoint
as of May 2026. We use Playwright Chromium with a saved session file
(data/underdog_session.json) populated by setup_underdog_session.py — same
pattern as DraftKings. Re-run setup when 403 errors return (~every 2 weeks).

Usage:
    python3 scripts/adp/fetch_underdog_postdraft_adp.py [--dry-run]
"""

import csv
import datetime
import io
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
SOURCE = "underdog_postdraft"
TODAY = datetime.date.today().isoformat()

UNDERDOG_CSV_URL = (
    "https://app.underdogsports.com/rankings/download/"
    "a9c04e81-1ace-4b16-a31d-4c725a47f16f/"
    "ccf300b0-9197-5951-bd96-cba84ad71e86/"
    "9e62863e-1b29-53e8-8aca-2aae06aaac5f"
    "?product=fantasy"
    "&product_experience_id=018e1234-5678-9abc-def0-123456789002"
    "&state_config_id=7b937c4c-58ae-467c-90e7-c8dc2202a02a"
)

SESSION_FILE = os.path.normpath(os.path.join(_script_dir, "..", "..", "data", "underdog_session.json"))
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"

SLOT_TO_POS = {"QB": "QB", "RB": "RB", "WR": "WR", "TE": "TE", "K": "K", "FLEX": None}


def fetch_underdog_csv():
    """Use saved session + Playwright Chromium to download the Underdog CSV.

    The Underdog CSV endpoint is behind Cloudflare bot management as of
    May 2026 — plain urllib gets 403. Playwright with valid cookies +
    stealth args passes the check.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: playwright not installed.")
        print("  Run: pip install playwright && python3 -m playwright install chromium")
        sys.exit(1)

    if not os.path.exists(SESSION_FILE):
        print("ERROR: No saved Underdog session found.")
        print(f"  Expected: {SESSION_FILE}")
        print("  Run setup first:")
        print("    python3 scripts/adp/setup_underdog_session.py --test")
        sys.exit(1)

    print(f"  Loading session from {os.path.basename(SESSION_FILE)}")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            storage_state=SESSION_FILE,
            accept_downloads=True,
            user_agent=USER_AGENT,
        )
        page = ctx.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        text = None
        try:
            with page.expect_download(timeout=30000) as dl_info:
                try:
                    page.goto(UNDERDOG_CSV_URL, timeout=30000)
                except Exception:
                    pass  # 'Download is starting' is the expected exception
            dl = dl_info.value
            path = dl.path()
            with open(path) as f:
                text = f.read()
        except Exception as e:
            print(f"ERROR: download failed: {type(e).__name__}: {str(e)[:200]}")
            print("  Likely Cloudflare 403 — re-run setup_underdog_session.py")
            ctx.close()
            browser.close()
            sys.exit(1)

        ctx.close()
        browser.close()

    return list(csv.DictReader(io.StringIO(text)))


def fetch_players_with_postdraft_id():
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    players = []
    offset = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/players"
            f"?select=player_id,underdog_postdraft_id,first_name,last_name,position"
            f"&underdog_postdraft_id=not.is.null"
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


def patch_underdog_postdraft_id(player_id, ud_id):
    """PATCH a single player to set underdog_postdraft_id (for first-run backfill)."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/players?player_id=eq.{player_id}"
    data = json.dumps({"underdog_postdraft_id": ud_id}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={
        "apikey": key, "Authorization": f"Bearer {key}",
        "Content-Type": "application/json", "Prefer": "return=minimal",
    }, method="PATCH")
    urllib.request.urlopen(req)


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

    print("Fetching Underdog post-draft ADP CSV...")
    ud_rows = fetch_underdog_csv()
    print(f"  {len(ud_rows)} rows downloaded")

    print("Fetching players from Supabase...")
    pd_players = fetch_players_with_postdraft_id()
    pd_to_pid = {p["underdog_postdraft_id"]: p["player_id"] for p in pd_players if p.get("underdog_postdraft_id")}
    print(f"  {len(pd_to_pid)} players with underdog_postdraft_id")

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
    skipped_no_adp = 0
    not_found = []
    backfill_count = 0
    matched_by_id = 0
    matched_by_name = 0

    for row in ud_rows:
        ud_id = row.get("id", "").strip()
        adp_val = row.get("adp", "").strip()
        if not adp_val or adp_val == "-":
            skipped_no_adp += 1
            continue

        first = row.get("firstName", "").strip()
        last = row.get("lastName", "").strip()
        name = f"{first} {last}"
        slot = row.get("slotName", "").strip()

        # Match by underdog_postdraft_id first
        player_id = pd_to_pid.get(ud_id)
        match_method = "id"

        # Fallback: name+position
        if not player_id:
            norm = normalize_name(name)
            pos = SLOT_TO_POS.get(slot, slot)
            if pos:
                player_id = by_name_pos.get((norm, pos)) or by_name.get(norm)
            else:
                player_id = by_name.get(norm)
            if player_id:
                match_method = "name"

        if not player_id:
            not_found.append(f"  {name} ({slot}) [ud_id={ud_id}] adp={adp_val}")
            continue

        # Backfill: if matched by name, write the underdog_postdraft_id back
        if match_method == "name" and ud_id and not dry_run:
            try:
                patch_underdog_postdraft_id(player_id, ud_id)
                pd_to_pid[ud_id] = player_id  # update cache
                backfill_count += 1
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                print(f"  WARN: PATCH failed for {name}: {e.code} {body[:150]}")

        if match_method == "id":
            matched_by_id += 1
        else:
            matched_by_name += 1

        proj_pts = row.get("projectedPoints", "").strip()
        pos_rank = row.get("positionRank", "").strip()

        adp_rows.append({
            "player_id": player_id,
            "source": SOURCE,
            "year": YEAR,
            "date": TODAY,
            "adp": float(adp_val),
            "projected_points": float(proj_pts) if proj_pts and proj_pts != "0.0" else None,
            "position_rank": pos_rank if pos_rank else None,
        })

    print(f"\n  Matched by underdog_postdraft_id: {matched_by_id}")
    print(f"  Matched by name+pos (backfilled):  {matched_by_name}")
    print(f"  Backfilled IDs to players table:    {backfill_count}")
    print(f"  Skipped (no ADP):                  {skipped_no_adp}")
    print(f"  Not found in DB:                   {len(not_found)}")

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
    print(f"Underdog CSV rows:   {len(ud_rows)}")
    print(f"Matched & upserted:  {len(adp_rows)}")
    print(f"Not found in DB:     {len(not_found)}")
    print(f"Skipped (no ADP):    {skipped_no_adp}")

    if not_found:
        print(f"\nUnmatched players (top 20):")
        for line in not_found[:20]:
            print(line)
        if len(not_found) > 20:
            print(f"  ... and {len(not_found) - 20} more")


if __name__ == "__main__":
    main()
