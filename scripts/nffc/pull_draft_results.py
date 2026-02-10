"""
Pull draft results for all NFFC leagues across all years (2018-2025).
Saves results as one JSON file per year in data/raw/drafts/.

Usage: python3 scripts/pull_draft_results.py
"""

import json
import os
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Config
API_KEY = os.environ.get("NFFC_API_KEY", "22c3eaf3f16842fda979d38c83880386")
BASE_URL = "https://nfc.shgn.com/api/public"
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
DRAFTS_DIR = DATA_DIR / "drafts"
LEAGUES_DIR = DATA_DIR / "league_details"
MAX_WORKERS = 5  # concurrent requests — be polite to the API
YEARS_HISTORICAL = range(2018, 2025)  # 2018-2024 use historical endpoints
YEAR_CURRENT = 2025


HEADERS = {"User-Agent": "NFFC-Draft-Explorer/1.0"}


def fetch_json(url):
    """Fetch JSON from a URL, return parsed data or None on error."""
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, json.JSONDecodeError, Exception) as e:
        return {"_error": str(e)}


def get_leagues_for_year(year):
    """Get list of leagues for a given year."""
    leagues_file = DATA_DIR / f"historical_leagues_{year}.json"
    if leagues_file.exists():
        with open(leagues_file) as f:
            return json.load(f)

    if year == YEAR_CURRENT:
        url = f"{BASE_URL}/publicleagues/football?api_key={API_KEY}"
    else:
        url = f"{BASE_URL}/historicalleagues/football/{year}?api_key={API_KEY}"

    data = fetch_json(url)
    if isinstance(data, list):
        with open(leagues_file, 'w') as f:
            json.dump(data, f, indent=2)
    return data if isinstance(data, list) else []


def pull_draft_for_league(year, league_id):
    """Pull draft results for a single league."""
    if year == YEAR_CURRENT:
        url = f"{BASE_URL}/publicdraftresults/football/{league_id}?api_key={API_KEY}"
    else:
        url = f"{BASE_URL}/historicaldraftresults/football/{year}/{league_id}?api_key={API_KEY}"

    data = fetch_json(url)

    if isinstance(data, dict) and "draft_results" in data:
        return data["draft_results"]
    elif isinstance(data, dict) and "_error" in data:
        return None
    elif isinstance(data, dict) and "message" in data:
        return None  # e.g. "Invalid league id"
    return data


def pull_league_detail(year, league_id):
    """Pull league detail (roster, scoring, team outcomes) for a single league."""
    if year == YEAR_CURRENT:
        url = f"{BASE_URL}/publicleagues/football/{league_id}?api_key={API_KEY}"
    else:
        url = f"{BASE_URL}/historicalleagues/football/{year}/{league_id}?api_key={API_KEY}"

    return fetch_json(url)


def process_year(year, pull_details=True):
    """Pull all draft results and league details for a year."""
    DRAFTS_DIR.mkdir(parents=True, exist_ok=True)
    LEAGUES_DIR.mkdir(parents=True, exist_ok=True)

    drafts_file = DRAFTS_DIR / f"drafts_{year}.json"
    details_file = LEAGUES_DIR / f"league_details_{year}.json"

    # Check if already done
    if drafts_file.exists() and (not pull_details or details_file.exists()):
        with open(drafts_file) as f:
            existing = json.load(f)
        print(f"{year}: Already pulled ({len(existing)} leagues with drafts)")
        return

    leagues = get_leagues_for_year(year)
    print(f"{year}: {len(leagues)} leagues to process...")

    all_drafts = {}
    all_details = {}
    errors = 0

    def fetch_league(league):
        league_id = league["id"]
        draft = pull_draft_for_league(year, league_id)
        detail = pull_league_detail(year, league_id) if pull_details else None
        return league_id, league["name"], draft, detail

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_league, l): l for l in leagues}
        done = 0
        for future in as_completed(futures):
            done += 1
            league_id, name, draft, detail = future.result()

            if draft is not None:
                all_drafts[str(league_id)] = {
                    "league_id": league_id,
                    "league_name": name,
                    "picks": draft
                }
            else:
                errors += 1

            if detail is not None and not (isinstance(detail, dict) and "_error" in detail):
                all_details[str(league_id)] = detail

            if done % 100 == 0:
                print(f"  {year}: {done}/{len(leagues)} done ({errors} errors)")

    # Save
    with open(drafts_file, 'w') as f:
        json.dump(all_drafts, f)
    print(f"{year}: Saved {len(all_drafts)} drafts ({errors} errors) -> {drafts_file.name}")

    if pull_details and all_details:
        with open(details_file, 'w') as f:
            json.dump(all_details, f)
        print(f"{year}: Saved {len(all_details)} league details -> {details_file.name}")


def main():
    start = time.time()

    # Process historical years first
    for year in YEARS_HISTORICAL:
        process_year(year, pull_details=True)

    # Process current year (no outcomes yet for 2025)
    process_year(YEAR_CURRENT, pull_details=True)

    elapsed = time.time() - start
    print(f"\nDone! Total time: {elapsed:.0f}s ({elapsed/60:.1f}min)")


if __name__ == "__main__":
    main()
