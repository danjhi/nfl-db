"""One-time setup: extract DraftKings cookies from Chrome and save session.

Reads Chrome's cookie database directly (no app control, no permission prompts).
Chrome can be open or closed. May prompt once for Keychain access to decrypt cookies.

Re-run when the daily fetch starts returning auth errors (~2 weeks).

Usage:
    python3 scripts/adp/setup_dk_session.py [--test]

    --test: also verifies the saved session works against the DK API
"""

import json
import os
import sys

_script_dir = os.path.dirname(os.path.abspath(__file__)) if os.path.exists(__file__) else os.path.join("scripts", "adp")
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import SUPABASE_URL  # noqa — triggers .env loading

CHROME_PROFILE = os.environ.get("DK_CHROME_PROFILE", "Profile 2").strip("\"'")
CHROME_PROFILE_DIR = os.path.join(os.path.expanduser("~"), "Library/Application Support/Google/Chrome", CHROME_PROFILE)
COOKIE_FILE = os.path.join(CHROME_PROFILE_DIR, "Cookies")

SESSION_FILE = os.path.normpath(os.path.join(_script_dir, "..", "..", "data", "dk_session.json"))
DK_DOMAIN = ".draftkings.com"
DK_API_URL = "https://api.draftkings.com/rankings/v1/draftgroups/141336/playerpool?format=json"


def extract_dk_cookies():
    """Read DK cookies from Chrome's Profile 2 cookie database."""
    try:
        import browser_cookie3
    except ImportError:
        print("ERROR: browser-cookie3 not installed.")
        print("  Run: pip install browser-cookie3")
        sys.exit(1)

    if not os.path.exists(COOKIE_FILE):
        print(f"ERROR: Cookie file not found: {COOKIE_FILE}")
        print(f"  Make sure Chrome profile '{CHROME_PROFILE}' exists and you've logged in to DraftKings there.")
        sys.exit(1)

    print(f"  Reading cookies from Chrome '{CHROME_PROFILE}' profile...")
    print("  (You may see a Keychain prompt — click Allow)")

    try:
        cj = browser_cookie3.chrome(cookie_file=COOKIE_FILE, domain_name="draftkings.com")
    except Exception as e:
        print(f"ERROR reading cookies: {e}")
        sys.exit(1)

    # Convert to Playwright storage_state cookie format
    cookies = []
    for cookie in cj:
        same_site = "None"
        rest = getattr(cookie, "_rest", {}) or {}
        if rest.get("SameSite"):
            ss = rest["SameSite"].capitalize()
            if ss in ("Strict", "Lax", "None"):
                same_site = ss

        cookies.append({
            "name": cookie.name,
            "value": cookie.value,
            "domain": cookie.domain if cookie.domain.startswith(".") else f".{cookie.domain}",
            "path": cookie.path or "/",
            "expires": int(cookie.expires) if cookie.expires else -1,
            "httpOnly": bool(rest.get("HttpOnly")),
            "secure": bool(cookie.secure),
            "sameSite": same_site,
        })

    return cookies


def verify_session(session_file):
    """Use Playwright Chromium to verify the saved session hits the DK API."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  (Skipping verification — playwright not installed)")
        return

    print("  Verifying session against DK API...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state=session_file)
        page = context.new_page()
        resp = page.goto(DK_API_URL, wait_until="load", timeout=30000)

        if resp is None or resp.status != 200:
            print(f"  WARNING: HTTP {resp.status if resp else 'none'} — session may not be authenticated.")
            print("  Make sure you're logged in to DraftKings in the Chrome profile and retry.")
        else:
            raw = page.evaluate("document.body.innerText")
            data = json.loads(raw)
            players = data.get("playerPool", {}).get("draftablePlayers", [])
            print(f"  Verified — {len(players)} players returned from DK API")

        context.close()
        browser.close()


def main():
    test = "--test" in sys.argv

    print(f"Extracting DraftKings cookies from Chrome profile '{CHROME_PROFILE}'...")
    cookies = extract_dk_cookies()

    dk_cookies = [c for c in cookies if "draftkings" in c["domain"]]
    print(f"  {len(dk_cookies)} DraftKings cookies found")

    if not dk_cookies:
        print("\nERROR: No DraftKings cookies found.")
        print(f"  Make sure you're logged in to DraftKings in Chrome's '{CHROME_PROFILE}' profile.")
        sys.exit(1)

    # Check for the session cookie
    session_cookie = next((c for c in dk_cookies if c["name"] == "identity_session"), None)
    if session_cookie:
        print(f"  identity_session cookie found (expires: {session_cookie['expires']})")
    else:
        print("  WARNING: identity_session cookie not found — you may not be logged in to DK")

    # Save as Playwright storage state
    os.makedirs(os.path.dirname(SESSION_FILE), exist_ok=True)
    storage_state = {"cookies": dk_cookies, "origins": []}
    with open(SESSION_FILE, "w") as f:
        json.dump(storage_state, f, indent=2)

    print(f"\nSession saved to: {SESSION_FILE}")

    if test:
        verify_session(SESSION_FILE)

    print("\nDone. The daily fetch script (fetch_draftkings_adp.py) will use this session.")
    print("Re-run this script if the daily fetch returns auth errors (~2 weeks).")


if __name__ == "__main__":
    main()
