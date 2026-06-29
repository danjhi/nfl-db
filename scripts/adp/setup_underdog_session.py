"""One-time setup: extract Underdog cookies from Chrome and save session.

Same pattern as setup_dk_session.py. Reads cookies from Chrome's Profile 2
(or whatever UD_CHROME_PROFILE points at) for both `underdogfantasy.com`
and `underdogsports.com` (Underdog migrated domains May 2026; both still
have cookies in active sessions).

Re-run when the daily fetch starts returning Cloudflare 403 errors
(typically every ~2 weeks as auth/Cloudflare cookies rotate).

Usage:
    python3 scripts/adp/setup_underdog_session.py [--test]
"""

import json
import os
import sys

_script_dir = os.path.dirname(os.path.abspath(__file__)) if os.path.exists(__file__) else os.path.join("scripts", "adp")
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import SUPABASE_URL  # noqa — triggers .env loading

CHROME_PROFILE = os.environ.get("UD_CHROME_PROFILE", "Profile 2").strip("\"'")
CHROME_PROFILE_DIR = os.path.join(os.path.expanduser("~"), "Library/Application Support/Google/Chrome", CHROME_PROFILE)
COOKIE_FILE = os.path.join(CHROME_PROFILE_DIR, "Cookies")

SESSION_FILE = os.path.normpath(os.path.join(_script_dir, "..", "..", "data", "underdog_session.json"))

UD_DOMAINS = ["underdogfantasy.com", "underdogsports.com"]

# Test URL — current post-draft contest CSV download
TEST_URL = (
    "https://app.underdogsports.com/rankings/download/"
    "a9c04e81-1ace-4b16-a31d-4c725a47f16f/"
    "ccf300b0-9197-5951-bd96-cba84ad71e86/"
    "9e62863e-1b29-53e8-8aca-2aae06aaac5f"
    "?product=fantasy"
    "&product_experience_id=018e1234-5678-9abc-def0-123456789002"
    "&state_config_id=7b937c4c-58ae-467c-90e7-c8dc2202a02a"
)


def extract_underdog_cookies():
    """Read Underdog cookies from Chrome's profile cookie database."""
    try:
        import browser_cookie3
    except ImportError:
        print("ERROR: browser-cookie3 not installed.")
        print("  Run: pip install browser-cookie3")
        sys.exit(1)

    if not os.path.exists(COOKIE_FILE):
        print(f"ERROR: Cookie file not found: {COOKIE_FILE}")
        print(f"  Make sure Chrome profile '{CHROME_PROFILE}' exists and you've logged in to Underdog there.")
        sys.exit(1)

    print(f"  Reading cookies from Chrome '{CHROME_PROFILE}' profile...")
    print("  (You may see a Keychain prompt — click Allow)")

    all_cookies = []
    for domain in UD_DOMAINS:
        try:
            cj = browser_cookie3.chrome(cookie_file=COOKIE_FILE, domain_name=domain)
        except Exception as e:
            print(f"  WARN: failed to read cookies for {domain}: {e}")
            continue

        for cookie in cj:
            same_site = "None"
            rest = getattr(cookie, "_rest", {}) or {}
            if rest.get("SameSite"):
                ss = rest["SameSite"].capitalize()
                if ss in ("Strict", "Lax", "None"):
                    same_site = ss

            all_cookies.append({
                "name": cookie.name,
                "value": cookie.value,
                "domain": cookie.domain if cookie.domain.startswith(".") else f".{cookie.domain}",
                "path": cookie.path or "/",
                "expires": int(cookie.expires) if cookie.expires else -1,
                "httpOnly": bool(rest.get("HttpOnly")),
                "secure": bool(cookie.secure),
                "sameSite": same_site,
            })

    return all_cookies


def verify_session(session_file):
    """Use Playwright Chromium to verify the saved session can pass Cloudflare."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  (Skipping verification — playwright not installed)")
        return

    print("  Verifying session against Underdog CSV download...")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            storage_state=session_file,
            accept_downloads=True,
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        )
        page = ctx.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            with page.expect_download(timeout=30000) as dl_info:
                try:
                    page.goto(TEST_URL, timeout=30000)
                except Exception:
                    pass  # 'Download is starting' is expected
            dl = dl_info.value
            path = dl.path()
            with open(path) as f:
                lines = f.read().splitlines()
            print(f"  Verified — {len(lines) - 1} player rows returned from Underdog CSV")
        except Exception as e:
            print(f"  WARNING: verification failed: {type(e).__name__}: {str(e)[:200]}")
            print("  Make sure you're logged in to Underdog in the Chrome profile and retry.")

        ctx.close()
        browser.close()


def main():
    test = "--test" in sys.argv

    print(f"Extracting Underdog cookies from Chrome profile '{CHROME_PROFILE}'...")
    cookies = extract_underdog_cookies()
    print(f"  {len(cookies)} cookies found across {UD_DOMAINS}")

    if not cookies:
        print("\nERROR: No Underdog cookies found.")
        print(f"  Make sure you're logged in to Underdog in Chrome's '{CHROME_PROFILE}' profile.")
        sys.exit(1)

    cf_names = [c["name"] for c in cookies if c["name"] in ("cf_clearance", "__cf_bm")]
    print(f"  Cloudflare cookies present: {cf_names}")

    os.makedirs(os.path.dirname(SESSION_FILE), exist_ok=True)
    storage_state = {"cookies": cookies, "origins": []}
    with open(SESSION_FILE, "w") as f:
        json.dump(storage_state, f, indent=2)

    print(f"\nSession saved to: {SESSION_FILE}")

    if test:
        verify_session(SESSION_FILE)

    print("\nDone. The daily fetch (fetch_underdog_postdraft_adp.py) will use this session.")
    print("Re-run this script if the daily fetch returns Cloudflare 403 errors (~2 weeks).")


if __name__ == "__main__":
    main()
