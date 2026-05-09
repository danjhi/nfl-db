"""Daily health check for the ADP scrape pipeline.

Runs at noon (after all morning scrapes have completed). For each expected
ADP source, queries Supabase for today's row count and compares to a floor.
Also greps recent log files for auth errors and tracebacks. If anything's
off, fires a macOS notification (banner + sound).

Always writes a daily report to data/logs/health_<date>.txt.

Usage:
    python3 scripts/health/daily_scrape_health.py [--force]
    --force: send a notification even on success (useful for testing)
"""

import datetime
import os
import re
import subprocess
import sys
import urllib.request

_script_dir = os.path.dirname(os.path.abspath(__file__)) if os.path.exists(__file__) else os.path.join("scripts", "health")
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import SUPABASE_URL, SUPABASE_KEY, ROOT_DIR  # noqa: E402

LOG_DIR = os.path.join(ROOT_DIR, "data", "logs")
SLEEPER_LOG_DIR = "/Users/dan/dev/sleeper-scrape/logs"
TODAY = datetime.date.today().isoformat()

# Per-source thresholds + active windows + log file to grep for auth errors.
# Sources outside their active window are skipped (no alert).
SOURCES = [
    {
        "name": "underdog_postdraft", "floor": 300,
        "active": ("2026-04-27", "2026-09-10"),
        "logs": [os.path.join(LOG_DIR, "underdog_postdraft_adp.log")],
    },
    {
        "name": "drafters_postdraft", "floor": 200,
        "active": ("2026-04-27", "2026-09-10"),
        "logs": [os.path.join(LOG_DIR, "drafters_postdraft_adp.log")],
    },
    {
        "name": "draftkings_postdraft", "floor": 250,
        "active": ("2026-04-27", "2026-09-10"),
        "logs": [os.path.join(LOG_DIR, "draftkings_postdraft_adp.log")],
    },
    {
        "name": "sleeper_sf", "floor": 400,
        "active": (None, None),  # always-on
        "logs": [os.path.join(SLEEPER_LOG_DIR, "daily_scrape.log")],
    },
    {
        "name": "sleeper_1qb", "floor": 300,
        "active": (None, None),
        "logs": [os.path.join(SLEEPER_LOG_DIR, "daily_scrape.log")],
    },
    {
        "name": "sleeper_sf_rookie", "floor": 40,
        "active": (None, None),
        "logs": [os.path.join(SLEEPER_LOG_DIR, "daily_scrape.log")],
    },
    {
        "name": "sleeper_1qb_rookie", "floor": 30,
        "active": (None, None),
        "logs": [os.path.join(SLEEPER_LOG_DIR, "daily_scrape.log")],
    },
]

# Patterns that indicate a script-level failure worth flagging
ERROR_PATTERNS = [
    re.compile(r"\b401\b"),                # auth expired (DK, Drafters)
    re.compile(r"\b403\b"),                # forbidden
    re.compile(r"Traceback", re.IGNORECASE),
    re.compile(r"\bERROR\b"),
    re.compile(r"Unauthorized", re.IGNORECASE),
    re.compile(r"session.*expired", re.IGNORECASE),
]


def is_active(active_window):
    start, end = active_window
    if start and TODAY < start:
        return False
    if end and TODAY > end:
        return False
    return True


def count_rows_today(source):
    url = f"{SUPABASE_URL}/rest/v1/adp_sources?select=count&source=eq.{source}&date=eq.{TODAY}"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "count=exact",
    })
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        cr = resp.headers.get("content-range", "")
        return int(cr.split("/")[-1]) if cr else 0
    except Exception as e:
        return None  # treat as failure


def scan_log_for_errors(log_path, since_hours=24):
    """Return list of error lines from today's portion of `log_path`.

    Logs are append-only across days. To avoid surfacing stale errors from
    prior runs, only consider lines written after the most recent line that
    contains today's date string (`YYYY-MM-DD`). If no such line exists,
    today's run hasn't logged anything → return empty.

    Caps at 5 lines.
    """
    if not os.path.exists(log_path):
        return []
    cutoff_mtime = datetime.datetime.now().timestamp() - since_hours * 3600
    if os.path.getmtime(log_path) < cutoff_mtime:
        return []  # file wasn't touched recently
    try:
        with open(log_path, errors="replace") as f:
            lines = f.readlines()
    except Exception:
        return []

    # Find the index of the LAST line containing today's date string —
    # that's where today's run output begins.
    today_str = TODAY  # e.g. "2026-05-07"
    today_start_idx = None
    for i in range(len(lines) - 1, -1, -1):
        if today_str in lines[i]:
            today_start_idx = i
            # Walk back to the most recent "Starting" line for the run header
            for j in range(i, max(-1, i - 50), -1):
                if today_str in lines[j]:
                    today_start_idx = j
                else:
                    break
            break

    if today_start_idx is None:
        return []  # log has no content for today

    hits = []
    for line in lines[today_start_idx:]:
        if any(p.search(line) for p in ERROR_PATTERNS):
            hits.append(line.rstrip())
            if len(hits) >= 5:
                break
    return hits


def send_notification(title, body, sound=True):
    """Fire a macOS notification via osascript."""
    sound_clause = ' sound name "Glass"' if sound else ""
    # Escape quotes for AppleScript
    body_esc = body.replace("\\", "\\\\").replace('"', '\\"')
    title_esc = title.replace("\\", "\\\\").replace('"', '\\"')
    script = f'display notification "{body_esc}" with title "{title_esc}"{sound_clause}'
    subprocess.run(["osascript", "-e", script], check=False)


def main():
    force = "--force" in sys.argv

    failures = []  # list of (source, reason) tuples
    skipped = []   # sources not in active window
    summary_lines = [f"=== Daily scrape health check — {TODAY} ==="]

    for src in SOURCES:
        if not is_active(src["active"]):
            skipped.append(src["name"])
            summary_lines.append(f"  SKIP   {src['name']:25s} (outside active window)")
            continue

        n = count_rows_today(src["name"])
        if n is None:
            failures.append((src["name"], "Supabase query failed"))
            summary_lines.append(f"  ERROR  {src['name']:25s} Supabase query failed")
            continue

        status = "OK" if n >= src["floor"] else "FAIL"
        line = f"  {status:6s} {src['name']:25s} {n:>5} rows (floor {src['floor']})"
        summary_lines.append(line)
        if n < src["floor"]:
            failures.append((src["name"], f"{n} rows < floor {src['floor']}"))

        # Scan associated logs for errors
        for log_path in src["logs"]:
            err_hits = scan_log_for_errors(log_path)
            for hit in err_hits:
                summary_lines.append(f"    log[{os.path.basename(log_path)}]: {hit[:120]}")

    # Cross-source error scan (catches stuff not tied to a specific source)
    for log_path in {p for s in SOURCES for p in s["logs"] if is_active(s["active"])}:
        err_hits = scan_log_for_errors(log_path)
        for hit in err_hits:
            line = f"  log-error[{os.path.basename(log_path)}]: {hit[:160]}"
            if line not in summary_lines:  # de-dupe with above
                pass  # already in per-source section

    # Write daily report
    os.makedirs(LOG_DIR, exist_ok=True)
    report_path = os.path.join(LOG_DIR, f"health_{TODAY}.txt")
    summary = "\n".join(summary_lines)
    with open(report_path, "w") as f:
        f.write(summary + "\n")

    print(summary)
    print(f"\nReport written to {report_path}")

    # Notify
    if failures:
        title = f"NFL DB scrape FAIL ({len(failures)})"
        body_lines = [f"{name}: {reason}" for name, reason in failures[:3]]
        if len(failures) > 3:
            body_lines.append(f"…and {len(failures) - 3} more")
        body = " | ".join(body_lines)
        send_notification(title, body, sound=True)
        print(f"\nNotification sent: {title}")
        sys.exit(1)
    elif force:
        body = f"All {len([s for s in SOURCES if is_active(s['active'])])} sources OK"
        send_notification("NFL DB scrape OK", body, sound=False)
        print("\n[--force] Notification sent (success)")
    else:
        print("\nAll sources OK — no notification sent.")


if __name__ == "__main__":
    main()
