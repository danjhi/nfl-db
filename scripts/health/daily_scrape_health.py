"""Daily health check for the ADP scrape pipeline.

Runs via launchd at 11:25 (laptop) / 12:00 (desktop); both machines send
the same email, so the machine name is in the subject and report header.
For each expected ADP source, checks that the most recent snapshot in
Supabase is FRESH (retrieved within MAX_AGE_HOURS) and has at least
`floor` rows. Freshness-based rather than "rows dated today" so a scrape
that pushes after this check runs doesn't raise a false alarm — a
genuinely missed day shows up as a ~46h-old snapshot at the next check.

Also greps recent log files for auth errors and tracebacks, and classifies
two situations distinctly instead of leaving them as generic FAILs:
  - AUTH: the source is stale AND its log shows an auth-expiry signature
    (DK 401 "Session has expired", Underdog Cloudflare 403, Drafters JWT).
    These need Dan (a logged-in-Chrome setup run); the report says exactly
    what to run.
  - just-woke: the machine came out of sleep right before this check
    (launchd fires missed jobs on wake), so staleness may be wake lag,
    not breakage — the 2026-07-22 vacation FAIL(11) email was this.

If anything's off, fires a macOS notification (banner + sound).
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
import urllib.error
import urllib.request

_script_dir = os.path.dirname(os.path.abspath(__file__)) if os.path.exists(__file__) else os.path.join("scripts", "health")
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import SUPABASE_URL, SUPABASE_KEY, ROOT_DIR  # noqa: E402

LOG_DIR = os.path.join(ROOT_DIR, "data", "logs")
SLEEPER_LOG_DIR = os.path.join(os.path.expanduser("~"), "dev", "sleeper-scrape", "logs")
TODAY = datetime.date.today().isoformat()

# Both machines run this check and send identically-shaped emails; label
# them (the 07-22 vacation FAIL(11) email was untraceable to a machine).
MACHINE = {"/Users/danielhindery": "laptop", "/Users/dan": "desktop"}.get(
    os.path.expanduser("~"), os.uname().nodename.split(".")[0]
)

# A snapshot older than this is considered stale. Daily pushes land between
# ~09:00 (desktop) and ~15:50 (laptop trio worst case: 13:20 start + stage
# caps), so day-to-day jitter never exceeds ~26h; a missed day reads ~46h.
MAX_AGE_HOURS = 28

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
        # footballguys.com/adp own scraper (laptop-primary, draft season).
        "name": "rtsports", "floor": 200,
        "active": ("2026-07-10", "2026-09-10"),
        "logs": [os.path.join(LOG_DIR, "rtsports_adp.log")],
    },
    {
        # NFFC OC (FBG Online Championship). Same log as `nffc` (one script).
        "name": "nffc_oc", "floor": 300,
        "active": ("2026-07-10", "2026-09-10"),
        "logs": [os.path.join(LOG_DIR, "nffc_adp.log")],
    },
    {
        "name": "nffc", "floor": 400,
        "active": ("2026-07-10", "2026-09-10"),
        "logs": [os.path.join(LOG_DIR, "nffc_adp.log")],
    },
    {
        # NFFC BestBall10s (game_type_id=941). Same log as nffc (one script).
        "name": "bestball10s", "floor": 180,
        "active": ("2026-07-10", "2026-09-10"),
        "logs": [os.path.join(LOG_DIR, "nffc_adp.log")],
    },
    {
        # ESPN redraft (kona_player_info); floor filtered to the drafted board.
        "name": "espn", "floor": 150,
        "active": ("2026-07-10", "2026-09-10"),
        "logs": [os.path.join(LOG_DIR, "espn_adp.log")],
    },
    {
        # CBS redraft (static HTML: offense PPR + K + DST pages).
        "name": "cbs", "floor": 150,
        "active": ("2026-07-10", "2026-09-10"),
        "logs": [os.path.join(LOG_DIR, "cbs_adp.log")],
    },
    {
        # Yahoo redraft (public read-only fantasy API, no auth).
        "name": "yahoo", "floor": 150,
        "active": ("2026-07-10", "2026-09-10"),
        "logs": [os.path.join(LOG_DIR, "yahoo_adp.log")],
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

# A stale source whose log matches one of these is an expired login, not a
# broken scrape — a distinct failure class because only Dan can fix it
# (the setup scripts read his logged-in Chrome session).
AUTH_RE = re.compile(r"session.*expired|re-run setup|cloudflare 403|\b401\b", re.IGNORECASE)
AUTH_ACTIONS = {
    "draftkings": "python3 scripts/adp/setup_dk_session.py (Chrome closed, logged in to DK)",
    "underdog": "python3 scripts/adp/setup_underdog_session.py (Chrome logged in to Underdog)",
    "drafters": "refresh DRAFTERS_JWT in .env",
}


def minutes_since_wake():
    """Minutes since the system last woke from sleep, or None if unknown.
    `sysctl kern.waketime` is the epoch time of the last wake on macOS."""
    try:
        out = subprocess.run(
            ["sysctl", "-n", "kern.waketime"],
            capture_output=True, text=True, timeout=5,
        ).stdout
        m = re.search(r"sec\s*=\s*(\d+)", out)
        if not m:
            return None
        return (datetime.datetime.now().timestamp() - int(m.group(1))) / 60
    except Exception:
        return None


def is_active(active_window):
    start, end = active_window
    if start and TODAY < start:
        return False
    if end and TODAY > end:
        return False
    return True


class NetworkDown(Exception):
    """Supabase is unreachable at the network level (no DNS, no route,
    connection refused). Distinct from a failed query: on 2026-07-20/21 the
    laptop was offline at check time and every source got reported as
    "Supabase query failed" when the real story was "no internet — and the
    scrapes before this check almost certainly didn't run either"."""


def _get(url, extra_headers=None):
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, headers=headers)
    return urllib.request.urlopen(req, timeout=15)


def _latest_snapshot_date(source):
    """(date, year) of the most recent snapshot for `source`, or (None, year)
    if the source has no rows. Equality on (source, year) + ORDER BY date
    keeps the probe on idx_adp_sources_source_year_date no matter how big
    adp_sources gets — the old ORDER BY retrieved_at had no index behind it
    and started brushing the statement timeout at ~300k rows (2026-07-23).
    Rows are keyed on season year; around the winter turnover the newest
    snapshot may still carry last season's year, so fall back one year."""
    import json

    this_year = datetime.date.today().year
    for year in (this_year, this_year - 1):
        url = (f"{SUPABASE_URL}/rest/v1/adp_sources"
               f"?select=date&source=eq.{source}&year=eq.{year}"
               f"&order=date.desc&limit=1")
        rows = json.loads(_get(url).read().decode("utf-8"))
        if rows:
            return (rows[0]["date"], year)
    return (None, this_year)


def latest_snapshot(source):
    """Return (snapshot_date, age_hours, row_count) for the source's most
    recent snapshot in adp_sources, or None on query failure. A source with
    no rows at all returns (None, None, 0). Raises NetworkDown when Supabase
    can't be reached at all.
    """
    import json

    try:
        snap_date, year = _latest_snapshot_date(source)
        if snap_date is None:
            return (None, None, 0)

        url = (f"{SUPABASE_URL}/rest/v1/adp_sources"
               f"?select=retrieved_at&source=eq.{source}&year=eq.{year}"
               f"&date=eq.{snap_date}&order=retrieved_at.desc&limit=1")
        rows = json.loads(_get(url).read().decode("utf-8"))
        retrieved = datetime.datetime.fromisoformat(rows[0]["retrieved_at"])
        now = datetime.datetime.now(datetime.timezone.utc)
        age_hours = (now - retrieved).total_seconds() / 3600

        url = (f"{SUPABASE_URL}/rest/v1/adp_sources"
               f"?select=count&source=eq.{source}&year=eq.{year}&date=eq.{snap_date}")
        resp = _get(url, {"Prefer": "count=exact"})
        cr = resp.headers.get("content-range", "")
        count = int(cr.split("/")[-1]) if cr else 0
        return (snap_date, age_hours, count)
    except urllib.error.HTTPError:
        return None  # server responded with an error — a query failure
    except urllib.error.URLError as e:
        raise NetworkDown(getattr(e, "reason", e)) from e
    except Exception:
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


def send_email(subject, body):
    """Send email via Gmail SMTP using app password. Silent no-op if env not configured."""
    import smtplib
    from email.message import EmailMessage

    user = os.environ.get("GMAIL_USER", "danhindery@gmail.com")
    pw = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not pw:
        print("  (skipping email — GMAIL_APP_PASSWORD not set in .env)")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = user
    msg.set_content(body)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as s:
            s.login(user, pw)
            s.send_message(msg)
        print(f"  Email sent: {subject}")
    except Exception as e:
        print(f"  Email send failed: {type(e).__name__}: {e}")


def main():
    force = "--force" in sys.argv

    failures = []  # list of (source, reason) tuples
    auth_failures = []  # subset of failures that are expired-login (Dan-only fix)
    skipped = []   # sources not in active window
    offline = None  # NetworkDown message when Supabase is unreachable
    summary_lines = [f"=== Daily scrape health check [{MACHINE}] — {TODAY} ==="]

    wake_min = minutes_since_wake()
    just_woke = wake_min is not None and wake_min < 30
    if just_woke:
        summary_lines.append(
            f"  NOTE: {MACHINE} woke from sleep {wake_min:.0f} min before this check."
        )
        summary_lines.append(
            "  launchd fires missed jobs on wake, so today's scrapes may still be catching"
        )
        summary_lines.append(
            "  up — staleness below can be wake lag, not breakage. Re-check once settled."
        )

    for src in SOURCES:
        if not is_active(src["active"]):
            skipped.append(src["name"])
            summary_lines.append(f"  SKIP   {src['name']:25s} (outside active window)")
            continue

        try:
            snap = latest_snapshot(src["name"])
        except NetworkDown as e:
            offline = str(e)
            summary_lines.append(f"  OFFLINE — Supabase unreachable ({offline}). Aborting source checks.")
            summary_lines.append("  The scheduled scrapes before this check almost certainly didn't run")
            summary_lines.append("  either — verify today's row counts once back online.")
            break
        if snap is None:
            failures.append((src["name"], "Supabase query failed"))
            summary_lines.append(f"  ERROR  {src['name']:25s} Supabase query failed")
            continue

        snap_date, age_hours, n = snap
        reasons = []
        if snap_date is None:
            reasons.append("no rows in adp_sources")
        else:
            if age_hours > MAX_AGE_HOURS:
                reasons.append(f"stale: last push {age_hours:.0f}h ago (max {MAX_AGE_HOURS}h)")
            if n < src["floor"]:
                reasons.append(f"{n} rows < floor {src['floor']}")

        # Scan associated logs for errors (today's portion only)
        src_hits = []
        for log_path in src["logs"]:
            for hit in scan_log_for_errors(log_path):
                src_hits.append((os.path.basename(log_path), hit))

        # A stale source with an auth signature in today's log is an expired
        # login — label it AUTH and say exactly what Dan needs to run.
        auth = bool(reasons) and any(AUTH_RE.search(hit) for _, hit in src_hits)
        status = "AUTH" if auth else ("FAIL" if reasons else "OK")
        detail = (f"{n:>5} rows @ {snap_date} ({age_hours:.1f}h ago, floor {src['floor']})"
                  if snap_date else "no rows")
        summary_lines.append(f"  {status:6s} {src['name']:25s} {detail}")
        if auth:
            action = AUTH_ACTIONS.get(
                src["name"].split("_")[0], "re-run this source's session setup"
            )
            summary_lines.append(f"    AUTH EXPIRED — needs Dan: {action}")
        for base, hit in src_hits:
            summary_lines.append(f"    log[{base}]: {hit[:120]}")
        if reasons:
            failures.append((src["name"], ("AUTH expired; " if auth else "") + "; ".join(reasons)))
            if auth:
                auth_failures.append(src["name"])

    # Write daily report
    os.makedirs(LOG_DIR, exist_ok=True)
    report_path = os.path.join(LOG_DIR, f"health_{TODAY}.txt")
    summary = "\n".join(summary_lines)
    with open(report_path, "w") as f:
        f.write(summary + "\n")

    print(summary)
    print(f"\nReport written to {report_path}")

    # Notify
    if offline is not None:
        title = f"[NFL DB {MACHINE}] Health check OFFLINE — Supabase unreachable"
        # osascript is local, so the banner still fires with no internet;
        # the email send will fail quietly and that's fine.
        send_notification(
            f"NFL DB {MACHINE} health check OFFLINE",
            "No internet at check time — scrapes probably didn't run. Verify row counts once back online.",
            sound=True,
        )
        send_email(title, summary + f"\n\nReport: {report_path}\n")
        print(f"\nNotification sent: {title}")
        sys.exit(1)
    elif failures:
        failed_names = ", ".join(name for name, _ in failures)
        woke_tag = ", just-woke" if just_woke else ""
        title = f"[NFL DB {MACHINE}] Scrape FAIL ({len(failures)}{woke_tag}): {failed_names}"
        if auth_failures:
            title += f" | AUTH needs Dan: {', '.join(auth_failures)}"
        body_lines = [f"{name}: {reason}" for name, reason in failures[:3]]
        if len(failures) > 3:
            body_lines.append(f"…and {len(failures) - 3} more")
        short_body = " | ".join(body_lines)
        send_notification(f"NFL DB {MACHINE} scrape FAIL ({len(failures)})", short_body, sound=True)
        send_email(title, summary + f"\n\nReport: {report_path}\n")
        print(f"\nNotification sent: {title}")
        sys.exit(1)
    elif force:
        active_count = len([s for s in SOURCES if is_active(s['active'])])
        send_notification(f"NFL DB {MACHINE} scrape OK", f"All {active_count} sources OK", sound=False)
        send_email(f"[NFL DB {MACHINE}] Scrape OK ({active_count} sources)", summary + "\n")
        print("\n[--force] Notification sent (success)")
    else:
        print("\nAll sources OK — no notification sent.")


if __name__ == "__main__":
    main()
