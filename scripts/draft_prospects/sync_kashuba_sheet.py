"""Pull 2027 prospect values from Mike Kashuba's shared Google Sheet into draft_prospects.

The pull model agreed 2026-08-25: the shared sheet is pure data (no Apps Script, no
keys), and this job reads it with the ff-stat-sim service account, validates every
row, and upserts ONLY value / sf_value / values_updated_at on prospects that already
exist in Supabase. DTVC Plus (production, api/services/core_queries.py) reads
draft_prospects straight from Supabase with the anon key and caches core data for
four hours, so a write here reaches the live site within that window with no deploy.

Guardrails (all live in this file, none in the sheet):
  - match by normalized name (+ position must agree); unmatched sheet names are the
    proposal queue: reported, never auto-created
  - prospects in the table but missing from the sheet are reported, never deleted
  - blank Value / SF_Value cells are skipped, not written as NULL (NULL hides a
    prospect from the site; pass --allow-cuts to let a blank pair cut a prospect)
  - non-numeric or out-of-range values (0..MAX_VALUE) are skipped and reported
  - a single change larger than MAX_STEP points, or more than MAX_CHANGES rows changing
    in one run, is held back until --force (guards a pasted-over or shifted sheet)
  - values_updated_at is stamped only on rows whose numbers actually changed
  - every apply writes a pre-state backup to data/draft_prospects/ first

Mike's `note` column is his dated report and IS site copy (Dan, 2026-09-15): it syncs
to analyst_note / analyst_note_date / analyst_name and leads the DTVC Plus prospect
card, credited to Mike Kashuba. The date shown is the sheet's `date` cell when it
parses (2Sep26, 9/14, 9/14/26, Sep 14, 2026-09-14); a blank date keeps the stored
date while the note text is unchanged, and stamps today (ET) when the text changes.
A cleared note clears the card. Note changes are not subject to the value guards
(only a length cap), and a held or bad value does not block its row's note.
--seed-note-dates FILE seeds first-sync dates from a JSON {name: {"note", "first_seen"}}
map (built from the sheet's revision history) for notes whose text still matches.

Nothing is written without --apply. The daily launchd job passes --apply; run it by
hand without the flag to preview.

Usage:
  .venv/bin/python scripts/draft_prospects/sync_kashuba_sheet.py            # dry run
  .venv/bin/python scripts/draft_prospects/sync_kashuba_sheet.py --apply    # write
  .venv/bin/python scripts/draft_prospects/sync_kashuba_sheet.py --apply --force
  .venv/bin/python scripts/draft_prospects/sync_kashuba_sheet.py --no-writeback
  .venv/bin/python scripts/draft_prospects/sync_kashuba_sheet.py --restore data/draft_prospects/backup_<ts>.json
      (rollback: re-PATCH value / sf_value / values_updated_at from a backup; no sheet read, no writeback)

Auth: the sheet must be shared with the service account's client_email (Viewer to
read; Editor for the "Sync Status" tab writeback). Key file: --creds, else
GOOGLE_APPLICATION_CREDENTIALS, else ~/dev/ff-stat-sim/.gcp-sheets-sa.json.
Supabase: SUPABASE_SERVICE_ROLE_KEY from nfl-db/.env (RLS allows anon SELECT only).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import signal
import sys
import urllib.error
import urllib.request
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR / "scripts" / "ids"))
from shared import SUPABASE_URL, SUPABASE_SERVICE_KEY, normalize_name  # noqa: E402

SHEET_ID = "1O-I4dnyizRn5p5JqHUpWv-ZtB9w9F8yTel7gymcg7s4"  # 2027 Dynasty Prospects - Shared Board
DRAFT_YEAR = 2027
TABLE = "draft_prospects"
STATUS_TAB = "Sync Status"
DEFAULT_CREDS = Path.home() / "dev" / "ff-stat-sim" / ".gcp-sheets-sa.json"
BACKUP_DIR = ROOT_DIR / "data" / "draft_prospects"

MAX_VALUE = 60      # dynasty_values scale: tier-top veterans sit in the 50s; no prospect exceeds it
MAX_STEP = 15       # largest single-run move accepted without --force
MAX_CHANGES = 30    # most rows accepted in one run without --force (half the board)
WALL_CLOCK_CAP = 300

ANALYST_NAME = "Mike Kashuba"   # byline on the site card; the sheet is his
MAX_NOTE_CHARS = 1500           # a note longer than this is reported, not synced
NOTE_COLS = ("analyst_note", "analyst_note_date", "analyst_name")
MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}


# ── Sheet ──────────────────────────────────────────────────────────────────

def open_sheet(creds_path: Path):
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
    return gspread.authorize(creds).open_by_key(SHEET_ID)


def read_board(sh) -> list[dict]:
    """First tab, header row 1. Returns one dict per non-empty data row."""
    ws = sh.get_worksheet(0)
    values = ws.get_all_values()
    if not values:
        sys.exit("ERROR: sheet is empty")
    header = [h.strip().lower() for h in values[0]]
    need = {"name", "position", "value", "sf_value"}
    missing = need - set(header)
    if missing:
        sys.exit(f"ERROR: sheet is missing columns {sorted(missing)}; header = {values[0]}")
    idx = {h: i for i, h in enumerate(header)}
    rows = []
    for n, raw in enumerate(values[1:], start=2):
        get = lambda k: raw[idx[k]].strip() if idx.get(k) is not None and idx[k] < len(raw) else ""
        if not get("name"):
            continue
        rows.append({
            "line": n,
            "name": get("name"),
            "position": get("position").upper(),
            "college": get("college"),
            "value": get("value"),
            "sf_value": get("sf_value"),
            "note": get("note"),
            "date": get("date"),
        })
    return rows


# ── Supabase (PostgREST, service key) ──────────────────────────────────────

def _headers(extra: dict | None = None) -> dict:
    h = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }
    if extra:
        h.update(extra)
    return h


def fetch_table() -> tuple[list[dict], bool]:
    """(rows, notes_enabled). Falls back to the value columns only, with notes
    disabled, while the analyst_* migration has not been applied."""
    base = "dan_id,name,position,college,value,sf_value,values_updated_at,updated_at"
    for select, notes in ((f"{base},{','.join(NOTE_COLS)}", True), (base, False)):
        url = f"{SUPABASE_URL}/rest/v1/{TABLE}?draft_year=eq.{DRAFT_YEAR}&select={select}&order=dan_id&limit=1000"
        req = urllib.request.Request(url, headers=_headers())
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read()), notes
        except urllib.error.HTTPError as e:
            if not notes or e.code != 400:
                raise
            print("WARN: analyst_* columns missing (run migrate_analyst_note.py); syncing values only")
    raise AssertionError("unreachable")


def patch_row(dan_id: str, payload: dict) -> int:
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?dan_id=eq.{urllib.request.quote(dan_id)}&draft_year=eq.{DRAFT_YEAR}"
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(),
        headers=_headers({"Prefer": "return=representation"}), method="PATCH",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return len(json.loads(resp.read()))


# ── Validation ─────────────────────────────────────────────────────────────

def parse_value(raw: str) -> tuple[float | None, str | None]:
    """(number, problem). Blank -> (None, None). Bad -> (None, reason)."""
    if raw == "":
        return None, None
    try:
        v = float(raw.replace(",", ""))
    except ValueError:
        return None, f"non-numeric {raw!r}"
    if not (0 <= v <= MAX_VALUE):
        return None, f"{v:g} outside 0..{MAX_VALUE}"
    return v, None


def parse_sheet_date(raw: str, today: dt.date) -> tuple[dt.date | None, str | None]:
    """Mike's `date` cell -> (date, problem). Blank -> (None, None).

    Accepts 2Sep26 / 14 Sep 2026, Sep 14 / September 14, 2026, 9/14 / 9/14/26,
    and 2026-09-14, with an optional leading "Updated". A missing year is the
    current one (last year if that would land more than a week ahead)."""
    s = raw.strip().lower().replace(",", " ").replace(".", " ")
    s = re.sub(r"^(updated|as of)\s+", "", s).strip()
    if not s:
        return None, None
    y = mo = d = None
    if m := re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s):
        y, mo, d = int(m[1]), int(m[2]), int(m[3])
    elif m := re.fullmatch(r"(\d{1,2})/(\d{1,2})(?:/(\d{2}|\d{4}))?", s):
        mo, d, y = int(m[1]), int(m[2]), m[3]
    elif m := re.fullmatch(r"(\d{1,2})\s*([a-z]{3,9})\s*(\d{2}|\d{4})?", s):
        d, mo, y = int(m[1]), MONTHS.get(m[2][:3]), m[3]
    elif m := re.fullmatch(r"([a-z]{3,9})\s*(\d{1,2})(?:\s+(\d{2}|\d{4}))?", s):
        mo, d, y = MONTHS.get(m[1][:3]), int(m[2]), m[3]
    if mo is None or d is None:
        return None, f"date {raw.strip()!r} not recognized"
    explicit_year = y is not None
    y = (int(y) + 2000 if len(str(y)) == 2 else int(y)) if explicit_year else today.year
    try:
        out = dt.date(y, mo, d)
    except ValueError:
        return None, f"date {raw.strip()!r} is not a real date"
    if not explicit_year and out > today + dt.timedelta(days=7):
        out = out.replace(year=y - 1)
    if out > today + dt.timedelta(days=1):
        return None, f"date {raw.strip()!r} is in the future"
    return out, None


def note_change(t: dict, r: dict, today: dt.date, seed: dict) -> tuple[dict | None, str | None]:
    """(analyst_* patch or None, problem or None) for one matched row."""
    note = r["note"].strip()
    cur_note = (t.get("analyst_note") or "").strip()
    cur_date = t.get("analyst_note_date")
    cur_name = t.get("analyst_name")
    if len(note) > MAX_NOTE_CHARS:
        return None, f"{r['name']}: note is {len(note)} characters (> {MAX_NOTE_CHARS}), not synced"
    sheet_date, date_problem = parse_sheet_date(r["date"], today)
    if date_problem:
        date_problem = f"{r['name']}: {date_problem}, used the fallback date"
    if not note:
        if cur_note or cur_date or cur_name:
            return {"analyst_note": None, "analyst_note_date": None, "analyst_name": None, "kind": "cleared"}, None
        return None, (f"{r['name']}: date {r['date']!r} has no note, ignored" if r["date"].strip() else None)
    if sheet_date:
        new_date = sheet_date.isoformat()
    elif note == cur_note and cur_date:
        new_date = cur_date
    else:
        s = seed.get(r["name"])
        s_date = s if isinstance(s, str) else (s or {}).get("first_seen")
        s_note = None if isinstance(s, str) else (s or {}).get("note")
        new_date = s_date if s_date and (s_note is None or s_note.strip() == note) else today.isoformat()
    if note == cur_note and new_date == cur_date and cur_name == ANALYST_NAME:
        return None, date_problem
    kind = "new" if not cur_note else ("updated" if note != cur_note else "redated")
    return {"analyst_note": note, "analyst_note_date": new_date, "analyst_name": ANALYST_NAME, "kind": kind}, date_problem


def num(x) -> float | None:
    return None if x is None else float(x)


def fmt(x) -> str:
    return "-" if x is None else f"{x:g}"


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="Sync Kashuba's 2027 prospect sheet into draft_prospects")
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    ap.add_argument("--force", action="store_true",
                    help=f"accept moves larger than {MAX_STEP} points and runs changing more than {MAX_CHANGES} rows")
    ap.add_argument("--allow-cuts", action="store_true",
                    help="a row with BOTH values blank nulls the prospect's values (hides it from the site)")
    ap.add_argument("--no-writeback", action="store_true", help="skip the Sync Status tab")
    ap.add_argument("--creds", default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", str(DEFAULT_CREDS)))
    ap.add_argument("--restore", metavar="BACKUP_JSON", help="rollback: restore value/sf_value/values_updated_at from a backup file")
    ap.add_argument("--seed-note-dates", metavar="JSON",
                    help="first-sync note dates: {name: {note, first_seen}} (used only when the note text matches)")
    args = ap.parse_args()

    if args.restore:
        return restore(Path(args.restore))

    signal.alarm(WALL_CLOCK_CAP)
    started = dt.datetime.now().astimezone()
    print(f"{started:%Y-%m-%d %H:%M:%S %Z}: Kashuba prospect sync ({'APPLY' if args.apply else 'dry run'})")

    creds_path = Path(args.creds)
    if not creds_path.exists():
        sys.exit(f"ERROR: service-account key not found at {creds_path}")
    if not SUPABASE_SERVICE_KEY:
        sys.exit("ERROR: SUPABASE_SERVICE_ROLE_KEY missing from nfl-db/.env")

    sh = open_sheet(creds_path)
    sheet_rows = read_board(sh)
    table, notes_enabled = fetch_table()
    print(f"sheet: {len(sheet_rows)} rows | {TABLE} ({DRAFT_YEAR}): {len(table)} rows")
    today_et = dt.datetime.now(ZoneInfo("America/New_York")).date()
    seed = json.loads(Path(args.seed_note_dates).read_text()) if args.seed_note_dates else {}

    by_key: dict[str, list[dict]] = {}
    for t in table:
        by_key.setdefault(normalize_name(t["name"]), []).append(t)

    changes: list[dict] = []      # safe to write
    note_changes: list[dict] = [] # analyst_* patches (Mike's dated reports)
    held: list[str] = []          # guardrail hits (need --force or a human)
    problems: list[str] = []      # bad cells, position mismatches
    unmatched: list[str] = []     # proposal queue
    unchanged = 0
    seen_ids: set[str] = set()

    for r in sheet_rows:
        cands = by_key.get(normalize_name(r["name"]), [])
        if not cands:
            unmatched.append(f"{r['name']} ({r['position']}, {r['college'] or 'no college'}) "
                             f"V={r['value'] or '-'} SF={r['sf_value'] or '-'}")
            continue
        if len(cands) > 1:
            cands = [c for c in cands if c["position"] == r["position"]]
        if len(cands) != 1:
            problems.append(f"{r['name']}: ambiguous match ({len(cands)} table rows)")
            continue
        t = cands[0]
        seen_ids.add(t["dan_id"])
        if r["position"] and t["position"] != r["position"]:
            problems.append(f"{r['name']}: sheet says {r['position']}, table says {t['position']} (skipped)")
            continue

        if notes_enabled:
            nc, note_problem = note_change(t, r, today_et, seed)
            if nc:
                note_changes.append({"t": t, "r": r, **nc})
            if note_problem:
                problems.append(note_problem)

        v, v_err = parse_value(r["value"])
        sf, sf_err = parse_value(r["sf_value"])
        if v_err or sf_err:
            problems.append(f"{r['name']}: {v_err or ''} {sf_err or ''}".strip() + " (skipped)")
            continue
        if v is None and sf is None:
            if args.allow_cuts and (t["value"] is not None or t["sf_value"] is not None):
                changes.append({"t": t, "r": r, "value": None, "sf_value": None, "cut": True})
            else:
                problems.append(f"{r['name']}: both values blank on the sheet (kept {fmt(num(t['value']))}/{fmt(num(t['sf_value']))}; "
                                f"--allow-cuts would hide the prospect)")
            continue
        if v is None or sf is None:
            problems.append(f"{r['name']}: only one value filled (V={r['value'] or '-'} SF={r['sf_value'] or '-'}), skipped")
            continue

        cur_v, cur_sf = num(t["value"]), num(t["sf_value"])
        if cur_v == v and cur_sf == sf:
            unchanged += 1
            continue
        step = max(abs((cur_v if cur_v is not None else v) - v), abs((cur_sf if cur_sf is not None else sf) - sf))
        chg = {"t": t, "r": r, "value": v, "sf_value": sf, "cut": False, "step": step}
        if step > MAX_STEP and not args.force:
            held.append(f"{r['name']}: {fmt(cur_v)}/{fmt(cur_sf)} -> {fmt(v)}/{fmt(sf)} moves {step:g} > {MAX_STEP} (held; --force to accept)")
            continue
        changes.append(chg)

    missing_from_sheet = [t for t in table if t["dan_id"] not in seen_ids]

    if len(changes) > MAX_CHANGES and not args.force:
        held.append(f"{len(changes)} rows would change in one run (> {MAX_CHANGES}); all held until --force")
        changes = []

    # ── Report ──
    print(f"\nmatched: {len(seen_ids)} | unchanged: {unchanged} | to change: {len(changes)} | held: {len(held)} "
          f"| problems: {len(problems)} | unmatched sheet names: {len(unmatched)} | in table not on sheet: {len(missing_from_sheet)}"
          + (f" | note changes: {len(note_changes)}" if notes_enabled else " | notes: disabled"))
    if changes:
        print("\nchanges (1QB/SF):")
        for c in changes:
            t, r = c["t"], c["r"]
            tag = " CUT" if c["cut"] else ""
            note = f"  [{r['note']}]" if r["note"] else ""
            print(f"  {t['name']:<24} {t['position']:<3} {fmt(num(t['value']))}/{fmt(num(t['sf_value']))} -> "
                  f"{fmt(c['value'])}/{fmt(c['sf_value'])}{tag}{note}")
    if note_changes:
        print(f"\nnote changes (shown on the site as {ANALYST_NAME}'s report):")
        for n in note_changes:
            text = "" if n["kind"] == "cleared" else f"  {n['analyst_note'][:110]}"
            print(f"  {n['t']['name']:<24} {n['kind']:<8} {n['analyst_note_date'] or '-':<10}{text}")
    for label, items in (("ATTENTION held", held), ("ATTENTION problems", problems),
                         ("ATTENTION unmatched sheet names (proposal queue, not created)", unmatched)):
        if items:
            print(f"\n{label}:")
            for i in items:
                print(f"  - {i}")
    if missing_from_sheet:
        print("\nin table but not on the sheet (left alone):")
        for t in missing_from_sheet:
            print(f"  - {t['name']} ({t['position']}) {fmt(num(t['value']))}/{fmt(num(t['sf_value']))}")

    # ── Apply ──
    applied = notes_applied = 0
    failures: list[str] = []
    if args.apply and (changes or note_changes):
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        backup = BACKUP_DIR / f"backup_{started:%Y-%m-%d_%H%M%S}.json"
        backup.write_text(json.dumps(table, indent=1, default=str))
        print(f"\nbackup: {backup}")
        now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
        # one PATCH per prospect, carrying its value change and/or its note change
        payloads: dict[str, dict] = {}
        for c in changes:
            payloads.setdefault(c["t"]["dan_id"], {}).update(
                {"value": c["value"], "sf_value": c["sf_value"], "values_updated_at": now_iso})
        for n in note_changes:
            payloads.setdefault(n["t"]["dan_id"], {}).update({k: n[k] for k in NOTE_COLS})
        for dan_id, payload in payloads.items():
            payload["updated_at"] = now_iso
            try:
                n = patch_row(dan_id, payload)
                if n == 1:
                    applied += "value" in payload
                    notes_applied += "analyst_note" in payload
                else:
                    failures.append(f"{dan_id}: PATCH matched {n} rows")
            except urllib.error.HTTPError as e:
                failures.append(f"{dan_id}: HTTP {e.code} {e.read()[:200]!r}")
            except Exception as e:  # noqa: BLE001
                failures.append(f"{dan_id}: {type(e).__name__}: {e}")
        print(f"applied: {applied}/{len(changes)} values, {notes_applied}/{len(note_changes)} notes"
              + (f"  FAILED: {len(failures)}" if failures else ""))
        for f in failures:
            print(f"  - {f}")
        if applied or notes_applied:
            print("note: DTVC Plus caches core data ~4h; the site picks these up within that window.")
    elif changes or note_changes:
        print("\ndry run: nothing written (pass --apply)")

    # ── Status writeback ──
    if not args.no_writeback:
        try:
            write_status(sh, started, args.apply, len(sheet_rows), len(seen_ids), unchanged,
                         changes, applied, failures, held, problems, unmatched, missing_from_sheet,
                         note_changes, notes_applied)
            print(f"status written to sheet tab '{STATUS_TAB}'")
        except Exception as e:  # noqa: BLE001
            print(f"WARN: could not write '{STATUS_TAB}' tab ({type(e).__name__}: {str(e)[:160]}); "
                  "share the sheet with the service account as Editor to enable it")

    print(f"{dt.datetime.now().astimezone():%Y-%m-%d %H:%M:%S %Z}: done"
          + ("" if not (held or problems or failures) else "  ** needs attention **"))
    return 1 if failures else 0


def restore(backup: Path) -> int:
    """Re-PATCH value / sf_value / values_updated_at for every row in a backup file,
    plus the analyst_* note columns when both the backup and the table carry them."""
    if not backup.exists():
        sys.exit(f"ERROR: backup not found: {backup}")
    if not SUPABASE_SERVICE_KEY:
        sys.exit("ERROR: SUPABASE_SERVICE_ROLE_KEY missing from nfl-db/.env")
    rows = json.loads(backup.read_text())
    table, notes_enabled = fetch_table()
    current = {t["dan_id"]: t for t in table}
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    restored = skipped = 0
    failures: list[str] = []
    print(f"restore from {backup.name}: {len(rows)} rows in backup, {len(current)} in table")
    for r in rows:
        cur = current.get(r["dan_id"])
        if cur is None:
            failures.append(f"{r['dan_id']}: not in table now (skipped)")
            continue
        with_notes = notes_enabled and all(k in r for k in NOTE_COLS)
        notes_match = not with_notes or all(cur.get(k) == r.get(k) for k in NOTE_COLS)
        if num(cur["value"]) == num(r["value"]) and num(cur["sf_value"]) == num(r["sf_value"]) and notes_match:
            skipped += 1
            continue
        payload = {"value": num(r["value"]), "sf_value": num(r["sf_value"]),
                   "values_updated_at": r.get("values_updated_at"), "updated_at": now_iso}
        if with_notes:
            payload.update({k: r.get(k) for k in NOTE_COLS})
        try:
            n = patch_row(r["dan_id"], payload)
            if n == 1:
                restored += 1
                print(f"  {cur['name']:<24} {fmt(num(cur['value']))}/{fmt(num(cur['sf_value']))} -> {fmt(num(r['value']))}/{fmt(num(r['sf_value']))}")
            else:
                failures.append(f"{r['dan_id']}: PATCH matched {n} rows")
        except Exception as e:  # noqa: BLE001
            failures.append(f"{r['dan_id']}: {type(e).__name__}: {e}")
    print(f"restored: {restored} | already matching: {skipped} | failures: {len(failures)}")
    for f in failures:
        print(f"  - {f}")
    return 1 if failures else 0


def write_status(sh, started, applied_mode, n_sheet, n_matched, unchanged, changes, applied,
                 failures, held, problems, unmatched, missing_from_sheet,
                 note_changes=(), notes_applied=0) -> None:
    import gspread

    try:
        ws = sh.worksheet(STATUS_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(STATUS_TAB, rows=80, cols=4)
    mode = "applied" if applied_mode else "dry run"
    lines = [
        ["Sync Status (written by the DTVC prospect sync job; do not edit)", ""],
        ["Last run", f"{started:%Y-%m-%d %H:%M %Z} ({mode})"],
        ["Sheet rows read", n_sheet],
        ["Matched to DTVC prospects", n_matched],
        ["Unchanged", unchanged],
        ["Changed this run", f"{applied} written" if applied_mode else f"{len(changes)} would change"],
        ["Notes changed this run", f"{notes_applied} written" if applied_mode else f"{len(note_changes)} would change"],
        ["Write failures", len(failures)],
        ["Held for review", len(held)],
        ["Problems", len(problems)],
        ["New names not yet in DTVC (Dan adds these)", len(unmatched)],
        ["In DTVC but not on sheet", len(missing_from_sheet)],
        ["", ""],
        ["How values reach the site", "value / SF_Value are copied into DTVC Plus each time the sync runs; the site refreshes within ~4 hours. "
                                      "Leave both blank only if a prospect should disappear (needs Dan). New players: add the row here and tell Dan."],
        ["How notes reach the site", f"The note shows on the prospect's card in DTVC Plus as {ANALYST_NAME}'s report. "
                                     "The date column is the date shown (for example 9/15 or 15Sep26). Leave it blank and the card shows "
                                     "the day the note last changed. Clearing a note removes it from the card."],
        ["", ""],
        ["Detail", ""],
    ]
    for c in changes:
        t = c["t"]
        lines.append([f"changed: {t['name']}", f"{fmt(num(t['value']))}/{fmt(num(t['sf_value']))} -> {fmt(c['value'])}/{fmt(c['sf_value'])}"])
    for n in note_changes:
        lines.append([f"note {n['kind']}: {n['t']['name']}", n["analyst_note_date"] or ""])
    for i in held:
        lines.append(["held", i])
    for i in problems:
        lines.append(["problem", i])
    for i in unmatched:
        lines.append(["new name (not in DTVC)", i])
    for i in failures:
        lines.append(["write failed", i])
    ws.clear()
    ws.update(range_name="A1", values=[[str(a), str(b)] for a, b in lines])


if __name__ == "__main__":
    sys.exit(main())
