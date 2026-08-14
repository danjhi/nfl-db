"""Tandem rankings: DraftKings drag-and-drop order -> Underdog + Drafters uploads.

Dan's workflow (heavy-draft-season loop):
  1. Update rankings on DK via their drag-and-drop editor, download the CSV
     (lands as ~/Downloads/DkPreDraftRankings (N).csv — row order = his order).
  2. Optionally discuss/adjust: per-site moves live in data/tandem_overrides.json.
  3. Run this script. It emits:
       ~/Downloads/DraftersRankings_from_DK_{date}.csv   (DK column format —
         Drafters' upload accepts it; same shape as the hand-made 2026-08-10 file)
       ~/Downloads/UnderdogRankings_from_DK_{date}.csv   (Underdog's own template,
         rows reordered; template comes from data/ud_rankings_latest.csv, which
         the daily fetch_underdog_postdraft_adp.py run persists each morning —
         no manual UD download needed; falls back to ~/Downloads/rankings-*.csv)
  4. Upload each file on its site.

Matching DK -> Underdog: primary via the players-table crosswalk
(draftkings_id -> player_id, which IS the Underdog UUID where present, per the
nfl-db ID conventions), fallback normalized name + position against the UD file.

Overrides file (optional), data/tandem_overrides.json:
  {
    "all":      [ {"player": "Some Name", "move": -3} ],   // both sites
    "underdog": [ {"player": "Some Name", "to": 12} ],     // absolute rank
    "drafters": [ {"player": "Some Name", "move": 5} ]     // +down / -up
  }
Overrides adjust the emitted site order only; the DK master file is never edited.

Usage:
    python3 scripts/rankings/tandem_from_dk.py [--top 250]
        [--dk FILE] [--ud FILE] [--overrides FILE]
"""

import argparse
import csv
import datetime
import glob
import json
import os
import sys
import urllib.request

_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_script_dir, "..", "ids"))
from shared import (  # noqa: E402
    SUPABASE_URL,
    SUPABASE_SERVICE_KEY,
    normalize_name,
    PLAYER_ALIASES,
)

TODAY = datetime.date.today().isoformat()
DOWNLOADS = os.path.expanduser("~/Downloads")
DEFAULT_OVERRIDES = os.path.normpath(os.path.join(_script_dir, "..", "..", "data", "tandem_overrides.json"))
UD_LATEST = os.path.normpath(os.path.join(_script_dir, "..", "..", "data", "ud_rankings_latest.csv"))


def newest(pattern, label):
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    if not files:
        sys.exit(f"ERROR: no {label} file matching {pattern}")
    return files[0]


def read_dk(path, top):
    """DK export: ID,Name,Position,ADP,Team[,,Instructions]. Row order = ranking."""
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            if not (r.get("ID") or "").strip():
                continue
            rows.append({
                "ID": r["ID"].strip(),
                "Name": (r.get("Name") or "").strip(),
                "Position": (r.get("Position") or "").strip(),
                "ADP": (r.get("ADP") or "").strip(),
                "Team": (r.get("Team") or "").strip(),
            })
    if len(rows) < top:
        print(f"  NOTE: DK file has only {len(rows)} rows (< --top {top}); using all")
    return rows[:top]


def read_ud(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if "playerId" not in (reader.fieldnames or []):
            sys.exit(f"ERROR: {path} does not look like an Underdog rankings CSV (no playerId column)")
        return reader.fieldnames, list(reader)


def fetch_crosswalk():
    """draftkings_id -> players.player_id (= Underdog UUID where present)."""
    url = (f"{SUPABASE_URL}/rest/v1/players"
           f"?draftkings_id=not.is.null&select=player_id,draftkings_id,underdog_postdraft_id,first_name,last_name,position&limit=3000")
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        rows = json.loads(resp.read())
    return {str(r["draftkings_id"]): r for r in rows}


def name_keys(name):
    """Normalized name plus its alias form (if any). The alias map is
    bidirectional pairs, so indexing AND looking up through a single hop
    would swap both sides past each other — index and look up under BOTH
    forms instead."""
    n = normalize_name(name)
    a = PLAYER_ALIASES.get(n)
    return [n, a] if a and a != n else [n]


def norm(name):
    return name_keys(name)[0]


def apply_overrides(order, moves, site, report):
    """order: list of dicts with Name. moves: list of {player, to|move}."""
    for m in moves:
        target = norm(m.get("player", ""))
        idx = next((i for i, r in enumerate(order) if norm(r["Name"]) == target), None)
        if idx is None:
            report.append(f"  OVERRIDE MISS ({site}): '{m.get('player')}' not in top list")
            continue
        row = order.pop(idx)
        if "to" in m:
            new = max(0, min(len(order), int(m["to"]) - 1))
        else:
            new = max(0, min(len(order), idx + int(m.get("move", 0))))
        order.insert(new, row)
        report.append(f"  override ({site}): {row['Name']} {idx + 1} -> {new + 1}")
    return order


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=250)
    ap.add_argument("--dk", default=None)
    ap.add_argument("--ud", default=None)
    ap.add_argument("--overrides", default=DEFAULT_OVERRIDES)
    args = ap.parse_args()

    dk_path = args.dk or newest(os.path.join(DOWNLOADS, "DkPreDraftRankings*.csv"), "DK rankings")
    if args.ud:
        ud_path = args.ud
    elif os.path.exists(UD_LATEST):
        ud_path = UD_LATEST
    else:
        ud_path = newest(os.path.join(DOWNLOADS, "rankings-*.csv"), "Underdog rankings")
    print(f"DK master:  {os.path.basename(dk_path)}  (modified {datetime.date.fromtimestamp(os.path.getmtime(dk_path))})")
    print(f"UD template: {os.path.basename(ud_path)}  (modified {datetime.date.fromtimestamp(os.path.getmtime(ud_path))})")

    master = read_dk(dk_path, args.top)
    ud_fields, ud_rows = read_ud(ud_path)
    print(f"Master top {len(master)} loaded; UD template has {len(ud_rows)} rows")

    overrides = {"all": [], "underdog": [], "drafters": []}
    if os.path.exists(args.overrides):
        with open(args.overrides) as f:
            overrides.update(json.load(f))
        print(f"Overrides: {args.overrides}")

    report = []
    xwalk = fetch_crosswalk()

    def reorder_template(fields, rows, site, out_name):
        """Reorder a site's own rankings template (UD and Drafters share the
        same UUID id/playerId schema) into master-plus-overrides order:
        matched players first, everything else tail-appended untouched."""
        by_pid = {r["playerId"]: r for r in rows}
        by_name = {}
        for r in rows:
            pos = (r.get("slotName") or "").strip()
            for k in name_keys(f"{r['firstName']} {r['lastName']}"):
                by_name.setdefault((k, pos), r)

        site_master = apply_overrides(
            apply_overrides(list(master), overrides["all"], f"all/{site}", report),
            overrides[site], site, report)

        ranked, seen, unmatched = [], set(), []
        id_hits = name_hits = 0
        for row in site_master:
            hit = None
            p = xwalk.get(row["ID"])
            # underdog_postdraft_id is the UD file's playerId for most players;
            # player_id doubles as the site UUID only for newer inserts.
            if p:
                for cand in (p.get("underdog_postdraft_id"), p.get("player_id")):
                    if cand and cand in by_pid:
                        hit = by_pid[cand]
                        id_hits += 1
                        break
            if hit is None:
                for k in name_keys(row["Name"]):
                    hit = by_name.get((k, row["Position"]))
                    if hit:
                        name_hits += 1
                        break
            if hit and hit["playerId"] not in seen:
                seen.add(hit["playerId"])
                ranked.append(hit)
            elif not hit:
                unmatched.append(f"{row['Name']} ({row['Position']})")

        tail = [r for r in rows if r["playerId"] not in seen]
        out = os.path.join(DOWNLOADS, out_name)
        with open(out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, quoting=csv.QUOTE_ALL)
            w.writeheader()
            w.writerows(ranked + tail)

        print(f"\n{site.capitalize()}: wrote {len(ranked)} ranked + {len(tail)} tail rows -> {out}")
        print(f"  matched via crosswalk: {id_hits} · via name+pos: {name_hits} · unmatched: {len(unmatched)}")
        if unmatched:
            print(f"  UNMATCHED (in DK top list, absent from the {site} file — check pool/name):")
            for u in unmatched[:15]:
                print(f"    - {u}")

    reorder_template(ud_fields, ud_rows, "underdog", f"UnderdogRankings_from_DK_{TODAY}.csv")

    # ── Drafters: their own format (id,position,name,preferred,team abbr,ADP,AVG
    # with Drafters numeric ids). Their download round-trips through their
    # importer as-is, so the transform preserves every original line verbatim
    # and reorders lines only. Template = Dan's freshest player-list download
    # (no automated fetch persists this one yet).
    dr_path = newest(os.path.join(DOWNLOADS, "drafters_players*.csv"), "Drafters player list")
    print(f"\nDrafters template: {os.path.basename(dr_path)}  "
          f"(modified {datetime.date.fromtimestamp(os.path.getmtime(dr_path))})")
    with open(dr_path, newline="", encoding="utf-8") as f:
        raw = f.readlines()
    header, data_lines = raw[0], raw[1:]
    parsed = list(csv.DictReader([header.lstrip("﻿")] + data_lines))

    dr_by_name = {}
    for line, r in zip(data_lines, parsed):
        pos = (r.get("position") or "").strip()
        for k in name_keys(r.get("name") or ""):
            dr_by_name.setdefault((k, pos), line)

    dr_master = apply_overrides(
        apply_overrides(list(master), overrides["all"], "all/drafters", report),
        overrides["drafters"], "drafters", report)
    ranked_lines, seen, dr_unmatched = [], set(), []
    for row in dr_master:
        hit = None
        for k in name_keys(row["Name"]):
            hit = dr_by_name.get((k, row["Position"]))
            if hit is not None:
                break
        if hit is not None and id(hit) not in seen:
            seen.add(id(hit))
            ranked_lines.append(hit)
        elif hit is None:
            dr_unmatched.append(f"{row['Name']} ({row['Position']})")
    tail_lines = [ln for ln in data_lines if id(ln) not in seen]

    dr_out = os.path.join(DOWNLOADS, f"DraftersRankings_from_DK_{TODAY}.csv")
    with open(dr_out, "w", newline="", encoding="utf-8") as f:
        f.write(header)
        f.writelines(ranked_lines + tail_lines)
    print(f"\nDrafters: wrote {len(ranked_lines)} ranked + {len(tail_lines)} tail rows -> {dr_out}")
    print(f"  matched by name+pos: {len(ranked_lines)} · unmatched: {len(dr_unmatched)}")
    if dr_unmatched:
        print("  UNMATCHED (in DK top list, absent from the drafters file — check pool/name):")
        for u in dr_unmatched[:15]:
            print(f"    - {u}")

    for line in report:
        print(line)
    print("\nUpload each file on its site. DK master was not modified.")


if __name__ == "__main__":
    main()
