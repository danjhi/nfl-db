"""Shared helpers for the FBG news pipeline (backfill + daily /updates scrape).

Turns a parsed FBG "News and Notes" item into a `news_items` row: resolves the
subject player, classifies the news_type, and upserts (insert-only, keyed on the
UNIQUE source_url so a human's later status changes are never clobbered).

Item shape (from staging/fbg-daily-news/news_items.jsonl and the /updates parser):
    {date, team, headline, source_outlet, source_author, source_url,
     fact_text, our_view_text}

Player resolution mirrors fbg-adp-demo/scripts/build-news.mjs: index players by
normalized full name, find every name that appears in fact_text+headline, and
pick the EARLIEST mention (that's the item's subject — FBG writes
"{Full Team} {POS} {Name}" up front), breaking ties by team agreement then
longer name. Team defenses are excluded from the index (their full-team-name
"player" name would swallow every "{Team} {POS} {Name}" sentence).
"""

import json
import os
import re
import sys
import unicodedata
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ids"))
from shared import (  # noqa: E402
    SUPABASE_URL, SUPABASE_KEY, SUPABASE_SERVICE_KEY, normalize_team,
)

# FBG team codes -> nflreadr style (superset of build-news.mjs TEAM_FIX).
TEAM_FIX = {"LAR": "LA", "JAC": "JAX", "WSH": "WAS", "ARZ": "ARI",
            "HST": "HOU", "BLT": "BAL", "CLV": "CLE"}


def fix_team(t):
    if not t:
        return None
    t = t.strip().upper()
    return normalize_team(TEAM_FIX.get(t, t)) or None


# ── Name normalization (matches build-news.mjs `norm`) ───────────────────────
_SUFFIX = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b")
_NONALNUM = re.compile(r"[^a-z0-9]+")


def _norm(s):
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn").lower()
    s = _NONALNUM.sub(" ", s)
    s = _SUFFIX.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return f" {s} "


# ── news_type classification (rules, first-match-wins; REPORT §classification) ─
_RULES = [
    ("injury", re.compile(r"\b(injur|surg|acl|mcl|hamstring|knee|ankle|shoulder|"
                          r"concussion|ir\b|pup\b|questionable|doubtful|out\b|"
                          r"strain|sprain|rehab|activated|placed on)\b", re.I)),
    ("trade", re.compile(r"\b(traded?|trade|acquir|deal(?:ing|t)? .*\bfor\b)\b", re.I)),
    ("contract", re.compile(r"\b(sign|signed|extension|contract|deal|guaranteed|"
                           r"franchise tag|holdout|holding out|restructur|"
                           r"released?|waiv|cut\b)\b", re.I)),
    ("depth_chart", re.compile(r"\b(starter|starting|backup|first team|first-team|"
                              r"rb1|rb2|wr1|wr2|te1|qb1|depth chart|committee|"
                              r"ahead of|behind|reps|snaps|role|carries|targets|"
                              r"lead back|number one)\b", re.I)),
    ("coaching", re.compile(r"\b(head coach|coordinator|\boc\b|\bdc\b|playcaller|"
                           r"fired|hired|coaching staff)\b", re.I)),
    ("scheme", re.compile(r"\b(scheme|system|route|footwork|offense|playbook|"
                         r"install|zone|man coverage)\b", re.I)),
    ("performance", re.compile(r"\b(impress|stood out|standout|struggl|shined|"
                              r"turned heads|best shape|looked|practice)\b", re.I)),
    ("offseason", re.compile(r"\b(minicamp|otas?|training camp|offseason|workout|"
                            r"visit|met with|reported|attend)\b", re.I)),
]


def classify_news_type(headline, fact_text):
    text = f"{headline or ''} {fact_text or ''}"
    for label, rx in _RULES:
        if rx.search(text):
            return label
    return "other"


# ── Player index + resolution ────────────────────────────────────────────────
def fetch_players():
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    out, offset = [], 0
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/players"
               f"?select=player_id,first_name,last_name,position,latest_team"
               f"&offset={offset}&limit=1000")
        req = urllib.request.Request(url, headers={"apikey": key, "Authorization": f"Bearer {key}"})
        batch = json.loads(urllib.request.urlopen(req, timeout=30).read())
        if not batch:
            break
        out.extend(batch)
        offset += 1000
    return out


def build_name_index(players):
    """normalized ' name ' -> list of (player_id, team). Excludes DEF/TDSP/TK."""
    idx = {}
    for p in players:
        pos = (p.get("position") or "").upper()
        if pos in ("DEF", "TDSP", "TK"):
            continue
        key = _norm(f"{p.get('first_name','')} {p.get('last_name','')}")
        if key == "  " or key == " ":
            continue
        idx.setdefault(key, []).append((p["player_id"], p.get("latest_team")))
    return idx


def resolve_player_id(item, name_index):
    """Return player_id for the item's subject, or None. Earliest name mention
    wins; team agreement then longer name break ties."""
    hay = _norm(f"{item.get('fact_text','')} {item.get('headline','')}")
    item_team = fix_team(item.get("team"))
    hits = []
    for name_key, entries in name_index.items():
        i = hay.find(name_key)
        if i >= 0:
            for pid, team in entries:
                hits.append((i, len(name_key), pid, team))
    if not hits:
        return None
    if len(hits) == 1:
        return hits[0][2]
    # earliest idx, then team match, then longer name
    hits.sort(key=lambda h: (h[0], 0 if (item_team and h[3] == item_team) else 1, -h[1]))
    return hits[0][2]


# ── Row build + upsert ───────────────────────────────────────────────────────
def to_row(item, player_id, news_type):
    fact = item.get("fact_text") or ""
    view = item.get("our_view_text") or ""
    return {
        "player_id": player_id,
        "team_abbr": fix_team(item.get("team")),
        "source_url": item.get("source_url"),
        "source_type": "article",
        "news_type": news_type,
        "headline": item.get("headline"),
        "summary": view or None,
        "raw_content": (fact + ("\n\nOur view: " + view if view else "")) or None,
        "published_at": item.get("date"),
        "status": "draft",
    }


def upsert_news(rows, batch_size=100):
    """Insert-only upsert keyed on source_url (existing rows untouched, so human
    status changes survive re-runs). Returns (inserted_or_ignored, errors)."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_KEY
    url = f"{SUPABASE_URL}/rest/v1/news_items?on_conflict=source_url"
    ok = errors = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        req = urllib.request.Request(url, data=json.dumps(batch).encode("utf-8"), headers={
            "apikey": key, "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=ignore-duplicates",
        }, method="POST")
        try:
            urllib.request.urlopen(req, timeout=60)
            ok += len(batch)
        except urllib.error.HTTPError as e:
            print(f"  ERROR batch at {i}: {e.code} {e.read().decode('utf-8','replace')[:300]}")
            errors += len(batch)
    return ok, errors
