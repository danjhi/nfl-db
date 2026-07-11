# ADP page: remaining source scrapers (ESPN, CBS, MFL, NFFC)

**Why this exists:** the footballguys.com/adp replacement (`~/dev/fbg-adp-demo`)
reads ADP from this DB's `adp_sources`. RTSports is now scraped, joined (K+DST
included), and rendering. This doc specs the next batch so they're mechanical
copies of `scripts/adp/fetch_rtsports_adp.py`. Each one you own here is one more
source the /adp page no longer needs from FBG's feed (i.e. one less thing the FBG
employee hand-pastes).

Prereqs already done: the K/DEF backfill (`docs/adp-kicker-defense-join.md`) —
so kickers and team defenses join for these too, for free.

Template: `scripts/adp/fetch_rtsports_adp.py`. Matching + upsert helpers:
`scripts/ids/shared.py`. Match by the source's id column first, name+pos fallback
(the `fetch_underdog_adp.py` pattern) — unlike RTSports, these sources have id
columns in `players`.

---

## Build these four (decisions already made with Dan)

| Source | `adp_sources.source` string | Match on | Demo treatment |
|---|---|---|---|
| **ESPN** | `espn` | `players.espn_id` → name+pos | Redraft column (overrides FBG feed's `espn`) |
| **CBS** | `cbs` | name+pos (`cbs_id` if the feed exposes one) | Redraft column (overrides FBG feed's `cbs`) |
| **MFL (redraft PPR)** | `mfl_redraft` | `players.mfl_id` (export `id` == mfl_id) | NEW redraft column, labeled "MFL". Kept distinct from the existing dynasty-SF `mfl` so it doesn't collide. |
| **NFFC (SHGN consensus)** | `nffc_shgn` | name+pos | NEW column, distinct from the OC-derived `nffc` the tool already shows |

**Not in this batch:** FBG-OC (already your daily job) and Yahoo (login-gated —
OAuth or authenticated Playwright session, its own task later).

The `source` strings above are a hard contract with the consumer. Write exactly
these; Dan wires the display side in fbg-adp-demo to match.

All four are **redraft PPR** (the /adp page is redraft), and all include K + DST.

---

## Per-source specs

### ESPN  → `source="espn"`
- ADP lives in ESPN's public `kona_player_info` JSON, **not** the
  `livedraftresults` HTML page. No login.
- Endpoint pattern (confirm/adjust for 2026):
  `https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/2026/segments/0/leaguedefaults/3?view=kona_player_info`
  with an `x-fantasy-filter` request header selecting/sorting by
  `averageDraftPosition`. ADP is at `player.ownership.averageDraftPosition`.
- Match: `players.espn_id` (the feed's player id), name+pos fallback.

### CBS  → `source="cbs"`
- `https://www.cbssports.com/fantasy/football/draft/averages/` is **static
  server-rendered HTML** (no JS). Columns: Rank, Player, Trend, Avg Pos, Hi/Lo,
  Pct. PPR + position filters are URL params — use the PPR view.
- Parse the table (name, pos, team, Avg Pos). Match name+pos.

### MFL (redraft PPR)  → `source="mfl_redraft"`
- Use the **export API on `api.myfantasyleague.com`** (the www host rejects it):
  `https://api.myfantasyleague.com/2026/export?TYPE=adp&PERIOD=RECENT&FCOUNT=12&IS_PPR=3&IS_KEEPER=N&IS_MOCK=1&CUTOFF=20&JSON=1`
  (tune params to mirror the employee's reports URL). Returns
  `{"adp":{"player":[{"id","averagePick","minPick","maxPick","rank",...}]}}`.
- The export gives **no name/position** — only `id`, which **is the `mfl_id`**.
  So match `players.mfl_id == player.id` directly; `adp = averagePick`.
- Sample size is thin pre-season (a handful of drafts); that's expected.

### NFFC (SHGN consensus)  → `source="nffc_shgn"`
- Host `nfc.shgn.com/api/public` (same host the OC draft-results job uses, but a
  different call). The `adp/football` page loads its table via an XHR — **discover
  the exact endpoint + params** (open the page, watch the network tab; pick the
  NFFC consensus contest, 12-team). This is the only one needing endpoint
  discovery.
- Match name+pos (no shgn id column in `players`).
- Note: this is intentionally a *different* number from the OC pick-level `nffc`
  the tool already has (`api/nfl/{year}/adp/nffc/oc/data`); both coexist.

---

## Shared pattern (copy from fetch_rtsports_adp.py)

- `YEAR=2026`, `SOURCE=<key above>`, `TODAY=date.today().isoformat()`.
- certifi SSL context (macOS trust store); `--dry-run` flag.
- Build `by_<source>_id` from `players` where that id is not null, plus
  `build_player_lookup(all_players)` for the name+pos fallback (handles aliases,
  and K↔DST now that the DB is backfilled).
- Rows: `{player_id, source, year, date, adp, projected_points, position_rank}`
  (proj/pos_rank NULL if the feed lacks them).
- `batch_upsert` with `Prefer: resolution=merge-duplicates` (idempotent).

---

## Verify each

1. `python3 scripts/adp/fetch_<source>_adp.py --dry-run` → check the match rate
   and eyeball the unmatched list (should be short: FA/practice-squad noise).
2. Live run → confirm rows land in `adp_sources` under the right `source`.
3. Hand back to Dan / the fbg-adp-demo session — he adds the key to
   `build-fixtures.mjs` step 3b + `DAN_OWNED_SOURCES`, and renders the column.

---

## Consumer side (Dan does this in fbg-adp-demo, not here)

- `espn`, `cbs`: direct override of the FBG-feed column in step 3b (like rtsports).
- `mfl_redraft`, `nffc_shgn`: new host entries in `shape.ts` `hostsFor(redraft)`
  + labels, plus step 3b overrides.
- All four: add to `DAN_OWNED_SOURCES` in `src/lib/data.ts` so the live FBG-feed
  overlay doesn't clobber them.

## Pointers
- Template: `scripts/adp/fetch_rtsports_adp.py`
- Helpers / aliases: `scripts/ids/shared.py`
- K/DEF join prereq (done): `docs/adp-kicker-defense-join.md`
- Schema + launchd conventions: `CLAUDE.md`
- Consumer: `~/dev/fbg-adp-demo/scripts/build-fixtures.mjs` (step 3b),
  `src/lib/shape.ts` (host keys), `src/lib/data.ts` (`DAN_OWNED_SOURCES`)
