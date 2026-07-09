# ADP page: clean kicker & team-defense joins

**Why this exists:** the footballguys.com/adp replacement (`~/dev/fbg-adp-demo`)
reads ADP from this database's `adp_sources` table, and the new per-source
scrapers (`scripts/adp/fetch_rtsports_adp.py` is live; MFL, CBS, ESPN, and the
SHGN NFFC/FBG-OC feed are next) all publish **kickers and team defenses** in
addition to offense. Those two position groups don't join cleanly today because
the `players` table was built for best ball / dynasty, where K and DST barely
matter. This doc says exactly what the ADP page needs so the DB work is a
one-time fix that benefits every source, not just RTSports.

Owner: Dan (DB work happens here in nfl-db). Consumer: the fixture builder in
fbg-adp-demo (`scripts/build-fixtures.mjs`).

---

## The join contract (what "clean" means)

For a player/kicker/defense to appear on the ADP page with a scraped value, the
same nfl-db `player_id` has to line up on both sides:

1. **Scraper side** (`fetch_*_adp.py` → `adp_sources`): the scraper matches the
   source's player to an nfl-db `players` row by name+position and writes an
   `adp_sources` row keyed by that `player_id`. → the row must **exist** with a
   matchable name.
2. **Fixture side** (`build-fixtures.mjs` → the tool): the builder resolves
   FBG's `adp-sources` feed ids to nfl-db `player_id`s via
   **`players.footballguys_id`** (then name+pos fallback, then it *synthesizes* a
   throwaway `fbg:{id}` row). It only loads `players` rows that have a
   `footballguys_id` (or appear in the best-ball series). → the row must carry
   **`footballguys_id`** so the fixture keys it under the real `player_id`
   instead of a synthetic one.

If either half is missing, the scraped value orphans: the scraper writes to
`player_id = DEF_HOU`, but the fixture rendered that defense under
`fbg:htxxxx99`, so they never meet.

**The load-bearing column is `players.footballguys_id`.** Populate it for
defenses and active kickers and both halves line up.

---

## Current state (measured 2026-07-09)

`players` position census: WR 636, RB 500, TE 319, QB 208, **PK 63**, TK 37,
TDSP 36, **DEF 32**, K 4, LB 1.

| Group | Rows | footballguys_id | Notes |
|---|---|---|---|
| **DEF** | 32 | **0 have it** | Clean team defenses. `player_id = DEF_{TEAM}` (e.g. `DEF_HOU`), name `"Houston Texans"`. This is what the ADP page uses. |
| **PK** (kickers) | 63 | 29 have it | Stale set — carries retired kickers (Janikowski, Parkey, Novak) but is **missing current starters** (Aubrey, McPherson, Dicker…). |
| K | 4 | 0 | Junk/legacy (`"Team Zendejas"`, etc.). Ignore. |
| TK | 37 | 0 | NFFC holder artifacts (`"Holder Kicker1"`, `"Missing Link"`). Not used by the ADP page. |
| TDSP | 36 | 0 | NFFC team defense/ST artifacts. Not used by the ADP page. |

How FBG represents the same things (from `api/nfl/2026/players` +
`api/nfl/2026/adp-sources`):

- **Defenses:** `pos="td"`, `id="{pfr3}xxx99"`, name `"{City} {Nickname}"`.
  Verified: HOU `htxxxx99`, SEA `seaxxx99`, DEN `denxxx99`, LAR `ramxxx99`.
- **Kickers:** `pos="pk"`, `id` in the normal player scheme (e.g.
  `AubrBr00`, `McPhEv44`). FBG's feed already carries `rtsports`/`espn`/etc ADP
  for them.

---

## The work

### 1. Defenses — backfill `footballguys_id` on the 32 `DEF_{TEAM}` rows  (highest leverage)

FBG's defense id is `{pfr3}xxx99`. **`scripts/nffc/backfill_team_fbg_ids.py`
already has the full team→pfr3 mapping and does exactly this** — but only for the
`TDSP` and `TK` artifact rows. Point the same mapping at the `DEF` rows:

```
DEF_HOU → htxxxx99   DEF_SEA → seaxxx99   DEF_DEN → denxxx99   DEF_LAR → ramxxx99   … (all 32)
```

Easiest path: extend that script (or copy its `TEAM → pfr3` dict) to also
`PATCH players SET footballguys_id = '{pfr3}xxx99' WHERE player_id = 'DEF_{TEAM}'`.
Run `--dry-run` first. This single step makes **every** ADP source's defenses
join, for all of RTSports/MFL/CBS/ESPN/NFFC at once.

Watch the team-code edge cases the mapping already handles: LAR→`ram`,
JAX→`jax`? (confirm against FBG — JAX may be `jax` or `clt`-style), WAS, LV, etc.
The existing script's dict is the source of truth; reuse it verbatim.

### 2. Kickers — add the missing current starters, with `footballguys_id`

Add these as `PK` rows (name, position `PK`, `latest_team`, `footballguys_id`).
8 of 9 already exist in FBG's `pk` list:

| Kicker | FBG id | Team |
|---|---|---|
| Brandon Aubrey | `AubrBr00` | DAL |
| Cameron Dicker | `DickCa44` | LAC |
| Cam Little | `LittCa00` | JAX |
| Jake Bates | `BateJa00` | DET |
| Tyler Loop | `LoopTy00` | BAL |
| Harrison Mevis | `MeviHa44` | FA |
| Will Reichard | `ReicWi44` | FA |
| Evan McPherson | `McPhEv44` | CIN |
| Andres Borregales | *(not in FBG pk list yet)* | NE |

Borregales (rookie) isn't in FBG's list yet — skip or add without a
`footballguys_id`; he'll join once FBG carries him. Better than a one-off: pull
FBG's full `pos=pk` list from `api/nfl/2026/players` and upsert any active kicker
missing from `players`, setting `footballguys_id` from FBG. The
`scripts/ids/` matching suite + `shared.py` helpers already do name resolution.

Optional: backfill `footballguys_id` on the 29→63 existing PK rows that are still
active. Most of the 34 without it are retired (low value); focus on current
kickers.

### 3. Position-code note (no DB change, just awareness)

Kickers are `PK` here; ADP sources emit `"K"`. The scrapers match kickers via the
name-only fallback, and the demo's `fixPos()` maps `pk→K`. Once the rows exist
with correct names this works. If we want exact (name, position) matching in the
scrapers, treat `K ≡ PK` there — but it's not required.

---

## Verify when done

1. Re-run the scraper: `python3 scripts/adp/fetch_rtsports_adp.py --dry-run` →
   "Unmatched offense/K" should drop from 9 toward 0.
2. Rebuild the demo fixture (in fbg-adp-demo): `npm run data` → the
   `RTSports (own scraper): N applied, M orphaned` line should show **M≈0** (was
   19 orphaned, all defenses).
3. In the tool, the RTSports column should show your scraped values for kickers
   and DSTs, not FBG's fallback.

---

## Pointers

- Team fbg-id backfill pattern: `scripts/nffc/backfill_team_fbg_ids.py`
- Name matching + aliases: `scripts/ids/shared.py`, `scripts/ids/`
- ADP scraper template: `scripts/adp/fetch_rtsports_adp.py`
- Consumer (why the join matters): `~/dev/fbg-adp-demo/scripts/build-fixtures.mjs`
  (step "3b" reads `source=rtsports`; the resolver is `resolveFbg`).
- Schema: `players` (player_id, footballguys_id, first/last_name, position,
  latest_team), `adp_sources` (see CLAUDE.md).
