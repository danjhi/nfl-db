# CLAUDE.md

## Project Overview

NFL Database — central repo for managing the Supabase database used across all NFL and fantasy football projects. Contains data pipelines, loading scripts, schema documentation, and DFS analysis notebooks. No app code lives here.

## Obsidian Vault — Sync Rules

Vault path: `/Users/dan/obsidian-vault/Fantasy Football/`

**Always update the vault proactively — without waiting to be asked — whenever any of the following happen:**

### 1. Schema changes (new table, column, migration)
- `nfl-db Schema History.md` — add a dated entry (what changed, why, row counts)
- `nfl-db Schema.md` — update table count and list if tables added/removed
- `Tables/[table_name].md` — create or update the individual table note

### 2. New scripts or automation
- Find the most relevant existing task note (e.g. `Task - Multi-Source ADP.md`) and update it: mark action items complete, add implementation notes (API details, match rates, gotchas, launchd schedule)
- Update `Architecture Overview.md` if the data flow or source status table changes
- Update `nfl-db.md` if the pipeline section changes

### 3. Task completion
- Find the task note and update its frontmatter (`status: complete`, `modified: today`) and add a completion summary with final stats/row counts

### General principle
The vault is the historical record and design rationale. CLAUDE.md is operational. Don't duplicate content verbatim — the vault holds the *why* and *what happened*; CLAUDE.md holds the *how to run it*.

## Supabase

- **Project ref:** `twfzcrodldvhpfaykasj`
- **URL:** `https://twfzcrodldvhpfaykasj.supabase.co`
- **Auth:** `.env` contains `SUPABASE_ANON_KEY`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_ACCESS_TOKEN` (PAT), `NFFC_API_KEY`, `SPORTSDATA_API_KEY`, `FBG_API_KEY`, `SUPABASE_DB_PASSWORD`
- **Direct Postgres:** `db.twfzcrodldvhpfaykasj.supabase.co:5432` — use for DDL migrations when MCP is unavailable

## External APIs

| API | Base URL | Auth | Key |
|-----|----------|------|-----|
| **NFFC** | `https://nfc.shgn.com/api/public` | Query param | `NFFC_API_KEY` |
| **SportsData.io** | `https://api.sportsdata.io/v3/nfl/` | Header `Ocp-Apim-Subscription-Key` | `SPORTSDATA_API_KEY` |
| **Footballguys** | `https://www.footballguys.com/api/` | Query param `apikey` | `FBG_API_KEY` |
| **Sleeper** | `https://api.sleeper.app/v1/` | None required | N/A |

### SportsData.io Endpoints Used
- `scores/json/Players` — Full player list with PlayerID, FanDuelPlayerID, DraftKingsPlayerID, bios, photos
- `scores/json/Rookies/{season}` — Rookies by draft year (e.g., 2026) with all IDs
- `scores/json/PlayersByTeam/{team}` — Players by team (fallback)
- `projections/json/PlayerGameProjectionStatsByWeek/{season}/{week}` — Weekly DFS projections + salaries
- `projections/json/PlayerSeasonProjectionStats/{season}` — Full season projections with ADP

### Sleeper Endpoints Used
- `players/nfl` — Full NFL player database (~5MB, call sparingly). Returns player_id (sleeper_id), sportradar_id, espn_id, yahoo_id, fantasy_data_id, stats_id, rotowire_id, rotoworld_id, team, position, height, weight, age, college, depth_chart info

### FBG Endpoints Used
- `projections/weekly?year={year}&week={week}` — Weekly stat projections keyed by FBG player ID
- FBG player IDs are abbreviated name+year codes (e.g., "RobiB101", "WillGa99")
- FBG→SportsDataIO crosswalk available via Google Sheet (cached in `data/imports/fbg_crosswalk.csv`)

## Scripts

Scripts are organized by data source.

### NFFC (`scripts/nffc/`)

| Script | Purpose |
|--------|---------|
| `pull_draft_results.py` | Pull raw NFFC API data (all contest types, 2018-2025) into `data/raw/` |
| `build_clean_dataset.py` | Filter to Rotowire OC, enrich via nflreadr, fix times_drafted from draft_picks, output CSVs to `data/clean/` |
| `load_to_supabase.py` | Load clean CSVs into Supabase via REST API |
| `build_player_seasons.R` | Build player-season-team CSV from nflreadr rosters (requires R + nflreadr) |

### Player ID Matching (`scripts/ids/`)

| Script | Purpose |
|--------|---------|
| `shared.py` | Shared utilities: name normalization, Supabase helpers, env loading |
| `match_nflreadr_ids.py` | Match nflreadr ff_playerids by sportradar_id → 12 ID columns |
| `match_sportsdata_ids.py` | Match SportsData.io players by name+team → sportsdata/fanduel/draftkings IDs |
| `match_underdog_ids.py` | Match Underdog CSV by name+position → underdog_id |
| `match_dk_ids.py` | Match DraftKings CSV by name+position → draftkings_id |
| `match_drafters_ids.py` | Match Drafters CSV by name+position → drafters_id |
| `match_fbg_ids.py` | Match FBG via SportsDataIO crosswalk → footballguys_id |
| `match_sleeper_ids.py` | Match Sleeper API by sportradar_id → sleeper_id + 6 other IDs |
| `match_sportsdata_rookies.py` | Fetch rookies by season from SportsData.io → sportsdata/fanduel/dk IDs |
| `update_supabase_ids.py` | Merge all matched JSONs and PATCH players in Supabase |
| `add_missing_players.py` | Insert players from Underdog top 500 not in DB |
| `generate_update_sql.py` | Generate .sql file for bulk updates via Management API |
| `load_underdog_adp.py` | Load Underdog ADP CSV into adp_sources table via REST API |
| `match_dan_ids.py` | Bootstrap dan_id on players + initial dynasty_values load from CSV |
| `enrich_from_fbg.py` | Fetch FBG NFLPlayers.json → fill footballguys_id, fantasy_data_id, height, weight gaps |
| `enrich_from_sportsdata.py` | Fetch SportsData.io Players → fill height, weight, headshot, college, IDs, status |
| `refresh_player_teams.py` | Daily: pull Sleeper API, compare teams, PATCH `latest_team` changes. Supports `--dry-run` |
| `run_daily_team_refresh.sh` | Bash wrapper with date-gating (Feb 19 – Apr 22) for launchd scheduling |

### Teams (`scripts/teams/`)

| Script | Purpose |
|--------|---------|
| `export_teams.R` | Export nflreadr `load_teams()` → `data/nflreadr/teams.csv` (requires R + nflreadr) |
| `load_teams.py` | Load teams CSV into Supabase teams table via REST API |
| `build_team_game_stats.R` | Build team-game-level stats from nflreadr (2016-2025) → `data/nflreadr/team_game_stats.csv` |
| `load_team_game_stats.py` | Load team game stats CSV into Supabase (excludes generated columns from payload) |

### Player Stats (`scripts/stats/`)

| Script | Purpose |
|--------|---------|
| `build_player_stats.R` | Export weekly player stats from nflreadr (2016-2025) → `data/nflreadr/player_stats.csv`. Maps gsis_id → sportradar_id via ff_playerids.csv |
| `load_player_stats.py` | Load player stats CSV into Supabase (filters to DB players, excludes generated PPR columns) |

### ADP (`scripts/adp/`)

| Script | Purpose |
|--------|---------|
| `fetch_underdog_adp.py` | Fetch daily Underdog ADP CSV → upsert into adp_sources (designed to run daily) |
| `run_daily_adp.sh` | Bash wrapper with date-gating (Feb 19 – Apr 22) for launchd scheduling |
| `fetch_drafters_adp.py` | Fetch daily Drafters ADP via Bearer JWT → upsert into adp_sources. Reads `DRAFTERS_JWT` from `.env`; logs a clear error if token has expired (401). ADP stored in round.pick float format (e.g. 1.089). |
| `run_daily_drafters_adp.sh` | Bash wrapper with date-gating (Feb 19 – Apr 22) for launchd scheduling |
| `load_draftkings_adp.py` | Load DraftKings ADP from manually downloaded CSV → upsert into adp_sources. Auto-finds latest `DkPreDraftRankings*.csv` in ~/Downloads. ADP in overall pick float format. |
| `setup_dk_session.py` | One-time setup: extract DK cookies from Chrome Profile 2 via `browser_cookie3` → save to `data/dk_session.json`. Re-run when daily fetch returns auth errors (~2 weeks). Chrome must be logged in to DK in Profile 2. |
| `fetch_draftkings_adp.py` | Daily automated DK ADP fetch → upsert into adp_sources. Uses Playwright Chromium with saved session (loads a DK page to trigger fresh `jwe`, then hits API). Requires `data/dk_session.json` from setup script. |
| `run_daily_draftkings_adp.sh` | Bash wrapper with date-gating (Feb 19 – Apr 22) for launchd scheduling |
| `export_dynasty_adp_merge.py` | Join today's Underdog ADP with dynasty values → CSV export for spreadsheets |

### Projections (`scripts/projections/`)

| Script | Purpose |
|--------|---------|
| `fetch_fbg_projections.py` | Fetch FBG preseason projections → calculate half-PPR → upsert into player_projections |

### Player Notes (`scripts/notes/`)

| Script | Purpose |
|--------|---------|
| `push_writeups.py` | Read `data/writeups/player_writeups.yaml`, filter non-empty writeups, upsert into `player_notes` via REST API. Supports `--dry-run` |

### Enrichment (`scripts/ids/`)

| Script | Purpose |
|--------|---------|
| `upload_rookie_headshots.py` | Upload rookie headshot PNGs to Supabase Storage → set headshot_url on players |
| `load_dynasty_value_history.py` | One-time backfill of change log CSV into dynasty_value_history table. Matches Player→dan_id→player_id with name fallback |

### Google Apps Script (`scripts/google-apps-script/`)

| File | Purpose |
|------|---------|
| `dynasty_values_sync.js` | Sync dynasty values from Google Sheet → Supabase. Paste into Extensions → Apps Script. Uses service role key stored in Script Properties. |
| `dynasty_value_history_sync.js` | Sync dynasty value change log from Google Sheet → Supabase. Same setup pattern as dynasty_values_sync. Matches Player names to player_id via normalized name lookup. |

### FBG Bowl (`scripts/fbg_bowl/`)

Complete ETL pipeline for FBG Bowl historical data. Both 2024 and 2025 fully loaded. 12-team Sleeper-based competition.

| Script | Purpose |
|--------|---------|
| `shared.py` | Shared utilities: Sleeper API fetch (0.15s sleep, retry), Supabase REST helpers, `compute_week_results()` for W/L from matchup data |
| `00_load_league_ids.py` | Load league IDs into `fbg_bowl_leagues`. 2025: reads local CSV. 2024: reads local CSV (IDs extracted from standings `league_id` column, deduplicated) |
| `01_fetch_leagues_and_rosters.py` | Fetch league metadata + rosters/users from Sleeper → `fbg_bowl_leagues` + `fbg_bowl_rosters` |
| `02_fetch_weekly_matchups.py` | Fetch weeks 1–14 matchups → `fbg_bowl_weekly_results` + `fbg_bowl_standings`. `--playoff` mode fetches weeks 15–17 → `fbg_bowl_playoff_results` |
| `03_fetch_draft_picks.py` | Fetch draft picks from Sleeper → `fbg_bowl_draft_picks` (100K+ rows) |
| `04_compute_fbg_scores.py` | Compute FBG Bowl meta-scores (wins + league bonus + semi/finals + top-10 bonuses) → `fbg_bowl_scores` |
| `05_validate.py` | Validate DB against local CSVs and Google Sheets. Checks row counts, week-14 standings, playoff sheet |
| `schema.sql` | DDL for all 7 tables. Applied via pg8000 (Management API blocked) |

**Scoring formula**: 1pt/win + 35 (1st in league) or 10 (2nd) + 35 (semi, week 16) + 35 (finals, week 17) + top-10 bonus (300/200/150/125/100/85/70/55/45/35)

**Playoff qualification**: league_rank ≤ 2 OR pts_for ≥ 1920 after week 14

**2025 data loaded** (as of Feb 2026): 417 leagues, 5,004 rosters, 70,056 weekly results, 4,371 playoff results, 100,080 draft picks, 5,004 scores

**2024 data loaded** (as of Feb 2026): 159 leagues, 1,896 rosters, 26,544 weekly results, 2,274 playoff results, 37,920 draft picks, 1,896 scores. League IDs extracted from standings column (deduplicated from 1,896 rows × 12 teams). Saved to `FBG Bowl 2024 League IDs.csv`.

### Sleeper Trade Data (`scripts/sleeper/`)

Upload pipeline for Sleeper dynasty trade data. Source data lives in a separate repo at `~/dev/sleeper-scrape/` which produces `sleeper.db` (274 MB SQLite). This section in nfl-db handles the Supabase upload only.

**Source data (in `~/dev/sleeper-scrape/sleeper.db`):**
- 174,965 users · 65,695 leagues (42,734 dynasty) · 12,629 leagues for 2026
- 15,509 trades · 66,927 trade assets (26,682 players, 40,245 picks) from 3,972 leagues
- Date range: Dec 2025 – Mar 2026

| Script | Purpose |
|--------|---------|
| `shared.py` | Supabase REST helpers (adapted from `fbg_bowl/shared.py`) |
| `upload_leagues.py` | Read `sleeper_leagues` from SQLite → upsert into Supabase `sleeper_leagues` |
| `upload_trades.py` | Read `sleeper_trades` + `sleeper_trade_assets` from SQLite → upsert into Supabase |
| `schema.sql` | DDL for all 3 Supabase tables + indexes + RLS |

**How to run:**
```bash
cd ~/dev/nfl-db
python3 scripts/sleeper/upload_leagues.py    # ~12,629 rows (2026 dynasty leagues)
python3 scripts/sleeper/upload_trades.py     # ~15,509 trades + ~66,927 assets
```

**Implementation notes:**
- Read from SQLite at path configured via `SLEEPER_DB_PATH` env var (default: `~/dev/sleeper-scrape/sleeper.db`)
- Use `supa_upsert()` with `on_conflict="transaction_id"` for trades, `on_conflict="league_id"` for leagues
- Batch size: 500 rows (matching nfl-db convention)
- Trade assets: delete + re-insert per transaction (no natural PK for upsert — use `transaction_id` to clear old assets first, then batch insert)
- Skip `raw_json` column to save space (can always re-read from local SQLite if needed)
- All 3 tables need to be created in Supabase before first upload — use `schema.sql`

**Supabase table schemas:**

```sql
-- Sleeper dynasty leagues (subset: 2026 dynasty leagues with trades)
CREATE TABLE sleeper_leagues (
  league_id         text PRIMARY KEY,
  season            int,
  name              text,
  total_rosters     int,
  status            text,
  is_dynasty        boolean DEFAULT false,
  is_superflex      boolean DEFAULT false,
  is_tep            boolean DEFAULT false,
  is_idp            boolean DEFAULT false,
  ppr_type          text,       -- 'full', 'half', 'standard'
  rec_ppr           numeric,
  te_premium        numeric,
  pass_td_pts       numeric,
  starter_qb        int,
  starter_rb        int,
  starter_wr        int,
  starter_te        int,
  starter_flex      int,
  starter_super_flex int,
  bench_count       int,
  taxi_slots        int,
  draft_rounds      int,
  pick_trading      boolean DEFAULT true
);

-- Completed trades from dynasty leagues
CREATE TABLE sleeper_trades (
  transaction_id    text PRIMARY KEY,
  league_id         text REFERENCES sleeper_leagues(league_id),
  season            int,
  week              int,
  created_ms        bigint,
  roster_ids        jsonb,
  consenter_ids     jsonb
);

-- Individual assets (players + picks) within each trade
CREATE TABLE sleeper_trade_assets (
  id                       serial PRIMARY KEY,
  transaction_id           text REFERENCES sleeper_trades(transaction_id),
  receiving_roster_id      int,
  asset_type               text,   -- 'player' or 'pick'
  sleeper_player_id        text,
  pick_season              int,
  pick_round               int,
  pick_original_roster_id  int,
  pick_slot                int
);
```

**Related repos:**
- `~/dev/sleeper-scrape/` — Python scraper that produces `sleeper.db` (run `python3 -u scrape_trades.py --skip-discovery` to refresh)
- `~/dev/trade-db/` — Next.js app for browsing trades (reads `sleeper.db` directly, will switch to Supabase later)

### Analysis (`analysis/`)

| File | Purpose |
|------|---------|
| `exploration.Rmd` | DFS analysis notebook: team FP prediction models, variance analysis, simulation parameters |

### Data Pipeline

```
NFFC API → data/raw/ (JSON)
    → build_clean_dataset.py → data/clean/ (CSV)
        → load_to_supabase.py → Supabase

nflreadr (R) → data/nflreadr/ff_playerids.csv
    → match_nflreadr_ids.py → data/matched/nflreadr_ids.json
        → update_supabase_ids.py → Supabase

SportsData.io API → match_sportsdata_ids.py → data/matched/sportsdata_ids.json
SportsData.io Rookies → match_sportsdata_rookies.py → (merges into sportsdata_ids.json)
Sleeper API → match_sleeper_ids.py → data/matched/sleeper_ids.json
Underdog CSV → match_underdog_ids.py → data/matched/underdog_ids.json
DraftKings CSV → match_dk_ids.py → data/matched/dk_ids.json
Drafters CSV → match_drafters_ids.py → data/matched/drafters_ids.json
FBG crosswalk → match_fbg_ids.py → data/matched/fbg_ids.json
    → update_supabase_ids.py → Supabase (PATCH all IDs)

Underdog ADP CSV → load_underdog_adp.py → Supabase adp_sources table (one-time historical)
Underdog ADP endpoint → fetch_underdog_adp.py → Supabase adp_sources table (daily snapshots)
Drafters node API → fetch_drafters_adp.py → Supabase adp_sources table (daily snapshots, source="drafters")
DraftKings CSV (manual download) → load_draftkings_adp.py → Supabase adp_sources table (manual snapshots, source="draftkings")
Chrome Profile 2 cookies → setup_dk_session.py → data/dk_session.json → fetch_draftkings_adp.py (Playwright) → Supabase adp_sources table (daily snapshots, source="draftkings")

Dynasty values CSV → match_dan_ids.py → Supabase players (dan_id) + dynasty_values (bootstrap)
Google Sheet → Apps Script (dynasty_values_sync.js) → Supabase dynasty_values (ongoing sync)
Change Log Sheet → Apps Script (dynasty_value_history_sync.js) → Supabase dynasty_value_history (ongoing sync)
Change Log CSV → load_dynasty_value_history.py → Supabase dynasty_value_history (one-time backfill)

FBG NFLPlayers.json → enrich_from_fbg.py → Supabase players (footballguys_id, fantasy_data_id, height, weight)
SportsData.io Players → enrich_from_sportsdata.py → Supabase players (height, weight, headshot, college, IDs, status)

nflreadr (R) → export_teams.R → data/nflreadr/teams.csv → load_teams.py → Supabase teams table
nflreadr (R) → build_team_game_stats.R → data/nflreadr/team_game_stats.csv → load_team_game_stats.py → Supabase team_game_stats
nflreadr (R) → build_player_stats.R → data/nflreadr/player_stats.csv → load_player_stats.py → Supabase player_stats

FBG preseason API → fetch_fbg_projections.py → Supabase player_projections (half-PPR season projections)
Rookie headshot PNGs → upload_rookie_headshots.py → Supabase Storage (headshots bucket) → players.headshot_url

Sleeper API → refresh_player_teams.py → Supabase players.latest_team (daily, via launchd)

Player writeups YAML → push_writeups.py → Supabase player_notes (upsert, service role key)

Sleeper scrape SQLite (~/dev/sleeper-scrape/sleeper.db)
    → scripts/sleeper/upload_leagues.py → Supabase sleeper_leagues (upsert, 12,629 rows)
    → scripts/sleeper/upload_trades.py → Supabase sleeper_trades + sleeper_trade_assets (15,509 + 66,927 rows)
```

### Daily Automation (launchd)

All jobs use `/usr/bin/python3 -c` inline Python via launchd. Date-gated to Feb 19 – Apr 22, 2026. (The `.sh` wrapper scripts exist but are not called by launchd — bash under launchd cannot access Desktop directory files due to macOS security restrictions.)

| Job | Plist | Schedule | Script |
|-----|-------|----------|--------|
| Underdog ADP | `~/Library/LaunchAgents/com.nfldb.daily-adp.plist` | 8:00 AM | inline Python → `fetch_underdog_adp.py` |
| Drafters ADP | `~/Library/LaunchAgents/com.nfldb.daily-drafters-adp.plist` | 8:05 AM | inline Python → `fetch_drafters_adp.py` |
| DraftKings ADP | `~/Library/LaunchAgents/com.nfldb.daily-draftkings-adp.plist` | 8:10 AM | inline Python → `fetch_draftkings_adp.py` |
| Team Refresh | `~/Library/LaunchAgents/com.nfldb.daily-team-refresh.plist` | 8:15 AM | inline Python → `refresh_player_teams.py` |

Logs: `data/logs/underdog_adp.log`, `data/logs/drafters_adp.log`, `data/logs/draftkings_adp.log`, `data/logs/team_refresh.log`, `data/logs/team_refresh.jsonl`

**Important**: All plists use `/usr/bin/python3 -c` with inline Python (pattern: `os.chdir(repo); exec(compile(open(script).read(), script, 'exec'))`). Do NOT use `/bin/bash` — bash under launchd cannot read files in `~/dev/` due to macOS security. The `com.apple.provenance` attribute on files created by VS Code/Claude is set by the OS and cannot be removed.

To manage:
- `launchctl load ~/Library/LaunchAgents/com.nfldb.daily-*.plist` — enable
- `launchctl unload ...` — disable
- `launchctl list | grep nfldb` — check status

**Team refresh policy**: Only updates `latest_team` when Sleeper shows a new team. Never nulls out teams — retired/FA players keep their last team. Sleeper is the controlling source (94.7% coverage, free, no auth, fast FA updates).

## Database Schema

### Tables

#### `players`
| Column | Type | Notes |
|--------|------|-------|
| `player_id` | text PK | Sportradar UUID (= NFFC player UUID), or Underdog UUID for rookies, or generated UUID for pre-draft prospects with neither |
| `first_name` | text | |
| `last_name` | text | |
| `position` | text | QB, RB, WR, TE, K, TK, TDSP |
| `birth_date` | date | NULL for invalid ("0000-00-00") |
| `gsis_id` | text | NFL GSIS ID (join key to nflreadr) |
| `espn_id` | text | |
| `yahoo_id` | text | |
| `sleeper_id` | text | |
| `pfr_id` | text | Pro Football Reference ID |
| `rotowire_id` | text | |
| `headshot_url` | text | |
| `college` | text | |
| `draft_year` | integer | NFL draft year |
| `draft_round` | integer | |
| `draft_pick` | integer | Overall NFL draft pick |
| `latest_team` | text | Most recent NFL team abbreviation |
| `status` | text | Active, Inactive, etc. |
| `pff_id` | text | PFF player ID (from nflreadr) |
| `fantasypros_id` | text | FantasyPros ID (from nflreadr) |
| `mfl_id` | text | MFL (MyFantasyLeague) ID (from nflreadr) |
| `stats_id` | text | Stats Inc ID (from nflreadr) |
| `stats_global_id` | text | Stats Global ID (from nflreadr) |
| `fantasy_data_id` | text | FantasyData ID (from nflreadr) |
| `cbs_id` | text | CBS Sports ID (from nflreadr) |
| `fleaflicker_id` | text | Fleaflicker ID (from nflreadr) |
| `swish_id` | text | Swish Analytics ID (from nflreadr) |
| `ktc_id` | text | KeepTradeCut ID (from nflreadr) |
| `cfbref_id` | text | College Football Reference ID (from nflreadr) |
| `rotoworld_id` | text | Rotoworld ID (from nflreadr) |
| `sportsdata_id` | text | SportsData.io integer PlayerID |
| `footballguys_id` | text | FBG abbreviated name+year code |
| `fanduel_id` | text | FanDuel player ID (from SportsData.io) |
| `draftkings_id` | text | DraftKings player ID (from SportsData.io or CSV) |
| `underdog_id` | text | Underdog Fantasy UUID |
| `drafters_id` | text | Drafters platform ID |
| `dan_id` | text | Personal custom ID (unique partial index, used for dynasty values sync) |
| `height` | text | e.g. "6-2" (from FBG) |
| `weight` | integer | In pounds (from FBG) |

#### `leagues`
| Column | Type | Notes |
|--------|------|-------|
| `league_id` | integer PK | NFFC league ID |
| `year` | integer | Season year |
| `name` | text | Full league name |
| `num_teams` | integer | Usually 12 for Rotowire OC |
| `third_round_reversal` | boolean | 3RR enabled |
| `draft_date` | text | ISO timestamp |
| `draft_completed_date` | text | |

#### `league_teams`
| Column | Type | Notes |
|--------|------|-------|
| `league_id` | integer | FK → leagues |
| `team_id` | integer | NFFC team ID |
| `year` | integer | |
| `draft_order` | integer | 1-12 slot position. NULL for 2018 |
| `league_rank` | integer | Final standing (1=winner). NULL for 2018, 2025 |
| `league_points` | numeric | Season fantasy points. NULL for 2018, 2025 |
| `overall_rank` | integer | Cross-league ranking |
| `overall_points` | numeric | |
| PK | | (league_id, team_id) |

#### `draft_picks`
| Column | Type | Notes |
|--------|------|-------|
| `league_id` | integer | FK → leagues |
| `year` | integer | |
| `round` | integer | 1-20 |
| `pick_in_round` | integer | 1-12 (derived: overall - (round-1)*12) |
| `overall_pick` | integer | 1-240 (from API `pick` field) |
| `team_id` | integer | FK → league_teams |
| `player_id` | text | FK → players. ~1.4% are empty (API bug) |
| `picked_at` | text | ISO timestamp |
| `pick_duration` | integer | Seconds (can exceed 32K for email drafts) |
| PK | | (league_id, overall_pick) |

#### `adp`
| Column | Type | Notes |
|--------|------|-------|
| `player_id` | text | FK → players |
| `year` | integer | |
| `adp` | numeric | Average draft position (NFFC Rotowire OC) |
| `min_pick` | integer | Earliest pick |
| `max_pick` | integer | Latest pick |
| `times_drafted` | integer | |
| PK | | (player_id, year) |

#### `adp_sources`
| Column | Type | Notes |
|--------|------|-------|
| `player_id` | text | FK → players |
| `source` | text | Platform name (e.g., "underdog", "draftkings", "drafters") |
| `year` | integer | Season year |
| `date` | date | Date of the ADP snapshot (defaults to CURRENT_DATE) |
| `adp` | numeric | Average draft position on that platform |
| `projected_points` | numeric | Platform's projected fantasy points (nullable) |
| `position_rank` | text | Platform's position rank, e.g. "RB1" (nullable) |
| `retrieved_at` | timestamptz | When data was pulled (defaults to now()) |
| PK | | (player_id, source, year, date) |

Daily snapshots allow ADP tracking over time. Each day's fetch creates new rows rather than overwriting.

#### `dynasty_values`
| Column | Type | Notes |
|--------|------|-------|
| `player_id` | text PK | FK → players |
| `value` | numeric | 1QB dynasty trade value (NOT NULL) |
| `sf_value` | numeric | Superflex dynasty trade value (nullable) |
| `updated_at` | timestamptz | Last sync time (defaults to now()) |

Synced from Google Sheet via Google Apps Script. Full replace (delete + insert) on each push.

#### `dynasty_value_history`
| Column | Type | Notes |
|--------|------|-------|
| `player_id` | text | FK → players, part of PK |
| `date` | date | Date of the value change, part of PK |
| `old_value` | numeric | Previous trade value (nullable) |
| `new_value` | numeric | Updated trade value (nullable) |
| `comment` | text | Editorial comment explaining the change (nullable) |

Synced from Google Sheet via Apps Script (`dynasty_value_history_sync.js`). Full replace on each push. Player names resolved via normalized name matching against Supabase players table.

#### `dynasty_pick_values`
| Column | Type | Notes |
|--------|------|-------|
| `year` | integer | Draft year (2027, 2028), part of PK |
| `round` | integer | 1-4, part of PK |
| `tier` | text | 'early', 'mid', 'late', or 'random', part of PK. CHECK constraint enforced |
| `value` | numeric | 1QB dynasty trade value (NOT NULL) |
| `sf_value` | numeric | Superflex dynasty trade value (nullable) |

32 rows (2 years × 4 rounds × 4 tiers). Manually maintained — values change rarely. No Apps Script sync; update via SQL when needed.

#### `positional_model_coefficients`
| Column | Type | Notes |
|--------|------|-------|
| `position` | text PK | 'QB', 'RB', 'TE', or 'WR'. CHECK constraint enforced |
| `intercept` | numeric | Model intercept |
| `league_size` | numeric | Coefficient for league size (6-16) |
| `num_rb` | numeric | Coefficient for number of RB starter slots |
| `num_wr` | numeric | Coefficient for number of WR starter slots |
| `num_te` | numeric | Coefficient for number of TE starter slots |
| `num_fl` | numeric | Coefficient for number of FLEX slots |
| `num_sf` | numeric | Coefficient for superflex slot (0 or 1) |
| `per_reception` | numeric | Coefficient for PPR value |
| `rb_ppr_prem` | numeric | Coefficient for RB PPR premium (RB PPR - base PPR) |
| `wr_ppr_prem` | numeric | Coefficient for WR PPR premium |
| `te_ppr_prem` | numeric | Coefficient for TE PPR premium (TEP - base PPR) |
| `per_passing_td` | numeric | Coefficient for points per passing TD |
| `per_rushing_first_down` | numeric | Coefficient for points per rushing first down |
| `per_receiving_first_down` | numeric | Coefficient for points per receiving first down |
| `per_carry` | numeric | Coefficient for points per carry |

Linear model coefficients extracted from original R multivariate regression (`mlm`). Predicts positional value share (% of total fantasy points above replacement) given league settings. 4 rows, 15 numeric columns.

**Usage**: `predicted_share = intercept + coef₁×x₁ + ... + coef₁₄×x₁₄`, then `multiplier = baseline_share / predicted_share`.

#### `positional_model_baselines`
| Column | Type | Notes |
|--------|------|-------|
| `format` | text PK | '1qb' or 'sf'. CHECK constraint enforced |
| `qb_share` | numeric | QB % of total value above replacement |
| `rb_share` | numeric | RB % |
| `te_share` | numeric | TE % |
| `wr_share` | numeric | WR % |

Default positional value shares that dynasty values are calibrated to. 1QB base: 12-team, full PPR, 4pt passing TD, 2RB/3WR/1TE/1FLEX. SF base: same + superflex slot. When user settings match baseline, multipliers = 1.0.

#### `colleges`
| Column | Type | Notes |
|--------|------|-------|
| `school` | text PK | School name (e.g., "Alabama", "Ole Miss", "NC State") |
| `mascot` | text | Team mascot (e.g., "Crimson Tide") |
| `abbreviation` | text | Short abbreviation (e.g., "ALA") |
| `conference` | text | Conference name (e.g., "SEC") |
| `division` | text | Conference division (e.g., "West") |
| `color` | text | Primary hex color |
| `alt_color` | text | Secondary hex color |
| `logo` | text | ESPN logo URL (light background) |
| `logo_dark` | text | ESPN logo URL (dark background) |

738 schools from ESPN. Join via `players.college = colleges.school`. Player college names normalized to match (Ole Miss not Mississippi, NC State not North Carolina State, Miami not Miami (FL), BYU not Brigham Young).

#### `player_projections`
| Column | Type | Notes |
|--------|------|-------|
| `player_id` | text | FK → players |
| `source` | text | Projection source (e.g., "fbg") |
| `year` | integer | Season year |
| `season_type` | text | "regular" (default) |
| `games` | numeric | Projected games |
| `pass_att` | numeric | Pass attempts |
| `pass_cmp` | numeric | Pass completions |
| `pass_yds` | numeric | Pass yards |
| `pass_td` | numeric | Pass TDs |
| `pass_int` | numeric | Interceptions |
| `pass_sck` | numeric | Sacks |
| `pass_first_downs` | numeric | Passing first downs |
| `rush_att` | numeric | Rush attempts |
| `rush_yds` | numeric | Rush yards |
| `rush_td` | numeric | Rush TDs |
| `rush_first_downs` | numeric | Rushing first downs |
| `targets` | numeric | Receiving targets |
| `receptions` | numeric | Receptions |
| `rec_yds` | numeric | Receiving yards |
| `rec_td` | numeric | Receiving TDs |
| `rec_first_downs` | numeric | Receiving first downs |
| `fumbles_lost` | numeric | Fumbles lost |
| `half_ppr_pts` | numeric | Calculated half-PPR fantasy points |
| PK | | (player_id, source, year, season_type) |

#### `teams`
| Column | Type | Notes |
|--------|------|-------|
| `team_abbr` | text PK | nflreadr standard abbreviation (e.g., LA not LAR for Rams) |
| `team_name` | text NOT NULL | Full name (e.g., "Los Angeles Rams") |
| `team_nick` | text | Nickname (e.g., "Rams") |
| `team_conf` | text | AFC or NFC |
| `team_division` | text | e.g., "NFC West" |
| `team_color` | text | Primary hex color |
| `team_color2` | text | Secondary hex color |
| `team_color3` | text | Tertiary hex color |
| `team_color4` | text | Quaternary hex color |
| `team_logo_wikipedia` | text | Wikipedia logo URL |
| `team_logo_espn` | text | ESPN logo URL |
| `team_wordmark` | text | Team wordmark image URL |
| `team_conference_logo` | text | Conference logo URL |
| `team_league_logo` | text | League logo URL |
| `team_logo_squared` | text | Squared logo URL |
| `team_id` | text | nflreadr numeric team ID |

No FK from `players.latest_team` — too rigid for FA/NULL/historical values.

#### `team_game_stats`

One row per team per regular-season game (2016-2025). PPR variants are **Postgres generated columns** — auto-computed from standard FP + receptions. Apps just SELECT the column they need.

| Column | Type | Notes |
|--------|------|-------|
| `game_id` | text | nflreadr format: "2024_01_KC_BAL" |
| `season` | integer | |
| `week` | integer | |
| `team` | text | Current nflreadr abbreviation (OAK→LV, SD→LAC) |
| `opponent` | text | |
| `location` | text | 'home' or 'away' |
| `team_score` | integer | |
| `opp_score` | integer | |
| `spread` | numeric | Standard convention: negative = this team favored |
| `total_line` | numeric | Over/under |
| `implied_total` | numeric | Vegas-implied team scoring total |
| `pass_att`..`rec_td` | integer | Raw team offensive stats (12 columns) |
| `off_pass_fp` | numeric | Passing FP (same across all scoring formats) |
| `off_rush_fp` | numeric | Rushing FP (same across all scoring formats) |
| `off_recv_fp` | numeric | Receiving FP (standard, no reception bonus) |
| `off_total_fp` | numeric | Total team FP (standard) |
| `off_recv_fp_hppr` | numeric | **Generated**: `off_recv_fp + 0.5 * receptions` |
| `off_recv_fp_ppr` | numeric | **Generated**: `off_recv_fp + 1.0 * receptions` |
| `off_total_fp_hppr` | numeric | **Generated**: `off_total_fp + 0.5 * receptions` |
| `off_total_fp_ppr` | numeric | **Generated**: `off_total_fp + 1.0 * receptions` |
| `qb_fp`, `qb_rec` | numeric, integer | QB standard FP + receptions |
| `rb_fp`, `rb_rec` | numeric, integer | RB standard FP + receptions |
| `wr_fp`, `wr_rec` | numeric, integer | WR standard FP + receptions |
| `te_fp`, `te_rec` | numeric, integer | TE standard FP + receptions |
| `{pos}_fp_hppr` | numeric | **Generated**: `{pos}_fp + 0.5 * {pos}_rec` (×4 positions) |
| `{pos}_fp_ppr` | numeric | **Generated**: `{pos}_fp + 1.0 * {pos}_rec` (×4 positions) |
| `def_pass_fp`..`def_total_fp` | numeric | Defensive FP allowed by category (standard) |
| `def_receptions` | integer | Opponent receptions (for generating PPR) |
| `def_recv_fp_hppr/ppr` | numeric | **Generated**: defensive receiving PPR variants |
| `def_total_fp_hppr/ppr` | numeric | **Generated**: defensive total PPR variants |
| `def_{pos}_fp`, `def_{pos}_rec` | numeric, integer | Defensive positional FP allowed (×4 positions) |
| `def_{pos}_fp_hppr/ppr` | numeric | **Generated**: defensive positional PPR variants (×4 positions) |
| PK | | (game_id, team) |

**Scoring formula**: `half_ppr = standard + 0.5 × receptions`, `full_ppr = standard + 1.0 × receptions`. Passing and rushing FP are identical across all formats — only receiving (and totals) change.

#### `player_stats`

One row per player per regular-season week (2016-2025). PPR variants are **Postgres generated columns**. Loaded from nflreadr `load_player_stats()` via gsis_id → sportradar_id mapping.

| Column | Type | Notes |
|--------|------|-------|
| `player_id` | text | FK → players, part of PK |
| `season` | integer | Part of PK |
| `week` | integer | Part of PK |
| `team` | text | NFL team abbreviation for that game |
| `position` | text | Position group (QB, RB, WR, TE) |
| `opponent` | text | Opponent team abbreviation |
| `pass_att`..`pass_2pt` | integer | 12 passing stat columns |
| `rush_att`..`rush_2pt` | integer | 6 rushing stat columns |
| `targets`..`rec_2pt` | integer | 9 receiving stat columns |
| `special_teams_tds` | integer | Special teams touchdowns |
| `fantasy_points` | numeric | Standard scoring (no reception bonus) |
| `fantasy_points_hppr` | numeric | **Generated**: `fantasy_points + 0.5 * receptions` |
| `fantasy_points_ppr` | numeric | **Generated**: `fantasy_points + 1.0 * receptions` |
| PK | | (player_id, season, week) |

Only includes players that exist in the `players` table (FK enforced). ~59K rows from 1,758 DB players out of ~166K total nflreadr rows.

#### `player_seasons`
| Column | Type | Notes |
|--------|------|-------|
| `player_id` | text | FK → players |
| `year` | integer | |
| `team` | text | NFL team abbreviation for that season |
| PK | | (player_id, year) |

#### `player_notes`
| Column | Type | Notes |
|--------|------|-------|
| `player_id` | text PK | FK → players |
| `writeup` | text NOT NULL | Dynasty-focused player writeup (neutral tone, no buy/sell tips) |
| `updated_at` | timestamptz | Defaults to now() |

314 players with writeups (all players with dynasty value >= 2). Managed via `data/writeups/player_writeups.yaml` → `push_writeups.py`. Full replace on each push.

#### `fbg_bowl_leagues`
| Column | Type | Notes |
|--------|------|-------|
| `id` | bigserial PK | Internal ID |
| `sleeper_id` | text UNIQUE | Sleeper league ID |
| `year` | integer | Season year (2024, 2025) |
| `name` | text | League name from Sleeper |
| `scoring_type` | text | 'ppr', 'half_ppr', or 'standard' (from `scoring_settings.rec`) |
| `roster_count` | integer | Number of teams (usually 12) |

#### `fbg_bowl_rosters`
| Column | Type | Notes |
|--------|------|-------|
| `id` | bigserial PK | Internal ID (used as FK throughout) |
| `league_id` | bigint | FK → fbg_bowl_leagues |
| `sleeper_user_id` | text | Sleeper user ID (owner) |
| `sleeper_roster_id` | integer | Sleeper roster number within league |
| `display_name` | text | Sleeper display_name (username) |
| `team_name` | text | Custom team name (from user metadata, nullable) |

#### `fbg_bowl_weekly_results`
One row per roster per week (weeks 1–14). Raw weekly W/L/points.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigserial PK | |
| `roster_id` | bigint | FK → fbg_bowl_rosters |
| `league_id` | bigint | FK → fbg_bowl_leagues |
| `week` | integer | 1–14 |
| `pts_for` | numeric(8,2) | Points scored (`fpts + fpts_decimal/100`) |
| `pts_against` | numeric(8,2) | Opponent points |
| `win` / `loss` / `tie` | boolean | Matchup outcome |

#### `fbg_bowl_standings`
Cumulative standings after each week (pre-computed for query speed). One row per roster per week.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigserial PK | |
| `roster_id` | bigint | FK → fbg_bowl_rosters. UNIQUE with week |
| `league_id` | bigint | FK → fbg_bowl_leagues |
| `week` | integer | 1–14 |
| `wins` / `losses` | integer | Cumulative through this week |
| `pts_for` / `pts_against` | numeric(10,2) | Cumulative points |
| `league_rank` | integer | Rank within league (set only on week 14) |
| `qualified_playoffs` | boolean | True if league_rank ≤ 2 OR pts_for ≥ 1920 (set only on week 14) |

#### `fbg_bowl_playoff_results`
One row per roster per playoff week (15, 16, 17). Only qualified teams included.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigserial PK | |
| `roster_id` | bigint | FK → fbg_bowl_rosters |
| `league_id` | bigint | FK → fbg_bowl_leagues |
| `week` | integer | 15, 16, or 17 |
| `pts_for` | numeric(8,2) | Points scored that week |
| `final_rank` | integer | Not currently used (NULL) |

#### `fbg_bowl_draft_picks`
One row per pick per league. No FK to `players` — join via `sleeper_player_id = players.sleeper_id`.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigserial PK | |
| `league_id` | bigint | FK → fbg_bowl_leagues |
| `roster_id` | bigint | FK → fbg_bowl_rosters |
| `sleeper_draft_id` | text | Sleeper draft ID |
| `pick_no` | integer | Overall pick number |
| `draft_slot` | integer | Draft position slot |
| `sleeper_player_id` | text | For DEF: team abbreviation ('PHI', 'LAR'). For players: numeric Sleeper ID |
| `first_name` / `last_name` | text | From Sleeper draft pick metadata |
| `position` | text | QB, RB, WR, TE, DEF |

93.3% of `sleeper_player_id` values match `players.sleeper_id` (472/506 unique IDs). 32 DEF picks link to synthetic `DEF_*` player records. 34 fringe players have no match.

#### `fbg_bowl_scores`
One row per roster per year. Final meta-scores for ranking.

| Column | Type | Notes |
|--------|------|-------|
| `id` | bigserial PK | |
| `roster_id` | bigint | FK → fbg_bowl_rosters |
| `year` | integer | Season year |
| `reg_season_wins` | integer | Wins through week 14 |
| `league_rank_bonus` | integer | 35 (1st) / 10 (2nd) / 0 |
| `semi_bonus` | integer | 35 if scored in week 16, else 0 |
| `finals_bonus` | integer | 35 if scored in week 17, else 0 |
| `top10_bonus` | integer | 300/200/150/125/100/85/70/55/45/35 for ranks 1–10 |
| `total_score` | integer | Sum of all bonuses + wins |
| `overall_rank` | integer | Rank among all participants that year |

#### `sleeper_leagues`
Sleeper dynasty leagues (2026 season). Subset of all leagues — only dynasty with `pick_trading` enabled.

| Column | Type | Notes |
|--------|------|-------|
| `league_id` | text PK | Sleeper league ID |
| `season` | int | Season year (2026) |
| `name` | text | League name |
| `total_rosters` | int | Number of teams (usually 10-14) |
| `status` | text | e.g., "pre_draft", "in_season" |
| `is_dynasty` | boolean | Always true (filtered on upload) |
| `is_superflex` | boolean | Has superflex starter slot |
| `is_tep` | boolean | TE premium (bonus PPR for TE) |
| `is_idp` | boolean | IDP league |
| `ppr_type` | text | 'full', 'half', 'standard' |
| `rec_ppr` | numeric | PPR value (1.0, 0.5, 0) |
| `te_premium` | numeric | TE bonus PPR (e.g., 1.5 for TEP) |
| `pass_td_pts` | numeric | Points per passing TD (4 or 6) |
| `starter_qb` | int | Number of QB starter slots |
| `starter_rb` | int | RB starter slots |
| `starter_wr` | int | WR starter slots |
| `starter_te` | int | TE starter slots |
| `starter_flex` | int | FLEX slots |
| `starter_super_flex` | int | Superflex slots (0 or 1) |
| `bench_count` | int | Bench roster spots |
| `taxi_slots` | int | Taxi squad spots |
| `draft_rounds` | int | Rookie draft rounds |
| `pick_trading` | boolean | Pick trading enabled |

12,629 rows. Uploaded from `~/dev/sleeper-scrape/sleeper.db` via `upload_leagues.py`.

#### `sleeper_trades`
Completed trades from dynasty leagues. One row per trade transaction.

| Column | Type | Notes |
|--------|------|-------|
| `transaction_id` | text PK | Sleeper transaction ID |
| `league_id` | text | FK → sleeper_leagues |
| `season` | int | Season year |
| `week` | int | NFL week when trade occurred |
| `created_ms` | bigint | Unix timestamp in milliseconds |
| `roster_ids` | jsonb | Array of roster IDs involved (e.g., [5, 7]) |
| `consenter_ids` | jsonb | Array of roster IDs who approved |

15,509 rows from 3,972 leagues. Date range: Dec 2025 – Mar 2026.

#### `sleeper_trade_assets`
Individual assets (players and draft picks) within each trade. Multiple rows per trade.

| Column | Type | Notes |
|--------|------|-------|
| `id` | serial PK | Auto-increment |
| `transaction_id` | text | FK → sleeper_trades |
| `receiving_roster_id` | int | Roster that received this asset |
| `asset_type` | text | 'player' or 'pick' |
| `sleeper_player_id` | text | Sleeper player ID (NULL for picks) |
| `pick_season` | int | Draft pick year (NULL for players) |
| `pick_round` | int | Draft pick round (NULL for players) |
| `pick_original_roster_id` | int | Roster that originally owns the pick (NULL for players) |
| `pick_slot` | int | Draft slot position (nullable) |

66,927 rows: 26,682 player assets + 40,245 pick assets. Join players via `sleeper_player_id = players.sleeper_id`.

#### `news_items`
News and intel items for the FF Intel System. Pipeline: draft → approved → published. Anon can only read published items.

| Column | Type | Notes |
|--------|------|-------|
| `id` | uuid PK | Default `gen_random_uuid()` |
| `player_id` | text NOT NULL | FK → players |
| `team_abbr` | text | NFL team abbreviation (nullable) |
| `source_url` | text UNIQUE | Deduplication key |
| `source_type` | text | CHECK: 'tweet', 'article', 'video', 'podcast', 'other' |
| `news_type` | text | CHECK: 'injury', 'depth_chart', 'trade', 'contract', 'scheme', 'coaching', 'offseason', 'performance', 'other' |
| `headline` | text NOT NULL | Short headline |
| `summary` | text | Longer summary (nullable) |
| `raw_content` | text | Original source content (nullable) |
| `importance` | int | 1–10 scale, CHECK constraint |
| `importance_note` | text | Why this importance level (nullable) |
| `published_at` | timestamptz | When originally published at source |
| `approved_at` | timestamptz | When approved for display |
| `status` | text NOT NULL | Default 'draft'. CHECK: 'draft', 'approved', 'published' |
| `cascade_flags` | jsonb | Default `'[]'::jsonb` — downstream update triggers |
| `created_at` | timestamptz NOT NULL | Default `now()` — when item entered pipeline |

### Views

#### `view_draft_board`
Pre-joined view for the draft board app.

```sql
SELECT dp.league_id, dp.round, dp.pick_in_round, dp.overall_pick, dp.year,
       p.first_name, p.last_name, p.position, p.latest_team, p.headshot_url,
       lt.team_id, lt.draft_order, lt.league_rank, lt.league_points,
       COALESCE(ps.team, p.latest_team) AS team
FROM draft_picks dp
JOIN players p ON dp.player_id = p.player_id
JOIN league_teams lt ON dp.league_id = lt.league_id AND dp.team_id = lt.team_id
LEFT JOIN player_seasons ps ON dp.player_id = ps.player_id AND dp.year = ps.year;
```

#### `team_season_stats`
View aggregating `team_game_stats` by (team, season). Per-game averages and standard deviations for all scoring formats. ~320 rows (32 teams × 10 seasons). Uses `security_invoker = true`.

#### `player_season_stats`
View aggregating `player_stats` by (player_id, team, season). Joins to `players` for first/last name. Includes season stat totals, FPG in all 3 scoring formats, and week-to-week SD. Groups by team so traded players get separate rows per team. Uses `security_invoker = true`.

Key columns: `player_id`, `first_name`, `last_name`, `position`, `team`, `season`, `games`, `pass_yds`, `pass_td`, `rush_yds`, `rush_td`, `receptions`, `rec_yds`, `rec_td`, `fantasy_points`/`_hppr`/`_ppr`, `fpg`/`_hppr`/`_ppr`, `fp_sd`/`_hppr`

Key columns: `games`, `off_total_fpg`/`_hppr`/`_ppr`, `off_pass_fpg`, `off_rush_fpg`, `off_recv_fpg`/`_hppr`/`_ppr`, `off_total_sd`, `qb_fpg`/`_hppr`/`_ppr`, `rb_fpg`/`_hppr`/`_ppr`, `wr_fpg`/`_hppr`/`_ppr`, `te_fpg`/`_hppr`/`_ppr`, `def_total_fpg`/`_hppr`/`_ppr`, `def_{pos}_fpg`/`_hppr`/`_ppr`, `off_*_sd`, `def_*_sd`

### Indexes

| Index | Table | Columns |
|-------|-------|---------|
| `idx_draft_picks_league_id` | draft_picks | (league_id) |
| `idx_draft_picks_player_id` | draft_picks | (player_id) |
| `idx_draft_picks_year` | draft_picks | (year) |
| `idx_draft_picks_league_team` | draft_picks | (league_id, team_id) |
| `idx_leagues_year` | leagues | (year) |
| `idx_players_position` | players | (position) |
| `idx_adp_player_id` | adp | (player_id) |
| `idx_adp_sources_date` | adp_sources | (date DESC) |
| `idx_adp_sources_source_year_date` | adp_sources | (source, year, date DESC) |
| `idx_players_dan_id` | players | (dan_id) WHERE dan_id IS NOT NULL (unique partial) |
| `idx_dynasty_values_updated` | dynasty_values | (updated_at) |
| `idx_player_projections_source_year` | player_projections | (source, year) |
| `idx_tgs_team_season` | team_game_stats | (team, season) |
| `idx_tgs_season_week` | team_game_stats | (season, week) |
| `idx_tgs_opponent_season` | team_game_stats | (opponent, season) |
| `idx_ps_season_week` | player_stats | (season, week) |
| `idx_ps_team_season` | player_stats | (team, season) |
| `idx_ps_position_season` | player_stats | (position, season) |
| `idx_sleeper_trades_league` | sleeper_trades | (league_id) |
| `idx_sleeper_trades_season_week` | sleeper_trades | (season, week) |
| `idx_sleeper_trade_assets_txn` | sleeper_trade_assets | (transaction_id) |
| `idx_sleeper_trade_assets_player` | sleeper_trade_assets | (sleeper_player_id) WHERE asset_type = 'player' |
| `idx_news_items_player_id` | news_items | (player_id) |
| `idx_news_items_status` | news_items | (status) |
| `idx_news_items_approved_at` | news_items | (approved_at DESC) WHERE status IN ('approved', 'published') |

### RLS

All tables: RLS enabled. Policies:
- **SELECT**: Public (anon can read all tables). Exception: `news_items` — anon can only read rows with `status = 'published'`
- **INSERT**: Only `adp_sources` allows anon insert. `dynasty_values` does NOT (writes via service role key from Apps Script)
- **All other writes**: Use `SUPABASE_SERVICE_ROLE_KEY` (bypasses RLS)

**Note:** The `postgres` role is subject to RLS. Use `pg_stat_user_tables.n_live_tup` for row counts, or connect via the REST API with the anon/service key.

### Migrations Applied

1. `create_tables` — All tables with PKs and FKs
2. `create_indexes` — Custom indexes
3. `fix_pick_duration_type` — smallint → integer
4. `add_missing_fk_index` — draft_picks(league_id, team_id)
5. `enable_rls_with_read_policy` — RLS + public SELECT
6. `create_view_draft_board` — Pre-joined view
7. `create_player_seasons` — Player-season-team table
8. `update_view_draft_board_season_team` — Add COALESCE team logic to view
9. `fix_view_security_invoker` — Security invoker on view_draft_board
10. `drop_permissive_anon_write_policies` — Removed anon INSERT/UPDATE from main tables

11. `add_dan_id_to_players` — dan_id column + unique partial index on players
12. `create_dynasty_values` — Dynasty values table with RLS (anon SELECT only)
13. `add_height_weight_to_players` — height (text) and weight (integer) columns on players
14. `add_date_to_adp_sources` — date column + new PK (player_id, source, year, date) for daily tracking
15. `create_player_projections` — Season projections table with RLS (anon SELECT only)
16. `create_teams` — 32-team reference table (abbr, name, conf, division, colors, logos) with RLS
17. `normalize_team_abbreviations` — UPDATE players SET latest_team = 'LA' WHERE latest_team = 'LAR'
18. `create_team_game_stats` — Team game stats with 24 generated columns for PPR variants + RLS + indexes
19. `create_team_season_stats_view` — Season-level aggregates view (security_invoker)
20. `create_player_stats` — Player weekly stats with 2 generated PPR columns + RLS + indexes
21. `create_player_season_stats_view` — Player season aggregates view (security_invoker)
22. `create_dynasty_value_history` — Change log table for dynasty value changes + RLS + date index
23. `create_dynasty_pick_values` — Future draft pick trade values (2027-2028, 4 rounds × 4 tiers) + RLS
24. `create_positional_model_tables` — Coefficients + baselines for positional value adjustment model + RLS
25. `create_colleges` — College reference table (738 schools with logos, mascots, colors, conferences) + RLS
26. `create_player_notes` — Player writeup table (PK player_id, FK → players) + RLS (anon SELECT only)
27. `create_fbg_bowl_tables` — 7 FBG Bowl tables (leagues, rosters, weekly_results, standings, playoff_results, draft_picks, scores) + RLS + indexes. Applied via pg8000 (Management API blocked by Cloudflare).
28. `create_sleeper_trade_tables` — 3 Sleeper trade tables (sleeper_leagues, sleeper_trades, sleeper_trade_assets) + 4 indexes + RLS. Applied via pg8000.
29. `create_news_items` — News/intel pipeline table with 3 indexes + RLS (anon SELECT published only, service role full access)

Applied via direct SQL (not tracked in migration system):
- College name normalization — UPDATE players: Mississippi→Ole Miss, North Carolina State→NC State, Pittsburg→Pittsburgh, Virgina Tech→Virginia Tech, Miami (FL)→Miami, Brigham Young→BYU

Applied via direct SQL (not tracked in migration system, pre-existing):
- Player ID columns — 18 new ID columns on players table
- `adp_sources` table — Multi-source ADP table with RLS (SELECT + INSERT for anon)

## Data Row Counts

| Table | Rows |
|-------|------|
| `players` | 1,790 (1,758 original + 32 DEF records) |
| `leagues` | 2,629 |
| `league_teams` | 31,548 |
| `adp` | 5,339 |
| `draft_picks` | 618,856 |
| `player_seasons` | 6,060 |
| `adp_sources` | ~2,000+ (growing daily) |
| `dynasty_values` | 714 |
| `player_projections` | 443 |
| `teams` | 32 |
| `team_game_stats` | 5,278 |
| `player_stats` | 59,328 |
| `dynasty_value_history` | 706 |
| `dynasty_pick_values` | 32 |
| `positional_model_coefficients` | 4 |
| `positional_model_baselines` | 2 |
| `colleges` | 738 |
| `player_notes` | 314 |
| `fbg_bowl_leagues` | 576 (417 for 2025, 159 for 2024) |
| `fbg_bowl_rosters` | 6,900 (5,004 for 2025, 1,896 for 2024) |
| `fbg_bowl_weekly_results` | 96,600 (70,056 for 2025, 26,544 for 2024) |
| `fbg_bowl_standings` | 96,600 (70,056 for 2025, 26,544 for 2024) |
| `fbg_bowl_playoff_results` | 6,645 (4,371 for 2025, 2,274 for 2024) |
| `fbg_bowl_draft_picks` | 138,000 (100,080 for 2025, 37,920 for 2024) |
| `fbg_bowl_scores` | 6,900 (5,004 for 2025, 1,896 for 2024) |
| `sleeper_leagues` | 12,629 (2026 dynasty leagues) |
| `sleeper_trades` | 15,509 (from 3,972 leagues, Dec 2025 – Mar 2026) |
| `sleeper_trade_assets` | 66,927 (26,682 players + 40,245 picks) |
| `news_items` | 0 (new, empty) |

### Player ID Coverage (1,758 players)

| ID Column | Count | Coverage |
|-----------|-------|----------|
| `sleeper_id` | 1,656 | 94.7% |
| `rotowire_id` | 1,605 | 91.8% |
| `stats_id` | 1,585 | 90.6% |
| `espn_id` | 1,573 | 89.9% |
| `mfl_id` | 1,548 | 88.5% |
| `stats_global_id` | 1,548 | 88.5% |
| `cbs_id` | 1,539 | 88.0% |
| `gsis_id` | 1,532 | 87.6% |
| `fantasypros_id` | 1,515 | 86.6% |
| `pff_id` | 1,512 | 86.4% |
| `pfr_id` | 1,503 | 85.9% |
| `fantasy_data_id` | 1,497 | 85.6% |
| `yahoo_id` | 1,486 | 85.0% |
| `draftkings_id` | 1,170 | 66.9% |
| `drafters_id` | 1,121 | 64.1% |
| `sportsdata_id` | 1,113 | 63.6% |
| `swish_id` | 1,111 | 63.5% |
| `fanduel_id` | 1,104 | 63.1% |
| `underdog_id` | 1,034 | 59.1% |
| `footballguys_id` | 1,021 | 58.4% |
| `cfbref_id` | 878 | 50.2% |
| `rotoworld_id` | 878 | 50.2% |
| `ktc_id` | 455 | 26.0% |
| `fleaflicker_id` | 70 | 4.0% |

### ADP Sources Coverage

| Source | Year | Dates Tracked | Latest Row Count |
|--------|------|---------------|-----------------|
| `underdog` | 2026 | daily since Feb 16 | ~2,000+ (growing daily) |
| `drafters` | 2026 | daily since Feb 26 | growing daily. ADP in round.pick float format (e.g. 1.089). JWT auth — update `DRAFTERS_JWT` in `.env` when 401 appears in log. |
| `draftkings` | 2026 | daily since Feb 27 | ~312/snapshot, 0 unmatched. ADP in overall pick float format. Fully automated via Playwright: `setup_dk_session.py` (one-time per ~2 weeks, reads Chrome Profile 2 cookies) → `fetch_draftkings_adp.py` (daily, Playwright Chromium loads DK page to get fresh `jwe`, then hits API). Re-run setup when 401 appears in `draftkings_adp.log`. |

## Supabase Storage

### Buckets

| Bucket | Public | Purpose |
|--------|--------|---------|
| `headshots` | Yes | Player headshot images (uploaded via `upload_rookie_headshots.py`) |
| `assets` | Yes | Shared icons, logos, and badges for use across apps |

### Assets (`assets/icons/`)

Base URL: `https://twfzcrodldvhpfaykasj.supabase.co/storage/v1/object/public/assets/icons/`

Each icon is available in SVG (`svg/{name}.svg`) and PNG at two sizes (`png/{name}@128.png`, `png/{name}@256.png`).

| Icon | File name | Description |
|------|-----------|-------------|
| Rookie | `rookie` | Gold shield with serif "R" — first/second-year players |
| Cornerstone | `cornerstone` | Gold shield with star — top-tier dynasty assets |
| Rising | `rising` | Green circle with up arrow — value trending up |
| Falling | `falling` | Red circle with down arrow — value trending down |
| Injury | `injury` | Red circle with medical cross — injured/IR |
| Free Agent | `free-agent` | Gray circle with "FA" — unsigned players |
| Draft Pick | `draft-pick` | Purple card with star — future draft picks |
| QB | `pos-qb` | Red circle — quarterback |
| RB | `pos-rb` | Blue circle — running back |
| WR | `pos-wr` | Green circle — wide receiver |
| TE | `pos-te` | Orange circle — tight end |

**Usage example**: `{BASE_URL}/svg/rookie.svg` or `{BASE_URL}/png/pos-qb@256.png`

### Logos (`assets/logos/`)

| File | Description |
|------|-------------|
| `fbg-mascot.png` | Footballguys "Black-Eyed Joe" mascot (hi-res) |
| `fbg-logo-black.png` | Footballguys logo (black, for light backgrounds) |
| `fbg-logo-white.png` | Footballguys logo (white, for dark backgrounds) |

## Data Import Files

Located in `data/imports/` (git-ignored):
- `underdog_ADP.csv` — Underdog Fantasy Early Best Ball rankings (1,372 players, 1,034 matched to DB)
- `DkPreDraftRankings.csv` — DraftKings pre-draft rankings (1,472 players)
- `drafters_players.csv` — Drafters platform player list (1,999 players)
- `fbg_crosswalk.csv` — FBG ID → SportsDataIO ID mapping (1,867 rows)
- `dynasty_values.csv` — Exported Google Sheet for bootstrapping dan_id + initial dynasty values
- `rookie_birthdates_2026.csv` — 77 rookie birthdates from DLF devy age table (manually compiled)
- `sportsdata_rookies_2026.json` — Cached SportsData.io Rookies/2026 API response (407 rookies)

## DFS Analysis Findings (from exploration.Rmd)

### Prediction Models (Steps 11-16)
- **Vegas implied total** is the best single predictor of team total FP (R² ≈ 0.19 on test). Rolling averages add nothing once Vegas is included.
- **Two-step model** is best for category prediction: (1) predict total from implied_total, (2) predict category shares from 8-game rolling averages, (3) multiply.
- **Rolling window sweet spot**: 5-10 games. Biggest marginal R² gains in first 3-5 games, diminishing returns after ~8.
- **Rushing** is the category where opponent defensive rolling avg adds most independent signal beyond Vegas.
- Even best models explain ~13% of category variance — individual games are inherently noisy.

### Year-over-Year Correlations (Steps 9-10)
- Offense (r=0.40) stickier than defense (r=0.26)
- By category: Passing r=0.43, Rushing r=0.44, Receiving r=0.45
- 2026 baselines projected using regression toward the mean: `proj = lg_mean + r × (team_2025 - lg_mean)`

### Variance / Simulation Parameters (Steps 17-20)
- **~85-90% of weekly variance is noise**, not team quality (ICC ≈ 0.10-0.15)
- **Team-level variance is NOT persistent** — a boom/bust team doesn't stay boom/bust (low YoY SD correlation)
- **Use a single league-wide noise SD**, not team-specific
- **Category correlation structure**: passing & receiving deviate together (both driven by pass volume); rushing substitutes for passing (negative/weak correlation)
- **Weekly residuals are approximately independent** (lag-1 autocorrelation ≈ 0) → season SD ≈ weekly SD × √17
- **Simulation recipe**: (1) set team baseline from regression projections, (2) each week adjust for Vegas, (3) draw correlated (pass/rush/recv) noise from multivariate normal, (4) sum 17 weeks, (5) repeat 10,000+ times

## Planned Future Tables

### Team Tables (Phase 3)
- `schedules` — Game schedule with scores, spreads, totals (standalone, beyond what's in team_game_stats)
- `team_projections` — FBG/SportsData team projections

### Other Planned Data
- 2026 season simulation engine

## Pre-Draft Prospect Add Process

When a high-value prospect emerges mid-season (combine, pro days, pre-draft hype), add them manually:

1. **Check Sleeper** — `GET https://api.sleeper.app/v1/players/nfl`, search by name → grab `sleeper_id`, `draftkings_id`, `underdog_id`, `drafters_id`
2. **Check DK rankings** — `data/imports/DkPreDraftRankings.csv` or fetch live from `fetch_draftkings_adp.py` run — grab `draftkings_id` if not on Sleeper
3. **Determine `player_id`**: use Underdog UUID if present; otherwise generate a UUID (`python3 -c "import uuid; print(uuid.uuid4())"`)
4. **Insert player** via REST API (use service role key). Insert **individually** (not batch) since field sets vary. Minimum fields: `player_id`, `first_name`, `last_name`, `position`
5. **PATCH IDs** individually: `draftkings_id`, `sleeper_id`, `underdog_id`, `drafters_id` as available
6. **Assign `dan_id`** — add to Google Sheet with next sequential ID (format: `2026NNN`), push dynasty values from Sheet → Supabase via Apps Script
7. **Verify** — run `fetch_draftkings_adp.py --dry-run` to confirm new player is now matched

**Note**: Pre-draft prospects won't have Sportradar IDs, gsis_id, or most nflreadr IDs until they're drafted and appear in nflreadr. Those populate later via the normal ID matching pipeline.

**Examples added**:
- Feb 27, 2026 (combine): Brenen Thompson (dan_id=2026072, WR), Jeff Caldwell (dan_id=2026073, WR), Deion Burks (dan_id=2026074, WR).
- Mar 4, 2026 (pre-draft): Taylen Green (2026075, QB), Cole Payton (2026076, QB), Behren Morton (2026077, QB), Drew Allar (2026078, QB), Cade Klubnik (2026079, QB), Carson Beck (2026080, QB — already in DB, dan_id patched), Jamarion Miller (2026081, RB — "Jam Miller" on Sleeper/DK/Underdog), Robert Henry Jr. (2026082, RB — no Sleeper or Underdog ID).

## Key Gotchas

- NFFC player UUIDs = Sportradar IDs (`sportradar_id` in nflreadr's `load_ff_playerids()`)
- Python `urllib` gets 403 from `nfc.shgn.com` — must set User-Agent header
- API `pick` field is already overall pick (1-240), NOT within-round
- `game_style_id` values change each year; not in historical data — filter by league name
- 2018 has no team outcomes (league_rank etc.); 2025 has no outcomes yet
- ~1.4% of draft picks have empty player_id (NFFC API bug)
- `pick_duration` can exceed 32K — needs integer, not smallint
- Some `birth_date` values are "0000-00-00" — treated as NULL
- Supabase Management API has tight rate limits (~2 req/min) — prefer REST API for bulk operations
- Supabase MCP token expires; can bypass with direct Postgres via RPostgres (R) using `SUPABASE_DB_PASSWORD`
- REST API batch POSTs require all objects to have identical keys — insert individually for variable schemas
- FBG API player IDs are abbreviated name+year codes, not numeric — need crosswalk to match
- Pre-draft prospects / rookies not in nflreadr use Underdog UUID as player_id, or a generated UUID if no Underdog UUID exists — see Pre-Draft Prospect Add Process above
- `/bin/bash` cannot access `~/dev/` files under launchd — use `/usr/bin/python3 -c` inline Python for all plists in this repo
- Sleeper API requires no auth, returns ~5MB — call sparingly (once/day). Best source for sleeper_id + cross-referencing sportradar_ids
- SportsData.io Rookies/{season} endpoint is best source for pre-draft rookie IDs
- Use `python3` not `python` on this Mac
- nflreadr `spread_line`: positive = home team favored (NOT standard betting convention)
- nflreadr `clean_homeaway()` does NOT transform `spread_line` — same value for both rows
- ggplot `scale_color_manual` labels: use NAMED vector to avoid alphabetical ordering bug
- nflreadr uses `LA` for the Rams (not `LAR`). Our `normalize_team()` in shared.py maps LAR→LA. All `players.latest_team` values now use nflreadr standard abbreviations
- `team_game_stats` PPR columns are Postgres generated columns — do NOT include them in INSERT/POST payloads. The Python loader excludes them via `GENERATED_COLS` set
- `team_game_stats.spread` uses standard betting convention (negative = favored). nflreadr's `spread_line` uses positive = home favored — the R script converts
- `team_game_stats` historical teams normalized to current abbreviations (OAK→LV, SD→LAC). Use `team` column directly to query across years
- NFFC API `"number"` field counts drafts across ALL contest types, not just Rotowire OC — `build_clean_dataset.py` recalculates `times_drafted` from actual `draft_picks`
- Supabase REST API silently caps results at 1000 rows even with `limit=2000` — use `Prefer: count=exact` header + `content-range` for accurate counts
- Duplicate player records can exist when same player has both sportradar UUID and Underdog UUID — merge by moving FK references before deleting
- 32 DEF records added to `players` table with `player_id = 'DEF_{ABBR}'` (e.g., `DEF_PHI`, `DEF_LAR`). `sleeper_id` = Sleeper abbreviation (LAR for Rams, matching Sleeper's format). `latest_team` uses normalized abbr (LA for Rams). Allows joining `fbg_bowl_draft_picks.sleeper_player_id = players.sleeper_id` for all positions
- FBG Bowl draft picks: 93.3% match to players table (472/506 unique IDs). 34 unmatched are fringe/practice-squad players not worth adding
