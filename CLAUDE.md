# CLAUDE.md

## Project Overview

NFL Database — central repo for managing the Supabase database used across all NFL and fantasy football projects. Contains data pipelines, loading scripts, schema documentation, and DFS analysis notebooks. No app code lives here.

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
```

### Daily Automation (launchd)

Both jobs use macOS launchd with bash wrappers. Date-gated to Feb 19 – Apr 22, 2026.

| Job | Plist | Schedule | Script |
|-----|-------|----------|--------|
| Underdog ADP | `~/Library/LaunchAgents/com.nfldb.daily-adp.plist` | 8:00 AM | `scripts/adp/run_daily_adp.sh` → `fetch_underdog_adp.py` |
| Team Refresh | `~/Library/LaunchAgents/com.nfldb.daily-team-refresh.plist` | 8:15 AM | `scripts/ids/run_daily_team_refresh.sh` → `refresh_player_teams.py` |

Logs: `data/logs/underdog_adp.log`, `data/logs/team_refresh.log`, `data/logs/team_refresh.jsonl`

**Important**: Plists must use `/bin/bash` as explicit interpreter (not just the script path) to avoid macOS `com.apple.provenance` blocking.

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
| `player_id` | text PK | Sportradar UUID (= NFFC player UUID), or Underdog UUID for rookies |
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

Only includes players that exist in the `players` table (FK enforced). ~59K rows from 1,748 DB players out of ~166K total nflreadr rows.

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

### RLS

All tables: RLS enabled. Policies:
- **SELECT**: Public (anon can read all tables)
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

Applied via direct SQL (not tracked in migration system):
- College name normalization — UPDATE players: Mississippi→Ole Miss, North Carolina State→NC State, Pittsburg→Pittsburgh, Virgina Tech→Virginia Tech, Miami (FL)→Miami, Brigham Young→BYU

Applied via direct SQL (not tracked in migration system, pre-existing):
- Player ID columns — 18 new ID columns on players table
- `adp_sources` table — Multi-source ADP table with RLS (SELECT + INSERT for anon)

## Data Row Counts

| Table | Rows |
|-------|------|
| `players` | 1,748 |
| `leagues` | 2,629 |
| `league_teams` | 31,548 |
| `adp` | 5,339 |
| `draft_picks` | 618,856 |
| `player_seasons` | 6,060 |
| `adp_sources` | ~2,000+ (growing daily) |
| `dynasty_values` | 704 |
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

### Player ID Coverage (1,748 players)

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
| `underdog` | 2026 | 3+ (daily since Feb 16) | ~2,000+ (growing daily) |

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
- DraftKings and Drafters ADP into `adp_sources`
- 2026 season simulation engine

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
- Rookies not in nflreadr may use Underdog UUID as player_id
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
