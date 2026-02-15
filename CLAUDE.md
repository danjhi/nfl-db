# CLAUDE.md

## Project Overview

NFL Database — central repo for managing the Supabase database used across all NFL and fantasy football projects. Contains data pipelines, loading scripts, and schema documentation. No app code lives here.

## Supabase

- **Project ref:** `twfzcrodldvhpfaykasj`
- **URL:** `https://twfzcrodldvhpfaykasj.supabase.co`
- **Auth:** `.env` contains `SUPABASE_ANON_KEY`, `SUPABASE_ACCESS_TOKEN` (PAT), `NFFC_API_KEY`, `SPORTSDATA_API_KEY`, `FBG_API_KEY`

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
| `build_clean_dataset.py` | Filter to Rotowire OC, enrich via nflreadr, output CSVs to `data/clean/` |
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
```

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
| `adp` | numeric | Average draft position |
| `min_pick` | integer | Earliest pick |
| `max_pick` | integer | Latest pick |
| `times_drafted` | integer | |
| PK | | (player_id, year) |

#### `player_seasons`
| Column | Type | Notes |
|--------|------|-------|
| `player_id` | text | FK → players |
| `year` | integer | |
| `team` | text | NFL team abbreviation for that season |
| PK | | (player_id, year) |

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

### RLS

All tables: RLS enabled. Policies:
- **SELECT**: Public (anon can read all tables)
- **INSERT**: Anon can insert into all tables
- **UPDATE**: Anon can update players table

### Migrations Applied

1. `create_tables` — All tables with PKs and FKs
2. `create_indexes` — Custom indexes
3. `fix_pick_duration_type` — smallint → integer
4. `add_missing_fk_index` — draft_picks(league_id, team_id)
5. `enable_rls_with_read_policy` — RLS + public SELECT
6. `create_view_draft_board` — Pre-joined view
7. `create_player_seasons` — Player-season-team table
8. `update_view_draft_board_season_team` — Add COALESCE team logic to view
9. `add_player_id_columns` — 18 new ID columns on players table
10. `add_write_rls_policies` — INSERT/UPDATE policies for anon key

## Data Row Counts

| Table | Rows |
|-------|------|
| `players` | 1,753 |
| `leagues` | 2,629 |
| `league_teams` | 31,548 |
| `adp` | 5,339 |
| `draft_picks` | 621,356 |
| `player_seasons` | 6,060 |

### Player ID Coverage (1,753 players)

| ID Column | Count | Coverage |
|-----------|-------|----------|
| `sleeper_id` | 1,656 | 94.5% |
| `rotowire_id` | 1,605 | 91.6% |
| `stats_id` | 1,585 | 90.4% |
| `espn_id` | 1,573 | 89.7% |
| `mfl_id` | 1,548 | 88.3% |
| `stats_global_id` | 1,548 | 88.3% |
| `cbs_id` | 1,539 | 87.8% |
| `gsis_id` | 1,532 | 87.4% |
| `fantasypros_id` | 1,515 | 86.4% |
| `pff_id` | 1,512 | 86.3% |
| `pfr_id` | 1,503 | 85.7% |
| `fantasy_data_id` | 1,497 | 85.4% |
| `yahoo_id` | 1,486 | 84.8% |
| `draftkings_id` | 1,173 | 66.9% |
| `drafters_id` | 1,123 | 64.1% |
| `sportsdata_id` | 1,115 | 63.6% |
| `swish_id` | 1,111 | 63.4% |
| `fanduel_id` | 1,106 | 63.1% |
| `underdog_id` | 1,034 | 59.0% |
| `footballguys_id` | 1,023 | 58.4% |
| `cfbref_id` | 878 | 50.1% |
| `rotoworld_id` | 878 | 50.1% |
| `ktc_id` | 455 | 26.0% |
| `fleaflicker_id` | 70 | 4.0% |

## Data Import Files

Located in `data/imports/` (git-ignored):
- `underdog_ADP.csv` — Underdog Fantasy Early Best Ball rankings (1,372 players)
- `DkPreDraftRankings.csv` — DraftKings pre-draft rankings (1,472 players)
- `drafters_players.csv` — Drafters platform player list (1,999 players)
- `fbg_crosswalk.csv` — FBG ID → SportsDataIO ID mapping (1,867 rows)

## Planned Future Tables

### Team Tables (Phase 3)
- `teams` — NFL team reference (abbr, name, conference, division, logos, colors)
- `schedules` — Game schedule with scores, spreads, totals
- `team_games` — Game-level team stats
- `team_seasons` — Season-level aggregates
- `team_projections` — FBG/SportsData team projections

### Player Enrichment (Phase 4)
- `player_stats` — Weekly/seasonal stats (from nflreadr)
- `player_projections` — Weekly projections (from FBG API, full season 2026)
- `adp_sources` — ADP from multiple sites (Underdog, DK, Drafters, NFFC)
- `dynasty_values` — User's dynasty valuations
- `player_notes` — Written content per player

### Other Planned Data
- Biographical enrichment (height, weight, photos from SportsData.io)
- Headshots (from SportsData.io `PhotoUrl`, `UsaTodayHeadshotNoBackgroundUrl`)
- Calculated values from past stats

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
- REST API batch POSTs require all objects to have identical keys — insert individually for variable schemas
- FBG API player IDs are abbreviated name+year codes, not numeric — need crosswalk to match
- Rookies not in nflreadr may use Underdog UUID as player_id
- Sleeper API requires no auth, returns ~5MB — call sparingly (once/day). Best source for sleeper_id + cross-referencing sportradar_ids
- SportsData.io Rookies/{season} endpoint is best source for pre-draft rookie IDs
