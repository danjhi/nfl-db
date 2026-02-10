# CLAUDE.md

## Project Overview

NFL Database — central repo for managing the Supabase database used across all NFL and fantasy football projects. Contains data pipelines, loading scripts, and schema documentation. No app code lives here.

## Supabase

- **Project ref:** `twfzcrodldvhpfaykasj`
- **URL:** `https://twfzcrodldvhpfaykasj.supabase.co`
- **Auth:** `.env` contains `SUPABASE_ANON_KEY`, `SUPABASE_ACCESS_TOKEN` (PAT), `NFFC_API_KEY`

## Scripts

Scripts are organized by data source.

### NFFC (`scripts/nffc/`)

| Script | Purpose |
|--------|---------|
| `pull_draft_results.py` | Pull raw NFFC API data (all contest types, 2018-2025) into `data/raw/` |
| `build_clean_dataset.py` | Filter to Rotowire OC, enrich via nflreadr, output CSVs to `data/clean/` |
| `load_to_supabase.py` | Load clean CSVs into Supabase via REST API |
| `build_player_seasons.R` | Build player-season-team CSV from nflreadr rosters (requires R + nflreadr) |

### Data Pipeline

```
NFFC API → data/raw/ (JSON)
    → build_clean_dataset.py → data/clean/ (CSV)
        → load_to_supabase.py → Supabase
                ↑
       data/nflreadr/ (R enrichment via build_player_seasons.R)
```

## Database Schema

### Tables

#### `players`
| Column | Type | Notes |
|--------|------|-------|
| `player_id` | text PK | Sportradar UUID (= NFFC player UUID) |
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

All tables: RLS enabled, public SELECT policy. No INSERT/UPDATE/DELETE via API.

### Migrations Applied

1. `create_tables` — All tables with PKs and FKs
2. `create_indexes` — Custom indexes
3. `fix_pick_duration_type` — smallint → integer
4. `add_missing_fk_index` — draft_picks(league_id, team_id)
5. `enable_rls_with_read_policy` — RLS + public SELECT
6. `create_view_draft_board` — Pre-joined view
7. `create_player_seasons` — Player-season-team table
8. `update_view_draft_board_season_team` — Add COALESCE team logic to view

## Data Row Counts (Rotowire OC only)

| Table | Rows |
|-------|------|
| `players` | 1,634 |
| `leagues` | 2,629 |
| `league_teams` | 31,548 |
| `adp` | 5,339 |
| `draft_picks` | 621,356 |
| `player_seasons` | 6,060 |

## Key Gotchas

- NFFC player UUIDs = Sportradar IDs (`sportradar_id` in nflreadr's `load_ff_playerids()`)
- Python `urllib` gets 403 from `nfc.shgn.com` — must set User-Agent header
- API `pick` field is already overall pick (1-240), NOT within-round
- `game_style_id` values change each year; not in historical data — filter by league name
- 2018 has no team outcomes (league_rank etc.); 2025 has no outcomes yet
- ~1.4% of draft picks have empty player_id (NFFC API bug)
- `pick_duration` can exceed 32K — needs integer, not smallint
- Some `birth_date` values are "0000-00-00" — treated as NULL
