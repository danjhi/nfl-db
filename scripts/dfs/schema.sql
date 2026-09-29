-- DFS salaries from SportsDataIO (DraftKings + FanDuel slates), 2026-09-07.
-- Migration: create_dfs_slates, create_dfs_salaries
-- Apply via pg8000 direct Postgres (Management API blocked by Cloudflare), same as games/schema.sql:
--   .venv/bin/python3 scripts/dfs/apply_schema.py
--
-- Feeds the DFS First Look article (2026-dfs `dfs.first_look`) and any lineup work downstream.
-- Written by scripts/dfs/fetch_sportsdata_dfs.py (daily 9:10 laptop launchd job in season,
-- plus a by-hand run before the Wednesday noon article snapshot).

-- ── dfs_slates ───────────────────────────────────────────────────────────────
-- One row per operator slate per week, straight from SportsDataIO's DfsSlatesByWeek
-- (slate_id = their SlateID, globally unique). Test slates the operator later pulled
-- keep their row with removed_by_operator = true so a removed "Main" never surprises
-- anyone; their player rows are NOT loaded.

CREATE TABLE IF NOT EXISTS dfs_slates (
  slate_id             INT PRIMARY KEY,
  season               INT  NOT NULL,
  week                 INT  NOT NULL,
  operator             TEXT NOT NULL,          -- DraftKings / FanDuel / Yahoo ...
  operator_slate_id    TEXT,
  operator_name        TEXT,                   -- "Main", "Wed-Mon", "NE @ SEA"
  operator_game_type   TEXT,                   -- Classic / Showdown Captain Mode / Single Game / SuperFlex ...
  operator_day         DATE,
  operator_start_time  TIMESTAMPTZ,            -- SDIO gives ET wall clock; stored as UTC
  number_of_games      INT,
  is_multi_day         BOOLEAN,
  removed_by_operator  BOOLEAN NOT NULL DEFAULT false,
  salary_cap           INT,
  roster_slots         JSONB,                  -- ["QB","RB","RB","WR","WR","WR","TE","FLEX","DST"]
  games                JSONB,                  -- [{"away":"NE","home":"SEA","kickoff":"2026-09-10T00:20:00+00:00","game_id":"2026_01_NE_SEA"}]
  player_count         INT,
  retrieved_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── dfs_salaries ─────────────────────────────────────────────────────────────
-- One row per slate per player (per roster slot on Showdown-style slates). slate_id = 0 is the synthetic "operator-wide weekly
-- salary" row from the PlayerGameProjectionStatsByWeek / FantasyDefenseProjectionsByGame
-- feeds (the fallback when an operator's slates are late; DK/FD only).
-- player_id is the players table key: Sportradar UUID for players (matched through
-- SportsDataIO's own SportRadarPlayerID, then players.sportsdata_id, then name+team),
-- DEF_<TEAM> for team defenses, NULL when nothing matched (depth bodies, mostly).
-- team is normalized to the nflreadr codes the rest of the schema uses (LA, JAX, WAS).

CREATE TABLE IF NOT EXISTS dfs_salaries (
  season               INT  NOT NULL,
  week                 INT  NOT NULL,
  operator             TEXT NOT NULL,
  slate_id             INT  NOT NULL,          -- dfs_slates.slate_id, or 0 (weekly feed)
  sdio_player_id       INT  NOT NULL,          -- SportsDataIO PlayerID (their TeamID for DST rows)
  player_id            TEXT,                   -- players.player_id / DEF_<TEAM> / NULL
  match_source         TEXT,                   -- sportradar | supabase | name | team | none
  operator_player_id   TEXT,
  name                 TEXT,
  position             TEXT,                   -- the operator's position label (DST, D, K ...)
  team                 TEXT,
  salary               INT,
  roster_slots         JSONB,
  slot                 TEXT NOT NULL DEFAULT '',  -- '' for Classic-family slates; 'CPT' / 'FLEX' etc. on Showdown and Single Game slates, where one player is two priced rows
  removed_by_operator  BOOLEAN NOT NULL DEFAULT false,
  retrieved_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (season, week, operator, slate_id, sdio_player_id, slot)
);

-- ── RLS ──────────────────────────────────────────────────────────────────────
ALTER TABLE dfs_slates   ENABLE ROW LEVEL SECURITY;
ALTER TABLE dfs_salaries ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "anon_select_dfs_slates"   ON dfs_slates;
DROP POLICY IF EXISTS "anon_select_dfs_salaries" ON dfs_salaries;
CREATE POLICY "anon_select_dfs_slates"   ON dfs_slates   FOR SELECT USING (true);
CREATE POLICY "anon_select_dfs_salaries" ON dfs_salaries FOR SELECT USING (true);

-- ── Indexes ──────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_dfs_slates_week      ON dfs_slates(season, week, operator);
CREATE INDEX IF NOT EXISTS idx_dfs_salaries_week    ON dfs_salaries(season, week, operator, slate_id);
CREATE INDEX IF NOT EXISTS idx_dfs_salaries_player  ON dfs_salaries(player_id);
