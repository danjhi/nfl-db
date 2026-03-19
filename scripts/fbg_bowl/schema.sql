-- FBG Bowl historical data schema
-- Migration: create_fbg_bowl_tables
-- Applied via direct Postgres connection (pg8000)

-- One row per FBG Bowl league per year
CREATE TABLE IF NOT EXISTS fbg_bowl_leagues (
  id            BIGSERIAL PRIMARY KEY,
  sleeper_id    TEXT NOT NULL,
  year          INT  NOT NULL,
  name          TEXT,
  scoring_type  TEXT,
  roster_count  INT,
  UNIQUE (sleeper_id)
);

-- One row per user/roster per league
CREATE TABLE IF NOT EXISTS fbg_bowl_rosters (
  id                BIGSERIAL PRIMARY KEY,
  league_id         BIGINT REFERENCES fbg_bowl_leagues(id),
  sleeper_user_id   TEXT NOT NULL,
  sleeper_roster_id INT,
  display_name      TEXT,
  team_name         TEXT
);

-- One row per roster per week (regular season weeks 1-14)
CREATE TABLE IF NOT EXISTS fbg_bowl_weekly_results (
  id          BIGSERIAL PRIMARY KEY,
  roster_id   BIGINT REFERENCES fbg_bowl_rosters(id),
  league_id   BIGINT REFERENCES fbg_bowl_leagues(id),
  week        INT NOT NULL,
  pts_for     NUMERIC(8,2),
  pts_against NUMERIC(8,2),
  win         BOOLEAN,
  loss        BOOLEAN,
  tie         BOOLEAN
);

-- Cumulative standings after each week (pre-computed for query speed)
CREATE TABLE IF NOT EXISTS fbg_bowl_standings (
  id                 BIGSERIAL PRIMARY KEY,
  roster_id          BIGINT REFERENCES fbg_bowl_rosters(id),
  league_id          BIGINT REFERENCES fbg_bowl_leagues(id),
  week               INT NOT NULL,
  wins               INT,
  losses             INT,
  pts_for            NUMERIC(10,2),
  pts_against        NUMERIC(10,2),
  league_rank        INT,
  qualified_playoffs BOOLEAN,
  UNIQUE (roster_id, week)
);

-- Playoff weeks (15, 16, 17)
CREATE TABLE IF NOT EXISTS fbg_bowl_playoff_results (
  id         BIGSERIAL PRIMARY KEY,
  roster_id  BIGINT REFERENCES fbg_bowl_rosters(id),
  league_id  BIGINT REFERENCES fbg_bowl_leagues(id),
  week       INT NOT NULL,
  pts_for    NUMERIC(8,2),
  final_rank INT
);

-- Draft picks (one row per pick per league)
CREATE TABLE IF NOT EXISTS fbg_bowl_draft_picks (
  id                BIGSERIAL PRIMARY KEY,
  league_id         BIGINT REFERENCES fbg_bowl_leagues(id),
  roster_id         BIGINT REFERENCES fbg_bowl_rosters(id),
  sleeper_draft_id  TEXT,
  pick_no           INT,
  draft_slot        INT,
  sleeper_player_id TEXT,
  first_name        TEXT,
  last_name         TEXT,
  position          TEXT
);

-- FBG Bowl meta-scores (computed from standings + playoffs)
CREATE TABLE IF NOT EXISTS fbg_bowl_scores (
  id                  BIGSERIAL PRIMARY KEY,
  roster_id           BIGINT REFERENCES fbg_bowl_rosters(id),
  year                INT NOT NULL,
  reg_season_wins     INT,
  league_rank_bonus   INT,
  semi_bonus          INT,
  finals_bonus        INT,
  top10_bonus         INT,
  total_score         INT,
  overall_rank        INT
);

-- RLS: enable on all tables, allow public SELECT
ALTER TABLE fbg_bowl_leagues        ENABLE ROW LEVEL SECURITY;
ALTER TABLE fbg_bowl_rosters        ENABLE ROW LEVEL SECURITY;
ALTER TABLE fbg_bowl_weekly_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE fbg_bowl_standings      ENABLE ROW LEVEL SECURITY;
ALTER TABLE fbg_bowl_playoff_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE fbg_bowl_draft_picks    ENABLE ROW LEVEL SECURITY;
ALTER TABLE fbg_bowl_scores         ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anon_select_fbg_bowl_leagues"         ON fbg_bowl_leagues         FOR SELECT USING (true);
CREATE POLICY "anon_select_fbg_bowl_rosters"         ON fbg_bowl_rosters         FOR SELECT USING (true);
CREATE POLICY "anon_select_fbg_bowl_weekly_results"  ON fbg_bowl_weekly_results  FOR SELECT USING (true);
CREATE POLICY "anon_select_fbg_bowl_standings"       ON fbg_bowl_standings       FOR SELECT USING (true);
CREATE POLICY "anon_select_fbg_bowl_playoff_results" ON fbg_bowl_playoff_results FOR SELECT USING (true);
CREATE POLICY "anon_select_fbg_bowl_draft_picks"     ON fbg_bowl_draft_picks     FOR SELECT USING (true);
CREATE POLICY "anon_select_fbg_bowl_scores"          ON fbg_bowl_scores          FOR SELECT USING (true);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_fbg_rosters_league       ON fbg_bowl_rosters(league_id);
CREATE INDEX IF NOT EXISTS idx_fbg_weekly_roster_week   ON fbg_bowl_weekly_results(roster_id, week);
CREATE INDEX IF NOT EXISTS idx_fbg_weekly_league_week   ON fbg_bowl_weekly_results(league_id, week);
CREATE INDEX IF NOT EXISTS idx_fbg_standings_roster     ON fbg_bowl_standings(roster_id);
CREATE INDEX IF NOT EXISTS idx_fbg_standings_league_wk  ON fbg_bowl_standings(league_id, week);
CREATE INDEX IF NOT EXISTS idx_fbg_playoff_roster       ON fbg_bowl_playoff_results(roster_id);
CREATE INDEX IF NOT EXISTS idx_fbg_picks_league         ON fbg_bowl_draft_picks(league_id);
CREATE INDEX IF NOT EXISTS idx_fbg_picks_player         ON fbg_bowl_draft_picks(sleeper_player_id);
CREATE INDEX IF NOT EXISTS idx_fbg_scores_year          ON fbg_bowl_scores(year);
CREATE INDEX IF NOT EXISTS idx_fbg_scores_rank          ON fbg_bowl_scores(overall_rank);
