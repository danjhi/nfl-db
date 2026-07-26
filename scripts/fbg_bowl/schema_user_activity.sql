-- FBG Bowl cohort — Sleeper activity outside the Bowl
-- Migration: create_fbg_bowl_user_activity
-- Apply via psql / pg8000 (Management API blocked by Cloudflare)
--
-- One row per (Bowl participant, season, snapshot). Snapshot dimension follows the
-- adp_sources pattern: re-running the pull on a later date appends a new snapshot
-- rather than overwriting, so in-season 2026 growth is a time series.
--
-- Source: Sleeper GET /v1/user/{user_id}/leagues/nfl/{season} for every distinct
-- sleeper_user_id in fbg_bowl_rosters. ~23K calls, ~2.5 min, no auth.

CREATE TABLE IF NOT EXISTS fbg_bowl_user_activity (
  sleeper_user_id      TEXT NOT NULL,
  season               INT  NOT NULL,
  snapshot_date        DATE NOT NULL DEFAULT CURRENT_DATE,

  -- league counts
  league_count         INT,  -- all NFL leagues on Sleeper that season
  bowl_league_count    INT,  -- FBG Bowl divisions
  fbg_other_count      INT,  -- other FBG-run leagues (home/listener/staff)
  outside_league_count INT,  -- everything not FBG-run: their own fantasy life

  -- format mix (counts of that season's leagues)
  dynasty_count        INT,
  redraft_count        INT,
  keeper_count         INT,
  elimination_count    INT,  -- Sleeper settings.type=3 (survivor/elimination)
  best_ball_count      INT,
  superflex_count      INT,
  tep_count            INT,

  -- cohort flags (denormalized from fbg_bowl_rosters for query convenience)
  played_bowl_2024     BOOLEAN,
  played_bowl_2025     BOOLEAN,

  retrieved_at         TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (sleeper_user_id, season, snapshot_date)
);

ALTER TABLE fbg_bowl_user_activity ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anon_select_fbg_bowl_user_activity"
  ON fbg_bowl_user_activity FOR SELECT USING (true);

CREATE INDEX IF NOT EXISTS idx_fbg_activity_season
  ON fbg_bowl_user_activity(season, snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_fbg_activity_user
  ON fbg_bowl_user_activity(sleeper_user_id);
CREATE INDEX IF NOT EXISTS idx_fbg_activity_cohort
  ON fbg_bowl_user_activity(played_bowl_2024, played_bowl_2025);
