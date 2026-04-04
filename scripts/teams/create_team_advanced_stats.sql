-- team_advanced_stats: Season-level advanced analytics per team
-- Source: nflverse CSVs (team_efficiency, red_zone, scheme, PROE, pressure)
-- Includes pre-computed league ranks (1-32) for every stat column
-- PK: (team, season)

CREATE TABLE IF NOT EXISTS team_advanced_stats (
  team TEXT NOT NULL,
  season INTEGER NOT NULL,

  -- Efficiency (from team_efficiency.csv)
  pass_att_g NUMERIC,
  rush_att_g NUMERIC,
  sacks_g NUMERIC,
  plays_g NUMERIC,
  cmp_pct NUMERIC,
  ypa NUMERIC,
  ypc NUMERIC,
  sack_rate NUMERIC,
  pass_epa_play NUMERIC,
  rush_epa_play NUMERIC,
  air_yds_pct NUMERIC,
  yac_pct NUMERIC,
  pass_td_rate NUMERIC,
  rush_td_rate NUMERIC,
  int_rate NUMERIC,
  cpoe NUMERIC,
  pass_att INTEGER,
  completions INTEGER,
  pass_yds INTEGER,
  pass_td INTEGER,
  pass_int INTEGER,
  rush_att INTEGER,
  rush_yds INTEGER,
  rush_td INTEGER,

  -- Red Zone (from red_zone_stats.csv)
  rz_trips INTEGER,
  rz_pass_rate NUMERIC,
  rz_rush_rate NUMERIC,
  rz_td_rate NUMERIC,
  rz_pass_td INTEGER,
  rz_rush_td INTEGER,
  rz_epa_play NUMERIC,
  i10_plays INTEGER,
  i10_td INTEGER,
  i10_td_rate NUMERIC,
  rz_trip_td_rate NUMERIC,

  -- Scheme (from scheme_stats.csv)
  play_action_rate NUMERIC,
  rpo_rate NUMERIC,
  screen_rate NUMERIC,
  no_huddle_rate NUMERIC,
  motion_rate NUMERIC,
  under_center_rate NUMERIC,
  shotgun_rate NUMERIC,
  pistol_rate NUMERIC,

  -- Pass Rate / PROE (from neutral_pass_rate.csv)
  neutral_pass_rate NUMERIC,
  leading_pass_rate NUMERIC,
  trailing_pass_rate NUMERIC,
  total_pass_rate NUMERIC,

  -- Pressure (from sack_pressure_stats.csv)
  pressure_rate NUMERIC,
  blitz_rate_faced NUMERIC,
  hurry_rate NUMERIC,
  hit_rate NUMERIC,
  drop_rate NUMERIC,
  bad_throw_rate NUMERIC,
  throwaway_rate NUMERIC,
  pocket_time NUMERIC,

  -- League ranks (1=best, 32=worst for that stat's context)
  pass_att_g_rank INTEGER,
  rush_att_g_rank INTEGER,
  sacks_g_rank INTEGER,
  plays_g_rank INTEGER,
  cmp_pct_rank INTEGER,
  ypa_rank INTEGER,
  ypc_rank INTEGER,
  sack_rate_rank INTEGER,
  pass_epa_play_rank INTEGER,
  rush_epa_play_rank INTEGER,
  air_yds_pct_rank INTEGER,
  yac_pct_rank INTEGER,
  pass_td_rate_rank INTEGER,
  rush_td_rate_rank INTEGER,
  int_rate_rank INTEGER,
  cpoe_rank INTEGER,
  pass_att_rank INTEGER,
  completions_rank INTEGER,
  pass_yds_rank INTEGER,
  pass_td_rank INTEGER,
  pass_int_rank INTEGER,
  rush_att_rank INTEGER,
  rush_yds_rank INTEGER,
  rush_td_rank INTEGER,
  rz_trips_rank INTEGER,
  rz_pass_rate_rank INTEGER,
  rz_rush_rate_rank INTEGER,
  rz_td_rate_rank INTEGER,
  rz_pass_td_rank INTEGER,
  rz_rush_td_rank INTEGER,
  rz_epa_play_rank INTEGER,
  i10_plays_rank INTEGER,
  i10_td_rank INTEGER,
  i10_td_rate_rank INTEGER,
  rz_trip_td_rate_rank INTEGER,
  play_action_rate_rank INTEGER,
  rpo_rate_rank INTEGER,
  screen_rate_rank INTEGER,
  no_huddle_rate_rank INTEGER,
  motion_rate_rank INTEGER,
  under_center_rate_rank INTEGER,
  shotgun_rate_rank INTEGER,
  pistol_rate_rank INTEGER,
  neutral_pass_rate_rank INTEGER,
  leading_pass_rate_rank INTEGER,
  trailing_pass_rate_rank INTEGER,
  total_pass_rate_rank INTEGER,
  pressure_rate_rank INTEGER,
  blitz_rate_faced_rank INTEGER,
  hurry_rate_rank INTEGER,
  hit_rate_rank INTEGER,
  drop_rate_rank INTEGER,
  bad_throw_rate_rank INTEGER,
  throwaway_rate_rank INTEGER,
  pocket_time_rank INTEGER,

  PRIMARY KEY (team, season)
);

-- RLS
ALTER TABLE team_advanced_stats ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_select_team_advanced_stats"
  ON team_advanced_stats FOR SELECT TO anon USING (true);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_tas_season ON team_advanced_stats (season);
CREATE INDEX IF NOT EXISTS idx_tas_team ON team_advanced_stats (team);
