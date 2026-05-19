-- NFL games + betting odds schema
-- Migrations: create_games, create_game_odds_snapshots
-- Apply via pg8000 direct Postgres (Management API blocked by Cloudflare).

-- ── games ────────────────────────────────────────────────────────────────────
-- One row per scheduled NFL game (regular + post).
-- game_id format matches nflreadr: "{season}_{week:02d}_{AWAY}_{HOME}"
-- e.g. "2026_01_CHI_CAR" for Bears @ Panthers in Week 1 of 2026.
-- This format aligns with team_game_stats.game_id for future joins.

CREATE TABLE IF NOT EXISTS games (
  game_id            TEXT PRIMARY KEY,
  season             INT  NOT NULL,
  week               INT  NOT NULL,
  season_type        TEXT NOT NULL CHECK (season_type IN ('reg', 'post')),
  game_date          DATE NOT NULL,
  kickoff            TIMESTAMPTZ,
  home_team          TEXT NOT NULL REFERENCES teams(team_abbr),
  away_team          TEXT NOT NULL REFERENCES teams(team_abbr),
  home_score         INT,
  away_score         INT,
  stadium            TEXT,
  roof               TEXT,
  surface            TEXT,
  network            TEXT,
  is_primetime       BOOLEAN,
  is_international   BOOLEAN,
  location_override  TEXT,
  sleeper_game_id    TEXT,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── game_odds_snapshots ──────────────────────────────────────────────────────
-- One row per game per book per snapshot date. Mirrors adp_sources pattern.
-- Spread convention: standard betting — negative home_spread = home favored.
-- Prices are American odds (e.g. -110, +135).

CREATE TABLE IF NOT EXISTS game_odds_snapshots (
  game_id              TEXT NOT NULL REFERENCES games(game_id) ON DELETE CASCADE,
  bookmaker            TEXT NOT NULL,
  date                 DATE NOT NULL DEFAULT CURRENT_DATE,
  home_spread          NUMERIC,
  home_spread_price    INT,
  away_spread_price    INT,
  total                NUMERIC,
  over_price           INT,
  under_price          INT,
  home_moneyline       INT,
  away_moneyline       INT,
  home_implied_total   NUMERIC,
  away_implied_total   NUMERIC,
  retrieved_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (game_id, bookmaker, date)
);

-- ── RLS ──────────────────────────────────────────────────────────────────────
ALTER TABLE games               ENABLE ROW LEVEL SECURITY;
ALTER TABLE game_odds_snapshots ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anon_select_games"               ON games               FOR SELECT USING (true);
CREATE POLICY "anon_select_game_odds_snapshots" ON game_odds_snapshots FOR SELECT USING (true);

-- ── Indexes ──────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_games_season_week   ON games(season, week);
CREATE INDEX IF NOT EXISTS idx_games_home_season   ON games(home_team, season);
CREATE INDEX IF NOT EXISTS idx_games_away_season   ON games(away_team, season);
CREATE INDEX IF NOT EXISTS idx_games_kickoff       ON games(kickoff);

CREATE INDEX IF NOT EXISTS idx_gos_date            ON game_odds_snapshots(date DESC);
CREATE INDEX IF NOT EXISTS idx_gos_game            ON game_odds_snapshots(game_id);
CREATE INDEX IF NOT EXISTS idx_gos_book_date       ON game_odds_snapshots(bookmaker, date DESC);
