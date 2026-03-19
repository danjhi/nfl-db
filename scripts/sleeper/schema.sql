-- Sleeper dynasty trade data
-- Apply via pg8000 (Management API blocked by Cloudflare)

-- Sleeper dynasty leagues (2026 dynasty leagues)
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

-- Indexes
CREATE INDEX idx_sleeper_trades_league ON sleeper_trades(league_id);
CREATE INDEX idx_sleeper_trades_season_week ON sleeper_trades(season, week);
CREATE INDEX idx_sleeper_trade_assets_txn ON sleeper_trade_assets(transaction_id);
CREATE INDEX idx_sleeper_trade_assets_player ON sleeper_trade_assets(sleeper_player_id) WHERE asset_type = 'player';

-- RLS
ALTER TABLE sleeper_leagues ENABLE ROW LEVEL SECURITY;
ALTER TABLE sleeper_trades ENABLE ROW LEVEL SECURITY;
ALTER TABLE sleeper_trade_assets ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read sleeper_leagues" ON sleeper_leagues FOR SELECT USING (true);
CREATE POLICY "Public read sleeper_trades" ON sleeper_trades FOR SELECT USING (true);
CREATE POLICY "Public read sleeper_trade_assets" ON sleeper_trade_assets FOR SELECT USING (true);

-- ── Sleeper dynasty drafts (added 2026-03-08) ──────────────────────────────

-- Draft metadata (startup + rookie + unknown)
CREATE TABLE sleeper_drafts (
  draft_id       text PRIMARY KEY,
  league_id      text REFERENCES sleeper_leagues(league_id),
  season         int,
  status         text,       -- 'complete', 'drafting', 'pre_draft'
  draft_format   text,       -- 'snake', 'linear', 'auction'
  draft_type     text,       -- 'startup', 'rookie', 'unknown'
  rounds         int,
  teams          int,
  start_time     bigint,
  last_picked    bigint,
  total_picks    int
);

-- Individual picks within each draft
CREATE TABLE sleeper_draft_picks (
  id             serial PRIMARY KEY,
  draft_id       text REFERENCES sleeper_drafts(draft_id),
  league_id      text,
  pick_no        int,
  round          int,
  draft_slot     int,
  roster_id      int,
  player_id      text,       -- Sleeper player ID
  picked_by      text,
  is_keeper      int DEFAULT 0,
  player_name    text,
  position       text,
  team           text,
  amount         int          -- auction amount (NULL for snake/linear)
);

-- Indexes
CREATE INDEX idx_sleeper_drafts_league ON sleeper_drafts(league_id);
CREATE INDEX idx_sleeper_drafts_type ON sleeper_drafts(draft_type);
CREATE INDEX idx_sleeper_draft_picks_draft ON sleeper_draft_picks(draft_id);
CREATE INDEX idx_sleeper_draft_picks_player ON sleeper_draft_picks(player_id);

-- RLS
ALTER TABLE sleeper_drafts ENABLE ROW LEVEL SECURITY;
ALTER TABLE sleeper_draft_picks ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Public read sleeper_drafts" ON sleeper_drafts FOR SELECT USING (true);
CREATE POLICY "Public read sleeper_draft_picks" ON sleeper_draft_picks FOR SELECT USING (true);
