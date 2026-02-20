#!/usr/bin/env Rscript
# Build team-game-level stats from nflreadr for 2016-2025.
# Aggregates player stats into team offensive totals, positional FP,
# and defensive FP allowed. Includes game context (Vegas lines, scores).
#
# Output: data/nflreadr/team_game_stats.csv
#
# Usage: Rscript scripts/teams/build_team_game_stats.R

library(nflreadr)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)

SEASONS <- 2016:2025

# Map historical abbreviations to current
normalize_team <- function(team) {
  case_when(
    team == "OAK" ~ "LV",
    team == "SD"  ~ "LAC",
    team == "STL" ~ "LA",
    TRUE ~ team
  )
}

# ── 1. Load data ─────────────────────────────────────────────────────────────
cat("Loading player stats for", min(SEASONS), "-", max(SEASONS), "...\n")
player_stats <- load_player_stats(SEASONS)
cat(sprintf("  %d player-week rows\n", nrow(player_stats)))

cat("Loading schedules...\n")
schedules <- load_schedules(SEASONS)
cat(sprintf("  %d schedule rows\n", nrow(schedules)))

# ── 2. Prep player stats ─────────────────────────────────────────────────────
# nflreadr load_player_stats() column is "recent_team" — rename for clarity
player_stats <- player_stats |>
  rename(recent_team = team) |>
  filter(week <= 18) |>
  mutate(
    recent_team = normalize_team(recent_team),
    # Replace NA with 0 for scoring components
    across(c(passing_2pt_conversions, rushing_2pt_conversions,
             receiving_2pt_conversions, sack_fumbles_lost,
             rushing_fumbles_lost, receiving_fumbles_lost),
           ~ replace_na(.x, 0)),
    # FP components by category (standard scoring)
    fp_passing = passing_yards * 0.04 + passing_tds * 4
                 - passing_interceptions * 2
                 + passing_2pt_conversions * 2
                 - sack_fumbles_lost * 2,
    fp_rushing = rushing_yards * 0.1 + rushing_tds * 6
                 + rushing_2pt_conversions * 2
                 - rushing_fumbles_lost * 2,
    fp_receiving = receiving_yards * 0.1 + receiving_tds * 6
                   + receiving_2pt_conversions * 2
                   - receiving_fumbles_lost * 2
  )

# ── 3. Aggregate team offensive totals ────────────────────────────────────────
cat("Aggregating team offensive totals...\n")
team_off <- player_stats |>
  group_by(recent_team, season, week) |>
  summarise(
    # Raw stats
    pass_att = sum(attempts, na.rm = TRUE),
    pass_cmp = sum(completions, na.rm = TRUE),
    pass_yds = sum(passing_yards, na.rm = TRUE),
    pass_td  = sum(passing_tds, na.rm = TRUE),
    pass_int = sum(passing_interceptions, na.rm = TRUE),
    rush_att = sum(carries, na.rm = TRUE),
    rush_yds = sum(rushing_yards, na.rm = TRUE),
    rush_td  = sum(rushing_tds, na.rm = TRUE),
    targets  = sum(targets, na.rm = TRUE),
    receptions = sum(receptions, na.rm = TRUE),
    rec_yds  = sum(receiving_yards, na.rm = TRUE),
    rec_td   = sum(receiving_tds, na.rm = TRUE),
    # FP by category (standard)
    off_pass_fp  = sum(fp_passing, na.rm = TRUE),
    off_rush_fp  = sum(fp_rushing, na.rm = TRUE),
    off_recv_fp  = sum(fp_receiving, na.rm = TRUE),
    off_total_fp = sum(fantasy_points, na.rm = TRUE),
    .groups = "drop"
  )
cat(sprintf("  %d team-week offensive rows\n", nrow(team_off)))

# ── 4. Aggregate positional FP ───────────────────────────────────────────────
cat("Aggregating positional FP...\n")
pos_fp <- player_stats |>
  filter(position_group %in% c("QB", "RB", "WR", "TE")) |>
  group_by(recent_team, season, week, position_group) |>
  summarise(
    pos_fp  = sum(fantasy_points, na.rm = TRUE),
    pos_rec = sum(receptions, na.rm = TRUE),
    .groups = "drop"
  ) |>
  pivot_wider(
    names_from = position_group,
    values_from = c(pos_fp, pos_rec),
    values_fill = 0
  ) |>
  rename(
    qb_fp = pos_fp_QB, rb_fp = pos_fp_RB,
    wr_fp = pos_fp_WR, te_fp = pos_fp_TE,
    qb_rec = pos_rec_QB, rb_rec = pos_rec_RB,
    wr_rec = pos_rec_WR, te_rec = pos_rec_TE
  )
cat(sprintf("  %d team-week positional rows\n", nrow(pos_fp)))

# ── 5. Build game context from schedules ──────────────────────────────────────
cat("Building game context...\n")
game_ctx <- schedules |>
  filter(game_type == "REG") |>
  select(game_id, season, week, home_team, away_team,
         spread_line, total_line, home_score, away_score) |>
  clean_homeaway() |>
  mutate(
    team = normalize_team(team),
    opponent = normalize_team(opponent),
    # implied_total: team's Vegas-implied scoring total
    implied_total = (total_line + if_else(location == "home",
                                          spread_line, -spread_line)) / 2,
    # Convert spread to standard convention (negative = favored)
    spread = -if_else(location == "home", spread_line, -spread_line)
  ) |>
  rename(team_score = team_score, opp_score = opponent_score) |>
  select(game_id, season, week, team, opponent, location,
         team_score, opp_score, spread, total_line, implied_total)

cat(sprintf("  %d game context rows\n", nrow(game_ctx)))

# ── 6. Join offense + positional + game context ──────────────────────────────
cat("Joining data...\n")
team_games <- team_off |>
  left_join(pos_fp, by = c("recent_team", "season", "week")) |>
  left_join(game_ctx, by = c("recent_team" = "team", "season", "week"))

cat(sprintf("  %d rows after joins\n", nrow(team_games)))

# Check for missing game_id (would indicate join issues)
missing_ctx <- sum(is.na(team_games$game_id))
if (missing_ctx > 0) {
  cat(sprintf("  WARNING: %d rows missing game context\n", missing_ctx))
}

# ── 7. Compute defensive stats (opponent's offense = this team's def allowed) ─
cat("Computing defensive stats...\n")
def_stats <- team_games |>
  select(recent_team, season, week,
         def_pass_fp = off_pass_fp,
         def_rush_fp = off_rush_fp,
         def_recv_fp = off_recv_fp,
         def_total_fp = off_total_fp,
         def_receptions = receptions,
         def_qb_fp = qb_fp, def_qb_rec = qb_rec,
         def_rb_fp = rb_fp, def_rb_rec = rb_rec,
         def_wr_fp = wr_fp, def_wr_rec = wr_rec,
         def_te_fp = te_fp, def_te_rec = te_rec)

team_games <- team_games |>
  left_join(
    def_stats,
    by = c("opponent" = "recent_team", "season", "week")
  )

# ── 8. Clean up and rename ────────────────────────────────────────────────────
team_games <- team_games |>
  rename(team = recent_team) |>
  select(
    # Identity
    game_id, season, week, team, opponent, location,
    # Game context
    team_score, opp_score, spread, total_line, implied_total,
    # Raw team offensive stats
    pass_att, pass_cmp, pass_yds, pass_td, pass_int,
    rush_att, rush_yds, rush_td,
    targets, receptions, rec_yds, rec_td,
    # Offensive FP (standard)
    off_pass_fp, off_rush_fp, off_recv_fp, off_total_fp,
    # Positional FP (standard + receptions)
    qb_fp, qb_rec, rb_fp, rb_rec, wr_fp, wr_rec, te_fp, te_rec,
    # Defensive FP allowed (standard + receptions)
    def_pass_fp, def_rush_fp, def_recv_fp, def_total_fp, def_receptions,
    def_qb_fp, def_qb_rec, def_rb_fp, def_rb_rec,
    def_wr_fp, def_wr_rec, def_te_fp, def_te_rec
  ) |>
  filter(!is.na(game_id)) |>
  arrange(season, week, game_id, team)

# ── 9. Round FP columns ──────────────────────────────────────────────────────
fp_cols <- c("off_pass_fp", "off_rush_fp", "off_recv_fp", "off_total_fp",
             "qb_fp", "rb_fp", "wr_fp", "te_fp",
             "def_pass_fp", "def_rush_fp", "def_recv_fp", "def_total_fp",
             "def_qb_fp", "def_rb_fp", "def_wr_fp", "def_te_fp")
team_games <- team_games |>
  mutate(across(all_of(fp_cols), ~ round(.x, 1)))

# Also round spread/implied_total
team_games <- team_games |>
  mutate(across(c(spread, implied_total), ~ round(.x, 1)))

# ── 10. Write CSV ─────────────────────────────────────────────────────────────
out_path <- "data/nflreadr/team_game_stats.csv"
write_csv(team_games, out_path, na = "")
cat(sprintf("\nWrote %s (%d rows, %d columns)\n", out_path, nrow(team_games), ncol(team_games)))

# Summary
cat(sprintf("\nSeasons: %d-%d\n", min(team_games$season), max(team_games$season)))
cat(sprintf("Unique teams: %d\n", n_distinct(team_games$team)))
games_per_season <- team_games |> count(season) |> pull(n)
cat(sprintf("Rows per season: %s\n", paste(games_per_season, collapse = ", ")))
