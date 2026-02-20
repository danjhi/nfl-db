#!/usr/bin/env Rscript
# Build player-level weekly stats from nflreadr for 2016-2025.
# Maps gsis_id → sportradar_id (our DB player_id) via ff_playerids.csv.
#
# Output: data/nflreadr/player_stats.csv
#
# Usage: Rscript scripts/stats/build_player_stats.R

library(nflreadr)
library(dplyr)
library(tidyr)
library(readr)

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

# ── 1. Load sportradar_id mapping ────────────────────────────────────────────
cat("Loading ff_playerids for sportradar_id mapping...\n")
ids <- read_csv("data/nflreadr/ff_playerids.csv", show_col_types = FALSE) |>
  select(sportradar_id, gsis_id) |>
  filter(!is.na(sportradar_id), !is.na(gsis_id),
         sportradar_id != "", gsis_id != "")
cat(sprintf("  %d sportradar-to-gsis mappings\n", nrow(ids)))

# ── 2. Load player stats ────────────────────────────────────────────────────
cat("Loading player stats for", min(SEASONS), "-", max(SEASONS), "...\n")
player_stats <- load_player_stats(SEASONS)
cat(sprintf("  %d player-week rows\n", nrow(player_stats)))

# ── 3. Prep: rename, filter, normalize ───────────────────────────────────────
player_stats <- player_stats |>
  rename(recent_team = team) |>
  filter(week <= 18) |>
  mutate(
    recent_team = normalize_team(recent_team),
    across(c(passing_2pt_conversions, rushing_2pt_conversions,
             receiving_2pt_conversions, sack_fumbles_lost,
             rushing_fumbles_lost, receiving_fumbles_lost),
           ~ replace_na(.x, 0))
  )

cat(sprintf("  %d rows after regular-season filter\n", nrow(player_stats)))

# ── 4. Map gsis_id → sportradar_id ──────────────────────────────────────────
# nflreadr's player_id column IS the gsis_id
result <- player_stats |>
  inner_join(ids, by = c("player_id" = "gsis_id"),
             relationship = "many-to-many") |>
  distinct(sportradar_id, season, week, .keep_all = TRUE)

cat(sprintf("  %d rows matched to sportradar IDs\n", nrow(result)))

# ── 5. Select and rename to match DB schema ──────────────────────────────────
output <- result |>
  transmute(
    player_id = sportradar_id,
    season,
    week,
    team = recent_team,
    position = position_group,
    opponent = normalize_team(opponent_team),

    # Passing
    pass_att = attempts,
    pass_cmp = completions,
    pass_yds = passing_yards,
    pass_td = passing_tds,
    pass_int = passing_interceptions,
    sacks = sacks_suffered,
    sack_yds = sack_yards_lost,
    sack_fumbles_lost,
    pass_air_yds = passing_air_yards,
    pass_yac = passing_yards_after_catch,
    pass_first_downs = passing_first_downs,
    pass_2pt = passing_2pt_conversions,

    # Rushing
    rush_att = carries,
    rush_yds = rushing_yards,
    rush_td = rushing_tds,
    rush_fumbles_lost = rushing_fumbles_lost,
    rush_first_downs = rushing_first_downs,
    rush_2pt = rushing_2pt_conversions,

    # Receiving
    targets,
    receptions,
    rec_yds = receiving_yards,
    rec_td = receiving_tds,
    rec_fumbles_lost = receiving_fumbles_lost,
    rec_air_yds = receiving_air_yards,
    rec_yac = receiving_yards_after_catch,
    rec_first_downs = receiving_first_downs,
    rec_2pt = receiving_2pt_conversions,

    # Special teams
    special_teams_tds,

    # Fantasy (standard from nflreadr)
    fantasy_points = round(fantasy_points, 1)
  ) |>
  arrange(season, week, player_id)

# ── 6. Write CSV ─────────────────────────────────────────────────────────────
out_path <- "data/nflreadr/player_stats.csv"
write_csv(output, out_path, na = "")

cat(sprintf("\nWrote %s (%d rows, %d columns)\n", out_path, nrow(output), ncol(output)))
cat(sprintf("Seasons: %d-%d\n", min(output$season), max(output$season)))
cat(sprintf("Unique players: %d\n", n_distinct(output$player_id)))
cat(sprintf("Rows per season:\n"))
output |>
  count(season) |>
  mutate(label = sprintf("  %d: %d rows", season, n)) |>
  pull(label) |>
  cat(sep = "\n")
cat("\n")
