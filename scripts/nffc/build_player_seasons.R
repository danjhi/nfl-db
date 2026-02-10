#!/usr/bin/env Rscript
# Build player-season-team mappings from nflreadr rosters.
# Outputs: data/nflreadr/player_seasons.csv
#   Columns: player_id (sportradar UUID), year, team (NFL abbrev)

library(nflreadr)
library(dplyr)
library(readr)

cat("Loading ff_playerids for sportradar_id mapping...\n")
ids <- read_csv("data/nflreadr/ff_playerids.csv", show_col_types = FALSE) |>
  select(sportradar_id, gsis_id) |>
  filter(!is.na(sportradar_id), !is.na(gsis_id), sportradar_id != "", gsis_id != "")

cat(sprintf("  %d sportradar-to-gsis mappings\n", nrow(ids)))

cat("Loading rosters for 2018-2025...\n")
rosters <- load_rosters(2018:2025)

cat(sprintf("  %d total roster entries\n", nrow(rosters)))

# Keep one row per player per season — use the team from Week 1 (or earliest available)
# This gives us the team the player started the season on
player_seasons <- rosters |>
  filter(!is.na(gsis_id), gsis_id != "") |>
  arrange(season, gsis_id, week) |>
  group_by(gsis_id, season) |>
  slice_head(n = 1) |>
  ungroup() |>
  select(gsis_id, year = season, team)

cat(sprintf("  %d player-season rows after dedup\n", nrow(player_seasons)))

# Join to get sportradar_id (= NFFC player_id)
result <- player_seasons |>
  inner_join(ids, by = "gsis_id", relationship = "many-to-many") |>
  select(player_id = sportradar_id, year, team) |>
  distinct() |>
  arrange(player_id, year)

cat(sprintf("  %d rows matched to sportradar IDs\n", nrow(result)))

out_path <- "data/nflreadr/player_seasons.csv"
write_csv(result, out_path)
cat(sprintf("Wrote %s (%d rows)\n", out_path, nrow(result)))
