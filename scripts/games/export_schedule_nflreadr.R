#!/usr/bin/env Rscript
# Export nflreadr's load_schedules() for a given year to CSV.
# Used by enrich_schedule_nflreadr.py to PATCH metadata (kickoff, stadium,
# roof, surface, network proxy) onto rows already loaded from Sleeper.
#
# Usage: Rscript scripts/games/export_schedule_nflreadr.R [year]

suppressPackageStartupMessages({
  library(nflreadr)
  library(dplyr)
  library(readr)
})

args <- commandArgs(trailingOnly = TRUE)
year <- if (length(args) >= 1) as.integer(args[1]) else 2026L

# Map historical/inconsistent abbrs to nflreadr-standard (matches our normalize_team)
norm <- function(t) {
  dplyr::case_when(
    t == "LAR" ~ "LA",
    t == "OAK" ~ "LV",
    t == "SD"  ~ "LAC",
    t == "STL" ~ "LA",
    t == "JAC" ~ "JAX",
    t == "WSH" ~ "WAS",
    TRUE ~ t
  )
}

cat(sprintf("Loading schedules for %d...\n", year))
s <- load_schedules(year)
cat(sprintf("  %d rows\n", nrow(s)))

out <- s |>
  mutate(
    home_team = norm(home_team),
    away_team = norm(away_team),
    # Combine gameday + gametime into UTC ISO timestamp (gametime is ET HH:MM)
    # nflreadr times are Eastern; we keep them as ET text and let Python parse.
    kickoff_et_text = ifelse(
      !is.na(gameday) & !is.na(gametime),
      paste0(gameday, " ", gametime),
      NA_character_
    )
  ) |>
  select(
    season,
    week,
    season_type   = game_type,    # 'REG' / 'POST' / 'PRE' / 'WC' etc.
    game_date     = gameday,
    weekday,
    kickoff_et_text,
    home_team,
    away_team,
    home_score,
    away_score,
    stadium,
    roof,
    surface,
    location,                       # 'Home' / 'Neutral' (international/Super Bowl)
    div_game,
    referee,
    away_qb_name,
    home_qb_name,
    spread_line,
    total_line,
    away_moneyline,
    home_moneyline
  )

out_path <- sprintf("data/nflreadr/schedule_%d.csv", year)
write_csv(out, out_path, na = "")
cat(sprintf("Wrote %s (%d rows)\n", out_path, nrow(out)))
