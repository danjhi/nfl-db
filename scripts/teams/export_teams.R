#!/usr/bin/env Rscript
# Export nflreadr load_teams() to CSV for loading into Supabase.
# Output: data/nflreadr/teams.csv
#
# Usage: Rscript scripts/teams/export_teams.R

library(nflreadr)
library(readr)

cat("Loading nflreadr teams...\n")
teams <- load_teams()
cat(sprintf("  %d teams, %d columns\n", nrow(teams), ncol(teams)))
cat(sprintf("  Columns: %s\n", paste(names(teams), collapse = ", ")))

out_path <- "data/nflreadr/teams.csv"
write_csv(teams, out_path)
cat(sprintf("Wrote %s (%d rows)\n", out_path, nrow(teams)))
