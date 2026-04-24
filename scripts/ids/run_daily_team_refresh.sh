#!/bin/bash
# Daily team refresh wrapper — updates players.latest_team from Sleeper API.
# Scheduled via launchd (com.nfldb.daily-team-refresh.plist).

TODAY=$(date +%Y%m%d)

# Only run Feb 19 – Apr 30, 2026 (free agency + draft + early UDFA window)
if [ "$TODAY" -lt 20260219 ] || [ "$TODAY" -gt 20260430 ]; then
    echo "$(date): Outside date range (${TODAY}), skipping."
    exit 0
fi

echo "$(date): Starting team refresh from Sleeper..."
/usr/bin/python3 /Users/dan/dev/nfl-db/scripts/ids/refresh_player_teams.py
echo "$(date): Done."
