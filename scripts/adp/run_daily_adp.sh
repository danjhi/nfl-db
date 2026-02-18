#!/bin/bash
# Daily ADP fetch wrapper — checks date range before running.
# Scheduled via launchd (com.nfldb.daily-adp.plist).

TODAY=$(date +%Y%m%d)

# Only run Feb 19 – Apr 22, 2026
if [ "$TODAY" -lt 20260219 ] || [ "$TODAY" -gt 20260422 ]; then
    echo "$(date): Outside date range (${TODAY}), skipping."
    exit 0
fi

echo "$(date): Starting Underdog ADP fetch..."
/usr/bin/python3 /Users/dan/Desktop/nfl-db/scripts/adp/fetch_underdog_adp.py
echo "$(date): Done."
