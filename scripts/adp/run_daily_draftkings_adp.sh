#!/bin/bash
# Daily DraftKings ADP fetch — date-gated Feb 19 – Apr 22, 2026
# Designed for launchd; called by com.nfldb.daily-draftkings-adp.plist

set -e

cd /Users/dan/dev/nfl-db

# Load env vars (needed for DK_CHROME_PROFILE, Supabase keys)
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

TODAY=$(date +%Y%m%d)
if [[ "$TODAY" < "20260219" || "$TODAY" > "20260422" ]]; then
    echo "$(date): Outside date range ($TODAY), skipping."
    exit 0
fi

echo "$(date): Starting DraftKings ADP fetch (profile: ${DK_CHROME_PROFILE:-Profile 2})..."
python3 scripts/adp/fetch_draftkings_adp.py
echo "$(date): Done."
