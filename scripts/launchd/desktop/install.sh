#!/bin/bash
# Install the desktop-primary scrape schedule (7:00-7:55 block + 8:45 health).
#
# Run ON THE DESKTOP after `git pull`:
#   bash scripts/launchd/desktop/install.sh
#
# What it does:
#   1. Retires the pre-draft trio (contests locked before the April NFL
#      draft; gone for good): unloads + deletes those plists.
#   2. Retimes the existing team-refresh job to 7:00 in place.
#   3. Installs/updates the 12 plists in this directory (paths rewritten
#      to this machine's $HOME), replacing the old 8:20-8:45 schedule.
#   4. Reloads everything and prints the resulting schedule.
#
# Prereqs on this machine: ~/dev/nfl-db with .venv and .env (needs
# SUPABASE_SERVICE_ROLE_KEY for news + spotlights), ~/dev/sleeper-scrape.

set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LA="$HOME/Library/LaunchAgents"

echo "== preflight =="
[ -d "$HOME/dev/nfl-db/.venv" ] || { echo "FATAL: ~/dev/nfl-db/.venv missing"; exit 1; }
[ -d "$HOME/dev/sleeper-scrape" ] || { echo "FATAL: ~/dev/sleeper-scrape missing"; exit 1; }
grep -q '^SUPABASE_SERVICE_ROLE_KEY=' "$HOME/dev/nfl-db/.env" \
  || echo "WARN: SUPABASE_SERVICE_ROLE_KEY missing from nfl-db/.env (news + spotlights need it)"

echo "== retire the pre-draft trio =="
for label in com.nfldb.daily-adp com.nfldb.daily-drafters-adp com.nfldb.daily-draftkings-adp; do
  if [ -f "$LA/$label.plist" ]; then
    launchctl unload "$LA/$label.plist" 2>/dev/null || true
    rm "$LA/$label.plist"
    echo "  retired $label"
  fi
done

echo "== retime team refresh to 7:00 (in place) =="
if [ -f "$LA/com.nfldb.daily-team-refresh.plist" ]; then
  launchctl unload "$LA/com.nfldb.daily-team-refresh.plist" 2>/dev/null || true
  /usr/libexec/PlistBuddy -c "Set :StartCalendarInterval:Hour 7" \
                          -c "Set :StartCalendarInterval:Minute 0" \
                          "$LA/com.nfldb.daily-team-refresh.plist"
  launchctl load "$LA/com.nfldb.daily-team-refresh.plist"
  echo "  team refresh -> 7:00"
else
  echo "  WARN: com.nfldb.daily-team-refresh.plist not found; skipping"
fi

echo "== install the 7:00 block =="
for src in "$HERE"/com.*.plist; do
  name="$(basename "$src")"
  label="${name%.plist}"
  launchctl unload "$LA/$name" 2>/dev/null || true
  # Rewrite the authoring machine's home to this machine's.
  sed "s|/Users/danielhindery|$HOME|g" "$src" > "$LA/$name"
  plutil -lint "$LA/$name" >/dev/null
  launchctl load "$LA/$name"
  echo "  installed $label"
done

echo "== resulting schedule =="
for f in "$LA"/com.nfldb.daily-*.plist "$LA"/com.sleeper.daily-scrape.plist; do
  [ -f "$f" ] || continue
  h=$(/usr/libexec/PlistBuddy -c "Print :StartCalendarInterval:Hour" "$f" 2>/dev/null || echo "?")
  m=$(/usr/libexec/PlistBuddy -c "Print :StartCalendarInterval:Minute" "$f" 2>/dev/null || echo "?")
  printf "  %02s:%02d  %s\n" "$h" "$m" "$(basename "$f" .plist)"
done | sort
echo "Done. Machine must be awake at fire times; missed jobs run once on wake."
