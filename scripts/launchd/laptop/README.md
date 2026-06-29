# Laptop redundancy launchd jobs (nfl-db)

Backup copies of the Desktop's daily ADP/health scrapes, set to run on the **laptop**
as redundancy. The Desktop stays the primary (always-on, mornings); the laptop reruns
them midday so whichever machine is awake captures the data. All upserts are idempotent
(`resolution=merge-duplicates` keyed by source+date), so both machines running the same
day just overwrite the same rows — no duplicates.

| Plist | Runs | Desktop counterpart |
|-------|------|---------------------|
| `com.nfldb.laptop-drafters-postdraft-adp` | 13:00 | daily-drafters-postdraft-adp (08:25) |
| `com.nfldb.laptop-underdog-postdraft-adp` | 13:05 | daily-underdog-postdraft-adp (08:20) |
| `com.nfldb.laptop-draftkings-postdraft-adp` | 13:10 | daily-draftkings-postdraft-adp (08:30) |
| `com.nfldb.laptop-health-check` | 14:00 | daily-health-check (12:00) |

## Laptop-specific differences vs the Desktop plists

- Paths use `/Users/danielhindery/` (Desktop uses `/Users/dan/`).
- Python is the repo venv `/Users/danielhindery/dev/nfl-db/.venv/bin/python3` (system
  python3 lacks deps), not `/usr/bin/python3`.
- `SSL_CERT_FILE` / `REQUESTS_CA_BUNDLE` point at the venv's certifi bundle — **required**,
  or urllib fails under launchd with `CERTIFICATE_VERIFY_FAILED`.
- Underdog/DK read the Chrome login from the **Default** profile (set via
  `UD_CHROME_PROFILE` / `DK_CHROME_PROFILE` in `.env`); re-run `setup_*_session.py`
  ~every 2 weeks when 403s return.

## Install / reinstall

```bash
cp scripts/launchd/laptop/com.nfldb.laptop-*.plist ~/Library/LaunchAgents/
for L in com.nfldb.laptop-drafters-postdraft-adp com.nfldb.laptop-underdog-postdraft-adp \
         com.nfldb.laptop-draftkings-postdraft-adp com.nfldb.laptop-health-check; do
  launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/$L.plist
done
launchctl list | grep nfldb.laptop          # verify
launchctl kickstart -k gui/$(id -u)/com.nfldb.laptop-drafters-postdraft-adp   # test-run one
```
