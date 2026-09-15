# Laptop launchd jobs (nfl-db)

Daily ADP/health jobs that run on the **laptop** (the primary device going forward).
The postdraft-ADP + health jobs began as midday redundancy copies of the Desktop's
morning scrapes (Desktop stays primary for those, always-on mornings; the laptop
reruns them midday so whichever machine is awake captures the data). The
footballguys.com/adp own-source scrapers (`rtsports`, `nffc`) are **laptop-primary** —
there is no Desktop counterpart. All upserts are idempotent
(`resolution=merge-duplicates` keyed by source+date), so a machine running the same
day just overwrites the same rows — no duplicates.

| Plist | Runs | Desktop counterpart |
|-------|------|---------------------|
| `com.nfldb.laptop-team-refresh` | Tue 09:20 (weekly) | daily-team-refresh (08:15, gated Feb 19 to Apr 22) — laptop job is the in-season half: Sleeper API -> `players.latest_team`, gated `20260901`–`20270215`, writes only real team changes, never nulls (Dan 2026-09-15, after 72 stale teams surfaced on the DTVC chart) |
| `com.nfldb.laptop-drafters-postdraft-adp` | 13:00 | daily-drafters-postdraft-adp (08:25) |
| `com.nfldb.laptop-underdog-postdraft-adp` | 13:05 | daily-underdog-postdraft-adp (08:20) |
| `com.nfldb.laptop-draftkings-postdraft-adp` | 13:10 | daily-draftkings-postdraft-adp (08:30) |
| `com.nfldb.laptop-rtsports-adp` | 13:15 | — (laptop-primary; `source=rtsports`) |
| `com.nfldb.laptop-nffc-adp` | 13:20 | — (laptop-primary; `source=nffc_oc` + `nffc` + `bestball10s`) |
| `com.nfldb.laptop-espn-adp` | 13:25 | — (laptop-primary; `source=espn`) |
| `com.nfldb.laptop-cbs-adp` | 13:30 | — (laptop-primary; `source=cbs`) |
| `com.nfldb.laptop-yahoo-adp` | 13:35 | — (laptop-primary; `source=yahoo`) |
| `com.nfldb.laptop-fbg-news` | 13:40 | — (laptop-primary; scrapes /updates → `news_items`, **no date gate**) |
| `com.nfldb.laptop-kashuba-prospects` | 11:20 | — (laptop-primary; Mike Kashuba's 2027 prospect Google Sheet → `draft_prospects` value/sf_value via `scripts/draft_prospects/sync_kashuba_sheet.py --apply`, **no date gate**; needs `GOOGLE_APPLICATION_CREDENTIALS` → ff-stat-sim's `.gcp-sheets-sa.json`, set in the plist) |
| `com.nfldb.laptop-health-check` | 14:00 | daily-health-check (12:00) |

The two own-source scrapers are date-gated `20260710`–`20260910` (draft season) inline;
edit the gate in the plist to extend. `nffc` uses `game_type_id=936` (2026 FBG Online
Championship) — bump yearly.

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
for L in com.nfldb.laptop-team-refresh com.nfldb.laptop-drafters-postdraft-adp com.nfldb.laptop-underdog-postdraft-adp \
         com.nfldb.laptop-draftkings-postdraft-adp com.nfldb.laptop-rtsports-adp \
         com.nfldb.laptop-nffc-adp com.nfldb.laptop-espn-adp com.nfldb.laptop-cbs-adp \
         com.nfldb.laptop-yahoo-adp com.nfldb.laptop-fbg-news com.nfldb.laptop-kashuba-prospects com.nfldb.laptop-health-check; do
  launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/$L.plist
done
launchctl list | grep nfldb.laptop          # verify
launchctl kickstart -k gui/$(id -u)/com.nfldb.laptop-nffc-adp   # test-run one
```
