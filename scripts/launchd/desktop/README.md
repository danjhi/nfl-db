# Desktop-primary scrape schedule

## Recovery checklist (state as of 2026-07-26)

The desktop's INSTALLED jobs are still the previous generation (old
`/Users/dan` plists, health check at 12:00; see the vault note "Daily
Scrape Jobs - launchd Backup") — this directory's schedule was never
installed there. Its underdog fetch has logged "playwright not
installed" for weeks (interpreter without the package); the laptop's
10:00 pass covered the data daily, so only the desktop's own log/email
noise gave it away. When the desktop is next awake:

1. `cd ~/dev/nfl-db && git pull` — machine-labeled health emails, AUTH
   classification, self-diagnosing fetcher errors, hardened installer.
2. `.venv/bin/python3 -c "import playwright"` — if it fails:
   `.venv/bin/python3 -m pip install playwright && .venv/bin/python3 -m playwright install chromium`
3. `cd ~/dev/sleeper-scrape && git pull` — concurrent trades refresh,
   adp preflight, push self-heal.
4. `bash scripts/launchd/desktop/install.sh` — preflight now refuses to
   install over a venv missing playwright/httpx.
5. If the first DK run 401s, its `data/dk_session.json` is on its own
   expiry clock: re-run `setup_dk_session.py` on that machine.

The desktop owns every daily scrape, early morning. The laptop runs the
same jobs later in the morning as redundancy (see `../laptop/`), and the
ADP site rebuild fires from the laptop at 11:45 after FBG's 11:30 ET feed
import (see `fbg-adp-demo/launchd/`).

| Time | Job | Notes |
|---|---|---|
| 7:00 | team refresh | retimed in place by install.sh |
| 7:05 / 7:10 / 7:15 | best ball trio (Underdog, Drafters, DraftKings) | Apr 27 – Sep 10 gate |
| 7:20 | Sleeper trio (~65 min) | trades → drafts → compute ADP |
| 7:25–7:45 | RTSports, NFFC (3 boards), ESPN, CBS, Yahoo | Jul 10 – Sep 10 gate |
| 7:50 / 7:55 | News and Notes, Player Spotlights | year-round, insert-only |
| 8:45 | nfl-db health check | after the Sleeper trio finishes |

Install/update on the desktop:

```bash
cd ~/dev/nfl-db && git pull
bash scripts/launchd/desktop/install.sh
```

The installer also retires the pre-draft trio (contests locked before the
April NFL draft) and rewrites paths for this machine's $HOME. These plists
were generated from the proven laptop set — same venv-python inline
pattern, `-u`, `signal.alarm` caps, and date gates.

Note on news timing: FBG's publish time for the day's News and Notes issue
isn't pinned down; if 7:50 turns out to catch yesterday's issue, the
laptop's 11:10 redundancy pass catches same-day, and insert-only dedupe
makes the double-scrape free.
