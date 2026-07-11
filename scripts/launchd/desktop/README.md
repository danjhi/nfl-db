# Desktop-primary scrape schedule

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
