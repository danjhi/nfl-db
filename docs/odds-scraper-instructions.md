# NFL Odds Scraper — Instructions for nfl-db Claude Instance

## What This Is

Instructions for building an odds scraping module inside this repo. The goal is daily NFL odds snapshots into Supabase, following whatever patterns already exist here for data pipelines (e.g., the ADP scrapers).

---

## Decisions Made

### Data Source: OddsPapi
- **API:** `https://api.oddspapi.io/v4`
- **Auth:** Query parameter `?apiKey=YOUR_KEY` — no OAuth, no SDK, just `requests`
- **Free tier:** 250 requests/month. All bookmakers, all sports, historical data included.
- **Docs:** https://oddspapi.io

### What to Capture
- **Game lines:** Spread, total, moneyline for all NFL games
- **Player props:** Passing/rushing/receiving yards, TDs, receptions, anytime TD — as many prop types as available
- **Futures & draft odds:** Super Bowl winner, MVP, draft pick markets. Lower frequency (weekly or on-demand). Kalshi (free, no auth) is a potential secondary source for draft pick markets specifically.

### Sportsbook Selection
Store a curated list of books, not all 350+. Filter API responses to only these before inserting:

**Sharp reference:** Pinnacle, Singbet
**Major US books:** DraftKings, FanDuel, BetMGM, Caesars, ESPN BET

Store this list in config so books can be added/removed without touching scraper logic. The exact OddsPapi slug strings for each book need to be confirmed via an exploratory API call — don't assume them.

### Cadence
- Daily snapshots via GitHub Actions (same pattern as existing ADP pipeline)
- All workflows should also support `workflow_dispatch` for manual runs
- Scripts should be season-aware — game lines and props only make sense during NFL weeks 1–18

### Player ID Mapping
Sleeper IDs are canonical. OddsPapi will return player name strings that need to be resolved to Sleeper IDs.

**Approach:** Build a persistent mapping table (`odds_player_map` or similar):
- On each scrape, check incoming player names against the map first
- If no match, fuzzy match against the Sleeper player table (use `rapidfuzz` or similar)
- High confidence matches (≥0.95) auto-insert into the map
- Low confidence matches go to an unresolved queue table for manual review
- Always store the raw player name from the API on prop records, even after mapping, for debugging/remapping later

### Schema Design Principles
- Follow existing table naming and column conventions in this repo
- Use upserts / `ON CONFLICT` on unique constraints so re-runs are idempotent
- Normalize all odds to decimal format on ingestion
- Store `implied_probability` as a computed field or derive at query time
- Foreign key player props and game lines back to existing player and team tables via Sleeper IDs
- Track snapshot dates so the dataset builds historical depth over time

---

## OddsPapi API Details

This is the external knowledge you won't have from the repo.

### Key Endpoints
- `GET /v4/sports` — list available sports with IDs
- `GET /v4/fixtures` — list fixtures (games) for a sport, filterable by league/date
- `GET /v4/odds` — get odds for a fixture, returns all bookmakers in one response

### Response Structure
Odds responses nest like: `bookmakerOdds → {slug} → markets → {marketId} → outcomes → {outcomeId} → players → 0 → price`

Auth is just `?apiKey=YOUR_KEY` on every request.

### Important Unknowns (Investigate First)
Before building the full pipeline, make an exploratory API call or two to determine:

1. **Does one `/odds` request per fixture return BOTH game lines and player props?** Or are props a separate call? This determines whether 250 free requests/month is sufficient.
2. **What are the exact sportsbook slug strings?** (e.g., is it `pinnacle` or `Pinnacle` or `pinnacle-sports`?)
3. **What market IDs correspond to NFL spread, total, ML, and common player props?**
4. **How are NFL fixtures identified?** What's the sport ID for NFL? How do fixtures map to weeks?
5. **How are futures/draft markets structured?** Separate sport ID? Different endpoint?

Spend the first few requests on exploration and document what you find before writing the full scraper.

### Rate Limit Budget
250 requests/month is tight. Rough math:
- 16 NFL games/week × ~4 weeks/month = ~64 fixture requests just for game lines
- If player props require separate requests, that could double or triple the count
- Futures/draft: maybe 4-8 requests/month

**If the free tier isn't enough:** Options are upgrading to paid OddsPapi, starting with game lines only, or supplementing with Kalshi's completely free API for draft/futures specifically.

### Kalshi (Optional Secondary Source for Draft Markets)
- **API:** `https://api.elections.kalshi.com/trade-api/v2`
- **No auth required for market data endpoints**
- Draft markets use tickers like `KXNFLDRAFT1` (first pick), `KXNFLDRAFTPICK` (specific picks)
- Prices are $0.00–$1.00 representing implied probability directly
- Could be a nice complement since it's completely free and unlimited for read-only market data

---

## Implementation Order

1. **Explore the API.** Make a few manual requests to OddsPapi, understand the response structure for NFL, document the findings.
2. **Schema.** Design and create the Supabase tables following existing repo conventions. Need tables for: game line snapshots, player prop snapshots, futures snapshots, player name mapping, and unresolved player queue.
3. **Game lines scraper.** Simplest starting point — no player ID mapping needed, just team mapping.
4. **Player mapper.** Build the fuzzy matching utility and mapping table management.
5. **Player props scraper.** Depends on the player mapper being solid.
6. **Futures scraper.** Lower priority, can run less frequently.
7. **GitHub Actions workflows** for each scraper.
8. **Kalshi integration** (optional, later) for draft markets.

---

## What NOT to Do
- Don't store all 350+ books — filter to the curated list
- Don't hardcode API keys — use env vars / GitHub secrets
- Don't silently drop unmatched players — queue them for review
- Don't assume OddsPapi slug strings or market IDs — confirm from actual API responses
