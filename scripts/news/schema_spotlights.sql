-- Player Spotlight articles scraped from footballguys.com/articles?category=15.
-- One row per article; player_id resolved from the title's "{Player}: ..." prefix
-- (nullable when unresolved). Insert-only on url, mirroring news_items, so any
-- later human edits survive re-runs. RLS on with no anon policy (service-key
-- reads only), consistent with news_items.
-- Applied 2026-07-11 via pg8000 (Management API PAT expired); see
-- fetch_fbg_spotlights.py for the daily scrape.

create table if not exists spotlight_articles (
    id uuid primary key default gen_random_uuid(),
    player_id text references players(player_id),
    title text not null,
    url text not null unique,
    photo_url text,
    author text,
    published_at date,
    year integer not null,
    created_at timestamptz not null default now()
);

alter table spotlight_articles enable row level security;
