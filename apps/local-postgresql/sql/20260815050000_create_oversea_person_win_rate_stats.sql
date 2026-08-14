-- Precomputed JV win-rate populations for overseas race displays.
-- Runtime all-venue aggregation can exceed the viewer statement timeout, so
-- snapshots retain their explicit population and calculation cutoff.
-- This is a one-off A8/04 snapshot, not a local scheduled production process.
-- A recurring design must be implemented on Cloudflare if future overseas
-- races require it.

create table if not exists oversea_person_win_rate_stats (
  race_source text not null,
  kaisai_nen text not null,
  kaisai_tsukihi text not null,
  keibajo_code text not null,
  race_bango text not null,
  category text not null,
  name text not null,
  current_horse_numbers text not null,
  stats_source text not null,
  scope text not null,
  years smallint not null,
  minimum_starts smallint not null default 20,
  calculated_through date not null,
  calculated_at timestamptz not null default now(),
  starts integer not null,
  horse_count integer not null,
  win_count integer not null,
  quinella_count integer not null,
  show_count integer not null,
  win_rate numeric(5, 1) not null,
  quinella_rate numeric(5, 1) not null,
  show_rate numeric(5, 1) not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (
    race_source,
    kaisai_nen,
    kaisai_tsukihi,
    keibajo_code,
    race_bango,
    category,
    name
  ),
  constraint oversea_person_win_rate_stats_category check (
    category in ('jockey', 'trainer', 'owner')
  ),
  constraint oversea_person_win_rate_stats_source check (stats_source = 'jv'),
  constraint oversea_person_win_rate_stats_scope check (scope = 'all_venues_all_conditions'),
  constraint oversea_person_win_rate_stats_years check (years > 0),
  constraint oversea_person_win_rate_stats_minimum_starts check (minimum_starts = 20),
  constraint oversea_person_win_rate_stats_counts check (
    starts >= 20
    and horse_count >= 0
    and win_count between 0 and starts
    and quinella_count between win_count and starts
    and show_count between quinella_count and starts
  )
);
