create table if not exists result_cache_bust_outbox (
  race_key text primary key,
  desired_result_signature text not null,
  desired_is_complete integer not null,
  trend_delivered_result_signature text,
  trend_delivered_is_complete integer,
  race_delivered_result_signature text,
  race_delivered_is_complete integer,
  lease_until text,
  updated_at text not null,
  foreign key (race_key) references realtime_race_sources(race_key) on delete cascade
);

create index if not exists idx_result_cache_bust_outbox_pending
  on result_cache_bust_outbox (lease_until, updated_at);
