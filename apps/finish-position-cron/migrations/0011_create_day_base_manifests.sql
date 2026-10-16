create table if not exists finish_position_day_base_manifests (
  category text not null,
  run_ymd text not null,
  repair_generation_id text not null,
  status text not null check (status in ('pending', 'published')),
  object_key text,
  object_etag text,
  watermark_json text,
  updated_at text not null,
  primary key (category, run_ymd),
  check (status = 'pending' or (object_key is not null and object_etag is not null and watermark_json is not null))
);
