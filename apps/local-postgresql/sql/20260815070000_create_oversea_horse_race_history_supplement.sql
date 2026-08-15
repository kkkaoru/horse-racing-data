-- Source-separated fields published on an official card after the base horse
-- history was captured. The base history row remains unchanged and retains its
-- own source provenance.
create table if not exists oversea_horse_race_history_supplement (
  -- Logical reference only: a physical FK would prevent replica:push:neon from
  -- truncating and refreshing the parent table.
  history_id bigint not null,
  supplement_source text not null,
  source_url text not null,
  captured_at timestamptz not null,
  country_or_jra_venue_label text,
  grade text,
  field_size smallint,
  gate_number smallint,
  popularity smallint,
  weight_carried_kg numeric(4, 1),
  race_time_text text,
  race_time_seconds numeric(5, 1),
  race_time_parse_status text not null,
  going text,
  -- Ordered comma-separated source values (for example "1,4,3"). Text keeps
  -- generic CSV COPY replication independent of PostgreSQL array handling.
  corner_positions_text text,
  comparison_horse_name text,
  comparison_margin_text text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (history_id, supplement_source),
  constraint oversea_horse_history_supplement_source check (
    supplement_source = 'jra_official'
  ),
  constraint oversea_horse_history_supplement_url check (source_url ~ '^https?://'),
  constraint oversea_horse_history_supplement_label_nonempty check (
    country_or_jra_venue_label is null or btrim(country_or_jra_venue_label) <> ''
  ),
  constraint oversea_horse_history_supplement_grade check (
    grade is null or grade in ('G1', 'G2', 'G3')
  ),
  constraint oversea_horse_history_supplement_field_size check (
    field_size is null or field_size >= 1
  ),
  constraint oversea_horse_history_supplement_gate check (
    gate_number is null or gate_number >= 1
  ),
  constraint oversea_horse_history_supplement_popularity check (
    popularity is null or popularity >= 1
  ),
  constraint oversea_horse_history_supplement_carried_weight check (
    weight_carried_kg is null or weight_carried_kg > 0
  ),
  constraint oversea_horse_history_supplement_time_text check (
    race_time_text is null or race_time_text ~ '^[0-9]+:[0-9]{2}[.][0-9]$'
  ),
  constraint oversea_horse_history_supplement_time_seconds check (
    race_time_seconds is null or race_time_seconds > 0
  ),
  constraint oversea_horse_history_supplement_time_parse_status check (
    race_time_parse_status in ('parsed', 'unparsed', 'missing')
  ),
  constraint oversea_horse_history_supplement_time_parse_consistency check (
    (race_time_parse_status = 'parsed' and race_time_text is not null and race_time_seconds is not null)
    or (race_time_parse_status = 'unparsed' and race_time_text is not null and race_time_seconds is null)
    or (race_time_parse_status = 'missing' and race_time_text is null and race_time_seconds is null)
  ),
  constraint oversea_horse_history_supplement_going_nonempty check (
    going is null or btrim(going) <> ''
  ),
  constraint oversea_horse_history_supplement_corners check (
    corner_positions_text is null or corner_positions_text ~ '^[1-9][0-9]*(,[1-9][0-9]*)*$'
  ),
  constraint oversea_horse_history_supplement_comparison_horse check (
    comparison_horse_name is null or btrim(comparison_horse_name) <> ''
  ),
  constraint oversea_horse_history_supplement_margin check (
    comparison_margin_text is null or btrim(comparison_margin_text) <> ''
  )
);

comment on column oversea_horse_race_history_supplement.race_time_seconds is
  'Parseable source time only; foreign course and timing distributions are not proven comparable to JRA model features.';
comment on column oversea_horse_race_history_supplement.corner_positions_text is
  'Ordered official source values only; foreign corner conventions and straight-course applicability require separate validation.';

create index if not exists oversea_horse_history_supplement_source_idx
  on oversea_horse_race_history_supplement (supplement_source, history_id);
