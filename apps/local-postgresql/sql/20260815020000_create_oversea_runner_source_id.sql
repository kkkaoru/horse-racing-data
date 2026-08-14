-- Root cause: oversea_runner_identity stores one canonical display row per runner,
-- so replacing its single source identity would disconnect histories from another source.
-- Scope: add a non-destructive runner-by-source mapping for horses and people.
-- Precedent: oversea_runner_identity remains the canonical display-name projection.
-- The six race-entry columns logically reference the matching identity row. No
-- foreign key is used because replica refreshes truncate tables independently.

begin;

create table if not exists oversea_runner_source_id (
  race_source text not null check (race_source in ('jra', 'nar')),
  kaisai_nen text not null,
  kaisai_tsukihi text not null,
  keibajo_code text not null,
  race_bango text not null,
  umaban text not null,
  source text not null check (btrim(source) <> ''),
  source_horse_id text not null check (btrim(source_horse_id) <> ''),
  source_jockey_id text check (source_jockey_id is null or btrim(source_jockey_id) <> ''),
  source_trainer_id text check (source_trainer_id is null or btrim(source_trainer_id) <> ''),
  source_owner_id text check (source_owner_id is null or btrim(source_owner_id) <> ''),
  gate_number smallint check (gate_number is null or gate_number >= 1),
  source_url text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (
    race_source,
    kaisai_nen,
    kaisai_tsukihi,
    keibajo_code,
    race_bango,
    umaban,
    source
  ),
  constraint oversea_runner_source_id_url_http check (
    source_url is null or source_url ~ '^https?://'
  )
);

alter table oversea_runner_source_id
  add column if not exists gate_number smallint check (gate_number is null or gate_number >= 1);

create index if not exists oversea_runner_source_id_source_horse_idx
  on oversea_runner_source_id (source, source_horse_id);

commit;
