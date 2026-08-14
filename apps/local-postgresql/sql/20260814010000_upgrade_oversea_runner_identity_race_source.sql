-- Upgrade an already-created oversea_runner_identity table to the race-source-aware schema.
--
-- This compatibility migration is required only when 20260814000000 was applied before race_source
-- and updated_at were added to its canonical definition. Because destructive DDL is prohibited,
-- an empty pre-upgrade table is preserved under the oversea_runner_identity_v0 name instead of
-- dropping or rewriting it. The canonical table is then created with the complete source-scoped PK.
--
-- The guard makes this safe on fresh databases: when race_source already exists, no rename occurs
-- and every following CREATE is a no-op. Apply manually to BOTH local PostgreSQL and Neon;
-- push-neon-sync does not synchronize DDL.

begin;

do $$
begin
  if to_regclass('public.oversea_runner_identity') is not null
     and not exists (
       select 1
       from information_schema.columns
       where table_schema = 'public'
         and table_name = 'oversea_runner_identity'
         and column_name = 'race_source'
     ) then
    if exists (select 1 from oversea_runner_identity limit 1) then
      raise exception 'pre-upgrade oversea_runner_identity must be empty before additive replacement';
    end if;

    alter table oversea_runner_identity rename to oversea_runner_identity_v0;
    alter table oversea_runner_identity_v0
      rename constraint oversea_runner_identity_pkey to oversea_runner_identity_v0_pkey;
    alter index oversea_runner_identity_source_horse_idx
      rename to oversea_runner_identity_v0_source_horse_idx;
  end if;
end $$;

create table if not exists oversea_runner_identity (
  race_source text not null check (race_source in ('jra', 'nar')),
  kaisai_nen text not null,
  kaisai_tsukihi text not null,
  keibajo_code text not null,
  race_bango text not null,
  umaban text not null,
  source text not null check (source in ('jra-van', 'netkeiba')),
  source_horse_id text not null,
  horse_name_full text not null,
  jockey_name_full text,
  trainer_name_full text,
  owner_name_full text,
  source_url text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (race_source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban)
);

create index if not exists oversea_runner_identity_source_horse_idx
  on oversea_runner_identity (source, source_horse_id);

commit;
