-- Create oversea_runner_identity: source-native identity metadata for overseas race runners.
--
-- Root cause:
-- JV stores many overseas runners without an issued ketto_toroku_bango. Those entries correctly
-- retain the all-zero JV placeholder, so that value cannot identify an individual horse and must
-- never be replaced with a synthetic JV key. A synthetic value could collide with a real code
-- issued later and contaminate horse history and prediction features.
--
-- Scope:
-- Each row augments one race entry, identified by the JV race key plus umaban, with an identity
-- from an external source. The source and source_horse_id remain separate from JV identifiers;
-- full display names and provenance can therefore be retained without mutating jvd_se. The primary
-- key is intentionally the complete race-entry key. Tables without a primary key are silently
-- omitted from Neon synchronization.
--
-- Precedent:
-- 20260725000000_hand_ingest_ascot_king_george.sql preserves unresolved all-zero JV identifiers
-- for overseas runners. 20260607000000_create_race_entry_finish_vectors.sql demonstrates an
-- additive, source-scoped auxiliary table with an explicit primary key and idempotent DDL.
--
-- Deployment:
-- There is no migration runner. Apply this file manually to BOTH local PostgreSQL and Neon.
-- push-neon-sync does not synchronize DDL; if Neon lacks this table, synchronization of this table
-- fails independently. Do not insert dependent identity rows until both schemas have been updated.
--
-- Idempotent and additive only: re-runs are safe. This migration performs no DELETE, TRUNCATE,
-- DROP, or mutation of JV source tables.

begin;

create table if not exists oversea_runner_identity (
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
  primary key (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban)
);

create index if not exists oversea_runner_identity_source_horse_idx
  on oversea_runner_identity (source, source_horse_id);

commit;
