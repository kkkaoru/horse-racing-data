-- Source-native pedigree names and IDs for overseas horse identities.
-- Keep this separate from JV horse masters: external IDs are not JV keys, and
-- a source's spelling must remain auditable instead of being normalized in place.

create table if not exists oversea_horse_pedigree (
  source text not null,
  source_horse_id text not null,
  sire_source_id text,
  sire_name text not null,
  sire_sire_source_id text,
  sire_sire_name text not null,
  dam_source_id text,
  dam_name text not null,
  dam_sire_source_id text,
  dam_sire_name text not null,
  source_url text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (source, source_horse_id),
  constraint oversea_horse_pedigree_source_nonempty check (btrim(source) <> ''),
  constraint oversea_horse_pedigree_horse_nonempty check (btrim(source_horse_id) <> ''),
  constraint oversea_horse_pedigree_sire_nonempty check (btrim(sire_name) <> ''),
  constraint oversea_horse_pedigree_sire_sire_nonempty check (btrim(sire_sire_name) <> ''),
  constraint oversea_horse_pedigree_dam_nonempty check (btrim(dam_name) <> ''),
  constraint oversea_horse_pedigree_dam_sire_nonempty check (btrim(dam_sire_name) <> ''),
  constraint oversea_horse_pedigree_url_http check (source_url ~ '^https?://')
);

create index if not exists oversea_horse_pedigree_ancestor_names_idx
  on oversea_horse_pedigree (sire_name, sire_sire_name, dam_sire_name);
