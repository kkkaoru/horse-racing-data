-- Migration: race_running_styles
-- Stores per-race per-horse running-style predictions populated by the
-- pc-keiba-viewer backfill script (Phase G/H MVP). Read by the viewer
-- race-detail and horse-detail pages.

create table race_running_styles (
  race_key text not null,
  horse_number integer not null,
  ketto_toroku_bango text not null,
  bamei text,
  category text not null,
  kaisai_nen text not null,
  model_version text not null,
  p_nige real not null,
  p_senkou real not null,
  p_sashi real not null,
  p_oikomi real not null,
  predicted_label text not null,
  predicted_at text not null,
  primary key (race_key, horse_number)
);

create index race_running_styles_race_key_idx
  on race_running_styles (race_key);

create index race_running_styles_horse_history_idx
  on race_running_styles (ketto_toroku_bango, predicted_at);

create index race_running_styles_category_year_idx
  on race_running_styles (category, kaisai_nen);
