-- sync-realtime-data-features 0001 initial schema
-- Tables for running-style prediction, finish-position prediction, and inference state.
-- daily_race_entries は含まない (R2 Parquet に移行)。

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

create table running_style_inference_state (
  race_key text primary key,
  source text not null,
  kaisai_nen text not null,
  kaisai_tsukihi text not null,
  keibajo_code text not null,
  race_bango text not null,
  status text not null,
  features_r2_key text,
  model_version text,
  expected_horse_count integer,
  written_horse_count integer,
  attempted_at text,
  completed_at text,
  error_message text
);

create index running_style_inference_state_status_idx
  on running_style_inference_state (status, kaisai_nen, kaisai_tsukihi);

create index running_style_inference_state_source_date_idx
  on running_style_inference_state (source, kaisai_nen, kaisai_tsukihi);

create table finish_position_inference_state (
  race_key text primary key,
  source text not null,
  kaisai_nen text not null,
  kaisai_tsukihi text not null,
  keibajo_code text not null,
  race_bango text not null,
  status text not null,
  predictions_r2_key text,
  model_version text,
  attempted_at text,
  completed_at text,
  error_message text
);

create index finish_position_inference_state_status_idx
  on finish_position_inference_state (status, kaisai_nen, kaisai_tsukihi);

create index finish_position_inference_state_source_date_idx
  on finish_position_inference_state (source, kaisai_nen, kaisai_tsukihi);

create table race_finish_position_predictions (
  race_key text primary key,
  source text not null,
  predictions_json text not null,
  predicted_at text not null,
  predictor_version text not null
);

create index race_finish_position_predictions_source_idx
  on race_finish_position_predictions (source);
