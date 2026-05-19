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
