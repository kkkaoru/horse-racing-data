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
