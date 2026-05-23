-- Migration: WIN5 schedules and predictions

create table win5_schedules (
  kaisai_nen text not null,
  kaisai_tsukihi text not null,
  sale_deadline text,
  source text not null,
  legs_json text not null,
  fetched_at text not null,
  primary key (kaisai_nen, kaisai_tsukihi)
);

create index win5_schedules_year_idx on win5_schedules (kaisai_nen);

create table win5_predictions (
  kaisai_nen text not null,
  kaisai_tsukihi text not null,
  model_version text not null,
  recommended_budget_yen integer not null,
  default_budget_yen integer not null default 2000,
  prediction_json text not null,
  predicted_at text not null,
  primary key (kaisai_nen, kaisai_tsukihi, model_version)
);

create index win5_predictions_date_idx on win5_predictions (kaisai_nen, kaisai_tsukihi);

create table win5_inference_state (
  kaisai_nen text not null,
  kaisai_tsukihi text not null,
  status text not null,
  attempt_count integer not null default 0,
  last_error text,
  updated_at text not null,
  primary key (kaisai_nen, kaisai_tsukihi)
);
