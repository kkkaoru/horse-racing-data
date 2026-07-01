alter table race_running_styles
  add column cell_model_key text;

alter table race_running_styles
  add column cell_variant_id text;

create index race_running_styles_cell_lookup_idx
  on race_running_styles (category, kaisai_nen, cell_variant_id, cell_model_key, race_key);

alter table running_style_inference_state
  add column cell_model_key text;

alter table running_style_inference_state
  add column cell_variant_id text;
