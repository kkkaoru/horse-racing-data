alter table race_running_styles
  add column predicted_corner_front_score real;

alter table race_running_styles
  add column predicted_corner_rank integer;
