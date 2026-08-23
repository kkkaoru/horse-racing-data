create table if not exists finish_position_day_base_repair_requests (
  category text not null,
  run_ymd text not null,
  requested_at text not null,
  primary key (category, run_ymd)
);
