-- finish_position_coverage_gap_events: one row per per-race coverage gap the
-- self-healing cron (coverage-self-heal.ts, doc §4.3 of
-- docs/cf-only-serving-architecture.md) found and acted on -- either
-- re-enqueuing a fresh focused-full message for a race whose expected
-- prediction rows were still missing well after its grace window, or
-- refusing to re-enqueue once the per-race escalation cap
-- (MAX_SELF_HEAL_ENQUEUES_PER_RACE) was reached (escalated = 1, surfaced as
-- a SELF_HEAL_ESCALATE log line for operator/log-monitor visibility). This
-- is the direct functional replacement for race-prediction-guard.sh's
-- day-wide COUNT check, at per-race granularity
-- (feedback_production_per_race_granularity) -- a routine tick that finds
-- every race either complete or legitimately still in flight writes no row
-- here; only actionable gap-fill or escalation events are logged. Insert-only
-- audit (feedback_no_data_delete) -- never DELETE / TRUNCATE / DROP.
create table if not exists finish_position_coverage_gap_events (
  id integer primary key autoincrement,
  run_ymd text not null,
  category text not null,
  keibajo_code text not null,
  race_bango text not null,
  race_start_at_jst text not null,
  prior_enqueue_count integer not null,
  enqueued integer not null,
  escalated integer not null,
  recorded_at text not null default (datetime('now'))
);

create index if not exists finish_position_coverage_gap_events_run_ymd_idx
  on finish_position_coverage_gap_events (run_ymd);
