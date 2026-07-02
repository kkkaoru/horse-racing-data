-- Store target-specific detailed metrics for cell training evaluations.
--
-- Shared scalar columns remain the adoption-compatible surface:
-- finish-position uses top/place metrics directly, while running-style maps
-- top1=accuracy, place2=top2_accuracy, place3=macro_f1. The JSONB payload keeps
-- target-native details such as running-style per-class scores and race-level
-- corner/finish metrics without changing the primary key.

begin;

alter table cell_training_evaluations
  add column if not exists metric_payload jsonb not null default '{}'::jsonb;

commit;
