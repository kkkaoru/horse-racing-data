-- Add per-class routing column to finish_position_active_models.
--
-- Phase B of the per-class JRA model architecture pivot (see
-- docs/finish-position-accuracy/runbook/PER_CLASS_ROUTING.md). The container's
-- predict_lib.per_class.resolve_per_class_model_version reads
-- ``subclass`` to route a race to its per-class winner; rows with subclass NULL
-- are the category-global fallback (current behaviour). PER_CLASS_MODEL_VERSIONS
-- in predict_lib/per_class.py is intentionally empty as of v8 iter 20, so no
-- per-class row needs to be inserted by this migration — existing
-- category-only rows (subclass=NULL) keep serving every class.
--
-- Lookup semantics (consumer side):
--   select model_version
--   from finish_position_active_models
--   where category = $1 and (subclass = $2 or subclass is null)
--   order by subclass nulls last
--   limit 1;
-- so a per-class row, when present, wins over the NULL fallback row.
--
-- Idempotent — re-runs are safe. Per feedback_no_data_delete this migration
-- never DELETEs or TRUNCATEs; it only adds the column + replaces the PK
-- constraint with a coalesce-based unique index that admits one NULL-subclass
-- row per category plus one row per (category, registered subclass).

begin;

alter table finish_position_active_models
  add column if not exists subclass text;

-- Replace the simple ``category`` PK with a uniqueness constraint that admits
-- one NULL-subclass row per category plus one row per (category, subclass).
-- Postgres unique indexes treat NULLs as distinct by default, so we coalesce
-- subclass to '' (a value that can never appear as a real kyoso_joken_code,
-- which is a 3-character string) inside the expression to enforce uniqueness
-- across the NULL fallback row.
alter table finish_position_active_models
  drop constraint if exists finish_position_active_models_pkey;

create unique index if not exists finish_position_active_models_category_subclass_idx
  on finish_position_active_models (category, coalesce(subclass, ''));

commit;
