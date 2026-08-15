# 2026-08-16 local one-shot recovery playbook

Measured on 2026-08-16 03:00–05:40 JST. Use this before restarting a
failed host generation. Do not treat container per-race as a same-night
replacement: measured throughput was about 8 races / 90 minutes.

## What worked

- Host one-shot with DuckDB `1.5.5` + `PIPELINE_FORCE_MEMORY_GB=8`
  `PIPELINE_FORCE_THREADS=4`.
- Category order: JRA then NAR then Ban-ei. Never in parallel.
- Neon writes via existing `SET TRANSACTION READ WRITE`
  (`85bfba82`). Do not `secret put`, swap endpoints, or `ALTER ROLE`.
- After write, count Neon rows. Ignore D1 `completed`.

Measured Neon at 05:50 JST after JRA + NAR + Ban-ei (80/80, 940 rows):

```
01=12  04=12  07=12   JRA 36   latest 05:04:14 UTC
35=12  44=10  55=10   NAR 32   latest 05:31:34 UTC
83=12                 Ban-ei 12 latest 05:50:18 UTC
```

Ban-ei 83/09 and 83/10 scored `banei-cb-v8-window2011-wf-15y` because
`cell_routing.json` maps `grade_code == E` to variant base. Not a
write failure. Do not redeploy to change this tonight.

NAR one-shot writes 32 races, not 44. Ban-ei (`keibajo 83`) is a
separate `PREDICT_CATEGORIES=ban-ei` run.

## Layer counts (code, not guess)

From `LAYER_CHAIN` in
`apps/finish-position-predict-container/src/predict_lib/pipeline_args.py`.
Plus base DuckDB build (`feat-<cat>-base`) and layer-0 copy.

| category | layers | notes                                                    |
| -------- | ------ | -------------------------------------------------------- |
| jra      | 17     | includes course-numerical, kohan3f, jockey-pedigree-cell |
| nar      | 10     | no course-numerical / kohan3f / jockey-pedigree-cell     |
| ban-ei   | 7      | lineage first; unique futan-class + grade-career         |

JRA 17-layer wall after FORCE 8/4: layer-7..16 finished in ~19 minutes
once base–layer-6 were already on disk.

## Do not restart `predict_upcoming.py` mid-category

`build_pipeline()` calls `_reset_category_work_dirs()` and deletes
`feat-<cat>-base` plus every `feat-<cat>-layer-*` for that category.

If base through layer-N already exist:

1. Do not start another `predict_upcoming.py` for the same category.
2. Resume from layer N+1 with the existing `build_layer_argv` scripts.
3. After the last layer parquet exists, call `_score_and_flush_races`.

JRA tonight: base–layer-6 survived an OOM. Restarting the orchestrator
would have thrown away ~40 minutes. Manual resume from layer-7, then
score/flush, wrote 36 races / 490 rows at 05:04:14 UTC.

## Memory rules (measured)

- `_auto_memory_limit_gb()` reads available RAM at process start.
  `PIPELINE_MAX_MEMORY_GB` is a cap (`min(auto, cap)`), not a raise.
- Host OOM text: `failed to pin 256KiB (2.7GiB/2.7GiB used)`.
  That was DuckDB `memory_limit`, not OS jetsam.
- JRA `add-head-to-head-features.py` used RSS 6.95GB. A 3GB or 6GB
  limit is not enough for JRA. NAR head-to-head peaked ~3.2GB.
- `PIPELINE_FORCE_MEMORY_GB` / `PIPELINE_FORCE_THREADS` skip auto and
  `PIPELINE_MAX_*`. Unset / invalid keep the old path (`9007a6e6`).
- `pipeline_runner.py` does not forward CLI `--memory-limit` unless the
  layer argv already contains it. Container / orchestrator path needs
  the env override.
- Most layer scripts already accept `--memory-limit` / `--threads` via
  shared `add_resource_args`. A grep of the script files themselves will
  miss this. Still copy argv from `build_layer_argv`:
  `add-course-numerical-features.py` rejects `--pg-url` / `--from-date`,
  and `add_kohan3f_going_features.py` rejects `--from-date`.
- Stop colima / Docker builds before a host generation. Auto memory is
  computed from available RAM at start; a concurrent build dropped the
  limit to 2.7GiB and OOM'd.
- colima default VM is ~8GiB. `colima stop` frees it. Apple container
  `horse-racing-local-postgresql` is a different runtime and stays up.
- `feat-<cat>-v7-final` is a 1-row / 8-column summary, not features.
  The scored body is the last layer parquet (`feat-jra-layer-16`,
  `feat-nar-layer-9`, `feat-ban-ei-layer-6`).

## DuckDB versions (measured, not assumed)

| version | result against R2 Catalog                                |
| ------- | -------------------------------------------------------- |
| 1.2.2   | `Unhandled options found: default_schema` on ATTACH      |
| 1.5.3   | `IcebergScan serialization not implemented` after ~5 min |
| 1.5.5   | base Iceberg build proceeds                              |

Do not run host generation on whatever `uv.lock` resolved last week.

## Neon write checklist

1. `SHOW default_transaction_read_only` may stay `on` (`source=session`).
2. `BEGIN; SET TRANSACTION READ WRITE; SHOW transaction_read_only`
   must print `off` immediately before flush.
3. After flush, count:

```sql
SELECT keibajo_code,
       COUNT(DISTINCT race_bango),
       COUNT(*),
       MAX(prediction_generated_at)
FROM race_finish_position_model_predictions
WHERE kaisai_nen = '2026' AND kaisai_tsukihi = '0816'
GROUP BY 1
ORDER BY 1;
```

Expected 2026-08-16: `01/04/07=12`, `35=12`, `44=10`, `55=10`, `83=12`.

## Host launch template

```sh
export PIPELINE_DIR="$REPO/apps/pc-keiba-viewer/src/scripts"
export SOURCE_DATABASE_URL='r2-catalog://pc-keiba'
export RUN_DATE=YYYYMMDD
export PREDICT_DAYS_AHEAD=0
export PREDICT_CATEGORIES=jra   # then nar, then ban-ei
export PIPELINE_FORCE_MEMORY_GB=8
export PIPELINE_FORCE_THREADS=4
unset PREDICT_SERVE_MODE
uv run --with 'duckdb==1.5.5' python src/predict_upcoming.py
```

One category at a time. Confirm Neon counts before starting the next.

## What not to do tonight

- Do not deploy the finish-position container during racing hours.
- Do not start a second `predict_upcoming.py` for a category that
  already has layer dirs.
- Do not parallelize JRA / NAR / Ban-ei on the same host.
- Do not trust D1 `finish_position_inference_state.completed`.
- Do not re-score existing Neon rows just to "refresh" unless the
  caller asked. UPSERT is safe but wastes the writable belt.
