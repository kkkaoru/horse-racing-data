# Overnight FP index (2026-08-16)

One page. If finish-position stalls after midnight, start here.
Do not deploy during racing hours. Count Neon rows, not D1 `completed`.
Git vs production (5 undeployed runtime fixes): `tonight-commit-summary-20260816.md`.
09:09 JRA weight window checklist: `jra-weight-0909-checklist-20260816.md`.
JRA R1 close (cause unknown): `weight-rescore-cause-unknown-20260816.md`.
NAR 12:04 / Ban-ei 13:54 observe-only: `nar-banei-1204-observe-checklist-20260816.md`.
User one-pager: `user-report-20260816.md`.
Work handoff (do not re-raise dropped claims): `handoff-weight-rescore-20260816.md`.

## Start from the symptom

| what you see                                                  | do this first                                                                                                                                                                                             | then read                                                                                                                                     |
| ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Predictions are 0 / D1 says completed but Neon is empty       | Count Neon with the SQL below. Ignore D1 `completed`. Check queue `delivery_paused`.                                                                                                                      | `local-oneshot-recovery-playbook.md`, `neon-primary-session-readonly-20260816.md`, 08-15 handoff (paused queue)                               |
| Rows exist but `prediction_generated_at` is hours old         | Do not redeploy. Check whether a weight/odds rescore should have fired.                                                                                                                                   | `weight-rescore-observe-plan-20260816.md`, `weight-triggered-per-race-20260816.md`, `nar-banei-weight-window-20260816.md`                     |
| Pedigree or market columns are 0 / NaN / NULL                 | JRA per-race `--target-race` kills pedigree (`INNER JOIN jra_um`, `MIN_RACES=5`). Host whole-day rows are healthy. Market NULL on serve is the 08-15 family. `predicted_top1/top3_prob` NULL is **spec**. | `pedigree-target-race-experiment-0401-20260816.md`, `pedigree-score-first-dead-column-20260816.md`, `predicted-prob-null-is-spec-20260816.md` |
| feat-cache MISS                                               | MISS is safer than a bad HIT. Do not seed from `feat-*-v7-final` without counting rows/cols. NAR/Ban-ei 44/44 HIT at 07:49 JST; JRA sparse.                                                               | `feat-cache-seed-blocked-20260816.md`, `feat-cache-healthy-seed-0816.md`, `feat-cache-0412-provenance-20260816.md`                            |
| Container looks dead / `wrangler tail` is 0 exceptions 0 logs | Tail is the Worker, not Python. focused-full returns `accepted` then goes dark.                                                                                                                           | `container-observability-design-20260816.md`, `focused-full-0401-timeout-20260816.md`, `per-race-latency-phase3-log-verify-20260816.md`       |
| PREWARM cron fired but R2 404                                 | success without `parquetKey` was logged as success. Fixes are git-only.                                                                                                                                   | `day-base-prewarm-status-20260816.md`, `day-base-prewarm-failure-sites-20260816.md`                                                           |
| DuckDB / Iceberg dies locally                                 | Need `1.5.5`. 1.5.3 IcebergScan. 1.2.2 ATTACH.                                                                                                                                                            | `local-duckdb-icebergscan-gap-20260816.md`                                                                                                    |
| OOM / memory_limit 2.7GiB                                     | FORCE 8/4. Do not restart `predict_upcoming.py` mid-category.                                                                                                                                             | `local-oneshot-recovery-playbook.md`                                                                                                          |
| Viewer NAR/Ban-ei page looks empty                            | RSC payload has runners. 08-15 Morioka has zero `nvd_*` rows.                                                                                                                                             | this page, "Other notes"                                                                                                                      |
| Ban-ei model is v8 on one race                                | `grade_code == E` → variant base. Not a write failure.                                                                                                                                                    | `banei-83-10-v8-routing-20260816.md`                                                                                                          |
| Tempted to enable day-base split                              | Never enabled. Quality reason exists; team-lead still required. Not tonight.                                                                                                                              | `day-base-split-0812-lineage-20260816.md`, `per-race-latency-phase4-proposal-20260816.md`                                                     |
| Tempted to deploy the container                               | Racing hours 09:40–20:50 banned. Live is `0c76062e`.                                                                                                                                                      | `container-deploy-window-20260816.md`, `tonight-commit-summary-20260816.md`                                                                   |
| A8 / overseas board                                           | Oversea track.                                                                                                                                                                                            | `a8-early-market-board-0712-20260816.md`, `a8-main-generation-20260816.md`                                                                    |

## Tonight's outcome

Neon 80/80 at 05:50 JST (940 rows): JRA 36 + NAR 32 + Ban-ei 12.
Production unchanged: no deploy, no secret put, no queue edit.
First post 09:40 JST. Ban-ei 83/09 and 83/10 scored v8 because
`grade_code == E` maps to variant base (`banei-83-10-v8-routing-20260816.md`).

Coverage is not quality. Tonight's JRA scores are market-free **and**
most JRA pedigree features on production feat-cache HITs are dead.
That is larger than the overnight stall. Do not seed cache from those
HITs. Mechanism work belongs to the optimize track, not this page.

## If generation stops

1. Count Neon, not D1:

```sql
SELECT keibajo_code, COUNT(DISTINCT race_bango), COUNT(*)
FROM race_finish_position_model_predictions
WHERE kaisai_nen = '2026' AND kaisai_tsukihi = '0816'
GROUP BY 1 ORDER BY 1;
```

Expected: `01/04/07=12`, `35=12`, `44=10`, `55=10`, `83=12`.

2. Host one-shot, one category at a time (JRA → NAR → Ban-ei).
   Full steps: `local-oneshot-recovery-playbook.md`.
3. Never restart `predict_upcoming.py` mid-category.
   `build_pipeline()` rmtree's every `feat-<cat>-*` dir.
   Resume from the next layer, then `_score_and_flush_races`.
4. Force memory. Auto/`PIPELINE_MAX_*` cannot raise a cap.
   `PIPELINE_FORCE_MEMORY_GB=8` `PIPELINE_FORCE_THREADS=4` (`9007a6e6`).
   JRA head-to-head used RSS 6.95GB. 3GB OOM'd.
5. DuckDB must be `1.5.5` (`67440b8b`). 1.5.3 dies on IcebergScan.
   Pin is in both viewer and container `pyproject.toml` / lock / Dockerfile.
6. Neon write: `BEGIN; SET TRANSACTION READ WRITE` (`85bfba82`).
   `default_transaction_read_only=on` with `source=session` is pooler reuse
   (`neon-primary-session-readonly-20260816.md`). Do not `secret put`.
7. Stop colima before a host build. Apple container local PG stays up.
8. Layer counts from `LAYER_CHAIN`: JRA 17 / NAR 10 / Ban-ei 7.

## What the leftover files mean

| path                                 | measured  | use                                                                                                                                                                                                                                                                                         |
| ------------------------------------ | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `feat-jra-layer-16`                  | 490 x 390 | JRA scored body tonight (manual flush)                                                                                                                                                                                                                                                      |
| `feat-nar-v7-final`                  | 333 x 327 | NAR scored body (`rename(final_dir)`)                                                                                                                                                                                                                                                       |
| `feat-ban-ei-v7-final`               | 117 x 271 | Ban-ei scored body                                                                                                                                                                                                                                                                          |
| `feat-jra-v7-final/features.parquet` | 1 x 8     | rescore snapshot, not broken features. Columns: `race_id`, `umaban`, `ketto_toroku_bango`, `tansho_odds`, `tansho_ninkijun`, `odds_score`, `popularity_score`, `weight_diff_from_avg`. Row is `jra:2026:0712:05:11`. Dir mtime 05:38, file 06:13 — a later rescore overwrote the same path. |

Same path, two jobs. `_final_parquet_dir` is the last-layer rename target
_and_ the rescore cache overlay (`features.parquet`). NAR/Ban-ei still
hold the full scored body because nothing overwrote them. JRA was scored
from `layer-16` (never renamed), then a 06:13 rescore wrote the 1x8 file
into `v7-final`. Do not trust the name. Count rows and columns.

## PREWARM / day-base (do not enable tonight)

Design exists, not enabled.

- Cron `30 0 * * *` = 09:30 JST. Too late for 09:40 first post.
- R2 key: `feat-daybase/catalog-v1/{cat}/{ymd}/features.parquet`.
- 08-14/08-15 objects are 404 (`day-base-prewarm-status-20260816.md`,
  `day-base-prewarm-failure-sites-20260816.md`).
- Local fixes, not deployed: payload miss is `error` (`cd90cb73`);
  success without `parquetKey` is failed (`3cd71358`).
- `DAY_BASE_SPLIT_ENABLED` is a Worker secret forwarded into the container.
  The key exists today; the value is unreadable. Empty = full `LAYER_CHAIN`
  even if R2 has a day-base. Same function as
  `build_upcoming_feature_rows_split` / `ensure_day_base`.
- Do not enable it because it is unused. 08-14 restore message is
  `keep split off`. 08-12 outage cause in the handoff is a paused queue,
  not split. Lineage: `day-base-split-0812-lineage-20260816.md`.

## Container deploy (window = user confirmation)

Full checklist: `container-deploy-window-20260816.md`.
Do not run this during 09:40–20:50 JST. 07-18 outage was a mid-card deploy.

Current production version (read-only `wrangler deployments list` at 06:50 JST):

`0c76062e-03c6-4b8b-9a25-a501a4f4c9cc` (100% since 2026-08-14T15:00:58Z).
Emergency previous rollback: `953d086b-4342-42ce-a146-0a5061d51575`.

Local image already built, not shipped: `finish-position-predict-local:duckdb-1.5.5`.
Commits that would ride a deploy: `85bfba82` writable txn, `2139645b`
`PIPELINE_DIR`, `67440b8b` DuckDB 1.5.5, `cd90cb73` / `3cd71358` PREWARM
logging. `9007a6e6` FORCE env is viewer-side; container needs the same
env in the DO `envVars` map before FORCE works in production.

When the window opens:

```sh
cd apps/finish-position-predict-container
bun run artifact:verify -- --artifact-root models --system finish-position
cd ../finish-position-cron
bun run deploy -- --containers-rollout immediate
```

Verify after deploy (read-only):

1. `wrangler deployments list` shows a new version; keep `0c76062e` as rollback.
2. Probe one already-scored race. UPSERT must not drop the 80/80 count.
3. `SHOW transaction_read_only` after `SET TRANSACTION READ WRITE` is `off`.
4. Do not set `DAY_BASE_SPLIT_ENABLED` on the same deploy.

Rollback: Wrangler rollback to `0c76062e-03c6-4b8b-9a25-a501a4f4c9cc`.
Prediction writes are UPSERT-only. Do not DELETE Neon rows.

## JRA pedigree collapse (larger than the stall)

Production feat-cache HITs, counted by optimize (do not re-measure here):

- 08-16 HITs: degenerate **3/10**. JRA **2/2** dead, NAR **0/4** live,
  Ban-ei **1/4**. On dead races, 5 of 7 `pedigree_*` columns are 0.0/NaN.
- 08-15 JRA HITs: degenerate **4/5** (`04/09` live). Not today-only.
- Same family as the 08-15 market-feature serve miss: JRA serve is missing
  a whole input class, not one race.

Provenance (`feat-cache-0412-provenance-20260816.md`, `ae3bd052`):

- 04/12 R2 HIT Last-Modified **04:09:35 JST**. Current Neon 04/12 is the
  **05:04:07 JST** host UPSERT. Different jobs.
- Host `feat-jra-base` at **03:55** already has 15/15 finite
  `pedigree_score_for_race` for 04/12. Those 15 `jvd_um` rows have
  sire/damsire and `data_sakusei_nengappi` 20260720–20260810.
- Late master load does **not** explain a 04:09 cache of 0.0/NaN.
- PUT path copies parquet bytes through base64. It does not recompute
  ranks. Do not PUT tonight.

Not proven (no container stdout): whether the 04:09 writer attached the
same catalog generation the host used at 03:55.

## Observability gap (third stop tonight)

`wrangler tail` showing 0 exceptions is not a healthy container. Python
stdout does not reach the Worker. Missing container logs blocked:

1. Why 04/01 focused-full accepted at 05:51 and stayed MISS at +20 min.
2. Which job wrote the 04:09 04/12 HIT, and which catalog it attached.
3. Whether PREWARM 08-15 `success` + 404 was a swallowed payload or a
   timeout with no last line.

Until container logs are durable, treat production feature quality as
unobservable. Do not flip `DAY_BASE_SPLIT_ENABLED` or seed feat-cache
to "fix" a collapse you cannot see from the Worker.
Design (no impl): `container-observability-design-20260816.md`.

## Other notes from tonight

- Viewer NAR/Ban-ei runners are in the RSC payload. SSR is a skeleton.
  08-15 Morioka (`35`) has zero `nvd_ra`/`nvd_se` rows, so that page is empty.
- All 0816 scores are market-free (`odds-missing`). Do not chase that tonight.
- Weight trigger / queue order: `weight-triggered-per-race-20260816.md`,
  `weight-trigger-queue-proposal-20260816.md`.
- Per-race p50 ~9.9 min is full `LAYER_CHAIN`, not RACE_CHAIN after a warm
  day-base (`per-race-latency-phase4-proposal-20260816.md`).
- `feat-cache` seed is blocked (`feat-cache-seed-blocked-20260816.md`).
- 04/01 focused-full accepted 05:51 and was still MISS at +20 min
  (`focused-full-0401-timeout-20260816.md`).

## Commit map (local, not pushed)

| commit                | what                                            |
| --------------------- | ----------------------------------------------- |
| `9007a6e6`            | FORCE memory/threads                            |
| `85bfba82`            | Neon writable txn                               |
| `2139645b`            | local `PIPELINE_DIR`                            |
| `67440b8b`            | DuckDB 1.5.5 pin                                |
| `aaf533c6` `c7df1b8b` | host playbook                                   |
| `a0f56e20`            | PREWARM failure sites                           |
| `cd90cb73` `3cd71358` | PREWARM cannot log success without an upload    |
| `c51ab570` `ae3bd052` | 04/12 HIT is 04:09; pedigree masters predate it |
