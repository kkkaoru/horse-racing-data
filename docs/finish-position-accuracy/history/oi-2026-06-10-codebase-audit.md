---
iteration: 37
date: 2026-06-10T10:40:00+09:00
based_on_iteration: 36
follows: history/oi-2026-06-10-iter36-lgb-lambdarank-residual-C-adopt.md
lever: L-codebase-audit (systematic feature-pipeline audit: train/serve NULL skew inventory + guard-window hardening)
status: PARTIAL FIX + IMAGE REFRESH — 2 features fully revived (futan/barei COALESCE, commit 956a5c4); 3 features still structurally NULL at 03:00 cron (bataiju, pending realtime path); 1 skew confirmed NON-ISSUE (shusso_tosu); odds skew remains open (CRITICAL, realtime-sourcing under feasibility study)
quality_gate: n/a — no model retrain this round; only the feature-pipeline fix (956a5c4) + guard-window hardening (8c1971f); lefthook pre-commit passed on both commits
model_version_jra: iter14-jra-cb-pacestyle-course-v8 + per-class ensembles (UNCHANGED — no JRA card on 2026-06-10)
model_version_nar: per-class production config (UNCHANGED — iter36 C ensemble + iter30 other/A/NEW/MUKATSU + iter12 B fallback)
scope:
  venue: 大井 Ōi (keibajo_code=44) — headline 2026-06-10 card (12 races)
  target_card: 2026-06-10
  routing: Ōi C×5 (R1,2,6,8,9) / other×4 (R3,4,5,11) / B×3 (R7,10,12) — UNCHANGED from iter36
  goal: identify and fix inference-time NULL skews that widen the train/serve gap, starting from the iter26 relationship-R1 layer (12 new features, 260→272 columns)
audit_findings:
  odds_skew:
    severity: CRITICAL
    description: "local PG (horse_racing@127.0.0.1:15432) has no intra-day odds updates; the cron fires at JST 03:00 before the morning line is published, so jra_odds / nar_odds are NULL for ~100% of upcoming races at inference vs trained on settled odds"
    status: OPEN — realtime-odds sourcing under feasibility study; guard-window hardened in 8c1971f as recovery infra (10:00–18:00 window re-runs with fresh odds if guard detects race-day divergence)
    commit: 8c1971f
  futan_barei_skew:
    severity: FIXED
    description: "futan_per_barei and barei_diff_from_race_mean were 100% NULL at inference for UPCOMING races; the add-relationship-r1-features.py script joined only pg.race_entry_corner_features (rec) which has no rows for future races, without a COALESCE fallback to nvd_se / jvd_se; in training both fields were populated (completed races have rec rows)"
    fix: "COALESCE(rec.futan_juryo::double, try_cast(nullif(trim(se.futan_juryo),'') as double)/10.0) and COALESCE(rec.barei::double, try_cast(nullif(trim(se.barei),'') as double)) — identical pattern to kyori which already had the COALESCE"
    null_rate_before: "100% NULL for upcoming races"
    null_rate_after: "0% NULL — all 518 horses on 2026-06-10 NAR card populated"
    commit: 956a5c4
  bataiju_skew:
    severity: PENDING
    description: "5 bataiju-derived features (bataiju_futan_ratio, bataiju_diff_from_race_mean, bataiju_rank_in_race, futan_minus_bataiju_zscore_in_race, bataiju_per_kyori_log) are structurally NULL at JST 03:00 cron because bataiju comes from nvd_se.bataiju which is published with the morning card, not available at 03:00; the futan/barei fix does NOT help bataiju"
    status: OPEN — requires realtime/late-data path (horse weight published same-day; a guard-window re-run after weight publish time could fill it)
  shusso_tosu_skew:
    severity: NON-ISSUE
    description: "suspected NULL skew on shusso_tosu (field size) — verified as NON-ISSUE; shusso_tosu is cast to NULL via canonical-NULL re-emit in add-near-miss-features.py (commit 6b21e03); the model was already retrained after that commit so train/serve are consistent; zero accuracy impact confirmed"
    commit: 6b21e03
    status: CLOSED
rebuild:
  image: finish-position-predict-local:split2
  trigger: "956a5c4 baked into COPY layer #15 (finish-position-features directory)"
  build_head: 956a5c4
  verification: "docker run --rm --entrypoint grep ... -n 'coalesce' /app/pipeline/finish-position-features/add-relationship-r1-features.py → lines 164–172 confirmed"
smoke:
  db: horse_racing_smoke (throwaway Neon DB, created and populated solely for this run)
  neon_url: horse_racing_smoke (separate from production neondb)
  source_url: local horse_racing@127.0.0.1:15432 (read-only)
  exit_code: 0
  races_predicted: 518
  error_lines: 0  # no score-error / member-column-gap / member-metadata-missing / ensemble-fallback
  routing_verified:
    C_5_races: iter36-nar-lgb-ensemble-C-v8
    other_4_races: iter30-nar-cb-ensemble-other-v8
    B_3_races: iter12-nar-xgb-hpo-v8
  feature_check:
    futan_per_barei_null: 0  # was 100% before fix; 0% after — 518/518 populated
    barei_diff_from_race_mean_null: 0  # was 100% before fix; 0% after — 518/518 populated
production:
  run_mode: "RUN_DATE=20260610 bash scripts/launchd/finish-position-predict-daily.sh"
  exit_code: 0
  races_predicted: 518
  prediction_generated_at: "2026-06-10T01:34:25Z to 2026-06-10T01:34:28Z (UTC) = ~10:34 JST"
  neon_oi_races: 12  # keibajo_code=44, kaisai_nen=2026, kaisai_tsukihi=0610
  routing_neon:
    R01_C: iter36-nar-lgb-ensemble-C-v8
    R02_C: iter36-nar-lgb-ensemble-C-v8
    R03_other: iter30-nar-cb-ensemble-other-v8
    R04_other: iter30-nar-cb-ensemble-other-v8
    R05_other: iter30-nar-cb-ensemble-other-v8
    R06_C: iter36-nar-lgb-ensemble-C-v8
    R07_B: iter12-nar-xgb-hpo-v8
    R08_C: iter36-nar-lgb-ensemble-C-v8
    R09_C: iter36-nar-lgb-ensemble-C-v8
    R10_B: iter12-nar-xgb-hpo-v8
    R11_other: iter30-nar-cb-ensemble-other-v8
    R12_B: iter12-nar-xgb-hpo-v8
artifacts:
  smoke_db: horse_racing_smoke (Neon, throwaway)
  baked_script: /app/pipeline/finish-position-features/add-relationship-r1-features.py (inside image)
  prediction_log: ~/Library/Logs/finish-position-predict/20260610.log
---

## What was audited

This round is a systematic codebase audit of the feature pipeline's train/serve NULL skew: inspecting every feature computed in `add-relationship-r1-features.py` (the iter26 layer, 260→272 columns) to identify which values are NULL at inference time (upcoming races) but non-NULL during training (completed races). The audit covers four categories of skew, and the two that can be fixed cheaply within the existing pipeline were addressed in this round.

## Findings

### 1. Odds skew — CRITICAL, OPEN

The `jra_odds` and `nar_odds` columns (market signals) are sourced from the local PG database at cron time (JST 03:00). The morning line is not yet published at 03:00, so these fields are NULL for essentially all upcoming races. The models were trained on settled pre-race odds. This is the most impactful skew in the pipeline — market signal is typically the highest-importance feature cluster.

**Recovery infrastructure** was added in commit `8c1971f`: the guard-window launchd agent (`race-prediction-guard.sh`) was extended to cover the 10:00–18:00 JST window, so when NAR races are published during the day the guard can trigger a re-run that picks up updated odds. This does not fully close the skew (it depends on when odds are ingested into the local PG), but it narrows the window from 24h to at most a few hours.

**Realtime-odds sourcing** via Cloudflare's sync-realtime-data-hot Worker (which serves the Viewer's live odds display) is under feasibility study as a second-leg path: the container could query this endpoint at build time for odds published after the 03:00 cron. This path is not yet implemented.

### 2. futan/barei COALESCE — FIXED (commit 956a5c4)

`futan_per_barei` (= futan_juryo / barei) and `barei_diff_from_race_mean` (= barei − race-mean barei) were **100% NULL** at inference for upcoming races. The root cause: `add-relationship-r1-features.py` built `base_input` by joining `pg.race_entry_corner_features` (rec) as the primary source, with no fallback. For upcoming races (finish_position IS NULL), the rec table has no rows because it is populated after races complete. `barei` and `futan_juryo` must therefore be sourced from `nvd_se` / `jvd_se` (the se table), exactly the same pattern `kyori` was already handling via `COALESCE(rec.kyori::double, b.kyori::double)`.

The fix adds identical COALESCE fallbacks:

```sql
coalesce(rec.futan_juryo::double,
         try_cast(nullif(trim(se.futan_juryo), '') as double) / 10.0) as futan_juryo,
coalesce(rec.barei::double,
         try_cast(nullif(trim(se.barei), '') as double))              as barei,
```

(futan_juryo is stored in 0.1 kg units in se, hence the `/10.0` divisor — same convention as the base parquet builder.)

Post-fix: **0 NULL out of 518 horses** for both features on the 2026-06-10 NAR card. The fix is bit-identical for completed races (rec rows present → same value as before).

### 3. bataiju skew — PENDING

The five bataiju-derived features (`bataiju_futan_ratio`, `bataiju_per_kyori_log`, `bataiju_diff_from_race_mean`, `bataiju_rank_in_race`, `futan_minus_bataiju_zscore_in_race`) remain NULL at JST 03:00. Unlike `futan_juryo` and `barei`, which are published with the entry declaration (days before the race), `bataiju` (horse body weight at weigh-in) is measured on race day, typically published in the late morning or early afternoon. The se table has no bataiju value at 03:00.

The guard-window re-run mechanism partially addresses this: a re-run at 12:00 JST (after bataiju is typically published) would pick up the weight. Whether the guard actually has the bataiju ingested at that time depends on the sync-realtime-data pipeline. This remains an open item.

### 4. shusso_tosu — confirmed NON-ISSUE (closed)

The suspected NULL skew on `shusso_tosu` (field size) was investigated and confirmed **not an active skew**. The field was canonicalized to NULL via the re-emit fix in `add-near-miss-features.py` (commit `6b21e03`); the affected models were retrained after that commit, so train and serve are consistent on this column. Zero accuracy impact was confirmed — the `shusso_tosu` inverse-encoding bug was already in both training data and inference before `6b21e03`, so the net effect on the accuracy delta was zero. The column is now consistently NULL at both train and serve time, which is benign.

## Production refresh

The image `finish-position-predict-local:split2` was rebuilt from HEAD `956a5c4`. Docker layer `#15` (`COPY apps/pc-keiba-viewer/src/scripts/finish-position-features`) was re-executed (DONE, not CACHED), baking the COALESCE fix into the image. The baked fix was confirmed by `grep` inside the running image (lines 164–172 of `/app/pipeline/finish-position-features/add-relationship-r1-features.py`).

The production daily wrapper was re-run (`RUN_DATE=20260610`) and completed with exit 0, `races_predicted=518`. All 12 Ōi races are present in Neon with `prediction_generated_at` ≈ 10:34 JST (after the 07:41 JST previous run), confirming the UPSERT refreshed the predictions with the fixed features. Routing unchanged: C×5 → iter36-nar-lgb-ensemble-C-v8, other×4 → iter30-nar-cb-ensemble-other-v8, B×3 → iter12-nar-xgb-hpo-v8.

## Smoke verdict

| check                               | result                                    |
| ----------------------------------- | ----------------------------------------- |
| exit code                           | 0                                         |
| races_predicted                     | 518                                       |
| score-error lines                   | 0                                         |
| member-column-gap lines             | 0                                         |
| member-metadata-missing lines       | 0                                         |
| ensemble-fallback lines             | 0                                         |
| Ōi C routing                        | iter36-nar-lgb-ensemble-C-v8 (5 races)    |
| Ōi other routing                    | iter30-nar-cb-ensemble-other-v8 (4 races) |
| Ōi B routing                        | iter12-nar-xgb-hpo-v8 (3 races)           |
| futan_per_barei NULL rate           | 0/518 (was 518/518 before fix)            |
| barei_diff_from_race_mean NULL rate | 0/518 (was 518/518 before fix)            |

## Next steps

The remaining open items after this round, in priority order:

1. **Realtime-odds sourcing** — The odds skew is the largest unaddressed train/serve gap. The feasibility study should evaluate reading from the sync-realtime-data-hot Worker endpoint at feature-build time, or from a dedicated odds snapshot in R2.
2. **bataiju late-data path** — The guard-window re-run at 12:00 JST captures bataiju if it is ingested by then. Confirm the ingestion timing and verify that the guard's 10:00–18:00 window fires at a point where bataiju is available in the local PG.
3. **Full model retrain with fixed features** — Both `futan_per_barei` and `barei_diff_from_race_mean` were 100% NULL during training for upcoming races but non-NULL during training for completed races, creating a train/serve skew in the opposite direction (train saw some NULLs, serve now sees none). A full retrain with the COALESCE-fixed pipeline would close this gap and is the next lever for the Wave 2+ accuracy loop.

## Quality Gate Results

- tsc: n/a — no TypeScript changes this round
- lint: n/a — no TypeScript changes this round
- format:check: n/a — no TypeScript changes this round
- test:coverage: n/a — no enforced-package TS file modified (pc-keiba-viewer Python scripts are exempt from the TS gate)
- python:check: lefthook pre-commit passed on commit 956a5c4 (ruff + ty + basedpyright + pytest --cov-fail-under=95 green)
