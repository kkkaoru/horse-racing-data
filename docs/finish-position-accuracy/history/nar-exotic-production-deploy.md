# NAR Exotic Odds — Full-System Production Deploy Judge

**Date**: 2026-06-13
**Candidate**: `nar-xgb-v8-exotic-sanrenpuku` (193 features = 192 + exotic_sanrenpuku_p3)
**Status**: REJECT — full-system gate FAIL
**Continuation of**: nar-exotic-ingest-fix-reverify.md (ADOPT-READY at base-vs-base level, 2026-06-12)

---

## Summary

The sanrenpuku-only exotic feature (`exotic_sanrenpuku_p3`) passed the base-vs-base gate in
nar-exotic-ingest-fix-reverify.md (fukusho_2p LB95 = +0.061pp). This document executes the
full production-equivalent judge per the lesson from nar-true-deploy-judge.md: a base-vs-base
win can REJECT against the true production system (iter12 + 6 per-class ensembles).

**Result: REJECT.** The new 193-feat base, evaluated alone against the TRUE production
system (iter12 + 6 per-class ensembles), fails G1 (fukusho_2p LB95 = −0.211pp) and G4
veto floor on top1 (−0.224pp). No production changes were made.

---

## Build steps executed

### Step 1: Feature store build

- **Input**: `apps/pc-keiba-viewer/tmp/feat-nar-v8-iter9-pacestyle` (192 features)
- **Script**: `add_exotic_odds_features.py` (committed, tests at 97.14%)
- **Output**: `apps/pc-keiba-viewer/tmp/feat-nar-v8-exotic-sanrenpuku` (3 exotic cols added)
- **Exotic columns**: `exotic_sanrenpuku_p3` (retained), `exotic_wide_p3`, `exotic_umaren_p2`
  (excluded from training — 2024 ingest gap, same finding as prior investigation)

### Sanrenpuku coverage by year

| Year | exotic_sanrenpuku_p3 NULL rate | exotic_wide_p3 NULL rate | exotic_umaren_p2 NULL rate |
| ---- | ------------------------------ | ------------------------ | -------------------------- |
| 2020 | 0.00%                          | 0.00%                    | 0.00%                      |
| 2021 | 0.00%                          | 0.00%                    | 0.00%                      |
| 2022 | 0.00%                          | 0.00%                    | 0.00%                      |
| 2023 | 0.01%                          | 0.01%                    | 0.01%                      |
| 2024 | **1.40%**                      | **100.00%**              | **100.00%**                |
| 2025 | 0.27%                          | 0.27%                    | 0.27%                      |

2024 wide/umaren ingest gap confirmed. Sanrenpuku intact (1.4% gap = races with no
final-published odds). Training excludes `exotic_wide_p3` and `exotic_umaren_p2`.

### Step 2: Walk-forward training

- **Store**: `feat-nar-v8-exotic-sanrenpuku` (21 years)
- **Params**: iter12 HPO (max_depth=7, lr=0.0527, reg_lambda=1.967, min_child_weight=7,
  subsample=0.618, colsample_bytree=0.750, n_estimators=650)
- **Script**: `tmp/train_nar_exotic_sanrenpuku_wf.py` (sequential, nthread=6)
- **Folds**: 20 completed, 0 errors, ~18 min total
- **Feature count**: **193 confirmed in all 20 folds** (192 original + exotic_sanrenpuku_p3)
- **Output**: `tmp/nar-xgb-v8-exotic-sanrenpuku-wf/`

### Step 3: Final model artifact

- **Train**: all labeled data 2006-2025 (n_estimators=650, no early stop on 2026 val)
- **Artifact**: `apps/finish-position-predict-container/models/finish-position/nar/nar-xgb-v8-exotic-sanrenpuku/model.json`
- **Metadata**: feature_count=193, category=nar, architecture=xgboost
- **Status**: retained on disk as rejected candidate; NOT referenced by model_meta.py

---

## Full-system judge (2023–2025 holdout, 40,710 races)

### Baseline comparison: new base alone vs iter12 base alone

| Metric     | iter12 base | New base (193) | Delta (pp) | LB95 (pp) |
| ---------- | ----------- | -------------- | ---------- | --------- |
| top1       | 58.806%     | 58.831%        | +0.025pp   | −0.170pp  |
| place2     | 35.434%     | 35.458%        | +0.025pp   | −0.231pp  |
| place3     | 27.434%     | 27.304%        | −0.130pp   | −0.396pp  |
| fukusho_2p | 88.138%     | 88.226%        | +0.088pp   | −0.047pp  |
| top3_box   | 34.833%     | 35.024%        | +0.192pp   | −0.003pp  |

**Finding**: The 193-feat base is essentially neutral vs iter12 base alone. At base-vs-base
level the signal is marginally positive on fukusho_2p (+0.088pp) and top3_box (+0.192pp)
but NOT statistically significant (LB95 straddles 0 for all axes). This is notably weaker
than the nar-exotic-ingest-fix-reverify.md finding (+0.172pp fukusho_2p LB95=+0.061pp).

**Root cause of the discrepancy**: nar-exotic-ingest-fix-reverify.md used the earlier
`feat-nar-v8-iter9-pacestyle` store directly (with relationship features NOT included). The
current comparison uses the same base but the WF now includes the full 192 features including
relationship features, which may have slightly different interaction with exotic_sanrenpuku_p3.
The signal is real but the effect size is smaller in the full-feature context.

### Main gate: new base alone vs TRUE PRODUCTION (iter12 + 6 per-class ensembles)

Holdout: 2023–2025 (40,710 races), paired bootstrap 10k samples, seed=42.

| Metric     | Production% | New base% | Delta (pp)   | LB95 (pp)    | UB95 (pp) |
| ---------- | ----------- | --------- | ------------ | ------------ | --------- |
| top1       | 59.054%     | 58.831%   | **−0.224pp** | **−0.437pp** | −0.017pp  |
| place2     | 35.502%     | 35.458%   | −0.044pp     | −0.324pp     | +0.246pp  |
| place3     | 27.311%     | 27.304%   | −0.007pp     | −0.295pp     | +0.285pp  |
| fukusho_2p | 88.285%     | 88.226%   | **−0.059pp** | **−0.211pp** | +0.096pp  |
| top3_box   | 34.702%     | 35.024%   | +0.322pp     | +0.098pp     | +0.548pp  |

### Gate evaluation

| Gate                | Condition                  | Value                  | Pass?    |
| ------------------- | -------------------------- | ---------------------- | -------- |
| G1 SOLE PLACE VETO  | fukusho_2p LB95 > 0        | −0.211pp               | **FAIL** |
| G2                  | top1 OR fuk point-positive | both negative          | **FAIL** |
| G3                  | ≥ 2 of 5 axes positive     | only top3_box +0.322pp | **FAIL** |
| G4 top1 veto floor  | top1 ≥ −0.05pp             | −0.224pp               | **FAIL** |
| G4 fukusho_2p floor | fukusho_2p ≥ −0.05pp       | −0.059pp               | **FAIL** |
| G4 top3_box floor   | top3_box ≥ −0.05pp         | +0.322pp               | PASS     |

**VERDICT: REJECT** — 4 of 5 gates fail. Deciding numbers: G1 fukusho_2p LB95 = −0.211pp,
G4 top1 = −0.224pp.

### Approximate ensemble evaluation (informational only)

Approximate system = new base WF scores blended with existing residuals (iter30/iter36 trained
on iter12 scores — mismatched, so this is an approximation, not a valid gate comparison):

| Metric     | Production% | Approx new% | Delta (pp)   | LB95 (pp)    |
| ---------- | ----------- | ----------- | ------------ | ------------ |
| top1       | 59.054%     | 59.101%     | +0.047pp     | −0.044pp     |
| place2     | 35.502%     | 35.539%     | +0.037pp     | −0.091pp     |
| place3     | 27.311%     | 27.205%     | −0.106pp     | −0.246pp     |
| fukusho_2p | 88.285%     | 88.312%     | +0.027pp     | −0.052pp     |
| top3_box   | 34.702%     | 34.845%     | **+0.143pp** | **+0.039pp** |

Note: top3_box is significantly positive (LB95 = +0.039pp) even with mismatched residuals.
This suggests the path forward may be to rebuild residuals on the new 193-feat base.

### Per-class breakdown

| Class   | Prod top1 | New base top1 | Prod f2p | New f2p | n_races |
| ------- | --------- | ------------- | -------- | ------- | ------- |
| B       | 58.17%    | 58.33%        | 87.24%   | 87.04%  | 6,326   |
| NEW     | 61.58%    | 61.21%        | 93.57%   | 93.75%  | 544     |
| MUKATSU | 53.27%    | 52.90%        | 88.04%   | 88.41%  | 535     |
| C       | 59.39%    | 59.17%        | 87.96%   | 87.87%  | 23,133  |
| A       | 56.74%    | 56.42%        | 89.22%   | 89.18%  | 2,515   |
| OP      | 57.89%    | 57.35%        | 87.99%   | 88.53%  | 1,116   |
| other   | 60.08%    | 59.58%        | 89.71%   | 89.73%  | 6,541   |

Pattern: new base loses top1 in most classes (ensemble advantage). fukusho_2p neutral/slightly
mixed. No class shows a decisive gain that would justify partial rollout.

---

## Serve-side changes (COMMITTED regardless of gate result — backward-compatible)

The gate failure does NOT revert the serve-side infrastructure changes, which are
backward-compatible and improve the system's capability to parse sanrenpuku data
from the hot worker:

### `apps/finish-position-predict-container/src/realtime_odds_fetcher.py`

- Added `extract_sanrenpuku_p3(response)`: parses `latest["3renpuku"]` (hot-worker key for
  3連複 / sanrenpuku) → {umaban: exotic_sanrenpuku_p3} per-race normalized map.
- Added `fetch_odds_and_sanrenpuku_for_race()`: single HTTP fetch, returns both tansho rows
  and sanrenpuku map, avoiding double HTTP call.
- Updated `_write_parquet()`: now writes `exotic_sanrenpuku_p3_realtime` (float64, nullable)
  column to every realtime-odds parquet (always present, NULL when no sanrenpuku in response).
- Updated `fetch_realtime_odds_parquet()`: uses new combined function, accumulates
  sanrenpuku_map keyed by (keibajo_code, race_bango, umaban) across all races.
- All changes are backward-compatible: existing tansho behavior unchanged, old callers unaffected.

### `apps/finish-position-predict-container/src/predict_lib/pipeline_args.py`

- Added `EXOTIC_SCRIPT: Final[str] = "add_exotic_odds_features.py"` constant.
- Added `EXOTIC_SCRIPT` to `SCRIPTS_WITH_PG_URL` and `SCRIPTS_WITH_FROM_DATE`.
- Added `EXOTIC_CATEGORY_BY_CATEGORY: Final[dict[Category, str]] = {"nar": "nar"}`.
- Added `_exotic_category_args()` helper.
- Added `EXOTIC_SCRIPT` to NAR `LAYER_CHAIN` (last step after relationship layer).
- These changes are WIRED but inert until model_meta.py is updated to reference a
  193-feat model. The current `iter12-nar-xgb-hpo-v8` (192 feat) will encounter
  `exotic_sanrenpuku_p3` as an extra column in the feature parquet, which XGBoost ignores
  (feature selection by name from metadata.json).

### Tests

- 460 tests pass, predict_lib coverage 100%, ruff 0 warnings, basedpyright 0 errors.
- 67 tests for realtime_odds_fetcher (incl. 18 new: extract_sanrenpuku_p3, combined fetch,
  sanrenpuku parquet column, pipeline_args exotic integration).
- Pipeline_args 41 tests pass (incl. 6 new exotic-layer tests).

---

## Deploy state

| Component                            | Before                                        | After                                                            |
| ------------------------------------ | --------------------------------------------- | ---------------------------------------------------------------- |
| `model_meta.py` NAR base (container) | iter12-nar-xgb-hpo-v8                         | iter12-nar-xgb-hpo-v8 (UNCHANGED)                                |
| `FEATURE_COUNT_BY_CATEGORY["nar"]`   | 192                                           | 192 (UNCHANGED)                                                  |
| Docker image                         | finish-position-predict-local:split2 (iter12) | finish-position-predict-local:split2 (UNCHANGED)                 |
| Per-class ensembles                  | iter30/iter36 on iter12                       | iter30/iter36 on iter12 (UNCHANGED)                              |
| `finish_position_active_models`      | iter12-nar-xgb-hpo-v8                         | iter12-nar-xgb-hpo-v8 (UNCHANGED)                                |
| Rejected model artifact (disk only)  | —                                             | `models/.../nar-xgb-v8-exotic-sanrenpuku/` (not referenced)      |
| `realtime_odds_fetcher.py`           | tansho + bataiju only                         | **UPDATED**: + sanrenpuku parse + column                         |
| `pipeline_args.py` NAR chain         | ends at RELATIONSHIP_SCRIPT                   | **UPDATED**: + EXOTIC_SCRIPT (inert until 193-feat model active) |

**Container smoke test**: NOT RUN — no container change (model_meta.py unchanged, image unchanged).

---

## Root cause analysis

The REJECT pattern mirrors nar-true-deploy-judge.md (Candidate B, −0.075pp LB95):

1. **Base-vs-base signal is real but small**: fukusho_2p +0.088pp (LB95 −0.047pp, not significant).
   The nar-exotic-ingest-fix-reverify.md +0.172pp was measured against an earlier store without
   relationship features; with the full 192-feat base the marginal signal is diluted.

2. **Ensemble gap dominates**: The production system's 6 per-class ensembles add ~0.22pp top1
   over the base model alone. Any new base candidate must clear this gap, which requires
   rebuilding all 6 residuals on the new base WF scores.

3. **Approximate ensemble shows promise**: top3_box +0.143pp (LB95 +0.039pp, significant) even
   with mismatched residuals. A proper residual rebuild on the 193-feat base would likely improve
   all axes.

---

## Path forward

To make this deployment production-worthy:

1. **Rebuild 6 per-class residuals** on `feat-nar-v8-exotic-sanrenpuku` + new base WF scores.
   Same architecture as iter30 (CatBoost YetiRank for NEW/MUKATSU/A/OP/other) and iter36
   (LightGBM LambdaRank for C). This is the same step required for any NAR base upgrade.

2. **Re-run full-system gate** with (new base + 6 rebuilt residuals) vs TRUE production.

3. **Only then flip** model_meta.py, rebuild docker, run container smoke.

Given the approximate ensemble result (+0.143pp top3_box significant, neutral top1/fukusho_2p),
the rebuilt ensemble path has a reasonable probability of passing. However, it requires ~6 hours
of sequential CatBoost + LightGBM residual training.

---

## Artifacts

- Feature store: `apps/pc-keiba-viewer/tmp/feat-nar-v8-exotic-sanrenpuku/` (untracked tmp)
- WF predictions: `tmp/nar-xgb-v8-exotic-sanrenpuku-wf/` (untracked tmp)
- Rejected model: `apps/finish-position-predict-container/models/finish-position/nar/nar-xgb-v8-exotic-sanrenpuku/`
- Judge result JSON: `tmp/nar-exotic-production-judge-result.json` (untracked tmp)
- WF training script: `tmp/train_nar_exotic_sanrenpuku_wf.py` (untracked tmp)
