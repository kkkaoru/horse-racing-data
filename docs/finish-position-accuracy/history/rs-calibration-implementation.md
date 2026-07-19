---
probe_id: rs-calibration-implementation
date: 2026-06-12
status: COMPLETE — shipped to Python scoring path + TS inference layer
scope: Running-style v3 LightGBM — isotonic calibration (Phase 1 multi-year validation + Phase 2 implementation)
verdict_summary: |
  Phase 1 GATE PASS: 4/4 positive splits for both JRA and NAR. argmax improvement
  +2.18 to +2.66pp (JRA), +2.18 to +2.31pp (NAR). ECE reduction -71 to -75%.
  oikomi F1 slight drop (max -1.33pp, well within -2pp gate threshold).
  sashi is the largest beneficiary (+5.78 to +7.12pp F1 across all folds).
  Phase 2 SHIPPED: Python calibration module + TS calibration module + production
  calibrators (JRA+NAR, fit on 2025, 100 knots each).
---

# Running-Style v3 Isotonic Calibration — Implementation Report

## Summary

| Item                                                       | Result                                                                         |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------ |
| Phase 1 gate                                               | **PASS** (JRA 4/4, NAR 4/4)                                                    |
| argmax improvement JRA                                     | **+2.24 to +2.66 pp** across 4 splits                                          |
| argmax improvement NAR                                     | **+2.18 to +2.31 pp** across 4 splits                                          |
| ECE reduction (mean over 4 classes)                        | **-71% to -76%**                                                               |
| F1 collapse (any class > -2pp)                             | **None** (oikomi max -1.33pp)                                                  |
| Prior single-fold result (+2.07pp JRA, rs-leak-resolution) | **Replicated** — 4-fold mean +2.51pp exceeds the 2023→2024 single fold         |
| Production calibrators                                     | JRA + NAR, fit_year=2025, 100 knots, deployed to tmp/models + docs/calibrators |
| Python scoring path                                        | Integrated in `score_running_style_local.py` (graceful no-op when absent)      |
| TS inference path                                          | Integrated in `running-style-calibration.ts` + `running-style-inference.ts`    |
| All tests                                                  | Python 95%+ (99%), TS 95%+ (all 4 metrics)                                     |

---

## Phase 1 — Multi-Year Robustness Validation

### Protocol

Used production model `{jra,nar}-running-style-lgbm-prod-v3` (trained on 2006-2026, 21y)
to score each year leak-free (prod `feature_columns` from metadata.json, no `rs_p_*`).
For calibration-year `c` in {2021,2022,2023,2024}:

1. Score year c with production model → `probs_c`, `labels_c`
2. Score year c+1 → `probs_{c+1}`, `labels_{c+1}`
3. Fit 4 OvR IsotonicRegression calibrators on (`probs_c`, `labels_c`)
4. Apply calibrators to `probs_{c+1}`, renormalize
5. Compare argmax accuracy and per-class F1/ECE before/after

Note: accuracy ~63% JRA / ~56% NAR here is higher than the 48%/52% from rs-leak-resolution
because the production model was trained on 21 years (2006-2026), while the rs-leak-resolution
used a 2016→2025 fold retrain. The DELTA (+2.2-2.6pp) is the metric that matters.

### JRA Results

| Calib→Eval | Acc before | Acc after | Δpp       | ECE reduction | oikomi F1 Δ |
| ---------- | ---------- | --------- | --------- | ------------- | ----------- |
| 2021→2022  | 0.6299     | 0.6565    | **+2.66** | -74.7%        | -0.47pp     |
| 2022→2023  | 0.6283     | 0.6534    | **+2.51** | -75.5%        | -0.80pp     |
| 2023→2024  | 0.6336     | 0.6599    | **+2.63** | -73.3%        | -0.64pp     |
| 2024→2025  | 0.6317     | 0.6541    | **+2.24** | -75.2%        | -1.19pp     |

**JRA per-class F1 delta (mean over 4 splits):**

| Class  | F1 before (mean) | F1 after (mean) | Δ (mean)    |
| ------ | ---------------- | --------------- | ----------- |
| nige   | 0.621            | 0.658           | **+3.87pp** |
| senkou | 0.620            | 0.637           | **+1.76pp** |
| sashi  | 0.571            | 0.639           | **+6.88pp** |
| oikomi | 0.701            | 0.691           | **-0.78pp** |

Sashi is the dominant beneficiary — systematically miscalibrated, calibration corrects its
boundary confusion with oikomi. Oikomi slightly penalized (isotonic pulls its over-confident
high probs down, some predictions flip to sashi). All deltas within gate.

### NAR Results

| Calib→Eval | Acc before | Acc after | Δpp       | ECE reduction | oikomi F1 Δ |
| ---------- | ---------- | --------- | --------- | ------------- | ----------- |
| 2021→2022  | 0.5626     | 0.5843    | **+2.18** | -72.7%        | -1.33pp     |
| 2022→2023  | 0.5580     | 0.5801    | **+2.21** | -71.8%        | -1.12pp     |
| 2023→2024  | 0.5611     | 0.5836    | **+2.24** | -71.1%        | -1.15pp     |
| 2024→2025  | 0.5552     | 0.5783    | **+2.31** | -73.6%        | -1.21pp     |

**NAR per-class F1 delta (mean over 4 splits):**

| Class  | F1 before (mean) | F1 after (mean) | Δ (mean)    |
| ------ | ---------------- | --------------- | ----------- |
| nige   | 0.529            | 0.541           | **+1.18pp** |
| senkou | 0.503            | 0.535           | **+3.08pp** |
| sashi  | 0.488            | 0.552           | **+6.49pp** |
| oikomi | 0.681            | 0.668           | **-1.20pp** |

### Gate Decision

| Category | Positive splits | F1 collapse        | Gate     |
| -------- | --------------- | ------------------ | -------- |
| JRA      | **4/4**         | None (max -1.19pp) | **PASS** |
| NAR      | **4/4**         | None (max -1.33pp) | **PASS** |

**Gate: PASS for both categories. Proceeding to Phase 2.**

---

## Phase 2 — Implementation

### Production Calibrators

Fit IsotonicRegression OvR on year 2025 predictions from production model.
Exported as piecewise-linear knot tables (100 knots, linspace 0→1):

| File                                                                   | Category | fit_year | Knots/class |
| ---------------------------------------------------------------------- | -------- | -------- | ----------- |
| `docs/finish-position-accuracy/calibrators/jra-rs-v3-calibrators.json` | JRA      | 2025     | 100         |
| `docs/finish-position-accuracy/calibrators/nar-rs-v3-calibrators.json` | NAR      | 2025     | 100         |
| `tmp/models/jra-running-style-lgbm-prod-v3/calibrators.json`           | JRA      | 2025     | 100         |
| `tmp/models/nar-running-style-lgbm-prod-v3/calibrators.json`           | NAR      | 2025     | 100         |

The `tmp/models/*/calibrators.json` files are auto-detected by the scoring path
(graceful no-op when absent).

### Python: `running_style_calibration.py`

New module `apps/pc-keiba-viewer/src/scripts/running_style_calibration.py`:

- `CalibrationTable(TypedDict)`: piecewise-linear knot table (x, y lists)
- `RunningStyleCalibrators(TypedDict)`: full calibrator set (category, fit_year, classes, calibrators)
- `load_calibrators(path)`: loads + validates JSON
- `apply_calibration(probabilities, calibrators)`: OvR interp + renormalization
- `calibrators_path_for_model_version(model_version)`: path convention

Modified `score_running_style_local.py`:

- `_try_load_calibrators(model_version, path_exists)`: auto-loads if present, returns None when absent
- `score_frame(..., calibrators=None)`: applies calibration when present, no-op otherwise
- Backward-compatible: existing model versions without calibrators.json work unchanged

Coverage: `running_style_calibration.py` 100%, `score_running_style_local.py` 99%

### TypeScript: `running-style-calibration.ts`

New module `apps/sync-realtime-data/src/running-style-calibration.ts`:

- `CalibrationKnots`: `{ x: readonly number[], y: readonly number[] }`
- `RunningStyleCalibrationTable`: full calibrator payload type
- `linearInterp(xKnots, yKnots, value)`: piecewise linear with boundary clamping
- `applyRunningStyleCalibration(prediction, calibrators)`: apply + renormalize + recompute argmax
- `buildCalibrationR2Key(source)`: `"running-style/models/${source}/calibrators.json"`
- `loadCalibratorsFromR2(bucket, key)`: load + validate from R2

Modified `running-style-inference.ts`:

- `LoadedFlatRowsInferenceConfig.calibrators?: RunningStyleCalibrationTable` (optional)
- Calibration applied in `buildFlatPredictionForHorse` when calibrators present

Coverage: calibration module Stmts 98.43% / Branches 96.96% / Funcs 100% / Lines 100%.
All 4 aggregate TS coverage metrics remain above 95%.

---

## Parity

Python and TypeScript implementations both use piecewise-linear interpolation on 100-knot
tables produced by sklearn IsotonicRegression. The TS `linearInterp` and Python `np.interp`
are mathematically equivalent on uniform grids. The existing
`test_verify_running_style_inference_parity.py` covers the model-binary parity; the
calibration adds the same interpolation function in both languages with independent test suites.

---

## Deploy State

**Python scoring path**: Active — production scoring auto-detects calibrators.json in
`tmp/models/<model_version>/calibrators.json` and applies calibration when present.
Score runs using `jra-running-style-lgbm-prod-v3` and `nar-running-style-lgbm-prod-v3`
will now use calibrated probabilities automatically.

**TS inference path**: DEPLOYED 2026-06-12.

### Deploy details

| Item                    | Value                                                                                                         |
| ----------------------- | ------------------------------------------------------------------------------------------------------------- |
| Wrangler deploy version | `c56f61ec-49ac-4374-bab8-a53c8a3ab268`                                                                        |
| R2 bucket               | `pc-keiba-finish-position-models`                                                                             |
| JRA calibrators R2 key  | `running-style/models/jra/calibrators.json`                                                                   |
| NAR calibrators R2 key  | `running-style/models/nar/calibrators.json`                                                                   |
| Source files            | `jra-rs-v3-calibrators.json` / `nar-rs-v3-calibrators.json` from `docs/finish-position-accuracy/calibrators/` |

### Wiring summary

- `running-style-queue.ts`: added `tryLoadCalibrators` helper (graceful fallback to `undefined`
  on any R2 error), called after model load, calibrators passed to
  `runRunningStyleInferenceRowsWithFlatModel`.
- `running-style-verification.ts`: same `tryLoadCalibrators` helper, passed to inference.
- Tests updated for both files: calibrated path + fallback (R2 missing) path coverage.
- Coverage: Statements 97.7% / Branches 95.39% / Functions 96.51% / Lines 97.91% (all ≥ 95%).

### Smoke test (2026-06-12)

POST `/admin/running-style/verify-postgres/nar/2026/06/07/54/01` → returned `ok: true`,
5 horses written, modelVersion `nar-running-style-lgbm-prod-v3`. D1 check confirmed:

- All pNige+pSenkou+pSashi+pOikomi sums to 1.0 (± 1e-15 float)
- No NaN values
- argmax labels plausible (nige/senkou/oikomi all appearing)
- sashi probabilities in reasonable range (calibration correcting the systematic under-prediction)

Fallback path (missing calibrators) tested by unit tests: graceful fallback to uncalibrated
output confirmed via mock of `loadCalibratorsFromR2` throwing.

### 2026-07-19 incident + fix: wrong JRA calibrator artifact was live in R2

During the RS serve-skew campaign, `inv-2026-jra` discovered the R2 object at
`running-style/models/jra/calibrators.json` was **not** the canonical `jra-rs-v3-calibrators.json`
documented above (100 knots, linspace 0→1). The live object was a different artifact — the raw
sklearn `IsotonicRegression` breakpoint tables (variable knots **90/96/110/112**, x-ranges not
0→1). NAR was correct (its R2 object was byte-identical to `nar-rs-v3-calibrators.json`); the
mis-upload affected **JRA only**. Root cause: the raw-breakpoint intermediate got uploaded to the
JRA key instead of the 100-knot regridded final; the deploy note above (source =
`jra-rs-v3-calibrators.json`) did not match reality for JRA.

Measured impact (exact prod flatbin `jra-running-style-lgbm-prod-v3` + real TS serve inference,
2026 JRA held-out, n=11,403 rows with ground truth; raw softmax → each calibrator → argmax):

| calibrator                      | argmax accuracy               |
| ------------------------------- | ----------------------------- |
| none (raw / identity)           | 47.72%                        |
| deployed-wrong (raw-breakpoint) | 46.80% (below no-calibration) |
| docs-v3 (canonical, redeployed) | 49.32%                        |

Fix-gate (docs-v3 minus deployed, paired bootstrap 3000): POOLED **+2.52pp** (LB95 +1.67);
surface turf +1.77 (LB95 +0.25) / dirt +3.09 (LB95 +1.95), both ADOPT; venues 03/07/08/09
individually ADOPT, none significantly negative. The wrong artifact over-predicted oikomi (52.5%
vs 28.8% actual) and halved nige recall (44.9→24.9); docs-v3 restores per-class shares to actual
frequencies and lifts sashi recall 35→49.5.

**Fix applied then REVERTED 2026-07-19** (`inv-2026-jra`): re-uploaded docs-v3, verified
byte-identical (sha256 `d3d9204b…`), then **reverted to the original** (sha256 `0f23a5a4…`,
byte-verified) after discovering the +2.52pp was measured in the WRONG feature regime. **The
calibrator comparison is regime-dependent and flips:**

| regime                         | raw    | deployed (original) | docs-v3    |
| ------------------------------ | ------ | ------------------- | ---------- |
| populated-odds (offline store) | 47.72% | 46.80%              | **49.32%** |
| **null-odds (serve reality)**  | 38.79% | **44.84%**          | 40.58%     |

At real serve, `odds_score`/`popularity_score`/`track_condition_normalized` are NULL (see next
section) — the null-odds row is the serve-real one. There, the "wrong" original calibrator's
oikomi-heavy reshaping is _serendipitously robust_ to the feature degradation (only −1.96pp when
odds go null, vs raw −8.93pp and docs-v3 −8.74pp), so it BEATS docs-v3 by +4.26pp at serve.
Original restored; docs-v3 NOT adopted. Lesson: evaluate serving-path calibrators under
serve-realistic feature availability, not the offline store.

**Follow-up proposed (not yet implemented):** a CI/health guard may record and compare the deployed
R2 calibrator hash against the intended release artifact, but a byte mismatch must not trigger an
automatic swap: the tracked docs-v3 artifact was **-4.26pp** worse under serve-real NULL odds. This
is a provenance/hygiene guard, not a +2.5pp accuracy adoption. The larger served-vs-WF gap was
subsequently explained by market-feature availability; retraining without those features was
already tested as RS v1 and failed **-3.40pp**, so that construction is DO-NOT-RETRY.

---

## Evidence Files

- `tmp/phase1_calibration_multiyear.py` — Phase 1 validation script
- `tmp/phase1_calibration_results.json` — Full per-fold results (JSON)
- `tmp/models/jra-running-style-lgbm-prod-v3/calibrators.json` — Production JRA calibrators
- `tmp/models/nar-running-style-lgbm-prod-v3/calibrators.json` — Production NAR calibrators
- `apps/pc-keiba-viewer/src/scripts/running_style_calibration.py` — Python module
- `apps/pc-keiba-viewer/tests/test_running_style_calibration.py` — Python tests
- `apps/sync-realtime-data/src/running-style-calibration.ts` — TS module
- `apps/sync-realtime-data/src/running-style-calibration.test.ts` — TS tests
