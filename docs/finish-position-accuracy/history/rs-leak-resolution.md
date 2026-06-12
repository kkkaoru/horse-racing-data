---
probe_id: rs-leak-resolution
date: 2026-06-12
status: COMPLETE
scope: Running-style (脚質) LightGBM — definitive accuracy contradiction resolution
verdict_summary: |
  rs-model-audit is correct on every point. Honest serve accuracy: JRA 48.3% / NAR 52.3%.
  Production model is NOT leaky (trained without rs_p_*). The 94-95% is a leaky EVAL
  artifact (rs_p_* pulled in via naive resolve_feature_columns on the finish-position
  feature parquet). metadata.json 0.48 is the HONEST number, not a NaN bug.
  Calibration improvement (ECE -77%) is real and larger on leak-free predictions.
---

# Running-Style Accuracy Contradiction — Definitive Resolution

## TL;DR

| Claim                                                    | Source                     | Verdict     |
| -------------------------------------------------------- | -------------------------- | ----------- |
| Baseline accuracy ~94-95% is REAL, 0.48 is NaN bug       | rs-architecture-hypotheses | **WRONG**   |
| True OOS accuracy ~48% JRA / ~52% NAR, 95% is leaky eval | rs-model-audit             | **CORRECT** |

The production v3 model is trained correctly (without `rs_p_*` features). The ~94-95%
numbers came from the walk-forward eval being run on a **different parquet** (the finish-position
feature parquet `feat-v15-rs`) that contains `rs_p_{nige,senkou,sashi,oikomi}` as stored
features for the downstream finish-position model. Those columns are picked up by the naive
`resolve_feature_columns(df.columns)` call and used as training features, creating a circular
self-consistency leak. The `metadata.json` walk-forward (0.478–0.485 JRA) was computed on the
clean training parquet (without `rs_p_*`) and reflects the true OOS accuracy.

---

## 1. Feature Leak Classification

### LABEL_COLUMNS (excluded by `running_style_lightgbm.py`)

```python
LABEL_COLUMNS = (
    "finish_position",
    "finish_norm",
    "target_corner_1_norm",   # realized current-race corner 1 position — EXCLUDED
    "target_corner_3_norm",   # realized current-race corner 3 position — EXCLUDED
    "target_corner_4_norm",   # realized current-race corner 4 position — EXCLUDED
    "target_running_style_class",  # the training target — EXCLUDED
)
```

These are correctly excluded from features in all production training runs. `target_corner_1_norm`
and related columns are realized CURRENT-RACE data (not available at serve time) and are
properly treated as labels.

### rs*p*\* columns — THE LEAK SOURCE

The finish-position feature parquet (`feat-v15-rs`, `feat-v15-rs-labeled`, `feat-v20-merged`)
contains four additional columns:

```
rs_p_nige, rs_p_senkou, rs_p_sashi, rs_p_oikomi
```

These are **softmax probabilities from a prior RS model run**, stored as features for the
downstream finish-position LightGBM. They sum to 1.0 per row (verified) and are fully
populated even for historical years back to 2016 (0% null).

**Classification:**

| Feature                                    | Type                                          | At serve time     | In production feature_columns |
| ------------------------------------------ | --------------------------------------------- | ----------------- | ----------------------------- |
| `past_corner_1_norm_avg_5`                 | pre-race (PAST races history)                 | available         | YES                           |
| `target_corner_1_norm`                     | CURRENT-RACE REALIZED (label)                 | unknown           | NO (label)                    |
| `rs_p_nige/senkou/sashi/oikomi`            | RS model self-output stored back into parquet | produced by model | NO (not in list)              |
| All 146 `feature_columns` in metadata.json | pre-race-knowable                             | available         | YES                           |

`rs_p_*` are **NOT** in the 146 production `feature_columns`. The production model is leak-free.

### The leak mechanism

When `run_walk_forward_command()` loads the finish-position feature parquet and calls:

```python
base_feature_columns = resolve_feature_columns(list(df.columns))
```

`resolve_feature_columns()` excludes only `META_COLUMNS` and `LABEL_COLUMNS`. Since `rs_p_*`
are in neither set, they are **silently included** as training features. At training time,
the model learns to predict `target_running_style_class` from `rs_p_*` — which are themselves
high-quality RS probability estimates — creating circular self-consistency.

**Empirical proof (this probe):**

| Condition                   | Features                   | Accuracy (2016-2023 train → 2024 holdout) |
| --------------------------- | -------------------------- | ----------------------------------------- |
| Leaky (with rs*p*\*)        | 138 features incl. rs*p*\* | **0.9433** (94.33%)                       |
| Leak-free (without rs*p*\*) | 134 features               | **0.4797** (47.97%)                       |
| **Gap**                     |                            | **46.35 pp**                              |

The 46 pp gap is entirely explained by `rs_p_*` circular self-consistency.

---

## 2. Definitive Serve Accuracy

At serve time (upcoming race), the current-race corner positions are unknown and
**`rs_p_*` do not yet exist** (they are the model's own output that hasn't been run yet).
Serving uses only pre-race features — exactly the 146 `feature_columns` in `metadata.json`.

**Honest serve accuracy (from production metadata.json walk-forward, no `rs_p_*`):**

| Category | Year           | Accuracy   |
| -------- | -------------- | ---------- |
| JRA      | 2023           | 0.4787     |
| JRA      | 2024           | 0.4846     |
| JRA      | 2025           | 0.4846     |
| JRA      | 2026 (partial) | 0.4782     |
| JRA      | **mean**       | **~0.481** |
| NAR      | 2023           | 0.5251     |
| NAR      | 2024           | 0.5283     |
| NAR      | 2025           | 0.5238     |
| NAR      | 2026 (partial) | 0.5202     |
| NAR      | **mean**       | **~0.524** |

Independently confirmed by the rs-model-audit leak-free single-fold retrain:

- JRA 2025 holdout: **48.35%** overall, macro-F1 **0.465**
- NAR 2025 holdout: **52.30%** overall, macro-F1 **0.510**

**Per-class (JRA 2025 holdout, leak-free):**

| Class  | Precision | Recall | F1    | Support |
| ------ | --------- | ------ | ----- | ------- |
| nige   | 0.346     | 0.414  | 0.377 | 3,093   |
| senkou | 0.449     | 0.487  | 0.467 | 9,479   |
| sashi  | 0.479     | 0.386  | 0.428 | 12,991  |
| oikomi | 0.561     | 0.619  | 0.589 | 10,700  |

**Per-class (NAR 2025 holdout, leak-free):**

| Class  | Precision | Recall | F1    | Support |
| ------ | --------- | ------ | ----- | ------- |
| nige   | 0.379     | 0.622  | 0.471 | 10,080  |
| senkou | 0.456     | 0.457  | 0.457 | 23,576  |
| sashi  | 0.530     | 0.396  | 0.453 | 35,217  |
| oikomi | 0.636     | 0.685  | 0.660 | 30,951  |

---

## 3. metadata.json 0.48 Verdict

**The 0.48 is the HONEST OOS number. The NaN→0 bug theory is wrong.**

### Why the NaN bug theory is wrong

The rs-architecture-hypotheses claimed that `to_numpy(dtype=np.int64)` on NaN targets
cast NaN→0 (class "nige"), inflating nige counts and reporting ~48% as a broken number.

Code trace of `run_walk_forward_for_year()`:

```python
predictions_df = build_predictions_df(valid_df, probabilities)
evaluation_subset = predictions_df.dropna(subset=[TARGET_COLUMN])  # drops NaN rows FIRST
predicted = evaluation_subset["predicted_class"].to_numpy(dtype=np.int64)
actual = evaluation_subset[TARGET_COLUMN].to_numpy(dtype=np.int64)  # only non-NaN here
```

`dropna(subset=[TARGET_COLUMN])` is called **before** the int64 cast. NaN rows are removed
from `evaluation_subset` before `to_numpy`. There is no NaN→0 cast in accuracy computation.

### Why the production training parquet did NOT have rs*p*\*

The v3 production model (`jra-running-style-lgbm-prod-v3`) was trained on a different parquet
(the Phase A feature parquet from the v3 training run, which predates the finish-position
feature store `rs_p_*` columns). Its 146 `feature_columns` do not include `rs_p_*` (confirmed
by inspection). The `--enable-walk-forward-eval` in the `train-production` run used the same
clean training parquet, so the `walk_forward_results` in `metadata.json` correctly reports ~0.48.

### Why the baseline-v1 eval shows 94-95%

The `running-style-eval-baseline-v1` walk-forward was run on the `feat-v15-rs` / finish-position
feature parquet, which contains `rs_p_*`. The `resolve_feature_columns()` call picked them up
naively. The 2025 fold accuracy in that eval is **95.28%** — effectively identical to what
the rs-model-audit calls "bogus 95.3% from unguarded retrain". The ~94-95% range across folds
reflects varying rs*p*\* signal strength across years, not honest OOS variance.

---

## 4. Calibration Re-Validation on Leak-Free Predictions

### Protocol

Train on 2016-2022 (leak-free, 134 features, 290K rows) → calibrate IsotonicRegression OvR
on 2023 → evaluate on 2024 holdout (n=40,021).

### Results

**ECE per class (2024 holdout, leak-free):**

| Class       | ECE before | ECE after  | Reduction |
| ----------- | ---------- | ---------- | --------- |
| nige        | 0.0706     | 0.0091     | **−87%**  |
| senkou      | 0.0076     | 0.0062     | −19%      |
| sashi       | 0.0709     | 0.0096     | **−86%**  |
| oikomi      | 0.0143     | 0.0127     | −11%      |
| **average** | **0.0409** | **0.0094** | **−77%**  |

**Argmax accuracy:** 0.4847 → 0.5054 (**+2.07 pp**).

### Interpretation

The calibration improvement is **real and larger** on leak-free predictions than on the leaky
eval reported in rs-architecture-hypotheses (which showed −74% ECE average). The two worst-
calibrated classes are `nige` (ECE 0.071) and `sashi` (ECE 0.071) — both systematically
miscalibrated in the same direction:

- `nige`: over-confident (inverse-frequency weighting pushes nige probs too high)
- `sashi`: the largest middle class with adjacent-class boundary confusion

The argmax accuracy gain of +2.07 pp on this slice is larger than the rs-architecture-
hypotheses result (+0.79 pp on the leaky eval). This is because the leak-free model's
probabilities are poorly calibrated at the decision boundary — isotonic calibration moves
borderline cases to the correct class more often than with the leaky model (which had
essentially perfect confidence in its leaky predictions).

### Conclusion on calibration

- The calibration improvement is **structural and real**, not an artifact of the leaky eval
- nige and sashi are the most poorly calibrated classes on leak-free predictions
- ECE −77% average is achievable; argmax accuracy gain is +2 pp (meaningful, not just noise)
- The downstream benefit for the finish-position model (`rs_p_*` features) remains valid
- **PROCEED with calibration implementation** (same recommendation as rs-architecture-hypotheses,
  but now validated on honest predictions)

---

## 5. Production Leak Assessment

**Is the production model trained with a leak?** No.

The `jra/nar-running-style-lgbm-prod-v3` models were trained using the Phase A feature parquet
from the v3 training run. That parquet did not contain `rs_p_*` columns. The 146
`feature_columns` in `metadata.json` contain no `rs_p_*` entries (confirmed by grep).

**Is there a production leak at SERVE TIME?** No.

At serve time, the inference Worker uses only pre-race features to produce `rs_p_*` outputs.
The production model predicts on features matching the 146 `feature_columns`. It does not
consume `rs_p_*` as inputs.

**Expected serve accuracy:**

The 48% / 52% is the honest serve accuracy. There is **no fix that would materially improve
this from a training perspective** without new data:

- The production model is correctly trained; no retrain is needed to "fix a leak"
- The model is at empirical frontier for available pre-race signals
- Per-horse sectional/halon split times remain the only unblocked signal for middle-class
  improvement, but are structurally absent from the source

**What DOES improve serve accuracy:** isotonic calibration (+2 pp argmax) + the downstream
finish-position model receives better-calibrated `rs_p_*` inputs.

---

## 6. What Happened — Root Cause Summary

The contradiction arose because two agents evaluated accuracy in different computational
environments:

1. **rs-architecture-hypotheses** ran the production `walk-forward` CLI on the finish-position
   feature parquet (`feat-v15-rs`), which contains `rs_p_*` as stored features. The naive
   `resolve_feature_columns()` picked them up, creating a circular self-consistency loop.
   Result: 94-95%. This agent inferred (incorrectly) that the leaky eval = honest performance
   and that the metadata.json 0.48 must be a NaN bug.

2. **rs-model-audit** used the production `feature_columns` from `metadata.json` as a leak-
   guard, explicitly excluding `rs_p_*`. Result: 48% / 52%. This agent's methodology was
   correct. The leak guard assertion in `run_fold.py` (`assert not leaky_present`) directly
   confirms the approach.

3. **This probe** directly measured the gap experimentally: leaky → 94.33%, leak-free →
   47.97% on the same 2016-2023→2024 split. 46 pp gap is fully explained by 4 `rs_p_*`
   features providing circular information.

---

## 7. Recommended Actions

| Action                                                                    | Priority | Expected gain                                             |
| ------------------------------------------------------------------------- | -------- | --------------------------------------------------------- |
| Add isotonic calibration layer to v3 inference                            | HIGH     | ECE −77%, argmax +2 pp, better finish-pos downstream      |
| Fix walk-forward eval CLI to use prod feature_columns (not naive resolve) | MEDIUM   | Prevents future confusion                                 |
| Update rs-architecture-hypotheses verdict section                         | MEDIUM   | Accuracy for docs                                         |
| No RS retrain needed                                                      | —        | Model is leak-free; 48%/52% is the correct serve baseline |

---

## Appendix: Evidence Files

- `tmp/rs_oos_confusion/run_fold.py` — leak-guarded retrain script (reference implementation)
- `tmp/rs_oos_confusion/result_jra_2025.json` — JRA 2025 leak-free results
- `tmp/rs_oos_confusion/result_nar_2025.json` — NAR 2025 leak-free results
- `tmp/models/jra-running-style-lgbm-prod-v3/metadata.json` — production feature*columns (no rs_p*\*)
- `tmp/models/nar-running-style-lgbm-prod-v3/metadata.json` — production feature*columns (no rs_p*\*)
- `tmp/running-style-eval-baseline-v1/report.json` — leaky eval (94-95%, do not cite as accuracy)
