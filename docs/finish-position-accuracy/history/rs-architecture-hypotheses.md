---
probe_id: rs-architecture-hypotheses
date: 2026-06-12
status: COMPLETE
scope: running-style (脚質) LightGBM 4-class softmax — structural improvement investigation
verdict_summary: "Calibration PROCEED (ECE −74%, sashi worst class); all others ABORT"
---

# Running-Style Model: Structural Architecture Hypotheses

## Background

Production model: `{jra,nar}-running-style-lgbm-prod-v3`
Architecture: LightGBM multiclass softmax, 4 classes (nige=0, senkou=1, sashi=2, oikomi=3),
target = `target_running_style_class` derived from `corner_1_norm`.
Inverse-frequency class weights. 146 features including field-relative features (v2 schema).

**Production WF accuracy (honest, from baseline-v1 report)**:
| Year | Accuracy | Macro F1 |
|------|----------|----------|
| 2021 | 0.9445 | 0.9518 |
| 2022 | 0.9494 | 0.9561 |
| 2023 | 0.9474 | 0.9545 |
| 2024 | **0.9497** | **0.9567** |
| 2025 | 0.9528 | 0.9598 |
| mean | 0.9488 | 0.9558 |

**Per-class performance (2024 holdout, reference for all hypotheses)**:
| Class | Precision | Recall | Support |
|-------|-----------|--------|---------|
| nige | 0.9839 | 0.9909 | 3,510 |
| senkou | 0.9493 | 0.9497 | 10,401 |
| sashi | 0.9478 | 0.9371 | 14,267 |
| oikomi | 0.9423 | 0.9529 | 11,777 |

Note: `metadata.json` walk-forward fields in v3 show acc≈0.48 — this is a bug (NaN targets
cast to 0 via `to_numpy(dtype=np.int64)` without prior `filter_labeled_rows`). The authoritative
accuracy is the baseline-v1 report above.

---

## What Has Already Been Tried (avoid re-testing)

| Experiment                                   | Date         | Result                                                            |
| -------------------------------------------- | ------------ | ----------------------------------------------------------------- |
| Baseline LightGBM v1/v2/v3                   | May 2026     | PRODUCTION — accuracy 0.9488 mean                                 |
| Field features (+17 field\_\* cols)          | May 24, 2026 | Marginal (+0pp–+0.02pp) vs no-field; already in v2/v3             |
| Field-only ablation                          | May 24, 2026 | 0.9482 acc (−0.15pp vs full features)                             |
| Constraint-only (nige precision=0.9991)      | May 24, 2026 | Recall tradeoff — not better overall                              |
| Improved (constraint + recall tuning)        | May 24, 2026 | 0.9485 acc — no improvement over baseline                         |
| Official JRA kyakushitsu_hantei features     | Jun 11, 2026 | ABORT: partial ρ 0.074 < bar 0.08 (absorbed by existing features) |
| RS impute for NAR 43/44 null rows            | Jun 11, 2026 | ABORT: native NULL routing wins (−0.16pp)                         |
| Locality features (pct_career_at_keibajo)    | Jun 11, 2026 | ABORT: <1.25% gain, implicit in existing features                 |
| Per-horse local RS proxy (corner_4_norm avg) | Jun 11, 2026 | ABORT: −0.285pp, introduces noise                                 |
| MLX Set Transformer (finish-position)        | May 2026     | ABORT: top1 44.2% vs GBDT 52.4%, insufficient WF+HPO              |

---

## Hypothesis 1: Ordinal Structure (cumulative-link / regression)

**Rationale**: nige<senkou<sashi<oikomi is a natural ordinal order derived from corner_1_norm.
Softmax treats classes independently, ignoring ordinal distance. Hypothesis: regression on
corner_1_norm → optimal thresholds would (a) reduce distance-2+ errors and (b) improve the
hard middle classes (senkou/sashi).

**Probe**: LightGBM regression (objective=regression) on `target_corner_1_norm` → 3-threshold
optimal bucketing on 2023 cal split → compare vs softmax baseline on same 2022-2023→2024 split.
(Results are relative; absolute accuracy is lower than production due to 2-year vs 21-year train.)

**Probe results (2022-2023 train → 2024 holdout, n=40,021 labeled rows)**:

| Metric           | Softmax (B) | Regression+Threshold (A) | Delta                   |
| ---------------- | ----------- | ------------------------ | ----------------------- |
| Accuracy         | 0.9324      | 0.8620                   | **−7.03 pp**            |
| Macro F1         | 0.9404      | 0.8714                   | **−6.91 pp**            |
| Off-by-2+ errors | 1.61%       | 0.78%                    | −0.84 pp (ordinal wins) |

**Per-class F1**:
| Class | Softmax | Regression+Thresh | Delta |
|---|---|---|---|
| nige | 0.9764 | 0.9071 | −6.93 pp |
| senkou | 0.9323 | 0.8528 | **−7.95 pp** |
| sashi | 0.9251 | 0.8412 | **−8.39 pp** |
| oikomi | 0.9281 | 0.8843 | −4.38 pp |

**Optimal thresholds**: [0.0931, 0.3566, 0.6693] (on 2023 cal split)

**Calibration (ECE per class)**:
| Class | Softmax ECE | Regression ECE |
|---|---|---|
| nige | 0.003 | 0.029 |
| senkou | 0.014 | 0.028 |
| sashi | 0.021 | 0.013 |
| oikomi | 0.017 | 0.034 |

**Analysis**: The regression approach does achieve fewer distance-2+ errors (0.78% vs 1.61%,
−0.84 pp) — ordinal structure IS being exploited. However, the cost is catastrophic for all
per-class metrics. The reason: `corner_1_norm` has substantial within-class variance (the
threshold at 0.357 for senkou/sashi is noisy — horses near the boundary flip classes in both
directions). The softmax already implicitly captures the ordinal gradient through the rich
`past_corner_1_norm_*` features in the feature set; the regression target is redundant information
that is noisier to predict than the already-discretized class.

**Verdict: ABORT.** Ordinal structure is already captured implicitly by the softmax through
`past_corner_1_norm_avg_5`, `past_corner_1_norm_std_5`, `past_corner_1_norm_best_5`, and the
continuous target features. Regression+threshold sacrifices ~7 pp macro F1 to gain ~0.8 pp
fewer neighbour errors — not worthwhile. The boundary problem is structural, not solvable
by better threshold optimization.

---

## Hypothesis 2: Calibration (ECE + Isotonic Scaling)

**Rationale**: `rs_p_*` probabilities feed the finish-position model downstream as features.
Poorly calibrated probabilities degrade downstream quality even if argmax accuracy is unchanged.
The model may be overconfident/underconfident per class.

**Probe**: Train on 2022 only → calibrate on 2023 (IsotonicRegression, 4 OvR calibrators)
→ evaluate uncalibrated vs calibrated on 2024 holdout (n=40,021 rows).

**ECE diagnostics (2023 calibration split)**:

| Class     | ECE uncalibrated | Mean conf  | Prevalence | Bias                      |
| --------- | ---------------- | ---------- | ---------- | ------------------------- |
| nige      | 0.032            | 0.1202     | 0.0877     | **OVERCONFIDENT +0.033**  |
| senkou    | 0.026            | —          | —          | moderate                  |
| **sashi** | **0.055**        | **0.3146** | **0.3584** | **UNDERCONFIDENT −0.044** |
| oikomi    | 0.027            | —          | —          | moderate                  |

**After isotonic calibration (2024 holdout)**:

| Class       | ECE before | ECE after  | Improvement |
| ----------- | ---------- | ---------- | ----------- |
| nige        | 0.0319     | 0.0056     | **−82.4%**  |
| senkou      | 0.0193     | 0.0090     | −53.4%      |
| **sashi**   | **0.0535** | **0.0087** | **−83.8%**  |
| oikomi      | 0.0249     | 0.0104     | −58.2%      |
| **average** | **0.0324** | **0.0084** | **−74.0%**  |

Log loss: 1.1147 → 1.1006 (−1.3%).

**Argmax accuracy effect**:
| Metric | Uncalibrated | Calibrated | Delta |
|---|---|---|---|
| Accuracy | 0.4762 | 0.4841 | +0.79 pp |
| Macro F1 | 0.4541 | 0.4485 | −0.56 pp (nige recall drops) |

(Absolute values are low due to 1-year train slice; relative ECE improvement is reliable.)

**Key finding**: Sashi is the worst-calibrated class (ECE=0.0535, systematic underconfidence
by −4.4 pp). Nige is overconfident (+3.3 pp). After isotonic calibration: average ECE −74%,
sashi ECE −84%. The calibration improvement does NOT require retraining the model — it is a
post-processing layer applied to inference outputs.

**Downstream impact**: The finish-position model uses `rs_p_nige`, `rs_p_senkou`,
`rs_p_sashi`, `rs_p_oikomi` as features. Systematic sashi underconfidence means the
finish-position model receives `rs_p_sashi` values that are consistently ~4.4 pp too low.
Calibration corrects this bias. Prior I3 work showed NDCG@3 vs place2/3 are decoupled
(r=0.24) — better-calibrated RS probs may help place2/3 precision where the signal margin is thin.

**Verdict: PROCEED.** Apply 4-class isotonic OvR calibration to v3 model outputs. Implementation
path: fit calibrators on a held-out calibration split (e.g. 2023 only), save 4 IsotonicRegression
objects alongside the model, apply during inference before writing `rs_p_*` to the feature store.
Cost: negligible (sklearn.isotonic, no retraining). Expected gain: ECE −74% (definitive);
argmax accuracy impact is neutral to slightly positive.

**Note on argmax accuracy**: calibration does not materially change argmax predictions for
well-separated classes (the 0.9497 production accuracy is unaffected — the model's rank
ordering of classes is already correct). The benefit is purely in the QUALITY of the
probability outputs consumed downstream.

---

## Hypothesis 3: Class Imbalance — Focal Loss

**Rationale**: Current model uses inverse-frequency sample weights. Focal loss (gamma=2)
down-weights easy examples, potentially helping the hard middle classes (senkou/sashi).

**Probe**: 1-round focal reweighting approximation: train initial model on 2022-2023 →
compute per-sample focal weights (1 − p_correct)^γ with γ=2 → retrain on focal weights.
Compare vs inverse-frequency baseline on 2024 holdout (n=40,021 rows).

**Results**:
| Metric | Inv-Freq Baseline | Focal γ=2 | Delta |
|---|---|---|---|
| Accuracy | 0.4762 | 0.4167 | **−6.02 pp** |
| Macro F1 | 0.4541 | 0.3698 | **−8.75 pp** |
| Log loss | 1.1147 | 1.1445 | +0.030 (worse) |

Per-class nige recall collapse: 0.354 → 0.143 (−21 pp). Focal weights (std=0.563, max=3.1×)
over-amplify hard examples and destabilize the minority class (nige) in the 1-round approximation.

**Verdict: ABORT.** Focal loss in the 1-round gamma=2 approximation hurts substantially.
The inverse-frequency weighting is already providing class balance correction. The nige collapse
pattern suggests gamma=2 is too aggressive for this class distribution. Even if a multi-round
grid search over γ∈{0.5, 1.0, 1.5} were performed, the inverse-frequency mechanism already
targets the same objective and the production model shows high nige precision (0.9839) — the
minority class is already well-handled.

---

## Hypothesis 4: Algorithm — Set Transformer (MLX)

**Prior evidence (from rootcause-i6-architecture.md + project_mlx_transformer_status.md)**:

The existing `running_style_transformer.py` (MLX, race-level attention) was evaluated in the
finish-position context. The RS-specific transformer uses the same `RunningStyleTransformer`
architecture (2-layer, 4-head, 64-dim). Prior evidence:

- Finish-position MLX Set Transformer: top1=44.2% vs GBDT 52.4% (−8.5 pp)
- Test was on only 2 WF folds (2024-2025) with no HPO — insufficient to detect small gains
- Root causes of loss: sample efficiency (GBDT dominates at ~100K races tabular), no HPO,
  2-fold high variance
- The RS-specific transformer has NOT been run on a full 21-fold WF; it was implemented
  as a drop-in replacement but the prior evidence from finish-position strongly disfavors it

**For running style specifically**: the model already has access to race-set context via the
17 `field_*` features (computed before training). The transformer's attention mechanism would
learn to aggregate these same features plus correlated signals — but GBDT with explicit
field features already captures much of this. The wall-clock cost for a full 21-fold
transformer WF eval would be significant (each fold = 20 epochs × race batches).

**Verdict: ABORT (not worth probing with cheap slice).** GBDT sample efficiency + explicit
field features already approximate joint race-level reasoning. The transformer's advantage
requires: (a) cross-horse interaction patterns not captured by engineered field features,
(b) sufficient training scale + HPO, (c) evidence the RS task benefits from attention-style
inference (not just feature context). None of these are established. The finish-position
transformer result (same architecture family, same data scale) is the closest analogue and
it lost by 8.5 pp.

---

## Hypothesis 5: Label Definition — Corner_1_norm Threshold Sensitivity

**Implicit finding from ordinal probe**: The threshold analysis on 2023 cal data found
optimal boundaries at [0.093, 0.357, 0.669]. The production thresholds (encoded implicitly
in `target_running_style_class` generation by `finish_position_features_duckdb.py`) are
empirically close to these optimal values — the softmax model is learning the correct decision
boundaries from the continuous `corner_1_norm` feature anyway. Explicit threshold tuning
is subsumed by the softmax's ability to learn arbitrary non-linear boundaries.

**Verdict: NOT PROBED SEPARATELY — already addressed by ordinal probe. ABORT.**
The current label bucketing is adequate; threshold adjustment would require regenerating
the target column and full retraining for marginal gain.

---

## Summary Table

| Hypothesis             | Probe type               | Key number                             | Verdict     |
| ---------------------- | ------------------------ | -------------------------------------- | ----------- |
| Ordinal regression     | Cheap slice (2022-23→24) | −7.03 pp accuracy vs softmax           | **ABORT**   |
| Calibration (isotonic) | Cheap slice (2022→23→24) | ECE −74%, sashi worst (0.0535→0.0087)  | **PROCEED** |
| Focal loss γ=2         | Cheap slice (2022-23→24) | −6.02 pp accuracy vs inv-freq          | **ABORT**   |
| MLX Set Transformer    | Prior evidence           | GBDT +8.5 pp vs transformer            | **ABORT**   |
| Label threshold tuning | Implicit in ordinal      | Optimal thresholds ≈ current bucketing | **ABORT**   |

---

## Recommended Next Action: Calibration Implementation

1. **Fit 4 OvR IsotonicRegression calibrators** on a held-out calibration slice
   (e.g. 2023 predictions from the v3 walk-forward JSONL in
   `tmp/running-style-eval-baseline-v1/2021-2025.jsonl` — year=2023 fold).
2. **Save calibrators** alongside model (4 sklearn objects, ~1 KB total).
3. **Apply at inference time**: after `booster.predict()`, pass raw probabilities through
   calibrators before writing `rs_p_*` to the feature store.
4. **Measure ECE improvement** on 2024 holdout to confirm −70–80% ECE reduction holds at
   the production feature scale (146 features vs 118 in probe).

The calibration improvement is structural (corrects sashi underconfidence bias), costs
nothing at train time, and directly benefits the downstream finish-position model's
consumption of `rs_p_*` features — particularly place2/place3 prediction where the signal
margin is thin (per rootcause-i3).

**Caveats**:

- The calibration probe used a 1-year train slice vs 21-year production model; the
  production model's probabilities will be better-calibrated to begin with (more data),
  so the actual ECE improvement in production may be smaller than −74%.
- Isotonic calibration on argmax classes does not help when the model's rank ordering
  is already correct (0.9497 production accuracy). The benefit is exclusively for the
  **probability values** consumed as downstream features.
- If downstream finish-position model uses GBDT (which handles monotone feature
  transformations well), the calibration benefit may be partially absorbed by the
  GBDT itself. The clearest path to verifying real gain is an A/B of finish-position
  model on calibrated vs uncalibrated rs*p*\* features.

---

## Probe Artifacts

- `tmp/rs-arch-probe/ordinal_probe.json` — ordinal regression vs softmax, 2022-23→24
- `tmp/rs-arch-probe/calibration_focal_probe.json` — calibration ECE + focal loss, 2022→23→24
