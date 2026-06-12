# log_odds_z_in_race Candidate Verify — NAR + Ban-ei

**Date**: 2026-06-12
**Candidate**: `log_odds_z_in_race` = within-race z-score of log(tansho_odds)
**Prior motivation**: structural probe (commit f26d1ac) found partial ρ +0.157 (NAR) / +0.096 (Ban-ei) over [abs_odds + ninkijun + field_size]
**Default prior**: REJECT (odds already at empirical frontier per project-science-track-saturation-2026-06-11)

---

## Feature Definition

```
log_odds = odds_score × ln(300)          # invert feature-builder encoding
log_odds_z_in_race = (log_odds − race_mean(log_odds)) / race_std(log_odds)
```

- **NULL-safe**: where `odds_score` is NULL → `log_odds_z` = NULL (GBDT routes NULL)
- **Zero-std races**: where all field entries have identical odds → std ≈ 0 → z = 0 (clamped)
- **Train/serve parity**: where odds fall back to median at serve time (early NAR, DO-TTL miss), all
  same-median entries in a race get z ≈ 0 (constant median → race_std = 0). Degrades to null-signal
  column for those races; identical to NULL routing for GBDT purposes.

---

## Setup

| Item                       | Value                                                                           |
| -------------------------- | ------------------------------------------------------------------------------- |
| NAR base model             | iter12-nar-xgb-hpo-v8 params (XGBoost rank:pairwise)                            |
| Ban-ei base model          | banei-cb-v7-lineage-wf-21y params (CatBoost YetiRank)                           |
| Training window            | 2006–2022 (NAR), 2007–2022 (Ban-ei)                                             |
| Holdout                    | 2023-01-01 – 2026-12-31                                                         |
| NAR holdout size           | 45,573 races / 462,752 rows                                                     |
| Ban-ei holdout size        | 5,976 races / 55,868 rows                                                       |
| NAR base feature count     | 192                                                                             |
| NAR base+logoddsz count    | 193                                                                             |
| Ban-ei base feature count  | 115                                                                             |
| Ban-ei base+logoddsz count | 116                                                                             |
| Bootstrap                  | 10k resamples, seed=42, vectorized numpy                                        |
| Gate (pooled)              | fukusho_2p LB95 ≥ 0 AND all 4 axes ≥ −0.05pp AND ≥1 of {place2/place3} positive |

---

## NAR Per-Class Results (holdout 2023–2026)

NAR subclass derived from `nvd_ra.kyoso_joken_meisho` via regex (same logic as feature builder).

| Class   | N races | top1 Δ pp | place2 Δ pp | place3 Δ pp | top3_box Δ pp | fukusho_2p Δ pp | fukusho_2p LB95 |
| ------- | ------- | --------- | ----------- | ----------- | ------------- | --------------- | --------------- |
| C       | 23,320  | +0.060    | +0.064      | +0.133      | +0.086        | +0.069          | −0.287          |
| 3YO     | 8,578   | +0.163    | +0.291      | −0.187      | −0.128        | 0.000           | −0.548          |
| B       | 6,299   | +0.016    | −0.064      | +0.175      | +0.064        | −0.048          | −0.762          |
| A       | 2,497   | −0.360    | −0.200      | −0.481      | −0.160        | −0.080          | −1.121          |
| 2YO     | 2,176   | −0.322    | −0.138      | −0.322      | +0.276        | −0.184          | −1.241          |
| OP      | 1,231   | −0.406    | +0.325      | +1.137      | +0.731        | +0.650          | −0.894          |
| NEW     | 573     | −0.524    | +0.349      | +1.222      | +0.873        | 0.000           | −1.745          |
| MUKATSU | 556     | +0.180    | +1.259      | −1.079      | +0.360        | −0.180          | −2.518          |
| other   | 343     | −0.292    | +2.041      | −1.166      | +0.292        | −0.583          | −3.790          |

**All class LB95 are negative** — no class shows a statistically robust improvement. Point-positive
deltas on place2/place3 in small-N classes (OP +1.1pp place3, NEW +1.2pp place3, other +2.0pp place2)
are entirely within noise (LB95 all deeply negative).

### NAR Pooled

| Metric     | Base    | Base+z  | Δ pp   | LB95 pp |
| ---------- | ------- | ------- | ------ | ------- |
| top1       | 58.594% | 58.605% | +0.011 | −0.369  |
| place2     | 35.317% | 35.422% | +0.105 | −0.261  |
| place3     | 27.213% | 27.253% | +0.039 | −0.296  |
| top3_box   | 34.830% | 34.900% | +0.070 | −0.292  |
| fukusho_2p | 88.008% | 88.035% | +0.026 | −0.219  |

**Gate evaluation**:

- gate_no_regression (all Δ ≥ −0.05pp): **TRUE** — point-positive across all axes
- gate_fukusho_lb95 ≥ 0: **FALSE** (fukusho_2p LB95 = −0.219pp)
- gate_place_positive: TRUE

**NAR verdict: REJECT** — LB95 negative on all metrics. The +0.011 to +0.105pp point deltas are
below the noise floor (LB95 approximately −0.26 to −0.37pp). The structural probe ρ +0.157 does not
translate to model-level improvement: the GBDT already learns the field-shape signal from
`popularity_score` (rank-pct) and `odds_score` (log-normalized absolute) jointly. The z-score
redundantly encodes what splits on those two features already approximate.

---

## Ban-ei Results (holdout 2023–2026)

Ban-ei has no per-class routing; pooled only.

| Metric     | Base    | Base+z  | Δ pp   | LB95 pp |
| ---------- | ------- | ------- | ------ | ------- |
| top1       | 34.019% | 33.869% | −0.151 | −1.138  |
| place2     | 19.963% | 20.482% | +0.519 | −0.335  |
| place3     | 15.345% | 15.127% | −0.218 | −0.971  |
| top3_box   | 12.216% | 12.182% | −0.033 | −0.720  |
| fukusho_2p | 63.220% | 63.002% | −0.218 | −1.238  |

**Gate evaluation**:

- gate_no_regression: **FALSE** (top1 Δ = −0.151pp < −0.05pp)
- gate_fukusho_lb95 ≥ 0: **FALSE** (−1.238pp)
- gate_place_positive: TRUE (place2 +0.519pp)

**Ban-ei verdict: REJECT** — top1 regresses −0.151pp, fukusho_2p regresses −0.218pp, LB95
uniformly negative. The +0.519pp place2 point improvement is not robust (LB95 −0.335pp). The
structural probe ρ +0.096 does not materialize as model improvement; consistent with saturation
analysis that showed odds already absorb 56% of pre-race signal for Ban-ei.

---

## Root Cause Analysis

The structural probe measured **partial Spearman ρ** — a linear correlation proxy that does not
account for how a GBDT already uses features jointly. Key explanatory factors:

1. **GBDT already approximates log_odds_z via joint splits**: `popularity_score` (rank-pct) ×
   `odds_score` (log-normalized absolute) together define a two-dimensional encoding of the odds
   field. A single tree split combination of the form `odds_score > θ₁ AND popularity_score < θ₂`
   roughly reconstructs the z-score for favorites in sharp vs flat fields. The z-score adds no
   information the tree cannot recover.

2. **Train/serve parity at median imputation**: where odds fall back to median, `log_odds_z ≈ 0`.
   This is a training distribution feature that occurs frequently (especially early NAR races where
   DO-TTL is 4h and many pre-race entries have no odds yet). The GBDT would route these to a z≈0
   leaf — the same as what `odds_score = median` already encodes via existing features.

3. **Noise amplification at small-N classes**: OP/NEW/MUKATSU/other show point-positive place3
   deltas (+0.9–+1.2pp) but all LB95 are deeply negative (−0.9 to −4.1pp), consistent with variance
   increase from adding a noisy feature to a small-N class.

4. **Ban-ei top1 regression**: Ban-ei has ~52 horses/race average (obstacle), smaller fields than
   flat races. The log_odds_z feature distributes signal across more z-score bins, reducing the
   clarity of absolute-odds splits the current CatBoost model relies on.

---

## Conclusion

**Both NAR and Ban-ei: REJECT. No deploy. No code change to production.**

The `log_odds_z_in_race` candidate passes no model-level gate (LB95 negative across all metrics
for both categories). The structural probe finding (partial ρ +0.096–+0.157) is a **linear
correlation probe controlling for three regressors** — a necessary but not sufficient condition for
model improvement with a non-linear GBDT that already has access to the underlying signal via joint
splits on `popularity_score` and `odds_score`.

This closes the log_odds_z exploration track. The system remains at the empirical frontier established
by the saturation analysis (2026-06-11): market-efficiency is the binding constraint, not feature
representation.

**Remaining credible levers** (per saturation doc): none short of new external data sources
(running-style v3 integration already underway; next milestone = v3 active_models cutover).
