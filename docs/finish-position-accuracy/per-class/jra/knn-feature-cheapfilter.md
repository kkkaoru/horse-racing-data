---
probe: within-race-relative-knn-additive-feature
date: 2026-06-17
class: 703
method: CatBoost YetiRank WITH vs WITHOUT V4 kNN score as additive feature
status: ABORT — ρ≈0.11 does NOT convert; GBDT already captures within-race-relative structure
---

# Within-Race-Relative kNN Feature Cheap-Filter — JRA 703

## Question

The clean-vectorization ablation (`clean-vectorization-ablation.md`, commit d83533a) found that
the **V4 kNN score** (within-race z-score, odds EXCLUDED, NULL-aware, curated feature subset,
univariate-weighted) has orthogonal partial-ρ ≈ −0.11 vs `finish_norm` after controlling for
both `log(odds)` and the GBDT score (iter19). This cleared the 0.08 gate comfortably.

The prior kNN-as-blend-member result gave top1 −0.166pp (iter32). But that used the old
value-prep (odds contamination, absolute features). Does the **clean V4 score**, added as a
single additive feature and retrained, actually improve the CatBoost model?

Partial-ρ is a necessary condition (orthogonality), not sufficient. GBDT may already capture
the within-race-relative structure non-linearly. This cheap-filter retrain tests whether it does.

## Experiment Design

**Split**: Train 2013–2024 (214,373 rows / 14,761 races), blind holdout 2025 (17,894 rows / 1,252 races).
Both train and holdout restricted to 703 (未勝利) via bucket membership parquet.

**V4 kNN score construction** (reproducing the ablation's winning variant):

1. Feature set (18 curated features, odds EXCLUDED):
   `speed_index_avg_5`, `speed_index_best_5`, `kohan3f_avg_5`, `career_win_rate`,
   `career_place_rate`, `same_distance_win_rate`, `jockey_recent_win_rate`,
   `trainer_career_win_rate`, `pedigree_score_for_race`, `last_race_finish_norm`,
   `finish_trend_5`, `weight_avg_5`, `career_top1_count`, `recent_win_count_5`,
   `recent_top3_count_5`, `avg_finish`, `recent_finish`, `days_since_last_race`.

2. Univariate |Spearman ρ| weighting (fit on train). Top-5 weights:
   `recent_finish` (0.492), `avg_finish` (0.487), `last_race_finish_norm` (0.485),
   `recent_top3_count_5` (0.396), `career_place_rate` (0.395).

3. Within-race z-score transform: `(val − race_mean) / race_std`.

4. Median impute + NULL indicator for features with >5% NULLs.

5. Global StandardScaler (fit on train).

6. kNN (L2, k=50, sklearn ball_tree). **Leak-free**: for each train row, neighbors
   come only from strictly earlier race dates (date-ordered cumulative index).
   For holdout (2025), all train rows (2013–2024) are prior — no leak.
   109 train rows had <50 prior neighbors and were filled with the train median.
   kNN score = `1 − mean(finish_norm of k nearest neighbors)`.

**CatBoost config** (mirrors iter20 JRA 703):
YetiRank, depth=8, lr=0.05, l2_leaf_reg=3.0, od_wait=30, random_seed=2068, iterations=1000,
bootstrap_type=Bayesian, thread_count=6. Time-decay sample weights (0.5→1.0, oldest→newest).

**WITH model**: 243 base features + 1 kNN score = 244 features.
**WITHOUT model**: 243 base features.

Both models use identical architecture, hyperparameters, seed, and data split.
Metrics evaluated on blind 2025 holdout.

**Exact-ordinal metrics**:

- `top1`: pred-rank-1 horse finishes 1st
- `place2`: pred-rank-2 horse finishes exactly 2nd
- `place3`: pred-rank-3 horse finishes exactly 3rd
- `top3_box`: any of pred-ranks 1-3 overlaps with actual top-3 (≥1 match)
- `fukusho_2p`: pred-rank-1 horse finishes 1st or 2nd

**Verdict gate** (relaxed per task spec): ADOPT iff ≥1 primary (top1/place2/place3) LB95≥0
AND no primary regression beyond −0.05pp observed.

## Results

### Model training

| Model   | Features | Best iter | Train time |
| ------- | -------- | --------- | ---------- |
| WITHOUT | 243      | 187       | 19.3s      |
| WITH    | 244      | 201       | 20.7s      |

### Exact metrics on blind 2025 holdout (1,252 races)

| Metric     | WITHOUT | WITH   | Δ (pp)    |
| ---------- | ------- | ------ | --------- |
| top1       | 49.04%  | 48.32% | **−0.72** |
| place2     | 25.88%  | 25.96% | +0.08     |
| place3     | 18.45%  | 18.85% | +0.40     |
| top3_box   | 97.60%  | 97.60% | 0.00      |
| fukusho_2p | 67.81%  | 67.41% | −0.40     |

### Paired bootstrap LB95 (10,000 iterations, seed=42, race-level resample)

| Metric     | Observed Δ | LB95    | Gate     |
| ---------- | ---------- | ------- | -------- |
| top1       | −0.72pp    | −1.76pp | **FAIL** |
| place2     | +0.08pp    | −1.04pp | **FAIL** |
| place3     | +0.40pp    | −0.64pp | **FAIL** |
| top3_box   | 0.00pp     | −0.16pp | **FAIL** |
| fukusho_2p | −0.40pp    | −1.36pp | **FAIL** |

All five metrics have LB95 below zero. No primary metric has LB95≥0.

## Verdict: ABORT

**ρ≈0.11 does NOT convert to a model gain.**

The V4 kNN score adds a structurally clean, odds-free, within-race-relative representation
of horse ability. Its orthogonal partial-ρ to finish_norm (after controlling for GBDT score
and odds) is −0.11 — well above the 0.08 gate. Despite this, adding it as a feature causes:

- top1 to **fall −0.72pp** (LB95 = −1.76pp)
- place2 gains only +0.08pp but LB95 = −1.04pp (noise)
- place3 gains +0.40pp but LB95 = −0.64pp (noise)

The gate requires ≥1 primary LB95≥0 AND no severe regression. 0/3 primaries clear LB95
and top1 falls 0.72pp (exceeds the −0.05pp no-regression threshold). Gate: FAIL.

### Interpretation

The CatBoost model on 243 features already captures the within-race-relative structure
through its tree splits. The information in `knn_v4_ability_score_k50` is redundant given
those 243 features — GBDT's non-linear combination of `recent_finish`, `avg_finish`,
`last_race_finish_norm`, `speed_index_*`, etc. already constructs a functionally equivalent
relative ranking. The partial-ρ gate (orthogonal after controlling for the GBDT **score**)
tests independence from the single scalar output, but the GBDT's **internal representation**
fully contains the signal.

This is consistent with the D-phase lesson: ρ is necessary, not sufficient. Non-linear
GBDT captures signals that appear residual under linear partial correlation.

### kNN score statistics (for reference)

- Train score: mean=0.5026, std=0.1776
- Holdout score: mean=0.5017, std=0.1807
- 109 train rows had <50 prior-date neighbors (early 2013 data) → filled with median

## Serve Feasibility Note

Had this ADOPT'd, serve-computation of the kNN score would require:

1. A historical embedding index over 2013–2024 703 rows (V4 vectors, ~214k rows).
2. At serve time: compute within-race-relative V4 vector for each race entry (requires
   all horses in the same race to have features — available at serve time).
3. kNN lookup against the embedding index → 1 float score per horse.
4. A vector index (e.g., FAISS or pgvector) hosted in the Worker or an external service.

This is architecturally feasible but adds ~10-50ms per race prediction call (depending
on index size and serving infrastructure). Since this is ABORT, serve infra is not needed.

## Closing the Vectorization Avenue

Combined with `clean-vectorization-ablation.md`:

- **Race-condition one-hot kNN** (ablation): genuine null — partial-ρ = −0.005, p=0.28.
- **V0 kNN (prior probe)**: partial-ρ partially inflated by odds contamination in vector.
- **V4 kNN score as additive feature (this experiment)**: partial-ρ = −0.11 clears gate,
  but model retrain yields no gain (top1 −0.72pp, all LB95 < 0).
- **kNN-as-blend-member (iter32)**: top1 −0.166pp with raw-value contaminated score.

**Conclusion**: The within-race-relative vectorization signal is real (ρ > 0.08, p < 10⁻¹⁴⁸)
but GBDT already captures it. Vectorization is genuinely redundant with the 243-feature
CatBoost model for JRA 703. Further kNN / vector-search exploration within this
feature space is closed. Any future vectorization work for this class would need either
(a) new input signals not currently in the feature store, or (b) a learned embedding trained
end-to-end on the ranking objective (not tested here; Ensemble C scope).

## Quality Gate (docs only — no code change)

This is a probe result. No model code, enforced-package files, or production features were
modified. No `tmp/` files were git-added. Artifacts are in `/tmp/` only.
