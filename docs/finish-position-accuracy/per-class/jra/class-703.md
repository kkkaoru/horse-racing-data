---
class: 703
label: 未勝利 (Maiden)
category: jra
n_races_holdout_2023_26: 4229
baseline_top1: 49.40
baseline_place2: 25.80
baseline_place3: 19.06
model_vs_market_top1_delta: +14.31
active_model: iter19-jra-cb-kohan3f-going-v8 (category-global fallback)
per_class_model: none (Phase B registry empty)
---

# JRA 703 — 未勝利 (Maiden, no wins)

## Status

Routes to category-global `iter19-jra-cb-kohan3f-going-v8`. No per-class model registered.
Largest JRA class by n (4,229 holdout races — best statistical power for JRA experiments).

## Headroom

| Metric | Model% | Market% | Oracle% | Status               |
| ------ | -----: | ------: | ------: | -------------------- |
| top1   |  49.40 |   35.09 |   35.23 | MODEL_EXCEEDS_ORACLE |
| place2 |  25.80 |   18.70 |       — | MODEL_EXCEEDS_ORACLE |
| place3 |  19.06 |   14.92 |       — | MODEL_EXCEEDS_ORACLE |

Gap to 60%: −10.60pp (top1). Largest class → most robust for per-class experiments.

## Active Hypotheses

_(To be populated as experiments are designed — see ROADMAP.md)_

## Evaluation Log

| Date       | Hypothesis                                  | Method                            | Verdict | Ref               |
| ---------- | ------------------------------------------- | --------------------------------- | ------- | ----------------- |
| 2026-06-17 | RL policy-gradient ranker (MLX)             | REINFORCE / MLP                   | ABORT   | See section below |
| 2026-06-17 | Ensemble (GBDT+kNN+RL method-diverse blend) | rank-avg / opt-w / Ridge stacking | ABORT   | See section below |

---

## RL policy-gradient ranker

**Date:** 2026-06-17  
**Verdict: ABORT**

### Architecture

- **Framework:** MLX 0.31.2 (Metal GPU, M5 Pro unified memory)
- **Policy network:** shallow MLP — Linear(236→64) → GELU → Linear(64→64) → GELU → Linear(64→1)
  - Output: per-horse scalar logit; no padding (variable field size up to 18 clipped)
- **Sampling:** Plackett-Luce via Gumbel-max trick — sample permutation proportional to `exp(logit)`
- **Features:** 236 numeric features from `feat-jra-v8-iter19-kohan3f-going` (all non-metadata numeric columns, NaN→0, z-scored on train)
- **Training data:** class 703 rows from race_year 2013–2021 (n=9,671 valid races)
- **Val:** race_year 2022 (n=1,094 races)
- **Holdout:** race_year 2023–2025 (n=3,258 races)

### Reward function

Per-race REINFORCE reward (exact-match definitions matching GBDT baseline):

```
r = 1.0 * top1_hit + 0.5 * place2_hit + 0.3 * place3_hit + 0.3 * top3_box_hit
```

- `top1_hit`: predicted-rank-1 horse finishes position 1
- `place2_hit`: predicted-rank-2 horse finishes **exactly** position 2
- `place3_hit`: predicted-rank-3 horse finishes **exactly** position 3
- `top3_box_hit`: our top-3 horses exactly equal the true top-3 set

Advantage = `reward − baseline_EMA` (α=0.05, updated per batch of 64 races).  
Log-probability computed via Plackett-Luce over top-3 steps only.

### Optimizer

Adam, lr=3e-4, batch=64 races, max 30 epochs, patience=6 on val top1.  
Early-stopped at epoch 17 (best val top1=37.57% at epoch 11).

### Training curve summary

| Epoch |    Loss | Mean reward | Baseline EMA | Val top1 |
| ----: | ------: | ----------: | -----------: | -------: |
|     1 |  0.1434 |      0.4372 |       0.4904 |   35.47% |
|     5 | -0.0321 |      0.5194 |       0.5120 |   36.29% |
|    11 | -0.0325 |      0.5264 |       0.4936 |   37.57% |
|    15 | -0.0176 |      0.5326 |       0.5231 |   36.84% |
|    17 | -0.0204 |      0.5355 |       0.5597 |   37.02% |

Reward steadily improved from 0.437→0.535 over training; val top1 plateaued at ~37% after epoch 11. The training loss turned negative because REINFORCE loss = −advantage × log_p, and with positive advantages (reward > baseline) the gradient correctly increases log_p — the negative value reflects the sign convention, not divergence.

### Holdout results (2023–2025, n=3,258 races)

| Metric     | RL (greedy argmax) | GBDT baseline |        Delta |
| ---------- | -----------------: | ------------: | -----------: |
| top1       |             38.67% |        49.40% | **−10.73pp** |
| place2     |             19.71% |        25.80% |  **−6.09pp** |
| place3     |             14.40% |        19.06% |  **−4.66pp** |
| top3_box   |             11.36% |             — |            — |
| fukusho_2p |             62.58% |             — |            — |

**Paired bootstrap LB95 (10k, seed=42) on top1 vs GBDT:** −12.11pp

All three main metrics are strongly negative. ABORT threshold: LB95 ≥ 0 required; actual = −12.11pp.

### Interpretation and why GBDT wins

1. **Sample efficiency.** REINFORCE uses sparse binary rewards (e.g., top1_hit ∈ {0,1}). With ~10k training races and 15–18 horses per race, the policy sees each race once per epoch and gets a noisy reward signal. GBDT trains on a dense pointwise prediction task (finish_norm), exploiting all 244 features with boosting's additive corrections. The effective signal-to-noise ratio is far higher for GBDT.

2. **Credit assignment.** The policy must learn which horse logit drove the winning outcome. REINFORCE attributes the reward equally to all top-3 logits via Plackett-Luce log-probability, creating diffuse gradients. GBDT computes exact feature-importance gradients per leaf.

3. **No surrogate loss.** The RL reward is the exact ranking metric, but this is a hard binary signal. GBDT optimizes a smooth surrogate (cross-entropy / ranking loss) that provides dense gradients everywhere in feature space. The RL policy converges to val top1~37%, never approaching the 49.4% GBDT achieves — consistent with the known gap between policy-gradient methods and supervised learning on tabular ranking.

4. **Odds feature dominance.** The `tansho_odds_raw` / `inverse_odds_implied_prob` features alone carry most of the predictive signal (market efficiency). GBDT builds exact split thresholds on these; the MLP must learn a smooth approximation. Given the existing market-efficiency ceiling confirmed in prior experiments (top1 oracle~35%), GBDT at 49.4% is already beyond market — the RL MLP at 38.7% cannot even match the market.

5. **Architecture ceiling.** A 2-layer MLP with 64 hidden units on 236 features is a weak learner. GBDT's ensemble of thousands of trees represents a substantially richer function class for tabular data.

**Conclusion:** REINFORCE / policy-gradient ranking on JRA tabular data is clearly inferior to GBDT. This is the expected result for tabular ranking problems and confirms the established empirical finding that tree-based methods dominate neural networks on structured/tabular data. The RL approach would require orders of magnitude more data, a stronger base model (e.g., transformer ranker fine-tuned with policy gradient), or a fundamentally different reward formulation (e.g., NDCG relaxation with smooth gradients) to compete.

---

## Ensemble (GBDT+kNN+RL method-diverse blend)

**Date:** 2026-06-17
**Verdict: ABORT**

### Motivation

The prior probes established three independently-derived per-horse scores for JRA 703:

- **GBDT** (`iter19-jra-cb-kohan3f-going-v8`): strong (~48–49% top1 on 703 holdout).
- **kNN** (pgvector-kNN probe, k=50, horse_ability embedding): weak (~36%), but partial-ρ = −0.0892 after controlling for GBDT + odds — marginally orthogonal by the probe gate.
- **RL** (REINFORCE/MLP policy-gradient ranker): weak (~38–39%), individual ABORT.

The question: does combining them beat GBDT-alone? Ensembling helps only when the components are both accurate **and** de-correlated. Here they are weak (~12pp below GBDT) and likely highly correlated (all use the same 236 features).

### Experimental design

Blind holdout discipline:

- **Tune split** (weight selection): race_year 2023–2024 (n=2,155 races)
- **Blind gate split**: race_year 2025 (n=1,103 races)

Three blend methods evaluated:

1. **Rank-average**: per-race rank-normalize each method, weight 1/3 each.
2. **Weight-optimized**: grid search over (w_gbdt, w_rl, w_knn) with constraint w_gbdt ≥ 0.5 on the tune split; apply best weights to blind.
3. **Ridge stacking**: fit Ridge regression (target = −finish_position) on rank-normalized scores in tune split, predict on blind.

Per-horse scores:

- GBDT: `iter19_score` from WF prediction parquets (already computed).
- RL: re-trained on 2013–2021 with same architecture as probe (MLX MLP, 236 features, early-stop at epoch 17, val top1=37.57%).
- kNN: horse_ability embedding (15 features), k=50, `StandardScaler` fit on 2013–2021 train, kNN score = `1 − mean(finish_norm of k nearest neighbors)`. All neighbors strictly from train years (confirmed in probe doc).

Coverage: 100% of races had all three scores available for both tune and blind splits.

### Results

#### GBDT-alone baseline

| Split            |   top1 | place2 | place3 |
| ---------------- | -----: | -----: | -----: |
| Tune (2023–2024) | 48.68% | 26.31% | 18.65% |
| Blind (2025)     | 48.23% | 25.66% | 20.40% |

#### All blend methods (blind split 2025, n=1,103 races)

| Method                            |   top1 | place2 | place3 | top3_box | fukusho_2p |   Δtop1 | Δplace2 | Δplace3 | LB95 (top1) |
| --------------------------------- | -----: | -----: | -----: | -------: | ---------: | ------: | ------: | ------: | ----------: |
| rank_avg (1/3, 1/3, 1/3)          | 42.07% | 21.67% | 15.23% |   12.78% |     65.82% | −6.17pp | −3.99pp | −5.17pp |     −8.61pp |
| opt_w (GBDT=1.0, RL=0.0, kNN=0.0) | 48.23% | 25.66% | 20.40% |   18.40% |     72.89% | +0.00pp | +0.00pp | +0.00pp |     −2.45pp |
| Ridge stacking                    | 48.32% | 25.75% | 20.40% |   18.40% |     72.89% | +0.09pp | +0.09pp | +0.00pp |     −2.36pp |

**Ridge stacking coefficients** (fit on tune): `gbdt=+0.628, rl=+0.017, knn=−0.063`.

#### Key observations

- **Weight optimizer assigns 100% to GBDT.** The tune-split grid search over w_gbdt ∈ [0.5, 1.0] finds the global optimum at w_gbdt=1.0, w_rl=0.0, w_knn=0.0 — meaning no blend of the three methods improves over GBDT alone on the selection split.
- **Ridge stacking is nearly identical to GBDT-alone.** The Ridge coefficients assign near-zero weight to RL (+0.017) and a negative weight to kNN (−0.063), confirming the kNN score adds noise rather than signal in the full model context. The blind-split top1 improvement (+0.09pp) is within bootstrap noise: LB95 = −2.36pp (required ≥ 0).
- **Rank-average severely hurts.** Diluting GBDT scores with the two weak components (RL~38%, kNN~36%) collapses top1 by −6.17pp and place2 by −3.99pp. This is the expected result when combining a strong model (49%) with two methods that individually underperform by ~12pp.
- **ABORT gate:** best-blend LB95 = −2.36pp < 0 required. All deltas on the blind split are either zero or statistically indistinguishable from zero.

### Why the ensemble fails

1. **Accuracy asymmetry is too large.** GBDT outperforms RL and kNN by ~12pp. For ensemble averaging to help, components need to be roughly equal in accuracy (or the weaker components must win on specific subsets that GBDT loses). Here, GBDT is uniformly dominant: whenever GBDT is wrong, the probability that RL or kNN is right is no higher than their individual ~36–38% rates — much lower than the 50% complementarity needed to net a gain.

2. **The kNN partial-ρ (−0.089) is real but insufficient.** The probe gate tests whether a marginally-orthogonal signal exists. It does — but "orthogonal to GBDT+odds" is not the same as "adds net value in a blend." The Ridge coefficient for kNN is **negative** (−0.063): after controlling for GBDT rank, higher kNN score is weakly associated with _worse_ finish outcomes on the tune split. This means the kNN's marginal signal is too noisy to survive blending.

3. **RL and kNN share the same 236 features as GBDT.** Any residual signal the MLP captures is a noisy approximation of what the GBDT already extracts optimally from the same feature set. True method diversity (e.g., a model using entirely different data sources) would be needed for useful de-correlation.

4. **This outcome was predicted a priori.** The prior kNN ensemble member test (iter32-jra-vec-knn-703-v8) showed top1 delta = −0.166pp when used as a blended member. The method-diverse blend here reproduces and extends that finding: combining GBDT with method-inferior components dilutes the dominant signal.

### Conclusion

**ABORT.** The method-diverse ensemble (GBDT + kNN + RL) does not beat GBDT-alone on JRA 703. The weight optimizer degenerates to GBDT-only (w=1.0, 0.0, 0.0) on the tune split; the blind-split LB95 = −2.36pp confirms no real improvement. Adding structurally different but accuracy-inferior components to a strong GBDT is confirmed to dilute performance. This closes the method-diversity avenue for JRA 703 unless a new component with individual accuracy ≥ GBDT or a qualitatively different feature source (not the same 236-feature set) can be identified.
