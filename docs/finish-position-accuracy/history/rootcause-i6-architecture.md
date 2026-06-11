---
investigation: I6
title: Architecture Ceiling — is the GBDT-pointwise-ish + top1-blend structure leaving place accuracy on the table?
date: 2026-06-11
scope: read-only + bounded feasibility computation
verdict: ARCHITECTURE IS NOT THE PRIMARY BINDING CONSTRAINT — signal saturation is
feasibility_result:
  calibration_joint_placement: place2 +0.5pp / place3 +0.333pp / top1 ±0.0pp (simulation)
  plackett_luce: zero lift (mathematically equivalent to raw ranking)
  conditional_rerank: negative (-1.167pp place2)
  calib_place2_rerank: catastrophic (-10pp top1)
mlx_re_analysis: loss was insufficient WF folds (2 vs 21) + no HPO, not architectural failure
artifact: tmp/rootcause/i6_arch.json
---

# I6: Architecture Ceiling Root-Cause Investigation

## 1. Question

Is the model ARCHITECTURE (not features) the primary cap on place2/place3 accuracy? Specifically:

- Is the current GBDT pipeline effectively pointwise, or does it exploit joint within-race structure?
- Would a calibration layer, multi-task head, or listwise ranking model (Plackett-Luce) show a lift?
- Why did MLX Set Transformer lose — was it data/feature representation, training scale, or fundamental?

## 2. Representation Analysis: Is the pipeline pointwise or joint?

**Verdict: HYBRID — training is listwise, inference is pointwise-with-field-context features.**

### 2.1 Training side

All three model families use listwise / pairwise training objectives:

- LightGBM: `lambdarank` objective with `ndcg` metric; races passed as groups to `lgb.Dataset`
- CatBoost: `YetiRank` (approx-NDCG) objective; races passed as `Pool.group_id`
- XGBoost: `rank:pairwise` (RankNet-style BT loss); races as groups

All three see the within-race structure during training. Gradients flow from the full NDCG@3 evaluation, which is inherently listwise.

### 2.2 Inference side

All three call `booster.predict(frame)` where `frame` is a flat per-horse DataFrame. There is no within-race communication at inference time. The ranking is derived purely from the independent per-horse scores.

### 2.3 Features: race-relative features ARE present

The feature set already contains explicit field-relative signals:

- `popularity_rank_in_race` — horse's rank within the field by popularity
- `inverse_odds_rank_in_race` — horse's rank by inverse odds
- `inverse_odds_market_share` — horse's share of the market
- `h2h_win_rate_vs_field`, `h2h_avg_finish_diff_vs_field` (v7) — head-to-head vs current field members
- Monotone constraints on odds/popularity ensure these features are exploited correctly

The GBDT is **not** naively pointwise in its inputs. It sees where each horse stands in the field.

### 2.4 What a full attention architecture adds

A full cross-horse attention model (like the existing `RaceSetTransformer`) can discover higher-order competitive patterns:

- "Horse A suppresses Horse B specifically when C is also present"
- Non-linear field composition effects
- Pace scenario multi-way interactions beyond the running-style feature columns

But these are speculative gains — the engineered features already approximate much of this.

---

## 3. Feasibility Test: Can calibration/multi-task/listwise layers lift place accuracy?

**Method**: 3,000 synthetic races × 10 horses, Gaussian noise model (sigma=0.8) calibrated to match ~52% top1 accuracy. 80% train for isotonic calibration fitting, 20% test evaluation. Pure Python + numpy + sklearn.isotonic. Single-threaded, 15 seconds wall time.

### 3.1 Strategies Tested

| Strategy                  | Description                                      |
| ------------------------- | ------------------------------------------------ |
| A — Baseline              | Raw GBDT score → descending rank                 |
| B — Plackett-Luce         | BT model expected rank from scores               |
| C1 — Calib top1 re-rank   | Isotonic P(finish=1) → rank by P(1)              |
| C2 — Calib place2 re-rank | Isotonic P(finish=2) → rank by P(2)              |
| C3 — Calib place3 re-rank | Isotonic P(finish=3) → rank by P(3)              |
| C4 — Joint placement      | Rank by 3·P(1) + 2·P(2) + 1·P(3)                 |
| D — Conditional place2    | Predict rank1 by P(1), then re-rank rest by P(2) |

### 3.2 Results (delta vs baseline, pp)

| Strategy             | top1 Δ      | place2 Δ   | place3 Δ   | box Δ  |
| -------------------- | ----------- | ---------- | ---------- | ------ |
| B Plackett-Luce      | 0.0         | 0.0        | 0.0        | 0.0    |
| C1 Calib top1 rank   | −0.167      | **+0.833** | +0.167     | −0.333 |
| C2 Calib place2 rank | **−10.167** | −2.5       | −0.667     | −1.5   |
| C3 Calib place3 rank | **−25.167** | −6.0       | −2.167     | −4.667 |
| C4 Joint placement   | 0.0         | **+0.5**   | **+0.333** | −0.833 |
| D Conditional place2 | −0.167      | **−1.167** | −0.833     | −1.667 |

### 3.3 Key Findings

**B — Plackett-Luce = zero lift.** Ranking by expected position under the Bradley-Terry/Plackett-Luce model is mathematically equivalent to ranking by the raw score when scores are in logit space. It is not a distinct architecture — it is the same ordering.

**C2/C3 — catastrophic.** Ranking by P(finish=2) to improve place2 is deeply wrong. P(finish=2) is a bell-shaped function of horse quality. Mediocre horses in weak fields have the highest P(2). Ranking by P(2) moves those horses to rank 2, which loses top1 accuracy (-10pp) without improving place2. This confirms the 2026-05-20 empirical cascade failure.

**C4 — Joint expected placement is the only promising architecture modification.** 3·P(1)+2·P(2)+1·P(3) showed +0.5pp place2 and +0.333pp place3 with zero top1 cost. The calibrated probabilities need to be accurate for this to work. The existing `calibrate_finish_position.py` (isotonic regression per-bucket) already fits this pattern — **it is the correct implementation if applied to re-rank by joint expected placement rather than by P(top1) alone**.

**D — Conditional place2 = negative.** Mirrors the transformer's `conditional_place2_logit` (concat horse emb + winner emb → MLP). The simulation and the 2026-05-20 Phase B experiment agree: this approach hurts because the conditional P(2|≠rank1) is dominated by the same horse quality signal, not by new information about who specifically runs 2nd.

---

## 4. MLX Transformer Re-Analysis

### 4.1 Architecture (existing code)

The `RaceSetTransformer` in `finish_position_transformer/model.py` is a genuine joint architecture:

- 2-layer TransformerEncoder, 4 heads, 64-dim embeddings
- All horses in the race attend to each other (masked for padding)
- 7 output heads: top1/top3/place2/place3/rank_score + conditional_place2 + conditional_place3
- Multi-task loss: BCE × 4 + pairwise ranking + ListNet + conditional BCE × 2
- Conditional heads concat horse embedding with softmax-weighted winner/runner-up embeddings

This is architecturally superior to GBDT in its ability to model cross-horse interactions.

### 4.2 Why it lost

| Root Cause                          | Evidence                                                                                                                                                                           | Weight              |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- |
| **Insufficient WF folds (2 vs 21)** | Transformer only tested on 2024-2025 (2 folds). GBDT evaluated on 2007-2026 (21 folds). 2-fold WF has high variance — cannot detect +0.1-0.5pp improvements reliably.              | HIGH                |
| **No HPO**                          | Transformer has 8 loss weights + 4 architecture params + LR schedule. GBDT had Optuna HPO (30 trials). Asymmetry hugely disadvantages transformer.                                 | HIGH                |
| **Same features**                   | Dataset.py shares feature columns with `finish_position_lightgbm.py`. No additional representation benefit from the architecture if the features are identical.                    | MEDIUM              |
| **Sample efficiency**               | At ~100K training races, GBDT tabular advantage is well-documented (Grinsztajn 2022, Gorishniy 2023). Transformer advantage grows with scale — not yet demonstrated at this scale. | MEDIUM              |
| **True architectural limitation**   | P(finish=2) is not separable from P(finish=1) without new features — no architecture fixes this.                                                                                   | LOW (same for GBDT) |

### 4.3 Would a probability-ranking layer capture the gain MLX couldn't?

Yes, partially — but only C4 (joint placement score) and only if calibration is accurate. The issue is that MLX lost on top1 as well (-8.5pp), which suggests its rank_score head was not competitive, not just its place heads. A calibration layer on GBDT scores (C4 approach) is the safer path.

---

## 5. Architecture Ceiling Verdict

**Architecture is NOT the primary binding constraint. Signal saturation at the current 241-feature set is.**

### 5.1 What architecture cannot fix

- The fundamental correlation between P(finish=1) and P(finish=2): both are driven by horse quality. No architecture can distinguish "this horse will finish 1st" from "this horse will finish 2nd" without features that capture why specific horses run 2nd rather than 1st.
- Current JRA top1 at 52.37% is near the ceiling achievable with available signals given natural race randomness.
- Place2 at 28.58% is approximately at the Pareto frontier of the current feature set (simulation ceiling ~28-29%).

### 5.2 What architecture could fix (speculative, worth one bounded experiment)

- **Joint expected placement scoring (C4)**: Re-ranking by 3·P(1)+2·P(2)+1·P(3) from calibrated probabilities showed +0.5pp place2 in simulation. The infrastructure exists (`calibrate_finish_position.py`). The experiment would be: run `calibrate_finish_position.py --mode apply` and change the `re_rank_predictions` call to use `predicted_top1_prob_calibrated * 3 + predicted_top3_prob_calibrated * 2 + ...` as the ranking key.
- **Full 21-fold transformer WF + HPO**: If the architecture can match GBDT on top1, its place heads (especially the conditional heads) may provide additional lift. But this requires ~20x more compute than the 2-fold test.

### 5.3 Highest-leverage paths (not architecture)

1. **Rivalry/competition-dynamic features**: horse_win_rate_when_field_contains_frontrunner, career_place2_rate_when_favorite_present. These encode WHY specific horses run 2nd vs 1st.
2. **Odds dynamics features (under investigation in feas-2026-06-11-odds-dynamics.md)**: late-market money flow may signal 2nd-place horses specifically.
3. **Timing/sectional data (under investigation in feas-2026-06-11-timing-data.md)**: lap-level speed profiles may distinguish horses that accelerate late (2nd-place tendency) from those that dominate wire-to-wire.

---

## 6. Recommendation

| Action                                                 | Priority                | Rationale                                                                                            |
| ------------------------------------------------------ | ----------------------- | ---------------------------------------------------------------------------------------------------- |
| Do NOT re-rank by P(place2) or P(place3) independently | CONFIRMED STOP          | Catastrophic -10pp top1, confirmed both in simulation and 2026-05-20 Phase B                         |
| Do NOT pursue conditional specialist reranking         | CONFIRMED STOP          | Negative cascade effect confirmed in 3 independent experiments                                       |
| Do NOT rely on Plackett-Luce as improvement            | CONFIRMED STOP          | Mathematically equivalent to raw ranking                                                             |
| INVESTIGATE: C4 joint placement re-rank                | LOW EFFORT (~1 day)     | +0.5pp place2 / +0.333pp place3 in simulation, zero top1 cost, infrastructure exists                 |
| INVESTIGATE: Full transformer 21-fold WF + HPO         | MEDIUM EFFORT (~3 days) | Architecture is sound; 2-fold test was insufficient. But unlikely to beat GBDT without new features. |
| PRIORITIZE: New rivalry/dynamics signal                | HIGH IMPACT             | This is where the remaining headroom lies, regardless of architecture                                |

---

## 7. Relation to Existing Investigation History

This investigation was prompted by the S1 trigger (4 consecutive rejects, iter 15-18) and the saturation confirmed across JRA, NAR, and Ban-ei in the 2026-06-11 science track cycle. The conclusion aligns with the 2026-05-20 legacy doc:

> "place2/place3 改善は現アプローチでは empirical 不可行 — 新 signal 投入が必要"

I6 adds the architectural analysis: the architecture head modifications tried (conditional attention, binary specialists, Plackett-Luce) were correctly identified as non-solutions. The ONE untried path that is architecturally motivated and shows simulation signal is the joint expected placement scoring (C4), which re-ranks by a probability-weighted combination rather than by P(1) alone.
