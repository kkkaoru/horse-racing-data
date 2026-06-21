# JRA Ablation — seibetsu_code + zogen_sa + kaisai_month

**Date**: 2026-06-20
**Category**: JRA finish-position
**Verdict**: **REJECT** — 3 raw features add no incremental signal to 121-feature GBDT.

## Ablation Design

- Feature store: `tmp/feat-jra-v8-season-sex-weight/` (656,057 rows, 142 cols, 2013-2026)
- Walk-forward: 3-fold (train 2013→{2022,2023,2024}, test {2023,2024,2025})
- Config: CatBoost YetiRank, iter20-jra-cb-2013-v8 production params (iters=500, depth=8, l2=3.0, lr=0.05)
- Baseline: 121 numeric features (excluding seibetsu_code, zogen_sa, kaisai_month)
- Candidate: 124 features (baseline + 3 new)
- Bootstrap: n=2000, seed=42, paired race-level resampling

## Results

### Pooled (n_races=10,365)

| Metric     | Delta (pp) | LB95   |
| ---------- | ---------- | ------ |
| top1       | +0.087     | -0.183 |
| place2     | -0.039     | -0.386 |
| place3     | -0.338     | -0.685 |
| top3_box   | -0.039     | -0.251 |
| fukusho_2p | -0.010     | -0.299 |

### Per-fold

| Fold          | top1   | place2 | place3 | fukusho_2p |
| ------------- | ------ | ------ | ------ | ---------- |
| 1 (test 2023) | +0.087 | -0.260 | -0.637 | -0.174     |
| 2 (test 2024) | -0.232 | +0.145 | -0.347 | -0.347     |
| 3 (test 2025) | +0.405 | +0.000 | -0.029 | +0.492     |

### Gate

- fukusho_2p LB95 > 0: **FALSE** (-0.299)
- Positive axes ≥ 2: **FALSE** (1/4)
- Veto floor > -0.05: **FALSE** (place3 = -0.338)
- **→ REJECT on all clauses**

## Interpretation

GBDT already captures weight (bataiju, weight_diff_from_avg), season (season_band), and sex effects non-linearly. The 3 raw features are redundant. zogen_sa (75.5% non-null) introduces partial-NULL noise that degrades place3 specifically — consistent with D-phase lesson that feeding partially-NULL raw features to GBDT hurts.

## Companion Probes (all REJECT)

### 着順×体重×増減 コンディション回復パターン — REJECT

User hypothesis: "bad result + weight loss → weight recovery → good run". Data shows the opposite:

- Recovery group win rate 4.56% vs baseline 11.31%
- Model already correctly pessimistic about recovery horses (precision 37.75%, only 551 picks)
- V-shape trajectory is 3rd of 4 patterns (worse than monotone gain)
- finish_trend_5 + weight_diff_from_avg + zogen_sa already encode this

### 脚質×競馬場×距離×芝ダート — REJECT

- Precision spread is base-rate sorting (nige > senkou > sashi > oikomi at every venue), not model defect
- track_bias_front + track_bias_inside already capture venue-conditional pace bias
- Model over-exploits nige uniformly (pick/win ratio 1.20-1.35 across venues)
- RS down-ranked-nige leak (~20%) = rs_p_nige estimation quality, not cross-term

### 騎手×競馬場×距離×芝ダート — INCONCLUSIVE (thin cells)

- Worst cells (松田大作/小倉芝 12.5%, 鮫島良太/阪神芝 15.6%) have N=30-50, too thin for action
- jockey_keibajo_win_rate + jockey career features cover most variance
- No systematic venue×jockey blind spot with sufficient N

### 体重×増減 Heatmap

- Light horses (<440kg) with weight loss = worst precision (34.6%)
- Very heavy horses (520+kg) best regardless of change (40.7-45.8%)
- Clear weight band × change interaction but GBDT already uses bataiju + weight_diff_from_avg
- Rank 1-6 avg weight monotonically decreasing (478.8 → 471.9) — model correctly prefers heavier horses

## Next Steps

Raw GBDT feature addition is exhausted for season×sex×weight.

### Layer 2 夏牝馬 serve-time swap — REJECT (2026-06-20)

Two independent analyses confirm REJECT:

**Analysis 1 — Rank swap (orchestrator)**: 2,455 candidates (male=#1 female=#2), help=576/hurt=958, net=-382, ratio=0.60. Per-venue: best=Niigata 0.842, worst=Tokyo 0.387. All 8 venues ratio < 1.0.

**Analysis 2 — Full score simulation** (`tmp/layer2_summer_mare_sim.py`, 66,967 races, real predicted_score):
Best bonus=0.10 (summer only): top1 +0.094pp but place2 -0.089pp.
Annual with bootstrap CI (n=2000 race-level):

| Metric     | Delta pp | LB95   | UB95   |
| ---------- | -------- | ------ | ------ |
| top1       | +0.024   | -0.036 | +0.079 |
| place2     | -0.022   | -0.085 | +0.040 |
| place3     | +0.004   | -0.057 | +0.063 |
| fukusho_2p | +0.040   | -0.021 | +0.099 |
| top3_box   | -0.016   | -0.060 | +0.028 |

Gate: fukusho_2p LB95 = -0.021 → **FAIL**. All CIs straddle zero — indistinguishable from noise.

The under-pick is real at base-rate (female win 6.99% vs pick 6.36%) but the model's **comparative ranking is correct** — mare picks have HIGHER precision (42.48%) than colt picks (41.54%). The conservatism is correct calibration, not a fixable bias.

**Conclusion**: All 5 methods (ML raw / coefficients / correction / calibration / RL) exhausted for season×sex×weight. No actionable lever remains in this focus area.

## Artifacts

- tmp/ablation_jra_season_sex_weight.py — ablation script
- tmp/ablation_jra_season_sex_weight_result.json — full results JSON
- tmp/ablation_jra_ssw.{stdout,stderr}.log — training logs
- tmp/feat-jra-v8-season-sex-weight/ — feature store with 3 new columns
