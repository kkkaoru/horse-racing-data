# 4着以上 boundary-structure place2/3 probe (JRA)

**Date**: 2026-06-18  
**Data**: JRA feat-jra-v8-iter26-relationships, 2006-2026, 70,417 races, 990,719 horse-race rows  
**Holdout**: 2023-2026 (11,703 races per position slot)  
**Verdict**: ABORT

---

## 1. Did prior vectors capture 4着以上 correlation?

**No, not directly.**

Prior feature vectors used per-horse input features (speed, pace, jockey stats, etc.) plus a scalar odds-implied strength value. Within-race relevance weighting collapsed 4着以上 positions to weight 0 in the {3,2,1,0} scheme — so the _joint distribution_ of how the also-ran pack relates to the in-money boundary was never encoded.

The existing field-level features (`field_avg_speed_index`, `field_spread_past_corner_1_norm`, `field_style_diversity`, `field_nige_pressure`, `field_dominant_favorite_indicator`) capture field competitiveness along **pace/corner/style dimensions**. None of them encode the **odds-implied rank-3 ↔ rank-4 strength cliff** or the density of horses crowding the 3着 cutoff.

This probe tests ten new boundary signals derived purely from within-race odds-implied probabilities (serve-safe, pre-race).

---

## 2. Per-feature orthogonal partial-ρ table

Controls: `inverse_odds_rank_in_race` + `inverse_odds_implied_prob` (removes per-horse base odds signal).  
Bar: |ρ| ≥ 0.08 (necessary, not sufficient for adoption).

**Strength proxy**: `inverse_odds_implied_prob` sorted desc = odds-implied rank within race.  
**Targets**: `hit_place2` = predicted-rank-2 horse finished exactly 2nd; `hit_place3` = predicted-rank-3 horse finished exactly 3rd.

### Full dataset (2006–2026, n = 70,417 per slot)

| Feature             | hit_place2 ρ | p      | hit_place3 ρ | p      | Clears bar |
| ------------------- | ------------ | ------ | ------------ | ------ | ---------- |
| `inmoney_cliff`     | −0.0187      | <0.001 | −0.0234      | <0.001 | No         |
| `n_near_3rd_cutoff` | −0.0062      | 0.101  | −0.0726      | <0.001 | No (0.073) |
| `tail_spread`       | −0.0255      | <0.001 | −0.0442      | <0.001 | No         |
| `top3_gap_12`       | +0.0701      | <0.001 | +0.0525      | <0.001 | No         |
| `top3_gap_23`       | −0.0174      | <0.001 | +0.0380      | <0.001 | No         |
| `field_entropy`     | −0.0426      | <0.001 | −0.0514      | <0.001 | No         |
| `gap_pct10`         | +0.0030      | 0.425  | +0.0041      | 0.276  | No         |
| `gap_pct25`         | −0.0032      | 0.394  | −0.0005      | 0.887  | No         |
| `gap_pct75`         | −0.0101      | 0.007  | −0.0031      | 0.408  | No         |
| `gap_pct90`         | −0.0170      | <0.001 | +0.0301      | <0.001 | No         |

### Holdout 2023–2026 (n = 11,703 per slot)

| Feature             | hit_place2 ρ | p      | hit_place3 ρ | p          | **Clears bar**     |
| ------------------- | ------------ | ------ | ------------ | ---------- | ------------------ |
| `inmoney_cliff`     | −0.0228      | 0.014  | −0.0224      | 0.015      | No                 |
| `n_near_3rd_cutoff` | −0.0192      | 0.038  | **−0.0832**  | **<0.001** | **hit_place3 YES** |
| `tail_spread`       | −0.0310      | 0.001  | −0.0343      | 0.0002     | No                 |
| `top3_gap_12`       | +0.0750      | <0.001 | +0.0512      | <0.001     | No                 |
| `top3_gap_23`       | −0.0043      | 0.643  | +0.0386      | <0.001     | No                 |
| `field_entropy`     | −0.0422      | <0.001 | −0.0476      | <0.001     | No                 |
| `gap_pct10`         | +0.0046      | 0.619  | +0.0029      | 0.752      | No                 |
| `gap_pct25`         | −0.0019      | 0.841  | −0.0008      | 0.931      | No                 |
| `gap_pct75`         | −0.0114      | 0.217  | −0.0003      | 0.970      | No                 |
| `gap_pct90`         | −0.0151      | 0.103  | +0.0291      | 0.002      | No                 |

**Summary of bar clearances**:

- `n_near_3rd_cutoff` → `hit_place3`, holdout only: ρ = −0.083 (clears 0.08)
- All other features × targets: below bar (|ρ| ≤ 0.075)
- `n_near_3rd_cutoff` → `hit_place2`, both splits: |ρ| ≤ 0.019 (no signal)

---

## 3. Calibration: inmoney_cliff quartile → place2/3 hit-rate

Races bucketed by `inmoney_cliff` (strength gap between rank-3 and rank-4 horse).  
Q1 = narrowest cliff (crowded boundary), Q4 = widest cliff (clear separation).

### hit_place2 (predicted-rank-2 finished exactly 2nd)

| Bucket       | cliff median | n races | hit_rate |
| ------------ | ------------ | ------- | -------- |
| Q1 (crowded) | 0.0077       | 17,619  | 0.1749   |
| Q2           | 0.0259       | 17,632  | 0.1726   |
| Q3           | 0.0500       | 17,562  | 0.1859   |
| Q4 (wide)    | 0.0937       | 17,604  | 0.1952   |

Q1→Q4 spread: +0.0203 (+11.6%)

### hit_place3 (predicted-rank-3 finished exactly 3rd)

| Bucket       | cliff median | n races | hit_rate |
| ------------ | ------------ | ------- | -------- |
| Q1 (crowded) | 0.0077       | 17,619  | 0.1318   |
| Q2           | 0.0259       | 17,632  | 0.1294   |
| Q3           | 0.0500       | 17,562  | 0.1377   |
| Q4 (wide)    | 0.0937       | 17,604  | 0.1512   |

Q1→Q4 spread: +0.0194 (+14.7%)

**Interpretation**: Wide cliff races do have higher place2/3 hit-rates (~11–15% improvement Q1→Q4). However, after controlling for per-horse odds rank + strength, most boundary features fail to carry orthogonal predictive signal — meaning the GBDT already routes this information through the per-horse odds features.

---

## 4. Verdict: ABORT

### Reasons

**1. No feature clears ρ ≥ 0.08 for hit_place2.**  
`top3_gap_12` reaches ρ = 0.075 (holdout) but misses the bar. This is the best candidate and it falls short.

**2. Only one feature × target clears the bar: `n_near_3rd_cutoff` → `hit_place3` (ρ = −0.083).**  
But this is a single holdout pass — it fails in the full sample (ρ = −0.073). The bar is necessary-not-sufficient; this marginal, split-inconsistent signal does not warrant a cheap-filter retrain. Full → holdout inconsistency suggests noise rather than genuine new information.

**3. Calibration confirms the hypothesis directionally but the effect size is small.**  
Wide-cliff races are ~11–15% more predictable in exact place2/3. However, this lift is already absorbed by the per-horse odds rank features in the GBDT — the partial-ρ controls for those and almost eliminates the signal.

**4. Conceptual explanation of why GBDT already captures this.**  
The GBDT has `inverse_odds_implied_prob`, `inverse_odds_rank_in_race`, `tansho_ninkijun_raw`, `odds_score_diff_from_race_avg`, and `popularity_score_diff_from_race_avg` as inputs. The within-race rank structure (including boundary shape) is implicitly encoded in these per-horse features — the boundary signals I built are functions of the same odds probabilities already in the model. GBDT's nonlinear cross-horse interactions during training likely capture whatever signal the boundary shape holds.

**5. History consistency with prior D-phase result.**  
The D-phase frontier analysis confirmed that "informative NULL routing" and existing feature interactions already represent the practical ceiling. Boundary features built on the same odds inputs are unlikely to break through this ceiling.

### What would reverse the verdict

A cheap-filter retrain adding `n_near_3rd_cutoff` + `top3_gap_12` to the JRA GBDT could still be run if the team decides the marginal 0.08 holdout bar for hit_place3 warrants the cost. Prior playbook says "ρ necessary not sufficient" — proceed only if incremental model verification (walk-forward) confirms improvement. Given the weak and split-inconsistent signal, the expected outcome is no-regression at best.

The hypothesis that "crowded boundary = harder place2/3" is **empirically confirmed in raw calibration** (11–15% hit-rate difference Q1→Q4), but the GBDT already exploits this through per-horse odds features. The boundary structure adds nothing orthogonal.

---

## Appendix: Feature definitions

All features are computed within-race using pre-race `inverse_odds_implied_prob` only (serve-safe):

- `inmoney_cliff`: strength gap between odds-rank-3 and odds-rank-4 horse
- `n_near_3rd_cutoff`: # horses within ε = 5% of field range of the rank-3 horse's strength
- `tail_spread`: std of rank-4+ horses' odds-implied strengths
- `top3_gap_12`: strength gap odds-rank-1 vs odds-rank-2
- `top3_gap_23`: strength gap odds-rank-2 vs odds-rank-3
- `field_entropy`: entropy of binned within-race strength distribution (10 bins)
- `gap_pct{10,25,75,90}`: percentiles of consecutive strength gaps (all horses, ascending)
