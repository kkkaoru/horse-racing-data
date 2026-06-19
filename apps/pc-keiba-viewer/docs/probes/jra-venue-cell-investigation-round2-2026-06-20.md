# JRA Venue×Cell 精度改善 Exhaustive 調査 Round 2

**Date**: 2026-06-20
**Category**: JRA finish-position venue-specific accuracy
**Verdict**: **ALL REJECT** — 29 SQL queries + 9 agents, 15 hypotheses all rejected

## Background

Production model: iter22-jra-etop2 (CatBoost iter20 + XGBoost E-top2 override, 244 features).
Prior Round 1 (2026-06-19): 7 hypotheses (H1-H7) all REJECTED. This Round 2 deepens with 29 SQL queries.

## Key Findings

### 1. Target 3 venues are BETTER than global for sprint turf (+1.8pp)

Sprint turf: target 3 (38.4%) > ALL JRA (36.6%). Weakness is universal, not venue-specific.

### 2. All 22 cells have rank-1 > rank-2 — swap universally harmful

Best case: Tokyo sprint turf Q1 (tiny gap): r1=27.4%, r2=27.4% (tied). swap_net ≤ 0 in ALL 36 cell×quartile combinations.

### 3. E-top2 proxy simulation (win5-xgb): 9/12 turf cells worse

| Cell             | Pure CB | With E-top2 | Delta               |
| ---------------- | ------- | ----------- | ------------------- |
| 函館 mile turf   | 39.52%  | 41.13%      | +1.61pp (N_fire=19) |
| 函館 sprint turf | 46.01%  | 46.01%      | 0.00                |
| 阪神 sprint turf | 36.12%  | 36.12%      | 0.00                |
| 東京 mile turf   | 40.52%  | 40.13%      | -0.39               |
| 阪神 mile turf   | 38.09%  | 37.55%      | -0.54               |
| 東京 sprint turf | 33.86%  | 32.92%      | -0.94               |
| 阪神 inter turf  | 39.16%  | 38.19%      | -0.97               |
| 東京 inter turf  | 43.31%  | 42.13%      | -1.18               |
| 阪神 long turf   | 47.62%  | 46.03%      | -1.59               |
| 東京 long turf   | 45.15%  | 43.20%      | -1.94               |
| 函館 inter turf  | 36.90%  | 34.52%      | -2.38               |
| 函館 long turf   | 55.56%  | 44.44%      | -11.11              |

### 4. Statistical power: ALL cells N < 2400 — 2pp improvement undetectable

### 5. Year-over-year: 18-38pp swing in weak cells — correction overfits noise

### 6. Model-odds agreement highest (64-75%) in weak cells — independent signal is weakest

### 7. 1400m turf is universally worst (not venue-specific): Tokyo 34.0%, Hanshin 35.8%

### 8. Model not aging: sprint turf FLAT 35-40% across 8 years (2018-2026)

### 9. Track condition irrelevant: Tokyo sprint turf = 33.9% even on firm ground

### 10. Running style precision universal: nige > senkou > sashi > oikomi at all venues

### 11. Draw: inner draw advantage visible at Hanshin sprint turf (43.0% vs 31.8%) but model captures via wakuban feature

### 12. All 13 alternative models: none significantly beats iter14 for sprint turf (best: transformer +1.1pp, N=258, p>0.3)

## Hypothesis Verdict Table

| ID  | Hypothesis                     | Method           | Verdict |
| --- | ------------------------------ | ---------------- | ------- |
| H1  | Cell-conditional rank-swap     | Correction       | REJECT  |
| H2  | Gap-conditional E-top2         | Correction+Stats | REJECT  |
| H3  | E-top2 cell-conditional gating | ML+Correction    | REJECT  |
| H4  | Venue-specific RS calibration  | Stats            | REJECT  |
| H5  | 1400m turf specialist          | ML               | REJECT  |
| H6  | Track condition correction     | Correction       | REJECT  |
| H7  | Class × venue routing          | ML               | REJECT  |
| H8  | Score gap threshold            | Stats+Math       | REJECT  |
| H9  | Odds-disagreement swap         | Correction       | REJECT  |
| H10 | Multi-model (win5-xgb)         | ML               | REJECT  |
| H11 | RL per-cell selection          | RL               | REJECT  |
| H12 | Entropy correction             | Math             | REJECT  |
| H13 | Draw conditional               | Correction       | REJECT  |
| H14 | Transformer ensemble           | ML               | REJECT  |
| H15 | Temporal error pattern         | Stats            | REJECT  |

## Root Cause Analysis

1. **Universal weakness**: target venues are better than global for sprint turf
2. **Market efficiency wall**: model follows odds (64-75% agreement), independent signal weakest in hard cells
3. **Structural entropy**: winner concentration inherently low in competitive turf sprints
4. **8-year ceiling**: accuracy flat, not degrading — this IS the performance floor

## Relationship to Prior Work

- Confirms project-venue-accuracy-investigation-2026-06-19 (7/7 REJECT)
- Confirms project-finish-position-frontier-2026-06-11
- Confirms project-science-track-saturation-2026-06-11
- Extends with E-top2 proxy simulation data

## Conclusion

Venue-specific post-scoring correction is not viable. All 15 hypotheses across ML, RL, statistics, math, and programmatic correction are REJECTED with quantitative evidence from 29 SQL queries. The accuracy floor is structural (market efficiency + outcome entropy), not a model deficiency.
