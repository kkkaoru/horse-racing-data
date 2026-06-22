---
class: C
category: nar
n_races_holdout_2023_26: 26060
baseline_top1: 58.945
baseline_place2: 35.288
baseline_place3: 27.268
baseline_top3_box: 34.793
active_ensemble: iter36-nar-lgb-ensemble-C-v8
iter36_top1: 59.286
iter36_place2: 35.322
iter36_place3: 27.057
iter36_top3_box: 34.601
---

# NAR C — class file

## Status

Active ensemble: `iter36-nar-lgb-ensemble-C-v8`
([iter12-nar-xgb-hpo-v8 @ 0.4976, iter36-nar-lgb-lambdarank-residual-C-v8 @ 0.5024]).

Adopted 2026-06-10 (user win-priority override; top1 +0.342pp robust LB95>0, place3 -0.211pp
accepted as tradeoff).

## Precise Baselines (holdout 2023-2026, 26 060 races)

| Metric   | iter30 prod (baseline) | iter36 (active) | iter36 delta |
| -------- | ---------------------: | --------------: | -----------: |
| top1 %   |                 58.945 |          59.286 |       +0.342 |
| place2 % |                 35.288 |          35.322 |       +0.035 |
| place3 % |                 27.268 |          27.057 |       -0.211 |
| top3_box |                 34.793 |          34.601 |       -0.192 |

iter36 top1 bootstrap LB95 = +0.0012 (p=0.0006).

## Candidate 5: Place-Preserving Ranking Objective (ROADMAP §4 Candidate 5)

**Goal**: find a ranking objective that JOINTLY optimizes top1 AND place2/place3, eliminating
the place3 −0.211pp regression visible in the deployed iter36. Two approaches tested.

### Wave 3 — LambdaRank objective/label knob sweep (8 variants)

**Design**: backbone hyperparams held fixed at H4 winner (`hpo_C.json`); only the objective
knobs varied. Per-fold OOS: train ≤ Y-1, predict Y for Y in 2018..2026, seed=42+Y,
num_threads=8, deterministic. Judge: nested inner 2018-20 / tuning 2021-22 / holdout 2023-26
(gated ONCE). Gate: ≥2/4 axes>0 AND ≥1 place>0 AND no axis<-0.05pp AND top1 bootstrap
LB95(×10 000)>0 AND Holm{C,B}.

Control v0 reproduced H4/iter36 exactly (top1 +0.3415, place2 +0.0345, place3 -0.2111,
box -0.1919, LB95 +0.00119) confirming harness correctness.

| Variant                   | top1 Δpp | place2 Δpp | place3 Δpp | top3_box Δpp |     LB95 | Verdict |
| ------------------------- | -------: | ---------: | ---------: | -----------: | -------: | ------- |
| v0_baseline_ndcg3_trunc3  |  +0.3415 |    +0.0345 |    -0.2111 |      -0.1919 | +0.00119 | REJECT  |
| v1_ndcg123_trunc3         |  +0.2533 |    -0.1458 |    -0.3377 |      -0.3147 | +0.00038 | REJECT  |
| v2a_ndcg3_trunc5          |  -0.0192 |    -0.0077 |    -0.0269 |      +0.0192 | -0.00046 | REJECT  |
| v2b_ndcg3_trunc10         |  -0.0192 |    -0.0077 |    -0.0269 |      +0.0192 | -0.00046 | REJECT  |
| v2c_ndcg123_trunc5        |  -0.0192 |    -0.0077 |    -0.0269 |      +0.0192 | -0.00046 | REJECT  |
| v3a_linear_labelgain      |  -0.0192 |    -0.0077 |    -0.0269 |      +0.0192 | -0.00046 | REJECT  |
| v3b_rel432_ndcg123_trunc5 |  +0.2111 |    +0.0345 |    -0.2379 |      -0.1458 | +0.00019 | REJECT  |
| v3c_rel432_linear_trunc5  |  -0.0192 |    -0.0077 |    -0.0269 |      +0.0192 | -0.00046 | REJECT  |

Deltas above are vs iter30 production baseline. v3b is the runner-up (top1 +0.211pp positive,
LB95>0), but place3 -0.238pp breaches the -0.05pp floor AND is strictly dominated by iter36
on top1 (-0.130pp vs iter36). No variant passes the full gate.

**W3 root cause**: the top1 gain and the place3/box regression are COUPLED — both arise from
the NDCG@3 with truncation=3 objective. Every place-protective lever (deeper truncation,
broader ndcg positions, flattened/widened label_gain) dilutes the top1 signal; the blend
optimizer collapses the residual weight to the cap floor (~0.02-0.31), yielding ~0 deltas on
all axes. Configs that preserve positive top1 (v0, v1, v3b) still regress place3 beyond -0.05pp.

Reference: `tmp/nar-perclass/w3_place_lambdarank/w3_final_verdict.json`

### Wave 4 — Place-specialist as 3rd ensemble member (6 specialist variants, classes C+B)

**Design**: 3-member ensemble [iter12 + iter36-lgb-top1 + place-specialist]. Six specialist
families tested (lgb_lambdarank rel{1,3,3}, lgb_lambdarank rel{2,3,3}, lgb_binary top3,
cb_binary top3, lgb avg_top2top3, cb_place_reg). All trained per-fold OOS on 174 features;
blend optimized on inner 2018-20 / tuning 2021-22; holdout 2023-26 gated ONCE.

| Specialist       | top1 Δpp | place2 Δpp | place3 Δpp | top3_box Δpp |     LB95 | ρ vs lgb-top1 | Verdict |
| ---------------- | -------: | ---------: | ---------: | -----------: | -------: | ------------: | ------- |
| lgb_rel133       |  +0.3185 |     0.0000 |    -0.2379 |      -0.1957 | +0.00092 |         0.934 | REJECT  |
| lgb_rel233       |  +0.3338 |    +0.0077 |    -0.2379 |      -0.1765 | +0.00107 |         0.960 | REJECT  |
| lgb_binary_top3  |  +0.3569 |    +0.0192 |    -0.2072 |      -0.1535 | +0.00127 |         0.979 | REJECT  |
| cb_binary_top3   |  +0.3530 |    +0.0269 |    -0.2302 |      -0.1458 | +0.00127 |         0.979 | REJECT  |
| lgb_avg_top2top3 |  +0.3300 |     0.0000 |    -0.2840 |      -0.1612 | +0.00107 |         0.982 | REJECT  |
| cb_placereg      |  +0.3492 |    +0.0844 |    -0.1650 |      -0.1305 | +0.00127 |         0.967 | REJECT  |

Best C specialist (cb_placereg): place3 -0.165pp vs production — still below -0.05pp floor.
vs iter36: top1 +0.008, place2 +0.050, place3 +0.046, box +0.061 — marginal recovery only.
Holm: C top1-p = 0.0009 (significant), B not significant.

**W4 root cause**: every place-specialist trained on the same 174 features is highly
rank-correlated (Spearman 0.93-0.98) with the lgb-top1 member. The top1/place3 coupling is
a property of the FEATURE SET, not the ranking objective. A 3rd member from the same features
carries the same coupling; the simplex optimizer cannot recover place3 without surrendering
top1. The best cb_placereg member claws back only +0.046pp place3 vs iter36 in absolute terms;
place3 remains -0.165pp vs the iter30 production floor.

Reference: `tmp/nar-perclass/w4_place_specialist/w4_final_verdict.json`

### Summary verdict: ABORT — place-preserving ranking objective infeasible on current features

**ABORT**. Both Option A (place-aware relevance / objective tuning, W3) and Option B (multi-
objective via specialist 3rd member, W4) have been exhaustively tested. 14 variants across
two waves produced a consistent null result: place3/top3_box cannot be improved relative to
the iter36 win-priority blend without breaching the -0.05pp floor, using only the current
174-feature set.

The root cause is structural: the 174-feature NAR set lacks features orthogonal to the
existing win-prediction signal on the podium (2nd/3rd place) axes. Objective retuning and
3rd-member ensembling are both fundamentally constrained by feature-space coupling.

**Implication for further work**: place3 improvement on NAR C requires NEW SIGNAL — features
that differentiate 2nd-from-3rd place finishers independently of the win pick (e.g., per-horse
running-style interaction with field topology, positional GPS data, or a structural signal
not derivable from jvd/nvd feeds). This is consistent with the broader GOAL-v3 science-track
finding that exact-ordinal place2/place3 improvement is structurally ill-posed on the current
feature set.

## Evaluation Log

| Date       | Hypothesis                              | Method                        | Verdict | Ref                                                             |
| ---------- | --------------------------------------- | ----------------------------- | ------- | --------------------------------------------------------------- |
| 2026-06-10 | H4: alt-loss LGB LambdaRank residual    | LGB lambdarank ndcg@3 trunc=3 | ADOPT\* | history/oi-2026-06-10-iter36-lgb-lambdarank-residual-C-adopt.md |
| 2026-06-17 | W3: place-protective objective variants | 8 LambdaRank knob variants    | ABORT   | tmp/nar-perclass/w3_place_lambdarank/w3_final_verdict.json      |
| 2026-06-17 | W4: place-specialist 3rd member         | 6 specialist families         | ABORT   | tmp/nar-perclass/w4_place_specialist/w4_final_verdict.json      |
| 2026-06-17 | Candidate 5: place-preserving objective | W3+W4 combined (Option A+B)   | ABORT   | this file                                                       |

\*ADOPT = user win-priority override; automatic gate would REJECT due to place3 -0.211pp < -0.05pp.
