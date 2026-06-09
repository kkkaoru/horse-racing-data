---
iteration: 33
date: 2026-06-10T05:30:00+09:00
based_on_iteration: 32
follows: history/oi-2026-06-10-per-class-round.md
lever: L-venue-targeted-Oi-rounds-R2..R4 + L-pgvector-jra-judge (parallel track)
status: REJECT all — R2 reweight / R3 standalone specialist / R4 blended specialist / pgvector-JRA-member all fail the strengthened gate
quality_gate: n/a — no code change adopted (R2-R4 + pgvector are evaluation-only; the two delivered wins are P0 routing already committed at iter32 + an eval-pipeline numpy-rescore speedup)
model_version_jra: iter14-jra-cb-pacestyle-course-v8 + per-class ensembles (UNCHANGED — no JRA card on 2026-06-10; pgvector member not adopted)
model_version_nar: per-class production config (UNCHANGED — iter30 C/other ensembles + iter12 baseline fallback for B)
scope:
  venue: 大井 Ōi (keibajo_code=44)
  target_card: 2026-06-10
  routing: C×5 / other×4 / B×3 races
  goal: raise tomorrow's Ōi NAR finish-position accuracy without regressing the global NAR holdout
baselines:
  source: tmp/nar-perclass/oi_round_analysis.json (production per-class config, Ōi holdout 2023-2026)
  validation_years: [2018, 2019, 2020, 2021, 2022]
  holdout_years: [2023, 2024, 2025, 2026]
  holdout_oi_per_class: # top1 / place2 / place3 / top3_box (%)
    C: { top1: 47.35, place2: 25.62, place3: 18.36, top3_box: 17.74, n_races: 1928 }
    other: { top1: 52.48, place2: 29.90, place3: 18.38, top3_box: 22.67, n_races: 1050 }
    B: { top1: 51.41, place2: 29.00, place3: 21.09, top3_box: 22.22, n_races: 531 }
strengthened_gate:
  split: nested — inner 2018-20 (fit blend weights) / tuning 2021-22 (pick rung) / holdout 2023-26 (touched once)
  multi_metric: ">=2 of {top1,place2,place3,top3_box} positive AND >=1 of {place2,place3} positive AND no axis < -0.05pp"
  significance: top1 paired-bootstrap (x10000, seed=42, race-resample) LB95 > 0
  family_wise: Holm across classes (alpha=0.05)
levers_tried:
  - id: R2-reweight
    source: tmp/nar-perclass/oi_round_analysis.json
    desc: Optuna TPE 300-trial simplex over (iter12 anchor >=0.20, iter30 residual, iter31 mid) maximising Ōi-validation top1 under HARD global non-regression (global top1 delta >= -0.05pp), + a multimetric grid candidate
    result: REJECT — every candidate fails; binding=oi_top1_not_improved (C Optuna Ōi top1 delta=-0.1037pp + global non-regression breached; other Optuna top1 -0.381pp; B candidates net 0 or place2 -1.32pp). No orthogonal signal in the existing member pool.
  - id: R3-standalone-specialist
    source: tmp/nar-perclass/oi_specialist_results.json
    desc: per-class Ōi-specialist CatBoost YetiRank (group_weight = time_decay x3.0 for keibajo=44), single train<=2022 / holdout 2023-26 split, + per-year walk-forward + pooled race-level paired bootstrap x10000 for the two eligible classes (other, B)
    result: REJECT_as_noise — top1 directionally positive (3/4 WF years up for both eligible classes) BUT no place axis clears LB95>0; other place2 LB95=-1.619 / place3 LB95=-0.6667; B place2 LB95=-1.8832 / place3 LB95=-3.0132. C only ever reaches 1 positive axis. The top1 lift is inside the bootstrap noise band.
  - id: venue-features
    source: tmp/nar-perclass/oi_feature_results.json
    desc: 7 Ōi-specific features bolted onto the per-class CatBoost (oi_umaban_top3_rate, oi_waku_top3_rate, oi_umaban_avg_finish_norm, oi_umaban_avg_corner4_relpos, horse_oi_top3_rate, jockey_oi_win_rate, trainer_oi_win_rate; 173->180 cols)
    result: REJECT — new-feature importance share ~0% (C 0.069% / other 0.155% / B negative -0.735%); every axis regresses vs round-0 (C top1 -1.97pp, other -2.10pp, B -3.39pp). The draw/pace/Ōi-form signal is already subsumed by existing features (target_corner_4_norm dominates at ~11.5%).
  - id: R4-blended-specialist
    source: tmp/nar-perclass/oi_r4_blend/{C,other,B,summary}.json
    desc: blend the R3 specialist (group_weight x3) into the production ensemble via rank-normalized simplex_softmax (iter12 anchor floor 0.20, inner-fit objective 0.5*top1+0.25*place2+0.25*place3, weight-ladder caps {0.30,0.15,0.05}), pick rung by tuning, evaluate once on holdout under the strengthened gate
    result: REJECT (all 3) — the unconstrained optimizer always picks heavy specialist weight (C spec 0.597 / other 0.800 / B 0.800), which reproduces the standalone place damage. C delta top1 +0.31 / place2 -0.31 / place3 -0.41 (only 1 axis positive); other +0.48 / -0.38 / +0.38 / +0.19 (place2 -0.381 breaches the -0.05pp floor); B +0.94 / -0.19 / -1.13 / +0.75 (no place axis up). All three: top1 boot LB95<0 (C -0.2593, other -0.2857, B -0.3766).
  - id: pgvector-jra-judge
    source: tmp/v8/vector/r3-judge/{005,010,016,703,other,summary}.json
    track: PARALLEL (JRA per-class — there is NO JRA card on 2026-06-10; this is the global-powered new-signal probe, judged for general adoption not for tomorrow's Ōi card)
    desc: add a pgvector kNN member (iter32-jra-vec-knn-{class}-v8) into each JRA per-class ensemble; nested-split blend + multi-metric gate + top1 paired-bootstrap LB95>0 + Holm across the 5 classes
    result: REJECT (all 5) — adopted_classes=[]. 005 is the near-miss: all 4 axes positive (top1 +0.064 / place2 +0.191 / place3 +0.191 / top3_box +0.032) and no axis regresses, but top1 boot LB95=-0.000953 (p=0.2727) so it fails the significance arm and is correctly NOT accepted. 703 top1 -0.166 (boot LB95 -0.006857); other top1 -0.094 (LB95 -0.010338); 010 top1 -0.758 (LB95 -0.015793); 016 top1 -2.338 (LB95 -0.042641). Holm: every class non-significant (adjusted p=1.0).
key_insight: |
  The Ōi-only gate is statistically underpowered. A saturated ensemble over the existing signal can only move the Ōi axes by sub-1pp, and the 531/1050/1928-race Ōi holdout cannot confirm a sub-1pp top1 gain under paired bootstrap — the LB95 is below 0 in every case (best top1 LB95 across all Ōi levers is -0.2593pp). So Ōi-specific incremental levers are effectively UNPASSABLE: the effect size the levers can produce is smaller than the noise the Ōi holdout can resolve. The path forward is (a) global-NAR-powered judging of a GENUINELY-NEW signal (a NAR similarity / pgvector member, mirroring the JRA probe, evaluated on the full-NAR holdout where n is ~10-25x larger and the same effect clears LB95), or (b) new data / features (multi-day window). Neither is a same-night lever.
methodology_note: |
  This round strengthened the accept gate vs iter32: nested split (inner 2018-20 fits blend weights / tuning 2021-22 picks the ladder rung / holdout 2023-26 touched exactly once) + multi-metric (>=2 of 4 axes positive, >=1 place positive, no axis < -0.05pp) + top1 paired-bootstrap LB95>0 + Holm across classes. The gate correctly PREVENTED a false-accept: pgvector 005 had all 4 axes positive and no axis regressing, which the old "axes-only" gate would have accepted, but the bootstrap significance arm (top1 LB95=-0.000953) and Holm both reject it. That is the gate working as designed — directional 4/4 positivity at n=3147 is not the same as a real effect.
decision: |
  No model change adopted for the Ōi card (or globally). Tomorrow's Ōi prediction stands on TWO genuine wins delivered tonight, both independent of the rejected levers:
  1. P0 per-class routing fix — already COMMITTED (iter32; commits 869c223 / b62169d / 6b21e03 / decbfc1) and verified live on the HEAD-rebuilt finish-position-predict-local:split2 image. Routing on 2026-06-10 confirmed: C->iter30-nar-cb-ensemble-C-v8, other->iter30-nar-cb-ensemble-other-v8, B->iter12-nar-xgb-hpo-v8 (correct baseline fallback, no registered B ensemble).
  2. Eval-pipeline ~665-878x speedup — the holdout rescore was moved to a vectorized numpy path, which is what made the R2-R4 sweep (300+400 Optuna trials x 4-class WF + x10000 bootstrap) and the 5-class pgvector Holm sweep tractable in a single night.
artifacts:
  r2_reweight: tmp/nar-perclass/oi_round_analysis.json
  r3_specialist: tmp/nar-perclass/oi_specialist_results.json # see .robustness for WF + pooled bootstrap
  venue_features: tmp/nar-perclass/oi_feature_results.json
  r4_blend: tmp/nar-perclass/oi_r4_blend/{C,other,B,summary}.json
  pgvector_judge: tmp/v8/vector/r3-judge/{005,010,016,703,other,summary}.json
  prediction_log: ~/Library/Logs/finish-position-predict/20260610.log
---

## What was tried

This is a follow-on to [`oi-2026-06-10-per-class-round.md`](oi-2026-06-10-per-class-round.md) (iter 32), which logged the R0 baseline plus the first batch of Ōi levers (reweight, venue features, the standalone specialist, and the training-logic audit). Tonight's round pushed three further Ōi-specific rounds (R2 reweight detail, R3 standalone-specialist robustness, R4 blended specialist) under a **strengthened** accept gate, and ran a **parallel** pgvector-JRA-member probe as the genuinely-new-signal track. The single question across all of it: does any new model robustly improve finish-position accuracy on the 2026-06-10 Ōi card (C×5 / other×4 / B×3) without regressing the holdout, under a gate strong enough to reject sampling noise?

The Ōi-slice baselines (production per-class config, top1/place2/place3/top3_box %, holdout 2023–2026):

| class | top1  | place2 | place3 | top3_box | n_races |
| ----- | ----- | ------ | ------ | -------- | ------- |
| C     | 47.35 | 25.62  | 18.36  | 17.74    | 1928    |
| other | 52.48 | 29.90  | 18.38  | 22.67    | 1050    |
| B     | 51.41 | 29.00  | 21.09  | 22.22    | 531     |

## Implementation summary

No production code changed this round. The R2–R4 and pgvector drivers live under `tmp/` (git-excluded) and are intentionally NOT committed. The two load-bearing wins are external to this evaluation: the P0 routing fix (already committed at iter32) and an eval-pipeline numpy-rescore speedup (~665–878×) that made the full sweep tractable in one night. The only artifact committed for this round is this docs record.

## Results

### R2 — member reweight (Optuna 300-trial + multimetric grid)

Re-optimising per-class blend weights over the existing member pool (iter12 / iter30-residual / iter31-mid) fails the Ōi gate for every class. There is no orthogonal signal left to extract by reweighting.

| class | best candidate            | Ōi top1 Δ | place2 Δ | place3 Δ | box Δ   | verdict | binding                                          |
| ----- | ------------------------- | --------- | -------- | -------- | ------- | ------- | ------------------------------------------------ |
| C     | Optuna (0.20/0.003/0.797) | -0.1037   | +0.4668  | -0.4149  | -0.2075 | FAIL    | oi_top1_not_improved (+ global non-reg breached) |
| other | Optuna (0.20/0.276/0.524) | -0.3810   | +0.0952  | +0.0952  | +0.4762 | FAIL    | oi_top1_not_improved                             |
| B     | grid (0.4/0.1/0.5)        | 0.0000    | -1.3183  | -0.3766  | +0.5650 | FAIL    | oi_top1_not_improved                             |

### R3 — standalone Ōi-specialist (group_weight ×3) + per-year WF + pooled bootstrap

The `weighted_groupweight` specialist applies the ×3 Ōi tilt correctly via CatBoost `group_weight`. On the single train≤2022 split it looks promising (other PASSes the naive 4-axis gate, B PASSes 3-axis), but the per-year walk-forward + pooled race-level paired bootstrap kills it: **no place axis clears LB95>0** for either eligible class.

| class | top1 3/4-yr up | pooled top1 LB95 | place2 LB95 | place3 LB95 | worst-yr top1 Δ | verdict         |
| ----- | -------------- | ---------------- | ----------- | ----------- | --------------- | --------------- |
| other | yes (3/4)      | -0.2857          | -1.6190     | -0.6667     | -0.3185         | REJECT_as_noise |
| B     | yes (3/4)      | -0.3766          | -1.8832     | -3.0132     | 0.0000          | REJECT_as_noise |

C never reaches ≥2 positive axes in any specialist variant. The directional top1 lift is indistinguishable from sampling noise.

### Venue features (7 Ōi-specific signals, 173→180 cols)

Importance share of the 7 new features ≈ 0% (C 0.069% / other 0.155% / B −0.735%); every axis regresses vs round-0 (C top1 −1.97pp / other −2.10pp / B −3.39pp). The draw/pace/Ōi-form information is already subsumed (`target_corner_4_norm` alone is ~11.5% importance). **REJECT.**

### R4 — blended specialist into production ensemble (strengthened nested-split gate)

Blending the R3 specialist into the production ensemble does not rescue it. With the iter12 anchor floor at 0.20 and the rest free, the optimizer always loads heavy specialist weight (C 0.597 / other 0.800 / B 0.800), which simply re-imports the standalone place damage. All three REJECT.

| class | spec wt | top1 Δ  | place2 Δ | place3 Δ | box Δ   | n_pos | top1 boot LB95 | binding                      |
| ----- | ------- | ------- | -------- | -------- | ------- | ----- | -------------- | ---------------------------- |
| C     | 0.597   | +0.3112 | -0.3112  | -0.4149  | 0.0000  | 1     | -0.2593        | fewer_than_2_axes_positive   |
| other | 0.800   | +0.4762 | -0.3810  | +0.3810  | +0.1905 | 3     | -0.2857        | place2 -0.381 < -0.05 floor  |
| B     | 0.800   | +0.9416 | -0.1883  | -1.1299  | +0.7533 | 2     | -0.3766        | no place2/place3 improvement |

Every class fails the top1 paired-bootstrap arm regardless (all three LB95 < 0). `summary.json` → `adopt_classes: []`.

### pgvector-JRA-member judge (parallel track — no JRA card on 06-10)

Adding a pgvector kNN member (`iter32-jra-vec-knn-{class}-v8`) into each JRA per-class ensemble, under the same nested-split + multi-metric + top1-bootstrap + Holm gate. All 5 classes REJECT; `adopted_classes: []`.

| class | top1 Δ  | place2 Δ | place3 Δ | box Δ   | n_pos | top1 boot LB95 | boot p | verdict                |
| ----- | ------- | -------- | -------- | ------- | ----- | -------------- | ------ | ---------------------- |
| 005   | +0.0636 | +0.1907  | +0.1907  | +0.0318 | 4     | **-0.000953**  | 0.2727 | REJECT (top1 LB95 ≤ 0) |
| 703   | -0.1655 | +0.0473  | +0.2601  | -0.0236 | 2     | -0.006857      | 0.7614 | REJECT                 |
| other | -0.0940 | -0.8459  | -0.3759  | +0.0940 | 1     | -0.010338      | 0.6203 | REJECT                 |
| 010   | -0.7581 | -0.0632  | +0.3790  | +0.2527 | 2     | -0.015793      | 0.9761 | REJECT                 |
| 016   | -2.3384 | -0.8253  | -0.8253  | -1.1004 | 0     | -0.042641      | 0.9920 | REJECT                 |

Class **005 is the instructive near-miss**: all four axes positive and no axis regressing — exactly the pattern that the old axes-only gate would have _accepted_ — yet the top1 paired-bootstrap LB95 is −0.000953 (p=0.2727), and Holm leaves it non-significant (adjusted p=1.0). The strengthened gate correctly rejects it.

## Per-bucket findings

Across R2–R4, the best top1 paired-bootstrap LB95 achieved on _any_ Ōi class/lever is −0.2593pp (R4 C). The point estimates wander to ±1pp, but the Ōi holdout (n = 531 / 1050 / 1928) cannot resolve a sub-1pp effect: every LB95 sits below zero. The place axes are the consistent binding failure — the specialist trades a fraction of a top1 point for place2/place3 damage that the bootstrap then flags.

## Decision

**No model change adopted — for the Ōi card or globally.** R2 (reweight), R3 (standalone specialist), the venue features, R4 (blended specialist), and the pgvector-JRA member all REJECT under the strengthened gate.

Tomorrow's Ōi prediction stands on two genuine wins delivered tonight:

1. **P0 per-class routing fix** — already committed at iter32 (`869c223` / `b62169d` / `6b21e03` / `decbfc1`) and verified live on the HEAD-rebuilt `finish-position-predict-local:split2` image. The 2026-06-10 Ōi routing is confirmed: C (R1/R2/R6/R8/R9) → `iter30-nar-cb-ensemble-C-v8`, other (R3/R4/R5/R11) → `iter30-nar-cb-ensemble-other-v8`, B (R7/R10/R12) → `iter12-nar-xgb-hpo-v8` (correct baseline fallback).
2. **Eval-pipeline ~665–878× speedup** — moving the holdout rescore to a vectorized numpy path is what made tonight's sweep (R2 300-trial + R4 400-trial Optuna × 4-class WF × ×10000 bootstrap, plus the 5-class pgvector Holm sweep) finish in a single night.

## Next iteration recommendation

The Ōi-only gate is **statistically underpowered**: a saturated ensemble over the existing signal can only produce sub-1pp Ōi deltas, and the 531–1928-race Ōi holdout cannot confirm those under paired bootstrap (best top1 LB95 = −0.2593pp). Ōi-specific incremental levers are therefore effectively unpassable — the producible effect is smaller than the resolvable noise. The path forward is:

1. **Global-NAR similarity / pgvector member**, judged on the **full-NAR holdout** (n ~10–25× the Ōi slice), mirroring the JRA pgvector probe. The larger n is what lets a genuinely-new signal clear LB95 where the same effect dies on the Ōi slice. (NAR similarity member is in progress.)
2. Failing that, a **genuinely-new horse-level signal or more data** (e.g. a multi-day window) trained via full retrain or HPO/L4 — not a same-night blend or feature bolt-on.

The 005 pgvector near-miss is the template: directional 4/4 positivity at n=3147 is not yet a real effect; the only honest way to adopt a new member is to make its effect big enough (new signal) or its holdout big enough (full-NAR / multi-day) that the bootstrap LB95 clears zero.

**Follow-on:** [`oi-2026-06-10-r5-nar-similarity-member.md`](oi-2026-06-10-r5-nar-similarity-member.md) (iter 34) built exactly recommendation (1) — a NAR kNN similarity ("pgvector-style") member judged on the FULL powered NAR holdout. The probe PROCEEDED (the signal is genuinely orthogonal: within-race Spearman vs production ~0.82–0.88, real winner signal, null collapses), but the judge REJECTED all 7 NAR classes (`adopted_classes=[]`): the optimizer assigned the vector member ≈0 weight everywhere, proving the orthogonal variance is target-noise. That closes the 7-lever saturation conclusion — the bottleneck was never holdout power, it is a saturated feature set.

## Quality Gate Results

- tsc: n/a — no code change adopted this round
- lint: n/a — no code change adopted this round
- format:check: n/a — no code change adopted this round
- test:coverage: n/a — no enforced-package file modified
- python:check: n/a — R2-R4 + pgvector drivers live under tmp/ (not an enforced package)
