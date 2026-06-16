# Plan C: Top1 +5% Lever Campaign

**Date:** 2026-06-17
**Scope:** JRA (iter19, CatBoost YetiRank, 244 features, base-only) and NAR (iter12 XGBoost
rank:pairwise + iter30/36 per-class ensembles).
**Goal interpretation:** The user's request "+5% top1" is ambiguous; both interpretations are
stated below and the more aggressive is targeted.

---

## 1. Interpreting "+5%"

| Interpretation                                          | JRA baseline               | JRA target           | NAR baseline | NAR target          |
| ------------------------------------------------------- | -------------------------- | -------------------- | ------------ | ------------------- |
| **Relative +5%** (5% of current → JRA ×1.05, NAR ×1.05) | 0.450 (WF holdout 2023-25) | **0.4725** (+2.25pp) | 0.585        | **0.6143** (+2.9pp) |
| **Absolute +5pp**                                       | 0.450                      | **0.500** (+5pp)     | 0.585        | **0.635** (+5pp)    |

**Honest baseline clarification.** The WF holdout top1 numbers (JRA ~0.450, NAR ~0.585) are
post-serve-fix walk-forward numbers evaluated on historical final odds. Live serve accuracy
is lower (rootcause I4: JRA −8.65pp, NAR −5.68pp before the serve-skew fixes). The iter19
JRA deploy and the NAR 09:30-run / DO-TTL4h fixes have recovered much but not all of this
gap. For the purpose of this plan, all target comparisons use the WF holdout metric (the
standard gate), not live serve.

**Target:** relative +5% (JRA ≥ 0.4725 WF, NAR ≥ 0.6143 WF). This is the achievable
interpretation. Absolute +5pp (JRA ≥ 0.500, NAR ≥ 0.635) would require multiple
iter19-class wins stacking — probability assessed in section 8.

---

## 2. What Has Already Been Done — DO-NOT-RETEST Summary

The following are **confirmed closed** and must not be re-proposed. All entries are verified
against the DO-NOT-RETEST list in `imperative-riding-wave.md` and the docs corpus.

**Feature search (signal exhausted):** all JRA sci-track H1–H9 probes, all science-track
B1–B5 probes, all Wave 1 H1–H4 NAR per-class probes, graded relevance objectives
(B/C/D), exotic/fukusho odds, yoso_soha_time, workout/jvd_hc/jvd_wc, gate-draw,
jockey-trainer combo, sire-surface-split, age-sex-bw z-score, speed-fade (B4),
kyakushitsu_keiko running style (ρ=0.074 just sub-gate), odds-decoupling, NAR locality
features, NAR similarity, pgvector, WF-based LGB lambdarank for JRA classes 703/005/010
(all 4 axes negative on full judge), calibration/isotonic stacking on per-class residuals
(REJECT all 7 NAR classes), partial-pooling / James-Stein (identity limit proof),
per-class HARD split (iter20), post-hoc cascade, NAR B lgb-lambdarank residual (REJECT
H4, place3 −0.421pp).

**Architecture:** MLX transformer (2-fold underpowered), C4 expected-placement rerank
(real-holdout REJECT), per-place calibration (collinear P̂₁/P̂₃, all-REJECT).

**HPO:** NAR iter12 HPO (ADOPT, already production). JRA iter13 HPO on iter9 features
(REJECT — depth=5, CV/WF mismatch). LGB lambdarank HPO on JRA 016 within full judge
(stale-residual reject; discussed further in Lever 2).

---

## 3. Current Production State

| Category | Model                                                                             | Features                           | WF top1 (2023-25 holdout)     |
| -------- | --------------------------------------------------------------------------------- | ---------------------------------- | ----------------------------- |
| JRA      | iter19-jra-cb-kohan3f-going-v8 (CatBoost YetiRank)                                | 244 (241 iter14 + 3 kohan3f-going) | 0.4548 (pooled, 11,703 races) |
| NAR      | iter12-nar-xgb-hpo-v8 base + iter36 lgb-lambdarank C + iter30 ensembles (other/A) | 174                                | 0.585 (global NAR WF)         |

**Iter19 params inherited from iter14 (never HPO'd on the 244-feature store):**
depth=8, lr=0.05, l2_leaf_reg=3.0, iterations=1000, od_wait=30, Bayesian bootstrap,
random_strength=1.0, bagging_temperature=1.0, seed=2068.

---

## 4. Levers — Enumerated and Ranked

Ranking criterion: `confidence × expected_top1_pp`. Confidence = probability that the
mechanism is real AND not already captured. Expected pp = point estimate range under
an optimistic but plausible scenario.

### Lever 1 — HPO on iter19 244-feature JRA store (JRA only)

**Target model:** iter19-jra-cb-kohan3f-going-v8 (current JRA production).

**Hypothesis:** iter19 inherited iter14's hyperparameters verbatim (depth=8, lr=0.05,
l2_leaf_reg=3.0, od_wait=30). The iter13 HPO was run on the iter9 pacestyle store (241
features, before the 7 course-numerical features in iter14 and the 3 kohan3f-going features
in iter19). The iter13 HPO was rejected due to two distinct flaws: (a) CV only on 2023-2025
(too narrow — trained on 2022 and earlier, no 2024-2026 held out); (b) the winning config
had depth=5, which is too shallow for JRA's high-cardinality categorical structures. These
flaws are fixable. The optimal hyperparameters for 244 features (including the
`kohan3f_firm_avg5` feature with 72% NULL rate that CatBoost must route via missing-value
splits) may differ meaningfully from iter14's v7-lineage defaults.

**Mechanism:** Hyperparameter space is genuinely orthogonal to feature space. On the iter14
store, the default params were never validated against a properly-designed CV protocol.
On the iter19 store (different NULL structure in 3 new features), the optimal depth/l2/lr
tradeoff may shift. Concrete examples: (a) `kohan3f_soft_avg5` has 84% NULL — a shallower
or more-regularised tree may split on it more conservatively, avoiding overfit to the small
non-NULL population; (b) higher `od_wait` (30→50-100) could allow better convergence on
folds where early stopping fires prematurely (iter19 best_iter=315, well below the 1000
cap on average, suggesting premature stopping is possible in high-variance folds).

**Distinction from DO-NOT-RETEST:** The iter13 HPO is CLOSED (ran on iter9 store, CV
protocol flawed). This lever is HPO on the iter19 store (244 features, different NULL
structure) with a corrected CV protocol (5-fold leave-one-year-out on 2019-2023 instead
of 3-fold on 2023-2025; depth floor ≥ 7 to exclude the shallow-tree failure mode). This
is precisely the "PARTIALLY NOVEL" lever documented in `jra-hypo-training-procedure.md`
(Rank-4), confirmed NOT in DO-NOT-RETEST: "untested on the iter14 feature set and on
the serve-realistic distribution." Since iter19 is now the production base, running it
on the iter19 store is the correct scope.

**Expected top1 gain:** +0.1–0.3pp on WF holdout if a winning config is found; ~0pp
if params are already near-optimal (which is plausible given CatBoost's adaptive defaults).
The gain is bounded by the market efficiency ceiling, so >0.5pp from HPO alone is
unlikely without also finding a new feature.

**Cost/RAM:** 30-50 Optuna NSGA-II trials × 5 CV folds × ~5-7 min/fold ≈ 2.5–5 hours
wall time. Sequential single-process. Peak RSS ~10-14 GB (within the 48 GB budget given
DuckDB limit 6 GB; safe if only 1 heavy process runs). No GPU.

**Gate:** ADOPT iff WF-3y (2023/2024/2025 folds) shows top1 improvement on ≥2 of 3 folds
without >-0.05pp regression; then confirm on full 21-fold WF judge vs iter19 baseline
(fukusho_2p paired-bootstrap LB95 > 0).

**NOT in DO-NOT-RETEST:** confirmed above. The iter13 HPO on iter9 features is closed; HPO
on the iter19 244-feature store is explicitly NOT in the DO-NOT-RETEST list.

---

### Lever 2 — Training Window Ablation: 2013+ vs 2006+ (JRA and NAR)

**Target models:** JRA iter19 base; NAR iter12 base.

**Hypothesis:** Both JRA and NAR models train on the full 21-year window (2006-2026), with
time-decay weighting linearly ramping from 0.5 (oldest) to 1.0 (most recent). The assumption
that years 2006-2012 add net information has never been tested for either category. Evidence
from the error analysis (`jra-hypo-error-analysis.md`, Year-by-Year slice): pre-2017 WF
top1 averages 37-38% while post-2017 averages 40-46%, a 6-8pp gap driven by "cold start"
effects (older training folds have less data). However, there is also a genuine question of
stationarity: JRA racing conditions (track configurations, horse breeding dominance, going
preparation) have evolved significantly between 2006 and 2026. If 2006-2012 data is
non-stationary with respect to 2023-2026 racing patterns, even down-weighted rows could
be injecting noise that a 2013+ window would eliminate.

**Mechanism:** Removing pre-2013 data (a) raises the effective weight-per-row for recent
training data (2013+ receives higher gradient contribution); (b) may improve temporal
consistency of the learned decision boundaries for deep features like pedigree scores
(`sire_total_wr` built over long windows could behave differently with a tighter anchor
year); (c) reduces training time by ~40% (13y vs 21y), enabling faster HPO iterations.

**Distinction from DO-NOT-RETEST:** `jra-hypo-training-procedure.md` Rank-3 explicitly
confirms: "NOVEL for JRA finish-position — untested. The RS v3 model used 21y explicitly;
the finish-position model inherited this by default without ablation." No iteration document
records a window ablation for either JRA or NAR. This is not in the DO-NOT-RETEST list.

**Expected top1 gain:** Low-to-moderate, +0.1–0.3pp if pre-2013 data is net-negative. If
net-neutral (sufficiently handled by time-decay), the ablation rejects cleanly with minimal
regression risk. The pre-2017 underperformance in WF could also be a leave-one-year-out
artifact (training on post-year-only data when folds are small), not a stationarity issue;
if so, no gain. Honest assessment: more likely to reject than adopt, but very cheap to test.

**Cost/RAM:** Cheap filter: 3 variants (20060101 / 20130101 / 20160101) × 3 folds × ~5-7
min/fold ≈ 45-63 min wall. Full run if a winner: similar to iter19 but fewer rows, ~4-5
hours. Low RAM (smaller training sets).

**Gate:** Cheap filter: per-fold top1 improves on ≥2 of 3 folds without >-0.05pp regression.
Full run: fukusho_2p paired-bootstrap LB95 > 0 vs iter19 (JRA) or iter12 (NAR).

**NOT in DO-NOT-RETEST:** confirmed.

---

### Lever 3 — Post-Hoc Isotonic Score Recalibration (JRA; NAR secondary)

**Target model:** JRA iter19 base scores (used for rank-1 assignment at serve time).

**Hypothesis:** Both JRA and NAR models are systematically under-confident: JRA ECE=0.062,
decile-10 gap −14pp (model assigns 0.71 implied win probability; horse actually wins 85%).
The error analysis documents this as "Failure Mode B" and labels it "highest ROI lever"
(I2 §8) — yet it has NEVER been implemented. CatBoost's cross-entropy regularisation
shrinks scores toward the prior (uniform 1/n), compressing the rank boundary between close
competitors. Post-hoc isotonic regression on softmax scores would unfold this compression,
directly sharpening rank decisions at the margin.

**Mechanism:** The rank-1 assignment is an argmax over CatBoost scores. Calibration does not
change the global ordering but it can flip close pairs: if horse A has score 1.23 and horse
B has score 1.19, the model currently picks A. But if the calibrated probability of A is 0.55
and B is 0.50 (within noise), the calibrated ordering may shift in ~5% of close calls where
the true probability difference matters. The 57.3% top1 in high-confidence races vs 29-31%
in low-confidence races shows that the score gap already carries strong metacognitive signal;
calibration would amplify this separation.

**Distinct from DO-NOT-RETEST calibration items:** The DO-NOT-RETEST "calibration" entries
are:

- Isotonic/Platt stacking (iter15): applied as a SECONDARY model on top-1 binary output
  (logistic/isotonic on binary label, not on raw scores). REJECT.
- C4 joint-placement rerank (phase3): reranked by `3P̂₁+2P̂₃+P̂₃` using calibrated probas.
  REJECT on real holdout.
- Per-class isotonic on blended member scores (H3): calibration of the blended ensemble
  output. REJECT all 7 NAR classes.

This lever is different: **direct isotonic calibration of the raw CatBoost NDCG score
distribution** (before rank assignment), fit on a 2021-2022 calibration window, applied to
2023-2026 holdout — then measuring Δtop1 from rank order changes. This specific procedure
is documented in `jra-hypo-error-analysis.md` Hypothesis 1 as "Never tried." The distinction
from prior calibration experiments is the target: raw NDCG score (not binary top-1 label,
not blended ensemble score).

**Expected top1 gain:** +0.5–2.0pp top1 (I2 estimate; the 2.0pp bound is optimistic and
assumes 20-30% of close-pair rank decisions flip to correct). Conservative: +0.3–0.8pp.
This is potentially the highest single-lever gain available WITHOUT retraining.

**Cost/RAM:** Trivial. Fit scikit-learn `IsotonicRegression` or `CalibratedClassifierCV`
on 2021-2022 WF prediction parquets (already exist). Apply to 2023-2026. Total wall time:
< 1 hour. RAM: < 2 GB (predictions only, no feature parquets needed at scale).

**Gate:** ADOPT iff top1 on 2023-2026 holdout improves ≥ 0.1pp without >-0.05pp regression
on fukusho_2p. Since this is strictly a post-processing step, the gate can be strict (no
retraining risk). For serve: calibration function must be applied at the same time as
score computation in `calibrate_finish_position.py`.

**NOT in DO-NOT-RETEST:** confirmed (the "Never tried" verdict is explicit in
`jra-hypo-error-analysis.md` H1; all three prior calibration experiments attacked different
targets).

---

### Lever 4 — Distance-Band Win-Rate Feature for 1400-1599m (JRA)

**Target model:** JRA iter19 base, as an additive feature layer.

**Hypothesis:** 1400-1599m is JRA's worst well-populated distance band (38.6% top1,
−6.2pp vs global, n=1,582 races = 13.5% of holdout). The existing features include
`same_distance_win_rate` at the exact-distance level (e.g., 1400m or 1600m), but a horse
that has raced only at 1200m and 1600m will have zero or one prior start in the 1400-1599m
band. The hypothesis is that a **band-level win rate** (aggregating all starts between
1400-1599m regardless of exact distance) captures a horse's tactical preference at the
"sprint-to-mile transition zone" better than exact-distance rates. A feature
`horse_1400_1599_win_rate − career_win_rate` would capture over/under-performance at
this distance.

**Mechanism:** At 1200-1399m pace is sprint-uniform (all speed); at 1800m+ it is settled
gallop. At 1400-1599m tactics are most variable (front-runner vs mid-field balance is
neutral), leading to the highest fraction of rank-4+ surprise winners (25.9% vs 22.8%).
Distance-band specialisation — whether a horse is a "mile specialist" vs a "sprinter stepping
up" — is not captured by exact-distance features when the band span is 200m.

**Distinction from DO-NOT-RETEST:** `jra-hypo-error-analysis.md` H3 identifies this as
"Documented but never probed." The `h-laplevel-speed-fade.md` (B4, ABORT) tested
per-lap sectional deceleration (a within-race pace shape metric), which is a different
concept from per-horse band-level win rates. The distinction is explicit in H3: "H-LAPLEVEL
tested sectional deceleration. This hypothesis is distance-band win rate." Not in
DO-NOT-RETEST.

**Expected top1 gain:** +0.2–0.5pp global (13.5% of races; recovering 3pp within the
1400-1599m slice yields 0.41pp global; realistic upper bound ~0.4pp accounting for
redundancy with `same_distance_win_rate` and potential NULL sparsity). The probe must
clear the partial ρ ≥ 0.08 gate controlling for odds and `same_distance_win_rate` first.

**Cost/RAM:** Probe: 1 new feature SQL over existing PG mirror, compute partial ρ on
2023-2026 holdout. < 30 min. If probe clears: add 2-3 features to the iter19 store
(iter19 + distance-band layer), run 3-fold WF cheap filter. Full retrain only if
cheap filter passes. RAM within budget.

**Gate:** Probe: partial ρ ≥ 0.08 in 2023-2026 holdout controlling for odds and
career_win_rate. Cheap filter (3 folds): top1 improves ≥2/3 without >-0.05pp regression.
Full judge: fukusho_2p LB95 > 0.

**NOT in DO-NOT-RETEST:** confirmed.

---

### Lever 5 — Per-Class HPO on NAR Model: Full Optuna Sweep on iter12 Store (NAR only)

**Target model:** NAR iter12-nar-xgb-hpo-v8 (XGBoost rank:pairwise, max_depth=7,
reg_lambda=1.967, subsample=0.618, colsample_bytree=0.750, n_estimators=650).

**Hypothesis:** The iter12 NAR HPO used a 3-fold CV on 2023/2024/2025 with search space
bounded by: max_depth ∈ [5,10], lr ∈ [0.04, 0.08], reg_lambda ∈ [1.0, 3.0],
n_estimators ∈ [400, 800]. This was 50 Optuna NSGA-II trials. Since iter12's acceptance
(+0.044pp top1, +0.071pp place3), the prediction codebase has improved (serve-skew fixes,
09:30 cron, DO-TTL4h), and the production feature store is the same 174-feature iter12
store. A **wider HPO search** with (a) expanded search space (allow depth up to 12, lower
lr floor to 0.03, allow reg_lambda up to 5.0, add XGBoost-specific gamma / min_child_weight
floor modifications), (b) 100 trials (double the budget), (c) the 5-fold CV on 2019-2023
(same fix as proposed for JRA Lever 1) could find a locally-optimal config not reached by
the initial 50-trial 3-fold search. The iter12 winner was trial #49 (out of 50) — a near-end
win suggesting the search space boundary may not have been fully explored.

**Mechanism:** The iter12 winner (depth=7, subsample=0.618, reg_lambda=1.967) achieves
stronger regularisation than the defaults. But the 50-trial search with 3-fold CV may not
have fully explored the joint (subsample, colsample_bytree, reg_lambda, depth) interaction.
Specifically: the combination of lower depth (6) + lower lr (0.03) + higher reg_lambda (4.0)
may perform better on the 5-fold extended CV (which penalises overfit to 2023 more heavily)
than the current winner which was selected partly on 2023 performance.

**Distinction from DO-NOT-RETEST:** iter12 HPO is ADOPTED (in production). This lever is a
WIDER + CORRECTED re-run of the same HPO on the same feature store, with double the trial
budget and a corrected CV protocol. The prior run was not exhaustive (50 trials, trial #49
was the winner — 1 trial from the budget ceiling). This is NOT proposing a new objective
or new features. It is genuinely novel in the sense that no HPO has run on NAR with 100+
trials or a 5-fold CV protocol.

**Distinction from H4 (Wave 1):** H4 tested LGB lambdarank as a NEW per-class residual
member (not a base model HPO). This lever is an expanded HPO on the existing XGBoost
base model with rank:pairwise objective — a completely different scope.

**Expected top1 gain:** Low-moderate. The iter12 HPO already found a strong config; a wider
search may find +0.05–0.2pp additional top1. The NAR top1 gain from iter12 over iter9 was
only +0.044pp — this is a squeeze, not a breakthrough. Probability of clearing the gate
is moderate-low. Honest: likely to add ≤0.1pp top1 if the new config is found, possibly
nothing.

**Cost/RAM:** 100 Optuna trials × 5 CV folds × ~2-3 min/fold ≈ 16-25 hours wall time.
This is the most expensive lever here. Should run overnight (avoid 03:00 JST cron window).
Peak RSS ~10 GB (NAR 174 features, subsample 0.618 → smaller matrices). Concurrent ≤2
heavy processes.

**Gate:** Same as iter12: 5-condition gate (all 4 axes ≥ -0.05pp; ≥2 positive; ≥1 of
place2/place3 positive; quality green; per-bucket Wilson LB ≥ -2pp) vs iter12 as baseline.
If no improvement: REJECT cleanly.

**NOT in DO-NOT-RETEST:** confirmed (iter12 HPO is ADOPTED production, not a dead end; this
is an expansion of a successful experiment).

---

### Lever 6 — Large-Field Pace Variance Entropy Feature (JRA, probe-first)

**Target model:** JRA iter19 base, as an additive feature layer.

**Hypothesis:** Field size 15-16 races are 42% of the JRA holdout and show -3.1pp top1
vs global; 17+ races are -6.1pp. Combined, 51% of all JRA races are in these buckets.
The existing features (`field_nige_pressure`, `field_pace_index`, `field_style_diversity`,
`field_spread_past_corner_1_norm`, `field_has_pure_nige_horse`) are first-order aggregates
that do not capture **second-order pace volatility**: in a 16-horse field, the variance
of how many horses will genuinely contest the lead is higher than in an 8-horse field.
A Shannon entropy of `rs_p_nige` across the field (in races where field ≥ 15) would
capture "how ambiguous is the pace scenario?" as a race-level feature.

**Mechanism:** When many horses in a large field have moderate `rs_p_nige` (say, 0.30-0.50),
the race-level entropy is high, signifying that the pace outcome is unpredictable. This
creates conditions where the model's front-runner picks are less reliable (any one of 4-5
horses could take the lead). A horse positioned in the middle (rs_p_sashi) may benefit from
an unexpectedly fast pace created by multiple horses contesting the lead.

**Distinction from DO-NOT-RETEST:** `jra-hypo-error-analysis.md` H5 identifies this as
"Never tried." H-LAPLEVEL (B4, ABORT, ρ=0.0725) tested per-horse sectional deceleration
from race-level `zenhan_3f / kohan_3f` — a different concept. The RS features
`rs_p_nige` / `rs_p_*` are in the feature store; this lever uses them to construct a
race-level second-order aggregate, which has not been done.

**Caveat on RS coverage:** `rs_p_*` features have ~42% NULL rate across JRA horses. For
a field-level entropy feature, races where >50% of horses have NULL `rs_p_nige` will have
unreliable entropy estimates. Coverage analysis on the 2023-2026 holdout is required
before building: if more than 30% of large-field races have >50% NULL rs_p_nige, this
lever may be infeasible.

**Expected top1 gain:** +0.2–0.5pp global IF the probe clears ≥0.08 partial ρ and RS
coverage is sufficient (which is uncertain). The coverage constraint is the primary risk.

**Cost/RAM:** Probe: compute race-level entropy from existing feature parquets. < 30 min.
If probe clears: add 2-3 features, run cheap filter. Full retrain only if cheap filter passes.

**Gate:** Probe: partial ρ ≥ 0.08 in 2023-2026 holdout JRA controlling for odds and
field_nige_pressure. Coverage filter: ≥ 60% of large-field races have ≥ 50% of horses
with non-NULL rs_p_nige.

**NOT in DO-NOT-RETEST:** confirmed.

---

### Lever 7 — JRA per-class HPO residual on 016 (3勝クラス) only, with fresh iter19 base score

**Target model:** JRA class 016 only (n=727 holdout races 2023-2026).

**Hypothesis:** In `jra-kohan3f-verify.md`, class 016 shows the largest point-estimate
improvement from iter19: top1 +1.24pp (0.3796→0.3920). In `jra-hypo-rejection-gaps.md`
Rank-2, the asymmetry gap is documented: the JRA lgb-lambdarank residual experiments
(jra-lgb-lambdarank-full-judge.md) used **iter14** as the base score feature, not
**iter19**. Class 016 had a pooled +1.93pp top1 point estimate when using iter14 as
the residual base — but this residual was REJECTED because all 4 axes were negative when
judged against the iter25/26 full ensemble baseline (the "weld" pattern: residuals calibrated
against iter14 do not transfer to iter25/26 as the reference). Now that **iter19 is the
production base** (and iter25/26 ensembles have been retired), the correct residual
experiment is: a fresh HPO'd LGB lambdarank (or YetiRank) residual trained against
**iter19_score** as the base score feature, judged against the iter19 base alone.

**This is a genuinely different experiment from what was rejected:** the `jra-lgb-lambdarank-full-judge.md`
REJECT was for residuals calibrated against iter14 judged against the iter25/26
ensemble (stale residual + wrong reference distribution). A residual calibrated against
iter19 at the iter19 reference is unexplored.

**Warning on statistical power:** class 016 has n=727 holdout races. The paired-bootstrap
at n=727 requires a point-estimate Δtop1 ≥ ~0.7pp for LB95 > 0 (σ ≈ 0.018, LB95 ≈
Δ - 1.645×σ/√n, need Δ > 1.65×0.018 = ~0.7pp at n=727). The point estimate from the
pooled judge (+1.93pp) suggests this could clear, but the full residual retrain with
inner CV is required to produce the actual holdout delta. If the residual only adds
+0.5pp on the powered holdout it will not clear LB95 > 0.

**User-override precedent:** iter36 NAR class C was ADOPTED with a user win-priority
override (−0.211pp place3). If a class 016 residual produces, say, +1.5pp top1 with place3
regression, the same override is available.

**Distinction from DO-NOT-RETEST:** The CLOSED experiment is JRA lgb-lambdarank residual
on iter14 base vs iter25/26 ensemble baseline (jra-lgb-lambdarank-full-judge.md: REJECT,
all 4 axes negative). The experiment here uses iter19 as both base and reference. Explicitly
noted as "asymmetry gap" in jra-hypo-rejection-gaps.md Rank-2.

**Expected top1 gain for 016 class only:** +0.5–2.0pp on 016 holdout (high uncertainty,
n=727 is small). Global top1 impact: 016 is 6.2% of JRA holdout races, so +1.5pp on 016
→ ~+0.09pp globally. This lever is primarily valuable for improving one challenging class,
not for the global +2.25pp target.

**Cost/RAM:** 30-60 Optuna trials × 3 inner CV folds × ~3 min/fold for 016 only ≈ 1.5-4.5
hours. Small RAM footprint (n=727 holdout, ~20k training rows for the residual).

**Gate:** top1 LB95 > 0 on 016 holdout (n=727, 2023-2026) AND no axis < -0.05pp (or
explicit user win-priority override). Pooled global metrics must not regress >-0.05pp.

**NOT in DO-NOT-RETEST:** confirmed; different base model and reference baseline from the
closed experiment.

---

### Lever 8 — NAR per-class B: win-priority override residual (NAR only)

**Target model:** NAR class B (iter12 fallback, n=7,124 holdout races 2023-2026).

**Hypothesis:** H4 in `oi-2026-06-10-wave1-h1-h5.md` tested a fresh LGB lambdarank
residual for NAR class B. The result: top1 +0.239pp, place2 +0.295pp — but place3
**-0.421pp** (exceeds -0.05pp floor) and top1 LB95 = -0.000983 < 0 (Holm-adj p=0.1706).
The experiment was REJECTED. However, the top1 point estimate (+0.239pp) is positive, and
place2 (+0.295pp) is positive. This is structurally similar to the NAR class C situation
that was ADOPTED (iter36) via a user win-priority override (C: top1 +0.342pp, LB95 +0.0012,
place3 -0.211pp — ADOPT). The difference is that NAR-B's LB95 is **negative** (−0.000983
vs C's +0.0012) and Holm is non-significant (p=0.1706 vs p=0.0006 for C). This is a
**weaker signal than C was** and cannot be ADOPTED under the same win-priority override
logic without a fresh re-run under the iter12 reference (the H4 test used the iter30 C
baseline, not iter12 for B).

**What would need to change:** A **fresh retrain of the LGB lambdarank B residual against
iter12_score** (the actual iter12 XGBoost baseline scores for class B, not the iter30 C
chain), with 60-200 Optuna trials (more than H4's 12 per family), judged on the powered
B holdout (n=7,124). The prior H4 experiment used 12 trials per loss family — insufficient
for Optuna to converge. A 200-trial search could find a more-regularised LGB config that
avoids the place3 −0.421pp regression.

**Distinction from DO-NOT-RETEST:** `sci-track-2026-06-11-cycle-summary.md` §5(ii) states:
"NAR class B WAS given the lgb-lambdarank residual treatment in H4 and REJECTED. It is NOT
an untested proven-recipe lever. Re-running with the same recipe would reproduce the same
result." This lever proposes a **significantly different setup**: (a) fresh reference is
iter12_score (not iter30 B chain as in H4); (b) 200 Optuna trials (not 12 per family);
(c) place-protecting blend constraint (constrained weight so that place3 regression ≤
-0.05pp). This is more work than re-running H4 verbatim, and the outcome is still
uncertain. The honest assessment is: this is a marginal re-open, not a clean new lever.

**Expected top1 gain:** +0.1–0.3pp if the place3 regression can be tamed by constrained
weighting. Probability of clearing all gates (including place3) is low, given that H4
failed by a wide margin on place3. However, the n=7,124 holdout means the bootstrap is
powered for detecting +0.2pp effects reliably.

**Cost/RAM:** 200 Optuna trials × 3 folds × ~2 min/fold ≈ 20 hours. High cost relative
to expected gain. This is a stretch lever.

**Gate:** Top1 LB95 > 0 AND place3 ≥ -0.05pp (multi-metric gate, no user override by
default unless user explicitly approves win-priority for B as they did for C).

---

## 5. Levers Specifically NOT Proposed (Reasoning Documented)

| Lever idea                                        | Reason not proposed                                                                                                                                                                                                                        |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| JRA pooled LGB lambdarank residual on iter14      | CLOSED: all 4 axes negative (jra-lgb-lambdarank-full-judge.md)                                                                                                                                                                             |
| JRA LGB lambdarank residual for 703/005 on iter19 | Same mechanism as closed 703/005 test (stale residual was the flaw, but 703/005 ALL-NEGATIVE on full judge even with fresh HPO — n=4229/3147 → power is sufficient and the signal is absent)                                               |
| Graded relevance scheme D full-system (JRA/NAR)   | `jra-hypo-rejection-gaps.md` Rank-3: base-model was already REJECT; full-system judge unlikely to reverse. Low priority.                                                                                                                   |
| Serve-realistic parquet co-training               | `jra-hypo-training-procedure.md` Rank-2: depends on characterising advance-odds distribution vs settled. Engineering cost is high and the serve-skew has already been largely fixed by the 09:30 cron. Residual benefit unclear.           |
| Blend-weight re-optimization for JRA              | JRA is now base-only (no per-class ensembles). No blending system to re-optimize.                                                                                                                                                          |
| RS relationship features (nige_vs_field)          | Coverage gate still applies (87% RS coverage required; not achieved).                                                                                                                                                                      |
| kyakushitsu_keiko running style (official)        | ABORT at ρ=0.068 (just below 0.08 gate). Marginal re-open hypothesis in jra-hypo-rejection-gaps.md Rank-4 — partial ρ may be slightly underestimated with 90.7% NULL rs*p*\* as control, but 0.012 below gate is not enough to prioritise. |
| NAR lap-time per-furlong speed fade               | Feasibility unclear: `nvd_ra.lap_time` populated status unknown; B4 was sub-gate (ρ=0.0725) and requires warehouse investigation. Forward proposal in sci-track-2026-06-11-cycle-summary.md §5(i).                                         |
| NAR 2yo/3yo bucket re-routing                     | Data quality fix (~0.15pp NAR) documented in rootcause I5 but small and orthogonal to top1 target.                                                                                                                                         |
| NAR A/OP/MUKATSU/NEW class residuals              | All REJECTED in H3 (calibration + stacking); H4 not run for these classes but n is very small (OP/MUKATSU/NEW) and A already has iter30.                                                                                                   |
| H5 multi-class probability head (7-class)         | CUT on memory in Wave 1 (10-16 GB). Scoped solo re-run (one class at a time) not excluded but expected to reproduce the same capacity-ceiling finding as H1-H4 for NAR.                                                                    |
| Transformer / MLX architecture swap               | Explicitly NOT recommended: 2-fold WF, no HPO; would require 21-fold + full HPO (~3 days compute); Grinsztajn 2022 tabular GBDT advantage; speculative.                                                                                    |

---

## 6. Recommended Execution Sequence

Ordered by (expected_gain × probability_of_passing_gate / cost). Levers that require
probe gates are listed with probe first.

| Step | Lever                                                 | Wall time (cheap filter) | Expected global top1 Δ                 | Confidence                                    |
| ---- | ----------------------------------------------------- | ------------------------ | -------------------------------------- | --------------------------------------------- |
| 1    | **Lever 3: Isotonic calibration**                     | < 1 hour                 | +0.3–0.8pp (conservative), up to +2pp  | HIGH (never tried; mechanism well-understood) |
| 2    | **Lever 2: Training window ablation** (JRA 3-fold WF) | < 1 hour                 | +0.1–0.3pp if pre-2013 is net-negative | MEDIUM-LOW                                    |
| 3    | **Lever 4: 1400-1599m distance-band probe**           | < 30 min probe           | +0.2–0.5pp if probe clears             | MEDIUM (depends on partial ρ)                 |
| 4    | **Lever 6: Large-field pace entropy probe**           | < 30 min probe           | +0.2–0.5pp if probe + coverage clear   | MEDIUM-LOW                                    |
| 5    | **Lever 1: HPO on iter19 244-feat store**             | 3–5 hours                | +0.1–0.3pp                             | MEDIUM                                        |
| 6    | **Lever 7: 016 class per-class residual**             | 1.5–4.5 hours            | +0.05–0.12pp global (+0.5–2pp on 016)  | LOW-MEDIUM                                    |
| 7    | **Lever 5: NAR XGBoost expanded HPO**                 | 16–25 hours              | +0.05–0.2pp NAR                        | LOW-MEDIUM                                    |
| 8    | **Lever 8: NAR class B win-priority residual**        | 20 hours                 | +0.0–0.15pp NAR (uncertain gate)       | LOW                                           |

Execute steps 1-4 in parallel (all are cheap filters or no-retrain probes). Execute steps
5-8 sequentially after cheap filters report.

---

## 7. Stack Estimation — Can +2.25pp (relative 5%) Be Reached?

**JRA target: +2.25pp top1 (0.4548 → 0.4773).**

Optimistic stack:

- Lever 3 (calibration): +0.8pp (upper end of conservative range)
- Lever 4 (1400-1599m probe → retrain if pass): +0.3pp
- Lever 1 (HPO): +0.2pp
- Lever 6 (pace entropy → retrain if pass): +0.2pp
- **Optimistic total: +1.5pp**

Pessimistic stack (calibration adds 0.3pp, others reject):

- **Pessimistic total: +0.3pp**

**Central estimate: +0.5–1.0pp JRA top1**, which is approximately 50% of the relative-5%
target (0.4548 → 0.4773 requires +2.25pp). The relative-5% target is achievable only if
multiple levers stack positively — this requires at least 2-3 of levers 1-6 to pass their
gates, which is the optimistic scenario.

**NAR target: +2.9pp top1 (0.585 → 0.6143).**

NAR is harder: iter12 is already the result of a 50-trial HPO. The expanded HPO (Lever 5)
is the primary lever, but its ceiling is low (+0.05-0.2pp). The only path to +2.9pp on
NAR would require genuinely new signal (lap-time speed fade IF warehouse data exists, odds
drift IF historical intraday data exists) — both of which are multi-week feasibility
investigations, not same-session experiments.

**Central estimate: +0.05–0.2pp NAR top1** from available levers.

---

## 8. Honest Verdict on Achievability

### Relative +5% (JRA +2.25pp, NAR +2.9pp)

**JRA: Unlikely but possible in the optimistic scenario.** The calibration lever alone
could contribute +0.5-1.0pp if the GBDT score compression is as large as the error
analysis suggests. Combined with a successful window ablation or distance-band feature,
+2.25pp is within reach — but it requires 3+ levers to all pass, which has not occurred
in any prior 5-iteration run. The most honest probability: ~20-30%.

**NAR: Very unlikely from available levers.** +2.9pp requires new signal. The expanded
HPO adds marginal improvement on a model that was already HPO-optimised at iter12. Without
lap-time data, odds drift history, or a fundamentally new feature category, NAR cannot
reach +2.9pp from the levers enumerated here. Probability: < 5%.

### Absolute +5pp (JRA 0.450→0.500, NAR 0.585→0.635)

**Both: Not achievable with current levers.** The rootcause-2026-06-11 diagnosis shows the
model already beats the Harville oracle by +11.3pp (JRA) and +15.4pp (NAR). An additional
+5pp absolute on top of current accuracy would require either: (a) new pre-race data that
does not exist in the current warehouse (intraday odds dynamics, sectional timing at NAR);
or (b) a structural improvement in the feature set far exceeding anything probed so far.
Probability: << 1% for absolute +5pp.

### What is realistic

A successful execution of levers 1-7 in the recommended sequence should yield:

| Category | Realistic gain (central) | Stretch (optimistic stack) |
| -------- | ------------------------ | -------------------------- |
| JRA top1 | +0.5–1.0pp               | +1.5–2.0pp                 |
| NAR top1 | +0.05–0.15pp             | +0.2–0.3pp                 |

This is meaningful but short of the relative-5% target for JRA, and well short for NAR.
The plan is worth executing — particularly lever 3 (calibration, no retraining, < 1 hour)
which is the cleanest untried lever in either corpus — but the goal requires an honest
acknowledgement that the models are operating near the empirical frontier for available data.

---

## 9. Sources

All findings are sourced from (verified against DO-NOT-RETEST):

- `jra-kohan3f-verify.md` — iter19 deploy results and params
- `jra-hypo-rejection-gaps.md` — gap analysis, Rank 1-5 re-opens
- `jra-hypo-training-procedure.md` — HPO/window/od_wait hypotheses Rank 1-5
- `jra-hypo-error-analysis.md` — failure-mode analysis H1-H5
- `iter19.md` (L4 class weight) — the iter19 "old" doc (REJECT)
- `iter19-deploy.md` — production deploy confirmation
- `iter12.md` — NAR HPO ADOPT, trial #49 near-ceiling finding
- `oi-2026-06-10-wave1-h1-h5.md` — H4 NAR class B residual (REJECT)
- `oi-2026-06-10-iter36-lgb-lambdarank-residual-C-adopt.md` — NAR class C win-override
- `jra-lgb-lambdarank-full-judge.md` — JRA 703/005 lgb residual REJECT
- `rootcause-2026-06-11-DIAGNOSIS.md` — I1-I7 binding constraints
- `sci-track-2026-06-11-cycle-summary.md` — saturation summary, forward proposals
- `jra-unused-data-scan.md` — JRA warehouse scan (space exhausted verdict)
- `imperative-riding-wave.md` — DO-NOT-RETEST list (verified per lever)
