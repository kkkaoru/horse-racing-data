# JRA Training-Procedure Hypotheses — 2026-06-13

**Lens**: training procedure and system architecture (not new signals — JRA signals are
exhausted). Session context: serve-skew fixes (Fixes 1-3) are now live; the served feature
distribution changed on 2026-06-11/13 (realtime odds + market-signal + futan). The per-class
blend weights (iter 25 v2) were trained on the OLD distribution. The weld pattern is
confirmed: base-swap (NAR G-1/F1, scheme-D) regresses the blend → co-training is the
only unlock.

DO-NOT-RETEST corpus checked before each entry.

---

## Rank-1 — Serve-Realistic Re-optimization of Blend Weights Only

### What was done historically

The per-class ensemble blend weights (iter 23 v1, iter 25 v2) were optimized with Optuna
TPE 200 trials on validation years 2018-2022 / holdout years 2023-2026 using the
**WF (post-race final odds)** feature parquets. Those parquets contain fully-settled
`odds_score`, `inverse_odds_implied_prob`, `inverse_odds_market_share`,
`odds_score_diff_from_race_avg` (total 17 market-signal + futan features, ~35.6% SHAP
importance on iter14). The pre-fix serve path had all 17 features zeroed or at OOD median;
the post-fix path now populates them from D1 advance odds + Fix2 + Fix3 layers.

The blend weight optimizer (`optimize_per_class_ensemble.py`) minimizes validation top1 on
the score distribution that comes from these features being populated. At serve time
post-fix, the market-signal and futan features are populated with **advance odds** (not
settled final odds), which have a different numerical distribution (e.g., average
`inverse_odds_implied_prob` will differ from its settled value). The blend weights were
therefore learned on a distribution (WF settled odds) that is now closer to the real serve
distribution than before (because previously serve was all-zero), but still not identical to
advance-odds scale.

**Specific mechanism**: the per-class v2 ensemble does a within-race rank-blend of the
member scores (`lib.normalize_within_race` → softmax simplex blend). The normalization is
rank-based and therefore scale-invariant to absolute score changes, but the per-member
GBDT scores themselves shift when the feature distribution shifts (different odds → different
score ordering at the margin). The α_iter14 ≥ 0.20 baseline floor was set on WF
characteristics; on the corrected serve distribution the optimal floor and weights may differ.

### Novelty verdict

**NOVEL — genuinely untested.** The serve-skew fixes landed on 2026-06-11/13; the blend
weight optimizer has never been run on a serve-realistic validation slice (partial advance
odds, not settled). No iteration doc records a serve-condition re-optimization of weights.
The weld-pattern literature (G-1/F1, scheme-D REJECT, nar-schemeD-deploy-judge.md) shows
that retraining the BASE and then re-running blend weight optimization correctly judges the
blend system, but these were all base-swap experiments. Re-optimizing weights WITHOUT
changing the base (cheapest possible intervention) has not been attempted.

### Test design

**Cheap filter (1-2 hours wall time)**:

1. Build a "serve-realistic" parquet for 2023-2026 by replacing settled `odds_score` /
   market-signal group / futan group with their OOD-median values for the first 20% of
   each race day's volume (simulating the fraction of races that still land in the partial
   advance-odds window). This approximates the observed serve distribution post-fix.
2. Re-run `optimize_per_class_ensemble.py` for the two highest-n classes (703, 005) with
   `--validation-years 2018,2019,2020,2021,2022` and `--holdout-years 2023,2024,2025,2026`
   but score on the serve-realistic parquet (not WF).
3. Compare the new holdout weights and holdout top1 vs the existing v2 weights applied to
   the same slice.

**Full run**: run for all 6 classes (as in iter 25). Accept-gate: holdout top1 LB95 > 0
on ≥2 classes vs current v2 ensemble weights.

### Cost

Optuna 200 trials × 6 classes × ~1 min/trial (predict-only, no retraining) ≈ 20 min wall.
No GBDT retraining. Pure Python, 6 GB DuckDB cap sufficient.

### Expected gain bound

The serve-distribution shift changes the within-race score ordering for a minority of
horses (those at the margin of a rank flip when odds go from settled to advance). The
blend weight optimizer can compensate by downweighting members most sensitive to this
distribution shift. Estimated gain: 0.1-0.5pp top1 on the serve-realistic slice vs current
weights. Uncertain but cheap; the primary value is confirming or refuting whether the
post-fix distribution warrants a weight refresh before any full retrain is committed.

---

## Rank-2 — Joint Base + Blend Co-Training on Serve-Realistic Features

### What was done historically

All JRA base training (iter14 WF-21y, iter25 low-capacity per-class) used WF (settled
final odds) features. The blend weight optimizer then ran on top. The weld pattern
(established definitively by nar-schemeD-deploy-judge.md and the NAR G-1/F1 judge) shows
that swapping ONLY the base while keeping existing blend weights and residual structure
degresses the blend. The accepted recovery path is "retrain the base on exactly the same
feature distribution, then re-run the full per-class residual + weight optimization chain
in one pipeline run."

No single-shot co-training experiment has been run on the **serve-realistic** feature
distribution. The closest precedent is the full retrain triggered for NAR scheme-D, but
that was a relevance-scheme change, not a feature distribution change.

### Novelty verdict

**NOVEL as a serve-condition co-training.** The mechanism is documented in the weld-pattern
literature, but the specific experiment — train iter14 base on a parquet where market-signal

- futan features reflect the post-fix advance-odds distribution, then re-run the full per-
  class chain on that base — has never been executed.

Note: this is distinct from the iter14 training with WF features. The hypothesis is that
a base model trained on advance-odds features (matching serve) will produce score
distributions that are consistent with what the per-class residuals and blend weights will
see at serve time, eliminating the train/serve mismatch currently present in the blended
system.

### Test design

**Cheap filter (3-4 hours wall time)**:

1. Build a "serve-realistic" base parquet: take the existing iter14 feature parquet, replace
   settled odds/market-signal/futan values with the same OOD-median imputation used by the
   pre-fix serve path for the 2023-2026 window. This yields a "downgraded" parquet that
   approximates what the advance-odds path provides (advance odds are closer to OOD-median
   than to settled for the first few hours of the day).

   Alternatively, build a parquet where for each race-day, the first N% of races receive
   OOD-median odds (N drawn from the empirical distribution of the D1 advance-odds coverage
   rate, ~85-90% effective coverage at 09:30 per serve-combined-recovery-measurement.md).

2. Run WF-3y (2023/2024/2025 folds) on the serve-realistic parquet with iter14 hyperparams.
   Compare per-fold top1 and f2p LB95 vs the existing iter14 predictions on WF features.

3. If step 2 shows the serve-realistic base is not worse than WF base on these 3 folds (or
   is better in terms of real serve accuracy), proceed to full 21y run + per-class chain.

**Full run**: full 21y WF (2007-2026), then re-optimize per-class ensembles for all 6
classes. Accept-gate: holdout f2p LB95 > 0 on ≥2 classes AND no top1 regression > -0.05pp.

### Cost

Cheap filter: ~4 hours (3 WF folds × ~7 min/fold). Full run: ~7 min base + ~20 min per-
class chain = ~30 min total. Primary cost is engineering the serve-realistic parquet build
(1-2 hours to write and test the script, no new data dependencies).

### Expected gain bound

The WF train → serve gap (serve-skew) was confirmed at +12.93pp top1 at population scale
(serve-condition-baseline-population.md). A base trained on serve-realistic features should
produce a score distribution aligned with the post-fix serve path, eliminating the ~5-10%
of races where the current WF-trained model receives substantially different feature vectors
at serve vs train time. Expected top1 gain: 0.3-1.0pp on the serve-realistic holdout. The
full-system (base+blend) effect could compound further if the per-class residuals also
benefit from a more consistent base distribution.

---

## Rank-3 — Training Window Ablation (2006-2026 vs 2013-2026 and later)

### What was done historically

The JRA base model (iter14) trains on the window 2006-2026 (21 years,
`DEFAULT_TRAIN_START_DATE = "20060101"`, `fold_count = 20` in the iter14 YAML). This
decision mirrors the RS v3 model which also uses a 21y window (explicitly noted as
"21y 再学習" in project_running_style_v3_trained.md). No training-window ablation for
JRA finish-position has been recorded in any iteration document.

The `--train-start-date` parameter in `train_finish_position_catboost_walk_forward.py`
accepts an arbitrary start date. The time-decay weighting (`compute_time_decay_weights`)
linearly ramps from 0.5 (oldest year) to 1.0 (most recent year), so ancient data is
already down-weighted by 50%.

JRA racing conditions have changed significantly between 2006 and 2026:

- Track configuration and going preparation methods have evolved
- Horse breeding population has shifted (e.g., Deep Impact sire line now dominant)
- Race schedule structure has changed
- Feature engineering assumptions (e.g., course physical features via `jvd_cs`) are
  JRA-specific and stable across the full window, but horse-level and pedigree signals
  may have non-stationary relationships with finish position over a 20-year span

Whether years 2006-2012 add net information or net noise relative to 2013+ has never been
tested.

### Novelty verdict

**NOVEL for JRA finish-position — untested.** The RS v3 model used 21y explicitly; the
finish-position model inherited this by default without ablation. The NAR HPO
(iter12) used a 3-fold CV on 2023/2024/2025 which implicitly trains on 2006-2022, but
this was an HPO experiment not a window ablation.

DO-NOT-RETEST check: no iteration document mentions `--train-start-date` ablation, no
bucket-experiment doc covers window shrinking, no rootcause doc tested this axis.

### Test design

**Cheap filter (2-3 hours wall time)**:

1. Run WF-3y (folds 2023/2024/2025) with three start-date variants:
   - A: 20060101 (current — 21y)
   - B: 20130101 (13y — excludes pre-2013 era)
   - C: 20160101 (10y — recent era only)

   Use iter14 hyperparams unchanged. Compare per-fold top1 and f2p LB95.

2. If variant B or C shows a consistent per-fold improvement over A across ≥2 of 3 folds
   without top1 regression > -0.05pp, proceed to full 21y (or N-y as determined) WF run.

**Full run**: WF on the winning window (e.g., 2013-2026) across all available folds, then
re-run per-class chain. Accept-gate: holdout f2p LB95 > 0 on ≥2 classes vs iter14 baseline.

### Cost

Cheap filter: 3 variants × 3 folds × ~5 min/fold ≈ 45 min. Full run if window wins:
~5-6 min (13y has ~62% of 21y rows → proportionally faster).

### Expected gain bound

Low-to-moderate. Time-decay already penalizes old years by 50%. If 2006-2012 data is truly
net-negative, removing it could yield 0.1-0.3pp on the recent folds where temporal
mismatch is largest. If it is net-neutral (already down-weighted sufficiently), the ablation
rejects cleanly. Risk of a large regression is low because the remaining training data
(2013-2026) is the highest-weight portion under the current scheme.

---

## Rank-4 — Full HPO on Current v8 Store (Post-Serve-Fix Feature Distribution)

### What was done historically

The iter13 HPO (50 Optuna NSGA-II trials, 3-fold CV on 2023/2024/2025) **rejected**:
winner was depth=5 (vs default 8), lr=0.066, l2=4.21, bagging_temperature=0.036,
random_strength=3.40. The failure diagnoses were: (a) CV/WF mismatch — CV on 2023-2025
only did not generalize to the full 21-fold WF; (b) depth 5 too shallow for JRA's high-
cardinality categoricals; (c) bagging_temperature near 0 (near-deterministic draw) combined
with high random_strength produced noisy splits.

The iter13 HPO was run on the **iter9 pacestyle feature set** (253 columns). Iter14 added 7
course-numerical features (260 columns, effective 241 post resolve). The HPO has never been
run on the iter14 feature set.

The serve-skew fix further changes the training context: if a serve-realistic retrain
(Rank-2 above) is done, the resulting base model would benefit from re-HPO because the
optimal depth/lr/l2 for the new distribution may differ from v7-lineage defaults.

NAR iter12 had a full Optuna HPO (49 trials, NSGA-II) on the NAR iter9 pacestyle features
and produced a confirmed ACCEPT (+0.071pp place3). The JRA HPO was run independently on
different features and rejected. The asymmetry suggests JRA-specific HPO may benefit from
a wider search space that allows depth > 8 (iter10a at depth=10 rejected, but that was a
single-point test not a broad search that also varied l2/lr/bagging).

### Novelty verdict

**PARTIALLY NOVEL — untested on the iter14 feature set and on the serve-realistic
distribution.** The iter13 HPO is a documented prior. What is genuinely new: (a) running
HPO on the iter14 feature set (7 additional course features change the tree structure,
especially `course_final_straight_m` which appeared in the top-25 importances of all 20
folds); (b) fixing the CV/WF mismatch — use 5-fold leave-one-year-out on 2020-2024 instead
of 3-fold on 2023-2025, which covers more temporal variance; (c) constraining depth ≥ 7
(iter13's depth=5 failure is documented as too shallow — set a floor).

### Test design

**Cheap filter (3-4 hours wall time)**:

1. Run 30-trial Optuna NSGA-II on the iter14 feature parquet with:
   - CV: 5-fold leave-one-year-out on 2020/2021/2022/2023/2024 (wider than iter13's
     2023-2025; avoids over-fitting to the most recent 3 years)
   - Search space: depth ∈ [7, 11], lr ∈ [0.03, 0.08] log, l2 ∈ [2.0, 8.0] log,
     bagging_temperature ∈ [0.5, 2.0] (floor at 0.5 to avoid the near-deterministic
     failure mode), random_strength ∈ [0.5, 3.0], iterations ∈ [400, 1200]
   - Objective: picker_score = 0.7 × global_NDCG@3 + 0.3 × worst_bucket_NDCG@3

2. Retrain full WF-3y (2023/2024/2025) with the Pareto-winner params. Compare vs iter14
   default params (same folds).

3. Accept at cheap-filter if winner improves top1 on ≥2 of 3 folds without >-0.05pp
   regression on any metric.

**Full run**: full 21-fold WF (2007-2026) + per-class chain re-optimization.

### Cost

Cheap filter: 30 trials × 5 CV folds × ~1.5 min/fold ≈ 225 min ≈ 3.75 hours. Full run
if cheap filter accepts: ~7 min + ~20 min per-class chain = ~30 min.

### Expected gain bound

Low-to-moderate. The iter13 HPO rejected on the iter9 feature set with a suboptimal CV
protocol. With depth constrained ≥ 7 and a wider CV window, the risk of the iter13
failure mode (depth=5 shallow, CV→WF mismatch) is mitigated. Expected gain if a winning
config is found: 0.1-0.3pp top1. The gain is structurally bounded by the market ceiling
(iter14 already beats the Harville oracle by +11.3pp per rootcause-2026-06-11-DIAGNOSIS.md).

---

## Rank-5 — Early-Stopping Policy Tightening (od_wait=30 → od_wait=50)

### What was done historically

The CatBoost walk-forward trainer uses `early_stopping_rounds=30` (hardcoded in
`build_fold_namespace` in `train_finish_position_catboost_walk_forward.py`). The v7-lineage
defaults are `iterations=1000 with od_wait=30`. The iter14 average best_iter was 319
(min=149, max=562), meaning the model stops well before the 1000-iteration cap in most
folds.

An `od_wait` of 30 means: if the validation NDCG@3 does not improve for 30 consecutive
iterations, training stops. With lr=0.05 and typical tree counts of 150-600, 30 rounds
corresponds to 1.5-3.0 pp of effective learning rate progress. This is a relatively tight
early-stopping criterion that may be stopping some folds before the model fully converges,
especially for years where the validation set is small or noisy.

No early-stopping ablation (e.g., od_wait=50 or od_wait=100) has been recorded in any
iteration or rootcause document.

### Novelty verdict

**NOVEL — completely untested.** No iteration or HPO experiment varied `od_wait`. This is
one of the simplest possible training-procedure changes.

### Test design

**Cheap filter (30-60 min wall time)**:

1. Run WF-3y (2023/2024/2025) with three `od_wait` values: 30 (current), 50, 100.
2. Compare per-fold top1 and f2p LB95 vs current.
3. Accept if od_wait=50 or od_wait=100 wins on ≥2 of 3 folds without top1 regression.

**Full run**: full 21-fold WF with winning od_wait + per-class chain re-optimization.
Accept-gate: f2p LB95 > 0 on ≥2 classes.

### Cost

Cheap filter: 3 variants × 3 folds × ~7 min/fold ≈ 63 min. Full run: ~7-14 min depending
on od_wait (more iterations possible → slightly longer per fold). Per-class chain: ~20 min.

### Expected gain bound

Low. The model is already at convergence in most folds (best_iter avg=319, far from the
1000-iteration cap). Increasing od_wait by 20-70 iterations adds at most a few dozen extra
trees for folds where early stopping fires prematurely. Expected gain: 0.0-0.15pp top1 on
the folds where best_iter < 200. Risk: negligible (od_wait is a non-monotone effect; too
large od_wait can slightly overfit the validation fold, but 50-100 is well within the safe
range for the observed iter counts).

---

## Summary Table

| Rank | Hypothesis                                     | Novelty verdict | Cheap filter cost | Expected gain | Priority rationale                                                                                  |
| ---- | ---------------------------------------------- | --------------- | ----------------- | ------------- | --------------------------------------------------------------------------------------------------- |
| 1    | Blend-weight re-opt on serve-realistic distrib | NOVEL           | 1-2 hours         | 0.1-0.5pp     | No retrain needed; directly responds to the 2026-06-11 serve-distribution change; cheapest unlock   |
| 2    | Joint base+blend co-train on serve distrib     | NOVEL           | 3-4 hours         | 0.3-1.0pp     | Weld-pattern unlock; highest expected gain if base training and per-class chain are jointly aligned |
| 3    | Training window ablation (2013+ vs 2006+)      | NOVEL           | 45 min            | 0.1-0.3pp     | Trivial to test; resolves a long-standing inherited assumption; may reject cleanly                  |
| 4    | HPO on iter14 feature set + fixed CV protocol  | PARTIALLY NOVEL | 3.75 hours        | 0.1-0.3pp     | Iter13 HPO failure was protocol-specific, not fundamental; fixed CV design may succeed              |
| 5    | od_wait tightening (30 → 50-100)               | NOVEL           | 1 hour            | 0.0-0.15pp    | Fastest possible test; low gain ceiling but near-zero risk                                          |

## Recommended execution order

1. **Rank-5 first** (od_wait ablation, 1 hour): lowest cost, can be done while planning
   the serve-realistic parquet build. If it rejects, no loss; if it accepts, it is a free
   gain that stacks on everything else.

2. **Rank-1** (blend re-opt, 1-2 hours): depends on a serve-realistic parquet for the
   validation / holdout scoring. This is the only experiment that does not require any
   GBDT retraining. If the serve-distribution shift moved the optimal weights significantly,
   this is recoverable within hours.

3. **Rank-3** (window ablation, 45 min cheap filter): trivial to implement, gives a clear
   answer on whether 2006-2012 data is net-positive or net-negative for recent folds.

4. **Rank-2** (co-train, 3-4 hours cheap filter): only if Rank-1 confirms blend weights
   need refreshing AND the window ablation has determined the correct training window.
   Co-training with the wrong window is wasteful.

5. **Rank-4** (HPO, 3.75 hours): run after Rank-2 determines the serve-realistic base
   architecture. HPO on the correct feature distribution and with the fixed CV protocol.
   The two experiments compound (serve-realistic features + HPO-tuned hyperparams).

---

## DO-NOT-RETEST corpus (verified before each entry above)

| Hypothesis class                          | Status                                                                                         |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Training objective re-weighting (I3)      | CLOSED — all variants degrade vs NDCG@3 baseline                                               |
| C4 expected-placement re-rank (I6/phase3) | CLOSED — real holdout REJECT, synthetic-only gain                                              |
| Architecture swap (MLX, Transformer)      | NOT recommended — 2-fold WF failure, requires 21-fold + HPO, speculative                       |
| Condition-aware routing                   | CLOSED — no negative-alpha strata (I7)                                                         |
| Calibration isotonic (phase3)             | CLOSED — f2p LB95 < 0 on all classes/categories                                                |
| JRA HPO on iter9 features (iter13)        | CLOSED at iter9 — novel on iter14 features + fixed CV (Rank-4 above)                           |
| Relevance scheme D/B/C (graded-relevance) | CLOSED for JRA (REJECT WF) and NAR full-system (deploy-judge REJECT); CatBoost B/C also fail   |
| NAR G-1/F1 retrain                        | CLOSED — serve regression confirmed                                                            |
| NAR scheme-D full-system                  | CLOSED — f2p LB95 = -0.00481, deploy REJECT                                                    |
| Per-class base-swap (any)                 | CLOSED — weld pattern; base-swap without co-training always regresses blend                    |
| LGB lambdarank residual-C (iter36)        | CLOSED for NAR (adopted); never attempted for JRA (lgb-lambdarank-transfer-guard docs: REJECT) |
| Signal search (new features)              | JRA: declared exhausted (science-track saturation 2026-06-11, all signal-search ABORTs)        |
