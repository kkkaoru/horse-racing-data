# JRA CatBoost HPO campaign — armB-250 leak-clean spec (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position — full CatBoost hyperparameter/architecture
  HPO campaign against the live champion, team-lead-directed. First HPO run
  on this model family since the 2026-07-04 leak-clean fix.
- **Outcome: REJECT, clean and well-powered.** A 40-trial Optuna TPE search
  over a 9-dimensional CatBoost hyperparameter/architecture space (including
  `loss_function` and `grow_policy`, neither of which the last CatBoost HPO
  campaign on this family searched) found fold-B/2024 "winners" that
  **uniformly failed** an independent fold-A/2023 cross-check (10/10
  candidates non-positive) and **failed** the pre-registered fold-C/2025
  blind confirm on the project's standing accept gate. This closes broad
  CatBoost hyperparameter/architecture tuning as a lever for the clean
  champion spec — a direct replication of iter20's HPO-selection-bias
  finding (2026-06-17, old pre-leak-clean spec, narrower search space), now
  independently reconfirmed on the new leak-clean armB-250 spec with a much
  broader search. **MLflow run**: `2882e32ee6d246a0afcfe18dcc5ca8c4`
  (`finish-position/wf-eval`, `model_version=jra-cb-hpo-2026-07-17-reject`).

---

## 0. Headline result

Two configs reached the pre-registered fold-C/2025 blind-confirm stage
(`trial_26`, `trial_30` — the fold-B/2024 co-winners). Both **REJECT**.
`trial_30`, the stronger of the two, gets a directionally positive pooled
top1 (+0.42pp) but its 95% bootstrap CI lower bound does not clear zero
(LB95 = −0.039pp), and four other gated metrics (place3, place4, place5,
top3*box) breach the project's −0.05pp no-regression floor. `trial_26` is
weaker still and fails the primary-positive gate condition outright. **0 of 3 seeds independently
clear LB95 > 0 on top1 for either config**, and the venue×surface×
distance_band cell scan (29 cells, 11 with n≥200) produced zero adopting
cells under the full §8.12 multi-metric gate. The pivotal evidence, caught
\_before* fold-C compute was spent, was a fold-A/2023 cross-check: every one
of the top 10 fold-B/2024 performers — spanning both loss functions in the
search space — underperformed or exactly tied the champion on a blind year
none of them were ever optimized against (deltas 0.000pp to −0.579pp, zero
exceptions). This is the textbook HPO-selection-bias signature
(`feedback_hpo_selection_bias_blind_holdout`) and it reproduced cleanly on
the first attempt with this new spec.

---

## 1. Dedup / precedent check

The only prior CatBoost HPO on this model family is **iter20**
(2026-06-17, `docs/finish-position-accuracy/history/goal-jra-iter20-hpo.md`),
run on the **old pre-leak-clean 244-feature spec** with a narrower search
space (`depth`/`learning_rate`/`l2_leaf_reg`/`od_wait`/`random_strength`/
`bagging_temperature`, `iterations` fixed at 1000, no `loss_function` or
`grow_policy` axis). That campaign's own winning config **failed
independent blind-holdout confirmation due to selection bias**
(`feedback_hpo_selection_bias_blind_holdout`), concluding "champion
hyperparams near-optimal, HPO lever low EV."

This campaign is not an exact duplicate — different feature spec (armB-250,
leak-clean, vs. iter20's 244-feature pre-clean spec), and a materially
broader search space (adds `loss_function` and `grow_policy` as axes,
varies `iterations` instead of fixing it) — but it is a directly relevant
precedent. It turned out to **replicate iter20's finding almost exactly**,
just on the new spec and with more search breadth behind it. Combined with
this campaign's own result, HPO/architecture tuning on this model family
has now failed independent blind confirmation **twice**, on two different
feature specs, with two different search-space breadths.

---

## 2. Protocol, as pre-registered

### 2.1 Champion recipe pin

Verified by direct read of the live recipe script
(`tmp/candidate-leak-clean-retrain/jra_v9sim_artifacts.py`): CatBoost
YetiRank, `iterations=300`, `depth=8`, `learning_rate=0.05`,
`l2_leaf_reg=3.0`, `cat_indices=[]` — i.e. **zero categorical features**,
despite the general project convention naming 4 columns (`keibajo_code`/
`track_code`/`grade_code`/`umaban`) as categoricals elsewhere (§8.2 of the
system doc); this specific clean recipe treats all of them as plain floats.
`border_count=254`. This exact recipe is `trial 0` in every study below
(`study.enqueue_trial(..., skip_if_exists=True)`), giving a same-harness
apples-to-apples baseline reading rather than relying on a previously
cached number.

### 2.2 Feature spec

armB-250, the leak-free feature spec
(`tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`, key
`"armB"`) — the same spec the live champion trains on.

### 2.3 Fold definitions (harness: `hpo_common.py`)

| Fold | Train window             | Blind year | Role                                         |
| ---- | ------------------------ | ---------- | -------------------------------------------- |
| A    | `race_date` ≤ 2022-12-31 | 2023       | Cross-check (never touched during selection) |
| B    | `race_date` ≤ 2023-12-31 | 2024       | Selection (Optuna objective)                 |
| C    | `race_date` ≤ 2024-12-31 | 2025       | Reserved exclusively for final blind confirm |

Relevance labels 3/2/1/0 for finish rank 1/2/3/other, matching the live
recipe exactly.

### 2.4 Search space (Optuna TPE, seed `20260717`)

`iterations` [300, 2000] · `depth` [6, 10] · `learning_rate` [0.02, 0.10]
(log) · `l2_leaf_reg` [1, 10] · `random_strength` [1e-3, 10] (log) ·
`bagging_temperature` [0, 5] (forces `bootstrap_type=Bayesian`) ·
`grow_policy` ∈ {SymmetricTree, Depthwise, Lossguide} (+ `max_leaves`
[16, 64] when Lossguide) · `loss_function` ∈ {YetiRank,
`StochasticRank:metric=NDCG;top=3`, YetiRankPairwise — dropped after trial
11, see §3} · `border_count` [32, 255]. `cat_indices=[]` fixed throughout,
byte-identical to the live recipe.

### 2.5 Selection → cross-check → blind-confirm pipeline

Three scripts (`hpo_selection.py`, `hpo_blind_confirm.py`,
`hpo_gate_check.py`, sharing `hpo_common.py`), matching
docs/finish-position-prediction-system.md §7.3's standing rule that
**HPO requires a separate blind holdout for confirmation** (selection-bias
protection) on top of the usual WF fold convention, and its sort-before-mask
harness discipline (§7.3, last bullet) — `hpo_gate_check.py`'s cell scan
builds on `hc.stack_seed_joins`, which sorts both sides by `race_id` before
joining, so cell membership is a pure boolean filter on an already-safely-
paired frame, never a positional re-zip.

---

## 3. Operational finding: `YetiRankPairwise` does not scale on this data

Worth documenting on its own since it is a reusable fact for any future
CatBoost HPO on this dataset, not specific to this campaign's verdict.

`YetiRankPairwise` was found to be dramatically more expensive than the
other two loss functions: **a single fold-A fit measured 665 seconds
(11 minutes)**, versus a 20–40s baseline for `YetiRank`/`StochasticRank` per
`hpo_selection.py`'s own module docstring (directly consistent with the
actually-recorded per-trial `fit_seconds` for the 9 successful trials in the
initial two-fold-mode study, which ranged ~6.5s–73.6s across both losses and
all three `grow_policy` values). The cause: pairwise-mode loss scales with
per-race horse-pair count, and this dataset has large race groups (up to
~18 horses) across tens of thousands of races — a wall-clock/cost problem,
**not a quality or correctness issue**. `YetiRankPairwise` was dropped from
the search space after trial 11 for this reason.

A separate, earlier issue was also caught at trial 9: CatBoost hard-rejects
pairwise-mode losses on nonsymmetric trees on CPU (`catboost/private/libs/
options/catboost_options.cpp:764`: _"Pairwise mode is not supported for
nonsymmetric trees on CPU"_). This required forcing `grow_policy=
SymmetricTree` whenever `loss_function=YetiRankPairwise` was sampled — a
different failure mode than the cost issue, confirmed to be
`YetiRankPairwise`-specific only (`StochasticRank:metric=NDCG;top=3` fit
cleanly under both `Depthwise` and `Lossguide` in a standalone smoke test,
corroborated by trials 4/6/8 of the same study, which had already completed
with that exact combination before the fix existed).

**Takeaway for future CatBoost HPO on this dataset: exclude
`YetiRankPairwise` from the search space up front**, or budget well over an
order of magnitude more wall-clock per trial than `YetiRank`/
`StochasticRank` if it must be included (665s vs. a 20–40s typical range
per the docstring, i.e. roughly 17–33×; up to ~100× versus the single
fastest non-pairwise fit actually observed, 6.5s at trial 4).

---

## 4. Fold-B/2024 selection (40 trials)

Given real per-trial cost running ~3× slower than a smoke-test estimate
(compounded by other teams' concurrent training jobs on the same Mac) and a
tightened team-lead deadline mid-campaign, selection switched from fitting
both fold-A+fold-B per trial (2 fits/trial, objective = mean of the two
years' top1) to a faster fold-B-only screening mode (`--fold-b-only`,
objective = top1 on blind-2024 alone, 1 fit/trial) for the bulk of the
search. This required a **separate Optuna study**
(`jra-clean-hpo-2026-07-17-foldb-only`, vs. the original
`jra-clean-hpo-2026-07-17`, both in the same
`tmp/hpo-catboost-2026-07-17/optuna_study.db`) because Optuna hard-errors if
a categorical parameter's choice set changes mid-study — dropping
`YetiRankPairwise` from the 3-choice `loss_function` list would have
conflicted with trials already recorded under the old 3-choice distribution.

The original two-fold-mode study ran 13 trials total (trial 0 = pinned
baseline at pooled mean(top1_2023, top1_2024) = 34.15%; trials 1–8
completed normally, all within roughly ±0.3pp of baseline; trials 9–12
failed/were abandoned around the `YetiRankPairwise` issues in §3). The
fold-B-only study then ran **40 trials** (trial 0 = pinned baseline,
trials 1–39 = TPE-sampled) in well under 15 minutes of total wall-clock
training time.

**Baseline (trial 0, fold-B-only study): top1 = 34.71%** (blind 2024).

Top 15 of 40 trials by fold-B/2024 top1:

| Trial | Δpp vs. baseline | loss_function         | grow_policy   | depth | iterations | lr     | l2   |
| ----- | ---------------- | --------------------- | ------------- | ----- | ---------- | ------ | ---- |
| 26    | +0.318           | StochasticRank:NDCG@3 | SymmetricTree | 7     | 731        | 0.0215 | 5.31 |
| 30    | +0.318           | StochasticRank:NDCG@3 | SymmetricTree | 8     | 1127       | 0.0296 | 3.93 |
| 1     | +0.261           | StochasticRank:NDCG@3 | Depthwise     | 7     | 563        | 0.0286 | 5.64 |
| 21    | +0.261           | StochasticRank:NDCG@3 | SymmetricTree | 6     | 1036       | 0.0321 | 8.27 |
| 28    | +0.232           | StochasticRank:NDCG@3 | SymmetricTree | 6     | 1615       | 0.0326 | 9.82 |
| 11    | +0.174           | StochasticRank:NDCG@3 | Depthwise     | 7     | 529        | 0.0230 | 8.61 |
| 27    | +0.174           | StochasticRank:NDCG@3 | SymmetricTree | 6     | 414        | 0.0225 | 2.87 |
| 32    | +0.174           | StochasticRank:NDCG@3 | SymmetricTree | 7     | 979        | 0.0216 | 5.06 |
| 14    | +0.145           | StochasticRank:NDCG@3 | SymmetricTree | 6     | 935        | 0.0345 | 7.46 |
| 19    | +0.087           | YetiRank              | Depthwise     | 6     | 991        | 0.0277 | 4.55 |
| 22    | +0.087           | StochasticRank:NDCG@3 | Depthwise     | 7     | 440        | 0.0341 | 3.78 |
| 37    | +0.087           | StochasticRank:NDCG@3 | SymmetricTree | 7     | 1861       | 0.0273 | 3.63 |
| 25    | +0.058           | YetiRank              | SymmetricTree | 7     | 1479       | 0.0283 | 6.82 |
| 29    | +0.058           | YetiRank              | Lossguide     | 7     | 313        | 0.0224 | 4.89 |
| 31    | +0.058           | StochasticRank:NDCG@3 | SymmetricTree | 8     | 948        | 0.0417 | 4.20 |

**Pattern**: 12 of the top 15 use `StochasticRank:metric=NDCG;top=3`
(the champion uses `YetiRank`). Where `StochasticRank` wins, there is a
consistent lower-learning-rate / higher-`l2_leaf_reg` signature versus the
champion's own `lr=0.05, l2=3.0` (top performers cluster around
`lr≈0.02–0.03`, `l2≈4–10`). Best fold-B/2024 result: `trial_26` and
`trial_30` tied at top1 = 35.03% (+0.32pp vs. baseline).

---

## 5. Fold-A/2023 cross-check — the pivotal step

Before spending fold-C compute, the top 3 shortlisted configs
(`trial_26`, `trial_30`, `trial_1`) plus 7 more diverse fold-B performers
(`trial_21`, `trial_28`, `trial_11`, `trial_19`, `trial_25`, `trial_29`,
`trial_31` — **3 of which use the champion's own `YetiRank` loss**, not
`StochasticRank`, specifically to check whether the pattern was
loss-function-specific) were fit fresh on fold-A's training window
(≤2022) and evaluated on blind-2023 — a year **none of them were ever
optimized against**.

**Result: 10 of 10 candidates underperformed or exactly tied the champion
baseline on fold-A/2023** (champion baseline top1 = 33.594%, n=3,456
races), despite every one of them showing a positive delta on the
fold-B/2024 they were selected against:

| Trial | loss_function         | Fold-B/2024 Δpp (selection) | Fold-A/2023 Δpp (blind cross-check) | Fold-A LB95 |
| ----- | --------------------- | --------------------------- | ----------------------------------- | ----------- |
| 26    | StochasticRank:NDCG@3 | +0.318                      | **−0.579**                          | −1.331      |
| 30    | StochasticRank:NDCG@3 | +0.318                      | **−0.231**                          | −0.927      |
| 1     | StochasticRank:NDCG@3 | +0.261                      | **−0.174**                          | −0.926      |
| 21    | StochasticRank:NDCG@3 | +0.261                      | **−0.174**                          | −0.927      |
| 28    | StochasticRank:NDCG@3 | +0.232                      | **−0.550**                          | −1.331      |
| 11    | StochasticRank:NDCG@3 | +0.174                      | **−0.405**                          | −1.128      |
| 19    | YetiRank              | +0.087                      | **0.000**                           | −0.723      |
| 25    | YetiRank              | +0.058                      | **−0.579**                          | −1.331      |
| 29    | YetiRank              | +0.058                      | **−0.318**                          | −1.042      |
| 31    | StochasticRank:NDCG@3 | +0.058                      | **−0.058**                          | −0.810      |

Deltas range **0.000pp to −0.579pp — zero exceptions, unanimous
negative-or-flat direction**, and every candidate's LB95 sits well below
zero. The `YetiRank`-loss candidates (19, 25, 29) show exactly the same
failure pattern as the `StochasticRank` candidates, confirming the
fold-B "win" was not loss-function-specific — it is fold-B/2024-specific,
the classic signature of overfitting to the single blind year used for
selection.

This is a clean, well-powered replication of the exact HPO
selection-bias failure mode `feedback_hpo_selection_bias_blind_holdout`
documents from iter20's history, now independently reconfirmed on the new
leak-clean spec with a much broader search space.

---

## 6. Fold-C/2025 blind confirm + gate application

Per protocol, rather than stop on the fold-A interim look, the top 2
fold-B survivors (`trial_26`, `trial_30`) were still run through the full
pre-registered confirmation: **3 seeds each** (20260717/18/19), fit fresh
on fold-C's training window (≤2024, 579,301 rows / 41,452 races), scored
against blind-2025 (47,497 rows / 3,455 races), **paired-bootstrapped**
(2,000 resamples, `race_id`-sorted joins throughout) against the cached,
already-trusted live-champion fold-2025 model
(`tmp/candidate-leak-clean-retrain/models_jra_v9sim/armB/fold-2025/model.json`,
never retrained, loaded directly). Gate applied per
docs/finish-position-prediction-system.md §7.2/§8.12 exactly (see
`hpo_gate_check.py::apply_gate`'s docstring for how §7.2's loose `>0`
reading and §8.12's stricter `≥+0.08pp` reading are both checked at once).

Champion baseline (fold-C/2025, n=3,455): **top1 = 32.996%**, place2 =
18.205%, place3 = 13.835%.

### 6.1 Pooled 3-seed gate (n = 10,365 = 3 seeds × 3,455 races, stacked)

**`trial_26`** — `StochasticRank:NDCG@3`, depth=7, iterations=731,
lr=0.0215, l2=5.31 — **verdict: REJECT**

| Metric   | Champion | Candidate | Δpp        | LB95pp | No-regression (≥−0.05pp) |
| -------- | -------- | --------- | ---------- | ------ | ------------------------ |
| top1     | 32.996   | 33.121    | +0.125     | −0.338 | pass                     |
| place2   | 18.205   | 18.186    | **−0.019** | −0.540 | pass                     |
| place3   | 13.835   | 13.536    | −0.299     | −0.772 | **fail**                 |
| place4   | 12.069   | 11.645    | −0.425     | −0.907 | **fail**                 |
| place5   | 11.635   | 10.950    | −0.685     | −1.177 | **fail**                 |
| place6   | 9.899    | 10.169    | +0.270     | −0.222 | pass                     |
| top3_box | 9.465    | 9.320     | −0.145     | −0.415 | **fail**                 |
| ndcg3    | 54.759   | 54.651    | −0.108     | −0.302 | —                        |

Gate1 (≥2 of {top1,place2,place3} positive) = **FALSE** (only top1 is
positive — place2 is a slight negative) — fails the loosest gate condition
outright, before regression or significance are even considered.

**`trial_30`** — `StochasticRank:NDCG@3`, depth=8, iterations=1127,
lr=0.0296, l2=3.93 — **verdict: REJECT**

| Metric   | Champion | Candidate | Δpp    | LB95pp     | No-regression (≥−0.05pp) |
| -------- | -------- | --------- | ------ | ---------- | ------------------------ |
| top1     | 32.996   | 33.420    | +0.425 | **−0.039** | pass                     |
| place2   | 18.205   | 18.649    | +0.444 | −0.039     | pass                     |
| place3   | 13.835   | 13.526    | −0.309 | −0.781     | **fail**                 |
| place4   | 12.069   | 11.500    | −0.569 | −1.061     | **fail**                 |
| place5   | 11.635   | 10.912    | −0.724 | −1.206     | **fail**                 |
| place6   | 9.899    | 10.130    | +0.232 | −0.241     | pass                     |
| top3_box | 9.465    | 9.262     | −0.203 | −0.473     | **fail**                 |
| ndcg3    | 54.759   | 54.743    | −0.016 | −0.202     | —                        |

`trial_30` clears gate1 (top1 and place2 both positive — 2 of 3) and gate2
(place2 positive), and even gate4 (both are ≥+0.08pp significant). It fails
on **gate3** (place3/place4/place5/top3_box all breach the −0.05pp
no-regression floor) and **gate5** (top1's LB95 = −0.039pp does not clear
zero, and not all 7 gated metrics are significant either) — REJECT.

### 6.2 Per-seed consistency

**0 of 3 seeds independently clear LB95 > 0 on top1, for either config:**

| Config   | Seed 20260717           | Seed 20260718           | Seed 20260719           |
| -------- | ----------------------- | ----------------------- | ----------------------- |
| trial_26 | Δ −0.145pp, LB95 −0.955 | Δ +0.289pp, LB95 −0.521 | Δ +0.232pp, LB95 −0.550 |
| trial_30 | Δ +0.347pp, LB95 −0.406 | Δ +0.260pp, LB95 −0.492 | Δ +0.666pp, LB95 −0.116 |

`trial_30`'s pooled top1 LB95 (−0.039pp) is close to zero only because
pooling 3 seeds' worth of observations narrows the interval — no individual
seed on its own would have passed. `trial_26` is directionally inconsistent
across seeds (one negative, two positive on top1).

### 6.3 Summer-4-venues restricted recheck (n = 2,376: Sapporo/Hakodate/Fukushima/Kokura)

Both configs are **negative on place2** in this restricted population:

| Config   | top1 Δpp (LB95) | place2 Δpp (LB95)   | place3 Δpp (LB95) | gate3 no-regression |
| -------- | --------------- | ------------------- | ----------------- | ------------------- |
| trial_26 | +0.126 (−0.842) | **−0.463** (−1.557) | +0.168 (−0.926)   | fail                |
| trial_30 | +0.042 (−0.884) | **−0.379** (−1.473) | +0.168 (−0.926)   | fail                |

Both REJECT in this subset too (gate3 fails for both; `trial_30` also fails
gate4 here).

### 6.4 Cell scan (venue × surface × distance_band × class, n≥100 reported)

29 cells reported for each config (18 flagged low-power, n<200; 11 with
n≥200). Directional top1 counts: `trial_26` 16 positive / 9 negative / 4
flat; `trial_30` 18 positive / 7 negative / 4 flat — a mild positive lean
on the point estimate alone, but this is exactly the population where
per-cell noise is largest (most cells n<300).

**Zero cells adopt** under the full §8.12 multi-metric + no-regression
gate for either config. The single high-power (n≥200) cell with a
statistically significant top1 improvement — venue=08 (Kyoto), dirt,
mile, n=276 — illustrates why: `trial_30` shows top1 +4.71pp (LB95
+1.09, genuinely significant) and place2 +0.36pp (positive, not
significant), but **place4 collapses −4.35pp (LB95 −7.97)**, a clear
regression that fails gate3 outright regardless of top1's strength
(`trial_26`'s same cell: top1 +3.62pp LB95 +0.36, place4 identically
−4.35pp LB95 −7.97). No cell anywhere in the scan combines a significant
primary-metric improvement with a clean no-regression sweep.

### 6.5 Ensemble reading (mean of 3 seeds' predictions — informational only, not a binding gate)

For completeness: averaging the 3 seeds' predicted scores before ranking
(rather than treating each seed as an independent paired sample) gives
`trial_26` top1 +0.174pp (LB95 −0.608) and `trial_30` top1 +0.376pp
(LB95 −0.405) — both still fail to clear LB95>0, consistent with the
primary pooled-per-seed reading in §6.1. This reading is explicitly
informational per the harness (`gate_informational_not_binding`), not a
second independent gate.

---

## 7. MLflow record

Logged via `log-training-run` (`hr-mlflow-training-run/v1` manifest,
`register: false`, `champion: false` — this is a rejected research
artifact, not a deployable candidate):

- **Run ID: `2882e32ee6d246a0afcfe18dcc5ca8c4`**, experiment
  `finish-position/wf-eval`, run name `jra-cb-hpo-2026-07-17-reject`,
  status `FINISHED`.
- `aggregate_metrics`: `trial_30`'s fold-C pooled 3-seed metrics as 0–1
  fractions (`top1_accuracy=0.3342`, `place2_accuracy=0.1865`,
  `place3_accuracy=0.1353`, plus place4–6/top3_box/ndcg3), alongside the
  `_delta_pp`/`_lb95_pp` diagnostic metrics in their natural
  percentage-point scale (matching this same experiment's own existing
  convention, e.g. the `jra-cb-v9-sim-2013-clean-fresh2026h1` run's
  `overall_top1_delta_pp`) plus fold-A cross-check and cell-scan summary
  counts.
- `params`: `seed_count=3`, `folds=A+B+C`, `n_trials=40`,
  `best_config_label=trial_30` and its full hyperparameter set, the
  champion baseline hyperparameters for direct comparison, the search
  space, and a `precedent` pointer to iter20.
- `tags`: `campaign=2026-07-17-summer4`,
  `baseline_model=jra-cb-v9-sim-2013-clean`,
  `verdict=reject-selection-bias-confirmed`,
  `stage=jra-clean-hpo-2026-07-17-closed`,
  `run_kind=catboost-hpo-architecture-search`, `doc=<this file's path>`,
  and a one-sentence `note` summarizing the headline finding.
- `cell_report`: **not** `gate_report.json` directly — that file's
  top-level shape (`{meta, champion_baseline, configs}`, with nested
  per-seed/per-gate objects) is not a flat table and
  `pd.DataFrame(json.loads(gate_report.json))` raises `ValueError: Mixing
dicts with non-Series may lead to ambiguous ordering` when fed to the
  ingester's `read_cell_table`/`normalize_cell_dataframe` path (confirmed
  by direct test before touching the real store, not assumed). Instead,
  `trial_30`'s 29-row cell scan was flattened to a proper tabular file
  (`cell_report_trial30_flat.json`: `category`/`venue`/`surface`/
  `distance_band`/`class_code`/`race_count` + `{top1,place2,place3,
place4,place5}_{delta_pp,lb95_pp}` columns) and attached as
  `cell_report` — this ingested cleanly and produced the run's
  `overall_*` headline metrics (e.g. `overall_top1_delta_pp=1.284`, the
  unweighted-by-significance race-count-weighted mean cell delta). The
  full nested `gate_report.json` (pooled/per-seed/summer4/ensemble/cell
  detail for both configs) remains the authoritative source and its path
  is documented here and in this doc; only a derived flat subset is
  representable through the tabular `cell_report` mechanism.
- **Durability verified** by independent read-back through a fresh
  `MlflowClient(tracking_uri=config.get_tracking_uri())` built after
  `config.load_dotenv_local()` + `config.load_repo_root_env_fallback()`
  (never a bare `MlflowClient()`), confirming the tracking URI scheme is
  `postgresql` (the real Neon-backed store, not the frozen sqlite from the
  2026-07-08 phantom-run incident) _before_ trusting the write, then
  fetching the run back and confirming all 35 metrics (25 sent via
  `aggregate_metrics` + 10 `overall_*` metrics auto-derived from
  `cell_report` ingestion) and 23 params landed byte-for-byte as sent,
  all 7 manifest tags plus MLflow's own 6 auto-added identity/routing
  tags (13 total) present and correct, `status=FINISHED`, correctly
  routed to `finish-position/wf-eval`.

---

## 8. Verdict and closing

**REJECT — DO-NOT-RETEST.** Broad CatBoost hyperparameter/architecture
tuning (9-dimensional space including `loss_function` and `grow_policy`,
40 Optuna TPE trials, `iterations` free up to 2000) against the current
leak-clean armB-250 JRA champion spec is a closed, low-EV lever. The
evidence is unusually clean for a negative result:

1. A blind fold-A/2023 cross-check unanimously rejected all 10 candidates
   that looked like fold-B/2024 winners (0/10 positive, both loss
   functions represented) — this is not a borderline or noisy signal, it
   is a uniform reversal.
2. The two survivors that were still carried through the full
   pre-registered fold-C/2025 blind confirm both failed the project's
   standing accept gate (§7.2/§8.12) on independent, non-overlapping
   grounds (`trial_26`: fails the loosest primary-positive gate;
   `trial_30`: clears the primary-positive/significance gates but fails
   the no-regression floor and the LB95 gate).
3. Per-seed consistency is 0/3 for both configs — the pooled top1 LB95
   crossing close to zero for `trial_30` is a pooling artifact of stacking
   3 seeds, not evidence any single seed would independently pass.
4. The cell scan (29 cells) produced zero adopting cells under the
   project's own multi-metric + no-regression cell-adoption gate, including
   the one high-power cell with a significant top1 win (place4 regresses
   −4.35pp in that same cell for both configs).
5. This is now the **second** independent HPO/architecture campaign on
   this model family to fail blind confirmation via the selection-bias
   mechanism (`feedback_hpo_selection_bias_blind_holdout`) — iter20
   (old spec, narrower space) and this campaign (new leak-clean spec,
   much broader space) — reinforcing rather than merely repeating the
   prior conclusion.

This doc's own verdict is citable as a DO-NOT-RETEST entry for
`index_closed_probes.md`-style indexing (not edited here — maintained
under single-editor discipline by team-lead/another agent per this
project's convention): **broad CatBoost HPO/architecture search on the
JRA leak-clean champion spec, 2026-07-17, REJECT, selection-bias
replication of iter20, MLflow run `2882e32ee6d246a0afcfe18dcc5ca8c4`.**
The one durable, reusable finding this campaign leaves behind that is
_not_ about the reject verdict is the `YetiRankPairwise` cost/incompatibility
fact in §3 — worth checking before any future CatBoost HPO on this dataset
considers including pairwise-mode losses.

---

## 9. Artifact locations

All under `apps/pc-keiba-viewer/tmp/hpo-catboost-2026-07-17/` (gitignored
scratch, not committed):

- Harness: `hpo_common.py`, `hpo_selection.py`, `hpo_blind_confirm.py`,
  `hpo_gate_check.py`, `shortlist_top_configs.py`.
- Optuna studies: `optuna_study.db` (sqlite; study names
  `jra-clean-hpo-2026-07-17` and `jra-clean-hpo-2026-07-17-foldb-only`),
  `selection_trials.jsonl` (raw per-trial records, both studies).
- Selection output: `shortlist.json` (top-3 fold-B configs).
- Fold-A cross-check: `foldA_confirm.json` (top-3 shortlist),
  `foldA_confirm_extra.json` (7 more diverse configs),
  `extra_foldA_configs.json` (the 7 configs' param definitions).
- Fold-C blind confirm: `blind_confirm_configs.json` (the 2 configs
  carried forward), `blind_confirm_report.json` (raw per-seed + per-race
  results, ~11MB), `gate_report.json` (the formal gate application —
  pooled/per-seed/summer4/ensemble/cell-scan, both configs — the
  authoritative source for every number in §6 of this doc).
- MLflow manifest + derived flat cell table used for §7:
  `mlflow_manifest.json`, `cell_report_trial30_flat.json`.
