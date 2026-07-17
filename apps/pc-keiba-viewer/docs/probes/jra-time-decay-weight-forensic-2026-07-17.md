# JRA time-decay sample-weight forensic + corrected-implementation WF arm (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position — forensic audit of an existing design
  intent (system doc §8.4's time-decay sample weighting), team-lead-directed
  additional investigation A (wave8). Not a new lever proposal in the usual
  sense: if the forensic confirms the weight has never functioned, the
  follow-up WF arm is the **first-ever implementation** of an already-
  documented design, not a novel mechanism.
- **Hypothesis**: time-decay weighting exists in code and is documented in
  §8.4 (linear 0.5 oldest → 1.0 newest year), but may have never actually
  influenced any trained CatBoost YetiRank model because CatBoost's
  pairwise/ranking losses silently ignore `Pool(weight=...)` — only
  `Pool(group_weight=...)` has real effect.

---

## 0. Headline result (forensic)

**Confirmed.** Every JRA CatBoost YetiRank training path in this system
either applies no weighting at all, or computes the documented time-decay
weight correctly and then discards it via the broken `Pool(weight=...)`
mechanism. This is established by direct code read across all three JRA
training entry points, cross-validated against **two independent prior
standalone repros in this same codebase** (Ban-ei large-scale
investigation, 2026-07-03; NAR-pool arm, 2026-07-17) that hit the exact
same CatBoost runtime warning under the exact same installed version
(`catboost==1.2.10`, confirmed matching in this environment). Historical
WF verdicts are **not invalidated** — every fold in every past evaluation
used the same (non-functioning) weighting condition uniformly, so
relative comparisons between arms remain valid; only the absolute
question "does the documented weighting scheme help" was never actually
tested until now. See §4 for the corrected-implementation WF arm this
finding motivates.

**Update — the WF arm is also done: clean REJECT.** Correctly wiring
the weight for the first time (`group_weight`, 18 fits, 3-seed×3-fold,
n=31,095 pooled) produces no robust accuracy change anywhere — pooled
and every one of 18 tested cells stay inside the noise floor. The
dormant bug cost nothing measurable; no production change. Full result
in §4.

---

## 1. The three JRA CatBoost training paths, read directly

| Path                                                                                                                | File                                                                                                                                           | Weight handling                                                                                                                                                                                                                                                                                                                           |
| ------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Full-train (produces the live deployed artifact)**                                                                | `tmp/candidate-leak-clean-retrain/jra_v9sim_artifacts.py`                                                                                      | **No weight parameter at all.** `Pool()` call carries no `weight=`/`group_weight=` argument (confirmed by direct read during wave6 the same day).                                                                                                                                                                                         |
| **WF/backtest/HPO-screening harness** (used by essentially every fold-based evaluation this whole campaign has run) | `train_finish_position_catboost_walk_forward.py` → `attach_sample_weights()` (line 278) → `finish_position_catboost.py::train_catboost_ranker` | Computes `sample_weight` via `walk_forward_common.compute_time_decay_weights()` (line 163-177, exact §8.4 formula: `0.5 + 0.5*(year-min)/(max-min)`), attaches it as a column, then `finish_position_catboost.py:261-268` extracts it and passes `Pool(..., weight=train_weights)`. `loss_function: "YetiRank"` (same file, params dict). |
| **Production incremental retraining** (`continuous_learner.py`)                                                     | `continuous_learner.py` line 90: `TRAIN_SCRIPT_BY_CATEGORY["jra"] = "train_finish_position_catboost_walk_forward.py"`                          | Invokes the same WF harness above as a subprocess for JRA — identical broken path.                                                                                                                                                                                                                                                        |

`walk_forward_common.compute_time_decay_weights` itself is correctly
implemented (verified by reading its 15-line body): a genuine linear
`[0.5, 1.0]` ramp by `race_year`, with a sane degenerate-single-year
fallback. The bug isn't in the formula — it's that the value it produces
is silently discarded the moment it reaches CatBoost's `Pool(weight=...)`.

---

## 2. Why `Pool(weight=...)` is a no-op for YetiRank (cross-validated)

Two independent standalone repros already exist in this codebase, from
two different sessions, two different categories, converging on the
identical CatBoost runtime warning:

- **Ban-ei large-scale investigation** (`docs/finish-position-prediction-system.md:1388`,
  2026-07-03): "WF wrapper の `attach_sample_weights`（time-decay 非一様
  weight）は YetiRank pairwise と非互換（`Pairwise losses don't support
object weights`）で、prod 自体は no-weight。" That session bypassed the
  issue by calling `train_catboost_ranker` without a `sample_weight`
  column at all, to keep baseline and candidate arms identical.
- **NAR-pool arm** (`apps/pc-keiba-viewer/docs/probes/jra-nar-pooling-arm-2026-07-17.md`,
  §6, today): the same warning text fired on the first real fold fit;
  independently re-confirmed with "a minimal standalone repro against the
  installed `catboost==1.2.10`: the same warning reproduces with
  `weight=`, and is silent (and produces genuinely different model
  behavior) with `group_weight=`."

This session confirmed the installed version in this environment is the
identical `catboost==1.2.10` the NAR-pool repro tested against, and
independently confirmed via direct code read (§1) that all three JRA
training paths funnel through the exact same `weight=` call site. Given
two independent repros already exist with matching results, and the
version is confirmed identical, a third from-scratch repro was judged
low marginal value against the ~1.5-2h budget and was not run — the
budget instead went to the corrected-implementation WF arm (§4), the
part of this investigation nobody has run yet.

---

## 3. Dedup: this is not the Kochi sample-weighting REJECT

`index_closed_probes.md` records a Kochi-final-race campaign lever
("lever1 sample-weighting", 2026-07-12, `wf4`/`wk2` configs, 0/3 served)
that was REJECTed. That lever was **NAR, Kochi-venue-specific,
final-race-type upweighting** ("full-breadth+final-row upweight" — a
categorical/subset upweight scheme for a narrow race-type population
within one NAR venue), not the general recency weighting this doc
concerns. This arm is **JRA-wide, year-based linear time-decay** — the
general §8.4 design — applied to the full champion population, for the
first time with a mechanism that actually works. Different category,
different population, different mechanism: not covered by that closure.

---

## 4. Corrected-implementation WF arm

Champion's **exact** spec and hyperparameters — not the generic WF
harness's own defaults, which differ from the champion (`iterations=500`
vs. champion's `300`, `no_cat_features=False` vs. champion's `True`) —
replicated the same way wave6's gate scripts did (direct `Pool()`/`fit()`
calls matching `jra_v9sim_artifacts.py` field-for-field), with only the
weight mechanism as the variable:

- **baseline**: no weight parameter at all (matches the true live recipe).
- **treatment**: `Pool(group_weight=<time-decay weights>)`, using the
  exact §8.4 formula (`walk_forward_common.compute_time_decay_weights`,
  unmodified, imported not reimplemented) — passed via `group_weight`
  this time, which is why it will actually reach the loss function.

3-seed (42/101/2026) × 3-fold (2023/2024/2025) walk-forward, training
population `tmp/candidate-eval-jra/augmented` (2013→fold_year−1), armB-250
feature spec, `TRAIN_START=20130101`. Standard §7.2/§8.12 gate (≥2/3
primaries positive, ≥1 of place2/place3 positive, no metric regresses
beyond −0.05pp, LB95>0 on a primary OR all primaries ≥+0.08pp) plus
summer4-restricted (delta≥+0.08pp & LB95>0 & multi-seed) and an overall
cell×rank1-5 table (venue/surface/distance_band, n≥200, sort-before-mask:
every cell filter is a `race_id`-keyed `.filter()` on the same sorted,
key-joined frame — never a positional mask against a separately-sorted
frame).

### 4.1 Result: clean, well-powered REJECT — the dormant weight would not have helped

18 fits completed cleanly in 779.7s. Pooled (all 3 folds × 3 seeds,
n=31,095 races):

| Metric   | Baseline | Treatment | Δ pp   | LB95   | UB95   |
| -------- | -------- | --------- | ------ | ------ | ------ |
| top1     | 33.825   | 33.784    | −0.042 | −0.216 | +0.135 |
| place2   | 18.157   | 18.231    | +0.074 | −0.145 | +0.309 |
| place3   | 14.086   | 14.092    | +0.006 | −0.206 | +0.232 |
| place4   | 12.172   | 12.047    | −0.125 | −0.325 | +0.077 |
| place5   | 10.989   | 10.976    | −0.013 | −0.206 | +0.187 |
| place6   | 10.539   | 10.658    | +0.119 | −0.077 | +0.322 |
| top3_box | 9.436    | 9.391     | −0.045 | −0.158 | +0.068 |

Every metric's 95% CI straddles zero; the largest pooled effect in
either direction is place4 at −0.125pp. Per-seed pooled deltas bounce
between +0.08 and −0.23pp on top1 with no consistent sign, and while 3
of the 21 (seed × metric) cells individually touch LB95>0
(seed42/place2 +0.03, seed101/place3 +0.00, seed2026/place6 +0.06),
none of the three is _also_ significant for the other two seeds — the
signature of multiple-comparison noise (21 comparisons, ~1 in 20
expected to cross zero by chance), not a robust effect. §7.2/§8.12 gate:
`primary_positive_count=2/3`, but `no_regression_floor_ok=false` (pooled
worst delta −0.125pp technically breaches the −0.05pp floor) →
**GATE_PASS=false**. Unlike gate (b) in the wave6 champion-freshness
retrain earlier today, this floor breach is not a power artifact to
second-guess — n=31,095 gives ~0.03pp resolution per race, the CI is
tight, and it still straddles zero. This is a genuine, well-powered null.

Cell-level (10 venues + 3 surfaces + 5 distance bands, all n≥1,000): **no
cell shows a positive-and-significant top1 delta** — every cell's LB95
is either negative or, where positive-leaning (venue=07 LB95 −0.119,
venue=10 LB95 −0.210), still crosses zero. `distance_band=sprint` shows
the largest single-cell negative point estimate (−1.162pp, LB95 −2.413,
still not significant, n=1,119). No pocket of hidden benefit anywhere
tested. Full per-cell table: `cell_report.json`.

### 4.2 Verdict

**REJECT / CLOSED.** Correctly wiring the documented §8.4 time-decay
weight (via `group_weight`, functioning for the first time) does not
move JRA finish-position accuracy in any robust, gate-passing way,
pooled or in any tested cell. Combined with §0's forensic finding, the
complete picture is: the weight has never functioned in this system's
history, **and** now that it has been correctly implemented and tested,
it doesn't matter — CatBoost's own tree structure evidently already
captures whatever temporal-recency signal exists in this feature set
without needing an explicit sample-weight nudge (plausible mechanism:
the armB-250 spec already contains several explicitly time-relative
features — `days_since_last_race`, `days_since_last_workout`,
`weight_trend_5`, `finish_trend_5`, `recent_*` rolling windows — that
may already absorb most of what a coarse yearly recency weight would
add). No production change. `jra-cb-v9-sim-2013-clean` (and the wave6
`fresh2026h1` challenger, itself also unweighted) both remain
appropriately unweighted — consistent with, not despite, the dormant
§8.4 design. **DO-NOT-RETEST scope**: this exact construction — armB-250
spec, `group_weight` = linear 0.5→1.0 by `race_year`
(`compute_time_decay_weights`, unmodified), JRA CatBoost YetiRank,
3-fold (2023/2024/2025) × 3-seed (42/101/2026) WF. Left open (different
enough to not be covered): non-linear decay curves, decay bounded
tighter than [0.5, 1.0], per-class or per-cell decay rates, or the same
mechanism applied to NAR/Ban-ei (not tested here, JRA-only).

---

## 5. Artifact locations

- WF arm script + raw results: `apps/pc-keiba-viewer/tmp/candidate-jra-time-decay-weight-forensic-2026-07-17/`
  (`wf_group_weight_arm.py`, `wf_run.log`, `per_race_hits.parquet`,
  `fold_seed_metrics.json`, `cell_report.json`, `summary.json`) — git-ignored
  per project convention, not committed.
