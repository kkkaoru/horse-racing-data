---
probe: jra-longshot-top3-detector
date: 2026-07-17
category: jra
method: per-horse binary classifier flagging market-undervalued (longshot) horses likely to finish top-3, evaluated as a detection/lift task with its own pre-registered gate — NOT a ranking-accuracy lever
status: PRE-REGISTERED — design + gate fixed before any training; results pending
---

# JRA Longshot Top-3 Detector (2026-07-17)

## 0. Task and why this is a different kind of lever

USER instruction (2026-07-17, relayed via orchestrator): "現在の予測とオッズや人気順
通りではない、1,2,3着に入った不人気馬が予測で見つけられるようにしてください。手法は
問いません" — build something that surfaces unpopular (low-market-rank) horses that
actually finish top-3, method-agnostic.

**This is explicitly not a ranking-change lever.** The champion's own predicted
rank is left untouched. The deliverable is a second, independent signal — "how
likely is this specific longshot to hit the board" — that could in principle be
surfaced as a separate flag/badge, not a reordering of the model's own picks.
Evaluation is therefore a **detection/lift task** (does the flag concentrate top-3
outcomes inside the longshot population better than chance), not the §7.2
ranking-accuracy gate (which asks whether `predicted_rank` itself got better).

### 0.1 Dedup — why the closed rank-changing REJECTs don't apply here

- **E-top2 override** (`docs/finish-position-accuracy/per-class/jra/etop2-deploy.md`):
  swaps CatBoost's own rank-1/rank-2 pick when a second XGB model agrees on a
  different rank-1 — this **changes `predicted_rank`** (was ADOPTed 2026-06-18,
  later superseded by the v9-sim retrain). Different mechanism and different
  output (a rank swap vs. an additive flag) from this task.
- **Confidence-shrinkage** (`tmp/confidence-shrinkage/`, lever_bank.md
  Confirmed-dead table): shrinks the #1 pick's within-race z-margin by a factor
  `k` in gated Sapporo/Hakodate + inner-waku races — **also changes the model's
  effective score/rank**, REJECTed (`REJECT_no_k_clears_selection`). Different
  mechanism.
- **Dynamic-blend** (`tmp/candidate-dynamic-blend/`): per-race dynamic score
  blending across models — again a **score/rank-level intervention**.
- **`project_place_improvement_infeasible`** (memory, 2026-05-20): this closed
  avenue is about improving **exact-ordinal place2/place3 accuracy** (making
  `predicted_rank==2` land on `finish_position==2` more often) via hierarchical/
  specialist/blend architectures that all tried to **replace or correct the
  champion's own rank-2/rank-3 pick** — every variant underperformed standalone
  and diluted the champion. That is a different question from this task: this
  detector does not try to correctly _order_ the field or improve exact-ordinal
  place accuracy at all. It asks a narrower, additive question — "of the horses
  the market has already written off, which ones have elevated hit odds" — and
  is evaluated purely on lift within that subpopulation, never on `predicted_rank`
  quality. A detector that never touches the champion's rank cannot suffer the
  specialist-dilution failure mode that killed every place-improvement attempt.

None of the closed docs evaluate a detection/lift task; all of them evaluate
`predicted_rank` quality under §7.2. This is why a fresh pre-registered gate
(§3) is used instead of §7.2.

## 1. Label definition (pre-registered)

**Longshot population**: `tansho_ninkijun >= 7`.

Justified from the actual JRA distribution (2013-2025, `tmp/candidate-eval-jra/augmented`,
n=626,798 horse-rows): per-ninkijun top-3 rate is monotonically decreasing —
ninkijun 1-6 all sit _above_ the field-wide average top-3 rate (21.51%): 64.4% /
51.4% / 41.4% / 33.4% / 26.7% / 21.8%. Ninkijun 7 (16.7%, avg odds ~26x) is the
**first rank whose top-3 rate drops below the population average**, and every
rank from 7 up stays below it (down to 0.76% at ninkijun 18). This lands
squarely in the odds range (~15-35x average, widening to 280-350x at the tail)
the brief suggested, and gives a well-powered evaluation population:
n=358,454 rows, base top-3 rate 7.78% (27,902 positives) — large enough for
stable cell x rank breakdowns, not so rare that the classifier has nothing to
learn from.

**Target** (what the classifier predicts): `y = 1` iff `tansho_ninkijun >= 7 AND
finish_position <= 3`, trained only on rows with `tansho_ninkijun >= 7` (the
detector is never asked to compare a longshot against a favorite — it only ever
ranks longshots against other longshots, which is the actual product question:
"which of tonight's unfancied runners is worth a look").

**Per-rank breakdown** (USER's explicit rank-1/2/3 instruction): evaluated, not
trained separately — one score, four evaluation cuts: combined top-3, and each
of exactly-1st / exactly-2nd / exactly-3rd individually, each measured against
its own longshot-population base rate.

## 2. Features (pre-registered)

Built on top of the existing `tmp/candidate-eval-jra/augmented` store (same one
`retest_wf.py` and the vector-knn probe use), joined with:

1. **Physical + style_pace + speed_time families**, reused directly from
   `tmp/candidate-nonconform-decomp/families.py`'s `PHYSICAL` / `STYLE_PACE` /
   `SPEED_TIME` lists (imported, not re-typed) — per
   `docs/probes/jra-nonconforming-signal-decomposition-2026-07-04.md`, these are
   the least market-redundant, best-retaining families specifically when the
   market misprices the field, which is exactly this task's operating regime.
2. **Market columns** (already in the store: `tansho_ninkijun`, `tansho_odds_raw`,
   `odds_score`, `inverse_odds_implied_prob`, `inverse_odds_rank_in_race`, ...) —
   included as raw inputs so the tree can learn its own market-vs-ability
   interaction, not just rely on the hand-built divergence feature below.
3. **Explicit divergence feature, `ability_zmean_minus_mkt_rank`**: within-race
   rank of an unweighted z-mean of a small curated set of strong non-market
   ability columns (`speed_index_avg_5`, `career_win_rate`,
   `past_corner_progression_avg_5`, `pedigree_score_for_race`,
   `jockey_recent_win_rate`, `trainer_career_win_rate`, `weight_trend_5`,
   `max(rs_p_nige, rs_p_senkou, rs_p_sashi)`), **minus** `tansho_ninkijun`.
   Deliberately **pure arithmetic — no fitted weights, no model** — so it is
   leak-free for every single row regardless of fold by construction (see the
   leak-safety note in §2.1 on why the alternative, the cached champion CatBoost
   score, was deliberately NOT used here).
4. **`volatility_score`** (`tmp/candidate-race-volatility/volatility_scores.parquet`,
   race-level, joined on `race_id`): a WF-trained, odds-free "will this race
   upset" score (#36). Confirmed fold-structured (`fold`/`in_sample` columns) —
   used only where `in_sample=false` for a row's own year, i.e., only the
   genuinely held-out prediction is reused; see §2.1.
5. **E-grade x venue historical prior, `venue_egrade_longshot_rate`**: per
   (`keibajo_code`, `grade_code=='E'`) cell, the historical longshot-population
   top-3 rate computed on **train-window-only** rows per fold (same pattern as
   `retest_wf.py`'s `build_sameday`/venue-baseline helper) — operationalizes the
   Hakodate/Sapporo E-grade upset-hotspot finding as a per-fold-safe prior
   rather than a global (leaky) constant.
6. **Context**: `kyori_band`, `season_band`, `surface` (derived), `current_baba_condition`,
   `shusso_tosu` (field size).

### 2.1 Leak safety (must be verified by the execution agent before training, and quoted in §5)

- The **cached champion CatBoost base-arm score** (reused safely in the
  vector-knn probe earlier today) is **deliberately NOT reused here as a
  training feature**, because that reuse was safe there only for _blind-year_
  rows (each fold's cached model is trained on `<= fold_year - 1` and scored
  only on `fold_year`, which is genuinely out-of-fold for that one year). This
  detector needs the feature for **all rows including the fold's own training
  window** (2013 through fold*year-1) — scoring a 2015 row with the fold-2023
  model (trained on 2013-2022, i.e., including that exact 2015 row) would be
  in-sample for the detector's own training data, a real leak risk of the same
  class as the `target_corner*\*` incident. The arithmetic divergence feature in
  §2 item 3 sidesteps this entirely (no fitting = no in-sample/out-of-sample
  distinction to get wrong).
- `volatility_score`: verify the `fold`/`in_sample` columns actually mark 2023-25
  rows `in_sample=false` (i.e. genuinely held out relative to their own year)
  before joining; if a row's year lacks a genuinely-held-out score, leave the
  feature null for that row rather than falling back to the in-sample value.
- `venue_egrade_longshot_rate`: must be recomputed **per WF fold** using only
  that fold's own train window (`< fold_year`), exactly like every other
  train-only prior in this codebase's WF harnesses.

## 3. Model and training (pre-registered)

LightGBM binary classification (`objective=binary`, `is_unbalance=True` for the
7.78%-positive class imbalance — method chosen for simplicity and native
imbalance handling; USER left method open). Trained only on the longshot
population (`tansho_ninkijun >= 7`). Standard WF loop (no ad-hoc single fit):
3 folds (train `<=2022`/blind 2023, train `<=2023`/blind 2024, train `<=2024`/blind 2025) x 3 seeds (42, 101, 2026). Fixed, reasonable hyperparameters (no HPO,
consistent with keeping selection-bias risk at zero and matching this
codebase's "no ad-hoc fit, no un-blinded HPO" rules): `num_leaves=31`,
`learning_rate=0.05`, `n_estimators=400` with early stopping on a small
in-train validation slice (last train year), `min_child_samples=50`,
`feature_fraction=0.8`, `bagging_fraction=0.8`.

## 4. Evaluation protocol (pre-registered before any run)

Restricted to the longshot population (`tansho_ninkijun >= 7`) in each blind
year. Per race, the detector's **top-k flagged candidates** (k=1 and k=2,
reported separately) among that race's longshot horses only.

- **(a) Lift**: flagged-horse top-3 rate vs. the longshot population's own
  base rate, for that blind year / cell. Reported as a ratio (lift = flagged
  rate / base rate) and as a paired delta with bootstrap LB95 (2000 iters,
  race-level resample, same convention as the rest of this codebase's WF
  harnesses).
- **(b) Precision-recall curve** across the full score range (not just top-k),
  reported as a small table (5-10 threshold points) plus AUC-PR, for context.
- **(c) Calibration**: decile table (mean predicted probability vs actual
  top-3 rate), same shape as `tmp/candidate-race-volatility/wf_report.json`'s
  existing calibration table for consistency.
- **Per-rank breakdown**: of flagged true positives, the exact-rank
  distribution (1st/2nd/3rd), and lift computed separately against each rank's
  own longshot-population base rate (e.g. longshot-population win rate is much
  lower than its top-3 rate, so "win-lift" and "top3-lift" are different
  numbers and both are reported).
- **Cell-level** (`keibajo_code` / `kyori_band` / `season_band` /
  `current_baba_condition`, n>=200, **sort-before-mask** exactly per
  `docs/finish-position-prediction-system.md` §7.3/§9-K): lift computed
  per cell, not just pooled — pooled-only judgment is explicitly prohibited by
  the brief.
- **Summer-4-venue cut** (`keibajo_code` in 01/02/03/10): lift must not
  collapse here specifically, since that is this campaign's stated focus.

### Adoption bar (pre-registered)

**PASS** iff: flagged (top-k=1 or top-k=2) longshots' top-3 rate is
**>= 1.5x the longshot population's own top-3 base rate in all 3 blind years
(2023, 2024, 2025)**, AND the lift does not collapse in the summer-4-venue cut
(defined as: summer-4-venue lift >= 1.2x, i.e. allowed to be weaker than pooled
but must stay clearly above 1.0x/no-signal). Cell-level results (n>=200) are
reported in full regardless of the pooled verdict, for potential
cell-conditional productization even if the pooled bar narrowly misses.

This is a genuinely different bar shape from §7.2 (which is about pp deltas on
`predicted_rank` accuracy) because the underlying question is different: not
"did ranking accuracy improve" but "does this flag concentrate real longshot
winners better than chance, robustly across years and venues."

## 5. Results

_PENDING EXECUTION._

## 6. Verdict

_PENDING._

## 7. Productization proposal (only if bar clears)

_PENDING._
