---
probe: jra-longshot-top3-detector
date: 2026-07-17
category: jra
method: per-horse binary classifier flagging market-undervalued (longshot) horses likely to finish top-3, evaluated as a detection/lift task with its own pre-registered gate — NOT a ranking-accuracy lever
status: CLOSED (canonical) — v1 PASSED its population-relative gate but was ~equivalent to a naive market-rank heuristic (§5.4); v2 (§8, orchestrator-directed) re-registered the bar as market-free-model-vs-naive-baseline and FAILED cleanly and uniformly (0/3 years, 0/22 cells, summer-4-venue included). Final answer: even within the longshot band, non-market signal increment is zero with this feature set. DO-NOT-RETEST this construction; see §8.4.
mlflow_run_id: fa344cfb70a54c2cbf2f7452e09faf19 (v1), 21ff845f5030412dbead46d49eee2a19 (v2, parent-linked to v1)
mlflow_experiment: finish-position/longshot-detector
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
   `max(rs_p_nige, rs_p_senkou, rs_p_sashi)`). Implemented as
   `tansho_ninkijun - ability_zmean_rank_in_race` (positive = the non-market
   composite ranks this horse ahead of where the market has it — verified by
   spot-check in §5.1). Deliberately **pure arithmetic — no fitted weights, no
   model** — so it is leak-free for every single row regardless of fold by
   construction (see the leak-safety note in §2.1 on why the alternative, the
   cached champion CatBoost score, was deliberately NOT used here). A single
   feature's sign is a monotonic transform LightGBM is invariant to either way,
   so this is a documentation-clarity fix, not a design change post-results.
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

Feature build ran in 1.9s and independently reproduced the doc's own §1 numbers
exactly (626,798 total rows; 358,454 longshot rows; 27,902 positives; 7.784%
base rate), a useful cross-check that implementation and pre-registration are
reading the same store the same way. All `PHYSICAL` (18) / `STYLE_PACE` (49) /
`SPEED_TIME` (16) columns from `families.py`, all 7 named divergence columns
plus the 3 `rs_p_*` columns, and every context column survive unchanged in the
current 310-column store — zero drops.

### 5.1 Leak-safety verification (as required by §2.1)

- **`volatility_score`**: sampled `fold`/`in_sample` directly for 2023/2024/2025
  rows — every single row in each blind year carries `in_sample=False` with the
  matching `fold=oos_2023`/`oos_2024`/`oos_2025` label (genuinely held out
  against its own year). All 2013-2022 rows (`in_sample=True`) were correctly
  nulled pre-restriction (485,275 of 626,798). No blind-year row needed
  nulling — the file was already fold-safe for this reuse.
- **`ability_zmean_minus_mkt_rank`**: spot-checked the most extreme rows. The
  most-positive examples are `tansho_ninkijun=18` horses the ability composite
  ranked #1 in-race (max divergence, +17). The most-negative are
  `tansho_ninkijun=1` favorites the composite ranked dead last (-17) — all
  three sampled rows actually missed the board (finished 4th/4th/8th), a
  concrete real-world confirmation the sign and construction behave as
  intended.

### 5.2 WF training

9 LightGBM binary models (3 seeds x 3 folds) trained in 17.7s total, well
inside a single foreground call, `is_unbalance=True`, exactly the fixed
hyperparameters in §3 (no HPO). Memory checked before both the feature build
(59% free) and training (53% free) — comfortably above the 15% floor.

### 5.3 Pre-registered evaluation (§4)

Pooled seed-averaged (2023-2025, N=79,618 longshot horse-rows / 10,263 races),
bootstrap LB95 over 2000 race-level cluster resamples:

| k   | metric | flagged% | base% |      lift | delta (pp) | LB95 (pp) |
| --- | ------ | -------: | ----: | --------: | ---------: | --------: |
| 1   | top3   |    16.53 |  7.72 | **2.14x** |      +8.81 |     +8.13 |
| 1   | win    |     3.78 |  1.62 |     2.34x |      +2.16 |     +1.83 |
| 1   | place2 |     5.24 |  2.50 |     2.10x |      +2.74 |     +2.34 |
| 1   | place3 |     7.50 |  3.60 |     2.08x |      +3.90 |     +3.43 |
| 2   | top3   |    14.90 |  7.72 | **1.93x** |      +7.18 |     +6.78 |

Per-year top3 lift (k=1 / k=2): 2023 2.13x/1.96x — 2024 2.10x/1.90x — 2025
2.19x/1.93x — stable across all 3 blind years. **Summer-4-venue**: 1.98x (k=1)
/ 1.78x (k=2), LB95 +7.17pp / +5.97pp — does not collapse.

**Cell scan** (n>=200 races, sort-before-mask): all 22 scanned cells
(`keibajo_code` x10, `kyori_band` x4, `season_band` x4, `current_baba_condition`
x4) clear >=1.5x on k=1 top3 lift, **every one with a positive LB95** (range
1.60x-2.39x; weakest is `keibajo_code=02` at 1.60x / LB95+2.66pp). This
uniformity across 22 independent cuts — not a mix of hits and misses — is
itself evidence this is a real, broad effect rather than multiple-comparisons
noise (a spurious pattern would show some cells clearing and others not; here
every cell clears, by a wide and fairly consistent margin).

Precision-recall: AUC-PR = 0.171 vs. 0.077 base rate (~2.2x). Calibration is
strongly monotonic decile-over-decile but not absolutely calibrated
(`is_unbalance=True` reweights the loss and distorts the absolute probability
scale; rank-ordering, which is all top-k selection needs, is unaffected). Of
the k=1 true positives (1,696 races), 388 were outright wins, 538 placed
exactly 2nd, 770 placed exactly 3rd. Full tables:
`apps/pc-keiba-viewer/tmp/longshot-detector-2026-07-17/eval_result.json`.

**Verdict against the pre-registered bar (§4): PASS.** Both k=1 and k=2 clear

> =1.5x in all 3 blind years, and neither collapses on the summer-4-venue cut
> (>=1.2x required; both comfortably above it).

### 5.4 Supplementary diagnostic (not pre-registered — run after seeing the PASS, to stress-test what's actually driving it)

Because the pre-registered feature list deliberately includes raw market
columns (§2 item 2, to let the tree learn its own market-vs-ability
interaction), a natural question the pre-registered protocol does not answer
on its own is _how much_ of the lift is genuine non-market signal versus the
market's own within-band ordering restated. Three checks, run against the same
blind-year data, kept in a clearly separate, non-pre-registered artifact
(`sanity_vs_ninkijun_baseline_result.json`) rather than folded into §5.3:

1. **Trivial baseline**: "flag the single lowest-`tansho_ninkijun` horse within
   the longshot band, no model at all." This achieves **2.1525x** lift at k=1
   — numerically _higher_ than the trained detector's 2.1411x, on the same
   10,263-race population (both LB95 ranges are effectively identical: detector
   LB95+8.13pp vs. baseline LB95+8.22pp).
2. **Agreement**: the detector's k=1 pick agrees with this trivial rule in
   **91.46%** of races. On the 876 races (8.5%) where they disagree, the
   _naive rule wins_ (15.18% vs. 14.16% hit rate on that disagreement subset).
3. **Feature importance** (fold-2025 model): `inverse_odds_implied_prob` alone
   accounts for **39%** of total gain; the top 5 features are all pure
   market/odds columns and together account for **~72%** of total gain. The
   remaining ~100 non-market physical/style/speed/ability/volatility/venue
   features collectively contribute the other ~25%, spread thin (each <1%).

**Reading this honestly**: the pre-registered bar (lift over the longshot
population's _own_ base rate) is met robustly and by a wide, cell-universal
margin — that part of §5.3 is not in question. But the bar as specified does
not distinguish "the model found hidden non-market value" from "the model
mostly re-derives the fact that, even among horses the market has already
written off, the market's own ordering still carries information" — and the
diagnostic above shows it is overwhelmingly the latter. A bettor with the odds
board and no model gets essentially the same result. This is a **materially
different outcome from what the original USER ask was reaching for**
("見つけられるように" implies surfacing something not already obvious from
the odds themselves), even though it technically clears the gate as written.

## 6. Verdict (v1 — superseded by §8, kept as historical record)

> **This section's recommendation was superseded by the orchestrator's v2
> direction (§8) and should not be acted on independently.** Path 1 below
> ("ship as a triage tool") was explicitly declined: a tool that matches a
> naive market-rank heuristic adds nothing over what the viewer's existing
> odds display already shows a user. Path 2 was adopted and executed as §8.

**PASS on the pre-registered bar, with a caveat material enough to change the
practical recommendation.** The detector is statistically real and robust — 3/3
years, both k values, 22/22 cells, summer-4-venue included, no sign of
multiple-comparisons noise. It would reliably do what it is measured to do if
shipped. But §5.4 shows the mechanism is ~91% redundant with simply reading the
odds board within the longshot band, and feature importance confirms market
columns supply ~72% of the model's decision-making. This is not a discovery of
market inefficiency; it is closer to a convenient triage tool ("which 1-2 of
tonight's dozen no-hopers are least implausible") than to the "find what the
market missed" framing the brief was reaching for.

**Recommendation for the orchestrator**: two honest paths forward, not
mutually exclusive —

1. **Ship it as a triage aid with accurate framing** (per the productization
   sketch below) — genuinely useful for a user scanning a full field, cheap to
   build, statistically solid, as long as it is never described as "the model
   found value the market missed." The pre-registered bar was met in good
   faith and this is a legitimate, if modest, product outcome.
2. **If genuine non-market-signal discovery is still wanted**, the next
   iteration should change the _bar_, not just the model: strip all
   market/odds columns from training entirely (forcing the model to rely on
   physical/style_pace/speed_time/volatility/venue-prior only) and re-run the
   same naive-ninkijun-baseline comparison — the real target metric should be
   **incremental lift over the naive within-band favorite-picking baseline**
   (currently ~0, arguably slightly negative), not lift over the longshot
   population's raw base rate (which almost any reasonable ranking signal
   clears, market-column-heavy or not). This is a materially harder bar and
   was out of scope for the time budget here, but is the natural next
   pre-registration if this thread continues.

No deploy either way — reported to the orchestrator per the brief.

## 7. Productization proposal (v1, path 1 — DECLINED, see §6 and §8; kept as historical record only)

- **Storage**: a new, small Neon side-table (e.g. `race_longshot_flag`, keyed
  like `race_finish_position_model_predictions` on
  `model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango`)
  rather than a new column on the existing predictions table — keeps this
  cleanly additive and independently droppable, matching §0's "never touches
  `predicted_rank`."
- **Compute/serving**: piggyback on the existing `apps/finish-position-cron` ->
  `FinishPositionPredictContainer` -> `predict_lib` pipeline as one more
  scorer step restricted to `tansho_ninkijun >= 7` horses in that race,
  upserting into the new table on the same cadence.
- **Viewer surfacing**: reuse the existing badge pattern already in
  `apps/pc-keiba-viewer/src/app/races/detail/runners-table.tsx` (the
  blinker/surface-switch badge convention), shown only on the k=1 (optionally
  k=1+k=2 as a lighter secondary badge) flagged horse per race. Given §5.4,
  label it as something like "pick of the long shots" — a triage aid — not
  any "value found" or "market missed this" framing, which the evidence does
  not support.
- **Deploy-checklist analogue**: this doesn't map onto the champion-model
  deploy-rail checklist (no ranking model is being baked into a container
  image or replacing anything). The equivalent minimum bar: (a) a real
  full-train artifact + MLflow registration, (b) the new table + upsert
  wiring, (c) a smoke test confirming the flag populates for a real upcoming
  race before trusting it live, (d) rollback = stop writing / hide the badge
  (materially lower risk than any ranking-affecting change, since
  `predicted_rank` is never touched).
- **k=1 vs k=2**: k=1 gives a cleaner single-badge UX and the higher lift;
  k=2 could be a lighter secondary badge if more density is wanted. Both clear
  the pre-registered bar; the choice is a product call, not a statistical one.

## 8. v2 — market-free re-test against a naive-baseline bar (orchestrator-directed)

### 8.1 Why v2, and what changed

The orchestrator's read of §5.4/§6 (correct): the USER's ask was specifically
about finding longshots the market got wrong, and a "lift over population
base rate" gate cannot distinguish that from "lift over the population's own
market-implied ordering restated." v2 re-registers the bar to make that
distinction directly, and removes market information from the model entirely
so any surviving lift can only come from non-market signal.

**Feature set — market-free, verified by exhaustive accounting, not
recap-and-hope**: kept the full `PHYSICAL` (18) / `STYLE_PACE` (49) /
`SPEED_TIME` (16) families (0 drops), `volatility_score`, and race context
(`kyori_band`, `season_band`, `current_baba_condition`, `surface`,
`grade_code`, `keibajo_code`) — 90 columns, plus a fold-recomputed
`venue_egrade_longshot_rate` (train-only, exactly as §2 item 5 specified) =
91 features fed to the model. Excluded: all 15 `families.py` `MARKET`
columns, `tansho_ninkijun` itself, and — the easy one to miss —
**`ability_zmean_minus_mkt_rank`**, which despite its "ability" framing is
literally `tansho_ninkijun - ability_rank` and therefore directly encodes
market rank. The execution agent verified this exclusion programmatically
(an accounting assertion that would have raised if it leaked in) rather than
trusting the column name. `RECENT_FORM`/`CAREER_ABILITY`/`CONNECTIONS`/
`SIMILARITY` were confirmed absent from the cached feature table by a
zero-overlap check against their column names in `families.py` — not
assumed absent because v1 "shouldn't" have included them.

Two judgment calls made during implementation, noted for the record: (1)
`grade_code` is now a raw model feature (v1 only used it to build the E-grade
prior) — applied per this doc's literal instruction; (2) `keibajo_code` is
included as a raw feature (reusing v1's categorical-handling architecture,
and structurally required anyway as the E-grade-prior join key) even though
it wasn't explicitly named in the Context bullet — flagged rather than
silently assumed.

Model/WF spec unchanged from v1 (LightGBM binary, same hyperparameters, same
3-fold x 3-seed structure) — only the input features and the evaluation
baseline changed.

### 8.2 New pre-registered bar

Within the longshot population, per race: model top-k (by predicted
probability) vs. naive top-k (lowest `tansho_ninkijun` in the band), same
race set, paired. **PASS iff, for at least one of k=1 or k=2** (matching v1's
own k=1-OR-k=2 bar structure): `relative_lift = model_top3_rate /
naive_top3_rate >= 1.15` in all 3 blind years (2023/2024/2025, sign of the
delta must be positive in all 3 — "3/3 sign-stable" per the orchestrator's
instruction), AND summer-4-venue `relative_lift >= 1.0` for that same k (a
deliberately softer "does not collapse" bar than the main +15% requirement,
given the smaller n in that cut — this specific number was left to my
judgment by the orchestrator and is recorded here for transparency, not
silently chosen).

### 8.3 Results

Pooled 2023-2025 (N=79,618 longshot horse-rows / 10,263 races — **exact
match** to v1's population, confirming this is the identical evaluation set
under a different model and baseline, not an artifact of a different
sample):

| k   | metric | model% | naive% | relative_lift | delta (pp) | LB95 (pp) |
| --- | ------ | -----: | -----: | ------------: | ---------: | --------: |
| 1   | top3   |  11.69 |  16.61 |    **0.704x** |      -4.92 |     -5.71 |
| 1   | win    |   2.31 |   3.79 |        0.609x |      -1.48 |     -1.86 |
| 1   | place2 |   3.80 |   5.28 |        0.720x |      -1.48 |     -1.93 |
| 1   | place3 |   5.58 |   7.54 |        0.740x |      -1.96 |     -2.53 |
| 2   | top3   |  10.72 |  14.85 |    **0.722x** |      -4.12 |     -4.58 |

Per-year top3 relative_lift, k1/k2 (all negative delta, LB95 never crosses
zero): 2023 0.725/0.706 (LB95 -5.84/-5.21pp) — 2024 0.734/0.756 (LB95
-5.60/-4.32pp) — 2025 0.654/0.706 (LB95 -7.27/-5.19pp). **Sign is negative in
all 3 years for both k — 0/3, not the required 3/3.**

Summer-4-venue (n=2,420 races): 0.760x (k1) / 0.781x (k2), LB95 -5.66pp /
-4.31pp — fails even the softer >=1.0x "does not collapse" bar.

**Cell scan** (22 cells, n>=200, sort-before-mask): **0 of 22** clear
`relative_lift >= 1.0` on k1 top3 (range 0.635x-0.831x). This is the exact
mirror image of v1's "22/22 uniform pass" — here it is a uniform, clean
underperformance, not a mix of hits and misses, which is itself evidence this
is a real, coherent effect rather than noise (in either direction).

Per-rank breakdown (k1): the model caught 1,200 true positives (237 win /
390 place2 / 573 place3) against the naive rule's 1,705 (389 / 542 / 774) —
naive wins on every single rank category, at both k values.

**Cross-validation**: the independently-recomputed naive-baseline rate in v2
(16.6131% at k1 pooled) matches v1's own separately-computed
`sanity_vs_ninkijun_baseline_result.json` figure (16.613%) to 3 decimal
places, via a fully independent script and pipeline — strong evidence the
paired-comparison harness itself is correct and this is a genuine finding,
not a bookkeeping bug.

**Feature importance** (fold-2025 models, gain-based, averaged across seeds):
top-5 = `last_3_avg_kohan_3f` (5.0%), `kohan3f_avg_5` (4.8%),
`kohan3f_firm_avg5` (4.2%), `recent_soha_time_per_meter_avg5` (4.1%),
`weight_trend_5` (4.0%) — top-5 sum only **22%** of total gain (vs. v1's
72% market-column concentration). `speed_time` (closing-3F pace metrics
dominate the very top) and `physical` (weight trend, `futan_juryo_*`) carry
the most weight, with `style_pace` (corner-progression columns) also
represented; `keibajo_code` ranks #10 at 2.7%. Signal is genuinely diffuse
across the non-market families — there is no equivalent of v1's single
dominant `inverse_odds_implied_prob` feature — it is simply not enough,
in aggregate, to match a rule with direct access to market rank.

### 8.4 Final verdict: FAIL — canonical closure

**FAIL, cleanly and uniformly.** Both k values fail every component of the
bar: relative lift is confidently below 1.0 (not just below 1.15) in all 3
years, at both k, pooled, in the summer-4-venue cut, and in all 22 scanned
cells. This is not a near-miss or an ambiguous result requiring judgment — it
is a decisive, mechanistically coherent negative finding, consistent with
v1's §5.4 diagnosis (the original detector's "excess" lift over naive was
already essentially zero even _with_ market columns available; removing
them was always likely to leave the model behind a rule with direct market
access, and it does, by a wide and statistically confident margin).

**Canonical conclusion**: within the JRA longshot population
(`tansho_ninkijun>=7`), physical/style_pace/speed_time features plus an
odds-free volatility score and a train-only E-grade x venue prior carry
real, non-market, genuinely diffuse signal (feature importance confirms this
— it is not that the market-free model is degenerate or randomly-scored),
but that signal is **not sufficient, in aggregate, to out-rank a horse's own
market position within the band**. Reading the odds board remains the single
strongest available signal for "which longshot is relatively live," even
restricted to a population the market has already collectively written off.
This directly corroborates today's broader market-wall evidence from sibling
probes (wide-umaren, contender-reorder, upset-scan) run in parallel this
session.

**DO-NOT-RETEST**: this exact construction (market-free physical/style_pace/
speed_time/volatility/E-grade-prior feature set, LightGBM binary, longshot
population `ninkijun>=7`, evaluated against the naive-ninkijun baseline) is
now closed. A future attempt would need either genuinely new non-market
input signal not already in this feature set, or — the one explicitly
untested adjacent question this doc surfaced but did not test (§8.3's last
feature-importance note) — a **blend of market rank plus these features**
(rather than either alone), which is a different, not-yet-closed question
from "market-free model alone vs. naive," and was out of scope here.

No deploy (neither v1's declined triage-tool path nor a market-free model,
since it fails outright). Both v1 and v2 results are recorded in MLflow
(parent-linked) and this doc for the historical record; `path 1` (§7) was
declined per the orchestrator's explicit instruction as adding nothing over
existing odds display.

## Artifacts

- `apps/pc-keiba-viewer/tmp/longshot-detector-2026-07-17/labeled_features.parquet`,
  `feature_columns.json`, `build_features_report.json` (Step 1)
- `train_wf.py`, `train_wf_log.json`,
  `models/seed{42,101,2026}/fold-{2023,2024,2025}/{model.txt,predictions.parquet}`
  (Step 2)
- `evaluate.py`, `eval_result.json` — the pre-registered evaluation (Step 3)
- `sanity_vs_ninkijun_baseline.py`, `sanity_vs_ninkijun_baseline_result.json` —
  supplementary, not pre-registered, but decisive for §5.4/§6
- `build_mlflow_manifest.py`, `mlflow_manifest.json`, `verify_mlflow_run.py`
  (Step 4)
- MLflow run `fa344cfb70a54c2cbf2f7452e09faf19`
  (`finish-position/longshot-detector`, independently read-back verified)
- **v2** (`apps/pc-keiba-viewer/tmp/longshot-detector-2026-07-17/v2/`, v1's
  directory left untouched): `feature_columns_v2.json`, `train_wf_v2.py`,
  `train_wf_log_v2.json`,
  `models/seed{42,101,2026}/fold-{2023,2024,2025}/{model.txt,predictions.parquet}`,
  `evaluate_v2.py`, `eval_result_v2.json`, `build_mlflow_manifest_v2.py`,
  `mlflow_manifest_v2.json`, `verify_mlflow_run_v2.py`
- MLflow run `21ff845f5030412dbead46d49eee2a19`
  (`finish-position/longshot-detector`, `mlflow.parentRunId` +
  `parent_run_id` both set to v1's run, independently read-back verified)
- (not committed — `tmp/` per repo convention)
