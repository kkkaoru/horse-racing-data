# JRA — NAR Training-Data Pooling Arm (wave7) — WF Probe

- **Date**: 2026-07-17
- **Category**: JRA finish-position, training-data-scope lever (not a new feature column)
- **Source**: team-lead direct instruction, one of three parallel wave7 arms under
  USER directive "現在使える範囲の全てのデータとあらゆる選択肢で cell 精度向上"
  (use all currently-available data and every option to improve cell accuracy).
  This arm's remit: test whether adding NAR (regional racing) rows as
  auxiliary JRA training data improves JRA accuracy, JRA-only eval.

## 0. Headline result

**REJECT, both weight arms, on phase-1 evidence (folds 2023+2024) — no
fold-2025 blind confirm run, per the pre-registered decision rule ("only
escalate if promising") and out of consideration for concurrent sibling
agents' shared compute.**

Pooled gate (`n_primaries_passed=0/3`, `place2_or_place3=false`,
`ACCEPT_strict_gate=false`) fails identically for NAR sample weight 1.0
("w10") and 0.3 ("w03"). `place3` is negative in **both arms and both
individual folds** (4/4 consistent direction). The only nominally-large
positive number anywhere in the pooled result — `place2` in fold-2024 alone
(w10 +1.33pp[LB95+0.41], w03 +0.98pp[LB95+0.17]) — is exactly offset by a
sign-flipped loss in fold-2023 (w10 -0.72pp, w03 -0.41pp), the single-fold-
driven non-robust pattern this campaign's history repeatedly treats as
disqualifying rather than promising. The mechanical no-regression floor
(-0.05pp) is also breached in both arms (`worst_delta_pp` = -0.4776pp w10,
-0.2605pp w03). A full sort-before-mask-verified cell scan (`keibajo_code` /
`kyori_band` / `season_band` / `current_baba_condition`, n≥200) finds exactly
one cell with LB95>0 in both arms (`keibajo_code=01` / Sapporo, n=336) out of
roughly two dozen cells scanned — an isolated single hit consistent with
multiple-comparisons noise, not chased further here (Sapporo is already a
separately-flagged noisy small-sample venue in today's own campaign).

Two things worth keeping from this arm even though the lever itself is
rejected: (1) the data-alignment work (JRA-armB-250 vs NAR-store schema
mapping, 215/250 shared columns) is clean and reusable if a narrower
cross-category construction is ever tried; (2) a real CatBoost API gotcha
was caught and fixed mid-run — see §6 — worth remembering for any future
sample-weighted ranking-loss training in this codebase.

## 1. Dedup — why this is genuinely untested

Searched `index_closed_probes.md`, `docs/finish-position-prediction-system.md`
§11 lever bank, and all `docs/probes/*.md` for prior tests of "add NAR rows
to JRA training, evaluate JRA-only." None found. The closest neighbors are
both confirmed distinct:

- **NAR window/venue routing REJECT** (`project_nar_window_venue_reject_2026_06_23`)
  — this narrows NAR's _own_ training window/venue scope for the NAR model
  itself ("NAR は full 2006+ が必要" was the finding). This arm does the
  opposite: it widens _JRA's_ training scope by adding NAR volume, and NAR
  training/serving is completely untouched.
- **JRA condition-D pedigree/lineage pooling REJECT**
  (`project_jra_pedigree_condition_d_closed_2026_07_04`) — "系統pooling" there
  pools horses by bloodline/sire-line across the existing JRA-only corpus, a
  same-category same-store construction. This arm pools two different data
  _stores_ (JRA store + NAR store) across category, an unrelated axis.
- The `nvd_nu`/cross-source companion-column probes
  (`project_relationship_perclass_investigation_2026_06_12` family, and the
  2026-07-02 "cross-source 転入馬履歴 companion 列 REJECT") add _derived
  features_ computed from a horse's history in the _other_ category while
  training remains single-category. This arm adds raw _rows_ from the other
  category into the same training pool; no companion feature engineering.

## 2. Design

**Baseline**: `armB-250` (byte-identical to deployed `jra-cb-v9-sim-2013-clean`
feature list), JRA-only training rows, unweighted — trained fresh inside this
probe's own harness per fold for a clean paired comparison (not reusing an
externally cached artifact, to keep provenance self-contained).

**Candidate**: `armB-250 + is_nar` (251 features) trained on **JRA rows ∪ NAR
rows**, evaluated only on the same JRA-only validation rows used for the
baseline. `is_nar` is a single numeric 0.0/1.0 column, not a CatBoost native
categorical feature (`cat_indices=[]` unchanged) — this whole model lineage
is uniformly `no_cat_features=True`, and a binary 0/1 split is exactly as
expressive to CatBoost's oblivious trees as a 2-level categorical split would
be here, so introducing native categorical handling for the first time in
this lineage was judged unnecessary risk for a binary flag.

**Two weight arms**: NAR rows get CatBoost `group_weight=1.0` ("w10") or
`group_weight=0.3` ("w03"); JRA rows always weight 1.0. See §6 for why
`group_weight` and not `Pool(weight=...)`.

**NAR lower bound**: none — full NAR history back to 2006 included for every
fold (only the upper bound moves with the fold's train-window cutoff). This
unrestricted lower bound _is_ the hypothesis (more historical volume should
help if cross-category transfer is real); artificially windowing it to
match JRA's 2013+ start would have undertested the premise.

**Hyperparameters**: CatBoost YetiRank, iterations=300, depth=8, lr=0.05,
l2_leaf_reg=3.0, no early-stop, `random_seed=42+fold_year`, `thread_count=4`
— identical to the deployed champion spec (`thread_count` lower than some
sibling probes' default 6, deliberately, since two other wave7 agents were
training concurrently on the same machine).

## 3. Data alignment — JRA armB-250 vs NAR store schema

- JRA store: `tmp/candidate-eval-jra/augmented/**/*.parquet`, 635,453 rows,
  2013-2025, `race_id` format `jra:YYYY:MMDD:keibajo:racebango`, `keibajo_code`
  ∈ `01`-`10`.
- NAR store: `tmp/candidate-leak-clean-retrain/nar-full-regen/s11-pacestyle-FINAL/**/*.parquet`,
  2,730,085 rows (`finish_position is not null`), 2006-2026, `race_id` format
  `nar:YYYY:MMDD:keibajo:racebango`, `keibajo_code` ∈
  `{30,34,35,36,42,43,44,45,46,47,48,50,51,53,54,55,56,58,81,82,84}`.
- The `jra:`/`nar:` `race_id` prefixes and disjoint `keibajo_code` ranges
  mean **zero possible `race_id` collision** between the two stores — verified
  both structurally (prefix guarantee) and by an explicit runtime assert in
  the harness (`build_pooled_train`, aborts loudly on any overlap; none
  found in any fold).
- Of the 250 armB feature names, **215 exist natively in the NAR store**.
  The remaining **35 are structurally absent** (NAR pipeline never computes
  them, matching documented gaps: `docs/probes/nar-clean-retrain-fullstore-2026-07-04.md`
  lists the 6 `trainer_grade_*`/`trainer_target_race_*` columns as
  "skipped by design"; `sim_*` (19 columns) were explicitly NAR-REJECTed by
  a past probe, `project_similar_race_features_reject_2026_06_26`; 7
  `course_*` geometry columns and 3 `kohan3f_*_avg5`/`kohan3f_going_diff`
  columns were never built for NAR). All 35 are NULL-filled (typed to match
  the JRA-side dtype, per-column, not relying on automatic null-dtype
  unification across the concat) for NAR-origin rows; CatBoost handles
  missing numeric features natively via its learned missing-value split
  direction. A dry-run invariant check confirmed all 35 columns are 100%
  null on every NAR-origin training row, nothing more and nothing less.

## 4. Harness

Adapted from `tmp/crosspool-odds-divergence/retest_wf.py` (today's canonical,
sort-before-mask-compliant WF template for this model lineage — same gate
constants: `PRIMARIES=["top1","place2","place3"]`, `GATE_MIN_DELTA=0.08`,
`GATE_NO_REG=-0.05`, `N_BOOT=2000`, `BOOT_SEED=20260519`,
`CELL_DIMS=["keibajo_code","kyori_band","season_band","current_baba_condition"]`,
`CELL_MIN=200`). New script: `tmp/nar-pooling-arm/pool_wf.py` (gitignored,
not committed — evidence artifact only). Phase 1 = fold_year ∈ {2023, 2024}
only, single seed (42-base); fold 2025 deliberately not run (see §0).

The sort-before-mask discipline (mask arrays computed against a
`.sort("race_id")`-ordered frame, applied after `paired()`'s internal
re-sort+join) was independently re-verified in this session against the
actual installed Polars version (1.42.0) with a standalone repro comparing
positional-mask results to a key-based join ground truth on deliberately
shuffled input frames — exact match, no reordering artifact. This is the
same bug class documented in `feedback_harness_sort_before_mask` that
previously produced false near-miss cells twice in this project's history
(2026-07-04, 2026-07-12); it does not reproduce here.

## 5. Results — phase 1 (folds 2023+2024, pooled n=6,910 races)

| metric     | base % | w10 cand % | w10 Δpp | w10 LB95 | w03 cand % | w03 Δpp | w03 LB95 |
| ---------- | ------ | ---------- | ------- | -------- | ---------- | ------- | -------- |
| top1       | 33.951 | 33.994     | +0.0434 | -0.4924  | 33.907     | -0.0434 | -0.4776  |
| place2     | 17.930 | 18.234     | +0.3039 | -0.2894  | 18.220     | +0.2894 | -0.2460  |
| place3     | 14.284 | 13.806     | -0.4776 | -1.1143  | 14.023     | -0.2605 | -0.8104  |
| top3_box   | 9.421  | 9.378      | -0.0434 | -0.3763  | 9.262      | -0.1592 | -0.4486  |
| fukusho_2p | 75.036 | 74.660     | -0.3763 | -0.8538  | 74.834     | -0.2026 | -0.6223  |

Gate (both arms): `n_primaries_passed=0/3`, `place2_or_place3=false`,
`ACCEPT_strict_gate=false`.

Per-fold (place2, the only axis with any positive pooled point estimate):

| arm        | fold 2023 Δpp[LB95] | fold 2024 Δpp[LB95]   |
| ---------- | ------------------- | --------------------- |
| w10 place2 | -0.7234 [-1.5625]   | **+1.3318 [+0.4053]** |
| w03 place2 | -0.4051 [-1.1285]   | **+0.9844 [+0.1737]** |

place3 by fold (negative in all four fold×arm combinations):

| arm        | fold 2023 Δpp | fold 2024 Δpp |
| ---------- | ------------- | ------------- |
| w10 place3 | -0.3762       | -0.5790       |
| w03 place3 | -0.0868       | -0.4343       |

**Cell scan** (`cells_ge200`, sort-before-mask verified — see §4): the only
cell with LB95>0 in both arms is `keibajo_code=01` (Sapporo), n=336 — w10
top1 +2.38pp[LB95 0.000] (borderline), w03 top1 +2.98pp[LB95+1.190]. Single
isolated hit out of ~20-30 n≥200 cells scanned across the four dimensions;
not treated as a follow-up lead given (a) this project's repeated finding
that isolated single-cell hits in a multi-cell scan are pattern-free noise
(e.g. the 2026-07-17 vector-knn probe's 8/22 cell hits, dismissed on the
same grounds), and (b) Sapporo is independently flagged elsewhere in today's
campaign as an unusually small/noisy venue (the jockey-pedigree269 3-seed
venue01×intermediate re-evaluation closed as NOISE this same day). Full
per-dimension detail in the JSON report (not reproduced here).

## 6. Implementation note: `Pool(weight=...)` is a no-op for YetiRank

Mid-run, the first real fit (fold-2023, w10) printed a CatBoost runtime
warning: `"Pairwise losses don't support object weights."` CatBoost's
pairwise/ranking losses — YetiRank included — **silently ignore per-object
`Pool(weight=...)` entirely**; only `Pool(group_weight=...)` (scaling each
race-group's generated-pair loss contribution) has any effect. This was
independently re-confirmed in this session with a minimal standalone repro
against the installed `catboost==1.2.10`: the same warning reproduces with
`weight=`, and is silent (and produces genuinely different model behavior)
with `group_weight=`. The one model trained under the broken mechanism
(fold-2023/w10, ~5.3 min) was discarded and retrained before any reported
number was computed — no reported result in §5 used the broken path.

Because every race group in this pooled dataset is 100% single-source (JRA
xor NAR, never mixed — guaranteed by the disjoint `race_id` prefixes), a
constant-per-group weight is the _exact_ intended semantics for down-
weighting NAR's influence, not an approximation of a per-row design. This is
worth remembering for **any future NAR/JRA CatBoost ranking-loss training
with sample weights** in this codebase — the equivalent lesson for XGBoost
ranking groups (`group_weights_from_row_weights`, `train_xgboost_ranker`) was
already recorded from the 2026-07-02 `nvd_nu` probe; this is the CatBoost-side
counterpart.

## 7. Mechanism (explanatory, not the primary evidence)

NAR rows outnumber JRA training rows by roughly 4.5-4.7x even before any
loss-weighting (2.26-2.40M NAR vs 485K-533K JRA per fold). CatBoost computes
feature quantization/binning globally across the whole training pool
regardless of `group_weight` (which only scales the loss, not a row's
participation in split-finding statistics), so even the down-weighted arm
still lets NAR's row volume shape the tree structure. NAR's regional racing
dynamics and market efficiency differ materially from JRA's — this project
has independently established elsewhere that the iter40 Set-Transformer
architecture won specifically because NAR's market is comparatively thin/
inefficient (`project_transformer_blend_jra_banei_reject_2026_07_02`: "NARの
みで効く(→iter40)"), and was REJECTed on JRA's already-efficient market. The
pattern here is consistent with that precedent: pooling likely dilutes
CatBoost's fit to JRA-specific patterns faster than it contributes true
cross-category transfer signal, and the direction doesn't reverse at 0.3
weight, it just shrinks toward zero (w03 is uniformly closer to zero than
w10 across every metric in §5) — arguing against expecting a sweet spot
somewhere below 0.3 rather than for one.

## 8. Verdict

**REJECT.** `n_primaries_passed=0/3` for both arms, no-regression floor
breached for both arms, no robust positive axis anywhere in the pooled or
per-fold results, and the one large positive per-fold number is a single-
fold-driven sign-flip pattern this campaign's history treats as
non-adoptable. No fold-2025 blind confirm run (pre-registered as
conditional on phase-1 being "promising"; it isn't). No deploy action; no
change to `jra-cb-v9-sim-2013-clean` or any other production model.

**DO-NOT-RETEST scope**: this exact construction — `armB-250 + is_nar`
numeric flag, JRA∪NAR pooled training (NAR full 2006+ history, no lower
bound), JRA-only eval, `group_weight` ∈ {1.0, 0.3}, CatBoost YetiRank armB
spec. **Left genuinely open** (different enough constructions to not be
covered by this closure): weight values between 0 and 0.3 (though the
monotonic w10-worse-than-w03 pattern on place3 argues against expecting a
sweet spot); selective NAR inclusion (e.g. only NAR races/horses with a
documented JRA transfer history, a narrower population than blanket
pooling); NAR-as-pretraining/warm-start rather than joint pooling.

## 9. Artifacts

- Harness: `apps/pc-keiba-viewer/tmp/nar-pooling-arm/pool_wf.py` (gitignored).
- Report: `apps/pc-keiba-viewer/tmp/nar-pooling-arm/reports/pool_wf_phase1.json`.
- MLflow: `finish-position/wf-eval`, `model_version` =
  `jra-nar-pooling-w10-candidate-2026-07-17` /
  `jra-nar-pooling-w03-candidate-2026-07-17`, `eval_regime=wf`,
  `tags.gate_result=REJECT` for both, `register=False`, `champion=False`.
