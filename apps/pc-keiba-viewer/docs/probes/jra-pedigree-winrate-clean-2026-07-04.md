# JRA Pedigree Win-Rate Features — Clean Baseline WF (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position feature engineering
- **USER condition**: D — 競馬場×class×距離×血統(父/母父/父父)の勝率 (venue x class x
  distance x bloodline win-rate), with special focus on the 4 summer venues
  (`keibajo_code` 01=Sapporo, 02=Hakodate, 03=Fukushima, 10=Kokura). Sapporo +
  Hakodate turf is 洋芝 (the only JRA venues with it) — sire-line 洋芝 affinity is
  a genuine real-world signal and the flagship candidate here.

## What already exists (armB) vs what this probe adds

Checked `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json` (`armB`,
250-feat, the live `jra-cb-v9-sim-2013` clean spec) before engineering anything new.
armB **already has**: `sire_distance_win_rate`, `sire_track_win_rate` (surface),
`dam_sire_distance_win_rate`, `sire_avg_finish_at_distance`,
`damsire_avg_finish_at_track`, `sire_{nige,senkou,sashi,oikomi}_rate`,
`sire_corner_1_norm_avg`, `sire_grade_place2_rate`, `damsire_distance_place2_rate`,
`sire_baba_win_rate`, `damsire_baba_win_rate`, `sire_horse_baba_combined_score`,
plus `sim_sire_win_rate` / `sim_sire_place_rate` / `sim_damsire_win_rate`
(similar-race sire/damsire signals). Sire x class (grade) and sire x surface x
distance are therefore already covered.

**Genuinely absent** (per instruction: "engineer only what is ABSENT"):
sire x **venue** (10 JRA venues — armB only has surface, not per-venue), sire x
**洋芝** specifically (Sapporo/Hakodate turf — no JRA venue combo like this
exists anywhere in armB), damsire x **surface** as a rate (only
`damsire_avg_finish_at_track` = avg finish, not a rate), and a **top3-rate**
(vs win-rate-only) variant for sire/damsire x distance-band. Sire x class was
already deliberately excluded from the candidate list since `sire_grade_place2_rate`
covers it.

A prior LEAKED-baseline-era probe
(`tmp/candidate-jra-jockey-pedigree-cell/build_pedigree_cell.py`) tested
grandsire (父父, `ketto_joho_03b`) x dist/surface/venue and sire x class x surface
EB-shrunk win/top3-rate columns bundled together with unrelated jockey features
in a single `"all"` arm (500-iter, early-stop, cat-features-enabled — a
different, non-deployed-matching spec) and reported `top1 +2.48pp [LB95 +1.91]`
pooled — but that result is **confounded** (jockey-nichime + pedigree candidates
combined in one arm, never isolated) and used a different hyperparameter spec
than the deployed model. It is not reused as evidence here; this probe
re-derives pedigree-only candidates from scratch against the exact deployed
spec.

## Method

- **Harness**: `tmp/candidate-jra-pedigree-winrate-clean/wf_pedigree.py` — a
  self-contained copy of the masked-lever-retest harness pattern (NOT editing
  `tmp/candidate-masked-lever-retest/retest_wf.py` directly since a sibling
  probe was using it concurrently). The **control-arm models are reused
  read-only** from `tmp/candidate-masked-lever-retest/models/base/` (identical
  spec/seeds/folds, already trained by the sibling masked-lever campaign) —
  only the +5-col treatment models are trained fresh here.
- **Baseline (control)**: CLEAN `armB`, 250 feat, no leak cols.
- **Treatment**: control + 5 pedigree candidate columns (additive).
- **Model**: CatBoost YetiRank, `iterations=300, depth=8, lr=0.05, l2=3.0`, no
  early-stop, `cat_indices=[]` (all-numeric) — matches deployed
  `jra-cb-v9-sim-2013` exactly.
- **Folds**: 3 blind walk-forward folds, train `2013..Y-1` / test `Y`, for
  `Y in {2023, 2024, 2025}`.
- **Seeds**: multi-seed, `seed_base in {42, 101, 2026}` (`seed = seed_base +
fold_year` per fold), pooled via per-race hit-rate averaging across seeds
  before the paired bootstrap.
- **Metrics**: exact-ordinal `top1`/`place2`..`place6`, `top3_box`, `fukusho_2p`.
  Primaries = `{top1, place2, place3}`.
- **Significance**: paired race-level bootstrap, 2000 iterations, fixed seed
  20260519, `delta = treatment − control`.
- **Accept gate**: >=2/3 primaries `delta_pp >= +0.08` AND `LB95 > 0`; AND >=1
  of `{place2, place3}` passes; AND no metric regresses below `-0.05pp`.
- **Cell eval**: pooled seed-avg re-cut by `keibajo_code` / `kyori_band` /
  `season_band` / `current_baba_condition`, `n >= 200`. PLUS a summer-restricted
  subset (`keibajo_code in {01,02,03,10}`, `n >= 100`) and a Sapporo+Hakodate-only
  (洋芝-eligible) subset, given the flagship candidate targets exactly that cell.

## Candidate columns

Built by `tmp/candidate-jra-pedigree-winrate-clean/build_pedigree_winrate.py`
from local Postgres `jvd_se`/`jvd_ra`/`jvd_um` (port 15432). All strictly prior
(expanding window, `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`, ordered
by `(date, race_bango, umaban)` — the current race's own runners never
contribute to their own value). Empirical-Bayes shrinkage `k=50` toward the
lineage's own running overall top3 rate (not a hardcoded constant, matches
`build_pedigree_cell.py`'s convention). Uses DuckDB window functions over the
~910K-row horse-run-level breeding-joined table (**no horse-level self-join** —
the safe O(n log n) pattern from `build_sameday`/`build_draw_ablation`; a prior
self-join attempt on a similar table drove host free memory to 110MB before
being killed). Built in 2.7s, no memory incident (`memory_pressure -Q` stayed

> = 39% free throughout).

Pedigree table verification: `jvd_um` (213,286 rows) — `ketto_joho_01b` (sire
name, 99.9995% populated), `ketto_joho_03b` (grandsire/父父, 99.5%),
`ketto_joho_05b` (damsire/母父, 99.9995%) — confirms JRA convention `01=父,
02=母, 03=父父, 04=父母, 05=母父, 06=母母`.

| Column                 | Definition                                                                                                                                                                                                                                                                           | Coverage (2013-2025 store)                                                                                                                      |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `sire_venue_top3`      | sire offspring prior top3 rate at this row's own `keibajo_code`, EB k=50 vs sire overall                                                                                                                                                                                             | 99.10%                                                                                                                                          |
| `sire_dist_top3`       | sire offspring prior top3 rate at this row's own dist-band (top3-rate variant of the existing win-rate-only `sire_distance_win_rate`)                                                                                                                                                | 99.62%                                                                                                                                          |
| `sire_yoshiba_top3`    | **flagship** — sire offspring prior top3 rate specifically on Sapporo/Hakodate-turf races, via a `FILTER (WHERE ...) OVER (...)` cumulative window so the stat is visible on every row (not just yoshiba rows), letting the tree combine it with the row's own current venue/surface | 97.36% (97.36% of rows: sire has >=1 genuine prior yoshiba-race offspring; NULL only for the true first-crop-with-zero-prior-runners edge case) |
| `damsire_dist_top3`    | damsire offspring prior top3 rate at this dist-band (top3-rate variant of `dam_sire_distance_win_rate`)                                                                                                                                                                              | 99.54%                                                                                                                                          |
| `damsire_surface_top3` | damsire offspring prior top3 rate by surface (turf/dirt) — no existing damsire x surface rate column (only `damsire_avg_finish_at_track` = avg finish)                                                                                                                               | 99.68%                                                                                                                                          |

All well above the 40% coverage floor; no columns dropped for coverage.

## CRITICAL note: cell-mask bug found + fixed mid-probe

The sibling masked-lever-retest probe found a real bug in its own cell-eval
pattern (which this harness's cell/summer/yoshiba code was structurally
copied from): the paired-bootstrap helper (`paired()`) sorts both hit-frames
by `race_id` internally before joining, but the boolean cell mask was computed
against `base_avg.join(dims, ...)`'s row order straight out of
`group_by("race_id").agg(...)` — Polars does **not** guarantee that output
order matches a sort. That silently misaligned the mask against `paired()`'s
internally re-sorted rows, producing fake cell numbers (the sibling probe
found 3 phantom positive-LB95 summer-venue cells this way). **Pooled /
per-fold / per-seed numbers are unaffected** (`paired()` is called directly on
`base_avg`/`cand_avg` with no mask there). Fixed here by sorting `ba`/`ca` by
`race_id` immediately after the `dims` join, before any mask is computed
(`tmp/candidate-jra-pedigree-winrate-clean/wf_pedigree.py`, matches the
upstream fix in `tmp/candidate-masked-lever-retest/retest_wf.py` exactly —
diffed to confirm). Re-ran with all 9 candidate models cached (no retraining
needed, predict-only) — re-run completed in **9.9s**. The corrected numbers
below (summer worst-delta improved slightly from -0.177 to -0.1225pp; yoshiba-only
worst-delta got _more_ negative, from -0.2849 to -0.4986pp) are the ones
reported; the original uncorrected run is not used anywhere in this doc.

## Result: REJECT (pooled, summer-restricted, AND yoshiba-only)

Pooled (seed-avg, n=10,365 races):

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 33.796 | 33.845 | +0.048     | -0.148 |
| place2     | 18.119 | 18.257 | +0.138     | -0.084 |
| place3     | 14.163 | 14.218 | +0.055     | -0.196 |
| place4     | 12.166 | 12.217 | +0.052     | -0.196 |
| place5     | 11.076 | 11.092 | +0.016     | -0.200 |
| place6     | 10.417 | 10.532 | +0.116     | -0.103 |
| top3_box   | 9.410  | 9.445  | +0.035     | -0.090 |
| fukusho_2p | 74.912 | 74.838 | -0.074     | -0.244 |

Gate: `primaries_passed=0/3`, `lb95_positive=0/3`, `worst_delta=-0.074`
(within the `-0.05` no-reg bound, so the lever is flat rather than harmful
pooled) → **ACCEPT_strict_gate=false**.

Per-fold (top1/place2/place3 delta[LB95]):

| Fold | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 2023 | -0.048[-0.376] | -0.019[-0.415] | +0.203[-0.222] |
| 2024 | +0.029[-0.299] | +0.319[-0.106] | +0.087[-0.299] |
| 2025 | +0.164[-0.203] | +0.116[-0.261] | -0.125[-0.569] |

Per-seed (top1/place2/place3 delta[LB95]):

| Seed | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 42   | +0.174[-0.145] | +0.232[-0.174] | +0.164[-0.232] |
| 101  | -0.106[-0.453] | +0.174[-0.222] | +0.048[-0.338] |
| 2026 | +0.077[-0.232] | +0.010[-0.396] | -0.048[-0.463] |

Weakly positive on average but never LB95>0, and the sign flips per fold/seed
on at least one primary each time (2023 top1/place2 negative but place3
positive; 2025 place3 negative; seed101 top1 negative; seed2026 place3
negative) — consistent with noise around a small positive mean, not a stable
effect.

### Summer-restricted (`keibajo_code` in {01 Sapporo, 02 Hakodate, 03 Fukushima, 10 Kokura}, n=2,448)

| Metric | Base   | Cand   | Delta (pp) | LB95   |
| ------ | ------ | ------ | ---------- | ------ |
| top1   | 32.108 | 31.985 | -0.123     | -0.545 |
| place2 | 16.217 | 16.639 | +0.422     | -0.055 |
| place3 | 13.508 | 13.630 | +0.123     | -0.368 |

Gate: `0/3 primaries`, worst_delta -0.1225 (top1) → **REJECT**. `place2` is a
near-miss (+0.42pp but LB95 -0.05, just short of 0) — not adoption evidence on
its own.

### Yoshiba-only (Sapporo+Hakodate, n=936) — the flagship candidate's own target cell

| Metric | Base   | Cand   | Delta (pp) | LB95       |
| ------ | ------ | ------ | ---------- | ---------- |
| top1   | 34.046 | 34.437 | +0.392     | -0.321     |
| place2 | 17.201 | 17.557 | +0.356     | -0.570     |
| place3 | 12.429 | 12.892 | +0.463     | -0.285     |
| place5 | 13.533 | 13.034 | **-0.499** | **-1.247** |

Gate: `0/3 primaries LB95>0`, worst_delta **-0.4986** (place5 regression,
exceeds the `-0.05` no-reg bound) → **REJECT**. All three primaries point
positive in direction here (top1/place2/place3 all +0.35 to +0.46pp) but none
clears LB95>0 at n=936, and place5 regresses meaningfully — the flagship
`sire_yoshiba_top3` signal does not survive contact with a 936-race sample.

### Per-venue breakdown within summer (seed-avg, primaries delta[LB95], n)

| Venue                | top1           | place2         | place3         |
| -------------------- | -------------- | -------------- | -------------- |
| 01 Sapporo (n=504)   | +0.529[-0.463] | +0.860[-0.265] | +0.132[-0.794] |
| 02 Hakodate (n=432)  | +0.232[-0.849] | -0.232[-1.620] | +0.849[-0.386] |
| 03 Fukushima (n=720) | -0.926[-1.759] | +0.324[-0.602] | +0.093[-0.927] |
| 10 Kokura (n=792)    | 0.000[-0.589]  | +0.589[-0.210] | -0.253[-1.221] |

No venue clears LB95>0 on any primary; Fukushima is outright negative on
top1/top3_box/fukusho_2p; Hakodate is negative on place2. Direction is not
even consistent across the two 洋芝 venues (Sapporo positive-leaning,
Hakodate mixed/negative on place2) — undermines the "sire yoshiba aptitude"
hypothesis further, since a real turf-affinity signal should show the same
sign at both 洋芝 venues.

### Cell scan (`keibajo_code` / `kyori_band` / `season_band` / `current_baba_condition`, n>=200, seed-avg)

22 cells x 3 primaries = 66 tests; 4 had `LB95>0` on a primary
(`keibajo_code=06` Nakayama `place3 +0.756[+0.178]` n=1500; `kyori_band=1`
(mile) `place2 +0.661[+0.265]` n=3276; `season_band=2` `top1 +0.454[+0.080]`
n=2495; `current_baba_condition=4` `place2 +1.345[+0.192]` n=347) — a ~6% hit
rate at nominal alpha=0.05 with no multiple-comparison correction, i.e.
consistent with chance. None of the 4 hits are in the target venue/summer
dimension, and none replicate across the other two related primaries at the
same cell.

## Overall conclusion: REJECT — DO-NOT-RETEST

None of the 5 pedigree win-rate candidates (`sire_venue_top3`,
`sire_dist_top3`, `sire_yoshiba_top3`, `damsire_dist_top3`,
`damsire_surface_top3`) clear the accept gate pooled, summer-restricted, or
on the flagship 洋芝-only cell. The pooled effect is a flat, near-zero,
sign-unstable positive mean that never reaches LB95>0 on any primary across 3
folds x 3 seeds. The 洋芝-only cell (the strongest a priori hypothesis, n=936)
shows a promising-looking positive point estimate on all 3 primaries but a
real place5 regression and no LB95>0 — and the two component 洋芝 venues
(Sapporo/Hakodate) don't even agree in sign on `place2`, undermining the
mechanism. Consistent with the sibling `draw_ablation` / `sameday_bias` /
`draw_affinity` REJECT pattern from the same day's masked-lever campaign:
CatBoost depth=8, trained on 250 armB features that already include
sire/damsire win-rate by distance, surface, and grade (`sire_distance_win_rate`,
`sire_track_win_rate`, `dam_sire_distance_win_rate`, `sire_baba_win_rate`,
`sim_sire_win_rate`, `sim_damsire_win_rate`, etc.), already captures whatever
venue/yoshiba-specific bloodline signal exists via tree interactions between
those existing features and `keibajo_code`/`track_code` — the additional
explicit venue/yoshiba/surface-rate columns add no incremental information
the model didn't already have access to. **DO-NOT-RETEST** this exact
candidate set on this baseline; a materially different construction (e.g. a
true joint venue x class x dist x bloodline cell rather than marginal
EB-shrunk axes, or a much larger k or different shrinkage target) would be a
new hypothesis, not a retest.

## Artifacts

- Feature build: `tmp/candidate-jra-pedigree-winrate-clean/build_pedigree_winrate.py`
  -> `tmp/candidate-jra-pedigree-winrate-clean/pedigree_winrate_features.parquet`
  (660,834 rows, 2013-2025)
- Harness: `tmp/candidate-jra-pedigree-winrate-clean/wf_pedigree.py` (includes
  the race_id-sort fix for cell/summer/yoshiba masks — see CRITICAL note above)
- Report: `tmp/candidate-jra-pedigree-winrate-clean/reports/pedigree_winrate.json`
- Logs: `tmp/candidate-jra-pedigree-winrate-clean/wf.log` (fixed re-run, 9.9s),
  `tmp/candidate-jra-pedigree-winrate-clean/wf_prefix.log.bak` (original
  pre-fix run — pooled/per-fold/per-seed numbers identical, cell/summer/yoshiba
  numbers superseded by the fixed re-run)
- Reused (read-only) control-arm models:
  `tmp/candidate-masked-lever-retest/models/base/seed{42,101,2026}/fold-{2023,2024,2025}/model.json`
  (trained by the sibling masked-lever-retest campaign, identical spec/seeds/folds)
