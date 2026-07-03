# JRA Meeting-Day x Waku Engineered-Feature Clean-Baseline WF (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position feature engineering
- **Trigger / USER conditions**: A (keibajo x class x kyori x kaisai-nichime
  meeting-day-of-meet) + B (waku x running-style/corner-passage). Deployed JRA
  model is the clean 250-feature `jra-cb-v9-sim-2013` CatBoost YetiRank
  baseline (armB) after the 2026-07-04 within-race `target_corner_*` /
  `target_running_style_class` leak removal (see `project_target_corner_leak_2026_07_04`
  memory) — levers previously REJECTed against the LEAKED baseline deserve a
  retest, and genuinely NEW engineered interactions (never probed before)
  deserve a first-look WF test on the clean baseline.
- **Background diagnosis**: a summer venue-cell diagnosis
  (`docs/probes/jra-summer-venue-cell-focus-2026-07-04.md`) found inside-waku
  (1-2) rank-1 overconfidence at Kokura/Sapporo/Fukushima (-6.6 to -11.9pp
  top1 vs baseline accuracy) and late-meeting (kaisai_nichime 6+) Sapporo
  weakness (-9.3pp). This probe asks whether strictly-prior meeting-day x waku
  interaction features close any of that gap.
- **Prior related work**: a meeting-day-bias probe was run on the LEAKED
  baseline (`tmp/candidate-jra-meetingday-bias/`, cell-conditioned draw/front
  slopes interacted with per-horse draw/frontness) and was REJECTed
  (near-zero mean deltas, `frac_pos` around 0.2-0.6, no LB95>0 primary). That
  probe used deeper PG history (2004+) to fit cell-level draw/front slopes.
  This probe reuses its PG join pattern
  (`tmp/candidate-leak-clean-retrain/meetingday-bias-handoff/build_meetingday_features.py`)
  but tests a **different, simpler feature family**: same-meet PRIOR-DAY
  inner-waku residual bias (no cell-slope fitting) + raw waku x meetphase and
  horse-early-position x meetphase/straight products, gated on the CLEAN
  baseline rather than the leaked one. Per repo memory, a race-level "nige
  cap" constraint is out of scope; all candidates here are per-horse.

## Candidate columns

All 4 are strictly prior to the current race (same-meet PREVIOUS DAYS only,
or fixed pre-race schedule metadata / past-only horse stats) — no same-race
or future information.

1. **`meet_inside_bias_prior`** — within the current meet at this venue
   (`keibajo_code`, `kaisai_nen`, `kaisai_kai`), over PREVIOUS meeting-days'
   races only (`kaisai_nichime` strictly less than the current race's), the
   cumulative empirical top3-rate of inner-waku (`wakuban<=3`) starters minus
   that venue's TRAIN-period historical inner-waku top3-rate baseline (a
   rail-wear / track-bias-evolution-within-the-meet proxy: is this specific
   meet currently running more or less inside-biased than the venue's
   long-run norm?). The residual (not the raw rate) isolates "is this meet
   currently favoring the rail" from the venue's static baseline, which the
   model's existing `track_bias_inside` feature already captures. `NULL` on
   meeting-day 1 (no prior day in this meet yet). Fold-dependent: the
   TRAIN-only baseline is recomputed per WF fold from `year_str < fold_year`
   data only (mirrors `build_sameday` in
   `tmp/candidate-masked-lever-retest/retest_wf.py`), so it never sees future
   folds. Race-constant (same value for all horses in a race) by design — a
   CatBoost tree can still condition its splits on other per-horse features
   (waku, running style) differently depending on this race-level context.
2. **`waku_x_meetphase`** — `(0.5 - wakuban_norm) * meetphase_norm`, where
   `wakuban_norm = (wakuban-1)/7` and `meetphase_norm = min(kaisai_nichime,8)/8`.
   A raw per-horse interaction: does the current meeting-day phase modulate
   the effect of this horse's own draw position? `wakuban`/`kaisai_nichime`
   are both fixed schedule/draw metadata known before the race — 100%
   odds-free and result-free.
3. **`horse_early_pos_x_meetphase`** — `past_corner_1_norm_avg_5 * meetphase_norm`.
   `past_corner_1_norm_avg_5` (already in the store / armB base feature set)
   is the horse's own PAST-races average first-corner position (prior-only,
   never the current race's corner). Interacting it with meeting-day phase
   asks whether a habitually front-running / closing horse's edge shifts as
   the meet progresses (rail wear should help/hurt front-runners
   differentially as the meet wears on).
4. **`horse_early_pos_x_straight`** — `past_corner_1_norm_avg_5 * course_final_straight_m`.
   Both columns already exist in the store / armB feature set (past-only
   corner stat x fixed course geometry) — asks whether a horse's habitual
   early position interacts with how long the closing straight is (a longer
   straight should reward closers more, hurt habitual front-runners more).

Coverage (validation fold 2023, pre-training sanity check): `meet_inside_bias_prior`
87.7%, `waku_x_meetphase` 100.0%, `horse_early_pos_x_meetphase` 53.0%,
`horse_early_pos_x_straight` 38.7% — all comfortably above the 30% floor (the
two `past_corner_1_norm_avg_5`-based interactions inherit that column's
coverage, which is NULL for horses with fewer than the requisite past-race
history).

## Method

- **Harness**: `tmp/candidate-jra-meetingday-waku-clean/wf.py` (adapted from
  `tmp/candidate-masked-lever-retest/retest_wf.py`), feature build:
  `tmp/candidate-jra-meetingday-waku-clean/build_features.py`.
- **Meeting-day meta**: `kaisai_kai` / `kaisai_nichime` / `wakuban` are not in
  the eval store and are joined from local Postgres (`jvd_se`,
  `postgresql://127.0.0.1:15432/horse_racing`) keyed to the store's
  `(race_id, umaban)` universe, JRA venues `01`-`10`, `kaisai_nen>=2013`. The
  prior-day-in-meet inner-waku aggregation (`raw_inside_meet`) is computed
  entirely in DuckDB via an expanding window (`rows between unbounded
preceding and 1 preceding`, partitioned by meet, ordered by
  `kaisai_nichime`) — same leak-free pattern as the deployed `track_bias_*`
  features and the masked-lever `sameday_bias` candidate.
- **Baseline (control, both arms)**: CLEAN `armB` from
  `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json` — 250 feat,
  the live `jra-cb-v9-sim-2013` spec, leak-free by construction.
- **Treatment**: control + the 4 candidate columns above (additive).
- **Model**: CatBoost YetiRank, `iterations=300, depth=8, lr=0.05, l2=3.0`, no
  early-stop, `cat_indices=[]` (all-numeric) — matches deployed
  `jra-cb-v9-sim-2013` exactly.
- **Folds**: 3 blind walk-forward folds, train `2013..Y-1` / test `Y`, for
  `Y in {2023, 2024, 2025}`.
- **Seeds**: multi-seed, `seed_base in {42, 101, 2026}` (`seed = seed_base +
fold_year` per fold), pooled via per-race hit-rate averaging across seeds
  before the paired bootstrap.
- **Metrics**: exact-ordinal `top1`/`place2`..`place6`, `top3_box` (set
  equality of predicted vs actual top-3), `fukusho_2p` (any predicted top-2
  finished <=2). Primaries = `{top1, place2, place3}`.
- **Significance**: paired race-level bootstrap, 2000 iterations, fixed seed
  20260519, `delta = treatment - control`.
- **Accept gate**: >=2 of 3 primaries have `delta_pp >= +0.08` AND `LB95 > 0`;
  AND >=1 of `{place2, place3}` passes; AND no metric regresses below
  `-0.05pp`.
- **Cell eval**: pooled seed-avg re-cut by `keibajo_code` / `kyori_band` /
  `season_band` / `current_baba_condition` / `waku_band` (inner 1-2 / mid 3-6
  / outer 7-8) / `meetday_band` (1-2 / 3-5 / 6+) / `kyoso_joken_code`, `n>=200`
  per cell. Plus a SUMMER-RESTRICTED subset (`keibajo_code` in
  `{01 Sapporo, 02 Hakodate, 03 Fukushima, 10 Kokura}`) pooled + gated, and
  summer x `{keibajo_code, waku_band, meetday_band}` cross-cells at `n>=100`
  (multiple-comparison caution applies throughout).

## Result

### Harness bug caught and fixed mid-run

Before trusting any masked-cell number, note: `paired()` sorts its two input
frames by `race_id` internally before joining, but the frames the cell/summer
masks are computed against come from `group_by("race_id")` output, whose row
order polars does **not** guarantee. The teammate running the masked-lever
retest hit exactly this bug (3 phantom summer-cell "signals" in
`tmp/candidate-masked-lever-retest/retest_wf.py`, since fixed upstream) and
flagged it mid-run here. This harness had inherited the identical pattern in
`summer_rep`/`summer_gate`, `cell_report`, and `summer_cross`. The fix
(`.sort("race_id")` on the frame **before** computing any boolean mask from
it, so the mask's row order matches `paired()`'s post-sort order) was applied
to `tmp/candidate-jra-meetingday-waku-clean/wf.py`, but the already-running
training process had the pre-fix bytecode compiled in memory (editing a `.py`
file does not affect an already-running interpreter), so the first completed
run (archived as `reports/meetingday_waku.PRE_SORT_FIX.json`) still had the
bug in its summer/cell sections. All 18 CatBoost models were already cached
on disk, so `wf.py` was simply re-invoked -- `train_fold()` skips training
when the model file exists, so the rerun reused every model and only
recomputed predictions + hit-rates + bootstrap (8.4s total). **The fix
changed the summer-restricted numbers materially**: pre-fix `place2` showed
`+0.4766pp [LB95 -0.0003]` (a near-miss); post-fix it is `-0.2315pp [LB95
-0.7217]` (a clear negative) -- confirming the pre-fix summer number was a
misalignment artifact, not a real near-miss. The pooled/per-seed/per-fold
numbers (`paired()` called with `mask=None`) were never affected by this bug
and are identical between the two runs. All numbers below are from the
POST-FIX run (`reports/meetingday_waku.json`).

### Pooled (seed-avg, 3 folds x 3 seeds, n=10,365 races)

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 33.796 | 33.893 | +0.097     | -0.087 |
| place2     | 18.119 | 18.222 | +0.103     | -0.109 |
| place3     | 14.163 | 14.115 | -0.048     | -0.264 |
| place4     | 12.166 | 12.050 | -0.116     | -0.325 |
| place5     | 11.076 | 11.098 | +0.023     | -0.180 |
| place6     | 10.416 | 10.542 | +0.125     | -0.064 |
| top3_box   | 9.410  | 9.429  | +0.019     | -0.097 |
| fukusho_2p | 74.912 | 74.951 | +0.039     | -0.116 |

**Gate: `primaries_passed=0/3`, `lb95_positive=0/3`, `worst_delta=-0.1158`
(place4, exceeds the `-0.05` no-reg bound) → ACCEPT_strict_gate=false.** top1
and place2 are weakly positive but never LB95>0; place4 shows an actual
(mild) regression outside the no-reg tolerance.

Per-seed (top1/place2/place3 delta[LB95]):

| Seed | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 42   | +0.251[-0.048] | +0.299[-0.097] | +0.077[-0.309] |
| 101  | -0.077[-0.396] | -0.029[-0.406] | -0.241[-0.618] |
| 2026 | +0.116[-0.203] | +0.039[-0.348] | +0.019[-0.357] |

Per-fold (top1/place2/place3 delta[LB95]):

| Fold | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 2023 | -0.116[-0.434] | -0.338[-0.723] | -0.135[-0.521] |
| 2024 | +0.290[-0.019] | +0.647[+0.280] | +0.106[-0.328] |
| 2025 | +0.116[-0.183] | +0.000[-0.376] | -0.116[-0.473] |

Sign flips fold-to-fold (2023 all-negative, 2024 all-positive with `place2`
even clearing LB95>0 in isolation, 2025 mixed) and seed-to-seed -- consistent
with noise around zero rather than a real, stable effect. 2024 alone would
have passed the gate on `place2`, but no fold passes on 2/3 primaries
simultaneously, and the effect does not replicate across the other two folds.

### Summer-restricted (keibajo in {01 Sapporo, 02 Hakodate, 03 Fukushima, 10 Kokura}, n=2,448 races)

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 32.108 | 32.380 | +0.272     | -0.123 |
| place2     | 16.217 | 15.986 | -0.231     | -0.722 |
| place3     | 13.508 | 13.371 | -0.136     | -0.613 |
| place4     | 11.560 | 11.629 | +0.068     | -0.381 |
| place5     | 11.234 | 11.166 | -0.068     | -0.517 |
| place6     | 10.063 | 10.104 | +0.041     | -0.354 |
| top3_box   | 8.456  | 8.633  | +0.177     | -0.041 |
| fukusho_2p | 72.358 | 72.290 | -0.068     | -0.436 |

**Gate: `primaries_passed=0/3`, `worst_delta=-0.2315` (place2, a real
regression well outside the no-reg bound) → ACCEPT_strict_gate=false.** In
the exact subset this probe was designed for, the candidate is net negative:
`place2`/`place3` both regress, and only `top1` is weakly (non-significantly)
positive.

### Weak-cell check: does the candidate fix the known summer inner-waku / late-meeting gaps?

**No -- the inner-waku cell gets worse, not better.** Summer subset x
`waku_band` (n>=100, seed-avg):

| waku_band (summer only) | n    | top1 delta[LB95]   | place2 delta[LB95] | place3 delta[LB95] |
| ----------------------- | ---- | ------------------ | ------------------ | ------------------ |
| inner_1_2 (waku 1-2)    | 489  | **-0.750[-1.636]** | -0.000[-1.091]     | -0.682[-1.772]     |
| mid_3_6                 | 1262 | +0.185[-0.370]     | -0.581[-1.294]     | -0.370[-1.057]     |
| outer_7_8               | 697  | +1.148[+0.526]     | +0.239[-0.670]     | +0.670[-0.239]     |

The venue-cell this probe was diagnosed against (inner-waku overconfidence at
Kokura/Sapporo/Fukushima) is the one that regresses hardest under the
candidate (`top1 -0.75pp`, tightest LB95 of the three bands at -1.64,
n=489) -- the opposite of the intended fix. `outer_7_8` (the summer band
NOT flagged as weak) is the one that improves and is the only band/cell in
this whole probe with `LB95>0` on a primary (`top1 +1.148[+0.526]`), which is
an argument against `meet_inside_bias_prior`/`waku_x_meetphase` doing
anything cell-specific and useful -- if anything it shifts accuracy away from
the diagnosed weak cell.

Per-venue (all folds pooled, not summer-restricted since 3 of these 4 venues
are summer-only; `n>=200` seed-avg):

| keibajo_code | n   | top1 delta[LB95]   | place2 delta[LB95] | place3 delta[LB95] |
| ------------ | --- | ------------------ | ------------------ | ------------------ |
| 01 Sapporo   | 504 | **+1.191[+0.132]** | -0.066[-1.190]     | +0.331[-0.595]     |
| 02 Hakodate  | 432 | +0.077[-0.849]     | **-1.157[-2.392]** | -0.386[-1.620]     |
| 03 Fukushima | 720 | +0.185[-0.509]     | +0.093[-0.880]     | -0.185[-1.250]     |
| 10 Kokura    | 792 | -0.126[-0.716]     | -0.126[-0.884]     | -0.253[-1.052]     |

Sapporo's `top1` is the single positive-LB95 primary anywhere in this whole
cell sweep (n=504, `+1.19pp`) -- but `place2` at that same cell is flat/negative
(`-0.066[-1.190]`), so it fails the multi-metric gate even at the cell level,
and per repo convention a single positive metric among ~10 keibajo x 3
primaries = 30 comparisons is exactly the kind of result the
multiple-comparison caution warns against, not itself adoption evidence.
Hakodate, meanwhile, shows a large and fairly tight-interval `place2`
regression (`-1.157pp`, LB95 `-2.39`) -- the clearest genuinely-negative
signal in the whole sweep.

Late-meeting check: summer subset x `meetday_band` (n>=100, seed-avg) -- a
Sapporo-specific late-meeting cross-cell would fall below the `n>=100`
threshold (Sapporo alone is `n=504` across all 3 folds; late-meeting-only
would be a small fraction of that), so this reports the pooled
4-venue summer late-meeting cell instead:

| meetday_band (summer only) | n   | top1 delta[LB95] | place2 delta[LB95] | place3 delta[LB95] |
| -------------------------- | --- | ---------------- | ------------------ | ------------------ |
| day_1_2                    | 648 | +0.206[-0.463]   | -0.103[-1.132]     | +0.412[-0.617]     |
| day_3_5                    | 972 | +0.206[-0.446]   | -0.171[-0.892]     | -0.412[-1.303]     |
| day_6plus (late-meeting)   | 828 | +0.403[-0.201]   | -0.403[-1.288]     | -0.242[-0.966]     |

`day_6plus` (the band diagnosed as weak for Sapporo) shows a non-significant
`top1` improvement but a fairly large `place2` regression (`-0.40pp`) --
mixed at best, and the `place2` direction is negative exactly like the
pooled and summer-overall results. No evidence the candidate closes the
late-meeting gap.

### Other cells (n>=200, seed-avg, full population)

`kyori_band`, `season_band`, `current_baba_condition`, `kyoso_joken_code`:
no cell in any of these dimensions clears the 2-of-3-primaries-with-LB95>0
bar; scattered single-metric-single-cell positives (`season_band=2` place2
`+0.454[+0.027]`, `kyori_band` long-distance place3 `+0.50[-0.33]` n.s.,
`current_baba_condition=3` (heavy) place3 `+0.66[-0.08]` n.s.) are consistent
with the ~40-cell-wide multiple-comparison noise floor, not a coherent
signal -- none replicate across an adjacent cell in the same dimension family.
Full cell tables: `tmp/candidate-jra-meetingday-waku-clean/reports/meetingday_waku.json`
(`cells_ge200_seedavg`, `summer_cross_cells_ge100`).

## Conclusion: REJECT

None of the 4 candidates (`meet_inside_bias_prior`, `waku_x_meetphase`,
`horse_early_pos_x_meetphase`, `horse_early_pos_x_straight`, evaluated as one
additive bundle per the task spec) clear the accept gate globally
(`0/3 primaries`, `LB95` never positive) or on the summer-restricted subset
(`0/3 primaries`, and `place2`/`place3` actively regress `-0.23/-0.14pp`).
Most notably, the specific diagnosed weak cell (summer inner-waku,
Kokura/Sapporo/Fukushima 1-2) gets **worse** under the candidate
(`top1 -0.75pp`, tightest LB95 of any waku band at `-1.64`), the opposite of
what the probe hoped to find -- CatBoost depth=8 already captures the
inside-waku / meeting-day-evolution signal through its existing
`track_bias_inside`/`track_bias_front` (trailing-window) features plus venue

- weather + track-condition context, and adding a race-constant residual bias
  term plus 3 raw interaction products does not help it route around the
  serve-time weakness, it just adds a moderately noisy 4-feature perturbation
  that happens to redistribute error toward, not away from, the target cells.
  One isolated cell (Sapporo overall `top1 +1.19pp[LB95 +0.13]`) is
  LB95-positive but fails the multi-metric requirement at that same cell
  (`place2` flat/negative) and is not distinguishable from the
  ~40-cell-wide multiple-comparison noise floor documented across every other
  cell in this sweep. **DO-NOT-RETEST** this exact feature family; a genuinely
  different mechanism (not a residualized meet-day bias term, not raw
  draw/meetphase products) would be needed to move the inner-waku-overconfidence
  or late-meeting-Sapporo cells, and prior related probes
  (`project_jra_rs_cell_routing_reject_2026_07_03`,
  `project_venue_cell_round2_2026_06_20`) already found venue/cell-specific
  routing structurally difficult for this model family.

## Artifacts

- Feature build: `tmp/candidate-jra-meetingday-waku-clean/build_features.py`
  (PG meeting-day meta join + prior-day-in-meet expanding aggregate, 0.7s)
- Harness: `tmp/candidate-jra-meetingday-waku-clean/wf.py` (includes the
  sort-before-mask fix noted above)
- Feature parquet: `tmp/candidate-jra-meetingday-waku-clean/md_waku_features.parquet`
- Models: `tmp/candidate-jra-meetingday-waku-clean/models/{base,cand}/seed*/fold-*/model.json`
  (18 total, reused across the pre-fix and post-fix runs)
- Reports: `tmp/candidate-jra-meetingday-waku-clean/reports/meetingday_waku.json`
  (post-fix, authoritative) and `meetingday_waku.PRE_SORT_FIX.json` (archived,
  summer/cell sections unreliable -- kept only as a record of the bug's
  impact, do not cite its summer/cell numbers)
- Logs: `tmp/candidate-jra-meetingday-waku-clean/wf.log` (pre-fix training run,
  1296.5s, all 18 models) and `wf_rerun.log` (post-fix recompute-only rerun,
  8.4s, reused cached models)
