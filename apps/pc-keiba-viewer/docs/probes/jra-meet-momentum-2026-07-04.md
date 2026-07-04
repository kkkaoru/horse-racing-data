# JRA Within-Meet Jockey/Trainer Momentum Features — Clean-Baseline WF Test (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position feature engineering
- **USER condition**: C — 競馬場×class×距離×開催日数×騎手の勝率 (venue x class x
  distance x meeting-day x jockey win-rate interactions), tested here as a
  **DYNAMIC within-meet momentum** hypothesis rather than the static career-rate
  construction already REJECTED
  (`docs/probes/jra-jockey-winrate-clean-2026-07-04.md`,
  `jockey_venue_dist_win_eb`/`jockey_venue_dist_top3_eb`/`jockey_meetphase_win_eb`/
  `jockey_summer_venue_top3_eb`/`_edge` — all pooled OVER a jockey's entire history
  at a venue/dist-band/meeting-phase cell, DO-NOT-RETEST).
- **Hypothesis**: a jockey's form WITHIN the current meet (hot hand at this venue
  this week — reading the turf, local pace, track feel) carries signal beyond his
  static career rates. Candidate columns express this as a DELTA between the
  jockey's (and trainer's) very recent same-meet form and his own career baseline,
  plus an exposure count so CatBoost can gate the delta by sample size.
- **Baseline**: deployed JRA model, CLEAN 250-feature CatBoost YetiRank
  (`jra-cb-v9-sim-2013`, armB from `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`),
  leak-free as of 2026-07-04 (`target_corner_*` / `target_running_style_class`
  excluded).

## Candidate columns

Keyed by `(race_id, ketto_toroku_bango)`, built in
`tmp/candidate-jra-meet-momentum/build_meet_momentum.py`:

| Column                  | Definition                                                                                          |
| ----------------------- | --------------------------------------------------------------------------------------------------- |
| `jockey_meet_momentum`  | same-meet PRIOR top3 rate minus jockey's career EB top3 rate as-of (own-baseline delta, not raw)    |
| `jockey_meet_rides`     | count of same-meet prior rides (exposure/confidence proxy; lets CatBoost gate the delta by n)       |
| `jockey_meet_win_delta` | same-meet PRIOR win rate minus jockey's career EB win rate as-of                                    |
| `trainer_meet_momentum` | same construction for `chokyoshi_code` (stables are known to ship strings hot/cold to summer meets) |

## Method

- **Builder**: `tmp/candidate-jra-meet-momentum/build_meet_momentum.py`. Source:
  local Postgres (port 15432) `jvd_se`, JRA venues 01-10, `kaisai_nen >= 2008` for
  history depth / `>= 2013` for output rows.
- **Leak-free construction**: "same-meet prior" = strictly earlier
  `kaisai_nichime` within the same `(keibajo_code, kaisai_nen, kaisai_kai)` meet,
  OR the same `nichime` with a strictly earlier `race_bango` — i.e. only rides
  that happened before this exact race, ordered by `(nichime, race_bango)`,
  `rows between unbounded preceding and 1 preceding`, partitioned by
  `(kishu_code | chokyoshi_code, keibajo_code, kaisai_nen, kaisai_kai)`. Race 1
  of meet day 1 (jockey/trainer's first ride of the meet) has
  `jockey_meet_rides=0` and momentum/win_delta = NULL by construction — expected,
  not a bug (`count(*)` over an empty window frame is 0 in DuckDB, but
  `sum()` is NULL, verified before writing the SQL). Career EB baseline uses a
  SEPARATE, fully global expanding window (all JRA venues/years, strictly prior
  by a `(date, venue, race_bango, umaban)` sequence key), shrunk toward the JRA
  2013+ global base rate with `K_CAREER=50` (own-baseline should stay stable for
  low-n jockeys/trainers, unlike the raw same-meet rate which is intentionally
  left un-shrunk so the tree can learn to gate it via `jockey_meet_rides`).
  No horse-level self-joins — same expanding-window-over-race-level-aggregates
  pattern as `tmp/candidate-masked-lever-retest/build_draw_ablation.py` v2 (the
  O(n^2) self-join that exhausted host memory in an earlier lever was avoided
  from the start here since the meet partitions are naturally small, ~1-30 rows).
  Odds-free by construction: source = `jvd_se` result (`kakutei_chakujun`) only.
- **Coverage**: 660,834 output rows (2013+). `jockey_meet_momentum` /
  `jockey_meet_win_delta` 94.01% non-null (5.99% NULL = jockey's first ride of
  the meet, matches `jockey_meet_rides=0` count exactly). `jockey_meet_rides`
  100% (always defined, 0 for first ride). `trainer_meet_momentum` 87.8%
  non-null (trainers run smaller same-meet strings than jockeys ride, so more
  first-of-meet NULLs).
- **Harness**: `tmp/candidate-jra-meet-momentum/wf_gate.py` (own copy, modeled on
  `tmp/candidate-jra-jockey-winrate-clean/wf_gate.py`).
- **Model**: CatBoost YetiRank, `iterations=300, depth=8, lr=0.05, l2=3.0`, no
  early-stop, `cat_indices=[]` (all-numeric), matches deployed `jra-cb-v9-sim-2013`
  exactly. The seed_base=42 BASE models were reused directly from the shared
  deployed-baseline checkpoint
  (`tmp/candidate-leak-clean-retrain/models_jra_v9sim/armB/fold-*/model.json`,
  verified `random_seed=2065/2066/2067 = 42+fold_year` on disk) instead of
  retraining an identical model — only seed101/seed2026 base models and all 9
  candidate models were trained fresh.
- **Folds**: 3 blind walk-forward folds, train `2013..Y-1` / test `Y`, for
  `Y in {2023, 2024, 2025}`.
- **Seeds**: multi-seed, `seed_base in {42, 101, 2026}` (`seed = seed_base +
fold_year` per fold), pooled via per-race hit-rate averaging across seeds
  before the paired bootstrap.
- **Metrics**: exact-ordinal `top1`/`place2`..`place6`, `top3_box`, `fukusho_2p`.
  Primaries = `{top1, place2, place3}`.
- **Significance**: paired race-level bootstrap, 2000 iterations, fixed seed
  20260519, `delta = treatment − control`.
- **Accept gate** (`docs/finish-position-prediction-system.md` §7.2): >=2 of 3
  primaries have `delta_pp >= +0.08` AND `LB95 > 0`; AND >=1 of `{place2,
place3}` passes; AND no metric regresses below `-0.05pp`.
- **Cell eval**: pooled seed-avg re-cut by `keibajo_code` (venue) / `grade_code`
  (class) / `kyori_band` (dist-band) / `meetday_band` (`d1-2`/`d3-5`/`d6+` from
  the candidate parquet's raw `nichime`) / `rides_band` (`jockey_meet_rides`
  exposure bucket: `0`/`1-2`/`3-5`/`6-10`/`11+`), `n >= 200` per cell. Plus a
  SEPARATE summer-venue-restricted slice (`is_summer=1`, venues 01/02/03/10 only)
  with the same 5 cell dims at `n >= 100` — the **PRIMARY TARGET per task brief**.
  Cell masks are computed against the same `race_id`-sorted frames used by the
  paired bootstrap (the alignment bug fixed upstream in
  `tmp/candidate-masked-lever-retest/retest_wf.py` and re-fixed in the sibling
  jockey-winrate probe: `paired()` internally re-sorts by `race_id`, so any mask
  built off an unsorted `group_by()` frame would silently misalign — both `ba`
  and `ca` here are explicitly `.sort("race_id")`'d before `cell_scan()`).

## Result: pooled (seed-avg, n=10,365 races)

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 33.796 | 33.967 | +0.170     | -0.006 |
| place2     | 18.119 | 18.247 | +0.129     | -0.077 |
| place3     | 14.163 | 14.282 | +0.119     | -0.103 |
| place4     | 12.166 | 12.053 | -0.113     | -0.325 |
| place5     | 11.076 | 10.989 | -0.087     | -0.289 |
| place6     | 10.416 | 10.449 | +0.032     | -0.164 |
| top3_box   | 9.410  | 9.426  | +0.016     | -0.090 |
| fukusho_2p | 74.912 | 74.855 | -0.058     | -0.203 |

Gate: `primaries_passed=0/3`, `lb95_positive=0/3` (top1's LB95 of -0.0064 is the
closest of the three to crossing zero — the point estimate is positive on all 3
primaries but none clear the 95% lower bound), `worst_delta=-0.1126` (place4,
outside the `-0.05` no-reg bound, a mild regression) →
**ACCEPT_strict_gate=false**.

Per-fold (top1/place2/place3 delta[LB95]):

| Fold | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 2023 | +0.019[-0.309] | -0.106[-0.482] | +0.048[-0.367] |
| 2024 | +0.425[+0.116] | +0.540[+0.164] | +0.425[+0.058] |
| 2025 | +0.068[-0.241] | -0.048[-0.415] | -0.116[-0.463] |

**2024 alone clears the accept gate on all 3 primaries** (LB95>0 on top1/place2/
place3). 2023 and 2025 are flat-to-negative. The entire pooled positive drift is
driven by one of three blind folds — the same "single fold carries the pooled
result" failure mode documented across this campaign (2024 was also the standout
fold in the sibling jockey-winrate-clean REJECT, though weaker there: only
place2 crossed LB95 in that probe's 2024 fold, not top1/place3).

Per-seed (top1/place2/place3 delta[LB95]):

| Seed | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 42   | +0.569[+0.270] | +0.222[-0.154] | +0.174[-0.193] |
| 101  | -0.145[-0.473] | +0.029[-0.367] | +0.087[-0.289] |
| 2026 | +0.087[-0.232] | +0.135[-0.261] | +0.097[-0.299] |

`top1` flips sign entirely on seed101 (negative, LB95 -0.473) despite seed42
showing a strong, LB95-positive top1 result (+0.569[+0.270]) — the same seed
alone would have passed the individual-primary bar. Combined with the per-fold
non-replication, this is the standard noise signature seen throughout the
campaign: a strong point estimate in a minority of folds/seeds, washed out and
sign-flipped in the majority, never stable enough to survive pooling.

## Summer-restricted result (is_summer=1, venues 01/02/03/10, n=2,448 races) — PRIMARY TARGET

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 32.108 | 32.435 | +0.327     | -0.096 |
| place2     | 16.217 | 16.245 | +0.027     | -0.449 |
| place3     | 13.508 | 13.712 | +0.204     | -0.259 |
| place4     | 11.560 | 11.656 | +0.095     | -0.327 |
| place5     | 11.234 | 11.029 | -0.204     | -0.654 |
| place6     | 10.063 | 9.831  | -0.231     | -0.626 |
| top3_box   | 8.456  | 8.524  | +0.068     | -0.123 |
| fukusho_2p | 72.358 | 72.141 | -0.218     | -0.545 |

Summer gate: `primaries_passed=0/3`, `lb95_positive=0/3`, `worst_delta=-0.2315`
(place6, well outside the `-0.05` no-reg bound) → **ACCEPT_strict_gate=false**.
The summer-restricted slice is noisier and more negative than the global
pooled result on every non-primary metric (place5/place6/fukusho_2p all
regress > -0.2pp) — the within-meet momentum hypothesis does not hold up even
when restricted to exactly the 4 venues (summer meets) it was designed for.

## Cell highlights (n>=200 global / n>=100 summer, multiple-comparison caution)

- **Venue** (`keibajo_code`, global): `08` (Kyoto, n=1535) is the strongest hit —
  `top1 +0.717[LB95 +0.261]`, a genuinely LB95-positive cell, though `place2`/
  `place3` at the same cell are flat/negative (`+0.174[-0.326]`, `-0.043[-0.543]`).
  `01` (Sapporo, n=504) again lands right on the zero boundary
  (`top1 +1.058[LB95 +0.0000]`) — notably the SAME venue and the SAME
  rounds-to-exactly-zero pattern seen in the sibling jockey-winrate-clean probe
  (`top1 +0.926[LB95 +0.0000]` there too), suggesting this is a recurring
  small-n (n~500) artifact at this specific venue rather than a real effect of
  either candidate feature set.
- **Class** (`grade_code`): blank-grade ordinary conditions (n=7,570, ~73% of all
  races — the best-powered cell in the whole scan) shows `place2 +0.260[LB95
+0.013]`, a genuine but very thin LB95-positive hit; `top1 +0.154[-0.053]` and
  `place3 +0.216[-0.044]` at the same cell are close but don't cross.
- **Dist-band** (`kyori_band`): band `1` (mile, 1400-1799m, n=3,276) is the
  clearest cell in the entire scan — `top1 +0.499[LB95 +0.193]` AND
  `place2 +0.458[LB95 +0.071]` both clear the bar (2/3 primaries LB95-positive
  at a well-powered n), though `place3` does not (`+0.061[-0.336]`).
- **Meeting-day-band** (`meetday_band`, new dim): `d1-2` (early, n=2,591) is
  directionally the most positive of the three bands (`top1 +0.335[-0.052]`,
  `place2 +0.386[-0.026]`) but neither crosses LB95>0 — echoes but does not
  confirm the single noise-level early-meeting-day hit from the sibling
  jockey-winrate-clean probe (that probe's `place2 +0.425[LB95 +0.013]` at
  `nichime_bucket=early` was explicitly called noise, not motivation; this
  probe's `d1-2` point estimate is directionally consistent but the LB95 stays
  negative here).
- **Rides-band** (`rides_band`, jockey's same-meet exposure count): `0` (jockey's
  first ride of the meet, n=605 — exactly the subgroup where
  `jockey_meet_momentum`/`jockey_meet_win_delta` are NULL by construction) shows
  the single strongest hit in the whole scan: `top1 +1.102[LB95 +0.386]` and
  `place2 +1.763[LB95 +0.882]`, both clearly LB95-positive with a large margin.
  Since the jockey-side candidate columns are NULL in exactly this subgroup, any
  genuine effect here can only come from `trainer_meet_momentum` (non-null
  whenever the trainer, as opposed to the jockey, has same-meet history) or from
  `jockey_meet_rides=0` itself acting as a "new to this meet" flag correlated
  with something else the model already captures. At n=605 (5.99% of all races)
  this does not replicate in the summer-restricted cut of the same band
  (`rides_band=0`, summer-only, n=138: `place2 +1.691[LB95 -0.966]` — same
  positive direction, doesn't clear LB95 at the smaller n) — consistent with a
  single well-powered-by-chance cell rather than a confirmed effect, but the
  most promising signal in the scan and worth a narrow, non-EB-shrunk,
  trainer-only follow-up if this thread is revisited (see caution below).

## Verdict: **REJECT** (global and summer-restricted)

None of the 4 engineered within-meet momentum columns
(`jockey_meet_momentum`, `jockey_meet_rides`, `jockey_meet_win_delta`,
`trainer_meet_momentum`) clear the accept gate, pooled or summer-restricted.
The pooled positive drift on all 3 primaries (top1 +0.17, place2 +0.13, place3
+0.12) never reaches LB95>0, and per-fold/per-seed decomposition shows this is
driven almost entirely by a single fold (2024, which alone clears the gate on
all 3 primaries) and is not stable across seeds (top1 flips sign on seed101).
This is the identical failure signature documented in the sibling static
career-rate probe (`jra-jockey-winrate-clean-2026-07-04.md`) and consistent
with the broader campaign pattern (`project_cell_campaign_2026_07_02`,
13 REJECTED lever families) — CatBoost depth=8 with the CLEAN 250-feat armB
already captures enough jockey/trainer/venue signal that a dynamic same-meet
momentum delta adds no measurable, stable incremental accuracy. The
summer-restricted primary target (the specific test this candidate was
designed to pass) fails more decisively than the global pool, with larger
non-primary regressions (place5/place6/fukusho_2p all worse than -0.2pp).

**DO-NOT-RETEST** this exact feature set (jockey/trainer same-meet momentum
delta vs. career EB baseline, `K_CAREER=50`). If this thread is revisited,
the two candidates worth a narrow, well-powered follow-up rather than folding
back into a global scalar are: (1) the `rides_band=0` cell (jockey's first ride
of the meet, n=605, `place2 +1.763[LB95 +0.882]`) — isolate whether
`trainer_meet_momentum` alone (not bundled with the 3 jockey columns) drives
this, since jockey momentum is NULL there by construction; and (2) the mile
dist-band cell (`kyori_band=1`, n=3,276, 2/3 primaries LB95-positive) — test
whether a `jockey_meet_momentum x mile-dist-band` interaction is more stable
than the raw pooled delta. Neither is itself actionable without a dedicated,
larger, blind-holdout-confirmed probe (`feedback_hpo_selection_bias_blind_holdout`).

## Artifacts

- Builder: `tmp/candidate-jra-meet-momentum/build_meet_momentum.py`
- Feature parquet: `tmp/candidate-jra-meet-momentum/meet_momentum_features.parquet`
- Harness: `tmp/candidate-jra-meet-momentum/wf_gate.py`
- Report: `tmp/candidate-jra-meet-momentum/reports/meet_momentum.json`
- Logs: `tmp/candidate-jra-meet-momentum/{build_meet_momentum,wf_gate}.log`
