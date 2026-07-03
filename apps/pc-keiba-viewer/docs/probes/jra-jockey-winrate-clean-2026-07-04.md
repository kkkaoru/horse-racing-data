# JRA Jockey Win-Rate Interaction Features — Clean-Baseline WF Test (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position feature engineering
- **USER condition**: C — 競馬場×class×距離×開催日数×騎手の勝率 (venue x class x
  distance x meeting-day x jockey win-rate interactions), with a specific focus on
  the 4 summer venues (01 Sapporo, 02 Hakodate, 03 Fukushima, 10 Kokura).
- **Baseline**: deployed JRA model, CLEAN 250-feature CatBoost YetiRank
  (`jra-cb-v9-sim-2013`, armB from `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`),
  leak-free as of 2026-07-04 (`target_corner_*` / `target_running_style_class`
  excluded). Not a forbidden retest: the June jockey x venue probe
  (`tmp/candidate-jra-jockey-pedigree-cell/`) was gated on the LEAKED baseline;
  this is a clean-baseline test with richer interactions.

## What armB already has vs what's absent

armB carries jockey features as SEPARATE scalars, never jointly interacted:
`jockey_keibajo_win_rate` (venue alone), `jockey_distance_win_rate` (distance
alone), `jockey_grade_win_rate` (class alone), plus career/recent/pair/style
rates. `kaisai_nichime` (meeting-day) is entirely absent from armB — no feature
anywhere carries it. Per task instructions, only genuinely-absent interactions
were engineered:

| Candidate col                 | Absent dimension it adds                                                                         |
| ----------------------------- | ------------------------------------------------------------------------------------------------ |
| `jockey_venue_dist_win_eb`    | joint venue x dist-band cell (finer than either scalar alone)                                    |
| `jockey_venue_dist_top3_eb`   | same cell, top3 rate                                                                             |
| `jockey_meetphase_win_eb`     | meeting-day-phase (kaisai_nichime bucket), absent entirely                                       |
| `jockey_summer_venue_top3_eb` | pooled top3 rate across the 4 summer venues specifically                                         |
| `jockey_summer_venue_edge`    | signed differential vs jockey's own overall top3 rate (isolates summer-circuit-specialist skill) |

`(d) jockey_x_class_win` was skipped — redundant with the existing
`jockey_grade_win_rate` armB column.

## Method

- **Builder**: `tmp/candidate-jra-jockey-winrate-clean/build_jockey_winrate.py`.
  Source: local Postgres (port 15432) `jvd_se` join `jvd_ra`, JRA venues
  01-10, `kaisai_nen >= 2008` for history depth / `>= 2013` for output rows.
  Leak-free: every window is `rows between unbounded preceding and 1
preceding` over a strictly-increasing (date, venue, race_no, umaban)
  sequence — prior races only, current race's own outcome never included.
  Empirical-Bayes shrinkage toward the jockey's own running overall
  win/top3 rate (parent), fallback to JRA 2013+ global base rate
  (win=0.0707, top3=0.2268) for brand-new jockeys. Shrinkage constants:
  `K_venue_dist=15`, `K_meetphase=20`, `K_summer_venue=15`.
  Dist-bands: sprint<1400 / mile<1800 / middle<2200 / long>=2200 (matches
  `tmp/candidate-jra-jockey-pedigree-cell/build_jockey_nichime.py`'s scheme).
  Meeting-phase buckets: early (nichime<=2) / mid (3-5) / late (>=6),
  matching `summer_venue_focus.py`'s `nichime_bucket` convention.
  **Bug caught before the run**: `SUM() over an empty window frame` is SQL
  `NULL`, not 0 — the brand-new-cell case (jockey's first-ever race in a
  venue x dist-band cell) leaked `NULL` through the EB formula instead of
  falling back cleanly to the prior. Fixed with `coalesce(...,0)` on all
  window-sum numerators before the run; coverage went from ~98-99% to 100%
  non-null on all 5 candidate columns after the fix (verified,
  `tmp/candidate-jra-jockey-winrate-clean/build_jockey.log`).
- **Harness**: `tmp/candidate-jra-jockey-winrate-clean/wf_gate.py` (own copy,
  modeled on `tmp/candidate-masked-lever-retest/retest_wf.py`).
- **Model**: CatBoost YetiRank, `iterations=300, depth=8, lr=0.05, l2=3.0`,
  no early-stop, `cat_indices=[]` (all-numeric), matches deployed
  `jra-cb-v9-sim-2013` exactly.
- **Folds**: 3 blind walk-forward folds, train `2013..Y-1` / test `Y`, for
  `Y in {2023, 2024, 2025}`.
- **Seeds**: multi-seed, `seed_base in {42, 101, 2026}` (`seed = seed_base +
fold_year` per fold), pooled via per-race hit-rate averaging across seeds
  before the paired bootstrap.
- **Metrics**: exact-ordinal `top1`/`place2`..`place6`, `top3_box` (set
  equality of predicted vs actual top-3), `fukusho_2p` (any predicted
  top-2 finished <=2). Primaries = `{top1, place2, place3}`.
- **Significance**: paired race-level bootstrap, 2000 iterations, fixed
  seed 20260519, `delta = treatment − control`.
- **Accept gate** (`docs/finish-position-prediction-system.md` §7.2): >=2 of
  3 primaries have `delta_pp >= +0.08` AND `LB95 > 0`; AND >=1 of
  `{place2, place3}` passes; AND no metric regresses below `-0.05pp`.
- **Cell eval**: pooled seed-avg re-cut by `keibajo_code` (venue) /
  `grade_code` (class) / `kyori_band` (dist-band) / `nichime_bucket`
  (meeting-day-band, new dim from the candidate parquet), `n >= 200` per
  cell. Plus a SEPARATE summer-venue-restricted slice
  (`is_summer=1`, venues 01/02/03/10 only) with the same 4 cell dims at
  `n >= 100`, multiple-comparison caution applied throughout.

**Cell-mask alignment bug (caught before reporting)**: mid-run, a teammate
found a real bug in the shared cell-eval pattern (`retest_wf.py`): the paired
bootstrap helper `sort()`s rows by `race_id` internally before joining, but
cell boolean masks were computed against `group_by()` output whose row order
Polars does **not** guarantee — silently misaligned masks produced phantom
positive-LB95 cells. `wf_gate.py` had copied the same pattern
(`ba = base_avg.join(dims, ...)` / `ca = cand_avg.join(dims, ...)` feeding
`cell_scan()`'s `mask = (base_df[dim] == v).to_numpy()`). Fixed with
`.sort("race_id")` immediately after the dims join (identical fix as
upstream) and the whole harness was re-run — `train_fold()` skips
already-trained models, so the re-run reused all 18 cached fits and only
recomputed predictions + cells (6.5s vs the original 1302.8s). Pooled /
per-fold / per-seed numbers were unaffected by the bug (no mask involved);
only the cell tables below reflect the corrected alignment.

## Result: pooled (seed-avg, n=10,365 races)

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 33.796 | 33.829 | +0.032     | -0.148 |
| place2     | 18.119 | 18.296 | +0.177     | -0.035 |
| place3     | 14.163 | 14.157 | -0.006     | -0.228 |
| place4     | 12.166 | 12.195 | +0.029     | -0.167 |
| place5     | 11.076 | 11.027 | -0.048     | -0.261 |
| place6     | 10.416 | 10.478 | +0.061     | -0.138 |
| top3_box   | 9.410  | 9.397  | -0.013     | -0.125 |
| fukusho_2p | 74.912 | 74.983 | +0.071     | -0.087 |

Gate: `primaries_passed=0/3`, `lb95_positive=0/3`, `worst_delta=-0.048`
(within the `-0.05` no-reg bound — this lever is flat, not harmful) →
**ACCEPT_strict_gate=false**.

Per-fold (top1/place2/place3 delta[LB95]):

| Fold | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 2023 | -0.222[-0.531] | -0.097[-0.473] | -0.077[-0.482] |
| 2024 | +0.232[-0.077] | +0.540[+0.145] | +0.261[-0.097] |
| 2025 | +0.087[-0.222] | +0.087[-0.251] | -0.203[-0.569] |

Per-seed (top1/place2/place3 delta[LB95]):

| Seed | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 42   | +0.203[-0.087] | +0.338[-0.029] | -0.019[-0.405] |
| 101  | -0.193[-0.502] | +0.106[-0.261] | -0.145[-0.511] |
| 2026 | +0.087[-0.212] | +0.087[-0.289] | +0.145[-0.241] |

Signs flip fold-to-fold (2023 negative across the board, 2024 positive
across the board, 2025 mixed) and seed-to-seed (top1 flips sign on seed101;
place3 flips sign on seed2026) — consistent with noise around a small
positive `place2` drift, not a real, stable effect.

## Summer-restricted result (is_summer=1, venues 01/02/03/10, n=2,448 races)

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 32.108 | 32.081 | -0.027     | -0.409 |
| place2     | 16.217 | 16.367 | +0.150     | -0.300 |
| place3     | 13.508 | 13.412 | -0.095     | -0.531 |
| place4     | 11.560 | 11.928 | +0.368     | -0.082 |
| place5     | 11.234 | 11.370 | +0.136     | -0.300 |
| place6     | 10.063 | 10.090 | +0.027     | -0.341 |
| top3_box   | 8.456  | 8.483  | +0.027     | -0.204 |
| fukusho_2p | 72.358 | 72.304 | -0.055     | -0.409 |

Summer gate: `primaries_passed=0/3`, `lb95_positive=0/3` →
**ACCEPT_strict_gate=false**. The summer-specialist feature
(`jockey_summer_venue_top3_eb` / `_edge`) does not move the needle even
restricted to exactly the venues it targets.

## Cell highlights (n>=200 global / n>=100 summer, multiple-comparison caution)

- **Venue** (`keibajo_code`, global): only `01` (Sapporo, n=504) shows
  `top1 +0.926[LB95 +0.0000]` — LB95 rounds to exactly zero (not `>0`), and
  `place2`/`place3` at the same cell are flat/negative
  (`place2 +0.132[-0.992]`, `place3 -0.000[-0.992]`). Within the 4 summer
  venues specifically: `02` Hakodate `place2 -0.849[LB95 -2.006]` and `03`
  Fukushima `top1 -0.880[LB95 -1.668]` are the two clearest _negative_
  signals in the entire cell scan — i.e. at 2 of the 4 venues the
  summer-circuit feature was built for, the candidate model is directionally
  worse, not better.
- **Class** (`grade_code`): no cell reaches LB95>0; blank-grade (ordinary
  conditions, n=7,570) is flattest (`place2 +0.225[-0.018]`, closest to
  significance among class cells but still negative).
- **Dist-band** (`kyori_band`): band 1 (mile, 1400-1799m, n=3,276) is the
  most positive (`top1 +0.234[-0.092]`, `place2 +0.122[-0.254]`) but neither
  crosses LB95>0.
- **Meeting-day-band** (`nichime_bucket`, new dim, n/a in armB): `early`
  (nichime<=2, n=2,591) is the single cell in the whole scan with an
  LB95-positive primary — `place2 +0.425[LB95 +0.013]`. `top1` and `place3`
  at the same cell are not significant (`+0.219[-0.154]`, `+0.142[-0.322]`).
  One metric at one cell out of ~19 cells x 3 primaries (~57 tests) is
  consistent with multiple-comparison noise, not adoption evidence — but it
  is the only cell anywhere (global or summer) that clears the bar on any
  primary, and it is directly on the dimension `jockey_meetphase_win_eb` was
  designed for. Worth a note for any future targeted early-meeting-day
  follow-up, not itself actionable.

## Verdict: **REJECT** (global and summer-restricted)

None of the 5 engineered jockey win-rate interaction columns
(`jockey_venue_dist_win_eb`, `jockey_venue_dist_top3_eb`,
`jockey_meetphase_win_eb`, `jockey_summer_venue_top3_eb`,
`jockey_summer_venue_edge`) clear the accept gate, pooled or restricted to
the 4 summer venues they specifically target. The pattern matches every
other REJECTed lever in this campaign: a small positive point-estimate drift
on `place2` that never reaches LB95>0 and flips sign across folds/seeds. The
CLEAN 250-feat armB already captures venue x jockey (`jockey_keibajo_win_rate`)
and distance x jockey (`jockey_distance_win_rate`) as separate scalars, and
CatBoost depth=8 evidently reconstructs enough of their joint interaction
from existing features (plus the model's general jockey/venue/style
features) that finer explicit interaction cells add no measurable
incremental accuracy. The meeting-day dimension (`kaisai_nichime`) — wholly
new to the model — also fails to help, despite the one early-meeting-day
`place2` cell hit. **DO-NOT-RETEST** this exact feature set; if meeting-day
signal is revisited, target the `nichime_bucket=early` cell specifically
with a much larger, non-EB-shrunk direct probe rather than folding it back
into a global scalar.

## Artifacts

- Builder: `tmp/candidate-jra-jockey-winrate-clean/build_jockey_winrate.py`
- Feature parquet: `tmp/candidate-jra-jockey-winrate-clean/jockey_winrate_features.parquet`
- Harness: `tmp/candidate-jra-jockey-winrate-clean/wf_gate.py`
- Report: `tmp/candidate-jra-jockey-winrate-clean/reports/jockey_winrate.json`
- Logs: `tmp/candidate-jra-jockey-winrate-clean/{build_jockey,wf_gate}.log`
