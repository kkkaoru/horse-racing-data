# JRA Same-Meet Repeat-Starter (滞在馬) Engineered-Feature WF (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position feature engineering
- **Trigger / USER condition A** (`venue x meetday x waku x season`), the
  horse-level-repetition axis. Deployed JRA model is the clean 250-feature
  `jra-cb-v9-sim-2013` CatBoost YetiRank baseline (armB) after the 2026-07-04
  within-race `target_corner_*` / `target_running_style_class` leak removal.
- **Hypothesis**: summer meets (esp. Hokkaido Sapporo=01/Hakodate=02) run
  long, near-daily schedules, which makes it cost-effective for shipping
  trainers to stable a horse on-site (滞在) for the duration and enter it 2+
  times within the same venue-meet rather than shipping it home between
  starts. A horse's SECOND+ start of the meet should differ systematically
  from a shipper's one-off: acclimatized to the local ground/climate, on a
  short rest interval, and reflecting visible trainer intent (bringing a
  horse back quickly signals the trainer likes its form). This is a distinct
  axis from two related-but-dead candidates: `first_time_this_venue`
  (career-level venue-experience, wrong-direction, see upset doc §6) and
  `meet_inside_bias` (venue-level rail-wear residual,
  `docs/probes/jra-meetingday-waku-clean-2026-07-04.md`, REJECT) — neither of
  those is within-CURRENT-meet horse-level repetition.

## Candidate columns

All 5 are strictly prior to the current race (earlier-day starts within the
SAME venue-meet only, or a fixed pre-race schedule/base-feature product) — no
same-race or future information.

1. **`is_meet_repeat`** — 1 if this horse has >=1 EARLIER completed start
   (`ijo_kubun_code='0'`, finished) within the same venue-meet
   (`keibajo_code`, `kaisai_nen`, `kaisai_kai`), else 0. Never NULL.
2. **`meet_prior_finish_norm`** — `finish_position / field_size` of the
   horse's most recent EARLIER start in this meet. NULL if
   `is_meet_repeat=0`.
3. **`days_since_meet_first_start`** — calendar days between the horse's
   FIRST (earliest) EARLIER start in this meet and the current race date —
   a continuous "how long has this horse been stabled here" proxy. NULL if
   `is_meet_repeat=0`.
4. **`meet_repeat_x_hokkaido`** — `is_meet_repeat * (keibajo_code in {01,02})`,
   the 滞在-culture interaction the hypothesis specifically targets.
5. **`rest_days_x_meet_repeat`** — `days_since_last_race * is_meet_repeat`
   (short-rest repeat vs long-rest shipper contrast; `days_since_last_race`
   is already a live armB base feature — no extra PG join needed).

Leak-free: all `is_meet_repeat`/`meet_prior_finish_norm`/
`days_since_meet_first_start` derivation uses only PG `jvd_se` rows with a
strictly earlier `race_dt` within the same `(keibajo_code, kaisai_nen,
kaisai_kai)` partition (`rows between unbounded preceding and 1 preceding`);
the current row and any future-day row in the meet are never visible.
Scratches (`ijo_kubun_code<>'0'`) do not establish repeat-starter identity or
contribute a "prior finish". Rows whose OWN entry has an abnormal
`ijo_kubun_code` (~0.8% of targets, e.g. DQ/placing-lowered but
`kakutei_chakujun` still populated) would otherwise show a spurious NULL for
`is_meet_repeat`/`meet_repeat_x_hokkaido` purely because they're absent from
the "starts" table for themselves — coalesced to `0` (safe default) rather
than left NULL.

**Build stats** (all JRA 2013+, `md_meta_rows=660,834`, joined to the store's
target universe, `n=635,453`):

| Column                        | Coverage | Rate (mean) |
| ----------------------------- | -------- | ----------- |
| `is_meet_repeat`              | 100.0%   | 12.165%     |
| `meet_prior_finish_norm`      | 12.165%  | —           |
| `days_since_meet_first_start` | 12.165%  | —           |
| `meet_repeat_x_hokkaido`      | 100.0%   | 1.512%      |

`is_meet_repeat` at 12.2% is comfortably above the 5% thinness floor flagged
in the task brief — same-meet repetition is a real, non-rare pattern in JRA
(not just a Hokkaido curiosity), though the Hokkaido-specific interaction
(`meet_repeat_x_hokkaido`) is thinner at 1.5% of all rows as expected (only 2
of 10 JRA venues, summer-only meets).

## Method

- **Harness**: `tmp/candidate-jra-meet-repeat/wf.py`, feature build:
  `tmp/candidate-jra-meet-repeat/build_features.py`.
- **Meet identity**: `keibajo_code` x `kaisai_nen` x `kaisai_kai` (same meet
  grouping as the meeting-day-waku probe). Horse-level sequence within a meet
  ordered by `race_dt`, joined from local Postgres (`jvd_se`,
  `postgresql://127.0.0.1:15432/horse_racing`) keyed to the store's
  `(race_id, umaban)` universe, JRA venues `01`-`10`, `kaisai_nen>=2013`.
- **Baseline (control, both arms)**: CLEAN `armB` from
  `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json` — 250 feat,
  the live `jra-cb-v9-sim-2013` spec, leak-free by construction.
- **Treatment**: control + the 5 candidate columns above (additive, no
  fold-dependent residualization needed — unlike `meet_inside_bias_prior` in
  the meeting-day-waku probe, every candidate col here is a direct
  strictly-prior join/product).
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
  `season_band` / `current_baba_condition` / `kyoso_joken_code` (race-invariant
  dims, `first()` per race) PLUS `race_has_repeat_starter` (race-level: does
  > =1 entrant in this race have `is_meet_repeat=1` — the repeat-starter
  > subpopulation itself, the task's required own-axis cell), `n>=200` per
  > cell. Plus a SUMMER-RESTRICTED subset (`keibajo_code` in `{01 Sapporo, 02
Hakodate, 03 Fukushima, 10 Kokura}`) pooled + gated, the repeat-starter
  > subpopulation pooled + gated (global and, if `n>=50`, Hokkaido-restricted),
  > and a summer x `{keibajo_code, race_has_repeat_starter}` cross-cell at
  > `n>=100` (multiple-comparison caution applies throughout).
- **Bug-fixed cell-mask pattern applied** (mandatory, per system doc §9 #11):
  every frame a boolean mask is computed against is `.sort("race_id")`'d
  BEFORE the mask array is built, since `paired()` re-sorts its inputs
  internally and `group_by("race_id")` row order is not guaranteed by polars
  — the exact bug that produced 3 phantom summer-cell signals in
  `tmp/candidate-masked-lever-retest/retest_wf.py` and was caught mid-run in
  the meeting-day-waku probe.

## Result

Runtime 1235.6s (18 CatBoost models: base+cand x 3 folds x 3 seeds), all
cell/summer masks built with the sort-fixed pattern.

### Pooled (seed-avg, 3 folds x 3 seeds, n=10,365 races)

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 33.796 | 33.845 | +0.048     | -0.129 |
| place2     | 18.119 | 18.083 | -0.035     | -0.251 |
| place3     | 14.163 | 14.160 | -0.003     | -0.222 |
| place4     | 12.166 | 12.121 | -0.045     | -0.251 |
| place5     | 11.076 | 10.982 | -0.093     | -0.296 |
| place6     | 10.416 | 10.619 | +0.203     | +0.010 |
| top3_box   | 9.410  | 9.445  | +0.035     | -0.071 |
| fukusho_2p | 74.912 | 74.974 | +0.061     | -0.097 |

**Gate: `primaries_passed=0/3`, `lb95_positive=0/3`, `worst_delta=-0.093`
(within the `-0.05` no-reg bound only by chance elsewhere; place5 is the
worst) → ACCEPT_strict_gate=false.** All three primaries sit within ~0.1pp of
zero with no LB95 anywhere near positive — no pooled signal either direction.
`place6` alone clears `LB95>0` (`+0.010`), a single non-primary metric out of
8, and not something to build a claim on.

Per-seed (top1/place2/place3 delta[LB95]):

| Seed | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 42   | +0.193[-0.106] | +0.058[-0.338] | -0.125[-0.492] |
| 101  | -0.135[-0.453] | -0.116[-0.473] | +0.010[-0.347] |
| 2026 | +0.087[-0.251] | -0.048[-0.425] | +0.106[-0.280] |

Per-fold (top1/place2/place3 delta[LB95]):

| Fold | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 2023 | -0.232[-0.521] | -0.405[-0.820] | -0.222[-0.608] |
| 2024 | +0.212[-0.087] | +0.347[-0.049] | +0.309[-0.068] |
| 2025 | +0.164[-0.154] | -0.048[-0.396] | -0.097[-0.473] |

Same sign-flip-across-folds pattern seen throughout this campaign (2023
all-negative, 2024 all-positive-but-never-LB95>0, 2025 mixed) — consistent
with noise centered near zero, not a stable effect. No seed and no fold
passes 2/3 primaries with LB95>0.

### Summer-restricted (keibajo in {01 Sapporo, 02 Hakodate, 03 Fukushima, 10 Kokura}, n=2,448 races) — PRIMARY TARGET

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 32.108 | 32.013 | -0.095     | -0.477 |
| place2     | 16.217 | 15.904 | **-0.313** | -0.790 |
| place3     | 13.508 | 13.494 | -0.014     | -0.477 |
| place4     | 11.560 | 11.860 | +0.300     | -0.150 |
| place5     | 11.234 | 11.111 | -0.123     | -0.586 |
| place6     | 10.063 | 10.471 | +0.409     | +0.027 |
| top3_box   | 8.456  | 8.538  | +0.082     | -0.123 |
| fukusho_2p | 72.358 | 72.358 | +0.000     | -0.354 |

**Gate: `primaries_passed=0/3`, `worst_delta=-0.313` (place2) →
ACCEPT_strict_gate=false.** In the exact venue subset the hypothesis targets,
`place2` shows a real, meaningfully-sized regression (-0.31pp), `top1` is
also slightly negative, and only `place3` is roughly flat. No support for
the summer/Hokkaido hypothesis at the venue-pooled level.

### Repeat-starter subpopulation (races with >=1 repeat starter, n=4,342 — the task's required own-axis cell)

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 34.109 | 34.124 | +0.015     | -0.253 |
| place2     | 17.949 | 17.788 | **-0.161** | -0.514 |
| place3     | 14.394 | 14.371 | -0.023     | -0.361 |
| place4     | 12.245 | 12.260 | +0.015     | -0.284 |
| place5     | 10.886 | 10.924 | +0.038     | -0.261 |
| place6     | 10.088 | 10.180 | +0.092     | -0.184 |
| top3_box   | 9.527  | 9.558  | +0.031     | -0.138 |
| fukusho_2p | 75.242 | 75.280 | +0.038     | -0.192 |

**Gate: `primaries_passed=0/3`, `worst_delta=-0.161` → ACCEPT_strict_gate=false.**
Restricting to exactly the races the candidate features were designed to
inform (at least one 滞在-repeat starter in the field) does not surface a
positive signal — if anything `place2` regresses more here (-0.16pp) than in
the full pooled population (-0.035pp).

### Hokkaido x repeat-starter cross-cell (n=510 — the single sharpest intended-mechanism cell)

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 34.837 | 35.098 | +0.261     | -0.588 |
| place2     | 17.386 | 16.209 | **-1.177** | -2.288 |
| place3     | 12.092 | 12.353 | +0.261     | -0.523 |
| top3_box   | 9.346  | 9.477  | +0.131     | -0.196 |
| fukusho_2p | 72.941 | 73.464 | +0.523     | -0.065 |

This is the exact cell the hypothesis was built around (Sapporo/Hakodate meet
AND a repeat starter present in the race) and it produces the **single
largest regression in the whole sweep**: `place2 -1.18pp [LB95 -2.29]`. The
direction is the opposite of what "acclimatized 滞在 horses run better on
their 2nd+ start" would predict. `top1`/`place3` are mildly positive but far
from `LB95>0`, and do not offset the `place2` hit under the multi-metric
gate.

### Per-venue cell (all folds pooled, n>=200, seed-avg)

| keibajo_code | n    | top1 delta[LB95]   | place2 delta[LB95] | place3 delta[LB95] |
| ------------ | ---- | ------------------ | ------------------ | ------------------ |
| 01 Sapporo   | 504  | **+1.389[+0.463]** | -0.265[-1.323]     | +0.000[-0.926]     |
| 02 Hakodate  | 432  | +0.154[-0.694]     | **-1.003[-2.315]** | +0.231[-0.617]     |
| 03 Fukushima | 720  | -1.204[-1.944]     | -0.046[-0.880]     | +0.046[-0.788]     |
| 10 Kokura    | 792  | -0.168[-0.673]     | -0.210[-0.926]     | -0.210[-1.179]     |
| 05           | 1607 | +0.249[-0.207]     | +0.000[-0.602]     | -0.125[-0.685]     |
| 08           | 1535 | +0.760[+0.282]     | +0.239[-0.326]     | -0.152[-0.738]     |
| 04           | 935  | -0.464[-1.034]     | +0.250[-0.464]     | +0.357[-0.428]     |
| 06           | 1500 | -0.000[-0.467]     | -0.200[-0.734]     | +0.333[-0.200]     |
| 07           | 1128 | -0.059[-0.532]     | +0.236[-0.384]     | +0.118[-0.502]     |
| 09           | 1212 | -0.275[-0.825]     | -0.138[-0.798]     | -0.440[-1.128]     |

Sapporo `top1` (`+1.39pp [LB95 +0.46]`) is the one cell anywhere in this sweep
with a clean LB95-positive primary — but `place2` at that same cell is
negative (`-0.265[-1.323]`), so it fails the multi-metric gate even locally,
and 10 venues x 3 primaries = 30 comparisons puts a single positive cell
squarely inside the multiple-comparison noise floor (the same caveat applied
to an identical Sapporo `top1`-only cell finding in
`docs/probes/jra-meetingday-waku-clean-2026-07-04.md`). Hakodate's `place2`
regression (`-1.00pp`, tight-ish `LB95 -2.32`) is the more informative
signal here: it is large, consistent with the summer-restricted and
Hokkaido-x-repeat-starter results above, and points the same direction as
the pooled regression, not against it.

### Other cells (n>=200, seed-avg, full population)

`kyori_band`, `season_band`, `current_baba_condition`, `kyoso_joken_code`,
`race_has_repeat_starter` (0 vs 1): no cell in any of these dimensions clears
2-of-3-primaries-with-LB95>0. `race_has_repeat_starter=1` itself (n=4,342,
same as the dedicated subpopulation cell above) shows the same `place2
-0.161[-0.514]` regression seen in every repeat-starter-restricted cut in
this probe. Scattered single-metric near-misses (`kyori_band=1` top1
`+0.315[+0.020]`, `current_baba_condition=3` place2/place3
`+0.74[-0.04]`/`+0.70[-0.08]` both n.s., `kyoso_joken_code=010` top1
`+0.69[+0.26]` alone) are consistent with the ~25-cell-wide
multiple-comparison noise floor, not a coherent signal — none replicate
across an adjacent cell in the same dimension family, and none pair with a
positive `place2`/`place3`. Full cell tables:
`tmp/candidate-jra-meet-repeat/reports/meet_repeat.json`
(`cells_ge200_seedavg`, `summer_cross_cells_ge100`).

## Conclusion: REJECT

None of the 5 candidates (`is_meet_repeat`, `meet_prior_finish_norm`,
`days_since_meet_first_start`, `meet_repeat_x_hokkaido`,
`rest_days_x_meet_repeat`, evaluated as one additive bundle per the task
spec) clear the accept gate globally (`0/3 primaries`, no `LB95>0`), on the
summer-restricted subset (`0/3`, `place2 -0.31pp` real regression), on the
repeat-starter subpopulation (`0/3`, `place2 -0.16pp`), or — most
tellingly — on the Hokkaido x repeat-starter cross-cell that is the precise
intersection the 滞在 hypothesis was built around (`place2 -1.18pp [LB95
-2.29]`, the single largest regression anywhere in this sweep). The
coverage/thinness concern flagged in the task brief was not the limiting
factor here: `is_meet_repeat` sits at a healthy 12.3% of all JRA rows (not
`<5%`), so the model had ample repeat-starter examples to learn from — it
simply did not find a usable signal, and where the hypothesis most sharply
predicted a positive effect (Hokkaido, repeat starter present), the
candidate moved accuracy the wrong way instead. CatBoost depth=8 with the
existing 250-feature armB set (which already includes `days_since_last_race`,
`is_returning_from_layoff`, `weight_avg_5`/`weight_diff_from_avg`,
`same_keibajo_win_rate`, and course/venue features) likely already captures
most of what "has this horse run here recently" would predict through
those existing venue-experience and form-recency signals, leaving the
explicit repeat-starter identity/prior-finish/rest-interaction bundle
redundant at best and a source of noise at worst. Sapporo `top1` alone shows
an isolated `LB95>0` cell (`+1.39pp`, n=504) but fails the multi-metric gate
at that same cell (`place2` negative) and is indistinguishable from the
~25-cell multiple-comparison noise floor documented across every other cell
in this sweep — the same caveat that already applied to an analogous Sapporo
`top1`-only finding in the meeting-day-waku probe.
**DO-NOT-RETEST** this exact feature family (same-meet repeat-starter
identity/prior-finish/rest-interaction, additive bundle) for JRA; the
sharpest form of the intended mechanism (Hokkaido x repeat-starter, n=510)
is also the sharpest regression found, which argues against a "just needs a
finer cell" retry with the same features. A genuinely different mechanism —
e.g. an interaction with `field_size_normalized`/`shusso_tosu_1` (does
滞在-repeat status matter differentially in small vs large summer fields) or
with the running-style features (does a repeat starter's habitual pace
profile shift specifically at Hokkaido) — would be a different candidate
family, not a retest of this one.

## Artifacts

- Feature build: `tmp/candidate-jra-meet-repeat/build_features.py` (PG
  jvd_se meet-identity join + strictly-prior expanding window per horse, 0.9s)
- Harness: `tmp/candidate-jra-meet-repeat/wf.py` (sort-before-mask pattern
  applied from the start, following the meeting-day-waku probe's mid-run fix)
- Feature parquet: `tmp/candidate-jra-meet-repeat/meet_repeat_features.parquet`
- Models: `tmp/candidate-jra-meet-repeat/models/{base,cand}/seed*/fold-*/model.json`
  (18 total)
- Report: `tmp/candidate-jra-meet-repeat/reports/meet_repeat.json`
- Log: `tmp/candidate-jra-meet-repeat/wf.log` (1235.6s, all 18 models)
