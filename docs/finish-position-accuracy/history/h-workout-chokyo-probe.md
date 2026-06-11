---
science_track_entry: true
hypothesis_id: H-WORKOUT-CHOKYO
date: 2026-06-11
based_on_iteration: v7-lineage (JRA/NAR production baseline)
scope: JRA (keibajo_code IN 01-10), race_year 2016–2025 (full); 2023–2025 (holdout)
status: ABORT (best holdout partial rho 0.032 << bar 0.08)
verdict: ABORT — all workout/調教 features fail partial rho bar in both full and holdout periods
production_change: none (probe only)
artifacts:
  probe_script: tmp/feas/workout_probe.py
  probe_output: tmp/feas/workout_probe.json
---

## Hypothesis

**H-WORKOUT-CHOKYO** (Priority 1 candidate from feas-2026-06-11-offcard-data.md):

`jvd_hc` holds 11.8M rows / 23yr of JRA 調教 (workout/gallop) sectional times. An
implementation script `add-workout-features.py` already exists and was complete but
NOT wired into the training pipeline. Workout sharpness is a classic predictor in
horse-racing analytics — a horse that shows fast sectionals in pre-race gallop should
finish better than one with sluggish workouts. The feature set is **horse-level** and
**within-race-varying** (immune to the V8 race-level-bias trap that killed baba-par-time).

**Proposed signal**: 12 features from `jvd_hc` aggregated over 90-day / 5-workout window:
sectional lap times (1F, 2F, 3F, 4F avg & best), cumulative gokei (3F, 4F avg),
workout count, days since last workout, pace progression.

**Leak risk**: The script uses `workout_dt < race_dt` (strict less-than) — same-day or
future workouts are structurally excluded.

---

## Feature List (from add-workout-features.py)

| Feature                    | Description                                                       |
| -------------------------- | ----------------------------------------------------------------- |
| `workout_lap_1f_avg5`      | Avg last-1F lap time over last 5 workouts (lower = faster)        |
| `workout_lap_2f_avg5`      | Avg last-2F lap time over last 5 workouts                         |
| `workout_lap_3f_avg5`      | Avg last-3F lap time over last 5 workouts                         |
| `workout_lap_4f_avg5`      | Avg last-4F lap time over last 5 workouts                         |
| `workout_lap_1f_best5`     | Best (minimum) last-1F lap time over last 5 workouts              |
| `workout_lap_3f_best5`     | Best last-3F lap time over last 5 workouts                        |
| `workout_gokei_4f_avg5`    | Avg cumulative 4F time (gokei) over last 5 workouts               |
| `workout_gokei_3f_avg5`    | Avg cumulative 3F time (gokei) over last 5 workouts               |
| `workout_count_recent`     | Count of workouts in last 10 (activity indicator)                 |
| `workout_count_30d`        | Count of workouts in last 30 days                                 |
| `days_since_last_workout`  | Days between last workout and race day                            |
| `workout_pace_progression` | `gokei_4f_avg5 - lap_1f_avg5` (early vs late pace)                |
| `composite_gokei3f_pace`   | z(`-gokei_3f_avg5`) + z(`pace_progression`) (sharpness composite) |

---

## Leak-Free Verification

| Check                             | Result                                                           |
| --------------------------------- | ---------------------------------------------------------------- |
| `days_since_last_workout` minimum | **1** (no same-day workouts)                                     |
| Negative values                   | **0**                                                            |
| Zero values (race day)            | **0**                                                            |
| Filter mechanism                  | `workout_dt < race_dt` strict less-than in `stage_workout_agg()` |

**LEAK-FREE CONFIRMED.** The implementation correctly excludes all workout records
on or after the race date.

---

## Dataset

Data source: `apps/pc-keiba-viewer/tmp/feat-jra-prod2-workout` (pre-materialized by
`add-workout-features.py`), filtered to `source='jra'` AND `keibajo_code IN ('01'..'10')`.

| Period              | Entries | Races  |
| ------------------- | ------- | ------ |
| Full (2016–2025)    | 473,728 | 34,310 |
| Holdout (2023–2025) | 140,416 | 10,297 |

---

## Coverage

All lap/gokei/days features share the same 91.0% coverage on JRA keibajo (the 9% gap is
newcomers and horses with no jvd_hc records — e.g., foreign horses starting in Japan).
`workout_count_recent` and `workout_count_30d` are 100% covered (coalesced to 0 when NULL).

| Feature                                      | JRA Coverage            |
| -------------------------------------------- | ----------------------- |
| All lap/gokei/days features                  | **91.0%**               |
| `workout_count_recent` / `workout_count_30d` | **100.0%** (coalesce 0) |

---

## Probe Results — JRA Partial Spearman rho

Controls: `speed_index_avg_5`, `odds_score`, `popularity_score`, `recent_finish`,
`weight_avg_5`, `same_distance_win_rate`, `kohan3f_avg_5`.

Bar: partial rho ≥ 0.08 in holdout window.

| Feature                    | Full rho | Holdout rho | Holdout pass? |
| -------------------------- | -------- | ----------- | ------------- |
| `workout_lap_1f_avg5`      | -0.0082  | -0.0121     | NO            |
| `workout_lap_2f_avg5`      | -0.0126  | -0.0151     | NO            |
| `workout_lap_3f_avg5`      | -0.0146  | -0.0156     | NO            |
| `workout_lap_4f_avg5`      | -0.0153  | -0.0169     | NO            |
| `workout_lap_1f_best5`     | -0.0012  | -0.0077     | NO            |
| `workout_lap_3f_best5`     | -0.0194  | -0.0161     | NO            |
| `workout_gokei_4f_avg5`    | -0.0136  | -0.0158     | NO            |
| `workout_gokei_3f_avg5`    | -0.0122  | -0.0147     | NO            |
| `workout_count_recent`     | -0.0080  | -0.0091     | NO            |
| `workout_count_30d`        | -0.0194  | **-0.0201** | NO            |
| `days_since_last_workout`  | +0.0273  | **+0.0316** | NO            |
| `workout_pace_progression` | -0.0089  | -0.0042     | NO            |
| `composite_gokei3f_pace`   | +0.0028  | +0.0061     | NO            |

**Best individual**: `days_since_last_workout` — full 0.0273, holdout 0.0316.
**Bar**: 0.08. **Best holdout / bar = 0.40x — clear miss by a factor of 2.5.**

No feature is even close to the bar. The magnitude peak (`workout_count_30d` = -0.020,
`workout_lap_3f_best5` = -0.016 full) is in the count/speed cluster; all fail badly.

---

## Redundancy Analysis

| Feature                    | Max   | corr                |     | Closest existing feature |
| -------------------------- | ----- | ------------------- | --- | ------------------------ |
| `workout_lap_1f_best5`     | 0.124 | `odds_score`        |
| `workout_gokei_3f_avg5`    | 0.084 | `speed_index_avg_5` |
| `workout_pace_progression` | 0.116 | `odds_score`        |
| `days_since_last_workout`  | 0.062 | `weight_avg_5`      |
| `workout_count_30d`        | 0.098 | `weight_avg_5`      |

Low-to-moderate correlations with existing features — these are genuinely novel signals.
But novelty alone is insufficient when raw predictive content is absent.

---

## NAR Coverage Note

NAR workout data: `jvd_hc` covers ~42% of NAR horses (those that train at JRA-managed
tracen). The NAR `v7-lineage-full` parquet does not include workout columns (pipeline
not wired). A full NAR probe was not executed; however, with:

- 42% coverage (below 70% density ideal)
- JRA (100% coverage category) failing to clear bar by 2.5x
- Workout timing features structurally the same for NAR horses that train at JRA tracen

NAR result is expected to be similar or weaker. NAR is secondary per probe spec.

---

## Interpretation

The workout features are **orthogonal** (max |corr| ≤ 0.12 with existing features) and
**horse-level** (not race-level constants). Coverage is excellent (91%). Leak-free confirmed.
Yet partial rho is uniformly near zero across all 12 features and the composite.

**Why does workout sharpness fail?**

Several structural explanations are consistent with the data:

1. **Trainer strategy heterogeneity**: Trainers deliberately hold horses back in public
   workouts ("見せ調教" — conservative public workout vs. private sharpening). The
   publicly-recorded workout time at Miho/Ritto tracen does not fully reflect the horse's
   actual conditioning state. This is a well-known confounder in Japanese racing.

2. **Market already prices it in**: The odds/popularity controls absorb much of the
   workout signal — bettors and professional syndicates read workout timings and adjust
   odds accordingly. After controlling for odds, the residual workout contribution is ~0.

3. **Signal-to-noise in aggregate**: Averaging over 5 workouts within 90 days smooths
   out any race-specific peaking. A "sharpness at peak" feature (single best workout in
   14 days) might recover some signal, but the `workout_lap_1f_best5` feature already
   approximates this and still fails (holdout -0.008).

4. **Population-level vs. within-race**: JRA horses all train at approximately the same
   intensity tier; the cross-sectional variance in sectional times within a field may
   reflect horse ability (already captured by `speed_index_avg_5`) rather than form.

---

## Verdict

**ABORT**

**Primary reason**: Best holdout partial rho = 0.032 (`days_since_last_workout`) — 2.5x
below the 0.08 bar. All 12 features and the composite fail in both full and holdout periods.
The signal is near-zero after controlling for speed/odds/popularity/kohan3f.

**Not a coverage or leak issue**: 91% coverage, leak-free, horse-level, orthogonal.
The signal genuinely is not there at the required strength.

**Recommendation**: Do not wire `add-workout-features.py` into the training pipeline.
The integration cost is low but the expected gain after full retrain is at or below
measurement noise. Revisit only if a structural insight emerges for isolating peak-week
sharpness from market-priced workout data (e.g., a "workout surprise" feature measuring
deviation between observed workout time and the odds-implied expected workout quality).
