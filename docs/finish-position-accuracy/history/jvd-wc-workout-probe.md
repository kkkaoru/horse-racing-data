---
science_track_entry: true
hypothesis_id: H-JVD-WC-WORKOUT
date: 2026-06-12
based_on_iteration: v7-lineage (JRA production baseline)
scope: JRA (keibajo_code IN 01-10), holdout 2023-2025
status: ABORT (best holdout partial rho -0.028 << bar 0.08; combined HC+WC rho +0.027)
verdict: ABORT — jvd_wc adds no signal beyond jvd_hc; both workout surfaces fail the bar
production_change: none (probe only)
artifacts:
  probe_script: tmp/jvd_wc_probe/wc_probe3.py
  probe_output: tmp/jvd_wc_probe/wc_probe_results.json
---

## Hypothesis

**H-JVD-WC-WORKOUT**: `jvd_wc` (woodchip-course workout) extends the workout signal
beyond `jvd_hc` (slope-track workout). The slope-track probe (H-WORKOUT-CHOKYO,
`h-workout-chokyo-probe.md`) was ABORTed at best partial ρ = 0.032 — possibly because
the slope track covers a narrower horse set and distance range. Woodchip workouts cover
longer distances (up to 10F / 2000M vs. slope's 4F / 800M) and may capture a different
conditioning dimension. Two specific questions:

1. Does jvd_wc add **coverage** for horses that lack jvd_hc workouts?
2. Do woodchip sectional / cumulative times carry **predictive signal** after controlling
   for odds/popularity/speed/weight (same bar as jvd_hc: partial ρ ≥ 0.08)?

---

## Schema

`jvd_wc` — ウッドチップ調教 (woodchip-course workout)

| Column               | Description                              | Format                   |
| -------------------- | ---------------------------------------- | ------------------------ |
| `ketto_toroku_bango` | Horse registration key (PK component)    | varchar(10)              |
| `chokyo_nengappi`    | Workout date (PK component)              | varchar(8) yyyymmdd      |
| `chokyo_jikoku`      | Workout time of day (PK component)       | varchar(4) HHmm          |
| `tracen_kubun`       | Training centre (0=Miho, 1=Ritto) (PK)   | varchar(1)               |
| `course`             | Course code                              | varchar(1)               |
| `babamawari`         | Track direction                          | varchar(1)               |
| `time_gokei_Nf`      | Cumulative time from NF to 0F            | varchar(4) NNN.N s (÷10) |
| `lap_time_Nf`        | Lap time for the Nth-from-finish furlong | varchar(3) NN.N s (÷10)  |

Available distances: 1F–10F gokei (cumulative); 1F–10F individual laps.
Key difference from `jvd_hc`: extends to 10F/2000M (jvd_hc maxes out at 4F/800M).

**Total rows**: 742,151 | **Distinct horses**: 28,485 | **Table size**: 189 MB

---

## Dataset

Source: `jvd_se` (confirmed results, `data_kubun IN ('7','9')`) for JRA races 2023-2025.
Controls: `log_odds` (ln of tansho_odds/10), `ninkijun` (popularity rank), `soha_time_s`
(finish time), `kohan_3f_s` (last-3F sectional), `bataiju_kg` (body weight) — all from
`jvd_se`, all 100% dense in JRA holdout.

| Period            | Entries | Races (approx) |
| ----------------- | ------- | -------------- |
| Holdout 2023-2025 | 141,523 | ~10,300        |

---

## Leak-Free Verification

| Check                        | Result                                          |
| ---------------------------- | ----------------------------------------------- |
| `wc_days_since_last` minimum | **1** (no same-day workouts)                    |
| Entries with days ≤ 0 (leak) | **0**                                           |
| Filter mechanism             | `wc.workout_dt < re.race_dt` (strict less-than) |

**LEAK-FREE CONFIRMED.**

---

## Coverage Analysis

Race-entry level, 90-day lookback window:

| Category                                  | Count     | Pct       |
| ----------------------------------------- | --------- | --------- |
| Total entries                             | 141,523   | 100%      |
| WC covered (has recent woodchip workout)  | 120,976   | **85.5%** |
| HC covered (has recent slope workout)     | 130,115   | **91.9%** |
| Both WC + HC covered                      | 112,486   | 79.5%     |
| **WC only — incremental (no HC workout)** | **8,490** | **6.00%** |
| HC only (no WC)                           | 17,629    | 12.46%    |
| Neither                                   | 2,918     | 2.1%      |

**Incremental coverage is small (6.00%).** Nearly all horses with woodchip workouts
also have slope workouts: at the horse-level, 28,326 of 28,485 WC horses (99.4%) also
appear in jvd_hc. The 159 WC-only horses translate to only 281 / 141,523 entries
(0.20%) that are in jvd_wc but not in jvd_hc at all. The 6% incremental figure arises
from horses in jvd_hc whose most-recent workout was >90 days ago but who still have a
recent woodchip session — a modest incremental window.

---

## Feature List

All features derived from `jvd_wc` with 90-day lookback, last-5-workout windows.

| Feature               | Description                                        | n       |
| --------------------- | -------------------------------------------------- | ------- |
| `wc_days_since_last`  | Days since most-recent woodchip workout            | 120,976 |
| `wc_count_30d`        | Woodchip workout count in last 30 days             | 141,523 |
| `wc_count_recent`     | Workout count in last 10 (activity)                | 141,523 |
| `wc_lap_1f_avg5`      | Avg last-1F lap time, last 5 workouts              | 120,975 |
| `wc_lap_3f_avg5`      | Avg last-3F lap time, last 5                       | 120,970 |
| `wc_lap_4f_avg5`      | Avg last-4F lap time, last 5                       | 120,971 |
| `wc_gokei_3f_avg5`    | Avg cumulative 3F time, last 5                     | 120,971 |
| `wc_gokei_4f_avg5`    | Avg cumulative 4F time, last 5                     | 120,971 |
| `wc_gokei_5f_avg5`    | Avg cumulative 5F time, last 5                     | 118,110 |
| `wc_lap_1f_best5`     | Best (min) last-1F lap, last 5                     | 120,975 |
| `wc_lap_3f_best5`     | Best last-3F lap, last 5                           | 120,970 |
| `wc_gokei_4f_best5`   | Best cumulative 4F, last 5                         | 120,971 |
| `wc_gokei_5f_best5`   | Best cumulative 5F, last 5                         | 118,110 |
| `wc_5f_workout_count` | Count of 5F+ workouts in last 5                    | 141,523 |
| `wc_pace_progression` | `gokei_4f_avg5 − lap_1f_avg5` (early vs late pace) | 120,971 |

---

## Holdout Partial Spearman ρ Results

Controls partialled out: `log_odds`, `ninkijun`, `soha_time_s`, `kohan_3f_s`, `bataiju_kg`.
Bar: **|ρ| ≥ 0.08 AND within-race variation > 0.30**.

| Feature               | Holdout ρ   | n       | Within-race var | PASS? |
| --------------------- | ----------- | ------- | --------------- | ----- |
| `wc_days_since_last`  | -0.0189     | 120,976 | 0.997           | NO    |
| `wc_count_30d`        | +0.0202     | 141,523 | 0.992           | NO    |
| `wc_count_recent`     | -0.0065     | 141,523 | 1.000           | NO    |
| `wc_lap_1f_avg5`      | -0.0151     | 120,975 | 1.000           | NO    |
| `wc_lap_3f_avg5`      | -0.0192     | 120,970 | 1.000           | NO    |
| `wc_lap_4f_avg5`      | -0.0148     | 120,971 | 1.000           | NO    |
| `wc_gokei_3f_avg5`    | -0.0175     | 120,971 | 1.000           | NO    |
| `wc_gokei_4f_avg5`    | -0.0174     | 120,971 | 1.000           | NO    |
| `wc_gokei_5f_avg5`    | -0.0190     | 118,110 | 0.999           | NO    |
| `wc_lap_1f_best5`     | -0.0184     | 120,975 | 0.999           | NO    |
| `wc_lap_3f_best5`     | -0.0245     | 120,970 | 0.999           | NO    |
| `wc_gokei_4f_best5`   | **-0.0284** | 120,971 | 1.000           | NO    |
| `wc_gokei_5f_best5`   | -0.0250     | 118,110 | 0.999           | NO    |
| `wc_5f_workout_count` | -0.0020     | 141,523 | 1.000           | NO    |
| `wc_pace_progression` | -0.0176     | 120,971 | 1.000           | NO    |

**Best individual**: `wc_gokei_4f_best5` — ρ = **-0.0284** (best cumulative 4F time).
**Bar**: 0.08. **Best / bar = 0.36x — clear miss by a factor of 2.8.**

---

## Combined jvd_hc ∪ jvd_wc Signal

The combined signal (`LEAST(hc_days_since_last, wc_days_since_last)`) represents "any
workout surface, most recent":

| Signal                                         | ρ          | n        | PASS?  |
| ---------------------------------------------- | ---------- | -------- | ------ |
| jvd_hc `days_since_last_workout` (prior probe) | +0.032     | ~140,000 | NO     |
| jvd_wc `wc_days_since_last`                    | -0.019     | 120,976  | NO     |
| **Combined HC+WC `days_since_last`**           | **+0.027** | 138,605  | **NO** |

The combined signal (+0.027) is **weaker** than jvd_hc alone (+0.032). Adding the
woodchip surface does not push the combined ρ above the bar — and in fact slightly
dilutes the slope-track signal, possibly because some horses record a very recent
easy woodchip canter that replaces a longer-ago quality slope session in the MIN().

---

## Within-Race Variation

All 15 features show near-perfect within-race variation (wv > 0.99 for every feature),
confirming these are genuine horse-level signals, not race-level constants. The variation
condition is never the binding constraint — the predictive content is the only issue.

---

## Interpretation

1. **Not a coverage win**: jvd_wc provides only 6% incremental entries not covered by
   jvd_hc. The 99.4% horse-level overlap confirms these are the same horses training at
   the same tracen (Miho/Ritto), just on different track surfaces within the same facility.

2. **Not a signal win**: The longer distances (5F–10F) available only in jvd_wc add no
   measurable predictive content beyond the 4F slope times. The best-5F-best5 (ρ = -0.025)
   is not meaningfully stronger than best-4F-best5 (ρ = -0.028).

3. **Consistency with jvd_hc probe**: The magnitude and direction of all 15 woodchip
   features closely mirror the 12 slope features (all small negative, <0.030). This
   confirms the market-efficiency explanation from the slope probe — workout times for
   JRA horses are fully priced into tansho_odds before the race. After controlling for
   odds/popularity the residual workout contribution is near zero for both surfaces.

4. **Combined signal weakens, not strengthens**: The HC+WC combined ρ (+0.027) is below
   the HC-alone ρ (+0.032). This occurs because the LEAST() combination sometimes
   substitutes a recent easy woodchip canter for a less-recent but more-informative
   slope gallop — diluting rather than enriching the signal.

---

## Verdict

**ABORT**

**Primary reason**: Best holdout partial ρ = **-0.028** (`wc_gokei_4f_best5`) — 2.8x
below the 0.08 bar. All 15 features fail in holdout. Combined jvd_hc∪jvd_wc ρ = +0.027
— also well below bar, and weaker than jvd_hc alone.

**Not a coverage issue**: 85.5% WC coverage is high; only 6% is genuinely incremental
over jvd_hc. Horse-level overlap between the two surfaces is 99.4%.

**Consistent with structural market-efficiency wall**: Woodchip workout times carry the
same priced-in information as slope workout times. The surface difference (woodchip vs.
slope) does not expose any residual conditioning dimension after controlling for odds.

**Recommendation**: Do not wire jvd_wc into the training pipeline, either standalone
or as a complement to jvd_hc. The integration cost is non-zero and the expected gain
is at or below measurement noise (ρ ≈ 0.02–0.03 at best).

Revisit only if a "workout surprise" feature can be constructed — e.g., deviation
between observed woodchip sectional and the odds-implied expected workout quality —
but this would require per-horse baseline models not currently available.
