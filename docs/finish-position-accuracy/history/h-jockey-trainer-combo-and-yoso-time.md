---
science_track_entry: true
hypothesis_id: H-JOCKEY-TRAINER-COMBO-AND-YOSO-TIME
date: 2026-06-11
based_on_iteration: v7-lineage (JRA/NAR production baseline)
scope: JRA (keibajo_code IN 01-10) + NAR; full period and holdout 2023-2026
status: ABORT (both candidates fail partial rho bar in holdout)
verdict: ABORT — combo partial rho 0.002-0.007, yoso partial rho 0.066; both below bar 0.08
production_change: none (probe only)
investigation_status: NEW-DATA INVESTIGATION EXHAUSTED — all unused dense+orthogonal columns tested
artifacts:
  probe_script: tmp/feas/final_combo_probe.py
  probe_output: tmp/feas/final_combo_probe.json
---

## Hypotheses

This doc covers the **final two** unused-column candidates identified in
`feas-2026-06-11-offcard-data.md`. Both were selected because they had a plausible
odds-orthogonal residual; the workout/調教 probe (H-WORKOUT-CHOKYO) had already ABORTed
at holdout partial rho 0.032, establishing that the market efficiently prices public
workout signals.

---

### Candidate A — Jockey × Trainer Combo Win-Rate (JRA + NAR)

**Hypothesis**: The pair-level historical win-rate for a specific (kishu_code, chokyoshi_code)
combination carries information beyond each marginal rate already in the model. When a
jockey and trainer have a well-established partnership with many shared rides, the combo
synergy may reflect communication, horse management style, and preferred race strategy that
neither marginal rate captures alone.

**Existing controls**: `jockey_career_win_rate` and `trainer_career_win_rate` are in the model.
The NOVEL signal is `pair_win_rate - jockey_career_win_rate - trainer_career_win_rate`
(pure interaction term, labeled `pair_interaction` below).

**Data source**: `jvd_se.kishu_code × chokyoshi_code` (JRA) and
`nvd_se.kishu_code × chokyoshi_code` (NAR). Historical races strictly before the target
race (strict less-than on `kaisai_nen || kaisai_tsukihi`). Only entries with
`pair_count >= 5` used for the dense signal (excludes debut pairs).

---

### Candidate B — JRA yoso_soha_time Pre-Race Speed Figure (JRA only)

**Hypothesis**: `jvd_se.yoso_soha_time` (予想走破時計) is a pre-race published estimated
finish time provided by JV-Data, appearing in the race program before race day. At ~1.18M
rows (2002–2026), it is horse-level, within-race-varying, and unused in any current
feature. Within a race, slower predicted time = worse horse. After controlling for
`popularity_score` and `odds_score` (which reportedly correlate at r≈0.67 with it), a
residual orthogonal signal may remain.

**Leak verification**: The value is confirmed pre-race by three independent checks:
(1) avg 13.85 distinct values per race (per-horse, not a race constant),
(2) corr(yoso_soha_time, actual soha_time) = 0.917 — related but NOT identical to post-race
actuals, (3) `yoso_juni` (predicted rank) consistently differs from `kakutei_chakujun`
(actual finish) in all sampled rows.

---

## Probe Method

- **Bar**: partial Spearman ρ ≥ 0.08 in holdout window (2023-2026). PROCEED iff holdout passes.
- **Partial ρ method**: residualise both target and feature in rank space, regressing out all controls. Same method as H-WORKOUT-CHOKYO and H-BABA-PAR-TIME probes.
- Both **full period** and **holdout 2023-2026** reported.

---

## Candidate A: Leak-Free Verification

| Check                            | Result                                                                     |
| -------------------------------- | -------------------------------------------------------------------------- |
| Filter mechanism                 | `h.race_dt < t.race_dt` (kaisai_nen\|\|kaisai_tsukihi strict less-than)    |
| PG join key                      | (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango) |
| Entries with pair history (JRA)  | 95.8% of full, 95.7% of holdout                                            |
| Dense (pair_count >= 5) coverage | 85.3% full / 85.2% holdout (JRA); 93.7% holdout (NAR)                      |

**LEAK-FREE CONFIRMED.** The self-join uses strict inequality on the race date string.

---

## Candidate A: Results — JRA Partial Spearman ρ

Controls: `speed_index_avg_5`, `odds_score`, `popularity_score`, `recent_finish`,
`weight_avg_5`, `same_distance_win_rate`, `kohan3f_avg_5`, `jockey_career_win_rate`,
`trainer_career_win_rate`.

Dataset: `feat-jra-v7-lineage` (990,719 entries, 70,417 races full; 160,347 entries, 11,703 races holdout).
Dense-only rows (pair_count >= 5) used.

| Feature            | Full raw ρ | Full partial ρ | Holdout raw ρ | Holdout partial ρ | Holdout pass? |
| ------------------ | ---------- | -------------- | ------------- | ----------------- | ------------- |
| `pair_win_rate`    | -0.1874    | +0.0042        | -0.1891       | +0.0023           | NO            |
| `pair_top3_rate`   | -0.2332    | +0.0014        | -0.2329       | -0.0004           | NO            |
| `pair_interaction` | +0.0460    | +0.0023        | +0.0412       | +0.0021           | NO            |

**Deciding number (JRA)**: `pair_win_rate` holdout partial ρ = **0.0023** (bar = 0.08).

The raw rho of -0.19 for `pair_win_rate` reflects the known fact that winning pairs
(high pair_win_rate) tend to be in higher-quality races where finishing position is lower
in absolute terms (more horses). After removing this field-composition effect, the partial
rho collapses to near zero.

---

## Candidate A: Results — NAR Partial Spearman ρ

Dataset: `feat-nar-v7-lineage` (3,455,019 entries, 274,604 races full; 462,752 entries, 45,573 races holdout).

| Feature            | Full raw ρ | Full partial ρ | Holdout raw ρ | Holdout partial ρ | Holdout pass? |
| ------------------ | ---------- | -------------- | ------------- | ----------------- | ------------- |
| `pair_win_rate`    | -0.2730    | -0.0049        | -0.2693       | -0.0070           | NO            |
| `pair_interaction` | +0.0583    | -0.0032        | +0.0582       | -0.0071           | NO            |

**Deciding number (NAR)**: `pair_win_rate` holdout partial ρ = **-0.007** (bar = 0.08).

NAR is directionally consistent with JRA: residual near zero.

---

## Candidate A: Redundancy

| Feature            | Max \|corr\| | Closest existing feature  |
| ------------------ | ------------ | ------------------------- |
| `pair_win_rate`    | 0.582        | `jockey_career_win_rate`  |
| `pair_interaction` | 0.265        | `trainer_career_win_rate` |

`pair_win_rate` is 58% correlated with `jockey_career_win_rate` — confirming that the
jockey's marginal rate dominates the pair signal. `pair_interaction` is lower (0.27)
but orthogonality alone is insufficient when the signal content is absent.

---

## Candidate A: Verdict

**ABORT**

**Primary reason**: Best holdout partial ρ across JRA and NAR = **0.0023** (pair_win_rate JRA)
and **-0.007** (pair_win_rate NAR). Both are 35–40x below the bar of 0.08.

**Interpretation**: The market already prices in jockey-trainer partnership quality.
Odds and popularity absorb the pair-level synergy signal completely. After controlling
for both marginal rates plus the full existing feature set, no residual information
remains in the pair interaction.

---

## Candidate B: Leak-Free Verification

| Check                         | Result                                                                   |
| ----------------------------- | ------------------------------------------------------------------------ |
| Source                        | `jvd_se.yoso_soha_time` (予想走破時計) from JV-Data                      |
| Publication timing            | Pre-race: appears in race program published before race day              |
| Within-race distinct values   | Average **13.85** distinct values per race (2020+); per-horse prediction |
| vs. actual soha_time          | corr = 0.917 (related but NOT identical to post-race actuals)            |
| yoso_juni vs kakutei_chakujun | Consistently differs (pre-race predicted rank ≠ actual finish)           |

**LEAK-FREE CONFIRMED.** `yoso_soha_time` is the JV-Data pre-race estimated time,
published as part of the race program. It is structurally distinct from the post-race
`soha_time` field.

---

## Candidate B: Coverage

| Period                | Total entries | With yoso | Coverage |
| --------------------- | ------------- | --------- | -------- |
| Full (JRA v7 parquet) | 990,719       | 410,422   | 41.4%    |
| Holdout 2023-2026     | 160,347       | 131,807   | 82.2%    |

Coverage is 0% before 2002 (JV-Data did not include yoso_soha_time). From 2002 onward,
coverage rises from ~54% to ~86% (86.1% in 2026). The holdout window (2023+) has
excellent 82% coverage. Full-period coverage is suppressed by the pre-2002 zero-coverage
era but the probe uses all valid rows.

---

## Candidate B: Within-Race Variation

Within-race variation = **1.000** (every race with any yoso data has distinct per-horse values).
This confirms yoso_soha_time is a genuine horse-level signal, not a race-level constant.

---

## Candidate B: Results — JRA Partial Spearman ρ

Controls: `speed_index_avg_5`, `odds_score`, `popularity_score`, `recent_finish`,
`weight_avg_5`, `same_distance_win_rate`, `kohan3f_avg_5`.

**Important note on confounding**: `yoso_rank_in_race` (raw rank) shares a field-size
confound with `finish_position` — both grow with larger fields (corr with field_size = 0.29
each). This inflates the raw partial ρ to 0.14. The properly normalized
`yoso_rank_norm_in_race` = (rank−1)/(N−1) eliminates this confound (corr with field_size = 0.0006)
and is the correct feature for probing. Results below use `yoso_rank_norm_in_race`
(confirmed equivalent to `yoso_zscore_in_race`).

| Feature                  | Full raw ρ | Full partial ρ | Holdout raw ρ | Holdout partial ρ | Holdout pass? |
| ------------------------ | ---------- | -------------- | ------------- | ----------------- | ------------- |
| `yoso_rank_norm_in_race` | +0.4173    | +0.0696        | +0.4104       | +0.0658           | NO            |
| `yoso_zscore_in_race`    | +0.4174    | +0.0689        | +0.4092       | +0.0628           | NO            |

_(For reference: `yoso_rank_in_race` raw partial ρ = 0.1399 holdout, but field-size
controlled = 0.1049. The confound explains the apparent signal; the proper normalized
form gives 0.066.)_

Full n = 410,422 entries (70,417 races). Holdout n = 131,807 entries (11,703 races).

**Deciding number**: `yoso_rank_norm_in_race` holdout partial ρ = **0.0658** (bar = 0.08).

---

## Candidate B: Redundancy

| Feature                  | Max \|corr\| | Closest existing feature |
| ------------------------ | ------------ | ------------------------ |
| `yoso_rank_norm_in_race` | 0.667        | `popularity_score`       |

Full correlation profile:

| Existing feature         | \|corr\| with `yoso_rank_norm` |
| ------------------------ | ------------------------------ |
| `popularity_score`       | 0.667                          |
| `odds_score`             | 0.664                          |
| `recent_finish`          | 0.484                          |
| `speed_index_avg_5`      | 0.401                          |
| `kohan3f_avg_5`          | 0.107                          |
| `weight_avg_5`           | 0.096                          |
| `same_distance_win_rate` | 0.031                          |

`yoso_soha_time` is highly correlated with both `popularity_score` (0.667) and `odds_score`
(0.664). JV-Data's estimated time aggregates the same horse-quality signals that bettors
use when forming odds. After the market controls are residualised out, only 0.066 partial ρ
remains — below bar.

---

## Candidate B: Verdict

**ABORT**

**Primary reason**: `yoso_rank_norm_in_race` holdout partial ρ = **0.0658** — 18% below
the bar of 0.08. The signal is real (raw ρ = 0.41) but not odds-orthogonal.

**Interpretation**: JV-Data's pre-race speed figure is essentially a structured summary
of the same information bettors use: horse's recent speed (≈ speed_index_avg_5, r=0.40),
popularity (r=0.67), and recent form (r=0.48). After controlling for these, the residual
is 0.066. This is the same market-efficiency pattern observed in H-WORKOUT-CHOKYO
(workout data prices into odds).

**Field-size confound note**: The raw `yoso_rank_in_race` feature shows holdout partial ρ
of 0.14 without field-size control and 0.10 with field-size control. This above-bar value
is an artifact of field-size confounding (finish_position and raw rank both grow in larger
fields). The confound-free normalized form conclusively falls below bar.

---

## Summary Table

| Candidate                            | Full partial ρ | Holdout partial ρ | Bar  | Decision |
| ------------------------------------ | -------------- | ----------------- | ---- | -------- |
| A: pair_win_rate (JRA)               | +0.0042        | +0.0023           | 0.08 | ABORT    |
| A: pair_interaction (JRA)            | +0.0023        | +0.0021           | 0.08 | ABORT    |
| A: pair_win_rate (NAR)               | -0.0049        | -0.0070           | 0.08 | ABORT    |
| A: pair_interaction (NAR)            | -0.0032        | -0.0071           | 0.08 | ABORT    |
| B: yoso_rank_norm_in_race (JRA full) | +0.0696        | +0.0658           | 0.08 | ABORT    |

---

## New-Data Investigation: Status

**EXHAUSTED.**

All identified unused dense+orthogonal columns from `jvd_se`/`nvd_se` have now been probed:

| Signal                        | Source                             | Holdout partial ρ | Status                   |
| ----------------------------- | ---------------------------------- | ----------------- | ------------------------ |
| workout/調教 sectionals       | jvd_hc (H-WORKOUT-CHOKYO)          | 0.032             | ABORT                    |
| baba_par_time (V8 PROCEED)    | jvd_ra/nvd_ra (H-BABA-PAR-TIME)    | 0.180             | PROCEED (separate track) |
| jockey×trainer combo win-rate | jvd_se/nvd_se (this probe, Cand A) | 0.002 to −0.007   | ABORT                    |
| yoso_soha_time speed figure   | jvd_se (this probe, Cand B)        | 0.066             | ABORT                    |

The baba_par_time probe already PROCEEDed and is tracked separately (V8 ongoing iteration).
The three remaining candidates (workout, combo, yoso) all ABORT.

**Conclusion**: No further unused signal columns with plausible odds-orthogonal residual
remain in the JV-Data / NV-Data schema that have not been tested or are not already in the
feature set. The new-data feasibility investigation is complete.

The recurrent pattern across all ABORT probes is **market efficiency**: odds and popularity
absorb pre-race signals — workouts, trainer-jockey synergy, published speed figures — before
they can provide independent predictive value. Future accuracy improvements must come from
structural model improvements (architecture, regularization, or cross-category generalization),
not from new feature columns in the existing data schema.
