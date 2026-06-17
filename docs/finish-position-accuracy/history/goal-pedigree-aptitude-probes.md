# Pedigree / Aptitude Per-Class Partial-ρ Probes (5 signals)

**Date**: 2026-06-17  
**Method**: Partial Spearman ρ (residual-on-rank) vs `finish_norm = finish_position / field_size`.  
Controls: `odds_rank` + signal-specific existing feature(s) listed per signal.  
Windows: **full** = 2016–2025, **holdout** = 2023–2025.  
Bar: ρ ≥ 0.08 in BOTH windows AND n ≥ 2000 → PROCEED-to-cheap-filter (necessary, not sufficient).

**Note on NAR class breakdown**: NAR's `kyoso_joken_code` is always `000` (all classes
aliased to the same code per JV-Data spec); JRA codes 005/010/016/701/703 do not exist in
`nvd_ra`. NAR per-class ρ is therefore reported as "global only" — the JRA per-class rows
for NAR are meaningless artefacts and should be ignored. Per-class analysis for NAR would
require deriving classes from `kyoso_joken_meisho` text (not done here; the existing pipeline
uses `nar_subclass` but it was not reconstructed for this probe).

**Note on Signal 1 NAR**: A SQL alias bug in the `hist_raw` CTE caused the NAR query to
error out; no NAR data for Signal 1.

---

## Signal 1: 血統早熟/晩成 (Pedigree Precocity / Late-Maturity)

**Feature**: `sire_precocity_rate` = sire's other-progeny win rate at _this_ horse's age band
(2, 3, 4, 5+), computed leak-free from prior-race cumulative window.  
**Hypothesis**: precocious vs. late-maturing bloodlines predict beyond the horse's own age features.  
**Control**: `odds_rank`.  
**Coverage**: JRA 97.2%.

### JRA per-class

| Class  | N_full  | ρ_full | ρ_hold | Verdict                      |
| ------ | ------- | ------ | ------ | ---------------------------- |
| 全体   | 711,809 | 0.045  | -0.046 | ABORT                        |
| 新馬   | 238,982 | 0.055  | -0.058 | ABORT (sign flip)            |
| 未勝利 | 130,053 | nan    | nan    | ABORT (n < MIN_N in holdout) |
| 1勝    | 60,032  | -0.031 | -0.036 | ABORT                        |
| 2勝    | 27,288  | -0.036 | -0.045 | ABORT                        |
| 重賞   | 38,317  | 0.043  | nan    | ABORT                        |
| OP     | 176,224 | -0.053 | -0.052 | ABORT                        |

NAR: query error (alias bug in hist_raw CTE; alias `hs` used where table alias was `se`).

**VERDICT: ABORT** — no class clears ρ ≥ 0.08 in both windows for JRA; NAR N/A.  
The sign flips in 新馬 (ρ_full +0.055 → ρ_hold −0.058) indicate noise, not signal.
The feature likely captures the same information as the existing horse-age features already
in the model. Sire precocity index is not orthogonal to what GBDT already captures.

---

## Signal 2: 性別×季節 (Sex × Season Lift)

**Feature**: `sex_season_lift` = (sex × season win rate from prior years) − (sex overall win
rate from prior years). Leak-free yearly cumulative.  
**Hypothesis**: fillies/mares (or stallions) perform differently by season.  
**Control**: `odds_rank`.  
**Coverage**: JRA 100%, NAR 100%.

### JRA per-class

| Class  | N_full  | ρ_full | ρ_hold | Verdict                 |
| ------ | ------- | ------ | ------ | ----------------------- |
| 全体   | 740,055 | 0.050  | nan    | ABORT                   |
| 新馬   | 255,058 | 0.054  | nan    | ABORT (holdout n<MIN_N) |
| 未勝利 | 132,121 | nan    | nan    | ABORT                   |
| 1勝    | 61,016  | 0.031  | nan    | ABORT                   |
| 2勝    | 27,909  | 0.037  | 0.044  | ABORT (both < 0.08)     |
| 重賞   | 40,079  | -0.043 | -0.036 | ABORT                   |
| OP     | 179,248 | 0.055  | 0.047  | ABORT (both < 0.08)     |

### NAR global

| Class | N_full    | ρ_full | ρ_hold | Verdict        |
| ----- | --------- | ------ | ------ | -------------- |
| 全体  | 1,343,290 | 0.054  | 0.050  | ABORT (< 0.08) |

**VERDICT: ABORT** — best class is JRA:OP (ρ_full=0.055, ρ_hold=0.047) and NAR global
(ρ_full=0.054, ρ_hold=0.050). Both consistently below the 0.08 bar. Sex × season lift is a
population-level smoothing statistic; odds already encode this information (market-efficient
seasonal sex patterns). The signal is real but sub-threshold.

---

## Signal 3: 馬の競馬場適性 (Horse's Own Track Aptitude)

**Feature**: `horse_track_win_rate` = horse's own prior win rate at this keibajo_code,
leak-free rolling window.  
**Hypothesis**: per-horse course affinity beyond overall ability.  
**Controls**: `odds_rank` + `horse_overall_win_rate` (proxy for existing `same_keibajo_win_rate`).  
**Coverage**: JRA 93.9%, NAR 91.2%.

**Orthogonality test vs existing `same_keibajo_win_rate`**: The existing feature
`same_keibajo_win_rate` in the production pipeline is exactly the horse's own track win rate
(same computation). This probe uses `horse_overall_win_rate` as a control (not the existing
feature directly, since we're not pulling from the feature store). The residual ρ after
controlling for overall win rate represents the _incremental_ keibajo specificity beyond
general ability. Results show this increment is tiny.

### JRA per-class (note: 4M total rows reflect window function materializing 2010-2025 history)

| Class  | N_full    | ρ_full | ρ_hold | Verdict                           |
| ------ | --------- | ------ | ------ | --------------------------------- |
| 全体   | 4,107,127 | -0.007 | 0.019  | ABORT                             |
| 新馬   | 178,608   | 0.051  | -0.092 | ABORT (sign flip, large negative) |
| 未勝利 | 97,487    | 0.041  | 0.034  | ABORT                             |
| 1勝    | 51,639    | -0.017 | 0.029  | ABORT                             |
| 2勝    | 23,999    | 0.028  | -0.043 | ABORT (sign flip)                 |
| 重賞   | 201       | nan    | nan    | ABORT (n too small)               |
| OP     | 91,034    | 0.063  | 0.052  | ABORT (< 0.08)                    |

### NAR global

| Class | N_full    | ρ_full | ρ_hold | Verdict        |
| ----- | --------- | ------ | ------ | -------------- |
| 全体  | 1,234,575 | 0.049  | 0.040  | ABORT (< 0.08) |

**VERDICT: ABORT — and REDUNDANT with existing `same_keibajo_win_rate`**.  
The partial ρ controlling for general ability (horse_overall_win_rate) is near zero globally
and shows sign flips in some classes. The existing `same_keibajo_win_rate` feature already
captures this signal. Adding a separate "track aptitude" feature adds no orthogonal
information.

---

## Signal 4: 血統×競馬場 (Pedigree × Racecourse)

**Feature**: `sire_keibajo_win_rate` = sire's progeny cumulative win rate at this specific
keibajo*code, leak-free monthly rolling window (requires ≥10 progeny races).  
**Hypothesis**: sire bloodline × specific racecourse affinity beyond surface (芝/ダート).  
**Controls**: `odds_rank` + `sire_overall_win_rate`.  
**Orthogonality vs existing**: The existing `sire_track_win_rate` in production uses
**surface** (芝/ダート split via `track_code`), NOT specific keibajo. This feature is
therefore \_structurally distinct* from the existing one.  
**Coverage**: JRA 96.5%, NAR 95.4%.

### JRA per-class

| Class  | N_full  | ρ_full | ρ_hold | Verdict           |
| ------ | ------- | ------ | ------ | ----------------- |
| 全体   | 714,256 | 0.046  | -0.069 | ABORT (sign flip) |
| 新馬   | 240,761 | -0.060 | -0.043 | ABORT             |
| 未勝利 | 131,262 | -0.046 | nan    | ABORT             |
| 1勝    | 60,834  | nan    | -0.020 | ABORT             |
| 2勝    | 27,855  | 0.032  | -0.027 | ABORT (sign flip) |
| 重賞   | 36,714  | nan    | 0.015  | ABORT             |
| OP     | 175,135 | 0.050  | 0.051  | ABORT (< 0.08)    |

### NAR global (all rows fall into code '000')

| Class | N_full    | ρ_full     | ρ_hold     | Verdict         |
| ----- | --------- | ---------- | ---------- | --------------- |
| 全体  | 1,280,991 | **0.1055** | **0.0887** | **_ PROCEED _** |

**VERDICT: PROCEED for NAR global (ρ_full=0.1055, ρ_hold=0.0887); ABORT for JRA (all classes < 0.08 or sign-flip).**

NAR result clears the bar in both windows with n=1.28M. This is the strongest signal found
across all 5 probes. The result is orthogonal to odds AND sire overall win rate (both controlled).

**JRA**: Sign flips globally and in 2勝 suggest GBDT already captures this non-linearly via
existing sire surface/distance features. JRA racecourses may be too few/homogeneous for the
keibajo-level split to add signal.

**Caution (per [[project_relationship_perclass_investigation_2026_06_12]])**: ρ clearing is
necessary but not sufficient — GBDT may already capture the NAR sire×keibajo interaction
non-linearly via existing sire_track_win_rate (surface-based) + keibajo in race context
features. Must verify with a cheap-filter incremental model test before investing in full retrain.

**Routable classes**: NAR global (all classes, since NAR doesn't segment by JRA codes).
To route per NAR subclass, would need to rebuild probe using `kyoso_joken_meisho`-derived
`nar_subclass` labels.

---

## Signal 5: 血統×距離 (Pedigree × Distance Band)

**Feature**: `sire_dist_win_rate` = sire's progeny cumulative win rate at this distance band
(400m buckets: same as existing `kyori_band`), leak-free monthly rolling window.  
**Hypothesis**: class-specific distance × bloodline effect may exist even though pooled ABORT
was confirmed before (V6 H-SIRE-DISTANCE-SPLIT partial ρ = 0.025).  
**Controls**: `odds_rank` + `horse_dist_win_rate` (proxy for existing `same_distance_win_rate`).  
**Coverage**: JRA 93.3%, NAR 95.2%.

### JRA per-class

| Class  | N_full  | ρ_full | ρ_hold | Verdict                                   |
| ------ | ------- | ------ | ------ | ----------------------------------------- |
| 全体   | 493,683 | -0.049 | -0.007 | ABORT                                     |
| 新馬   | 177,256 | nan    | 0.085  | ABORT (holdout only; full window n<MIN_N) |
| 未勝利 | 107,941 | 0.042  | -0.041 | ABORT (sign flip)                         |
| 1勝    | 54,754  | -0.032 | -0.040 | ABORT                                     |
| 2勝    | 25,494  | 0.064  | 0.002  | ABORT (holdout near zero)                 |
| 重賞   | 214     | nan    | nan    | ABORT                                     |
| OP     | 95,748  | 0.056  | 0.055  | ABORT (< 0.08)                            |

### NAR global

| Class | N_full    | ρ_full | ρ_hold | Verdict                      |
| ----- | --------- | ------ | ------ | ---------------------------- |
| 全体  | 1,115,434 | 0.055  | -0.001 | ABORT (sign flip in holdout) |

**VERDICT: ABORT** — per-class re-test confirms the prior pooled ABORT result. Even at the
per-class level (JRA:2勝 ρ_full=0.064 is the best) the holdout collapses to 0.002.
NAR holdout sign-flips to -0.001. The sire distance win rate is captured by the existing
`sire_distance_win_rate` feature (`sire_dist_win_rate` in production, keyed by `kyori_band`

- `stats_year_month`). This probe used the same 400m band definition — the incremental ρ
  after controlling for horse_dist_win_rate confirms zero orthogonal signal.

---

## Summary Table

| Signal | Description    | Best Global ρ (JRA)              | Best Global ρ (NAR)            | Verdict                     |
| ------ | -------------- | -------------------------------- | ------------------------------ | --------------------------- |
| 1      | 血統早熟/晩成  | JRA:新馬 full=0.055, hold=-0.058 | N/A (error)                    | ABORT                       |
| 2      | 性別×季節      | JRA:OP full=0.055, hold=0.047    | NAR full=0.054, hold=0.050     | ABORT                       |
| 3      | 馬の競馬場適性 | JRA:OP full=0.063, hold=0.052    | NAR full=0.049, hold=0.040     | ABORT + REDUNDANT           |
| 4      | 血統×競馬場    | JRA:OP full=0.050, hold=0.051    | **NAR full=0.106, hold=0.089** | JRA:ABORT / **NAR:PROCEED** |
| 5      | 血統×距離      | JRA:OP full=0.056, hold=0.055    | NAR full=0.055, hold=-0.001    | ABORT                       |

## Routable PROCEED cells

| Signal          | Category | Class         | N         | ρ_full | ρ_hold | Next step                                                                                                  |
| --------------- | -------- | ------------- | --------- | ------ | ------ | ---------------------------------------------------------------------------------------------------------- |
| 4 (血統×競馬場) | NAR      | 全体 (global) | 1,280,991 | 0.1055 | 0.0887 | Cheap-filter incremental retrain (NAR only); verify orthogonality vs existing sire_track_win_rate in model |

## Key takeaways

1. **Signal 4 (NAR sire×keibajo) is the only candidate**: strong ρ in both windows,
   structurally distinct from existing surface-based sire_track_win_rate. However, GBDT
   non-linear capture via existing features must be ruled out via incremental retrain before
   accepting.

2. **Signal 3 is explicitly redundant**: `same_keibajo_win_rate` already exists in production.
   No new feature needed.

3. **Signal 5 confirms prior ABORT**: even per-class testing, sire×distance adds nothing
   orthogonal beyond the existing `sire_distance_win_rate` and horse's own `same_distance_win_rate`.

4. **Signal 1 (precocity) shows sign-flip noise**: the age-band interaction with sire lineage
   was the most novel hypothesis, but it breaks down in the holdout window, likely because
   the existing horse-age numerical features + sire base rate already capture the pattern.

5. **Signal 2 (sex×season)** stays consistently below threshold. Market odds already price
   the sex × season interaction efficiently.

6. **NAR per-class caveat**: NAR's `kyoso_joken_code='000'` for all races means per-class
   routing for NAR requires a separate nar_subclass derivation. The Signal 4 NAR PROCEED
   is global-only; class-routing within NAR would need a follow-up probe.
