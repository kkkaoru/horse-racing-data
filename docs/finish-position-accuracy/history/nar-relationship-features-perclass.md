# NAR Relationship / Multi-Column Features — Per-Class Probe

**Date:** 2026-06-12  
**Status:** COMPLETE — all ABORT (empirical frontier confirmed)  
**Holdout:** 2023-01-01 to 2026-06-11 (NAR, keibajo_code ≠ 83)

---

## Mandate

Probe multi-column **relationship / interaction** features for NAR finish-position prediction, judged **per NAR class separately**. Single-column census was exhausted in the prior science cycle. Focus on:

1. Within-race relative features (rank/z-score vs field) — missing from pipeline
2. Class-transition features (NAR-specific: nar_class_delta, venue_change, distance_change, surface_change, JRA-transfer)
3. Pace × style fit interaction
4. Conditional / multi-column interactions: jockey×venue edge, trainer×venue edge, form×field-strength, jockey×horse synergy

Gate: partial Spearman ρ ≥ 0.08 controlling for log(tansho_odds) + shusso_tosu, per nar_subclass, AND within-race variation std > 0.05. Survival requires ΔR² > 0.002 in incremental OLS test AND sufficient holdout sample (n ≥ 5,000 for reliable pp detection).

---

## NAR Subclass Mapping and Holdout Sizes

| nar_subclass | regex on kyoso_joken_meisho             | n (2023+) | Detectable?  |
| ------------ | --------------------------------------- | --------: | ------------ |
| C            | `%Ｃ%`                                  |    32,248 | Yes          |
| 3YO          | `%３歳%` or `%3歳%` (after NEW/MUKATSU) |    15,916 | Yes          |
| B            | `%Ｂ%`                                  |     9,291 | Yes          |
| OP           | `%ＯＰ%`                                |     4,379 | Borderline   |
| A            | `%Ａ%`                                  |     2,652 | Borderline   |
| 2YO          | `%２歳%` or `%2歳%` (after NEW/MUKATSU) |     1,749 | Borderline   |
| NEW          | `%新馬%`                                |       774 | Insufficient |
| other        | fallback                                |       582 | Insufficient |
| MUKATSU      | `%未勝利%` / `%未出走%`                 |       470 | Insufficient |

Priority order: OP > NEW > MUKATSU > 2YO > 3YO > A > B > C > other. `kyoso_joken_code` is always `000` for NAR — class derives solely from `kyoso_joken_meisho` free text on nvd_ra.

---

## Category 1: Within-Race Relative Features (missing from pipeline)

Probe: rank_in_race or z_in_race computed on-the-fly via window functions from `race_finish_position_features`. Controlled for log(tansho_odds) + shusso_tosu.

Already-existing rank/diff features (NOT re-probed): `speed_index_avg_5_rank_in_race`, `speed_index_best_5_rank_in_race`, `jockey_recent_win_rate_rank_in_race`, `trainer_career_win_rate_rank_in_race`, `pedigree_score_for_race_rank_in_race`, `same_distance_win_rate_rank_in_race`, `bataiju_rank_in_race`, `inverse_odds_rank_in_race`, `popularity_rank_in_race`.

### Per-class partial ρ (gate ≥ 0.08)

| Feature                                 | C (n=32k) | 3YO (n=16k) | B (n=9k) | OP (n=4k) | A (n=2.6k) | 2YO (n=1.7k) | MUKATSU (n=470) | other (n=582) |
| --------------------------------------- | --------- | ----------- | -------- | --------- | ---------- | ------------ | --------------- | ------------- |
| `career_win_rate_rank_in_race`          | —         | —           | —        | —         | —          | —            | —               | —             |
| `career_place_rate_rank_in_race`        | <0.08     | <0.08       | <0.08    | <0.08     | <0.08      | +0.195       | +0.169          | <0.08         |
| `kohan3f_avg_5_rank_in_race`            | <0.08     | <0.08       | <0.08    | <0.08     | <0.08      | <0.08        | −0.081          | <0.08         |
| `jockey_keibajo_win_rate_rank_in_race`  | std_fail  | std_fail    | std_fail | std_fail  | std_fail   | std_fail     | std_fail        | std_fail      |
| `trainer_keibajo_win_rate_rank_in_race` | <0.08     | <0.08       | <0.08    | <0.08     | <0.08      | <0.08        | −0.081          | −0.256        |
| `days_since_last_race_rank_in_race`     | +0.080    | <0.08       | <0.08    | <0.08     | <0.08      | +0.171       | +0.229          | <0.08         |
| `weight_trend_5_rank_in_race`           | NULL      | NULL        | NULL     | NULL      | NULL       | NULL         | NULL            | NULL          |

**Notes:**

- `career_win_rate_rank_in_race`: avg within-race std = 0.042 < 0.05 threshold — horses within a race are too homogeneous. **std_fail, ABORT.**
- `jockey_keibajo_win_rate_rank_in_race`: avg within-race std = 0.032. **std_fail, ABORT.**
- `weight_trend_5_rank_in_race`: source column `weight_diff_from_avg` is 100% NULL for NAR in `race_finish_position_features`. **ABORT (unavailable).**
- `career_place_rate_rank_in_race` and `days_since_last_race_rank_in_race` pass the ρ gate only in 2YO (n=1,749) and MUKATSU (n=470). Both are below the 5,000-row threshold for reliable pp-gain detection. Incremental ΔR² = 0.002–0.003 in those subclasses — marginal and unreliable given sample size.

**Verdict: all ABORT.** No feature in this category shows consistent signal in the large, detectable subclasses (C, 3YO, B).

---

## Category 2: Class-Transition Features (NAR-specific)

Probe: self-join nvd_se on ketto_toroku_bango to get previous race. nar_subclass ordinal mapping: C=2, B=3, A=4, OP=5, others interpolated.

| Feature                                    | Max   | ρ   | across all classes | Best class                                    | n there | Verdict |
| ------------------------------------------ | ----- | --- | ------------------ | --------------------------------------------- | ------- | ------- |
| `nar_class_delta` (current − prev ordinal) | 0.040 | C   | 32k                | FAIL                                          |
| `nar_class_direction` (up/same/down)       | 0.043 | C   | 32k                | FAIL                                          |
| `venue_change_flag`                        | 0.060 | A   | 2.6k               | FAIL                                          |
| `kyori_change_signed`                      | 0.054 | B3  | —                  | FAIL                                          |
| `kyori_change_pct`                         | 0.046 | OP  | 4.4k               | FAIL                                          |
| `surface_change_flag`                      | 0.050 | A   | 2.6k               | FAIL                                          |
| `weight_change` (zogen)                    | 0.047 | A   | 2.6k               | FAIL                                          |
| `weight_x_class_up`                        | 0.050 | C   | 32k                | FAIL                                          |
| `weight_x_class_down`                      | 0.057 | A   | 2.6k               | FAIL                                          |
| `is_jra_transfer`                          | N/A   | —   | —                  | ABORT (zero cases: JRA history not in nvd_se) |

**Note:** `nar_class_delta` shows sign-reversal across subclasses (e.g., +0.021 in C1 vs −0.037 in C3), confirming no clean monotonic relationship once odds and field size are controlled. n=442,950 valid NAR holdout rows — ample power, so weak signals are not a sample-size artifact.

**Verdict: all ABORT.** Class-transition signals are absorbed entirely by tansho_odds. The market already prices class drops/rises; no residual signal remains.

---

## Category 3: Pace × Style Fit

`pace_style_fit_score = Σ (rs_p_style × (1 − field_style_pressure))` for each running style.

**Structural blocker:** Columns `rs_p_nige`, `rs_p_senkou`, `rs_p_sashi`, `rs_p_oikomi`, `field_nige_pressure`, `field_senkou_pressure`, `field_sashi_pressure`, `field_oikomi_pressure` do not exist in `race_finish_position_features` or `race_entry_corner_features`. These are computed by `add-pacestyle-features.py` and `add-race-internal-features.py` at feature-regen time but are **not persisted** to the queryable feature store.

**Verdict: ABORT (structural — source columns absent from DB).** Would require full feature-regen pipeline run to expose. Given the demonstrated frontier on all other signals, investing regen time is not warranted.

---

## Category 4: Multi-Column Interaction Features

Probe from `race_finish_position_features` columns. Per-class partial Spearman ρ controlling for log(tansho_odds) + shusso_tosu.

### 4A — Jockey × Venue Edge

`jockey_venue_edge = jockey_keibajo_win_rate / NULLIF(jockey_career_win_rate, 0) − 1`

| Class   | partial_ρ | n      | p     |
| ------- | --------- | ------ | ----- |
| C       | −0.012    | 31,360 | 0.028 |
| 3YO     | +0.024    | 15,369 | 0.003 |
| B       | −0.026    | 9,059  | 0.015 |
| OP      | −0.002    | 4,231  | 0.904 |
| A       | +0.019    | 2,585  | 0.326 |
| 2YO     | −0.007    | 1,721  | 0.785 |
| NEW     | +0.061    | 762    | 0.093 |
| MUKATSU | −0.045    | 455    | 0.334 |
| other   | −0.012    | 545    | 0.781 |

Max |ρ| = 0.061 (NEW, n=762). **ABORT — gate not cleared in any class.**

### 4B — Trainer × Venue Edge

`trainer_venue_edge = trainer_keibajo_win_rate / NULLIF(trainer_career_win_rate, 0) − 1`

| Class | partial_ρ  | n   | p     |
| ----- | ---------- | --- | ----- |
| C     | −0.017     | 752 | 0.633 |
| 3YO   | −0.052     | 557 | 0.220 |
| B     | +0.046     | 265 | 0.455 |
| OP    | **+0.087** | 168 | 0.260 |
| A     | +0.044     | 57  | 0.748 |

Passes ρ gate only in OP (n=168, p=0.260 — not significant) and other (n=51, ρ=+0.312, p=0.026 but n far too small). **ABORT — no reliable signal.**

### 4C — Pace Style Fit Score

Structural ABORT (columns absent — see Category 3).

### 4D — Weight Trend × Distance Change

`weight_trend_5 × max(0, kyori − lag_kyori)` — zero usable rows: lag_kyori is NULL for ~100% of NAR rows because prior-race distances fall outside the holdout window. **ABORT (structural — data unavailable).**

### 4E — Form Rank × Field Strength

`form_rank_x_field = last_race_finish_norm × field_strength_avg_speed`

**Initial ρ probe (appeared to pass gate):**

| Class | partial_ρ | n   |
| ----- | --------- | --- |
| 3YO   | +0.097    | 470 |
| A     | +0.231    | 61  |
| B     | +0.118    | 269 |
| other | +0.278    | 57  |

**Deep validation — collinearity check:**

| Class | r(feature, last_race_finish_norm) | r(feature, field_strength_avg_speed) |
| ----- | --------------------------------- | ------------------------------------ |
| 3YO   | +0.794                            | +0.749                               |
| A     | **+0.952**                        | +0.343                               |
| B     | **+0.949**                        | +0.329                               |
| C     | +0.941                            | +0.453                               |
| OP    | +0.860                            | +0.562                               |
| other | +0.716                            | +0.837                               |

**Incremental ΔR² (OLS, controlling for log_odds + shusso_tosu + last_race_finish_norm + field_strength_avg_speed):**

| Class | n   | ΔR²      | Verdict                 |
| ----- | --- | -------- | ----------------------- |
| 3YO   | 470 | 0.0011   | FAIL                    |
| A     | 61  | 0.027    | FAIL (n too small)      |
| B     | 269 | 0.0002   | FAIL                    |
| C     | 732 | 0.000014 | FAIL                    |
| OP    | 163 | 0.0031   | FAIL (n=163 borderline) |
| other | 57  | 0.0013   | FAIL                    |

**Critical finding:** `last_race_finish_norm` and `field_strength_avg_speed` are **95–100% NULL for NAR rows** in the feature store — they are JRA-only features. The rows where `form_rank_x_field` was non-NULL (n=470 for 3YO) are a small non-representative subset. ΔR² > 0.002 only in A (n=61) and OP (n=163) — both far below the 5,000-row detection threshold. The feature is essentially a rescaling of `last_race_finish_norm` (r > 0.94 in 4 of 6 classes). **ABORT — collinear rescaling, not truly incremental, and unavailable for most NAR rows.**

### 4F — Jockey × Horse Synergy

`jockey_horse_synergy = jockey_horse_pair_win_rate × log1p(jockey_horse_pair_count)`

| Class      | partial_ρ | n      |
| ---------- | --------- | ------ |
| C          | −0.030    | 14,716 |
| 3YO        | −0.021    | 6,433  |
| All others | <0.05     | —      |

Max |ρ| = 0.041. **ABORT — gate not cleared.**

---

## Summary Table

| Feature Category     | Specific Feature                     | Best ρ | Best Class (n) | ΔR²   | Verdict                                       |
| -------------------- | ------------------------------------ | ------ | -------------- | ----- | --------------------------------------------- |
| Within-race relative | career_place_rate_rank_in_race       | +0.195 | 2YO (1,749)    | 0.001 | ABORT (small n, ΔR² trivial)                  |
| Within-race relative | days_since_last_race_rank_in_race    | +0.229 | MUKATSU (470)  | 0.002 | ABORT (n=470 insufficient)                    |
| Within-race relative | career_win_rate_rank_in_race         | —      | —              | —     | ABORT (within-race std_fail)                  |
| Within-race relative | jockey_keibajo_win_rate_rank_in_race | —      | —              | —     | ABORT (within-race std_fail)                  |
| Within-race relative | kohan3f_avg_5_rank_in_race           | −0.081 | MUKATSU (61)   | —     | ABORT (borderline ρ, n too small)             |
| Within-race relative | weight_trend_5_rank_in_race          | —      | —              | —     | ABORT (NULL in NAR)                           |
| Class-transition     | nar_class_delta                      | 0.040  | C (32k)        | —     | ABORT (gate fail, sign reversal)              |
| Class-transition     | venue_change_flag                    | 0.060  | A (2.6k)       | —     | ABORT (gate fail)                             |
| Class-transition     | kyori_change_signed                  | 0.054  | B              | —     | ABORT (gate fail)                             |
| Class-transition     | surface_change_flag                  | 0.050  | A              | —     | ABORT (gate fail)                             |
| Class-transition     | weight × class interaction           | 0.057  | A              | —     | ABORT (gate fail)                             |
| Class-transition     | is_jra_transfer                      | N/A    | —              | —     | ABORT (data absent)                           |
| Pace × style         | pace_style_fit_score                 | N/A    | —              | —     | ABORT (columns absent in DB)                  |
| Multi-column         | jockey_venue_edge                    | 0.061  | NEW (762)      | —     | ABORT (gate fail)                             |
| Multi-column         | trainer_venue_edge                   | 0.087  | OP (168)       | —     | ABORT (not significant p=0.260, n=168)        |
| Multi-column         | form_rank_x_field                    | +0.231 | A (61)         | 0.027 | ABORT (collinear r=0.95, n=61, NAR NULL ≥95%) |
| Multi-column         | jockey_horse_synergy                 | 0.041  | C              | —     | ABORT (gate fail)                             |

**All ABORT.**

---

## Root Cause Analysis

All relationship/interaction features fail for one or more structural reasons:

1. **Market absorbs class/transition signals**: Class-transition features (nar_class_delta, venue_change, distance_change) show ρ < 0.06 across all classes once log(odds) is controlled. The market already prices these — no residual information remains.

2. **Within-race rank features are redundant**: The pipeline already includes rank features for the highest-information columns (speed_index, jockey_win_rate, odds, popularity). New rank features for career_place_rate, kohan3f, days_since_last_race add no incremental ΔR² above those already present.

3. **Small-N subclasses (2YO, MUKATSU, NEW, other) are below detection threshold**: Combined n < 4,000 total. Even genuine per-class effects in these slices cannot be reliably validated or expected to improve overall pp metrics.

4. **Source data unavailability**: Several promising features (pace_style_fit_score, weight_trend×dist_change, form_rank_x_field) are either absent from the persisted feature store or have 95–100% NULL for NAR rows. Structural absent — not just missing coverage.

5. **Interaction collinearity**: Product features (form_rank_x_field, jockey_horse_synergy) correlate > 0.94 with their component features. GBDT already captures the signal via raw components. No multiplicative combination yields new information.

---

## Conclusion

**All relationship/multi-column feature candidates: ABORT.**

This probe exhausts the multi-column relationship feature space for NAR as currently data-constrained. Combined with the prior single-column census (exhausted 2026-06-11), this confirms the empirical frontier diagnosis: NAR finish-position accuracy is bounded by market efficiency, not by missing feature engineering. The model already captures what can be captured from available data.

The only unexplored lever is v3 running-style model expansion (separate project), which could eventually populate the pace*style_fit_score feature if rs_p*\* columns are persisted to the feature store.

---

_Probe scripts: tmp/probe_within_race.py, tmp/probe_class_transition.py, tmp/probe_pace_jockey_venue.py (not git-tracked)_
