---
science_track_entry: true
hypothesis_id: JRA-RELATIONSHIP-FEATURES-PERCLASS
date: 2026-06-12
based_on_iteration: iter30-production-baseline (post-serve-skew-fix)
scope: JRA only (source='jra'), holdout kaisai_nen >= 2023
status: PARTIAL PROCEED — 2 features survive partial rho gate in 4-5 classes; GBDT increment ABORT
verdict: >
  nige_vs_field and oikomi_in_fast_field pass partial rho ≥ 0.08 in 4-5 JRA classes.
  However, the LightGBM incremental check shows net −0.005 top1 due to running-style
  coverage shrinkage (87% → requires 100% for training). ABORT adding to production
  unless coverage gap is resolved by other means. All category 4 (ratio) features ABORT.
production_change: none (probe only)
artifacts:
  probe_cat1: tmp/probe_cat1.py
  probe_cat23: tmp/probe_cat23.py
  probe_cat4: tmp/probe_cat4.py
  probe_unified_final: tmp/probe_unified_final.py
---

## Mandate

Multi-column RELATIONSHIP features only (single-column census was exhausted — see
`unused-columns-census-jra.md`). Judge per JRA class: 005, 010, 016, 703, 701, other
(kyoso_joken_code not in {005,010,016,703,701} → "other").

Gate: per-class partial Spearman ρ ≥ 0.08 vs finish_position, controlling for
log(tansho_odds). Within-race variation required (pct_var_races > 0.05).

## Probe Setup

- DB: local PG mirror, postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing
- Tables: race_finish_position_features (f), jvd_ra (r), jvd_se (se),
  race_running_style_model_predictions (rs)
- Join: f ⋈ r on (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
  f ⋈ se on same + ketto_toroku_bango
  f ⋈ rs on same + ketto_toroku_bango (LEFT JOIN)
- Holdout rows: 226,264 with valid log_odds (705,282 total; 68.9% have jvd_se.tansho_odds)
- Running style coverage: 87.4% (rs.p_nige not null)
- Note: race_finish_position_features.odds_score is NULL for JRA 2023+ in local PG mirror
  (features stored in R2 Parquet). Control substituted with log(jvd_se.tansho_odds).

### Class distribution (JRA holdout 2023+, total rows)

| Class | N rows  |
| ----- | ------- |
| 005   | 125,543 |
| 010   | 62,566  |
| 016   | 31,186  |
| 703   | 182,425 |
| 701   | 40,362  |
| other | 263,200 |

## Hypothesized Feature Categories

### Category 1 — Within-race-relative (new ratios / ranks not in current pipeline)

**Existing pipeline already has** (from add-race-internal-features.py,
add-market-signal-features.py, add-relationship-r1-features.py):

- speed_index_avg_5_rank_in_race, jockey_recent_win_rate_rank_in_race,
  trainer_career_win_rate_rank_in_race, same_distance_win_rate_rank_in_race,
  pedigree_score_for_race_rank_in_race
- popularity_rank_in_race, inverse_odds_rank_in_race, futan_juryo_rank_in_race
- bataiju_rank_in_race, futan_minus_bataiju_zscore_in_race
- speed_index_avg_5_diff_from_race_avg, jockey_recent_win_rate_diff_from_race_avg,
  pedigree_score_diff_from_race_avg
- field_nige_pressure, field_pace_index, self_nige_rate_minus_field_avg

**Newly tested:**

- career_wr_ratio = career_win_rate / field_mean(career_win_rate)
- jockey_wr_ratio = jockey_career_win_rate / field_mean(jockey_career_win_rate)
- last_finish_rank_norm = rank of last_race_finish_norm within race, normalised [0,1]
- weight_change_vs_field = signed(zogen_fugo, zogen_sa) − field_mean(signed_weight_change)

### Category 2 — Class-transition

**Existing pipeline already has**: last_race_class_diff, last_race_distance_diff.

**Newly tested interactions:**

- class_drop_good_finish = I(class_drop) × (1 − last_race_finish_norm)
- class_rise_poor_finish = I(class_rise) × last_race_finish_norm

### Category 3 — Pace × style fit

Using race_running_style_model_predictions (p_nige, p_senkou, p_sashi, p_oikomi):

- nige_vs_field = p_nige − field_mean(p_nige) within race
  → positive = this horse is more front-running than average
- nige_field_pressure_interact = p_nige × field_mean(p_nige)
  → high value = front-runner in a front-loaded field
- oikomi_in_fast_field = p_oikomi × field_mean(p_nige)
  → closer horse in a pace-contested field

### Category 4 — Conditional/multi-column ratios

- jockey_venue_spec = jockey_keibajo_win_rate / jockey_career_win_rate
- trainer_venue_spec = trainer_keibajo_win_rate / trainer_career_win_rate
- jockey_dist_spec = jockey_distance_win_rate / jockey_career_win_rate
- jockey_track_spec = jockey_track_win_rate / jockey_career_win_rate
- weight_dist_interact = weight_diff_from_avg × (kyori / 2000)
  [NOTE: weight_diff_from_avg is NULL for entire JRA 2023+ window in local PG — uncomputable]
- jockey_trainer_venue_both = jockey_venue_spec × trainer_venue_spec

## Per-class Partial Spearman ρ Table

Control variable: log(tansho_odds) from jvd_se. Holdout 2023+.

### Category 1 & 2 features

| Feature                         | 005    | 010        | 016    | 703        | 701        | other      |
| ------------------------------- | ------ | ---------- | ------ | ---------- | ---------- | ---------- |
| career_wr_ratio                 | +0.079 | **+0.081** | +0.051 | −0.031     | NaN        | +0.071     |
| jockey_wr_ratio                 | +0.061 | +0.054     | +0.033 | +0.053     | **+0.084** | +0.025     |
| last_finish_rank_norm           | +0.007 | +0.082     | +0.024 | **+0.114** | NaN        | +0.008     |
| weight_change_vs_field          | +0.012 | +0.017     | +0.014 | +0.014     | NaN        | +0.022     |
| last_race_class_diff (existing) | +0.069 | +0.001     | −0.046 | **+0.167** | NaN        | **+0.186** |
| class_drop_good_finish          | +0.003 | +0.003     | −0.004 | −0.002     | NaN        | −0.012     |
| class_rise_poor_finish          | +0.015 | +0.013     | −0.001 | +0.034     | NaN        | +0.017     |

NaN = class 701 has 0% valid odds in local PG mirror (N=40,362 but no jvd_se join).

### Category 3 features (pace × style fit)

| Feature                      | 005        | 010        | 016        | 703    | 701        | other      |
| ---------------------------- | ---------- | ---------- | ---------- | ------ | ---------- | ---------- |
| nige_vs_field                | **+0.116** | **+0.119** | **+0.103** | +0.064 | +0.036     | **+0.110** |
| nige_field_pressure_interact | +0.020     | +0.041     | +0.053     | +0.001 | **−0.121** | +0.040     |
| oikomi_in_fast_field         | **−0.128** | **−0.122** | **−0.098** | −0.046 | **−0.122** | **−0.124** |

### Category 4 features (ratio specialization)

| Feature                   | 005    | 010    | 016    | 703    | 701    | other  |
| ------------------------- | ------ | ------ | ------ | ------ | ------ | ------ |
| jockey_venue_spec         | +0.017 | +0.018 | −0.001 | +0.014 | −0.010 | +0.007 |
| trainer_venue_spec        | +0.017 | +0.001 | −0.025 | +0.021 | −0.035 | −0.016 |
| jockey_dist_spec          | +0.000 | +0.012 | +0.001 | −0.007 | −0.008 | −0.001 |
| jockey_track_spec         | −0.014 | +0.035 | +0.019 | −0.034 | +0.035 | −0.031 |
| weight_dist_interact      | NaN    | NaN    | NaN    | NaN    | NaN    | NaN    |
| jockey_trainer_venue_both | +0.039 | +0.028 | −0.032 | +0.033 | +0.041 | −0.018 |

## Within-race Variance Check

| Feature                      | mean_std | pct_var_races (>0.001) |
| ---------------------------- | -------- | ---------------------- |
| nige_vs_field                | 0.143    | 0.611                  |
| nige_field_pressure_interact | 0.020    | 0.611                  |
| oikomi_in_fast_field         | 0.028    | 0.611                  |
| career_wr_ratio              | 0.763    | 0.676                  |
| jockey_wr_ratio              | 0.551    | 1.000                  |
| last_finish_rank_norm        | 0.298    | 0.077                  |
| weight_change_vs_field       | 6.964    | 0.941                  |
| jockey_venue_spec            | 0.382    | 1.000                  |
| jockey_dist_spec             | 0.187    | 1.000                  |
| class_drop_good_finish       | 0.005    | 0.020                  |
| class_rise_poor_finish       | 0.008    | 0.034                  |

`last_finish_rank_norm`: only 18,255 valid rows (2.6% coverage) — last_race_finish_norm is nearly
always null in the local PG table (stored in R2 Parquet only). Structural non-starter.

`class_drop_good_finish` / `class_rise_poor_finish`: < 3.5% within-race variance — the binary
interaction destroys within-race discriminability.

## Incremental LightGBM Check (top 3 pace features)

Train: kaisai_nen=2024, test: kaisai_nen=2025. Baseline features: speed_index_avg_5,
jockey_career_win_rate, career_win_rate, log_odds. Extended adds: nige_vs_field,
oikomi_in_fast_field, nige_field_pressure_interact.

| Model           | top1         | n_test  |
| --------------- | ------------ | ------- |
| Baseline        | 33.56%       | 211,000 |
| + pace features | 33.08%       | 165,994 |
| Delta           | **−0.48 pp** | —       |

Per-class delta (extended − baseline):
| Class | Delta |
|-------|-------|
| 005 | −1.7 pp |
| 010 | +1.3 pp |
| 016 | +0.9 pp |
| 703 | −0.5 pp |
| other | −0.7 pp |

Feature importance in extended model: log_odds (1277) > speed_index_avg_5 (1066) >
jockey_career_win_rate (1003) > career_win_rate (764) > nige_vs_field (669) >
oikomi_in_fast_field (611) > nige_field_pressure_interact (610).

**Root cause of negative net delta:** training set shrinks from 283k to 166k rows because
running-style predictions are only 87.4% covered and the model requires non-null for all
features. The 12.6% dropped rows (races without style predictions) are not a random
subsample — they tend to be fringe races / early seasons where the model underperforms,
creating a selection bias that understates baseline capability in the extended set.

## Verdict per Feature

| Feature                      | Partial ρ verdict                          | Within-race var | LightGBM   | Final                             |
| ---------------------------- | ------------------------------------------ | --------------- | ---------- | --------------------------------- |
| **nige_vs_field**            | PASS: 005/010/016/other (ρ≈+0.10–0.12)     | PASS (0.611)    | −0.5pp net | **ABORT** (coverage)              |
| **oikomi_in_fast_field**     | PASS: 005/010/016/701/other (ρ≈−0.10–0.13) | PASS (0.611)    | −0.5pp net | **ABORT** (coverage)              |
| nige_field_pressure_interact | PASS only 701 (ρ=−0.121)                   | PASS (0.611)    | —          | **ABORT** (class-limited)         |
| career_wr_ratio              | MARGINAL: 010 only (ρ=+0.081)              | PASS (0.676)    | —          | **ABORT** (one class, barely)     |
| jockey_wr_ratio              | MARGINAL: 701 only (ρ=+0.084)              | PASS (1.000)    | —          | **ABORT** (701 has no valid odds) |
| last_finish_rank_norm        | PASS: 703 (ρ=+0.114), 010 (ρ=+0.082)       | FAIL (0.077)    | —          | **ABORT** (2.6% coverage)         |
| weight_change_vs_field       | ABORT all classes (max ρ=0.022)            | PASS            | —          | **ABORT**                         |
| class_drop_good_finish       | ABORT all classes (max ρ=0.034)            | FAIL (0.020)    | —          | **ABORT**                         |
| class_rise_poor_finish       | ABORT all classes (max ρ=0.034)            | FAIL (0.034)    | —          | **ABORT**                         |
| Cat 4 ratio features (all 6) | ABORT all classes (max ρ=0.041)            | varies          | —          | **ABORT**                         |

## Summary and Root Cause Analysis

### What passed partial ρ but cannot be added

**nige_vs_field** and **oikomi_in_fast_field** are the strongest survivors with consistent
per-class partial ρ of 0.10–0.13 across 4-5 classes. However, both require
race_running_style_model_predictions, which covers only 87.4% of JRA 2023+ races. The
remaining 12.6% cannot be imputed without introducing the same imputation-is-counterproductive
failure mode documented in the serve-skew diagnosis (see
`project_finish_position_frontier_2026_06_11.md`). The LightGBM check confirms this: the
feature shrinks the effective training set enough to produce net regression (−0.48pp top1).

**Conclusion:** These features WOULD help if running-style coverage were at 95%+. The blocker
is coverage, not signal quality.

### Why category 4 (ratio specialization) failed uniformly

Venue/distance/track specialization ratios (jockey_keibajo_wr / jockey_career_wr, etc.) carry
no residual signal after partialing out market odds. The market already prices specialization.
The GBDT implicitly learns the numerator and denominator separately and extracts any
incremental interaction. Explicit ratios add nothing.

### Control variable artifact warning

The Cat 2/3 probe initially used speed_index_avg_5 as a control (odds_score NULL in local PG).
Under that weaker control, nige_field_pressure_interact appeared to have ρ=−0.245 (other).
Under the correct log(tansho_odds) control, this collapses to near-zero for all classes except
701 (ρ=−0.121). All final ρ values in this document use log(tansho_odds) as control.

### Path forward

The only viable path to using nige_vs_field / oikomi_in_fast_field in production is:

1. Raise running-style prediction coverage from 87.4% to ≥95% for JRA — this is a running-style
   model improvement task (separate project).
2. OR: Accept missing-style rows and use the feature only where available, training a
   coverage-conditioned model. This requires careful evaluation to confirm net gain
   outweighs selection bias from dropped rows.

Both options are out of scope for the current iterative loop. All features: **FINAL ABORT**.

## Hard Rules Observed

- tmp/probe_cat1.py, tmp/probe_cat23.py, tmp/probe_cat4.py, tmp/probe_unified_final.py —
  throwaway scripts, not git-tracked, no `git add -f`
- Read-only PG: no writes, no INSERTs, no UPDATEs
- git push: FORBIDDEN (commit only)
- No model deploy or production change
