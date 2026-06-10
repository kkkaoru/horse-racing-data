---
science_track_entry: true
hypothesis_id: H-AGE-SEX-BW-ZSCORE
date: 2026-06-11
based_on_iteration: iter30-nar-cb-residual-*-v8 (production baseline, 190 features)
scope: NAR (all keibajo except Banei), probe-only
status: ABORT (partial rho=−0.0264 << bar 0.08; growth-curve residual fully absorbed by existing BW family)
verdict: ABORT — bw_age_sex_z is not orthogonal to existing features after partialling
production_change: none
artifacts:
  probe_script: tmp/nar-perclass/sci_track/b2_bwzscore/probe.py
  probe_verdict: tmp/nar-perclass/sci_track/b2_bwzscore/probe_verdict.json
  zscore_parquet: tmp/nar-perclass/sci_track/b2_bwzscore/zscore_parquet/
citations:
  - 24_1312 (JES F) — BW/withers Z-scores, seasonal CG, mature-size proxy
  - 25_1402 (JES F) — percentile growth curves, relative birth date, age-adjusted BW residual
---

## Hypothesis

**H-AGE-SEX-BW-ZSCORE** (B2 — age×sex-normalized body-weight z-score):

A horse's raw body weight conflates breed/sex/age maturation. Standardizing bataiju
WITHIN (age_years, sex) cohorts produces a z-score = growth-curve residual that
represents whether the horse is heavy or light FOR its age and sex. The hypothesis
(from JES 24_1312 / 25_1402 growth-curve work) is that this residual is orthogonal
to raw weight and carries a genuine condition/frame signal not captured by the
existing body-weight family.

Formally:

```
bw_age_sex_z = (bataiju − mean_bataiju[age_years, sex]) / std_bataiju[age_years, sex]
```

Cohort statistics computed on frozen 2007–2017 window; applied to 2018–2024 probe.

**Key distinction from prior tests**: H-SEASONAL-BW (V1) standardized by
(sex × age_bucket × race_month) — a finer grid that also absorbed seasonal variation.
B2 uses only (age_years × sex) — a coarser grid that targets the growth-curve
maturation residual without the month dimension. The hypothesis is that this coarser
z-score is less correlated with the existing bataiju/futan family and carries an
independent signal.

## Cohort Statistics (frozen 2007–2017)

39 cells total: sex ∈ {1=牡, 2=牝, 3=騸} × age 2–15 (only age 2–13 for sex=3,
2–13 for sex=2 above threshold). All cells have n ≥ 30; most cells 1,000–200,000+.

Summary of maturation gradient (sex=1/male cohort peak norms):

| Age | Male norm (kg) | Female norm (kg) | Gelding norm (kg) |
| --- | -------------- | ---------------- | ----------------- |
| 2   | 460.9          | 436.2            | 449.5             |
| 3   | 470.3          | 441.5            | 460.3             |
| 4   | 476.5          | 447.6            | 464.8             |
| 5   | 478.8          | 452.5            | 468.0             |
| 6   | 481.2          | 455.5            | 469.7             |
| 7   | 482.5          | 456.0            | 472.9             |
| 8+  | ~483           | ~457             | ~473              |

Growth plateaus at age 6–8 for all three sexes, consistent with the JES growth-curve
literature. Sex gap: males ~25kg heavier than females at any age; geldings intermediate.

## Probe Results

**Feature**: `bw_age_sex_z` — single-column z-score, computed leak-free from 2007–2017
cohort norms applied to each starter in probe years 2018–2024.

### Coverage

| Metric                       | Value   |
| ---------------------------- | ------- |
| Coverage (non-null fraction) | 99.95%  |
| Within-race variation (≥2)   | 99.99%  |
| N merged rows                | 933,669 |
| N probe races                | 92,811  |

Coverage is excellent: essentially all NAR starters in 2018–2024 have bataiju recorded
and a matching (sex, age_years) cohort cell. Within-race variation is near-universal —
every race has multiple z-score values, so the feature is not a constant.

### Raw within-race Spearman ρ

| Feature      | Mean within-race ρ | N races |
| ------------ | ------------------ | ------- |
| bw_age_sex_z | **−0.0956**        | 92,811  |

Negative sign: heavier-than-cohort-norm → better finish (lower position number).
Raw ρ of −0.096 is comparable to the V1 seasonal z-score (−0.093), confirming
the growth-curve residual has a real body-weight signal at the raw level.

### Redundancy vs existing features

| Metric                | Value               |
| --------------------- | ------------------- |
| Max \|r\| vs existing | **0.930**           |
| Closest existing feat | bataiju_futan_ratio |

Signed correlations with the weight/age family:

| Existing feature              | Signed r |
| ----------------------------- | -------- |
| bataiju_futan_ratio           | −0.930   |
| bataiju_per_kyori_log         | +0.861   |
| weight_avg_5                  | +0.855   |
| futan_minus_bataiju_zscore    | −0.838   |
| bataiju_diff_from_race_mean   | +0.822   |
| bataiju_rank_in_race          | −0.695   |
| past_speed_kg_normalized_avg5 | +0.635   |
| weight_diff_from_avg          | +0.134   |
| speed_index_avg_5             | −0.135   |
| past_speed_age_adjusted_avg5  | −0.026   |
| barei_diff_from_race_mean     | −0.014   |

The feature is NOT flagged redundant by the |r| < 0.95 threshold (0.930 < 0.95),
but it is highly correlated with the existing BW family in 5 of 13 tested features.
The age-based normalization does NOT meaningfully differentiate from `bataiju_futan_ratio`
(|r|=0.930), `bataiju_per_kyori_log` (|r|=0.861), or `weight_avg_5` (|r|=0.855).

**Structural interpretation**: bataiju_futan_ratio = bataiju / futan_juryo encodes
relative weight per handicap. Since futan_juryo itself is a function of age/sex/grade
(higher for older heavier horses), the ratio already normalizes for age and sex
implicitly — making B2's explicit age×sex normalization largely redundant.

### Partial Spearman ρ (THE DECISION NUMBER)

After partialling out all 8 existing weight/age controls
(`weight_avg_5`, `weight_diff_from_avg`, `bataiju_futan_ratio`,
`bataiju_diff_from_race_mean`, `bataiju_rank_in_race`, `bataiju_per_kyori_log`,
`barei_diff_from_race_mean`, `past_speed_age_adjusted_avg5`):

| Feature      | Raw ρ   | Partial ρ   | Bar  | Pass?  |
| ------------ | ------- | ----------- | ---- | ------ |
| bw_age_sex_z | −0.0956 | **−0.0264** | 0.08 | **NO** |

The partial ρ drops from −0.096 (raw) to −0.026 after controlling for the existing
BW family — a 72% reduction. This confirms that the growth-curve residual does NOT
carry information orthogonal to the existing weight encoding: the signal in
bw_age_sex_z is almost entirely already captured by the combination of
`bataiju_futan_ratio` + `bataiju_per_kyori_log` + `weight_avg_5` +
`bataiju_diff_from_race_mean`.

### Per-venue Spearman (raw ρ, before partialling)

| keibajo | Venue  | Raw ρ  | N races |
| ------- | ------ | ------ | ------- |
| 30      | 門別   | −0.132 | 6,697   |
| 35      | 盛岡   | −0.093 | 5,155   |
| 36      | 水沢   | −0.084 | 4,776   |
| 42      | 浦和   | −0.068 | 4,658   |
| 43      | 船橋   | −0.120 | 4,884   |
| 44      | 大井   | −0.095 | 8,017   |
| 45      | 川崎   | −0.083 | 5,304   |
| 46      | 金沢   | −0.097 | 6,794   |
| 47      | 笠松   | −0.104 | 6,590   |
| 48      | 名古屋 | −0.096 | 9,198   |
| 50      | 園田   | −0.075 | 11,754  |
| 51      | 姫路   | −0.084 | 1,521   |
| 54      | 高知   | −0.110 | 8,610   |
| 55      | 佐賀   | −0.094 | 8,853   |

**Venue concentration**: top-2 venues (園田+大井) contribute 22.6% of race-weighted rho.
Not venue-concentrated (threshold 80%). Range −0.068 (浦和) to −0.132 (門別) —
moderate spread, but note this is the RAW ρ which mirrors the BW family already
in the model. The partial ρ across all venues would be near-zero.

## Historical bar context

| Signal                                 | Partial ρ                          | Outcome     |
| -------------------------------------- | ---------------------------------- | ----------- |
| V1 bataiju_seasonal_zscore (raw)       | ~−0.093 raw / not measured partial | REJECT (WF) |
| V3 age-month speed deviation           | +0.055                             | ABORT       |
| V5 race-volume density                 | +0.059                             | ABORT       |
| V6 sire distance split                 | +0.025                             | ABORT       |
| V7 joint-weak-orthogonal               | +0.005                             | ABORT       |
| **B2 bw_age_sex_z (this probe)**       | **−0.026**                         | **ABORT**   |
| V8 H-BABA-PAR-TIME (baba_adj_centered) | +0.180                             | PROCEED     |

B2's partial ρ of −0.026 is well below the 0.08 bar and is the lowest of all
BW-family probes, confirming that adding more age-normalization dimensions to
body weight does NOT unlock new predictive signal given the existing representation.

## Verdict

**ABORT** — growth-curve BW z-score is absorbed by the existing weight family.

**Binding reason**: partial Spearman ρ = −0.0264, below the bar of 0.08.
The feature loses 72% of its raw signal (−0.096 → −0.026) when the 8-feature
existing weight/age family is controlled for. The coarser (age_years × sex) grid
of B2 does not improve on V1's (sex × age_bucket × month) approach — both are
highly correlated with `bataiju_futan_ratio` (~0.93) and `bataiju_per_kyori_log`
(~0.86), which together already encode the age-sex-size relationship via the
handicap load.

**Coverage check (PASS)**: 99.95% — not a coverage issue.

**Venue concentration (PASS)**: top-2 = 22.6% — not concentrated.

**Failure mode**: The existing feature `bataiju_futan_ratio` = bataiju / futan_juryo
implicitly normalizes for age and sex because futan_juryo (handicap weight, kg) is
itself a function of age, sex, and career class level. B2's explicit cohort
normalization therefore approximates what futan_juryo already achieves, producing
near-zero additional information once the ratio is in the model.

**What would be needed for a BW residual to exceed the bar**:
The only path not already tried is an individual-horse growth curve (first N starts
as prior) that disentangles developmental trajectory from population norm — per
future-direction 1 of the V1 REJECT doc. This requires per-horse early-career
BW history, not available as a pre-computed static feature.

## Hard rules observed

- `tmp/` only: all artifacts in `tmp/nar-perclass/sci_track/b2_bwzscore/`
- No `git add tmp/`: tmp files not staged
- PG read-only: only SELECT queries issued via DuckDB postgres extension
- Cohort training window strictly 2007–2017; probe window 2018–2024 (no leak)
- No training invoked (probe phase only)
- No production change
