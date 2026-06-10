---
science_track_entry: true
hypothesis_ids: [H-RECENT-RACE-VOLUME, H-GASTRIC-RACE-DENSITY]
date: 2026-06-10
based_on_iteration: 30 (iter30-nar-cb-residual-*-v8 + iter12-nar-xgb-hpo-v8)
scope: NAR (all keibajo except Banei), per-class residual ensemble
status: ABORT (probe run; mechanical PROCEED overridden by honest assessment)
verdict: ABORT
production_change: none
artifacts:
  feature_builder: tmp/nar-perclass/sci_track/v5_volume/build_features.py
  vol_parquet: tmp/nar-perclass/sci_track/v5_volume/vol-parquet/race_year={YYYY}/data_0.parquet
  probe_script: tmp/nar-perclass/sci_track/v5_volume/probe.py
  probe_verdict: tmp/nar-perclass/sci_track/v5_volume/probe_verdict.json
  verdict_json: tmp/nar-perclass/sci_track/v5_volume/verdict.json
  gap_analysis: tmp/nar-perclass/sci_track/gap_analysis.json (rank 4, rank 6)
v3_warning_confirmed: true
---

## Hypotheses

Combined probe of the **schedule-data family** (Rank 4 + Rank 6 from gap_analysis.json):

### H-RECENT-RACE-VOLUME (Rank 4, ★ 30_1909)

**30_1909 (JES A ★, NAR case-control)** documents that cumulative race distance
over the prior 3 starts < 4000m (OR=1.40) and prior 6 starts < 8000m (OR=1.47)
predicts SDF tendinopathy risk. The reverse reading: horses with adequate recent
cumulative load have better-conditioned tendons and sustain pace in the final
furlongs. **vol53-no2 p.86** adds the quality-over-quantity distinction at top
stables (high 襲歩 ratio vs raw fast-gallop volume).

The claimed mechanism: under-conditioned horses (low cumulative distance) tire in
the finishing stretch, slipping from 2nd/3rd to 4th+ — directly targeting
place2/place3. This is **distinct** from `days_since_last_race` (time gap only)
and `consecutive_race_count` (count only): a horse with 3 races at 1000m has
very different conditioning than 3 races at 1800m.

**Proposed features**: `cum_kyori_last3`, `cum_kyori_last6`,
`cum_kyori_x_days_since` (load×freshness interaction).

### H-GASTRIC/RACE-DENSITY (Rank 6, ★ vol56-no4 p.372)

**vol56-no4 p.372** documents glandular gastric disease (EGGD) prevalence of 25%
in NAR/JRA racehorses, with weekly exercise ≥5 days OR=10.4x and low-performance
horses EGGD=3.7x. Racing density (races per 30/60/90-day window) proxies for
the intensive schedule that drives gastric stress. **vol54-no1 p.17** confirms
gastric symptoms at training/race peak → appetite loss → performance degradation.

The claimed mechanism: ≥3 races in 30 days = high-density = elevated EGGD risk =
performance decline manifesting as place2/3 slippage. This is **distinct** from
`days_since_last_race` (most-recent gap only) and `consecutive_race_count`
(streak count, not density).

**Proposed features**: `races_in_30d`, `races_in_60d`, `races_in_90d`,
`races_30d_x_class` (density × class level).

## Citations

| Citation                     | Relevance                                                                               |
| ---------------------------- | --------------------------------------------------------------------------------------- |
| 30_1909 (JES A ★)            | NAR SDF tendinopathy: cum dist last 3 <4000m OR=1.40, last 6 <8000m OR=1.47             |
| vol53-no2 p.86 (馬の科学 A)  | Fast-gallop distance predicts accident risk; quality (襲歩 ratio) > quantity            |
| vol54-no4 p.341 (馬の科学 C) | Detraining: aerobic capacity decay by rest modality; recent racing = conditioning proxy |
| 20_3_33 (JES C)              | Glycogen recovery ~24h; days-since × race distance → glycogen state at next race        |
| vol56-no4 p.372 (馬の科学 A) | EGGD 25%/ESGD 72%; weekly exercise ≥5d OR=10.4x; low-performance horses EGGD=3.7x       |
| vol54-no1 p.17 (馬の科学 A)  | Gastric symptoms at training/race peak → appetite loss → poor condition → performance   |
| vol52-no2 p.122 (馬の科学 A) | Carpal fracture × training: ≥6 fast works/30d OR=0.515; start count OR=0.430            |

## Feature Definitions (Leak-Free)

All aggregates use `LAG(n)` windows over `ORDER BY race_id` (NAR race_id encodes
date chronologically: `nar:YYYY:MMDD:venue:race#`). No current-race outcome is used.

| Feature                  | Definition                                                                      | Coverage |
| ------------------------ | ------------------------------------------------------------------------------- | -------- |
| `cum_kyori_last3`        | `kyori_lag1 + kyori_lag2 + kyori_lag3` (3 most recent prior races)              | 96.3%    |
| `cum_kyori_last6`        | Sum of kyori for up to 6 most recent prior races (COALESCE 0 for missing lags)  | 96.3%    |
| `cum_kyori_x_days_since` | `cum_kyori_last3 / (date_lag1 to race_date days + 1)` (load × freshness)        | 96.3%    |
| `races_in_30d`           | Count of prior races with `race_date` within 30 days before current `race_date` | 100%     |
| `races_in_60d`           | Same, 60-day window                                                             | 100%     |
| `races_in_90d`           | Same, 90-day window                                                             | 100%     |
| `races_30d_x_class`      | `races_in_30d × COALESCE(grade_code, 0)` — stress × class-level interaction     | 100%     |

**Partial-out regressors** (existing schedule features): `days_since_last_race`,
`days_since_last_race_log`, `consecutive_race_count`, `finish_trend_5`,
`past_speed_age_adjusted_avg5`.

## Probe Setup

- **Years**: 2018–2024 (NAR, no Banei)
- **Merged rows**: 933,669
- **Long-layoff slice** (days_since_last_race ≥ 60): 57,082 rows (6.1%), 2,128–2,144 races
- **High-density slice** (races_in_30d ≥ 3): 34,319 rows (3.7%), 1,250–3,575 races
- **Existing numeric features scanned**: 174
- **Seed**: 42

**Abort bar**: partial rho after regressing on schedule features < ~0.06 in best slice → ABORT.
Calibrated from: V2 aborted at 0.045 conditional, V3 age features redundant (0.84),
H2 best=0.142 raw → REJECT.

## Probe Results

| Feature                | Coverage | Raw rho (all) | Partial rho | Density slice rho | Layoff slice rho | Redundancy (max Pearson) | Closest existing         | Status                                      |
| ---------------------- | -------- | ------------- | ----------- | ----------------- | ---------------- | ------------------------ | ------------------------ | ------------------------------------------- |
| cum_kyori_last3        | 96.3%    | +0.006        | **−0.080**  | +0.012            | +0.002           | 0.465 vs kyori           | kyori (+0.47)            | DISQUALIFIED — sign inversion artifact      |
| cum_kyori_last6        | 96.3%    | +0.054        | **−0.029**  | +0.015            | +0.030           | 0.486 vs futan_per_barei | futan_per_barei (−0.49)  | FAIL — partial inverted, below bar          |
| cum_kyori_x_days_since | 96.3%    | +0.057        | **−0.055**  | +0.020            | −0.011           | 0.813 vs d.s.l.r.\_log   | days_since_last_race_log | FAIL — high redundancy 0.81, sign inverted  |
| races_in_30d           | 100%     | +0.102        | **+0.099**  | null (n=0)        | +0.092           | **0.985** vs consec.cnt  | consecutive_race_count   | DISQUALIFIED — redundant 0.985 > threshold  |
| races_in_60d           | 100%     | +0.114        | **+0.051**  | +0.114            | −0.001           | 0.714 vs d.s.l.r.\_log   | days_since_last_race_log | FAIL — partial 0.051 < 0.06 bar             |
| **races_in_90d**       | **100%** | **+0.127**    | **+0.059**  | **+0.133**        | −0.018           | 0.641 vs d.s.l.r.\_log   | days_since_last_race_log | MARGINAL MISS — partial 0.059 vs 0.06 bar   |
| races_30d_x_class      | 100%     | null          | null        | null              | null             | 0.000 (degenerate)       | —                        | DEGENERATE — grade_code=0 for all NAR races |

**Abbreviations**: d.s.l.r.\_log = days_since_last_race_log, consec.cnt = consecutive_race_count.

### Key redundancy finding

`races_in_30d` vs `consecutive_race_count`: Pearson = **0.9848** (> 0.95 threshold).

This is not surprising in retrospect: at NAR, the median inter-race interval is
~14 days. A horse with 3 consecutive races will almost always have all 3 within
30 days; a horse with consecutive_race_count=2 will almost always have 2 in 30 days.
The 30-day window is a near-perfect proxy for the consecutive race count already
in the 174 features. The 60-day and 90-day windows are more orthogonal (0.71, 0.60
respectively) but their partial rho drops below the abort bar.

### Sign-inversion analysis (cum_kyori features)

`cum_kyori_last3` raw rho = +0.006 (near zero). After OLS regression on schedule
features (particularly `finish_trend_5` correlation −0.08 and `past_speed` −0.30),
the residual flips to −0.080. This is a textbook confounder inversion: horses with
more cumulative recent distance tend to be better-quality horses with higher
`past_speed_age_adjusted_avg5`, so after controlling for past speed, residual
`cum_kyori_last3` is negatively associated with finish position (better horses run
more distance). The raw signal is null; the partial signal is an artifact.

### Best genuine candidate: races_in_90d

`races_in_90d` has the most defensible signal:

- Raw within-race Spearman = 0.127 (89k races)
- Partial rho after schedule partialling = **0.059** (vs 0.06 abort bar)
- High-density slice partial is consistent at 0.133 (1,250 races)
- Redundancy = 0.641 vs days_since_last_race_log (below the 0.95 threshold — genuinely orthogonal)
- Correct sign: more races in 90 days → higher (worse) finish position → heavy schedule = worse performance

The partial rho misses the 0.06 bar by **0.001**. Given:

- 11 consecutive rejections at the strengthened gate
- H2's best feature had raw=0.142 and was UNANIMOUSLY rejected
- The prior probability of a retrain converting 0.059 partial rho into LB95>0 improvement is <5%
- The mechanism (busy schedule → gastric stress → finishing-stretch fade) is real but manifests as a systematic cross-sectional effect rather than within-race rank discrimination

...the 0.001 margin does not justify a retrain slot.

## Verdict

**ABORT**

**Binding reason:** The probe script mechanically returned PROCEED_RECOMMENDED based
on `cum_kyori_last3` partial rho = −0.080 absolute passing the 0.06 bar. This is
**overridden by honest assessment**: the signal is a sign-inversion OLS artifact
(raw rho = 0.006 ≈ 0), not a genuine directional predictor. After correcting:

- `races_in_30d` (the strongest density signal, partial rho=0.099): **fully redundant**
  with `consecutive_race_count` (Pearson=0.985 > 0.95 threshold). The 30-day window
  adds no information beyond the existing feature.
- `races_in_90d` (the best genuinely orthogonal signal): partial rho = **0.059**,
  which misses the 0.06 abort bar by 0.001. Given the 11-consecutive-rejection prior
  and the high cost of a WF retrain (8–12h per class × 4 classes), the expected
  return does not justify the cost.
- `cum_kyori_last3/6`, `cum_kyori_x_days_since`: sign-inverted or highly redundant
  with days_since_last_race_log (0.81).
- `races_30d_x_class`: degenerate feature (grade_code=0 across all NAR races —
  NAR grade encoding does not use the expected numeric values from JRA).

**The V3 warning was correct**: these features are in the schedule-data family and
their partial-redundancy with existing features is high. The science mechanism is
real (gastric stress does degrade performance; under-conditioning does increase
tendinopathy) but the incremental signal after controlling for existing schedule
features is too weak to discriminate finish ordering within a race.

## Future Research Directions

1. **Gap-stratified cumulative load**: split `cum_kyori_last3` by inter-race gap
   category (<21d vs 21–60d vs >60d). The mechanism concentrates at short gaps
   (quick turn-around with high cumulative load = highest conditioning stress). The
   overall feature averages over too many gap profiles.

2. **Training-load proxy via weight change**: `bataiju_diff_from_avg × consecutive_race_count`
   combines the physiological state (weight loss under racing load) with schedule
   density. This is closer to the 30_1909 OR=1.59 for ≥5kg weight drop than the
   raw schedule density.

3. **Selective window by keibajo**: NAR venues differ in inter-race interval norms.
   Oi runs nightly with ~7-day gaps; rural NAR venues have ~14-day gaps. A
   venue-normalized density feature might extract the signal hidden by venue-level
   confounding.

4. **New horse-level signal required**: per the v7-lineage saturation analysis
   (project_v7_lineage_saturation_2026_06_04.md), breaking the current accuracy
   ceiling requires new horse-level signals + full retrain, not incremental feature
   additions to the per-class residual layer.

## Hard Rules Observed

- `tmp/` only: all artifacts written to `tmp/nar-perclass/sci_track/v5_volume/`
- No `git add tmp/`: vol-parquet not staged
- PG read-only: only SELECT queries issued (DuckDB postgres attach READ_ONLY)
- seed=42: enforced in probe script (`np.random.default_rng(42)`)
- No retrain started: probe-only as specified
- Honest override: probe script verdict overridden by manual analysis when the
  mechanical PROCEED was based on an artifact
