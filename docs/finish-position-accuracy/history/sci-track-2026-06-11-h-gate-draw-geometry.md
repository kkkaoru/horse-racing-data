---
science_track_entry: true
hypothesis_id: H-GATE-DRAW-GEOMETRY
date: 2026-06-11
based_on_iteration: feat-v20-merged (JRA + NAR, 2010–2026)
scope: NAR (all keibajo except Banei) + JRA (central venues 01–10)
status: ABORT (best holdout partial rho 0.038 << bar 0.08; no stratum clears bar)
verdict: ABORT — geometry-conditioned gate bias fails partial rho bar in every stratum and window
production_change: none (probe only)
artifacts:
  probe_script: tmp/gate-draw/probe.py
  probe_verdict: tmp/gate-draw/probe_verdict.json
---

## Hypothesis

**H-GATE-DRAW-GEOMETRY** (Science Track catalog #8: 枠×短距離×コーナー幾何):

Inside gate positions (low `wakuban`) are hypothesized to be favored in short-distance dirt
sprints because: (a) less ground is lost on the rail before the first corner, (b) fewer horses
need to be passed. The **NOVEL angle** is that gate bias varies by race geometry — inside
advantage is stronger in tight-turn short sprints vs. long sweeping routes.

**Proposed signals:**

1. `wakuban_norm_8`: raw gate number normalized to [0..1] over the 8-waku range, independent
   of field size (inside=0, outside=1). Orthogonal to the existing `umaban_norm` which
   normalizes the horse number within the field.
2. `gate_geom_score`: historical average finish_norm for the same `wakuban_bucket`
   (inside 1–3 / mid 4–5 / outside 6–8) within the same
   `(source, keibajo_code, kyori_band, n_corners_key)` stratum, computed **leak-free** from
   all races before the target race date.

**Existing features partialled out:**

- `umaban_norm` — linear horse-number normalization within field
- `track_bias_inside` — rolling 5-day venue inside-win rate
- `kyori` (continuous) — distance
- `keibajo_code` (venue identity, hash-encoded)

## Feature Availability Audit

`wakuban` exists in both `nvd_se` (NAR) and `jvd_se` (JRA) as a VARCHAR column.

| Category | Source rows (2010–2026) | wakuban populated | Coverage |
| -------- | ----------------------- | ----------------- | -------- |
| NAR      | 2,152,314               | 2,152,314         | ~100%    |
| JRA      | 785,344                 | 785,344           | ~100%    |

Join to existing feature parquets (feat-v20-merged):

| Match result                  | Count                 | Coverage |
| ----------------------------- | --------------------- | -------- |
| feat rows with wakuban joined | 2,884,244 / 3,329,617 | 86.6%    |

The 13.4% miss is due to Ban-ei rows in feat-v20-merged (not probed here) and minor
race-date range edge cases.

`n_corners` (course_corner_count) comes from the static `course-numerical-features-lookup.parquet`
(119 rows, JRA-only). NAR n_corners is unavailable in the lookup; the probe uses `'unk'` as a
fallback key for NAR, effectively bucketing all NAR geometry together within kyori_band.

## Feature Engineering

**wakuban_norm_8**: `(wakuban - 1) / 7.0` → 0 for gate 1 (inside), 1 for gate 8 (outside).

**gate_geom_score** (leak-free historical avg finish_norm):

1. For each horse-race target, group historical races (before target date) by
   `(source, keibajo_code, kyori_band, n_corners_key, waku_bucket)`.
2. Compute a rolling monthly-accumulated average finish_norm per bucket using a DuckDB
   window `rows between unbounded preceding and 1 preceding`, ordered by yyyymm.
3. Join back to target rows: the score for a horse in gate bucket B at venue K with
   distance bucket D is the accumulated avg finish_norm of all prior races at K/D/B.
4. Coverage of gate_geom_score: **99.2% full-period, 100% holdout** (sufficient history
   after ~2012 warm-up, which is prior to the earliest target dates analyzed).

Median historical n (gate_geom_history_n) across strata: 9,700–35,000 — dense and robust.

## Probe Results

### Coverage summary

| Metric                                                                         | Value     |
| ------------------------------------------------------------------------------ | --------- |
| Analysis rows (finish_norm non-null + wakuban non-null + umaban_norm non-null) | 2,898,454 |
| NAR rows                                                                       | 2,109,614 |
| JRA rows                                                                       | 788,840   |
| gate_geom_score coverage (full)                                                | 99.2%     |
| gate_geom_score coverage (holdout 2023+)                                       | 100.0%    |

### Main results table (partial rho vs finish_norm, controlling for umaban_norm + track_bias_inside + kyori + venue + wakuban_norm_8)

| Stratum                    | N         | Raw rho wakuban | Raw rho geom | Partial rho wakuban | **Partial rho geom (ctrl waku)** | Bar  | Pass? |
| -------------------------- | --------- | --------------- | ------------ | ------------------- | -------------------------------- | ---- | ----- |
| All — full                 | 2,898,068 | −0.0015         | 0.032        | 0.0026              | **0.031**                        | 0.08 | NO    |
| All — holdout 2023+        | 556,767   | −0.0174         | 0.029        | 0.0013              | **0.027**                        | 0.08 | NO    |
| NAR — full                 | 2,109,614 | −0.0086         | 0.033        | 0.0012              | **0.030**                        | 0.08 | NO    |
| NAR — holdout              | 412,815   | −0.0260         | 0.029        | −0.0001             | **0.022**                        | 0.08 | NO    |
| JRA — full                 | 788,840   | +0.0180         | 0.031        | 0.0027              | **0.023**                        | 0.08 | NO    |
| JRA — holdout              | 144,338   | +0.0076         | 0.030        | 0.0027              | **0.027**                        | 0.08 | NO    |
| NAR sprint (≤1300m) — full | 610,437   | −0.0239         | 0.039        | 0.0005              | **0.032**                        | 0.08 | NO    |
| NAR sprint — holdout       | 126,681   | −0.0355         | 0.038        | 0.0070              | **0.025**                        | 0.08 | NO    |
| JRA sprint — full          | 200,069   | +0.0065         | 0.035        | 0.0046              | **0.032**                        | 0.08 | NO    |
| JRA sprint — holdout       | 34,219    | −0.0036         | 0.038        | 0.0049              | **0.038**                        | 0.08 | NO    |
| NAR longer (>1300m) — full | 1,499,177 | −0.0024         | 0.029        | 0.0006              | **0.028**                        | 0.08 | NO    |
| NAR longer — holdout       | 286,134   | −0.0218         | 0.025        | −0.0031             | **0.020**                        | 0.08 | NO    |

**Deciding number: best holdout partial rho = 0.038 (JRA sprint, n=34,219) vs bar 0.08 — 47% of bar.**

### Per-venue breakdown — NAR sprint top-5 venues

| Venue      | N (full) | N (holdout) | Partial rho geom (full) | Partial rho geom (holdout) | Bar  | Pass? |
| ---------- | -------- | ----------- | ----------------------- | -------------------------- | ---- | ----- |
| 30 (大井)  | 106,637  | 23,854      | 0.023                   | 0.013                      | 0.08 | NO    |
| 44 (大井?) | 95,956   | 17,068      | −0.007                  | −0.014                     | 0.08 | NO    |
| 54 (高知)  | 95,608   | 12,771      | 0.002                   | −0.027                     | 0.08 | NO    |
| 55 (佐賀)  | 62,662   | 13,533      | −0.006                  | −0.021                     | 0.08 | NO    |
| 43 (金沢?) | 53,377   | 12,505      | 0.029                   | −0.021                     | 0.08 | NO    |

No single venue clears the 0.08 bar. Multiple venues show sign-flipping between full and
holdout periods, indicating the geometry-conditioned score is not a stable predictor.

### raw wakuban_norm_8 partial rho

The raw gate position feature (`wakuban_norm_8`) shows near-zero partial rho in all strata:

- Best partial rho: 0.007 (NAR sprint holdout) — **far below 0.08 bar**
- Most strata: partial rho < 0.003

This confirms that `umaban_norm` (the existing linear field-size-normalized gate proxy)
already absorbs all the information in the raw gate position. `wakuban_norm_8` adds nothing
beyond `umaban_norm`.

### Redundancy analysis

The closest existing feature to the NOVEL geometry-conditioned angle is `track_bias_inside`
(rolling 5-day venue inside-win rate). The `gate_geom_score` is designed to be more specific
(conditioned on kyori_band and n_corners, not just venue), but after controlling for
`track_bias_inside` the incremental partial rho drops from ~0.03 raw to ~0.02–0.03 partial.
The geometry conditioning does not meaningfully reduce redundancy with `track_bias_inside`.

## Interpretation

Three explanations for the null result:

1. **Direction inconsistency across venues**: Inside-gate advantage exists at some NAR venues
   but reverses at others (e.g., venue 44 and 54 show negative partial rho in holdout). The
   effect is not directionally stable across the NAR circuit.

2. **umaban_norm already captures the bias**: The existing `umaban_norm` (horse number / field)
   is highly correlated with `wakuban_norm_8` after field size is accounted for. The marginal
   information in the gate-specific grouping is near zero.

3. **track_bias_inside already captures venue-level inside-rail favoritism**: The 5-day rolling
   venue inside-win rate captures the same information as the long-run historical inside-gate
   performance rate. Adding geometry bucketing (n_corners × kyori_band) does not reveal
   dimensions not already encoded by venue + distance + the existing inside-win rate.

## Verdict

**ABORT**

**Primary reason**: Best holdout partial rho = **0.038** (JRA sprint, n=34k) vs bar 0.08 —
less than half the bar. No stratum, no window, no venue passes.

**Secondary reasons**:

- Raw gate position (wakuban_norm_8) partial rho < 0.003 in all strata — fully absorbed by
  existing `umaban_norm`.
- Per-venue sign inconsistency in holdout (4/5 top NAR-sprint venues show negative or near-zero
  partial rho in holdout while showing positive full-period values) — full-period signal is
  inflated by historical period patterns that do not hold in the recent window.
- The geometry conditioning (n_corners × kyori_band) does not add orthogonal signal beyond
  `track_bias_inside` (venue-level inside-rail bias already in the model).

## Comparison with Prior Science Track Entries

| Signal                      | Partial rho (holdout) | Coverage | Verdict   |
| --------------------------- | --------------------- | -------- | --------- |
| V2 H-DIRT-GOING             | ~0.045 (conditional)  | 31%      | ABORT     |
| V3 H-AGE-MONTH              | 0.055                 | 85%+     | ABORT     |
| V5 H-RACE-VOLUME            | 0.059                 | —        | ABORT     |
| V6 H-SIRE-DISTANCE-SPLIT    | 0.025                 | —        | ABORT     |
| V7 JOINT-WEAK-ORTHOGONAL    | 0.005                 | —        | ABORT     |
| V8 H-BABA-PAR-TIME          | 0.180                 | 93%      | PROCEED   |
| B4 H-SPEED-FADE-INDEX       | 0.0725                | 96%      | ABORT     |
| **#8 H-GATE-DRAW-GEOMETRY** | **0.038 (best)**      | **99%**  | **ABORT** |

## Hard Rules Observed

- `tmp/` only: all artifacts in `tmp/gate-draw/`
- No `git add tmp/`
- PG read-only: only SELECT queries through DuckDB postgres extension
- No DELETE/TRUNCATE/DROP issued
- No training or production change
- No push
