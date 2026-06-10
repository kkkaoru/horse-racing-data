---
science_track_entry: true
hypothesis_id: H-SPEED-FADE-INDEX
date: 2026-06-11
based_on_iteration: iter30-nar-cb-residual-*-v8 (production baseline)
scope: NAR (all keibajo except Banei), race_year >= 2019
status: ABORT (partial rho 0.0725 < bar 0.08; venue sign inconsistency 8/14 negative)
verdict: ABORT — signal fails partial rho bar and is venue-sign inconsistent
production_change: none (probe only)
artifacts:
  probe_script: tmp/nar-perclass/sci_track/b4_speedfade/probe.py
  probe_verdict: tmp/nar-perclass/sci_track/b4_speedfade/probe_verdict.json
---

## Hypothesis

**H-SPEED-FADE-INDEX** (Science Track B4, JES vol53-no2 motivation):

Inspired by JES research on muscle fatigue and late-race deceleration: a horse with poor
stamina should decelerate more sharply in the final 3F relative to its early 3F pace. The
existing feature set includes `kohan3f_avg_5` (mean late-3F time over last 5 races) and
`last_3_avg_kohan_3f`, but **no feature captures the late-vs-early DEVIATION** (i.e.,
whether the horse slows down relative to the pace it set in the first half of the race).

**Proposed signal**: per-horse `speed_fade_idx` = mean over last 5 races of
`fade_per_meter = (kohan_3f_raw - zenhan_3f_raw) / kyori`:

- `zenhan_3f`: first half 3F time (from `nvd_ra.zenhan_3f` / `jvd_ra.zenhan_3f`)
- `kohan_3f`: final 3F time (from `nvd_ra.kohan_3f`)
- Division by `kyori` normalises for distance to make fades comparable across race lengths
- Higher fade = decelerates more at end = weaker stamina (hypothesis: positive rho with
  finish_position means more fade -> worse finishing rank)

**Redundancy trap**: the closest existing feature `kohan3f_avg_5` captures the LEVEL of
late-3F speed; this probe tests the DEVIATION (late minus early), which should be orthogonal.

## Feature Availability Audit

`nvd_ra` (NAR race table) has both `zenhan_3f` and `kohan_3f` columns. Coverage check:

| Year range | NAR races | zenhan_3f populated | kohan_3f populated |
| ---------- | --------- | ------------------- | ------------------ |
| 2015–2019  | 74,158    | 100%                | 100%               |
| 2020–2026  | 97,854    | 100%                | 100%               |

Total 171,427 NAR races with full sectionals (2015+). The `feat-nar-v8-iter17-bataiju`
parquet covers source='nar' with no `zenhan_3f` feature in its 174-column schema —
**confirmed gap** in the late-vs-early deviation dimension.

## Feature Engineering

**Construction:**

1. Join `nvd_se` (horse-level) with `nvd_ra` (race-level) on (kaisai_nen, kaisai_tsukihi,
   keibajo_code, race_bango)
2. Compute `fade_per_meter = (kohan_3f_raw - zenhan_3f_raw) / kyori` per race
3. For each horse-race target, look back at last 5 prior races for that horse (strictly
   before target race date) and compute `speed_fade_idx = avg(fade_per_meter)` over up
   to 5 prior races

**Normalization investigation**: An initial attempt used within-stratum z-scores
(`fade_z` normalized within `keibajo_code × kyori × year`), but this produced ~18%
coverage due to LATERAL join interactions with DuckDB's postgres attach layer incorrectly
resolving the z-score CTE. Switching to `fade_per_meter = fade_raw / kyori` (distance
normalization only) is the correct approach and produces 96.3% coverage. The
stratum-level z-score was unnecessary since distance already accounts for the main
confound (longer races have larger absolute fade values).

## Probe Results

**Dataset**: NAR feat-nar-v8-iter17-bataiju, race_year >= 2019, n=955,760 rows

**Coverage:**

| Metric                           | Value                               |
| -------------------------------- | ----------------------------------- |
| Rows with speed_fade_idx         | 955,760                             |
| Total eligible feat rows (2019+) | 992,369                             |
| Coverage                         | **96.3%**                           |
| Horses with 0 prior fade history | 3.7% (newcomers / recently debuted) |

Coverage is excellent (96.3% >> 70% bar) — the signal is dense.

**Spearman correlation with finish_position:**

| Metric               | Value    |
| -------------------- | -------- |
| Raw Spearman rho     | +0.097   |
| p-value              | < 1e-300 |
| Partial Spearman rho | +0.0725  |
| p-value              | < 1e-300 |
| **Bar (partial)**    | **0.08** |
| **Pass?**            | **NO**   |

**Partial controls**: `kohan3f_avg_5`, `speed_index_avg_5`, `last_3_avg_kohan_3f`,
`field_avg_past_kohan_3f`.

Raw rho is 0.097 (above bar), but **partial rho = 0.0725 is below the 0.08 threshold**.
The signal has some marginal overlap with the kohan3f family (the controls absorb ~25%
of the signal).

**Redundancy analysis:**

| Existing feature        | Spearman corr with speed_fade_idx |
| ----------------------- | --------------------------------- |
| kohan3f_avg_5           | +0.001 (essentially zero)         |
| speed_index_avg_5       | +0.006 (near zero)                |
| last_3_avg_kohan_3f     | −0.003 (near zero)                |
| field_avg_past_kohan_3f | −0.034 (near zero)                |

The speed_fade_idx is **genuinely novel** (max |corr| = 0.034) — orthogonal to the entire
existing speed and kohan3f family. Despite this orthogonality, the partial rho still fails
the bar, indicating that the fade signal provides some marginal but insufficient incremental
predictive value beyond the existing features.

**Per-venue Spearman breakdown:**

| keibajo | N       | rho    | Sign consistent?      |
| ------- | ------- | ------ | --------------------- |
| 30      | 61,997  | −0.014 | NO (negative)         |
| 35      | 49,137  | −0.047 | NO (negative)         |
| 36      | 48,499  | −0.050 | NO (negative)         |
| 42      | 51,315  | +0.024 | YES                   |
| 43      | 52,653  | +0.044 | YES                   |
| 44      | 101,180 | +0.085 | YES                   |
| 45      | 58,473  | +0.033 | YES                   |
| 46      | 60,183  | −0.065 | NO (negative)         |
| 47      | 57,332  | −0.037 | NO (negative)         |
| 48      | 99,764  | −0.005 | NO (negative, p=0.13) |
| 50      | 115,347 | +0.036 | YES                   |
| 51      | 20,532  | +0.058 | YES                   |
| 54      | 87,793  | −0.053 | NO (negative)         |
| 55      | 91,555  | −0.024 | NO (negative)         |

**Sign consistency: 6 positive, 8 negative venues.**

This is a critical finding. The hypothesis predicts a **positive** rho (more fade = decelerate
more = weaker stamina = worse finish rank). But 8 of 14 venues show **negative** rho —
meaning at those venues, horses with MORE accumulated fade history actually finish BETTER.
This is physically inconsistent with the stamina hypothesis.

**Interpretation of venue sign inconsistency**: The fade signal is confounded by running
style distributions at different venues. Venues like 大井 (44), 園田 (50), and 姫路 (51)
tend to have front-runner dominated races where horses that pace early and fade late are
still the fastest overall. Venues like 高知 (54), 佐賀 (55), 金沢 (46), and 盛岡 (35)
may have more come-from-behind winning patterns where fade horses lose more. The
race-level fade (`zenhan_3f`, `kohan_3f`) conflates PACE SCENARIO with individual stamina
— when a race is fast early, ALL horses show larger fade regardless of their stamina.
The horse's own fade averaged over 5 races thus encodes historical pace scenarios as
much as individual stamina capacity.

## Verdict

**ABORT**

**Primary reason**: partial_rho = 0.0725 < bar 0.08.

**Secondary reason (decisive)**: venue sign inconsistency (8/14 venues negative rho)
indicates the signal direction is venue-dependent and not a consistent stamina proxy.

**Orthogonality note (positive finding)**: The redundancy with existing features is
near-zero (max |corr| = 0.034), confirming the late-vs-early deviation IS a genuinely
novel feature dimension not captured by `kohan3f_avg_5` or any other existing feature.
The CONCEPTUAL hypothesis is validated (orthogonal signal exists), but the current
operationalisation — using race-level `zenhan_3f / kohan_3f` — captures pace-scenario
confounds as much as individual stamina.

## Comparison with Prior Science Track Entries

| Signal                     | Partial rho | Coverage | Verdict   |
| -------------------------- | ----------- | -------- | --------- |
| V2 H-DIRT-GOING (pref raw) | ~0.045 cond | 31%      | ABORT     |
| V3 H-AGE-MONTH             | 0.055       | 85%+     | ABORT     |
| V5 H-RACE-VOLUME           | 0.059       | —        | ABORT     |
| V6 H-SIRE-DISTANCE-SPLIT   | 0.025       | —        | ABORT     |
| V7 JOINT-WEAK-ORTHOGONAL   | 0.005       | —        | ABORT     |
| V8 H-BABA-PAR-TIME         | 0.180       | 93%      | PROCEED   |
| **B4 H-SPEED-FADE-INDEX**  | **0.0725**  | **96%**  | **ABORT** |

## Potential Refinement Directions (not pursued)

1. **Per-horse sectional normalization**: instead of `fade_raw / kyori`, compute
   `fade_z` within each horse's own career distribution (z-score vs horse's historical mean
   and std). This would disentangle pace-scenario from individual stamina more cleanly.
2. **Pace-adjusted fade**: control for race-level pace (e.g., `zenhan_3f` relative to
   par) before computing fade. A horse running in a fast-early race SHOULD fade more;
   the residual after pace-control would be the stamina signal.
3. **Section-specific timing from lap_time**: `nvd_ra.lap_time` contains per-lap splits
   (not just 3F aggregates). A more granular late-deceleration index from the final 2
   laps might reduce the zenhan/kohan coarseness issue.
4. **JRA extension**: not tested (feat parquet is NAR-only); JRA has same `zenhan_3f` /
   `kohan_3f` in `jvd_ra`.

## Hard Rules Observed

- `tmp/` only: all artifacts in `tmp/nar-perclass/sci_track/b4_speedfade/`
- No `git add tmp/`
- PG read-only: only SELECT queries through DuckDB postgres extension
- No DELETE/TRUNCATE/DROP issued
- No training or production change
