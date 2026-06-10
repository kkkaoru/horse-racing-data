---
science_track_entry: true
hypothesis_id: H-PREV-BW-DROP (B1)
date: 2026-06-11
based_on_iteration: iter30-nar-cb-residual-*-v8 (production baseline)
scope: NAR (all keibajo except Banei/81/82/84) + JRA
status: ABORT (partial rho 0.027 NAR / 0.016 JRA << bar 0.08; raw signal near-zero despite strong coverage)
verdict: ABORT — prev-race BW drop does not carry rankable finish-position signal beyond existing weight family
production_change: none (probe only)
artifacts:
  probe_script: tmp/nar-perclass/sci_track/b1_bwdrop/probe_b1.py
  probe_verdict: tmp/nar-perclass/sci_track/b1_bwdrop/probe_verdict.json
---

## Hypothesis

**H-PREV-BW-DROP (B1)** — Source: JES journal G1 / 30_1909 (SDFT):

An acute body-weight drop versus the IMMEDIATELY PRIOR race is a performance-degradation
or latent-injury signal. The journal documents superficial digital flexor tendon (SDFT)
injury OR 1.4–2.3 for horses with acute weight drops. Define:

```
bw_drop_prev = bataiju(current race) − bataiju(previous race for the same horse)
```

(signed; negative = weight loss since previous start)

This is cataloged as 100% dense and is **distinct** from the existing `weight_diff_from_avg`
feature (which computes `current_bataiju − avg(bataiju over last 5 races)`). The
distinction is:

| Feature                | Definition                                    | Timescale           |
| ---------------------- | --------------------------------------------- | ------------------- |
| `bw_drop_prev`         | current_bataiju − prev_race_bataiju (lag 1)   | Single-race delta   |
| `weight_diff_from_avg` | current_bataiju − mean(bataiju, last 5 races) | 5-race rolling mean |

The single-race delta could capture acute injury signal that the 5-race average blurs out
(a horse dropping 8kg between consecutive starts may have an acute SDFT issue even if its
rolling average is unremarkable).

## Method

Single-pass PG extraction via `nvd_se` (NAR) and `jvd_se` (JRA), ordering each horse's
career chronologically and taking the LAG-1 bataiju as `bataiju_prev`. Races with
`bataiju_raw` outside 300–700 kg or missing `kakutei_chakujun` were excluded. Ban-ei
(keibajo=83) and non-race venues (81, 82, 84) excluded from NAR slice.

**Partial Spearman controls:** `bataiju_current` (absolute weight), `weight_diff_from_avg`
(5-race rolling deviation), `futan_raw` (burden weight). The partial rho strips the
contribution of the existing weight family and measures residual signal in `bw_drop_prev`.

**Bar:** partial Spearman ρ ≥ 0.08, coverage ≥ 70%, no venue concentration.

## Distinction Confirmation (B1 vs Existing weight_diff_from_avg)

The empirical correlation between `bw_drop_prev` and `weight_diff_from_avg` was measured
at ρ = 0.658 (NAR) and ρ = 0.679 (JRA). While non-trivial (the two features share the
same numerator `current_bataiju − something_prior`), the correlation is well below 1.0,
confirming they are distinct. The 5-race rolling average dilutes the single-step delta:
a horse that gained +6kg over 4 races then suddenly dropped 8kg will have a strong
negative `bw_drop_prev` but a still-positive or small-negative `weight_diff_from_avg`.

The partial Spearman analysis below directly resolves whether the remaining orthogonal
component (after removing `weight_diff_from_avg`) carries useful signal.

## Results

### NAR

| Metric                           | Value                                            |
| -------------------------------- | ------------------------------------------------ |
| Total starters                   | 2,864,753                                        |
| Starters with prev-race BW       | 2,750,528                                        |
| Coverage                         | **96.0%** (well above 70% bar)                   |
| N races (partial set)            | 283,388                                          |
| Mean within-race std(bw_drop)    | 4.947 kg                                         |
| Races with within-race variation | 283,136 / 283,388 (99.9%)                        |
| Raw Spearman ρ                   | **+0.017** (p=1.0e-170, n=2.75M)                 |
| Controls used                    | bataiju_current, weight_diff_from_avg, futan_raw |
| Partial Spearman ρ               | **+0.027**                                       |
| Bar                              | 0.08                                             |
| Pass?                            | **NO** (0.027 << 0.08)                           |
| Redundancy (max abs ρ)           | 0.658 vs `weight_diff_from_avg`                  |
| Venue ρ std                      | 0.014                                            |
| Top-2 venues coverage            | 23.4%                                            |
| Venue concentration              | BROAD (not concentrated)                         |
| **VERDICT**                      | **ABORT**                                        |

Per-venue raw Spearman ρ (NAR, selected venues, n ≥ 1,000):

| keibajo | N       | Raw ρ  |
| ------- | ------- | ------ |
| 30      | 131,639 | +0.024 |
| 35      | 126,943 | +0.029 |
| 36      | 140,115 | +0.021 |
| 42      | 125,630 | +0.041 |
| 43      | 132,334 | −0.009 |
| 44      | 283,752 | +0.017 |
| 45      | 153,468 | −0.005 |
| 46      | 170,822 | +0.022 |
| 47      | 174,593 | +0.022 |
| 48      | 256,417 | +0.009 |
| 50      | 335,170 | +0.013 |
| 54      | 226,066 | +0.027 |
| 55      | 216,429 | +0.008 |

The signal is uniformly near-zero across all venues. No single venue carries a meaningful
effect. The range [−0.009, +0.046] is fully within noise band at this sample size.

### JRA

| Metric                        | Value                            |
| ----------------------------- | -------------------------------- |
| Total starters                | 2,815,878                        |
| Starters with prev-race BW    | 2,572,899                        |
| Coverage                      | **91.4%**                        |
| N races (partial set)         | 222,266                          |
| Mean within-race std(bw_drop) | 6.496 kg                         |
| Raw Spearman ρ                | **−0.013** (p=2.4e-102, n=2.57M) |
| Partial Spearman ρ            | **+0.016**                       |
| Bar                           | 0.08                             |
| Pass?                         | **NO** (0.016 << 0.08)           |
| Redundancy (max abs ρ)        | 0.679 vs `weight_diff_from_avg`  |
| Venue concentration           | BROAD (top-2: 18.4%)             |
| **VERDICT**                   | **ABORT**                        |

JRA raw ρ is negative (−0.013), indicating a slight positive association of weight gain
with better finish in JRA — opposite sign to NAR (+0.017). Both are near-zero. The partial
ρ in JRA (0.016) is even weaker than NAR (0.027), suggesting the single-race delta does
not generalize across categories.

## Summary Table

| Source | Coverage | Raw ρ  | Partial ρ | Bar  | Redundancy (vs weight_diff_from_avg) | Verdict |
| ------ | -------- | ------ | --------- | ---- | ------------------------------------ | ------- |
| NAR    | 96.0%    | +0.017 | +0.027    | 0.08 | ρ=0.658                              | ABORT   |
| JRA    | 91.4%    | −0.013 | +0.016    | 0.08 | ρ=0.679                              | ABORT   |

## Verdict

**ABORT — H-PREV-BW-DROP does not meet the partial ρ ≥ 0.08 bar.**

**Decision number:** partial ρ(NAR) = **+0.027**, partial ρ(JRA) = **+0.016**.

Both are 3–5× below the bar. Coverage is excellent (96% NAR, 91% JRA) and the signal has
within-race variation in 99.9% of races, ruling out density or constant-feature failure
modes. The signal simply is not there:

1. **Raw signal near-zero:** +0.017 NAR, −0.013 JRA (opposite signs). At N=2.75M the
   standard error of ρ is ~0.0006, so even the raw signal is at the ~30 SE level — but
   the absolute magnitude is trivially small for a ranking task.

2. **After partialling weight family:** the residual (0.027 NAR, 0.016 JRA) captures
   nothing beyond `weight_diff_from_avg`. The single-race delta IS partially captured by
   the 5-race rolling deviation (ρ=0.66), and the orthogonal component carries no
   additional rankable information.

3. **Venue uniformity confirms global null:** per-venue ρ range [−0.009, +0.046] across
   19 NAR venues, close to zero in every venue including venues with strong drainage effects
   (門別, 高知) where the injury hypothesis might amplify.

**Interpretation:** The JES journal finding (SDFT injury OR 1.4–2.3) likely applies to
clinically significant weight drops (e.g., ≥10 kg) and post-injury contexts that are a
small minority of all starts. In the full population, body-weight fluctuations of ±5 kg
between starts are routine (fitness, digestive variation, travel) and uncorrelated with
finish position net of the existing weight family. The probe does not find a training
signal at population level.

## Historical Bar Context

| Probe                       | Signal                    | Partial ρ | Outcome   |
| --------------------------- | ------------------------- | --------- | --------- |
| V3 H-AGE-MONTH              | age-month speed deviation | 0.055     | ABORT     |
| V5 H-RACE-VOLUME-DENSITY    | race density              | 0.059     | ABORT     |
| V6 H-SIRE-DISTANCE-SPLIT    | sire distance split       | 0.025     | ABORT     |
| V7 JOINT-WEAK-ORTHOGONAL    | composite partial rho     | 0.005     | ABORT     |
| V8 H-BABA-PAR-TIME          | baba-adjusted speed       | 0.180     | PROCEED   |
| **B1 H-PREV-BW-DROP (NAR)** | prev-race BW delta        | **0.027** | **ABORT** |

B1's partial ρ (0.027) is comparable to V6 (0.025) and well below V3 (0.055), confirming
the JES journal's SDFT association does not translate to a population-level rankable signal.

## Next Steps

None for this hypothesis. Do not revisit. The distinction from `weight_diff_from_avg` is
empirically confirmed (ρ=0.658, not 1.0), but the orthogonal component carries no signal.

If acute-injury signal remains of interest, a threshold-based variant (e.g., indicator for
`bw_drop_prev < −10 kg`, interaction with `days_since_last_race`) could be explored as a
new hypothesis — but the evidence here does not motivate prioritizing it over other levers
in the current PROCEED queue (H-BABA-PAR-TIME WF retrain).
