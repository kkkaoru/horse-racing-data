---
science_track_entry: true
hypothesis_id: H-OFFICIAL-RUNNING-STYLE
date: 2026-06-11
based_on_iteration: iter14-jra-cb-pacestyle-course-v8 (JRA production baseline)
scope: JRA only (kyakushitsu_hantei exists in jvd_se, 98%+ coverage 2016+)
status: ABORT (best partial rho full=0.074 / holdout=0.068 — both below bar 0.08; heavily redundant with existing inferred-style features)
verdict: ABORT — official historical running style absorbed by existing corner1_norm / rs_p_* features; orthogonal residual below bar in both windows
production_change: none (probe only)
artifacts:
  probe_script: tmp/kyakushitsu/probe_kyakushitsu.py
  probe_verdict: tmp/kyakushitsu/probe_verdict.json
---

## Hypothesis

**H-OFFICIAL-RUNNING-STYLE** — JRA's official post-race running-style label
(`kyakushitsu_hantei`, `jvd_se`, values 1=逃げ/2=先行/3=差し/4=追込) is recorded
by JRA judges based on the horse's race behaviour. Coverage: ~98.3% for JRA 2016+
(77k–76k per year). The hypothesis is that the **historical distribution** of a
horse's official labels from **prior races** is a cleaner pace/positioning signal
than the system's inferred running style, and adds orthogonal information beyond
the existing features.

### Critical Leak Warning

`kyakushitsu_hantei` is a POST-race classification for the CURRENT race. Using the
current race's value directly is a **label leak** (the model would see the outcome
it is trying to predict). The probe uses only PRIOR-race values:

```
kh_hist_nige_rate(race R) = fraction of races with kh=1 in all races BEFORE race R
```

This is enforced via a window function with `ROWS BETWEEN UNBOUNDED PRECEDING AND
1 PRECEDING` on the PG `jvd_se` table.

### Existing Inferred-Style Features (Orthogonality Target)

The current JRA feature set already contains two inferred-style layers:

| Feature family                                                                                     | Source                                        | Description                                            |
| -------------------------------------------------------------------------------------------------- | --------------------------------------------- | ------------------------------------------------------ |
| `rs_p_nige/senkou/sashi/oikomi`                                                                    | Running-style v3 model                        | Model-inferred probabilities for each style class      |
| `past_nige_rate_self` / `past_senkou_rate_self` / `past_sashi_rate_self` / `past_oikomi_rate_self` | `corner1_norm` (normalized corner-1 position) | Fraction of prior races in each style bucket           |
| `past_corner_1_norm_avg_5`                                                                         | `corner1_norm`                                | Average normalized corner-1 position over last 5 races |

The probe tests whether `kyakushitsu_hantei` historical fractions add signal **orthogonal**
to all nine of these controls.

## Method

- **Source:** `jvd_se.kyakushitsu_hantei` (JRA, 2010–2026, 1.245M rows with valid kh)
- **Leak-free construction:** DuckDB window function, cumulative counts of each kh value
  over prior races only
- **Features built:**
  - `kh_hist_nige_rate` (fraction kh=1 in prior races)
  - `kh_hist_senkou_rate` (fraction kh=2)
  - `kh_hist_sashi_rate` (fraction kh=3)
  - `kh_hist_oikomi_rate` (fraction kh=4)
  - `kh_hist_consistency` (fraction of the dominant style = max fraction)
- **Join:** left-joined onto the existing JRA feature parquet (feat-v15-rs-labeled, 724,761 rows 2016–2025)
- **Target:** `finish_norm` (finish position / field size, lower = better)
- **Controls (partial rho):** `rs_p_nige`, `rs_p_senkou`, `rs_p_sashi`, `rs_p_oikomi`,
  `past_nige_rate_self`, `past_senkou_rate_self`, `past_sashi_rate_self`,
  `past_oikomi_rate_self`, `past_corner_1_norm_avg_5`
- **Partial rho method:** residual-on-rank (rank all variables, OLS residualize feature
  and target on control ranks, Pearson of residuals = partial Spearman ρ)
- **Windows:** full period (2016–2025) + holdout (2023–2025)
- **Bar:** partial ρ ≥ 0.08 in BOTH windows

## Coverage

| Threshold           | Rows with kh history | % of JRA parquet (724,761) |
| ------------------- | -------------------- | -------------------------- |
| kh_hist_n ≥ 0 (any) | 724,761              | 100.0%                     |
| kh_hist_n ≥ 1       | 724,761              | 100.0%                     |
| kh_hist_n ≥ 3       | 607,000              | 83.8%                      |
| kh_hist_n ≥ 5       | 506,693              | 69.9%                      |

Within-race mean std of `kh_hist_nige_rate`: **0.1356** — sufficient within-race
variation to distinguish horses positionally.

Partial analysis rows (both kh history and all controls non-null):

- Full period: 457,589
- Holdout 2023+: 128,405

## Results

### Partial Spearman ρ Table (key metric)

| Feature                   | Raw ρ full | Raw ρ holdout | Partial ρ full | Partial ρ holdout | n (partial, full) | n (partial, holdout) | Meets bar? |
| ------------------------- | ---------- | ------------- | -------------- | ----------------- | ----------------- | -------------------- | ---------- |
| `kh_hist_nige_rate`       | −0.003     | +0.002        | +0.027         | +0.030            | 457,589           | 128,405              | NO         |
| `kh_hist_senkou_rate`     | −0.121     | −0.122        | −0.047         | −0.043            | 457,589           | 128,405              | NO         |
| `kh_hist_sashi_rate`      | −0.057     | −0.049        | −0.063         | −0.055            | 457,589           | 128,405              | NO         |
| **`kh_hist_oikomi_rate`** | **+0.152** | **+0.152**    | **+0.074**     | **+0.068**        | 457,589           | 128,405              | **NO**     |
| `kh_hist_consistency`     | +0.006     | −0.001        | +0.013         | +0.008            | 457,589           | 128,405              | NO         |

**Deciding number:** `kh_hist_oikomi_rate` partial ρ = **+0.074 (full) / +0.068 (holdout)**.
Bar = 0.08. Both windows fail. Best feature is 7.0% / 15.0% below bar respectively.

### Redundancy vs Existing Features

| New feature           | Max       | ρ                          | vs existing | Closest existing feature |     | ρ   |     |
| --------------------- | --------- | -------------------------- | ----------- | ------------------------ | --- | --- | --- |
| `kh_hist_nige_rate`   | **0.796** | `past_nige_rate_self`      | 0.796       |
| `kh_hist_senkou_rate` | **0.494** | `past_senkou_rate_self`    | 0.494       |
| `kh_hist_sashi_rate`  | **0.299** | `past_sashi_rate_self`     | 0.299       |
| `kh_hist_oikomi_rate` | **0.564** | `past_corner_1_norm_avg_5` | 0.564       |
| `kh_hist_consistency` | **0.245** | `past_nige_rate_self`      | −0.245      |

`kh_hist_nige_rate` is 79.6% correlated with `past_nige_rate_self` — near-duplicate. The
other style fractions are 30–56% correlated with their `corner1_norm`-based counterparts,
confirming the official label and the INFERRED label (via corner position) encode largely
the same information.

### Correlation Profile of `kh_hist_oikomi_rate` (best feature)

| Control feature            | ρ      |
| -------------------------- | ------ |
| `past_corner_1_norm_avg_5` | +0.564 |
| `past_oikomi_rate_self`    | +0.547 |
| `rs_p_oikomi`              | +0.524 |
| `rs_p_senkou`              | −0.487 |
| `rs_p_nige`                | −0.463 |
| `past_senkou_rate_self`    | −0.385 |
| `past_nige_rate_self`      | −0.282 |
| `rs_p_sashi`               | +0.088 |
| `past_sashi_rate_self`     | +0.073 |

The oikomi-rate feature is correlated with MULTIPLE existing style features simultaneously.
After jointly removing this multi-feature collinear projection, the residual partial ρ
(0.074) barely falls below the bar — confirming the existing style bundle already captures
most of the official label's variance.

## Interpretation

1. **Official label ≈ inferred label.** `corner1_norm` is the normalized corner-1 rank
   (position at corner 1 / field size), which is a continuous proxy for the official
   1/2/3/4 classification. The two representations are essentially measuring the same
   racing behaviour from different vantage points (official judge vs. electronic timing).
   A Spearman ρ of 0.796 between `kh_hist_nige_rate` and `past_nige_rate_self` confirms
   near-equivalence.

2. **Residual orthogonal signal is marginal.** The best partial ρ (0.074 for
   `kh_hist_oikomi_rate`) represents the "disagreement" between the official judge and the
   corner-1 timing system for oikomi horses. That disagreement is real (ρ < 1.0) but its
   predictive content (0.074 vs bar 0.08) is below the minimum threshold required to
   justify model complexity.

3. **Holdout degradation.** The holdout partial ρ (0.068) is lower than the full-period ρ
   (0.074), indicating the marginal orthogonal signal does not improve out-of-sample. This
   is the expected pattern for a feature that is largely redundant with existing features —
   the residual captures measurement noise rather than a stable horse-level tendency.

4. **`kh_hist_nige_rate` near-zero raw ρ.** Despite being the most reliably classified
   style (JRA judges consistently identify nige horses), the raw Spearman ρ vs finish_norm
   is −0.003 (full) and +0.002 (holdout) — statistically negligible. Nige horses do not
   systematically finish better or worse than other styles at population level; their
   relative advantage depends on pace scenarios (already captured by the race-context
   features) rather than the historical base rate.

5. **Coverage is not the limiting factor.** 100% of rows have kh history entries
   (including first-start horses with kh_hist_n=0, which receive NULL fractions) and 83.8%
   have at least 3 prior starts. The ABORT is purely about signal strength, not data density.

## Verdict

**ABORT — H-OFFICIAL-RUNNING-STYLE does not meet the partial ρ ≥ 0.08 bar.**

**Deciding number:** best partial ρ = **0.074 (full) / 0.068 (holdout)**, both windows
below bar of 0.08.

The official JRA running-style label (`kyakushitsu_hantei`) is largely absorbed by the
existing inferred-style features (`rs_p_*` from running-style v3 model and
`past_nige/senkou/sashi/oikomi_rate_self` from corner-1 norm). Adding historical official
label fractions adds no meaningful orthogonal signal to the finish-position ranking task.

No production change. Do not revisit this exact feature construction.

## Historical Bar Context

| Probe                        | Signal                              | Partial ρ                          | Outcome   |
| ---------------------------- | ----------------------------------- | ---------------------------------- | --------- |
| V3 H-AGE-MONTH               | age-month speed deviation           | 0.055                              | ABORT     |
| V5 H-RACE-VOLUME-DENSITY     | race density                        | 0.059                              | ABORT     |
| V6 H-SIRE-DISTANCE-SPLIT     | sire distance split                 | 0.025                              | ABORT     |
| V7 JOINT-WEAK-ORTHOGONAL     | composite partial rho               | 0.005                              | ABORT     |
| V8 H-BABA-PAR-TIME           | baba-adjusted speed                 | 0.180                              | PROCEED   |
| B1 H-PREV-BW-DROP (NAR)      | prev-race BW delta                  | 0.027                              | ABORT     |
| **H-OFFICIAL-RUNNING-STYLE** | **official kh historical fraction** | **0.074 (full) / 0.068 (holdout)** | **ABORT** |

The 0.074/0.068 partial ρ is the highest of the recent ABORT probes, placing it above
V3/V5/V6/B1 but below the PROCEED bar. It did not clear either window.

## Hard Rules Observed

- `tmp/` only: probe script and verdict JSON in `tmp/kyakushitsu/` (not git-tracked)
- No `git add tmp/` (parquet, scripts, verdicts stay untracked)
- PG read-only throughout (no writes to any table)
- No production deployment or active_models registry change
- Current-race `kyakushitsu_hantei` NOT used (leak prevention verified via window clause)
- Threads ≤ 3 (single-threaded DuckDB probe)
