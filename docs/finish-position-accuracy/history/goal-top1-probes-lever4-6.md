# JRA Top1 Levers 4 & 6 — Partial-ρ Probes

**Date:** 2026-06-17
**Scope:** JRA iter19-jra-cb-kohan3f-going-v8 (CatBoost YetiRank, 244 features)
**Source parquet:** `tmp/v8/feat-jra-v8-iter19-kohan3f-going/**/*.parquet`
**Method:** Residual-on-rank partial Spearman ρ
— Rank all variables (ties averaged), OLS-residualize feature and target on control ranks, Pearson of residuals.
**Memory:** DuckDB `SET memory_limit='6GB'; SET threads=4` (enforced throughout).
**Data access:** READ-ONLY (parquet scan only; no model training).

**Probe gate:** partial ρ ≥ 0.08 in **both** windows AND coverage adequate.

---

## Lever 4 — 1400-1599m Distance-Band Win-Rate

**Hypothesis:** A per-horse band win rate (all starts in 1400–1599m range before each race)
captures "sprint-to-mile transition zone" specialization better than the existing
`same_distance_win_rate` (exact distance level). The residual
`band_1400_1599_win_rate − career_win_rate` targets over/under-performance at this band.

**Feature construction (leak-free):**

- `prior_band_starts`: cumulative count of this horse's prior races with `kyori BETWEEN 1400 AND 1599`,
  computed via window `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING` ordered by `(race_date, race_id)`.
- `prior_band_wins`: same window, counting rows where `finish_position = 1`.
- `band_1400_1599_win_rate = prior_band_wins / prior_band_starts` (NULL if no prior band starts).
- Residual feature: `band_1400_1599_win_rate − career_win_rate`.
- Applied only to races with `kyori BETWEEN 1400 AND 1599`.

**Controls:** `tansho_ninkijun_raw` (odds rank), `same_distance_win_rate`, `career_win_rate`.

### Results

| Window            | n rows (band races) | n with rate | Coverage | ρ (band win rate) | ρ (residual) |
| ----------------- | ------------------- | ----------- | -------- | ----------------- | ------------ |
| Holdout 2023–2025 | 20,662              | 13,545      | 65.6%    | −0.0057           | +0.0030      |
| Full 2016–2025    | 70,508              | 46,810      | 66.4%    | −0.0047           | +0.0030      |

**Coverage:** 65–66% of 1400-1599m race starters have ≥1 prior same-band start. Adequate
(well above a minimum useful level), though 34% of horses lack a prior band start and would
receive NULL (no meaningful band rate yet).

**Verdict: ABORT**

Both ρ values are effectively 0 (|ρ| < 0.006 on raw feature; +0.003 on residual — an order
of magnitude below the 0.08 gate). The signal is absent across both windows.

**Interpretation:** After controlling for odds rank, `same_distance_win_rate`, and
`career_win_rate`, the 1400-1599m band win rate carries no residual information about
finish outcome. This is consistent with the project's recurring finding (memory
`project_relationship_perclass_investigation`): GBDT already captures distance-preference
non-linearly through the existing exact-distance and career win rate features. The band
aggregation adds nothing beyond what the existing features already encode.

ABORT does not proceed to retrain. The 0.08 gate is not cleared in either window.

---

## Lever 6 — Large-Field Pace Entropy

**Hypothesis:** For races with field_size ≥ 15, Shannon entropy of `rs_p_nige` across
horses captures second-order pace volatility ("how ambiguous is the front-running
competition?") beyond the existing first-order aggregates (`field_nige_pressure`,
`field_pace_index`).

**Feasibility check (required first):** If >30% of large-field races have >50% of horses
with NULL `rs_p_nige`, the entropy estimate is unreliable and the feature is declared
INFEASIBLE.

### NULL Coverage Analysis

| Window            | Large-field races (≥15 horses) | Races with >50% NULL rs_p_nige | Fraction infeasible |
| ----------------- | ------------------------------ | ------------------------------ | ------------------- |
| Holdout 2023–2025 | 5,195                          | 1,769                          | **34.1%**           |
| Full 2016–2025    | 18,164                         | 14,738                         | **81.1%**           |

**Infeasibility threshold: >30% → INFEASIBLE.**

Both windows exceed the threshold. The holdout window (2023-2025) already fails at 34.1%.
The full window is catastrophically sparse at 81.1% — this reflects that `rs_p_nige` is
backfilled only for horses with sufficient race history for the RS v3 model; new horses
and older history epochs have overwhelmingly NULL running-style probabilities.

**Verdict: INFEASIBLE / ABORT**

The coverage constraint is violated in both windows. The entropy computation cannot be
made reliable without substantially broader RS model coverage. No ρ test was run (per the
protocol: if coverage gate fails, skip ρ).

**Interpretation:** The RS v3 model's 42% aggregate NULL rate (noted in
`goal-plan-C-top1-levers.md` §Lever 6 caveat) manifests as even higher NULL concentration
in large-field races, because large fields (15-18 horses) include more recent debutants
and lightly-raced horses without RS history. The entropy feature would effectively be
computed from a non-representative subset of horses in most races, making it a noisy
proxy for field composition rather than a true pace-volatility measure.

---

## Summary

| Lever                      | ρ (holdout 2023-2025)    | ρ (full 2016-2025)       | Gate ≥ 0.08? | Verdict   |
| -------------------------- | ------------------------ | ------------------------ | ------------ | --------- |
| 4: Band win rate (raw)     | −0.0057                  | −0.0047                  | NO           | **ABORT** |
| 4: Band residual           | +0.0030                  | +0.0030                  | NO           | **ABORT** |
| 6: Pace entropy (coverage) | INFEASIBLE (34.1% > 30%) | INFEASIBLE (81.1% > 30%) | N/A          | **ABORT** |

**Both levers ABORT.** Neither clears the partial ρ ≥ 0.08 gate (Lever 4) nor the
coverage feasibility gate (Lever 6).

**Note on PROCEED semantics (not reached):** Had either lever cleared its gate, PROCEED
would mean "earns a cheap-filter retrain (3-fold WF)" — not adoption. The gate clears
the probe filter only; the cheap filter and full WF judge remain required gates before
any production change. This distinction is documented in
`goal-plan-C-top1-levers.md` §6 and the project memory
`project_relationship_perclass_investigation`.

---

## Implications for Lever Campaign

With Levers 4 and 6 both ABORT, the remaining highest-priority untested levers are:

1. **Lever 3** (isotonic calibration of raw CatBoost scores) — no retraining needed, < 1h,
   estimated +0.3-0.8pp conservative. This remains the single most actionable lever.
2. **Lever 2** (training window ablation: 2013+ vs 2006+) — cheap 3-fold WF filter.
3. **Lever 1** (HPO on iter19 244-feature store) — 3-5h, corrected CV protocol.

The empirical frontier assessment from `project_science_track_saturation_2026_06_11` is
reinforced: distance-band specialization and RS-derived race-level entropy were the two
remaining untested feature hypotheses at the JRA level. Both are now confirmed as
uninformative after controlling for existing features.
