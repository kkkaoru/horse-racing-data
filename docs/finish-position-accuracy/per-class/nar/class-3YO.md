---
class: 3YO
category: nar
n_races_holdout_2023_26: 8578
baseline_top1: 59.63
baseline_place2: 36.21
baseline_place3: 27.93
active_ensemble: iter12 via other fallback
---

# NAR 3YO — class file

## Status

Active ensemble: `iter12 via other fallback`.

## Baseline (holdout 2023-2026)

| Metric  | Model% |
| ------- | ------ |
| top1    | 59.63  |
| place2  | 36.21  |
| place3  | 27.93  |
| n races | 8578   |

## Active Hypotheses

_(None active — see Evaluation Log for closed experiments.)_

## Evaluation Log

| Date       | Hypothesis                       | Method | Verdict | Ref                                          |
| ---------- | -------------------------------- | ------ | ------- | -------------------------------------------- |
| 2026-06-17 | Age-proxy features (Candidate 2) | FEAT   | ABORT   | `docs/per-class/nar/class-3YO.md` §Age probe |
| prior      | CB residual (iter37)             | ML     | REJECT  | `history/phase3-nar-2yo3yo-perclass.md`      |

---

## Age-proxy Feature Probe (2026-06-17) — ABORT

**Script**: `tmp/probe_nar_3yo_age_features.py`
**Source parquet**: `tmp/feat-nar-v8-iter17-bataiju/` (NAR, all years)
**PG join**: `nvd_ra.kyoso_joken_meisho` → nar_subclass = '3YO' filter (same regex as production)

### Method

Partial Spearman ρ of each feature vs `finish_norm`, controlling for
`popularity_score` + `career_win_rate` (rank-residual OLS, pure Python).
Bar: |ρ| ≥ 0.08 in BOTH full (2013-2026) AND holdout (2023-2026) → PROCEED.
Features are leak-free (window `ROWS UNBOUNDED PRECEDING AND 1 PRECEDING`,
ordered by `race_date, target_race_id`).

### Sample sizes

- Full window (2013-2026): 456,578 horse-race rows (3YO subclass)
- Holdout (2023-2026): 80,295 horse-race rows

### Results

| Feature                   |  ρ_full | ρ_holdout | coverage | Clears bar? |
| ------------------------- | ------: | --------: | -------: | ----------- |
| career_race_count         | +0.0033 |   −0.0151 |     100% | no          |
| days_since_first_race     | +0.0045 |   −0.0059 |     100% | no          |
| days_since_last_race      | +0.0315 |   +0.0331 |     100% | no          |
| races_in_current_season   | −0.0010 |   −0.0179 |     100% | no          |
| career_race_count × kyori | +0.0034 |   −0.0144 |     100% | no          |

Best feature: `days_since_last_race` (full +0.032, holdout +0.033) — less than half the bar.
All other features are at noise level (|ρ| < 0.02), with sign flips between windows
(career_race_count: +0.003 full vs −0.015 holdout — pure noise).

### Verdict: ABORT

No feature clears ρ ≥ 0.08 in either window, let alone both. The signal is not available
from race-history features alone. GBDT already captures these indirectly via `career_win_rate`,
`consecutive_race_count`, `days_since_last_race` (already in feature set), and
`career_place_rate`.

### foal_month (age-at-race)

SKIPPED — requires `nvd_um.foal_date` (nvd_um coverage collapses to ~21% for 2026 NAR runners,
same serve-blocking issue as Signal 4). Even if computable historically, it cannot be served
reliably for upcoming races and is therefore not a valid serve-side feature.

### Implication for 3YO

The 3YO class is at 59.63% top1 (−0.37pp from the 60% target). The iter37 CB residual
was already REJECTED (no signal in residual space without age-specific features). This probe
confirms that the candidate age-specific features have no extractable signal from race history.
**The 3YO gap to 60% cannot be closed by race-history age proxies; new data sources (foal
date with reliable coverage, or external performance data) would be required.**

3YO is considered **SATURATED** under current data availability.
