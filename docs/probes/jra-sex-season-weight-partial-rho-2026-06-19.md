# JRA sex × season × weight partial ρ probe (2026-06-19)

## Motivation

JRA finish-position model (iter20, 244 features) currently has **no sex feature** (`seibetsu_code`).
Probe whether sex, and its interactions with season / 馬体重 / 斤量 class, carry incremental
signal beyond market odds.

## Setup

- Feature store: `tmp/v8/feat-jra-v8-iter19-kohan3f-going/race_year={2023,2024,2025}/data_0.parquet`
- Sex source: `pg.jvd_um.seibetsu_code` (1=牡, 2=牝, 3=せん), joined on `ketto_toroku_bango`
- DuckDB `memory_limit='4GB'`, `threads=4` (kernel-panic guard)
- Metric: partial Spearman ρ vs `finish_position`, controlling for `odds_score`
  (rank-residualization). Gate: |ρ| ≥ 0.08 AND within-race variation.

## Coverage

- Joined rows: **141,523 / 141,523 = 100.0%** of valid FS rows (every horse matched a sex).
- Sex distribution (joined): 牡 70,912 / 牝 59,027 / せん 11,584.
- Within-race sex variation: 10,365 races, mean 2.36 distinct sexes/race, **84.0% of races mix sexes** → variation is healthy (not collinear with race).
- Raw mean finish_position by sex: 牡 7.33 / 牝 8.02 / せん 7.54 (牝 finishes slightly worse on average, but this is largely absorbed by odds).

## Partial ρ results (control = odds_score)

| feature                         | partial ρ | \|ρ\|  | gate |
| ------------------------------- | --------- | ------ | ---- |
| sex_code (numeric)              | +0.0379   | 0.0379 | fail |
| is_mare (牝=2)                  | +0.0380   | 0.0380 | fail |
| is_gelding (せん=3)             | +0.0063   | 0.0063 | fail |
| season_band × sex               | +0.0169   | 0.0169 | fail |
| season_band × is_mare           | +0.0301   | 0.0301 | fail |
| bataiju × sex                   | +0.0481   | 0.0481 | fail |
| bataiju × is_mare               | +0.0395   | 0.0395 | fail |
| season × sex × weight_class     | −0.0050   | 0.0050 | fail |
| season × is_mare × weight_class | −0.0050   | 0.0050 | fail |

## Verdict: NO-GO (all features fail |ρ| ≥ 0.08)

- Strongest signal is `bataiju × sex` at **+0.048**, only ~60% of the 0.08 threshold.
- The triple interaction `season × sex × weight_class` is essentially zero (−0.005), i.e. the
  hypothesized seasonal/weight modulation of sex effect does not survive odds control.
- Sex's marginal effect (牝 finishing worse) is real in raw means but **already priced into odds**
  — the residual partial ρ collapses to ~0.038.
- Consistent with prior frontier findings: market efficiency absorbs static per-horse attributes;
  partial ρ being necessary-but-not-sufficient, even a passing ρ would still need an incremental
  model check, and these don't even clear the probe gate.

**Recommendation:** do not add sex / sex-interaction features to the JRA iter20 model. No further
incremental-model validation warranted.
