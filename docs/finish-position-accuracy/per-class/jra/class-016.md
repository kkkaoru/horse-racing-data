---
class: 016
label: 3勝クラス
category: jra
n_races_holdout_2023_26: 727
baseline_top1: 37.55
baseline_place2: 20.91
baseline_place3: 12.79
model_vs_market_top1_delta: +5.64
active_model: iter19-jra-cb-kohan3f-going-v8 (category-global fallback)
per_class_model: none (Phase B registry empty)
probe_date: 2026-06-17
probe_verdict: ABORT
---

# JRA 016 — 3勝クラス

## Status

Routes to category-global `iter19-jra-cb-kohan3f-going-v8`. No per-class model registered.
Class-transition feature probe (Candidate 3) completed 2026-06-17: **ABORT** — all 4 features
decisively below the ρ ≥ 0.08 bar in both full and holdout windows.

## Headroom

| Metric | Model% | Notes           |
| ------ | -----: | --------------- |
| top1   |  37.55 | Mdl−Mkt +5.64pp |
| place2 |  20.91 | —               |
| place3 |  12.79 | —               |

## Probe: Class-Transition Features (2026-06-17)

### Method

Partial Spearman ρ of each candidate feature vs `finish_norm`, controlling for
`popularity_score + career_win_rate` (rank-residual OLS). Gate: |ρ| ≥ 0.08 in BOTH
full (2013-2026) AND holdout (2023-2026) windows.

Script: `tmp/probe_jra016_class_transition.py` (tmp/, not committed).

Data sources:

- `jvd_ra` (PG): race-level `kyoso_joken_code` for 016 filtering (JRA non-Ban-ei, 2006+)
- `jvd_se` (PG): horse-level results (nyusen_juni, time_sa) for history computation
- `feat-v20-merged-v5/jra/**/*.parquet`: finish_norm, popularity_score, career_win_rate

All features computed **leak-free**: only prior races (race_date < target race_date) used.

### Features Probed

| Feature                     | Definition                                                                         | Coverage |
| --------------------------- | ---------------------------------------------------------------------------------- | -------: |
| `class_transition_velocity` | total prior races / wins at 005+010 (lower=faster climber); NULL if no 005/010 win |    92.9% |
| `days_at_current_class`     | calendar days since first 016 race (0 if debut at 016)                             |    99.6% |
| `rival_class_delta`         | avg class level (0-6 scale) of 5 most recent prior races                           |    99.6% |
| `win_margin_trend`          | OLS slope of time_sa over 5 most recent prior races (negative=improving)           |    98.8% |

### Results

**Full window (2013-2026, n_full=36,414 entries):**

| Feature                   | partial ρ | n valid | Clears 0.08? |
| ------------------------- | --------: | ------: | :----------: |
| class_transition_velocity |   −0.0008 |  34,636 |      NO      |
| days_at_current_class     |   −0.0098 |  36,414 |      NO      |
| rival_class_delta         |   −0.0204 |  36,414 |      NO      |
| win_margin_trend          |   +0.0223 |  36,413 |      NO      |

**Holdout window (2023-2026, n_holdout=10,300 entries):**

| Feature                   | partial ρ | n valid | Clears 0.08? |
| ------------------------- | --------: | ------: | :----------: |
| class_transition_velocity |   −0.0154 |   9,824 |      NO      |
| days_at_current_class     |   −0.0085 |  10,300 |      NO      |
| rival_class_delta         |   −0.0264 |  10,300 |      NO      |
| win_margin_trend          |   +0.0125 |  10,300 |      NO      |

Best feature: `rival_class_delta` at ρ = −0.026 (holdout). All features ≤ 3× below bar.

### Interpretation

All four class-transition features fail decisively: the largest partial ρ (in absolute
value) is 0.026, versus the 0.08 bar. The spread across windows is consistent — no
feature shows even directional promise. This is the same pattern observed for JRA
relationship features (see `jra-relationship-features-perclass.md`): GBDT already
captures class-transition dynamics non-linearly via existing features such as
`last_race_class_diff`, `career_top1_count`, `same_grade_win_rate`, `career_win_rate`,
and `speed_index_avg_5`. Adding explicit class-transition features offers no incremental
signal.

### Root Cause Hypothesis

- **`class_transition_velocity`**: career_top1_count / same_grade_win_rate already
  encode this; the explicit velocity ratio adds nothing beyond what GBDT derives.
- **`days_at_current_class`**: the model already receives `days_since_last_race`,
  `consecutive_race_count`, and `avg_finish` which together encode tenure effects.
- **`rival_class_delta`**: the avg class of recent prior races ≈ the horse's career
  progression trajectory, already captured by `last_race_class_diff` (016 − prior
  class level of the immediately preceding race).
- **`win_margin_trend`**: time_sa trend is correlated with `finish_trend_5` and
  `avg_finish` already in the feature set.

### Verdict: ABORT

No retrain justified. JRA 016 headroom (+22.45pp to 60% goal) is not addressable via
class-transition features. The 016 saturation diagnosis aligns with the broader JRA
per-class relationship finding: at 016 level the GBDT's generic features already
encode all available class-progression signal.

## Evaluation Log

| Date       | Hypothesis                                                            | Method          | Verdict                                                             | Ref                                    |
| ---------- | --------------------------------------------------------------------- | --------------- | ------------------------------------------------------------------- | -------------------------------------- |
| 2026-06-17 | Class-transition features (velocity, days, rival_delta, margin_trend) | Partial ρ probe | ABORT — all features ρ ≤ 0.026, well below 0.08 bar in both windows | `tmp/probe_jra016_class_transition.py` |
