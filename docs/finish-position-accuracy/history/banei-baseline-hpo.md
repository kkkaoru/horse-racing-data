---
science_track_entry: true
hypothesis_id: BANEI-BASELINE-HPO
date: 2026-06-11
scope: Ban-ei (keibajo_code=83, ばんえい競馬)
status: COMPLETE — baseline established, HPO REJECT, futan-ratio probe ABORT
verdict: HPO REJECT (top1 regressed −0.201pp beyond −0.05pp gate); default hyperparams retained. Futan-ratio signal ABORT (partial ρ=−0.030, bar=0.08).
production_change: none
artifacts:
  baseline_holdout: tmp/banei/baseline_holdout.json
  hpo_result: tmp/banei/hpo_result.json
  futan_ratio_probe: tmp/banei/futan_ratio_probe.json
  features_cache: tmp/banei/features/ (20 parquet files, 311142 rows, 2007-2026)
---

## Context

Ban-ei (ばんえい競馬) is Hokkaido's unique draft-horse sled-pulling format. The active production
model is **banei-cb-v7-lineage-wf-21y** (CatBoost YetiRank, 111 features, 5 layers). Prior to this
entry, NO holdout metrics had ever been recorded for Ban-ei — the JRA/NAR iterative loop has been
accumulating since iter1, but Ban-ei was treated as an afterthought with no science-track history.

This entry serves three purposes:

1. Record the first formal holdout baseline (2023-2026) against which all future Ban-ei work is measured.
2. Bounded HPO (n_trials=40) to check if default hyperparams are already near-optimal.
3. Probe one Ban-ei-specific signal: `bataiju / futan_kg` body-mass-to-load ratio.

---

## Task 1 — Holdout Baseline

### Model

| Field              | Value                                  |
| ------------------ | -------------------------------------- |
| model_version      | banei-cb-v7-lineage-wf-21y             |
| architecture       | CatBoost YetiRank                      |
| feature_count      | 111                                    |
| hyperparams        | iter=300, depth=8, l2=3.0, lr=0.05     |
| train_date_range   | 2007-01-01 – 2022-12-31 (255,274 rows) |
| holdout_date_range | 2023-01-01 – 2026-06-xx (55,868 rows)  |

### Holdout Metrics (2023-2026, n=5,976 races)

| Axis          | Value   |
| ------------- | ------- |
| top1_accuracy | 0.34404 |
| place2_acc    | 0.55890 |
| place3_acc    | 0.43173 |
| top3_box_acc  | 0.09237 |

**Per-year race counts:**

| Year | n_races |
| ---- | ------- |
| 2023 | 1,788   |
| 2024 | 1,788   |
| 2025 | 1,692   |
| 2026 | 708     |

These are the **canonical reference values** for all future Ban-ei improvements. Any new model
must beat this table on the 4-axis multi-metric accept gate (≥2 positive, ≥1 of {place2/place3}
positive, no axis < −0.05pp, improving axis LB95 > 0).

---

## Task 2 — Feature Layer Inventory

### Parquet Cache

- Source: `tmp/feat-ban-ei-v7-grade-21y-parity/` (20 year-partitioned parquet files, 311,142 rows)
- Cached to: `tmp/banei/features/` (same schema, ready for instant retrain)
- Feature count: 111 numeric features (categorical columns excluded by `no_cat_features=True`)

### Layers Present vs JRA/NAR

**10 layers present in Ban-ei:**

| Layer          | Example Features                                       |
| -------------- | ------------------------------------------------------ |
| HORSE_CAREER   | career_win_rate, speed_index_avg_5, weight_avg_5       |
| JOCKEY_TRAINER | jockey_career_win_rate, trainer_career_win_rate        |
| PEDIGREE       | sire_distance_win_rate, pedigree_score_for_race        |
| RACE_CONTEXT   | field_strength_avg_speed, track_bias_inside            |
| RECENT_FORM    | last_race_finish_norm, finish_trend_5                  |
| MARKET_SIGNALS | popularity_score, odds_score                           |
| BABA_AFFINITY  | horse_baba_win_rate, sire_baba_win_rate                |
| FUTAN_CLASS    | current_futan_class, horse_futan_class_career_win_rate |
| GRADE_CAREER   | current_grade_rank, horse_grade_E/T/S/R/Q/P rates      |
| HEAD_TO_HEAD   | h2h_win_rate_vs_field, h2h_avg_finish_diff             |

**9 layers MISSING vs JRA (headroom ranking by expected impact):**

| Layer                    | Priority | Notes                                                             |
| ------------------------ | -------- | ----------------------------------------------------------------- |
| RACE_INTERNAL            | HIGH     | Relative position within race, post-weight rank features          |
| SECTIONAL                | HIGH     | Ban-ei has checkpoint timing (障害タイム) — unique to this format |
| PACESTYLE                | MED      | No running-style inference exists for Ban-ei                      |
| TRAINER                  | MED      | Stable-course affinity (stable_affinity, hiraba) absent           |
| NEAR_MISS                | MED      | Close finishes (margin to 3rd) not tracked                        |
| RELATIONSHIP             | MED      | Jockey-trainer combined win rates (relationship-r1)               |
| CLASS_PROMOTION_VELOCITY | LOW      | Grade promotion speed signal                                      |
| NON_PODIUM_PATTERNS      | LOW      | Non-podium distance correlation                                   |
| WORKOUT                  | LOW      | Ban-ei has no workout data in nvd_se                              |

**Priority assessment:** RACE_INTERNAL and SECTIONAL are the highest-value missing layers.
Ban-ei checkpoint timing (障害) is unique and has no equivalent in JRA/NAR — computing sectional
ranks within the race would be a Ban-ei-only differentiator.

---

## Task 3 — Bounded HPO (n_trials=40)

### Configuration

| Parameter         | Range                       | Seed |
| ----------------- | --------------------------- | ---- |
| depth             | [4, 10]                     | 42   |
| learning_rate     | [0.01, 0.2] log             | 42   |
| l2_leaf_reg       | [0.5, 10.0]                 | 42   |
| iterations        | {150,200,…,500}             | 42   |
| tuning_split      | train≤2020, valid=2021-2022 | —    |
| n_trials          | 40                          | TPE  |
| threads_per_trial | 4                           | —    |
| total_tune_time   | 320.5s                      | —    |

### HPO Best Config (from tuning split)

```json
{
  "iterations": 200,
  "depth": 9,
  "l2_leaf_reg": 2.23,
  "learning_rate": 0.01889
}
```

Tuning composite score (0.4×top1 + 0.3×place2 + 0.3×place3): **0.43833**

### Holdout Evaluation (2023-2026, same n=5,976 races)

| Axis          | Baseline | HPO     | Delta        |
| ------------- | -------- | ------- | ------------ |
| top1_accuracy | 0.34404  | 0.34203 | **−0.201pp** |
| place2_acc    | 0.55890  | 0.55807 | −0.083pp     |
| place3_acc    | 0.43173  | 0.43307 | +0.134pp     |
| top3_box_acc  | 0.09237  | 0.09254 | +0.017pp     |

### Bootstrap LB95 (10k race-resample, seed=42, HPO model)

| Axis     | LB95    |
| -------- | ------- |
| top1     | 0.33199 |
| place2   | 0.54769 |
| place3   | 0.42269 |
| top3_box | 0.08651 |

Note: LB95 values are **absolute** (bootstrapped lower bound on HPO model performance),
not deltas. The HPO deltas are small and within noise.

### Adopt Gate Result: **REJECT**

| Gate Criterion                     | Result                   |
| ---------------------------------- | ------------------------ |
| n_axes_positive ≥ 2                | YES (2)                  |
| place2_or_place3_positive          | YES                      |
| no_axis_regression > −0.05pp       | **FAIL** (top1=−0.201pp) |
| any_lb95_positive (absolute sense) | YES                      |
| **ADOPT-WORTHY**                   | **NO**                   |

The HPO-tuned config (depth=9, lr=0.019, iter=200) shows place3 +0.134pp and top3_box +0.017pp
but top1 −0.201pp exceeds the −0.05pp no-regression threshold. The default hyperparams
(iter=300, depth=8, lr=0.05, l2=3.0) are **retained**.

**Interpretation:** The default config is already well-tuned for Ban-ei. The HPO found a slightly
deeper tree with lower lr, but the top1 regression suggests the tuning-split years (2021-2022) are
not a fully representative proxy for the 2023-2026 holdout. HPO headroom at current feature set
is negligible.

---

## Task 4 — Futan-Ratio Probe (H-BANEI-BATAIJU-FUTAN-RATIO)

### Hypothesis

Ban-ei JES 12_1_1: body mass relative to sled load determines physical efficiency. The ratio
`bataiju_kg / futan_kg` (higher = lighter relative to load = more effort), centered within
race, should carry predictive information BEYOND the existing `current_futan_class` (which
encodes only the load bucket) and `self_futan_minus_field_avg` (which encodes relative load
rank within race).

### Data

| Item                 | Value                                    |
| -------------------- | ---------------------------------------- |
| PG rows pulled       | 259,312                                  |
| After quality filter | 81,501 (15,886 races)                    |
| Years covered        | 2010-2026                                |
| futan_kg range       | parsed from hex (e.g. 0x21C=540 → 540kg) |
| bataiju range        | integer kg (draft horses ~700-1000kg)    |

**futan_kg decoding note:** nvd_se.futan_juryo is a 3-char hex string (e.g. "21C"=540kg, "26C"=620kg).
`try_cast('0x'||trim(futan_juryo) as integer)` gives the actual load in kg.
The old `add-ban-ei-raw-features.py` silently produced NULLs here (decimal cast of hex string).

### Results

| Metric         | Value                                            |
| -------------- | ------------------------------------------------ |
| Raw Spearman ρ | −0.02813 (p=9.5e-16, statistically sig but tiny) |
| Partial ρ      | −0.03034 (p=4.8e-18)                             |
| Bar            | 0.08                                             |
| n_merged_rows  | 81,389                                           |
| **Verdict**    | **ABORT**                                        |

Signal direction is correct (negative ρ: heavier relative to load → better finish), but the
effect size is extremely small (|ρ|=0.030 vs bar=0.08). The existing `current_futan_class`
and `self_futan_minus_field_avg` features already capture most of the load-class information,
and the body-mass dimension adds only marginal incremental signal.

**Possible reasons for weak signal:**

- Ban-ei horse body weight is remarkably stable across starts (same horse, same weight season to season)
- The futan_class bucket already captures most of the variance (horses are assigned to loads by their
  performance history, so heavier horses tend to be in heavier-load races naturally)
- The ratio is confounded by horse age/development (younger = lighter, but also less experienced)

---

## Headroom Assessment

Ban-ei has the largest untapped headroom in the prediction pipeline, ranked by expected impact:

1. **SECTIONAL features (checkpoint timing)** — Ban-ei uniquely records timing at each obstacle
   (障害 checkpoints). Horses that stall vs power through checkpoints have distinct finishing patterns.
   This layer has NO equivalent in JRA/NAR and could yield substantial signal (est. 0.5-2pp top1).

2. **RACE_INTERNAL features** — relative position within field (rank-scaled features) are completely
   absent from Ban-ei. Adding within-race speed rank, futan rank, pedigree rank could improve
   discriminability (est. 0.2-0.5pp top1).

3. **TRAINER features** — stable and coach affinity with track/distance/class. Ban-ei has a small
   fixed set of trainers at Obihiro; their specialization patterns should be predictable
   (est. 0.1-0.3pp top1).

4. **NEAR_MISS features** — close margin to 3rd across past starts. Small but consistent signal
   in JRA/NAR (est. 0.05-0.15pp).

5. **RELATIONSHIP features** — jockey-horse pair win rates, jockey-trainer pair. Already present
   in JRA/NAR v8 (est. 0.05-0.15pp for Ban-ei).

6. **PACESTYLE** — no running-style model exists for Ban-ei. Given the checkpoint structure,
   a Ban-ei-specific "lead at obstacle 1" feature could proxy running style
   (est. 0.05-0.1pp with substantial engineering effort).

---

## Recommendations

**Immediate next step: build RACE_INTERNAL + SECTIONAL layers for Ban-ei.**

Priority script path:

1. `add-ban-ei-internal-features.py` already exists — verify what it produces (check if it generates
   any of the RACE_INTERNAL columns).
2. Pull checkpoint timing from `pg.nvd_se` columns (`corner_1`, `corner_3`, `corner_4`),
   compute rank and diff features within race.
3. Add within-race futan rank (distinct from futan bucket, already captured) and pedigree-score
   rank features (already present in JRA as `pedigree_score_for_race_rank_in_race`).
4. Retrain with these 15-25 new features (est. 122-136 total) using default hyperparams
   (HPO is confirmed near-optimal).
5. Evaluate on holdout 2023-2026 against this baseline table.

**Do NOT pursue:**

- HPO re-runs (confirmed near-optimal at current feature set)
- bataiju/futan ratio signal (partial ρ=0.030, well below bar=0.08)
- Stacking / ensemble (v7-lineage saturation finding applies here too)

---

## Commit Info

Generated by Ban-ei baseline/HPO agent, 2026-06-11.
Feature parquet cached at `tmp/banei/features/` (2007-2026, 20 partitions).
All computation bounded at ≤4 threads, peak RSS ~2.9GB.
