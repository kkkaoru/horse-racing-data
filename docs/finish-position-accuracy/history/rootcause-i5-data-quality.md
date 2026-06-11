# I5 Root-Cause: Data-Quality and Label Defects

**Date**: 2026-06-11
**Status**: COMPLETE — 3 fixable defects identified, 1 structural gap confirmed, 4 negligible/correctly-handled

## TL;DR

Audited 5 categories of data-quality defects across NAR / JRA / Ban-ei. Measured real row counts from the production PostgreSQL database and feature parquets. Three issues are fixable and worth retraining; the rest are either correctly handled already, structural, or negligible.

| ID   | Defect                                              | Category | Affected rows / races      | Est. pp impact            | Worth retrain?        |
| ---- | --------------------------------------------------- | -------- | -------------------------- | ------------------------- | --------------------- |
| B001 | Ban-ei '00' DQ rows leak into training              | Label    | 3 827 rows (2.35%)         | ~0.3 pp Ban-ei            | YES                   |
| F001 | NAR 2yo/3yo age races fall to 'other' bucket        | Routing  | 48 506 races (19.8%)       | ~0.15 pp NAR              | YES                   |
| C001 | JRA feat-v20-merged-v5 contaminated 33% non-central | Coverage | 92 532 rows (32.9%)        | Low (per-class eval bias) | YES (rebuild parquet) |
| R002 | JRA 701 structural accuracy gap vs peer classes     | Routing  | 12 372 races, 953 holdout  | -3.67 pp below 703        | NO (signal needed)    |
| R001 | NAR class B has no ensemble (correctly rejected)    | Routing  | 7 124 holdout races        | -0.014 pp (worse)         | NO                    |
| D001 | Dead-heat 1st-place ties                            | Label    | 363 NAR, 0 JRA, 193 Ban-ei | ~0.062 pp NAR             | NO                    |
| C002 | JRA tansho_odds 35% null (non-central rows)         | Coverage | structural                 | 0 (production clean)      | NO                    |
| C003 | JRA kohan3f_avg_5 27% null (newcomers)              | Coverage | structural                 | 0                         | NO                    |

---

## 1. Ban-ei '00' DQ/non-finisher Leak (B001) — HIGH

### Evidence

`nvd_se.kakutei_chakujun = '00'` encodes non-finishers / DQs in the NAR/Ban-ei feed. The historical Ban-ei feature builder at `finish_position_features_duckdb.py:391` uses a **single** `nullif`:

```sql
try_cast(nullif(trim(se.kakutei_chakujun), '') as int) as finish_position
```

`'00'` is not the empty string, so `nullif(..., '')` passes it through unchanged. `try_cast('00' as int)` = **0** (confirmed in PG: `SELECT '00'::integer` = 0). finish_position=0 is **not NULL**, so the `finish_position IS NOT NULL` guard in `split_train_valid` lets these rows into training.

The upcoming/inference builder at `:467` correctly uses double nullif:

```sql
try_cast(nullif(nullif(trim(se.kakutei_chakujun), ''), '00') as int) as finish_position
```

### Counts

| Source                 | '00' rows | Total labeled rows | Pct   |
| ---------------------- | --------- | ------------------ | ----- |
| nvd_se                 | 6 176     | 324 491            | 1.90% |
| feat-ban-ei-v3 parquet | 3 827     | 162 970            | 2.35% |

2025 rate: 3.69%, 2026 YTD: 6.81% — rising, possibly more retirements in the dataset window.

### Impact Mechanism

- `to_relevance(0)` = `DEFAULT_RELEVANCE = 0` (key 0 not in `{1:3, 2:2, 3:1}`). Treated as "last place" competitor in training.
- career_win_rate and similar aggregates use `AVG(CASE WHEN finish_position = 1 ...)` — the 0-position rows add to the denominator but not the numerator, slightly **diluting** win rates for horses with prior '00' runs.
- The asymmetry between training (finish_position=0) and inference (finish_position=NULL, row excluded) creates a train/inference distribution divergence for Ban-ei.

### Fix

Change line 391 in `_rec_select_banei_historical()` to double nullif, rebuild Ban-ei parquet, retrain Ban-ei model. One-line change identical to the inference path.

---

## 2. NAR 2yo/3yo Age-Restricted Races in 'other' Bucket (F001) — MEDIUM

### Evidence

`nar_subclass_case_sql` (lines 1551–1562) matches on fullwidth katakana letters: `Ａ`, `Ｂ`, `Ｃ`, `ＯＰ`, plus kanji `新馬`/`未勝利`/`未出走`. Races for 2-year-olds (`２歳`) and 3-year-olds (`３歳`) carry no class letter in the meisho field and fall to `else 'other'`.

```
kyoso_joken_meisho examples in 'other':
  "３歳　　　　　　　　　" → 4 025 races
  "２歳　　　　　　　　　" → 3 471 races
  "３歳　　　－３　　　　" → 1 503 races
  ... (age-graded variants)
```

### Counts

| subclass  | races      | % of NAR (non-Ban-ei) |
| --------- | ---------- | --------------------- |
| C         | 159 478    | 54.4%                 |
| **other** | **58 236** | **19.8%**             |
| B         | 44 083     | 15.0%                 |
| A         | 17 403     | 5.9%                  |
| OP        | 6 966      | 2.4%                  |
| MUKATSU   | 3 792      | 1.3%                  |
| NEW       | 3 348      | 1.1%                  |

Within the 58 236 'other' races: 2yo = 14 636 (25%), 3yo = 33 870 (58%), misc = 9 730 (17%). The 'other' bucket is dominated by age-restricted races.

### Current Handling

The `other` bucket is activated in per-class routing (`iter30-nar-cb-ensemble-other-v8`, holdout delta = +0.152pp vs baseline). Age-restricted races are receiving a per-class model, but all three sub-types (2yo, 3yo, misc) are blended together. The 2yo/3yo horse population has structurally sparser career history, which may dilute the ensemble's signal.

### Fix

Add regex arms before the existing `Ａ`/`Ｂ`/`Ｃ` checks:

```python
when regexp_matches({meisho_col}, '２歳|2歳') then '2YO'
when regexp_matches({meisho_col}, '３歳|3歳') then '3YO'
```

Then run per-class ensemble optimization for `2YO` and `3YO` sub-classes. Estimated uplift: moderate (these races are currently in the +0.152pp ensemble but lumped together).

---

## 3. JRA feat-v20-merged-v5 Non-Central Venue Contamination (C001) — INFORMATIONAL

### Evidence

The fix commit `16c1ea3` ("exclude NAR/overseas venues from JRA finish-position target rows", 2026-06-03) added the `JRA_KEIBAJO_CODES` filter to `build_target_table`. The `feat-v20-merged-v5` parquet was built on 2026-05-19 — 15 days before the fix.

```
feat-v20-merged-v5/jra/ (2022–2025 labeled rows):
  jra_central (01–10):  188 364 rows (67.1%)
  non_central (30–58):   92 532 rows (32.9%) ← all have tansho_odds_raw = NULL
```

Non-central rows (NAR venues in JV feed, stamped `source='jra'`) have:

- `tansho_odds_raw = NULL` → 100%
- `popularity_rank_in_race = NULL` → 100%
- `inverse_odds_implied_prob` = imputed median (non-null but wrong)

### Impact on Production Models

**iter14-jra-cb-pacestyle-course-v8** was trained on `feat-jra-v8-iter14-course` (not v20-merged-v5). That parquet is 100% central (confirmed: zero non-central rows for 2022–2025). **Production accuracy is NOT affected.**

The contaminated parquet was used for per-class ensemble optimization (holdout eval). Per-class summaries reference holdout race counts computed over this parquet. The contamination could slightly bias holdout race selection / class-code filtering, but the summaries showed 700–7000 race holdouts per class which is plausible for central-only scope.

### Fix

Rebuild `feat-v20-merged` equivalent using the current script (filter already in code), then re-run per-class optimization. Needed before any new JRA per-class iteration.

---

## 4. JRA 701 (OP/Listed) Structural Accuracy Gap (R002) — MEDIUM / NOT FIXABLE BY RETRAIN

### Evidence

JRA `kyoso_joken_code = '701'` races (12 372 races in JRA; 654 recent 2024 races in production predictions) cover OP / listed races. The ensemble optimization search (`per-class-ensemble-v4/701/summary.json`) tested 6 per-class iterations with 200 Optuna trials:

```json
{
  "decision": "reject",
  "delta_pp": -1.574,
  "iter14_holdout_top1": 0.4575,
  "holdout_top1": 0.4418,
  "holdout_races": 953
}
```

All per-class iterations scored worse than the iter14 baseline. Pairwise correlations among all 6 members are 0.97–0.99 — no new signal.

### Accuracy Comparison Across JRA Classes (v4 holdout)

| class   | baseline top1 | ensemble top1 | delta_pp   | decision   |
| ------- | ------------- | ------------- | ---------- | ---------- |
| 703     | 0.4942        | 0.4961        | +0.189     | accept     |
| 005     | 0.4128        | 0.4185        | +0.572     | accept     |
| 010     | 0.4359        | 0.4378        | +0.190     | accept     |
| 016     | 0.3824        | 0.3838        | +0.138     | accept     |
| other   | 0.4182        | 0.4192        | +0.094     | accept     |
| **701** | **0.4575**    | **0.4418**    | **-1.574** | **reject** |

701's baseline top1 (0.4575) already sits between 703 (0.4942) and 005 (0.4128). The structural gap vs 703 is -3.67pp and is unaddressed by any existing per-class candidate.

### Root Cause Hypothesis

OP/listed races have condensed high-quality fields. Jockey allocation, trainer-jockey pairing, and stable scheduling patterns (preferred jockey booking for special races) provide signal not captured in current features. Requires 701-specific engineered features.

---

## 5. NAR Class B — No Ensemble (Correctly Rejected) (R001) — LOW

`per-class-ensemble-nar/B/summary.json`:

```json
{
  "decision": "reject",
  "delta_pp": -0.014,
  "holdout_top1": 0.5809,
  "baseline_holdout_top1": 0.581,
  "holdout_races": 7124,
  "pairwise_correlations": { "iter12__iter30_B": 0.981 }
}
```

The iter30 CatBoost residual for B adds no discriminative signal (correlation 0.98). Falls back correctly to iter12 global. No fix needed.

---

## 6. Dead-Heat Labels (D001) — NEGLIGIBLE

Dead-heat ties (multiple horses with same `kakutei_chakujun`) receive identical relevance labels, providing zero within-tie ordering signal to lambdaRank.

| Category | 1st-place ties | Total races           | Impact rate |
| -------- | -------------- | --------------------- | ----------- |
| NAR      | 363            | 291 208               | 0.125%      |
| JRA      | 0              | 138 835               | 0%          |
| Ban-ei   | 193            | 35 148 (valid finish) | 0.55%       |

For NAR: 363 forced-coin-flip misses × 0.5 / 291 208 ≈ **0.062 pp** impact on top1. Negligible.

JRA has zero first-place ties in the JV data (ties encoded differently). Ban-ei has 0.55% — minor. Not worth implementing special handling.

---

## 7. Feature Coverage — Structural (C002, C003) — INFORMATIONAL

**C002 — JRA tansho_odds 35% null**: Caused entirely by non-central NAR venues (C001 above). JRA central venues have 0% null in source. Production iter14 is trained on clean data.

**C003 — JRA kohan3f_avg_5 27% null**: Structural — requires 5 prior races with kohan3f. JRA source has 100% kohan3f populated for finished races; nulls = newcomer/import without sufficient race history. LightGBM handles NaN natively. Not a bug.

**NAR futan_juryo 100% null in feat-v20-merged-v5**: NAR does not publish futan_juryo through the NV-DATA feed. The column exists but is never populated for NAR. Ban-ei uses hex-encoded futan_juryo correctly. Not a defect.

**Running style (past_corner_1_norm_avg_5) 37–55% null by track type**: Corner features are not populated for inner/jump track variants (keibajo 11=内ダート, 18=内芝, 17=障害). These races genuinely lack corner passage data. Model uses `nulls last` ordering for rank features. Not a bug.

---

## Action Plan

### Fix + Rebuild + Retrain (worth doing now)

1. **B001** (Ban-ei '00' leak): 1-line fix in `finish_position_features_duckdb.py:391`. Rebuild `feat-ban-ei-v3` parquet → retrain Ban-ei model. Expected gain: ~0.3 pp top1.

2. **F001** (NAR 2yo/3yo): Add `2YO`/`3YO` regex arms to `nar_subclass_case_sql`. Rebuild NAR parquet → run per-class ensemble optimization for the two new sub-classes. Expected gain: moderate (currently inside the +0.152pp `other` ensemble but diluted).

3. **C001** (JRA parquet contamination): Rebuild `feat-v20-merged` equivalent with current script → re-run JRA per-class optimization. This is prerequisite for valid JRA per-class holdout reporting.

### No Retrain Warranted

- **R002** (JRA 701): Requires novel feature engineering (jockey-booking, high-grade sire interaction). File as future work.
- **R001, D001, C002, C003**: Correctly handled or too small to justify rebuild cost.

---

## Artifacts

- `tmp/rootcause/i5_dataquality.json` — machine-readable defect registry with exact row counts and impact estimates
- `tmp/per-class-ensemble-nar/B/summary.json` — NAR B rejection evidence
- `tmp/per-class-ensemble-v4/701/summary.json` — JRA 701 rejection evidence
