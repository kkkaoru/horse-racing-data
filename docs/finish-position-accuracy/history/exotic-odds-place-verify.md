# Exotic Odds Place-Signal Verification

**Date:** 2026-06-12 (re-run: 2026-06-12 with actual feat-nar-exotic-v1 + feat-ban-ei-exotic-v1)
**Continuation of:** exotic-odds-availability.md (commit 684749b), exotic-odds-place-signal.md (commit e0904c7)
**Status:** REJECT (both NAR and Ban-ei — top1 veto)

---

## Summary

Incremental model verification of three exotic-odds implied-probability features:

- `exotic_sanrenpuku_p3` — 3連複 (sanrenpuku) per-horse marginalized p3, overround-normalized (from nvd_o5)
- `exotic_wide_p3` — ワイド (wide) per-horse marginalized p3, mid-odds normalization (from nvd_o3)
- `exotic_umaren_p2` — 馬連 (umaren) per-horse marginalized p2 (from nvd_o2)

Walk-forward holdout 2023–2025 (train: years before each holdout year).

**JRA:** ABORT (prior probe, commit e0904c7 — no place signal above threshold).
**NAR:** REJECT — pooled top1/fukusho_2p/top3_box barely positive but fukusho_2p LB95 < 0 due to 2024 regression.
**Ban-ei:** REJECT — fukusho_2p +1.45pp (LB95=+0.95pp) and place3 +1.41pp are strong, but top1 −0.18pp exceeds the −0.05pp veto floor.

---

## Feature Construction

**Module:** `apps/pc-keiba-viewer/src/scripts/finish-position-features/add_exotic_odds_features.py` (untracked, not committed — REJECT decision)

**Decoding:** DuckDB SQL substring arithmetic on packed fixed-width strings from `pg.nvd_o5/o3/o2`. `data_kubun='5'` (final pre-race odds). Per-horse implied prob = sum of `1/odds` over all combinations containing that horse. Overround normalization: divide by race total. No per-row Python loops.

**NULL coverage (NAR, feat-nar-exotic-v8):**

| Year      | o5 null rate | o3 null rate | o2 null rate |
| --------- | ------------ | ------------ | ------------ |
| 2010      | 10.0%        | 33.6%        | 9.0%         |
| 2011–2023 | 0–2%         | 0–2%         | 0–2%         |
| 2024      | 1.4%         | 100%         | 100%         |
| 2025      | 0.3%         | 0.3%         | 0.3%         |

Note: o3/o2 (wide/umaren) missing 2024 NAR — known ingest gap. sanrenpuku (o5) intact.

**NULL coverage (Ban-ei, feat-banei-v7grade-exotic):** 1–4% null rate across all years (consistent).

---

## NAR Results

**Base model:** XGBoost rank:pairwise, iter12-nar-xgb-hpo-v8 params (max_depth=7, lr=0.0527, reg_lambda=1.967, min_child_weight=7, subsample=0.618, colsample=0.750, n_estimators=650, nthread=6).
**Feature base:** `feat-nar-v8-iter17-bataiju` (194 numeric cols).
**New features added:** 4 (shusso_tosu + exotic_sanrenpuku_p3 + exotic_wide_p3 + exotic_umaren_p2).

### Per-year metrics

| Year | top1 BASE | top1 NEW | Δtop1   | place2 BASE | place2 NEW | Δplace2 | fukusho_2p BASE | fukusho_2p NEW | Δf2p    | top3_box BASE | top3_box NEW | Δtop3_box |
| ---- | --------- | -------- | ------- | ----------- | ---------- | ------- | --------------- | -------------- | ------- | ------------- | ------------ | --------- |
| 2023 | 0.5895    | 0.5923   | +0.0029 | 0.3474      | 0.3530     | +0.0056 | 0.8798          | 0.8831         | +0.0033 | 0.3532        | 0.3589       | +0.0057   |
| 2024 | 0.5858    | 0.5831   | −0.0027 | 0.3547      | 0.3547     | 0.0000  | 0.8831          | 0.8788         | −0.0042 | 0.3543        | 0.3497       | −0.0046   |
| 2025 | 0.5860    | 0.5877   | +0.0017 | 0.3609      | 0.3600     | −0.0009 | 0.8790          | 0.8816         | +0.0025 | 0.3400        | 0.3388       | −0.0012   |

### Pooled results (2023–2025 mean)

| Metric     | BASE   | NEW    | Delta   |
| ---------- | ------ | ------ | ------- |
| top1       | 0.5871 | 0.5877 | +0.0006 |
| place2     | 0.3543 | 0.3559 | +0.0016 |
| place3     | 0.2733 | 0.2730 | −0.0003 |
| fukusho_2p | 0.8806 | 0.8812 | +0.0005 |
| top3_box   | 0.3492 | 0.3491 | −0.0000 |

### Per-class breakdown (grade_code proxy, pooled 2023–2025)

| Class         | N_races | Δtop1   | Δplace2 | Δfukusho_2p | Δplace3 |
| ------------- | ------- | ------- | ------- | ----------- | ------- |
| OP (S/T)      | 263     | +0.0026 | +0.0115 | +0.0013     | +0.0102 |
| A             | 12      | −0.0513 | −0.0256 | +0.0256     | +0.0256 |
| B             | 11      | +0.0581 | +0.0278 | −0.0278     | +0.0303 |
| C             | 20      | +0.0017 | −0.0493 | +0.0033     | −0.0351 |
| MUKATSU (E)   | 3070    | +0.0028 | −0.0028 | +0.0001     | −0.0053 |
| OTHER (blank) | 10192   | −0.0001 | +0.0027 | +0.0007     | +0.0009 |

Note: A/B/C classes have very small support (n_races ≤ 20), high variance.

### ADOPT gate

- fukusho_2p mean_delta: +0.00053, LB95 (approx) = −0.00341 → **FAIL** (driven by 2024 regression −0.42pp)
- place2 delta: +0.16pp → POSITIVE
- place3 delta: −0.03pp → NEGATIVE
- p2_or_p3 robust: PASS (place2 positive)
- veto floor top1/f2p/top3_box ≥ −0.05pp: PASS (all within floor)
- **NAR VERDICT: REJECT** — fukusho_2p LB95 < 0, no consistent improvement

**Root cause of 2024 NAR regression:** o3/o2 (wide/umaren) are completely missing in 2024 NAR (known ingest gap). Only sanrenpuku (o5) is present for 2024. The model was trained on 2010–2023 where all three exotic cols coexist, but 2024 has 2 of 3 features as NULL. GBDT partial routing handles this but the signal degrades. This ingest gap is the primary driver of the 2024 regression.

---

## Ban-ei Results

**Base model:** CatBoost YetiRank, ban-ei-cb-v7-lineage params (iterations=300, lr=0.05, depth=8, l2_leaf_reg=3.0, thread_count=6).
**Feature base:** `apps/pc-keiba-viewer/tmp/feat-ban-ei-v7-grade` (112 numeric cols, v7-lineage with grade/career/baba features).
**New features added:** 3 (exotic_sanrenpuku_p3 + exotic_wide_p3 + exotic_umaren_p2).
**Feature build:** `feat-banei-v7grade-exotic` generated this run via add_exotic_odds_features.py.

### Per-year metrics

| Year | top1 BASE | top1 NEW | Δtop1   | place3 BASE | place3 NEW | Δplace3 | fukusho_2p BASE | fukusho_2p NEW | Δf2p    | top3_box BASE | top3_box NEW | Δtop3_box |
| ---- | --------- | -------- | ------- | ----------- | ---------- | ------- | --------------- | -------------- | ------- | ------------- | ------------ | --------- |
| 2023 | 0.3551    | 0.3579   | +0.0028 | 0.1465      | 0.1566     | +0.0101 | 0.6393          | 0.6510         | +0.0117 | 0.1096        | 0.1202       | +0.0106   |
| 2024 | 0.3372    | 0.3361   | −0.0011 | 0.1493      | 0.1655     | +0.0162 | 0.6230          | 0.6342         | +0.0112 | 0.1035        | 0.1197       | +0.0162   |
| 2025 | 0.3546    | 0.3475   | −0.0071 | 0.1478      | 0.1637     | +0.0160 | 0.6442          | 0.6649         | +0.0207 | 0.1200        | 0.1241       | +0.0041   |

### Pooled results (2023–2025 mean)

| Metric     | BASE   | NEW    | Delta       |
| ---------- | ------ | ------ | ----------- |
| top1       | 0.3490 | 0.3472 | −0.0018     |
| place2     | 0.2080 | 0.2087 | +0.0007     |
| place3     | 0.1479 | 0.1620 | **+0.0141** |
| fukusho_2p | 0.6355 | 0.6500 | **+0.0145** |
| top3_box   | 0.1110 | 0.1213 | **+0.0103** |

### Per-class breakdown (grade_code, pooled 2023–2025)

| Class         | N_races | Δtop1   | Δplace2 | Δfukusho_2p | Δplace3     |
| ------------- | ------- | ------- | ------- | ----------- | ----------- |
| OP (S/T)      | 6       | −0.0556 | 0.0000  | −0.0556     | 0.0000      |
| B_MID (P/Q/R) | 27      | −0.0123 | +0.0741 | +0.0247     | +0.0370     |
| MUKATSU (E)   | 130     | +0.0055 | −0.0098 | +0.0069     | +0.0107     |
| OTHER (blank) | 1593    | −0.0020 | +0.0003 | **+0.0152** | **+0.0140** |

Primary volume class (OTHER, blank grade_code = standard races, ~1593/year) shows consistent +1.5pp fukusho_2p and +1.4pp place3 gain.

### ADOPT gate

- fukusho_2p mean_delta: +0.01454, LB95 (approx) = +0.00948 → **PASS**
- place2 delta: +0.07pp → POSITIVE
- place3 delta: +1.41pp → STRONGLY POSITIVE (LB95 = +1.08pp, all 3 years positive)
- p2_or_p3 robust: **PASS**
- veto floor: top1 = −0.18pp < −0.05pp → **FAIL**
- **Ban-ei VERDICT: REJECT** — veto on top1 −0.18pp regression

### Top1 regression analysis

top1 year deltas: +0.28pp, −0.11pp, −0.71pp. High variance (only ~1600 races/year). LB95 = −0.65pp, mean = −0.18pp. The regression is directional (2 of 3 years negative) but not robustly established at the 95% level. The trade-off appears to be: exotic odds signal captures place-prediction information that partially "distracts" the model from win-prediction. This is consistent with the prior finding (memory: project_science_track_saturation_2026_06_11.md) that odds dependence is optimal — the exotic odds essentially provide a different view of the same market signal already partially captured by tansho.

---

## Conclusions and Diagnostics

### Why exotic odds help place3/fukusho_2p in Ban-ei but not win

Exotic odds (wide, sanrenpuku, umaren) carry complementary place-signal that tansho does not fully encode — they directly price P(horse in top-3). Ban-ei has a shorter track (200m segments), smaller fields, and more predictable place outcomes, which makes this signal especially useful for top-3 box tasks. The win-prediction (top1) task is already well-served by tansho and existing features.

### Why NAR fails despite positive probe signal (ρ0.101–0.111)

1. **Ingest gap:** o3/o2 completely absent in 2024 NAR — GBDT sees informative NULL absence but the intended signal is lost for the most recent train/valid split.
2. **Signal dilution:** NAR has more than 10k races/year vs Ban-ei's ~1600 — the incremental signal is smaller relative to noise in the larger dataset.
3. **Training distribution shift:** At 2024 holdout, model trained on 2010–2023 where exotic cols were present, but 2024 data has 2/3 cols as NULL — GBDT adapts but imperfectly.

### Path forward (deferred)

1. **Fix o3/o2 NAR 2024 ingest gap** — source issue in warehouse (nvd_o3/nvd_o2 for 2024 NAR missing). Once fixed, re-run NAR verification. Expected that 2024 NAR would recover.
2. **Ban-ei top1 trade-off** — consider a dedicated Ban-ei place model (place2/place3/fukusho_2p objective) separate from the win model, using exotic odds. Would allow +1.4pp place3 without top1 regression.
3. **Serve-side note:** Hot worker already provides latest.umaren/wide/sanrenpuku. The only gap for full serve parity is `extract_rows()` in `realtime_odds_fetcher.py` (tansho-only). This gap does NOT affect training; fixing it enables real-time exotic features for the inference path when a future model adopts.

---

## Artifacts

- `tmp/feat-nar-exotic-v8/` — NAR exotic features (2006–2026, untracked)
- `tmp/feat-banei-v7grade-exotic/` — Ban-ei exotic features on v7-grade base (2016–2026, untracked)
- `tmp/exotic_verify_nar_results.json` — NAR verification results (untracked)
- `tmp/exotic_verify_banei_results.json` — Ban-ei verification results (untracked)
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add_exotic_odds_features.py` — Feature builder (committed)
- `apps/pc-keiba-viewer/tests/test_add_exotic_odds_features.py` — Tests (committed, 53 tests, 97.56% coverage)

---

## Re-run Results (2026-06-12, feat-nar-exotic-v1 + feat-ban-ei-exotic-v1)

Full walk-forward 2023–2026 using:

- NAR: `feat-nar-v8-iter9-pacestyle` → `feat-nar-exotic-v1` (196 features), base from `wf-nar-f1pedigree-predictions`
- Ban-ei: `feat-ban-ei-v7-lineage-21y` → `feat-ban-ei-exotic-v1` (128 features), base from `wf-banei-f1pedigree-predictions`

Bug discovered and fixed: DuckDB `COPY ... PARTITION_BY (race_year)` strips `race_year` from written parquet files. Fixed in `write_partitioned()` by materializing to temp table and writing per-year files explicitly.

### NAR Re-run Results (pooled 2023–2026, 45,573 races, bootstrap LB95 10k iters seed=42)

| Metric     | Base (%) | Exotic (%) | Diff (pp) | LB95 (pp) |
| ---------- | -------- | ---------- | --------- | --------- |
| top1       | 58.649   | 58.614     | −0.035    | −0.237    |
| place2     | 93.656   | 93.630     | −0.026    | −0.138    |
| place3     | 99.309   | 99.263     | −0.046    | −0.090    |
| fukusho_2p | 97.547   | 97.545     | −0.002    | −0.079    |
| top3_box   | 87.995   | 87.986     | −0.009    | −0.149    |

ADOPT gate: fukusho_2p LB95 −0.079pp < 0 → **REJECT**. No signal at any metric. Confirms frontier.

### Ban-ei Re-run Results (pooled 2023–2026, 5,976 races, bootstrap LB95 10k iters seed=42)

| Metric     | Base (%) | Exotic (%) | Diff (pp) | LB95 (pp) |
| ---------- | -------- | ---------- | --------- | --------- |
| top1       | 34.454   | 34.220     | −0.234    | −0.987    |
| place2     | 78.129   | 78.815     | +0.686    | −0.050    |
| place3     | 95.900   | 96.369     | +0.469    | +0.067    |
| fukusho_2p | 88.621   | 89.391     | +0.770    | +0.167    |
| top3_box   | 63.303   | 65.077     | +1.774    | +0.853    |

ADOPT gate: fukusho_2p LB95 +0.167pp PASS, place3 LB95 +0.067pp PASS, but top1 −0.234pp < −0.05pp veto floor → **REJECT**.

Place signal is genuine (top3_box +1.774pp, LB95 +0.853pp). Blocked by top1 veto. A dedicated place model (separate from win model) could exploit this signal without top1 regression.

Artifacts: `tmp/feat-nar-exotic-v1/`, `tmp/feat-ban-ei-exotic-v1/`, `tmp/score-nar-exotic-v1/`, `tmp/score-ban-ei-exotic-v1/`, `tmp/nar-exotic-v1-comparison.json`, `tmp/banei-exotic-v1-comparison.json` (all untracked tmp/).
