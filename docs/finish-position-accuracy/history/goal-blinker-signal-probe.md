# Blinker Signal Probe — JRA + NAR

**Date:** 2026-06-17  
**Scope:** Leak-free blinker features (first-time, re-application, removal) as finish-position signals  
**Verdict:** **ABORT (JRA). NAR: data unavailable.**

---

## 1. Data Availability

### JRA (`jvd_se.blinker_shiyo_kubun`)

- Field: `character varying(1)`, values `'0'` (no blinker) / `'1'` (blinker on)
- Total rows in joined analysis (jvd_se + race_entry_corner_features, JRA keibajo_code 01–10, finished races): **1,838,638**
- Blinker-on rows overall: **143,339** (4.97% of all JRA race starts)

### NAR (`nvd_se.blinker_shiyo_kubun`)

- Field exists, same schema — but **100% of 3,273,490 rows = '0'**
- NAR does **not** record blinker usage. Analysis is JRA-only.

---

## 2. Category Definitions (leak-free, using window functions over race history)

All features use `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING` — current race blinker is declared pre-race at 出馬表, so no leak.

| Category               | Definition                                                                     | n         |
| ---------------------- | ------------------------------------------------------------------------------ | --------- |
| `first_blinker`        | current=1, no prior race with blinker=1                                        | 31,502    |
| `true_reapplication`   | current=1, prev race=0, but had blinker=1 at some earlier date (on→off→**ON**) | 6,906     |
| `blinker_continuation` | current=1, prev race=1                                                         | 103,251   |
| `true_removal`         | current=0, prev race=1                                                         | 21,076    |
| `blinker_off`          | current=0, prev=0 or first race                                                | 1,675,903 |

**Note on prior "reapplication" (105,896):** The original 105k figure was a misclassification — it captured all horses that _ever_ had blinker=1 somewhere in history and also blinker=0 somewhere, regardless of ordering relative to current race. The true on→off→on transition is 6,906.

---

## 3. Subgroup vs Market Expectation (JRA)

Market baseline: win/place-3 rates by popularity bucket (1–18), computed from `blinker_off` population (n=1,675,903). Expected rates are computed by applying these bucket rates to each subgroup's popularity distribution.

### 3.1 First Blinker (n=31,502, avg popularity=8.5)

| Metric       | Actual | Expected | Lift        | 95% CI            |
| ------------ | ------ | -------- | ----------- | ----------------- |
| Win rate     | 5.12%  | 5.33%    | **−0.21pp** | [−0.45, +0.04] pp |
| Place-3 rate | 16.15% | 17.69%   | **−1.53pp** | [−1.93, −1.09] pp |

- Holdout 2023+ (n=4,106): win_lift=−0.32pp, p3_lift=−1.20pp

**Interpretation:** First-blinker horses underperform their market-implied expectation. The place-3 lift CI is entirely negative — there is no exploitable positive signal; the market correctly prices (or slightly overprices) first blinker horses.

### 3.2 True Reapplication (n=6,906, avg popularity=8.9)

| Metric       | Actual | Expected | Lift        | 95% CI            |
| ------------ | ------ | -------- | ----------- | ----------------- |
| Win rate     | 5.02%  | 5.00%    | **+0.02pp** | [−0.44, +0.55] pp |
| Place-3 rate | 17.17% | 16.66%   | **+0.51pp** | [−0.39, +1.44] pp |

- Holdout 2023+ (n=955): win_lift=−0.26pp, p3_lift=−0.74pp

**Interpretation:** Reapplication shows essentially zero lift. CI spans 0 in both directions. Holdout is negative. Coverage is thin (6,906 total, 955 holdout) — insufficient for reliable signal detection even if effect existed.

### 3.3 True Removal (n=21,076, avg popularity=9.1)

| Metric       | Actual | Expected | Lift        | 95% CI            |
| ------------ | ------ | -------- | ----------- | ----------------- |
| Win rate     | 4.40%  | 4.55%    | **−0.15pp** | [−0.42, +0.13] pp |
| Place-3 rate | 15.10% | 15.58%   | **−0.47pp** | [−0.97, +0.03] pp |

- Holdout 2023+ (n=2,486): win_lift=−0.14pp, p3_lift=−1.04pp

**Interpretation:** Blinker removal also underperforms. Market prices it slightly too high (or these horses are lower quality going through this transition). No positive signal.

### 3.4 Blinker Continuation (n=103,251, avg popularity=7.7)

| Metric       | Actual | Expected | Lift        | 95% CI            |
| ------------ | ------ | -------- | ----------- | ----------------- |
| Win rate     | 6.73%  | 7.08%    | **−0.35pp** | [−0.50, −0.19] pp |
| Place-3 rate | 21.48% | 21.64%   | **−0.16pp** | [−0.42, +0.11] pp |

- Holdout 2023+ (n=14,269): win_lift=−0.03pp, p3_lift=−0.01pp (converges to zero long-run)

---

## 4. Partial Spearman ρ vs finish_norm (controlling for popularity)

Computed on random sample of 200,000 rows; residualized on rank(popularity). Sign: negative = better finish.

| Feature              | Full (rho) | 2023+ holdout (rho) | vs threshold (≥0.08) |
| -------------------- | ---------- | ------------------- | -------------------- |
| `first_blinker`      | −0.0380    | −0.0334             | **BELOW**            |
| `true_reapplication` | −0.0447    | −0.0434             | **BELOW**            |
| `true_removal`       | −0.0436    | −0.0406             | **BELOW**            |
| `blinker_on` (any)   | −0.0319    | —                   | **BELOW**            |
| `any_change`         | −0.0354    | —                   | **BELOW**            |

All features clear |ρ| < 0.08 in both full and holdout windows. The dilution is expected (blinker changes affect ~3–5% of starts), but even the directional signal is weak and does not pass the necessary-condition threshold.

---

## 5. Signal vs Rarity Assessment

| Feature            | Coverage (n)  | Subgroup lift           | ρ      | Assessment                      |
| ------------------ | ------------- | ----------------------- | ------ | ------------------------------- |
| first_blinker      | 31,502 (1.7%) | −0.21pp win, −1.53pp p3 | −0.038 | Negative signal, market correct |
| true_reapplication | 6,906 (0.4%)  | +0.02pp win, +0.51pp p3 | −0.045 | Noise, thin coverage            |
| true_removal       | 21,076 (1.1%) | −0.15pp win, −0.47pp p3 | −0.044 | Slightly negative               |

The signals are not "rare-but-strong" — they are rare AND directionally flat-to-negative.

---

## 6. Verdict

**JRA: ABORT**

- Neither PROCEED criterion met:
  - Subgroup lift: all CIs include 0 or are negative; no robust market-orthogonal outperformance
  - Partial ρ: max |ρ| = 0.045, well below 0.08 threshold in both full and holdout windows
- The market appears to correctly price blinker changes. First-blinker horses actually run slightly below their market expectation (consistent with market incorporating the blinker as a performance signal by giving longer odds).
- Adding blinker features would not be information-free: they correlate slightly with WORSE outcomes (market already prices it), so they would provide marginal at best, and risk GBDT over-indexing on a noisy feature.

**NAR: N/A (no data — all `blinker_shiyo_kubun='0'`)**

---

## 7. Method Notes

- Join: `jvd_se` × `race_entry_corner_features` on (ketto_toroku_bango, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
- Excluded: kakutei_chakujun='00' (scratch/DNS), finish_position=NULL
- Market baseline: blinker_off population only (n=1,675,903), bucketed by tansho_ninkijun (popularity rank)
- Bootstrap: 2000 iterations, seed=42
- Partial ρ: Frisch-Waugh residualization of ranks, one control (popularity)
- DuckDB not used (direct PG query via psycopg2); analysis script at `/tmp/blinker_refined.py` (not git-tracked)
