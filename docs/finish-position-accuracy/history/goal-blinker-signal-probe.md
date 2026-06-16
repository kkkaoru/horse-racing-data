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

## 5. Per-Class Subgroup Analysis (JRA)

Per-class market baseline computed separately within each class from the `blinker_off` population, bucketed by tansho_ninkijun. Bootstrap CI 95% (2000 iterations).

Class mapping from `kyoso_joken_code` / `grade_code`:

| Bucket    | Source codes                                                    |
| --------- | --------------------------------------------------------------- |
| 新馬      | kyoso_joken_code='703'                                          |
| 未勝利    | kyoso_joken_code IN ('701','702','005','004','006','007','008') |
| 1勝C      | kyoso_joken_code IN ('009','010')                               |
| 2勝C      | kyoso_joken_code IN ('016','015')                               |
| 3勝C      | kyoso_joken_code='014'                                          |
| OP/Listed | kyoso_joken_code='999', grade IN ('E','L',' ')                  |
| 重賞G3    | kyoso_joken_code='999', grade='C'                               |
| 重賞G2    | kyoso_joken_code='999', grade='B'                               |
| 重賞G1    | kyoso_joken_code='999', grade='A'                               |

### 5.1 first_blinker per class

| Class     | n      | avg_ninki | win_lift    | win_CI         | p3_lift     | p3_CI          | Significant?              |
| --------- | ------ | --------- | ----------- | -------------- | ----------- | -------------- | ------------------------- |
| 新馬      | 16,971 | 8.3       | −0.15pp     | [−0.48, +0.16] | **−1.26pp** | [−1.83, −0.68] | No (p3 CI fully negative) |
| 未勝利    | 10,204 | 8.6       | −0.28pp     | [−0.66, +0.15] | **−1.77pp** | [−2.44, −1.05] | No (p3 CI fully negative) |
| 1勝C      | 2,238  | 8.5       | +0.17pp     | [−0.77, +1.15] | −1.28pp     | [−2.84, +0.24] | No                        |
| 2勝C      | 778    | 9.3       | +1.36pp     | [−0.31, +3.03] | +0.03pp     | [−2.54, +2.60] | No (CI wide, includes 0)  |
| OP/Listed | 664    | 8.5       | **−2.07pp** | [−3.43, −0.57] | −2.80pp     | [−5.51, +0.06] | No (negative)             |
| 重賞G3    | 322    | 9.6       | +0.07pp     | [−2.11, +2.24] | −2.08pp     | [−5.49, +1.65] | No                        |
| 重賞G2    | 138    | 10.1      | −1.34pp     | [−3.51, +1.56] | −0.16pp     | [−5.24, +5.63] | No                        |
| 重賞G1    | 170    | 10.5      | +0.40pp     | [−1.96, +3.34] | +0.36pp     | [−4.34, +5.66] | No                        |

No class shows CI excluding 0 in the positive direction. The 2勝C cell (+1.36pp win) has a wide CI spanning 0 (n=778, p=0.09 approx). OP/Listed shows significantly negative lift.

### 5.2 true_reapplication per class

| Class     | n     | avg_ninki | win_lift | win_CI         | p3_lift | p3_CI           | Significant?       |
| --------- | ----- | --------- | -------- | -------------- | ------- | --------------- | ------------------ |
| 新馬      | 1,319 | 7.9       | +0.37pp  | [−0.92, +1.74] | +1.41pp | [−0.71, +3.61]  | No (CI includes 0) |
| 未勝利    | 3,392 | 8.9       | −0.16pp  | [−0.87, +0.57] | +0.28pp | [−1.04, +1.55]  | No                 |
| 1勝C      | 1,328 | 9.2       | +0.12pp  | [−0.94, +1.32] | +0.13pp | [−1.68, +2.09]  | No                 |
| 2勝C      | 399   | 9.5       | −0.95pp  | [−2.71, +0.80] | −0.83pp | [−4.34, +2.68]  | No                 |
| OP/Listed | 266   | 9.5       | +1.46pp  | [−1.17, +4.09] | −0.49pp | [−4.63, +4.02]  | No                 |
| 重賞G3    | 117   | 10.2      | −1.64pp  | [−4.20, +1.78] | −1.23pp | [−7.21, +4.75]  | No                 |
| 重賞G2    | 50    | 10.7      | +0.80pp  | [−3.20, +6.80] | +1.69pp | [−8.31, +11.69] | No (n too thin)    |
| 重賞G1    | 26    | 11.7      | −1.85pp  | —              | −4.62pp | [−8.47, +3.07]  | No (n too thin)    |

All cells span 0 or are negative. The 新馬 reapplication cell (+1.41pp p3) is the closest to positive but CI lower bound is −0.71pp — not significant. n=1,319 but effect does not clear noise floor.

**Cross-class conclusion:** The blinker effect does **not** concentrate in any identifiable subgroup. There is no class-conditional adoption path. The 2勝C first-blinker cell is the only positive directional win result with some plausibility, but n=778 and CI includes 0; pursuing per-class routing based on this signal would require a much larger, more confident effect.

---

## 6. 障害 (Jump) Races and Age Breakdown (JRA)

### 6.1 障害 races (kyoso_shubetsu_code IN ('21','22','23'))

Blinker use in jump races is materially higher than flat (519 blinker-on starts vs 12,315 blinker-off in joined dataset). Jump horses average popularity ~5–6 vs ~8–9 in flat, reflecting smaller fields.

| Category             | n   | avg_ninki | win_lift | win_CI          | p3_lift | p3_CI            | Routable?           |
| -------------------- | --- | --------- | -------- | --------------- | ------- | ---------------- | ------------------- |
| first_blinker        | 113 | 5.7       | +3.81pp  | [−2.38, +10.01] | +1.28pp | [−7.57, +9.24]   | No (CI wide, n=113) |
| true_reapplication   | 24  | 6.4       | +9.20pp  | [−3.30, +25.86] | −1.66pp | [−18.33, +15.01] | No (n too thin)     |
| blinker_continuation | 379 | 5.4       | +0.08pp  | [−2.82, +3.51]  | −0.78pp | [−5.53, +3.97]   | No                  |

障害 first_blinker shows nominally positive win lift (+3.81pp) but n=113 is far below the n≥300 threshold and the CI lower bound is −2.38pp. The reapplication cell (n=24) is noise only. **Not routable.**

### 6.2 Age breakdown

Per-age market baseline computed from blinker_off population in each age bucket.

#### first_blinker by age

| Age  | n      | avg_ninki | win_lift | win_CI         | p3_lift     | p3_CI              | Routable?                 |
| ---- | ------ | --------- | -------- | -------------- | ----------- | ------------------ | ------------------------- |
| 2yo  | 2,861  | 8.6       | −0.68pp  | [−1.38, +0.09] | **−2.09pp** | **[−3.39, −0.80]** | No (negative)             |
| 3yo  | 14,653 | 8.6       | −0.14pp  | [−0.49, +0.23] | **−1.40pp** | **[−1.96, −0.83]** | No (p3 CI fully negative) |
| 4yo+ | 13,988 | 8.4       | −0.15pp  | [−0.52, +0.21] | **−1.34pp** | **[−1.99, −0.72]** | No (p3 CI fully negative) |

All age buckets negative on place-3. 2yo horses show the strongest negative signal (p3_lift −2.09pp, CI [−3.39, −0.80]).

#### true_reapplication by age

| Age  | n     | avg_ninki | win_lift | win_CI         | p3_lift | p3_CI           | Routable?       |
| ---- | ----- | --------- | -------- | -------------- | ------- | --------------- | --------------- |
| 2yo  | 45    | 9.1       | +0.11pp  | [−4.34, +6.78] | +2.18pp | [−8.94, +13.29] | No (n too thin) |
| 3yo  | 1,217 | 8.3       | +0.02pp  | [−1.22, +1.33] | −0.37pp | [−2.42, +1.85]  | No              |
| 4yo+ | 5,644 | 9.0       | −0.03pp  | [−0.54, +0.54] | +0.44pp | [−0.57, +1.43]  | No              |

Flat across all ages for reapplication. 4yo+ p3 +0.44pp CI [−0.57, +1.43] — largest positive cell but CI lower bound negative.

### 6.3 Routable cells summary

**Threshold: CI entirely positive AND n ≥ 300.**

Scanning all (category × class) and (category × age) cells:

> **Zero routable cells found across all dimensions (class × age × jump/flat).**

The closest candidates:

- 障害 first_blinker: +3.81pp win but CI [−2.38, +10.01], n=113 — too thin, CI spans 0
- 2勝C first_blinker: +1.36pp win but CI [−0.31, +3.03], n=778 — CI spans 0
- 新馬 reapplication p3: +1.41pp but CI [−0.71, +3.61] — CI spans 0

---

## 7. Signal vs Rarity Assessment

| Feature            | Coverage (n)  | Subgroup lift           | ρ      | Assessment                      |
| ------------------ | ------------- | ----------------------- | ------ | ------------------------------- |
| first_blinker      | 31,502 (1.7%) | −0.21pp win, −1.53pp p3 | −0.038 | Negative signal, market correct |
| true_reapplication | 6,906 (0.4%)  | +0.02pp win, +0.51pp p3 | −0.045 | Noise, thin coverage            |
| true_removal       | 21,076 (1.1%) | −0.15pp win, −0.47pp p3 | −0.044 | Slightly negative               |

The signals are not "rare-but-strong" — they are rare AND directionally flat-to-negative.

---

## 8. Verdict

**JRA: ABORT (aggregate and per-class)**

- Neither PROCEED criterion met at aggregate level:
  - Subgroup lift: all CIs include 0 or are negative; no robust market-orthogonal outperformance
  - Partial ρ: max |ρ| = 0.045, well below 0.08 threshold in both full and holdout windows
- Extended per-class analysis (9 race-class buckets including 障害 × 2 blinker categories, plus 3 age buckets = 24+ cells) shows **zero routable cells (CI entirely positive with n≥300)**. The hypothesis that blinker lift concentrates in younger/lower-condition horses is falsified: 新馬 and 未勝利 first-blinker p3 CIs are fully negative. 2勝C win is nominally positive (+1.36pp) but CI spans 0 (n=778). 障害 first-blinker shows +3.81pp win but n=113 and CI lower bound is −2.38pp. 2yo first-blinker is the most negative age slice (p3 −2.09pp, CI fully negative).
- The market appears to correctly price blinker changes. First-blinker horses actually run slightly below their market expectation — consistent with the market already incorporating the blinker signal via longer odds.
- Adding blinker features would not be information-free: they correlate slightly with WORSE outcomes (market already prices it), risking GBDT over-indexing on noise.

**NAR: N/A (no data — all `blinker_shiyo_kubun='0'`)**

---

## 9. Method Notes

- Join: `jvd_se` × `race_entry_corner_features` on (ketto_toroku_bango, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)
- Excluded: kakutei_chakujun='00' (scratch/DNS), finish_position=NULL
- Market baseline: blinker_off population only (n=1,675,903), bucketed by tansho_ninkijun (popularity rank)
- Bootstrap: 2000 iterations, seed=42
- Partial ρ: Frisch-Waugh residualization of ranks, one control (popularity)
- DuckDB not used (direct PG query via psycopg2); analysis scripts at `/tmp/blinker_refined.py`, `/tmp/blinker_class_perclass.py`, `/tmp/blinker_shogai_age_full.py` (not git-tracked)
- 障害 identification: kyoso_shubetsu_code IN ('21','22','23') in race_entry_corner_features
- Age: jvd_se.barei (character varying, cast to int); buckets 2yo/3yo/4yo+
- Per-class baseline: computed per-class from `blinker_off` only (no cross-class contamination)
- NAR per-class: not applicable (all blinker_shiyo_kubun='0'); Ban-ei is a subset of NAR — same N/A
