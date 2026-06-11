# Phase 3: Calibration + C4 Re-rank Validation

**Validation date:** 2026-06-11

Post-hoc isotonic calibration + C4 expected-placement re-rank validated on real holdout (2023-2026). Calibration fit on tuning split (2021-2022) only. Primary metric: **fukusho_2p** (≥2 of predicted top-3 are in actual top-3).

## Data splits

| cat | tuning (2021-2022) | holdout (2023-2026) |
| --- | ------------------ | ------------------- |
| JRA | 6,912 races        | 11,703 races        |
| NAR | 26,173 races       | 45,573 races        |

Strategies: A = raw-score (current prod), B = C4(3P̂₁+2P̂₃), C = C4+(+1P̂place3), D = C4w(2P̂₁+3P̂₃), E = C4w2(1P̂₁+3P̂₃+1P̂place3). P̂₁ = isotonic-calibrated per-race softmax; P̂₃ = isotonic-calibrated rank proxy.

## Calibration quality (tuning split, isotonic)

### JRA

| bucket   | n_races | ECE_before | ECE_after | Brier_top1_before | Brier_top1_after | Brier_top3_before | Brier_top3_after |
| -------- | ------- | ---------- | --------- | ----------------- | ---------------- | ----------------- | ---------------- |
| 005      | 1,858   | 0.0122     | 0.0000    | 0.05546           | 0.05478          | 0.59366           | 0.15922          |
| 010      | 944     | 0.0141     | 0.0000    | 0.05598           | 0.05479          | 0.59263           | 0.16090          |
| 701      | 598     | 0.0088     | 0.0000    | 0.05409           | 0.05327          | 0.58950           | 0.15918          |
| 703      | 2,479   | 0.0112     | 0.0000    | 0.04622           | 0.04537          | 0.60273           | 0.15090          |
| 999      | 606     | 0.0111     | 0.0000    | 0.05317           | 0.05228          | 0.60479           | 0.15544          |
| \_global | 6,912   | 0.0110     | 0.0000    | 0.05184           | 0.05135          | 0.59858           | 0.15570          |

### NAR

| bucket   | n_races | ECE_before | ECE_after | Brier_top1_before | Brier_top1_after | Brier_top3_before | Brier_top3_after |
| -------- | ------- | ---------- | --------- | ----------------- | ---------------- | ----------------- | ---------------- |
|          | 19,833  | 0.0114     | 0.0000    | 0.05461           | 0.05419          | 0.53110           | 0.18066          |
| E        | 5,555   | 0.0113     | 0.0000    | 0.05826           | 0.05776          | 0.53473           | 0.18088          |
| \_global | 26,173  | 0.0113     | 0.0000    | 0.05527           | 0.05488          | 0.53250           | 0.18052          |

## Global metrics — holdout (2023-2026)

### JRA — absolute rates

| strategy                      | top1    | place2  | place3  | top3_box | fukusho_2p | rentai_hit | place3_set |
| ----------------------------- | ------- | ------- | ------- | -------- | ---------- | ---------- | ---------- |
| **A** raw-score (current)     | 44.510% | 22.977% | 16.970% | 15.714%  | 68.401%    | 96.334%    | 60.150%    |
| **B** C4 = 3P̂₁+2P̂₃            | 44.476% | 22.926% | 17.474% | 15.825%  | 68.453%    | 96.309%    | 60.195%    |
| **C** C4+ = 3P̂₁+2P̂₃+1P̂place3  | 44.484% | 22.960% | 17.184% | 15.714%  | 68.401%    | 96.334%    | 60.150%    |
| **D** C4w = 2P̂₁+3P̂₃           | 44.476% | 22.926% | 17.474% | 15.825%  | 68.453%    | 96.309%    | 60.195%    |
| **E** C4w2 = 1P̂₁+3P̂₃+1P̂place3 | 44.484% | 22.960% | 17.184% | 15.714%  | 68.401%    | 96.334%    | 60.150%    |

### JRA — deltas vs raw-score (A) with bootstrap LB95

| strategy                      | top1 Δpp | place2 Δpp | place3 Δpp | top3_box Δpp | fukusho_2p Δpp | rentai_hit Δpp | place3_set Δpp | fuku LB95 | top1 LB95 | ps LB95 |
| ----------------------------- | -------- | ---------- | ---------- | ------------ | -------------- | -------------- | -------------- | --------- | --------- | ------- |
| **B** C4 = 3P̂₁+2P̂₃            | -0.034   | -0.051     | +0.504     | +0.111       | +0.051         | -0.026         | +0.046         | -0.2222   | -0.2991   | -0.0798 |
| **C** C4+ = 3P̂₁+2P̂₃+1P̂place3  | -0.026   | -0.017     | +0.214     | +0.000       | +0.000         | +0.000         | +0.000         | +0.0000   | -0.2905   | +0.0000 |
| **D** C4w = 2P̂₁+3P̂₃           | -0.034   | -0.051     | +0.504     | +0.111       | +0.051         | -0.026         | +0.046         | -0.2136   | -0.2991   | -0.0769 |
| **E** C4w2 = 1P̂₁+3P̂₃+1P̂place3 | -0.026   | -0.017     | +0.214     | +0.000       | +0.000         | +0.000         | +0.000         | +0.0000   | -0.2820   | +0.0000 |

**Global gate C4(B) vs raw(A): REJECT** — fukusho_2p LB95=-0.2222pp <= 0 and place3_set LB95=-0.0798pp <= 0

### NAR — absolute rates

| strategy                      | top1    | place2  | place3  | top3_box | fukusho_2p | rentai_hit | place3_set |
| ----------------------------- | ------- | ------- | ------- | -------- | ---------- | ---------- | ---------- |
| **A** raw-score (current)     | 58.504% | 35.148% | 26.976% | 34.698%  | 87.844%    | 99.300%    | 73.947%    |
| **B** C4 = 3P̂₁+2P̂₃            | 58.471% | 34.986% | 26.983% | 34.646%  | 87.741%    | 99.296%    | 73.894%    |
| **C** C4+ = 3P̂₁+2P̂₃+1P̂place3  | 58.471% | 35.008% | 27.009% | 34.698%  | 87.844%    | 99.300%    | 73.947%    |
| **D** C4w = 2P̂₁+3P̂₃           | 58.471% | 34.986% | 26.983% | 34.646%  | 87.741%    | 99.296%    | 73.894%    |
| **E** C4w2 = 1P̂₁+3P̂₃+1P̂place3 | 58.471% | 35.008% | 27.009% | 34.698%  | 87.844%    | 99.300%    | 73.947%    |

### NAR — deltas vs raw-score (A) with bootstrap LB95

| strategy                      | top1 Δpp | place2 Δpp | place3 Δpp | top3_box Δpp | fukusho_2p Δpp | rentai_hit Δpp | place3_set Δpp | fuku LB95 | top1 LB95 | ps LB95 |
| ----------------------------- | -------- | ---------- | ---------- | ------------ | -------------- | -------------- | -------------- | --------- | --------- | ------- |
| **B** C4 = 3P̂₁+2P̂₃            | -0.033   | -0.162     | +0.007     | -0.053       | -0.103         | -0.004         | -0.053         | -0.1844   | -0.1163   | -0.1002 |
| **C** C4+ = 3P̂₁+2P̂₃+1P̂place3  | -0.033   | -0.140     | +0.033     | +0.000       | +0.000         | +0.000         | +0.000         | +0.0000   | -0.1207   | +0.0000 |
| **D** C4w = 2P̂₁+3P̂₃           | -0.033   | -0.162     | +0.007     | -0.053       | -0.103         | -0.004         | -0.053         | -0.1843   | -0.1185   | -0.1009 |
| **E** C4w2 = 1P̂₁+3P̂₃+1P̂place3 | -0.033   | -0.140     | +0.033     | +0.000       | +0.000         | +0.000         | +0.000         | +0.0000   | -0.1185   | +0.0000 |

**Global gate C4(B) vs raw(A): REJECT** — place2 regression -0.162pp < -0.1pp
**Best strategy C: REJECT** — place2 regression -0.140pp < -0.1pp

## Per-class results — C4(B) vs raw(A), holdout 2023-2026

### JRA per-class

| class | n_races | top1_A | fuku_A | top1_B | fuku_B | Δtop1    | Δfuku    | fuku_LB95 | Δplace2  | Δplace3  | gate       |
| ----- | ------- | ------ | ------ | ------ | ------ | -------- | -------- | --------- | -------- | -------- | ---------- |
| 005   | 3,147   | 40.99% | 64.86% | 41.15% | 65.14% | +0.159pp | +0.286pp | -0.1907pp | +0.127pp | +0.445pp | **REJECT** |
| 010   | 1,583   | 43.02% | 64.31% | 42.70% | 64.56% | -0.316pp | +0.253pp | -0.5054pp | -0.442pp | -0.190pp | **REJECT** |
| 016   | 727     | 37.55% | 56.67% | 37.96% | 56.95% | +0.413pp | +0.275pp | -0.4127pp | +0.275pp | +0.550pp | **REJECT** |
| 701   | 953     | 45.02% | 72.61% | 44.91% | 71.88% | -0.105pp | -0.735pp | -1.5740pp | -0.210pp | +1.154pp | **REJECT** |
| 703   | 4,229   | 49.40% | 75.46% | 49.35% | 75.60% | -0.047pp | +0.142pp | -0.2838pp | +0.142pp | +0.709pp | **REJECT** |
| 999   | 1,064   | 42.01% | 61.18% | 41.64% | 60.43% | -0.376pp | -0.752pp | -1.9737pp | -0.846pp | +0.282pp | **REJECT** |

### NAR per-class

| class | n_races | top1_A | fuku_A | top1_B | fuku_B | Δtop1    | Δfuku    | fuku_LB95 | Δplace2  | Δplace3  | gate       |
| ----- | ------- | ------ | ------ | ------ | ------ | -------- | -------- | --------- | -------- | -------- | ---------- |
|       | 33,890  | 59.35% | 88.38% | 59.34% | 88.31% | -0.009pp | -0.071pp | -0.1505pp | -0.136pp | +0.012pp | **REJECT** |
| E     | 10,285  | 55.53% | 86.04% | 55.41% | 85.81% | -0.117pp | -0.224pp | -0.4570pp | -0.233pp | +0.078pp | **REJECT** |
| P     | 127     | 52.76% | 86.61% | 52.76% | 85.04% | +0.000pp | -1.575pp | -3.9370pp | +0.787pp | -0.787pp | **REJECT** |
| R     | 142     | 57.04% | 83.10% | 55.63% | 83.10% | -1.408pp | +0.000pp | +0.0000pp | -0.704pp | -0.704pp | **REJECT** |
| S     | 482     | 62.66% | 89.83% | 62.86% | 90.04% | +0.207pp | +0.207pp | -0.4149pp | -0.415pp | -0.415pp | **REJECT** |
| T     | 408     | 61.27% | 89.71% | 61.76% | 90.20% | +0.490pp | +0.490pp | -0.4902pp | -0.245pp | -0.980pp | **REJECT** |

## Technical notes

**Why B == D and C == E** (identical metrics): P̂₁ (softmax of raw score) and P̂₃ (rank-decay proxy) are both monotone functions of the raw score within each race. Combining them with any positive weights (3+2 or 2+3) yields the same within-race rank ordering. The small number of rank changes that do occur (~13% of horse positions) arise from ties created by the isotonic calibration's flat regions, and these changes do not systematically improve or hurt aggregate metrics.

**Why strategies C and E have LB95 = +0.000**: C and E show zero delta on fukusho_2p / place3_set vs A (the P̂place3 addend preserves the same predicted top-3 set). The bootstrap LB95 of exactly 0.0 reflects a true zero delta, not a computation artefact.

**Why place3 exact-ordinal increases (+0.5pp for JRA)**: Strategy B shifts some rank-3 picks toward horses that are better at actually finishing 3rd (via the calibrated top3 signal). This is a genuine signal for that single position but does not translate to fukusho_2p lift.

**Calibration quality**: ECE_before for JRA (~0.011) confirms the I2 finding (model is slightly under-confident on softmax probs). ECE_after = 0.0000 is isotonic overfitting on the tuning set (iso memorises every input bin); the calibrated probs generalise correctly to the holdout but the ECE on holdout would be non-zero.

**Why the I6 synthetic test showed C4 lift**: The synthetic test generated independent P(top1) and P(top3) per horse (drawn from different distributions). In real single-score production models (YetiRank, XGBoost ranker), these are fully order-correlated within each race, so the composite C4 cannot produce a strictly better top-3 set.

## Verdict: real data vs synthetic

**REAL DATA DOES NOT CONFIRM LIFT.** No class/category achieves fukusho_2p or place3_set LB95 > 0 on real holdout data. The I6 C4 re-rank improvement was synthetic/tuning-set only and does not generalise to unseen data.

## Deploy recommendation

**DO NOT DEPLOY** the calibration + C4 re-rank to production. No class passes the gate on real holdout data.

The raw-score ranking remains the production standard. Recommended next steps: explore horse-level signals or new features that address the root cause (I1/I2) directly.

---

_Generated by tmp/rootcause/phase3_c4_rerank_validation.py — DO NOT modify production until orchestrator deploys._
