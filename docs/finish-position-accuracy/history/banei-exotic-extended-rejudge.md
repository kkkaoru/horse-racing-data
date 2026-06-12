# Ban-ei Exotic Odds: Extended Walk-Forward Re-judge

**Date:** 2026-06-12
**Continuation of:** exotic-odds-place-verify.md (commit e0904c7 / 684749b)
**Status:** REJECT-confirmed

---

## Purpose

The prior verification (exotic-odds-place-verify.md) covered only 2023–2025 (3 folds, ~5268 races).
This re-judge extends to 2019–2025 (7 folds, 12175 races) with train starting from 2016 to provide
more statistical power and verify whether the top1 regression was a fluke of the small holdout window.

---

## Setup

- **Base features:** `feat-ban-ei-v7-grade` (115 numeric + 4 categorical = 119 features after resolution)
- **Exotic features added:** `exotic_sanrenpuku_p3`, `exotic_wide_p3`, `exotic_umaren_p2` (+3 cols → 118 total)
- **Model:** CatBoost YetiRank (iterations=300, lr=0.05, depth=8, l2_leaf_reg=3.0, thread_count=6)
- **Holdout years:** 2019, 2020, 2021, 2022, 2023, 2024, 2025
- **Train start:** 2016 (minimum 3 years history)
- **Bootstrap LB95:** 10 000 reps, seed=42, paired per-race delta

**Known data caveat — 2024:** wide (o3) and umaren (o2) are 100% NULL for Ban-ei 2024 (ingest gap).
sanrenpuku (o5) is intact (2.1% null). This is the same gap documented in the prior verify.

---

## Per-year results

| Year | Races | top1 BASE | top1 NEW | Δtop1       | place2 Δ | place3 Δ | fukusho_2p BASE | fukusho_2p NEW | Δf2p        | top3_box Δ |
| ---- | ----- | --------- | -------- | ----------- | -------- | -------- | --------------- | -------------- | ----------- | ---------- |
| 2019 | 1668  | 0.3663    | 0.3735   | +0.0072     | 0.000    | +0.0066  | 0.6649          | 0.6876         | **+0.0228** | +0.0150    |
| 2020 | 1722  | 0.3635    | 0.3641   | +0.0006     | +0.0023  | +0.0180  | 0.6452          | 0.6713         | **+0.0261** | +0.0197    |
| 2021 | 1759  | 0.3189    | 0.3189   | 0.000       | +0.0085  | +0.0051  | 0.6515          | 0.6771         | **+0.0256** | +0.0188    |
| 2022 | 1758  | 0.3555    | 0.3458   | −0.0097     | −0.0006  | −0.0017  | 0.6308          | 0.6348         | +0.0040     | +0.0040    |
| 2023 | 1788  | 0.3546    | 0.3596   | +0.0050     | +0.0112  | +0.0073  | 0.6421          | 0.6504         | +0.0084     | +0.0056    |
| 2024 | 1788  | 0.3406    | 0.3400   | −0.0006     | −0.0011  | +0.0050  | 0.6247          | 0.6387         | +0.0140     | +0.0123    |
| 2025 | 1692  | 0.3534    | 0.3381   | **−0.0154** | −0.0118  | +0.0047  | 0.6495          | 0.6678         | +0.0183     | +0.0106    |

Year 2019–2021 show consistent strong gains on fukusho_2p/place3, with top1 neutral or positive.
Year 2022 and 2025 show top1 regression (−0.97pp and −1.54pp respectively).

---

## Pooled results (2019–2025, n=12175 races)

| Metric     | BASE   | NEW    | Delta       | LB95 (10k bootstrap) |
| ---------- | ------ | ------ | ----------- | -------------------- |
| top1       | 0.3502 | 0.3484 | **−0.18pp** | **−0.62pp**          |
| place2     | 0.2051 | 0.2064 | +0.13pp     | −0.42pp              |
| place3     | 0.1543 | 0.1607 | **+0.64pp** | **+0.08pp**          |
| fukusho_2p | 0.6439 | 0.6608 | **+1.69pp** | **+1.22pp**          |
| top3_box   | 0.1147 | 0.1270 | **+1.22pp** | **+0.85pp**          |

---

## Adopt gate evaluation

| Gate condition                        | Value         | Pass?    |
| ------------------------------------- | ------------- | -------- |
| top1 Δ ≥ −0.05pp veto floor           | −0.18pp       | **FAIL** |
| fukusho_2p LB95 > 0                   | +1.22pp       | PASS     |
| place3 LB95 > 0 (robust place signal) | +0.08pp       | PASS     |
| place2 LB95 > 0                       | −0.42pp       | FAIL     |
| ≥ 1 of {place2/place3} LB95 robust    | place3 passes | PASS     |
| **Overall ADOPT gate**                | **top1 veto** | **FAIL** |

**VERDICT: REJECT-confirmed**

The extended evaluation with 7 folds and 12 175 races confirms the prior 3-fold result.
Top1 regression mean is −0.18pp with LB95 = −0.62pp — clearly and consistently below the −0.05pp veto floor.
fukusho_2p and place3 remain strongly positive and statistically robust at LB95, but these cannot override
the top1 veto gate.

---

## Per-class breakdown (pooled 2019–2025)

| Class         | N_races | Δtop1       | Δplace2 | Δplace3 | Δfukusho_2p | Δtop3_box   |
| ------------- | ------- | ----------- | ------- | ------- | ----------- | ----------- |
| OP (S/T)      | 43      | +2.33pp     | +2.33pp | +2.33pp | +2.33pp     | +2.33pp     |
| B_MID (P/Q/R) | 186     | +0.54pp     | +1.61pp | +1.08pp | +2.15pp     | +0.54pp     |
| MUKATSU (E)   | 920     | −0.33pp     | +0.87pp | +1.20pp | +1.74pp     | +1.52pp     |
| OTHER (blank) | 11026   | **−0.19pp** | +0.04pp | +0.58pp | **+1.68pp** | **+1.21pp** |

The bulk of races are OTHER (blank grade_code, 90.6% of total). This class drives the pooled top1 regression
(−0.19pp) while simultaneously providing the strongest absolute fukusho_2p gain (+1.68pp).
The signal pattern is consistent across all classes: exotic odds reliably improve place predictions
but divert win-prediction quality.

---

## Diagnostic: why top1 regresses despite strong place gain

The exotic implied-probability features encode P(top-3) information from the betting market.
When these are added as model inputs, the YetiRank objective (NDCG@3, top-heavy relevance 3/2/1)
re-weights its attention toward the multi-horse place signal, which can displace win-specific differentiation.

This is consistent with:

1. The 2019–2021 folds (where wide/umaren have 0% null rate) showing net-positive or neutral top1.
2. The 2022 and 2025 folds (which are not null-gap years) also showing top1 regression — the regression
   is not driven solely by the 2024 null gap; it is structural.
3. The odds-decouple probe (memory: project_science_track_saturation_2026_06_11.md) showing that
   Ban-ei market pricing is optimally captured by tansho; the exotic features add a different slice of
   the same market signal, which is already partially captured.

---

## Notes on future candidates (deferred, do NOT build now)

- A dedicated Ban-ei place model (separate from win model, using place2/place3/fukusho_2p objective)
  could exploit the +1.69pp fukusho_2p signal without incurring the top1 regression. This requires
  a separate serve-side ensemble routing, which is a different project.
- YetiRank optimizes NDCG@3/top-heavy; exotic place signal may be better exploited by a dedicated
  place-ranking objective. This is noted as a future candidate only.

---

## Artifacts

- `tmp/feat-banei-v7grade-exotic-extended/` — extended exotic feature store (2016–2026, untracked)
- `tmp/banei_exotic_extended_results.json` — full numeric results (untracked)
- `tmp/banei_exotic_extended_wf.py` — WF script (untracked, not committed on REJECT)
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add_exotic_odds_features.py` — feature builder (untracked)
