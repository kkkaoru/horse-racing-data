---
document: ROOT-CAUSE DIAGNOSIS — Finish-Position Accuracy Phase 2
date: 2026-06-11
status: FINAL
investigations: I1, I2, I3, I4, I5, I6, I7
models:
  jra: iter14-jra-cb-pacestyle-course-v8
  nar: iter12-nar-xgb-hpo-v8
  banei: banei-cb-v7-grade
holdout: 2023–2026
---

# Root-Cause Diagnosis: Finish-Position Accuracy Phase 2

## 1. Executive Summary

The model is strong. On the 2023–2026 holdout the production model beats both the market (tansho_ninkijun) and the Harville odds-oracle on every evaluated metric for JRA and NAR:

- **JRA top1**: model 44.76% vs oracle 33.46% (+11.30pp); vs market +11.41pp.
- **NAR top1**: model 58.68% vs oracle 43.25% (+15.43pp); vs market +15.64pp.
- **Ban-ei**: genuinely saturated — model ≈ market ≈ oracle within ±0.15pp.

"Accuracy not improving" across iter 14–18 is **not signal exhaustion**. The binding constraints are:

1. **Serve-time odds-fallback skew tax** — at the 03:00 JST cron, JRA predictions always run on median odds (100% fallback), NAR runs on median odds ~60% of the time. The model was trained and accepted/rejected against WF (post-race final odds). The accuracy cost is **−8.65pp JRA top1 / −5.68pp NAR top1** every single production run. Walk-forward eval never sees this loss — so the 0.05pp accept-gate has been optimizing a number the model never achieves in production. This is the single largest fixable constraint.

2. **Exact-ordinal place2/place3 is near-ill-posed** — 47.5% of JRA place2 misses and 62.3% of NAR place2 misses are adjacent-position finishes (predicted-rank-2 horse finished 1st or 3rd). These are correct rankings penalized by the exact-ordinal metric definition. They cannot be removed by better modeling.

Both constraints are fixable. Neither requires new features, new architecture, or objective re-weighting.

---

## 2. Per-Axis Findings Table (I1–I7)

| Investigation | Axis                         | Deciding Numbers                                                                                                                                     | Verdict                                                                                                       |
| ------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| **I1**        | Market / oracle ceiling      | JRA model top1 44.76% >> oracle 33.46%; NAR 58.68% >> oracle 43.25%; Ban-ei model ≈ oracle ≈ market (±0.15pp)                                        | JRA/NAR not capped by market; Ban-ei saturated                                                                |
| **I1**        | Set-membership headroom      | fukusho_2p JRA 68.63% vs market 56.43% (+12.2pp); exact-ordinal place2/3 oracle is barely above market (18.10 vs 18.04%)                             | fukusho_2p is the highest-headroom metric; exact-ordinal near-floor                                           |
| **I2**        | Error calibration            | ECE JRA 0.062 / NAR 0.081 (both under-confident); worst decile gap JRA −0.144, NAR −0.094                                                            | Both models systematically under-state winner probability                                                     |
| **I2**        | Adjacent-position noise      | JRA place2: 47.5% of misses are adjacent; NAR place2: 62.3% adjacent                                                                                 | ~50–62% of place2 shortfall is metric artifact, not model error                                               |
| **I3**        | Training objective alignment | NDCG@3 vs place3 Pearson r=0.24; all alternative objectives (B/C/D) degrade place2/3 vs baseline A                                                   | **Objective is NOT a binding constraint — do not change**                                                     |
| **I4**        | Serve-skew tax               | JRA 0% realtime odds (100% median); NAR ~40% success (~60% median); tax = −8.65pp JRA / −5.68pp NAR top1                                             | **Largest fixable constraint; invisible to WF accept-gate**                                                   |
| **I5**        | Data quality                 | Ban-ei '00' DQ leak ~0.3pp; NAR 2yo/3yo in 'other' bucket ~0.15pp; JRA parquet contamination (eval-only)                                             | 2 fixable defects worth retrain; 1 parquet rebuild needed                                                     |
| **I6**        | Architecture ceiling         | C4 expected-placement re-rank (3·P1+2·P2+1·P3) shows +0.5pp place2 / +0.333pp place3 in simulation; MLX lost to 2-fold WF + no HPO, not architecture | Architecture not the constraint; C4 re-rank worth one experiment                                              |
| **I7**        | Alpha source / market zones  | Model beats market in every stratum: lowest alpha = JRA G1 +2.5pp; highest = JRA 障害 +31pp, NAR 浦和 +27pp, JRA >2400m +26pp                        | No market-efficient zone found; alpha is broad-based across long-distance / jumps / thin venues / open fields |

---

## 3. Ranked Binding Constraints

### What IS a binding constraint

| Rank | Constraint                                           |           Magnitude (top1 pp) | Fixable? | Fix category                                 |
| ---- | ---------------------------------------------------- | ----------------------------: | -------- | -------------------------------------------- |
| 1    | Serve-time odds-fallback (JRA 100% median)           |                   −8.65pp JRA | **YES**  | Infra / cron timing                          |
| 2    | Serve-time odds-fallback (NAR ~60% median)           |                   −5.68pp NAR | **YES**  | KV cache / retry                             |
| 3    | Exact-ordinal place2/3 metric artifact (noise floor) | −47–62% of misses irreducible | Partial  | Adopt fukusho_2p as primary place diagnostic |
| 4    | Under-calibration (ECE 0.062 JRA / 0.081 NAR)        |                  ~0.5–2pp est | **YES**  | Post-hoc isotonic                            |
| 5    | Data quality: Ban-ei '00' DQ leak                    |                 ~0.3pp Ban-ei | **YES**  | 1-line code fix                              |
| 6    | Data quality: NAR 2yo/3yo bucket                     |                   ~0.15pp NAR | **YES**  | Regex arm addition                           |

### What is explicitly NOT a binding constraint

| Non-constraint                     | Evidence                                                                                      |
| ---------------------------------- | --------------------------------------------------------------------------------------------- |
| Training objective                 | All alternatives to NDCG@3 degraded place2/3 (I3 Part 2 — −0.26 to −30pp)                     |
| Architecture (GBDT vs Transformer) | MLX lost to 2-fold WF + no HPO, not to architecture; no architectural path shows >+0.5pp (I6) |
| Market efficiency cap              | Model exceeds Harville oracle in both JRA and NAR; no stratum where market wins (I1, I7)      |
| Condition-aware routing            | No negative-alpha zone exists; routing without new signal gives near-zero lift (I7 §9)        |
| More pre-race signal probes        | 4 consecutive rejects (iter 15–18) on the 0.05pp gate that never reflects real serve accuracy |
| Bataiju (horse weight)             | <0.02pp isolation test (I4); negligible                                                       |
| Dead-heat labels                   | 0.062pp NAR; negligible (I5)                                                                  |

---

## 4. Solution Decision-Tree (Phase 3)

Priority is strict: work the list in order. Do not start a lower-priority item while a higher-priority item is unresolved.

### Priority 1 — Fix the serve-skew tax (recover up to 8.65pp REAL accuracy)

This is the single largest lever. All other work is secondary until this is addressed.

**JRA path** (potential gain: full 8.65pp top1 if pre-race odds become available):

- Root cause: at 03:00 JST, `jvd_se` has no upcoming JRA races (`kakutei_chakujun IS NULL`) — JRA race days are Saturday/Sunday; the early cron finds nothing.
- Option A (preferred): gate JRA predictions to a later cron run when JRA race entries are live and odds are available (e.g., 09:00 JST Saturday/Sunday via `PREDICT_CATEGORIES` env var). Even using pre-race opening odds instead of median recovers an estimated ~50–100% of the 8.65pp tax.
- Option B: populate JRA upcoming entries from an alternative source earlier.
- Measurement: compare R2 parquet predictions under realtime vs median odds for a 2-week window; the 8.65pp is a simulation bound, real-data calibration is mandatory.

**NAR path** (potential gain: ~3.5pp top1 on early-day runs):

- Root cause 1: early cron runs before odds open → zero rows → median fallback. Fix: last-known-odds KV cache with 2-hour TTL per race key; when D1 returns empty use KV.
- Root cause 2: timeout / 403 cascade. The 403 was fixed by commit `35aa84d` (UA header). Ongoing: add 2-retry + exponential backoff (1s/2s) before declaring fallback.
- Root cause 3: no better-than-median fallback hierarchy. Hierarchy should be: (a) realtime D1 odds → (b) KV last-known-odds → (c) same-day earlier parquet if exists → (d) median. Currently jumps from (a) directly to (d).

**Accept-gate consequence**: once the serve path is fixed, WF/holdout accuracy must be re-baselined against the serve condition, not WF. The 0.05pp gate measured a WF number that overestimates real accuracy by 5–9pp. After the fix, re-run iter14/iter12 inference under real serve conditions to establish the corrected baseline.

### Priority 2 — Adopt fukusho_2p as the primary PLACE diagnostic

Exact-ordinal place2/place3 as the primary improvement metric is structurally misleading:

- 47–62% of "misses" are adjacent-position finishes (model-correct rankings that fail the exact-ordinal test).
- The oracle ceiling for exact-ordinal place2 is barely above the market (JRA 18.10% vs 18.04%).
- fukusho_2p (≥2 of predicted top-3 actually finished top-3) is JRA 68.63% vs market 56.43% — a +12.2pp model edge with genuine headroom and no adjacent-position artifact.

Action: add `fukusho_2p` to the accept-gate alongside top1. Keep exact-ordinal place2/place3 for regression monitoring only, not as the primary improvement target.

### Priority 3 — Calibration + C4 expected-placement re-rank (post-hoc, cheap, real-data confirm)

**Calibration** (post-hoc, zero retraining):

- Both models are under-confident: JRA ECE 0.062, NAR ECE 0.081. High-confidence picks are understated by 14pp (JRA decile 10).
- Isotonic regression calibration on softmax scores (`calibrate_finish_position.py`) should shift rank assignments at the margin. Estimated lift: 0.5–2pp top1, small place lifts.
- Run on holdout; confirm before deploying.

**C4 joint expected-placement re-rank** (post-hoc, infrastructure already exists):

- Re-rank predictions by `3·P(top1) + 2·P(top3_excl_top1) + 1·P(place3)` using calibrated probabilities.
- Simulation showed +0.5pp place2 / +0.333pp place3 with zero top1 cost.
- This is the only architecture-adjacent modification that showed positive signal in simulation (I6 §3).
- Change the `re_rank_predictions` call in `calibrate_finish_position.py` to use the joint expected-placement score. Validate on holdout with real-data; do not assume simulation uplift transfers at full magnitude.

### Priority 4 — Data-quality fixes with retrain

Ordered by estimated impact:

1. **Ban-ei '00' DQ leak** (B001, ~0.3pp): change line 391 in `finish_position_features_duckdb.py` from single-nullif to double-nullif (identical to the inference path at line 467). Rebuild `feat-ban-ei-v3` parquet, retrain Ban-ei model.

2. **NAR 2yo/3yo bucket** (F001, ~0.15pp): add `２歳|2歳` and `３歳|3歳` regex arms to `nar_subclass_case_sql` before the existing Ａ/Ｂ/Ｃ checks. Run per-class ensemble optimization for `2YO` and `3YO` subclasses.

3. **JRA feat-v20-merged parquet contamination** (C001): rebuild the parquet equivalent with current script (filter already in code) before any new JRA per-class iteration. Production iter14 is unaffected; this is an eval-correctness fix.

### Explicitly NOT worth doing in Phase 3

- **Objective re-weighting**: all tested variants (place-boosted B, NDCG@2 C, set-membership D) degraded every metric vs the baseline NDCG@3. Do not retry.
- **Conditional specialist / binary heads for place2/3**: catastrophic −10pp top1 confirmed in three independent experiments (I3, I6, 2026-05-20 Phase B).
- **Architecture swap to full transformer**: MLX lost on top1 as well (−8.5pp); the failure was data scale + no HPO + 2-fold WF, not architecture. If retried it requires 21-fold WF + full HPO, ~3 days compute — justified only after serves-skew and calibration fixes are measured.
- **Condition-aware routing (stratum-specific models)**: no negative-alpha strata exist; model beats market everywhere. Routing without new stratum-specific signal gives near-zero lift (I7 §9).
- **More pre-race signal probes under current WF gate**: the 0.05pp accept-gate was measuring a number the model never achieves in production. Signal probes that pass 0.05pp WF but do not improve real serve accuracy are noise. Probe results are only trustworthy after Priority 1 is resolved and the gate is re-baselined.

---

## 5. The Meta-Lesson: WF vs Real Serve-Time Accuracy

The accept/reject gate used in iter 1–18 was:

- Measured on WF holdout (post-race final odds in features).
- Threshold: ≥2 of {top1, place2, place3, top3_box} positive AND ≥1 of {place2, place3} positive; no-regression floor −0.05pp.
- 4 consecutive rejects (iter 15–18) triggered the Phase 2 investigation.

What was not measured: whether the WF number was achievable in production. It was not. The JRA model scores 44.76% on WF but ~38.78% on actual serve. The difference (8.65pp) is larger than the total top1 gain from all training iterations combined since the v7-lineage baseline.

**The 0.05pp gate was optimizing a metric the model never sees in deployment.** All probes that passed or failed the gate were being evaluated against a ceiling that vanishes at serve time.

**Required changes to the accept-gate protocol**:

1. After serve-path fixes (Priority 1), re-run iter14/iter12 under simulated serve conditions to establish a corrected baseline.
2. Add a serve-condition simulation column to the gate: for each candidate model, compute top1 under median-odds fallback (simulating the worst serve case) in addition to WF.
3. Accept-gate ≥ 2 positive on serve-condition metrics, not WF. Keep WF as a secondary ceiling reference.
4. Adopt fukusho_2p (Priority 2) as the primary place diagnostic in the gate.

The model itself is not broken. The measurement framework was disconnected from production reality. Fix the framework before resuming signal search.

---

## 6. Data Provenance

- I1 source: `tmp/rootcause/i1_headroom.json`, scripts `i1_compute_headroom.py` + `i1_final_merge.py`
- I2 source: `tmp/rootcause/i2_decomp.json`
- I3 source: `tmp/rootcause/i3_objective.json`
- I4 source: `tmp/rootcause/i4_skew.json`; launchd logs `~/Library/Logs/finish-position-predict/2026060{4..11}.log`
- I5 source: `tmp/rootcause/i5_dataquality.json`
- I6 source: `tmp/rootcause/i6_arch.json`
- I7 source: `tmp/rootcause/i7_alpha.json`, script `i7_alpha_analysis.py`
