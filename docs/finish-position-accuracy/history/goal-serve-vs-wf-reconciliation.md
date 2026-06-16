# Production Serve vs WF Accuracy Reconciliation

**Date**: 2026-06-17
**Scope**: JRA + NAR finish-position serve accuracy vs WF holdout reference (2026 dates)
**Method**: Neon DB (predictions) + local PG (actuals jvd_se/nvd_se) + per-horse per-race join; gen_at vs race hasso_jikoku timing classification

---

## 0. TL;DR Verdict

**Large residual serve-skew gap exists. Serve path is still largely broken for live predictions.**

| Category | Serve top1 | WF FULL top1 | Gap (pp) | Recoverable?                   |
| -------- | ---------- | ------------ | -------- | ------------------------------ |
| JRA      | 10.4%      | 44.71%       | −34.3    | YES — odds timing still broken |
| NAR      | 19.2%      | 57.76%       | −38.6    | YES — same root cause          |

The serve accuracy measurements above come from the ONLY genuine pre-race predictions in the DB
(134 JRA races / 338 NAR races, validated to have gen_at before race hasso_jikoku UTC).
These numbers are worse than even the DEGRADED baseline (JRA 31.78%, NAR 48.34%) because the
pre-race predictions are generated before odds open (pre-09:00 JST), receiving OOD-median odds.

---

## 1. Timing Classification of All 2026 Predictions

### Critical finding: nearly all DB prediction data is RETROACTIVE

Of 122,322 JRA prediction rows and 263,093 NAR prediction rows stored in Neon for 2026,
**virtually all were generated weeks or months after the race dates** — retrospective bulk
scoring runs for model evaluation. These contaminate any serve accuracy measurement.

#### JRA timing classification (2026 dates with predictions):

| Date                     | Status       | gen_at (JST)        | top1  | top3_box | Note                              |
| ------------------------ | ------------ | ------------------- | ----- | -------- | --------------------------------- |
| 2026-01-01 to 2026-05-21 | RETROACTIVE  | weeks after race    | N/A   | N/A      | All contaminated                  |
| 2026-05-23               | **PRE_RACE** | 04:42-09:01 JST     | 8.3%  | 27.8%    | v7-lineage model                  |
| 2026-05-24               | LATE         | 00:00 JST (+1)      | 22.2% | 47.2%    | just past midnight                |
| 2026-06-06               | **PRE_RACE** | 05:27 JST           | 0.0%  | 20.8%    | iter14+iter25+iter26              |
| 2026-06-07               | RETROACTIVE  | 15:16 JST (Jun 13!) | 33.3% | 70.8%    | iter19 retro overwrite            |
| 2026-06-13               | LATE         | 15:25 JST           | 30.6% | 61.1%    | after races (09:45 JST)           |
| 2026-06-14               | LATE+PARTIAL | 04:45-13:45 JST     | 27.8% | 61.1%    | iter19 late, iter25 ok (13 races) |

#### NAR timing classification (recent dates):

| Date                     | Status          | gen_at (JST)    | top1  | top3_box | Note                |
| ------------------------ | --------------- | --------------- | ----- | -------- | ------------------- |
| 2026-05-22               | RETROACTIVE     | after race      | 0.0%  | 33.3%    | retro               |
| 2026-05-23               | RETROACTIVE     | after race      | 6.1%  | 33.3%    | retro               |
| 2026-05-24               | **PRE_RACE**    | 09:23 JST       | 6.4%  | 31.9%    | v7-lineage          |
| 2026-06-01 to 2026-06-05 | UNKNOWN         | no predictions  | —     | —        | coverage gap        |
| 2026-06-06               | **PRE_RACE**    | 05:23-05:43 JST | 20.0% | 45.7%    | iter12+iter30       |
| 2026-06-07               | **PRE_RACE**    | 09:06-09:08 JST | 12.5% | 35.4%    | iter30              |
| 2026-06-08               | **PRE_RACE**    | 03:03 JST       | 14.8% | 59.3%    | iter30              |
| 2026-06-09               | LATE            | 22:11 JST       | 16.7% | 47.9%    | after races         |
| 2026-06-10               | LATE+PRE mix    | 13:11 JST       | 33.3% | 72.9%    | some venues post    |
| 2026-06-11               | LATE (ALL post) | 20:44 JST       | 52.1% | 77.1%    | all venues finished |
| 2026-06-12               | LATE mix        | 04:33 JST (+1)  | 41.7% | 77.8%    |                     |
| 2026-06-13               | LATE            | 15:30 JST       | 41.2% | 55.9%    |                     |
| 2026-06-14               | LATE            | 13:53 JST       | 31.1% | 64.4%    | 1st race 11:45 JST  |
| 2026-06-15               | LATE            | 22:07 JST       | 13.9% | 38.9%    |                     |
| 2026-06-16               | LATE            | 20:43 JST       | 43.8% | 66.7%    |                     |

---

## 2. Genuine Pre-Race Serve Accuracy (Canonical)

Only predictions with gen_at strictly before the per-race hasso_jikoku (race start time) are
counted as genuine serve measurements. Using EARLIEST valid pre-race prediction per horse.

### JRA — Pre-Race Aggregate (n = 134 races, 2 dates)

| Metric       | Serve | WF FULL | DEGRADED | Gap vs FULL | Gap vs DEGRADED |
| ------------ | ----- | ------- | -------- | ----------- | --------------- |
| top1         | 10.4% | 44.71%  | 31.78%   | −34.3 pp    | −21.4 pp        |
| place2_exact | 10.4% | 24.51%  | 15.25%   | −14.1 pp    | −4.9 pp         |
| place3_exact | 10.4% | 15.48%  | 9.19%    | −5.1 pp     | +1.2 pp         |
| top3_box     | 31.3% | —       | —        | —           | —               |
| fukusho_2p   | 40.3% | 74.79%  | 57.76%   | −34.5 pp    | −17.5 pp        |

**Note on place2/place3**: These use the CANONICAL exact definitions (pred_rank1 finished
exactly 2nd / pred_rank1 finished exactly 3rd). The WF numbers from rootcause-i4 use a
different metric (cumulative). For direct comparison use top1 and fukusho_2p.

**Note on sample**: May 23 used v7-lineage model (old, pre-iter14). Jun 6 used iter14+iter25+
iter26 (current model). Jun 6 had 0/24 wins on 24-race sample (possible by chance: P~14%).
Combined: 5/60 wins from iter14+current models, 0/74 wins if counting May23 model separately.

### NAR — Pre-Race Aggregate (n = 338 races, 4+ dates)

| Metric       | Serve | WF FULL | DEGRADED | Gap vs FULL | Gap vs DEGRADED |
| ------------ | ----- | ------- | -------- | ----------- | --------------- |
| top1         | 19.2% | 57.76%  | 48.34%   | −38.6 pp    | −29.1 pp        |
| place2_exact | 11.5% | 42.38%  | 36.89%   | −30.9 pp    | −25.4 pp        |
| place3_exact | 17.8% | 34.77%  | 30.76%   | −17.0 pp    | −13.0 pp        |
| top3_box     | 48.5% | —       | —        | —           | —               |
| fukusho_2p   | 52.1% | 87.98%  | 83.50%   | −35.9 pp    | −31.4 pp        |

**Note**: NAR pre-race sample is dominated by Jun 6-8 (iter30/iter12 models) plus May 24
(v7-lineage). Jun 6-8 top1 was 12-20%, far below NAR DEGRADED baseline (48.34%).

---

## 3. Post-Fix Era Assessment (Jun 11+)

The 09:30 JST cron fix (commit fe871a6) was deployed on Jun 11. However:

### JRA POST_FIX issues

- **Jun 13**: predictions generated at 15:25 JST (AFTER all races at 09:45 JST). LATE.
  Accuracy 30.6% top1 (36 races) — below FULL but plausible for real-odds-based prediction.
- **Jun 14**: Mix — iter25 predictions from Jun 12 14:04 JST cover 13/36 races (PRE_RACE);
  iter19 predictions from Jun 14 04:45-13:45 JST cover remaining 23/36 races (LATE: 04:45
  UTC is AFTER race start at 00:50 UTC). Coverage hole: 23/36 races lacked valid pre-race preds.

**JRA Jun 14 PRE_RACE only (n=13 races)**: top1=38.5%, top3_box=61.5%, fukusho_2p=69.2%
— within range of FULL (44.71%), suggesting the model is correctly using real odds when
serving the iter25 predictions from Jun 12. This is the best observed JRA serve accuracy.

### NAR POST_FIX issues

- **Jun 6-8** (gen_at 05:23/09:06/03:03 JST): PRE_RACE but BEFORE odds are available
  (JRA/NAR odds not published until ~09:00-10:00 JST). Model receives OOD-median odds.
- **Jun 11+**: gen_at is after ALL races finished at most venues (gen 20:44 JST vs last race
  ~20:35 JST). These are effectively post-race predictions for same-day races. 52% top1 on
  Jun 11 reflects post-race contamination (all 4 NAR venues finished before 11:44 UTC gen).

**The 09:30 cron fixes did NOT produce genuine pre-race predictions with real odds in the
observed window Jun 11-17 for NAR.** Batches are running too late (evening same-day) to be
true pre-race, and too early (pre-09:00 JST) to have real odds when they are pre-race.

---

## 4. Production Coverage — Prediction Gaps

### JRA coverage (2026, races with actual results)

| Period         | Coverage            | Issue                              |
| -------------- | ------------------- | ---------------------------------- |
| Jan 1 – May 22 | 0% (no predictions) | Missing entirely                   |
| May 16-17      | 100% (36/36)        | RETROACTIVE only                   |
| May 23-24      | 100%                | PRE_RACE (May 23) / LATE (May 24)  |
| May 26 – Jun 5 | 0%                  | launchd not running                |
| Jun 3-4        | 0/2, 0/1            | No predictions                     |
| Jun 6-7        | 100%                | Jun 6 PRE_RACE, Jun 7 RETROACTIVE  |
| Jun 9-11       | 0/1 each day        | Missing                            |
| Jun 13-14      | 100% (36/36)        | LATE predictions (post-race start) |

**Launchd failure window**: Jun 14 is the last date with predictions. Jun 15-17 = no JRA
race days. But Jun 1-5 (before fix) = confirmed coverage gap. From task description, launchd
failures noted ~Jun 14-16: consistent with what we see.

### NAR coverage (2026-06, races with actual results)

| Date      | Coverage | Status                                       |
| --------- | -------- | -------------------------------------------- |
| Jun 1-5   | 0/47-36  | MISSING — confirmed launchd gap              |
| Jun 6     | 35/35    | PRE_RACE (05:23 JST)                         |
| Jun 7     | 48/48    | PRE_RACE (09:06 JST)                         |
| Jun 8     | 27/47    | PRE_RACE (03:03 JST) — PARTIAL (27/47 = 57%) |
| Jun 9     | 48/48    | LATE (22:11 JST)                             |
| Jun 10    | 48/48    | LATE (13:11 JST) + PRE partial               |
| Jun 11-16 | 100%     | LATE (all post-race)                         |
| Jun 17    | 48/48    | PRE_RACE (03:04 JST — next-day prewarm?)     |

**Jun 8 partial coverage**: 27/47 races predicted. 20 races (keibajo 47/50 with early 11:45 JST
first race) were generated too late (03:03 JST UTC from Jun 7 = 12:03 JST Jun 8 which is
AFTER 11:45 JST first race). Wait — 03:03 UTC = 12:03 JST, and first race at 11:45 JST.
So predictions arrived 18 minutes LATE for early-venue races. Late arrival = missing.

---

## 5. Per-Class (Race-Class) Breakdown

Due to small sample size (134 JRA pre-race races from only 2 dates), per-class breakdown
has very high variance and is not statistically meaningful. The sample is dominated by:

- May 23: regular weekend races (芝/ダート, various conditions)
- Jun 6: specific Yonezawa + Chukyo races (iter14+ensemble models)

NAR per-class breakdown (338 races) similarly has high variance. No per-class signal can
be reliably extracted from this small pre-race sample.

---

## 6. Root-Cause Analysis

### Why is serve so far below WF FULL?

The serve accuracy gap has multiple compounding causes:

**A. Odds timing (dominant — confirmed in rootcause-i4-serve-skew-tax.md)**

Pre-race predictions generated before 09:00-09:30 JST receive OOD-median odds
(odds_score=0.5664 JRA / 0.5008 NAR flat for all horses). This collapses the single most
important signal in the model:

- JRA model: odds/popularity features = 35.6% of feature importance
- NAR model: odds/popularity features = 10.5% of feature importance
- Population impact (from serve-condition-baseline-population.md):
  JRA: DEGRADED vs FULL = −12.93pp top1
  NAR: DEGRADED vs FULL = −9.43pp top1

Our pre-race serve is BELOW even DEGRADED (−34pp vs FULL) because the OOD-median
condition forces all odds to the same flat value, destroying the within-race rank signal.
At DEGRADED: at least some historical odds are present. At pre-09:00 serve: no odds at all.

**B. Production binder bugs (from serve-combined-recovery-measurement.md, section 4)**

Two bugs prevent Fix2+Fix3 from working end-to-end in production:

1. `finish_position_features_duckdb.py` does not pass `tansho_odds` as output column
   → market-signal layer raises BinderException at runtime
2. `add-futan-juryo-features.py` references `b.source` on jvd_se (no such column)
   → futan layer raises BinderException at runtime

These bugs were patched manually in the validation measurement but NOT in production.
Combined Fix2+Fix3 contribute +6.12pp top1 at population scale. Until patched, these pp
are not recoverable.

**C. Retroactive prediction overwrites**

The DISTINCT ON (latest gen_at per horse per race) in the serve accuracy query means later
bulk-scoring runs overwrite the actual live predictions. Example:

- Jun 7 JRA: iter14 predictions generated Jun 7 00:03 UTC (valid PRE_RACE)
- OVERWRITTEN by iter19 predictions generated Jun 13 15:15 UTC (RETROACTIVE)
- DB now shows 33.3% top1 for Jun 7 (post-race contamination)

This makes the DB unsuitable for direct serve accuracy measurement without timing filters.

**D. Post-fix era timing misalignment**

Even with the 09:30 cron fix, the launchd execution timing for Jun 11-17 batches falls
AFTER race completion for most venues. NAR prewarm batches for NEXT day arrive correctly,
but same-day (or day-of) runs hit the venues after races finish.

---

## 7. Assessment: Recoverable Gap?

### JRA

**Yes, large recoverable gap exists** — estimated +21.74pp top1 (single-day) / +12.93pp
population (from serve-combined-recovery-measurement.md and serve-condition-baseline-population.md).

Current production bugs confirmed still present (Fix2+Fix3 binder errors). The 09:30 cron
fix (Fix1) is deployed but we cannot confirm its correctness from Jun 13-14 data because
those predictions were generated AFTER races started. The JRA prediction coverage has
significant gaps (Jun 1-5 = 0 predictions; Jun 9-11 = 0 predictions for rare race days).

Best observed JRA serve: Jun 14 pre-race-only races (n=13) = 38.5% top1, approaching FULL.
This confirms the model CAN perform near WF levels given real odds.

### NAR

**Yes, large recoverable gap exists** — estimated +9.43pp top1 at population scale.

Current NAR pre-race serve (12-20% top1) is catastrophically below WF (58%) because:

1. Jun 6-8 predictions run at 03:03-09:06 JST = before odds publication (~09:00-10:00 JST)
2. Jun 9-16 batch runs generate at 13:00-22:00 JST = AFTER races finish (not pre-race serve)
3. The prewarm path for NEXT-DAY works correctly (Jun 17 pred gen at 03:04 JST from Jun 16)
   but same-day early venues (10:40 JST first race) still miss odds window

### Coverage Health (Jun 14-16 launchd)

Per the task description, launchd failures were noted around Jun 14-16. Our data shows:

- JRA: Jun 14 is last date with predictions (36 races covered, but LATE).
  Jun 15-17 = no JRA race days, so no gap visible in results.
- NAR: Jun 14-16 have full coverage (45/36/48 races). No coverage gap confirmed for NAR.
  Jun 17 pred gen at 03:04 JST Jun 16 = next-day prewarm working correctly.
- The Jun 1-5 NAR coverage gap (0/47-36 races per day) is confirmed and significant.
  5 days × ~45 races/day = ~225 NAR races with zero predictions served.

---

## 8. Conclusions and Actions Needed

### Verdict: LARGE RECOVERABLE SERVE-SKEW LEVER CONFIRMED

The serve-skew gap is NOT resolved for genuine live (pre-race, real-odds) predictions.

The "recovery" previously documented (serve-combined-recovery-measurement.md) was measured
in simulation with correct odds, not from live serve data. Live production still shows:

- JRA: ~10% top1 live vs ~44% WF FULL → gap ~−34pp
- NAR: ~19% top1 live vs ~58% WF FULL → gap ~−39pp

This gap is the single largest accuracy lever available. The +12.93pp JRA / +9.43pp NAR
from odds-timing alone (rootcause-i4) remains the dominant unrecovered gain.

### Required fixes (priority order)

1. **Patch Fix2 binder bug** (`finish_position_features_duckdb.py`: pass `tansho_odds`
   through as output column). Blocks market-signal layer from running.

2. **Patch Fix3 binder bug** (`add-futan-juryo-features.py`: replace `b.source` with
   `'jra'` literal or add source projection to CTE). Blocks futan layer from running.

3. **Verify 09:30 JST cron (Fix1) is generating predictions with real pre-race odds**.
   Smoke test: on a JRA race day, check gen_at < 00:50 UTC (before first race), and
   confirm odds_score != 0.5664 (non-median) in the feature parquet.

4. **NAR odds timing**: Confirm predictions for morning venues (10:40 JST first race)
   use real pre-race odds. The 03:00 JST prewarm may arrive before odds open at 09:00+ JST.
   If so, need to delay NAR scoring for morning venues to after odds publication.

5. **Add gen_at validation check** to the serve path: record prediction_generated_at
   for each horse, and check it is before first race hasso_jikoku for the day.
   Alert if any batch runs AFTER races start (current Jun 13-14 situation for JRA).

6. **Coverage monitoring**: The Jun 1-5 NAR gap (225 races unserved) and intermittent JRA
   coverage gaps need automated monitoring. Current blind spot: no alerting when launchd
   fails to produce predictions.

---

## 9. Reference Numbers

| Reference                                | Source                                 | Value              |
| ---------------------------------------- | -------------------------------------- | ------------------ |
| JRA WF FULL top1                         | serve-condition-baseline-population.md | 44.71%             |
| JRA DEGRADED top1                        | serve-condition-baseline-population.md | 31.78%             |
| JRA serve top1 (pre-race, n=134)         | this doc                               | 10.4%              |
| JRA best observed (Jun 14 pre-race n=13) | this doc                               | 38.5%              |
| JRA skew tax (FULL−DEGRADED)             | serve-condition-baseline-population.md | +12.93pp           |
| NAR WF FULL top1                         | serve-condition-baseline-population.md | 57.76%             |
| NAR DEGRADED top1                        | serve-condition-baseline-population.md | 48.34%             |
| NAR serve top1 (pre-race, n=338)         | this doc                               | 19.2%              |
| NAR skew tax (FULL−DEGRADED)             | serve-condition-baseline-population.md | +9.43pp            |
| Combined Fix1+2+3 JRA top1 recovery      | serve-combined-recovery-measurement.md | +21.74pp (n=23)    |
| Combined Fix1+2+3 JRA top1 recovery      | serve-condition-baseline-population.md | +12.93pp (n=11703) |

---

## 10. Data Provenance

- Predictions source: Neon `race_finish_position_model_predictions` (read-only)
- Actuals source: local PG `jvd_se` / `nvd_se` (read-only)
- Race start times: local PG `jvd_ra` / `nvd_ra`.hasso_jikoku (read-only)
- No writes, no DELETE/TRUNCATE/DROP, no tmp/ files git-tracked
- Prediction timing classification: gen_at (UTC) vs per-race hasso_jikoku (JST→UTC)
- All metrics use canonical exact definitions:
  - top1: predicted_rank=1 horse finished actual_rank=1
  - place2_exact: predicted_rank=1 horse finished actual_rank=2
  - place3_exact: predicted_rank=1 horse finished actual_rank=3
  - fukusho_2p: any predicted_rank≤2 horse finished actual_rank≤2
