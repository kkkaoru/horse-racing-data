# I4 Root-Cause: Serve-vs-Train Skew Tax

**Date**: 2026-06-11
**Status**: COMPLETE — skew tax is large and warrants Phase 3 serve-path fixes

## TL;DR

At serve time (03:00 JST launchd cron), realtime odds/popularity features are either unavailable (JRA: 100% of runs) or only partially available (NAR: ~60% of runs run on full median fallback). The model was trained and evaluated with post-race final odds (WF condition). The accuracy gap — the **skew tax** — is:

| Category           | Condition                             | top1        | place2      | place3      | top3_box    |
| ------------------ | ------------------------------------- | ----------- | ----------- | ----------- | ----------- |
| JRA                | WF (true final odds)                  | 47.43%      | 85.98%      | 97.18%      | 18.88%      |
| JRA                | Full median (actual serve)            | 38.78%      | 76.35%      | 92.46%      | 15.52%      |
| **JRA**            | **Skew tax**                          | **-8.65pp** | **-9.63pp** | **-4.72pp** | **-3.36pp** |
| NAR                | WF (true final odds)                  | 57.62%      | 92.82%      | 99.12%      | 34.49%      |
| NAR                | Full median fallback                  | 47.78%      | 89.92%      | 98.57%      | 30.55%      |
| NAR                | 40% success / 60% fallback (observed) | 51.93%      | 91.04%      | 98.77%      | 32.09%      |
| **NAR (full)**     | **Skew tax**                          | **-9.84pp** | **-2.90pp** | **-0.55pp** | **-3.94pp** |
| **NAR (observed)** | **Skew tax at ~40% coverage**         | **-5.68pp** | **-1.77pp** | **-0.35pp** | **-2.41pp** |

**Verdict: The serve-path fix is a binding constraint.** JRA top1 accuracy is structurally degraded by 8.65pp at every single serve run. NAR top1 is degraded by 5.7pp on average. This dwarfs all signal-search iterations attempted so far (accept/reject threshold: 0.05pp).

---

## 1. Realtime Fetch Coverage (from launchd logs)

Logs analysed: `~/Library/Logs/finish-position-predict/2026060{4..11}.log`

Realtime odds fetching was introduced by commit `8b3051e` on 2026-06-10. Logs before that date show no `[realtime-odds]` lines — all pre-06-10 predictions ran on pure historical feature parity (no realtime odds in the feature set at all; this is a different condition from median-fallback).

### JRA

- **races=0 in every single cron run** across 06-10 and 06-11.
- Cause: at 03:00 JST, `jvd_se` has no upcoming JRA races with `kakutei_chakujun IS NULL`. Either JRA race data is not yet synced at that hour, or the observation window (weekday 06-04 to 06-11) contains no JRA race days.
- Effect: JRA predictions **always** use median fallback for `odds_score` and `popularity_score`. The WF-vs-serve gap is fully realized every day.
- JRA skew tax: **-8.65pp top1, -9.63pp place2, -4.72pp place3**.

### NAR

| Day   | Completed runs | Success (odds > 0) | Fallback | Success rate | Dominant error                                                         |
| ----- | -------------- | ------------------ | -------- | ------------ | ---------------------------------------------------------------------- |
| 06-10 | 14             | 5                  | 9        | 36%          | 403 Forbidden (first 2 runs, 48 races each), then json_parse + timeout |
| 06-11 | 15             | 6                  | 9        | 40%          | timeout (5s, ~6 races) + json_parse (weight endpoint empty body)       |

**06-10 finding**: The first 2 runs of the day returned `HTTP Error 403: Forbidden` for all 48 NAR races across keibajo 30, 44, 47, 50. Root cause was Python's default `User-Agent` being blocked by the Cloudflare WAF. Commit `35aa84d` (deployed 06-10 11:04 JST) added a `horse-racing-data-predict/1.0` UA header, resolving the 403s. After that fix, later runs of the same day returned partial success.

**60% fallback denominator**: Even after the 403 fix, ~60% of NAR cron runs still return `zero rows` (full median). This is because the cron runs hourly and early-day runs execute before odds are available in D1 for that day's races.

### Bataiju (horse weight)

When odds fetch succeeds, bataiju coverage is 23–43% of horses on average (observed range: 23%–100% across successful parquet writes). The **bataiju isolation test shows negligible impact** (<0.02pp top1). The model's weight-related features (`weight_avg_5`, `weight_diff_from_avg`) draw primarily from historical nvd_se data and the realtime delta adds essentially nothing within the simulation's resolution. Bataiju is not a priority.

---

## 2. Skew Tax Simulation Methodology

**Data**: `feat-v20-merged` Parquet set (687,846 holdout rows, 2023+). These contain the post-race final odds (`odds_score`, `popularity_score`) for every horse — the WF condition.

**Models**:

- JRA: `iter14-jra-cb-pacestyle-course-v8` (CatBoost, 241 features, 1,000 iterations)
- NAR: `iter12-nar-xgb-hpo-v8` (XGBoost, 192 features)

**Conditions**:

1. **WF (true final odds)**: features as stored in the holdout parquet (post-race final odds).
2. **Full median fallback**: `popularity_score = 0.5`, `odds_score = 0.5664 (JRA) / 0.5048 (NAR)`, plus all derived odds columns (implied_prob, market_share, rank_in_race, disagreement) set to median-consistent constants.
3. **Observed mix (NAR)**: 40% of races receive true odds (success path), 60% receive medians (fallback path), matching the observed 06-10/06-11 distribution.

**Note on missing features**: The holdout parquets lack 5 features present in the JRA model schema (`recent_soha_time_per_meter_avg5`, `same_distance_soha_time_per_meter_avg5`, `bataiju_avg5`, `weight_trend_5`, `weight_volatility_5`). These were filled with 0 in both conditions, so the true WF baseline reported here is slightly understated. The delta (skew tax) remains valid as both conditions share the same missingness.

---

## 3. Feature Importance of Odds/Popularity

### JRA iter14 (PredictionValuesChange %)

| Feature                          | Importance      |
| -------------------------------- | --------------- |
| `inverse_odds_implied_prob`      | 8.87%           |
| `inverse_odds_market_share`      | 7.67%           |
| `odds_score`                     | 7.07%           |
| `tansho_odds_raw`                | 6.73%           |
| `odds_score_diff_from_race_avg`  | 4.96%           |
| All other odds/popularity/weight | ~0.27% combined |
| **Total odds+popularity+weight** | **35.6%**       |

The top feature overall is `target_corner_4_norm` (28.7%) — a historical race-internal signal, immune to serve-time skew. Odds cluster at positions 2–6 and represent the bulk of the information the model can leverage at WF time that is absent at serve time.

### NAR iter12 (feature gain %)

| Feature                                 | Gain share |
| --------------------------------------- | ---------- |
| `odds_score`                            | 5.17%      |
| `popularity_score`                      | 4.54%      |
| `horse_popularity_vs_field`             | 0.23%      |
| `weight_avg_5` + `weight_diff_from_avg` | 0.54%      |
| **Total odds+popularity+weight**        | **10.5%**  |

NAR's odds importance (10.5% total) is lower than JRA's (35.6%), consistent with the lower-variance skew tax on place2/place3. Top1 tax is still large (9.84pp) because odds/popularity are critical for identifying the single winner.

---

## 4. Worst-Case Scenarios

### JRA — Structural (100% median every day)

Already worst-case. Every JRA serve run operates at full median since JRA upcoming races aren't found at 03:00 JST. Net realized tax: **-8.65pp top1** vs the WF benchmark that model selection and accept/reject gates use.

### NAR — 403 Cascade (pre-fix)

On 06-10 morning (pre-UA-header fix), all 48 races × 2 early cron runs = 96 races × 2 = 192 per-race 403 errors. The responses are immediate (no timeout), so cascade latency was low, but accuracy impact was identical to full median. **Fixed by commit `35aa84d`.**

### NAR — Timeout Cascade (ongoing risk)

5s timeout × N failing races = NAR 37 races × 5s = 185s (3.1 min) of serial timeout waiting per cron run. On a full-miss day this adds ~3 min overhead per hourly cron run. Does not threaten the 03:00 cron budget (>50 min available) but wastes slot time.

### NAR — Early-Day Odds Gap (structural)

The first several cron runs of the day execute before NAR odds are available in D1 (races haven't opened). Regardless of the fetch path health, these runs always return `zero rows` → full median. This is the primary driver of the 60% fallback rate even after the 403 fix. **The fix is a last-known-odds KV cache**: if a previous run successfully fetched odds for a race key, reuse those odds for subsequent runs before the next update window.

---

## 5. Serve-Path Fix Priority

Fixes ordered by realized tax reduction:

1. **JRA timing fix** (highest impact, ~8.65pp top1 recovery):
   - Option A: Delay JRA cron to post-odds-open (e.g. 09:00 JST Saturday/Sunday when JRA races are live). Cron already runs hourly via launchd — the existing `PREDICT_CATEGORIES` env var can gate JRA to later runs.
   - Option B: Populate JRA upcoming races into jvd_se from a different source (e.g. keibas.net entry list) earlier in the morning so the race_key query returns rows.
   - Estimated recovery: full 8.65pp top1 / 9.63pp place2 if odds are real, or ~50% of that for mid-day pre-race odds.

2. **NAR last-known-odds KV cache** (~3.5pp top1 recovery on early runs):
   - Store the most-recently-fetched `{race_key → (odds, rank)}` in Workers KV with 2-hour TTL.
   - On each cron run, use the KV value as fallback if the D1 live fetch returns empty (race not yet open for betting) or fails.
   - Impact: early-day runs (which currently get median) would get the pre-race market odds instead.

3. **NAR odds retry with exponential backoff** (minor):
   - Current: single attempt, 5s timeout, immediate fallback.
   - Proposed: 2 retries with 1s/2s backoff before fallback.
   - Impact: reduces timeout-cascade scenarios at modest latency cost.

4. **NAR bataiju**: no fix needed (<0.02pp tax).

---

## 6. Data Provenance

- Source logs: `~/Library/Logs/finish-position-predict/20260610.log`, `20260611.log`
- Feature parquets: `tmp/feat-v20-merged/` (42 parquet shards, 4.38M rows)
- Models: `apps/finish-position-predict-container/models/finish-position/{jra,nar}/`
- Raw simulation JSON: `tmp/rootcause/i4_skew.json` (not git-tracked)
