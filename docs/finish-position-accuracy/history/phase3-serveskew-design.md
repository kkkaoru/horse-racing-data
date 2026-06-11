# Phase 3: Serve-Skew Tax Fix Design

**Date**: 2026-06-11
**Based on**: rootcause-i4-serve-skew-tax.md (commit fc95963)
**Status**: DESIGN COMPLETE — ready for implementation dispatch

---

## 1. JRA Pre-Race Odds Verdict

### The critical question

I4 found `races=0` for every JRA cron run at 03:00 JST. Why?

### Root cause: dual problem

**Problem A — Wrong day (weekdays)**: JRA races only occur Saturday and Sunday. On Mon-Fri `jvd_se` has no rows with `kakutei_chakujun IS NULL` for JRA keibajo codes, so the upcoming-races query correctly returns zero races. The 03:00 cron has always been wasted on weekdays for JRA.

**Problem B — Wrong time (race days)**: On Saturday and Sunday, the Neon Postgres `jvd_se` table is populated by the PC-Keiba daily sync, which mirrors the JVD data at **~09:03 JST** (next-morning mirror). At 03:00 JST on a race day, the Saturday/Sunday race entries have not yet arrived in `jvd_se`. The table still shows last night's state (previous race day's completed races). The upcoming-races query returns zero because there are no new races yet.

### JRA odds in D1: already there

**JRA pre-race odds ARE obtainable.** The `sync-realtime-data-hot` worker uses `fetchJraOddsWithPlaywright` (Cloudflare Playwright, `@cloudflare/playwright`) to scrape `jra.go.jp` odds pages. The advance-odds window opens at **JST 19:00 the prior day** (`raceDayStart - 5h` per `getJraAdvanceOddsFetchSlotAt` in `time.ts`). From 19:00 Friday through Saturday race starts, and from 19:00 Saturday through Sunday race starts, the hot worker has been writing JRA tansho odds into `odds_snapshots` + `OddsCache` Durable Object hourly.

The predict container's `realtime_odds_fetcher.py` already supports `source="jra"` in `_SOURCE_BY_CATEGORY` and calls `GET https://sync-realtime-data-hot.kkk4oru.com/api/odds/{jra:YYYY:MMDD:KK:RR}`. The path is fully wired. It has never produced results only because the container always ran at 03:00 before `jvd_se` had race entries.

### Does shifting the JRA cron work?

**YES — unambiguously.**

- After ~09:03 JST on race days, `jvd_se` has JRA race entries.
- By 09:30 JST, the hot worker has had 12+ hours of advance-window snapshots in D1 (since 19:00 prior evening) and 30+ minutes of same-day odds.
- The predict container at 09:30 will: (a) find N races in `_query_upcoming_race_keys`, (b) fetch odds from the hot worker D1 for each race_key, (c) pass real odds into the DuckDB COALESCE, recovering the full WF accuracy.

**Estimated recovery**: full 8.65pp top1 / 9.63pp place2 on JRA race days (Sat+Sun).

---

## 2. Ranked Fix Design

### Fix 1 — JRA cron timing shift (HIGHEST PRIORITY)

**Impact**: +8.65pp top1 / +9.63pp place2 on JRA race days
**Effort**: LOW — launchd config + shell script only
**Risk**: LOW

#### Mechanics

The launchd plist (`scripts/launchd/com.kkk4oru.finish-position-predict.plist`) currently has a single `StartCalendarInterval` at `Hour=3 Minute=0`. Replace with two schedule entries:

1. **03:00 JST daily** — run NAR + Ban-ei only (`PREDICT_CATEGORIES=nar,ban-ei`)
2. **09:30 JST Sat+Sun** — run all categories (`PREDICT_CATEGORIES=jra,nar,ban-ei`)

The wrapper script (`scripts/launchd/finish-position-predict-daily.sh`) needs to detect day-of-week (or be driven by the `PREDICT_CATEGORIES` env var already wired in the plist). `predict_upcoming.py` already reads `PREDICT_CATEGORIES` and short-circuits categories not in the allowlist — zero Python changes.

#### Files to change

| File                                                        | Change                                                                                                            |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `scripts/launchd/com.kkk4oru.finish-position-predict.plist` | Add second `StartCalendarInterval` dict for Hour=9 Minute=30; set `PREDICT_CATEGORIES=nar,ban-ei` for 03:00 entry |
| `scripts/launchd/finish-position-predict-daily.sh`          | Wire `PREDICT_CATEGORIES` default to the plist-injected value; no logic change needed if plist sets it            |

macOS launchd does not support day-of-week filtering in `StartCalendarInterval` without multiple dict entries. Two dicts are needed: the 03:00 entry (Mon-Sun, nar+ban-ei) and the 09:30 entry (must fire only Sat+Sun). Since launchd has no weekday filter, the 09:30 entry fires daily; the wrapper script or the container itself should detect whether `jvd_se` has JRA races before predicting (the `_query_upcoming_race_keys` function already handles the zero-race case gracefully — it returns an empty dict and skips scoring). A daily 09:30 run for JRA that finds zero races on weekdays is harmless.

#### Test plan

1. On next JRA race day (Sat or Sun), verify launchd fires at 09:30.
2. Check dated log: `[realtime-odds] wrote N rows` for JRA category.
3. Check `finish_position_cron_executions` audit table: `races_predicted > 0` for JRA.
4. Check `race_finish_position_model_predictions` for JRA race_keys with today's model version.

---

### Fix 2 — NAR last-known-odds KV cache (MEDIUM PRIORITY)

**Impact**: ~3pp top1 for NAR (50% of currently-fallback early-day runs get real odds instead of OOD median)
**Effort**: MEDIUM — new Python class + new hot worker endpoint + tests
**Risk**: MEDIUM — new HTTP call per run; TTL tuning needed

#### Root cause

60% of NAR hourly cron runs (primarily 03:00-09:00 JST) return zero rows from the hot worker because NAR odds sale opens at 10:00 JST (day races) or 12:00 JST (night meetings). The D1 odds_snapshots table has no entries for that day's races yet. The predict container falls back to OOD median.

#### Proposed design

After a successful realtime odds fetch for a race, write the `(race_key, odds_rows)` to a short-TTL KV entry via the hot worker. On subsequent runs where the D1 response is empty for that race, read back the KV entry. Use a **4-hour TTL** so early-day runs (03:00–07:00) can use the previous run's real data if it exists.

The cleanest implementation reuses the existing `OddsCache` Durable Object which already implements `GET /races/{raceKey}` (read with TTL check) and `PUT /races/{raceKey}` (write with configurable TTL). The default TTL is 2h (`ODDS_DO_TTL_SECONDS`). The predict container already calls `GET /api/odds/{raceKey}` on the hot worker which reads from the DO.

**The change reduces to**: when `extract_rows` returns an empty list (zero rows from the DO response), issue a second GET to a KV-backed endpoint with 4h TTL. The "last known odds" are already stored in the DO by previous odds-fetch runs — the only gap is TTL: the current 2h DO TTL expires before 03:00 if the last odds fetch was before 01:00. Extending to 4h (configurable via `ODDS_DO_TTL_SECONDS=14400` in the hot worker wrangler config) would cover the overnight gap without code changes to the predict container.

**Simplest implementation**: Increase `ODDS_DO_TTL_SECONDS` from 7200 (2h) to 14400 (4h) in the hot worker's env config. No Python changes, no new endpoints. The DO alarm cleanup in `OddsCache.alarm()` already handles TTL expiry. The predict container's existing hot-worker call path recovers real odds from the DO TTL extension alone.

#### Files to change (simple path)

| File                                                                    | Change                            |
| ----------------------------------------------------------------------- | --------------------------------- |
| `apps/sync-realtime-data-hot/wrangler.jsonc` (or equivalent env config) | Set `ODDS_DO_TTL_SECONDS = 14400` |

If the DO already has today's odds from an afternoon run the previous day — but that is impossible since NAR races are same-day only. The relevant scenario is: a NAR cron at, say, 04:00 finds zero D1 rows for today (races not yet open); a 12:00 run finds real odds and stores them in the DO; a 14:00 run reads back from DO (within 4h TTL). This is already the happy path. The early-day 04:00 case still gets median because no DO entry exists yet for today's races. The KV cache approach remains useful for the scenario where the **prior run succeeded** (e.g. 09:00 run found real odds, 10:00 run re-queries and wants to use the 09:00 result). The DO TTL extension solves this for afternoon/evening runs. For genuine early-day-before-opening runs (04:00-09:00), there is no prior success to cache, so median fallback is unavoidable regardless.

**Revised expected recovery**: ~1-2pp top1 from DO TTL extension (covers intra-day re-use of already-fetched odds). The full 3pp gain requires additionally caching the pre-open baseline state, which requires the prior-raceday same-horse path (ruled out above). Accept ~1.5pp recovery from the TTL extension alone.

#### Tests

For DO TTL change: no new Python tests (no Python change). Verify in integration that a DO entry written 3.5h ago is still served.

---

### Fix 3 — NAR realtime fetch retry with exponential backoff (LOW PRIORITY)

**Impact**: <0.5pp top1 — only helps when timeout is transient
**Effort**: LOW — <25 lines Python
**Risk**: LOW

#### Mechanics

Current `fetch_odds_for_race` makes a single attempt with 5s timeout and returns `[]` on any exception. Replace with a 2-retry loop with 1s/2s backoff:

```python
def _fetch_with_retry(fetcher, url, timeout, max_retries=2, backoff_base=1.0):
    for attempt in range(max_retries + 1):
        try:
            return fetcher.fetch(url, timeout)
        except Exception as exc:
            if attempt == max_retries:
                raise
            time.sleep(backoff_base * (2 ** attempt))
```

#### Files to change

| File                                                                         | Change                                                       |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------ |
| `apps/finish-position-predict-container/src/realtime_odds_fetcher.py`        | Add `_fetch_with_retry` helper; use in `fetch_odds_for_race` |
| `apps/finish-position-predict-container/tests/test_realtime_odds_fetcher.py` | Add retry path tests                                         |

#### Test plan

Unit test: mock fetcher raises `TimeoutError` on first call, returns valid dict on second call. Verify `extract_rows` receives the valid response. Mock both calls raise: verify empty list returned and no exception propagated.

---

## 3. Fallback Hierarchy (post-fix)

```
Tier 1 (unchanged): Live fetch from hot worker D1/DO for that race_key
Tier 2 (new via TTL extension): DO entry from a previous fetch run (TTL = 4h)
Tier 3 (unchanged): OOD median — odds_score=0.5, popularity_score=0.5 + derived constants
```

Tier 3 is still required for:

- JRA early runs (03:00) on weekdays (no races at all — median is correct since model won't run)
- NAR races whose betting window has not opened yet and no prior run succeeded

---

## 4. Recommended Implementation Order

| Order | Fix                   | Expected pp recovery                                 | Effort | Deploy target                              |
| ----- | --------------------- | ---------------------------------------------------- | ------ | ------------------------------------------ |
| 1     | JRA cron timing shift | +8.65pp top1 JRA / +9.63pp place2 JRA (on race days) | LOW    | launchd reload only                        |
| 2     | DO TTL extension (4h) | ~1.5pp top1 NAR                                      | LOW    | wrangler config change + deploy hot worker |
| 3     | Fetch retry backoff   | <0.5pp top1 NAR                                      | LOW    | bundle with fix 2                          |

Fix 1 is strictly non-overlapping with Fix 2-3 (JRA vs NAR). Both can be implemented in the same dispatch cycle.

---

## 5. What This Does NOT Fix

- **JRA top1 on weekdays**: weekday JRA predictions were always noop (no races). Fix 1 preserves this correctly.
- **NAR pre-open early-day gap (genuine zero data)**: races betting not open = no odds anywhere = median is correct. No fix can recover this without inventing odds that don't exist.
- **OOD median quality**: the median (0.5 for odds_score) is not trained-distribution-consistent for every race. A per-keibajo-code learned median would recover ~0.3-0.5pp but is low priority given the structural fixes above.

---

## 6. Data Provenance

- Architecture investigation: `apps/sync-realtime-data/src/race-key.ts`, `time.ts`, `odds-cache.ts`, `jra.ts`
- Hot worker odds fetch: `apps/sync-realtime-data-hot/src/fetch-odds.ts`, `plan.ts`
- Python serve path: `apps/finish-position-predict-container/src/realtime_odds_fetcher.py`, `pipeline_runner.py`
- Feature SQL: `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py` lines 450-509
- Skew tax measurements: `docs/finish-position-accuracy/history/rootcause-i4-serve-skew-tax.md`
- Design JSON (not git-tracked): `tmp/rootcause/serveskew_design.json`
