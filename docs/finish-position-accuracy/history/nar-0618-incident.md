# NAR 6/18 Accuracy Crash — Incident Report

**Date**: 2026-06-18 (JST)
**Category**: NAR finish-position predictions
**Severity**: Catastrophic — top1 collapsed to 16.7% vs baseline ~33% and normal ~58%

---

## Quantified Impact

| Metric                    | 6/17 (baseline, post-race odds) | 6/18 (incident) | Drop              |
| ------------------------- | ------------------------------- | --------------- | ----------------- |
| Top1                      | 33.3% (16/48 races)             | 16.7% (8/48)    | −16.7pp           |
| Place2                    | 56.2% (27/48)                   | 22.9% (11/48)   | −33.3pp           |
| Place3                    | 66.7% (32/48)                   | 29.2% (14/48)   | −37.5pp           |
| Avg within-race score std | 0.3614                          | 0.3057          | −0.0557 (flatter) |

Normal NAR top1 ~58% (from WF evaluation). The 6/18 crash is −41pp vs normal and −16.7pp vs 6/17.

Note: 6/17 baseline itself is below normal (33.3%) because that day's predictions were generated at
20:45 JST (post-race) with full real odds; the comparison is valid for same-day timing differences.

---

## Median-Odds Fallback: Confirmed

The 6/18 predictions were **entirely generated at 2026-06-17T18:04 UTC = 03:04 JST 6/18** (the 03:00
JST NAR daily cron). NAR tansho odds open ~09:00-10:00 JST. At 03:04 JST, zero odds rows exist in
D1/Hyperdrive, so the pipeline emits:

```
[realtime-odds] zero rows collected category=nar target_date=20260618 races=48 — using null-odds fallback
```

With null-odds fallback, `odds_score` is set to the median constant within each race, completely
destroying the within-race odds ranking signal. Since NAR odds are the dominant predictor, this
collapses the model to near-random within-race ordering.

The within-race score std dropped from 0.3614 (6/17, real odds) to 0.3057 (6/18, null-odds), a
measurable but not fully quantified signal flatness increase. The score distribution still shows
some spread from non-odds features, but the ranking is fundamentally compromised.

**Smoking gun**: every single one of the 48 races on 6/18 has prediction_generated_at = 2026-06-17
18:04 UTC (a 3-second burst, one timestamp per race). No later prediction refresh ever occurred.

---

## Guard Refresh: DID NOT FIRE — Root Cause

The race-prediction-guard (every 20 min, 10:00-20:00 JST) is designed to re-kick predictions
during race hours to incorporate fresh odds. It **completely failed for all of 6/18 race hours**.

### Guard failure timeline (all UTC):

| Time (UTC)       | JST            | Event                                           |
| ---------------- | -------------- | ----------------------------------------------- |
| 2026-06-17T23:00 | 08:00 JST 6/18 | Last successful D1 query (EXPECTED_COUNT=48)    |
| 2026-06-18T00:01 | 09:01 JST 6/18 | First D1 "fetch failed" error                   |
| 2026-06-18T00:37 | 09:37 JST 6/18 | Docker also fails: DNS cannot resolve Neon host |
| 2026-06-18T01:14 | 10:14 JST 6/18 | Race-hours band begins — D1 still failing       |
| 2026-06-18T14:14 | 23:14 JST 6/18 | Last guard run — D1 still failing               |

**31 consecutive guard runs** (from 09:01 JST through the entire race day and evening) all returned:

```json
{ "error": { "text": "fetch failed" } }
```

The guard exits early with `return 1` when D1 query fails (before reaching the finish-position kick
logic), so zero race-hours refresh predictions were generated.

### Concurrent docker DNS failure (same window):

At 09:37 JST, an independent docker run also failed with:

```
psycopg.OperationalError: failed to resolve host 'ep-frosty-cloud-ao28v17l.c-2.ap-southeast-1.aws.neon.tech':
[Errno -2] Name or service not known
```

Both the host wrangler D1 `fetch failed` and the docker DNS failure at the same time confirm: the
**Mac lost internet connectivity starting ~09:01 JST 6/18** and remained offline for the entire
race day. This is not a Cloudflare D1 outage — it is local network loss.

---

## Comparison to 6/19 D1 Error (Different Root Cause)

The 6/19 guard shows a structurally different error:

```json
{
  "error": {
    "text": "In a non-interactive environment, it's necessary to set a CLOUDFLARE_API_TOKEN..."
  }
}
```

This means the wrangler OAuth token expired or was not available for the background guard process.
The token in `~/Library/Preferences/.wrangler/config/default.toml` expires at `2026-06-19T07:11:12Z`
(16:11 JST 6/19). This is a separate, ongoing issue from the 6/18 network outage.

---

## Deploy Correlation: NOT the Cause

The E-top2/lazy-Neon-connect deploy (commit `47598be` + `8062402`) completed at **02:11 JST 6/18**,
one hour before the 03:00 NAR cron. The cron ran successfully at 03:04 JST and produced 876 NAR
predictions (all 48 races covered). The lazy-connect change (`predict_upcoming.py`) did not break
the 03:04 cron.

The guard failures started at **09:01 JST** (6.5 hours after deploy). The causal link is:

- Deploy: 02:11 JST → OK
- Cron (pre-odds, null-fallback): 03:04 JST → RUNS but with no odds
- Network down: ~09:01 JST → guard blocked all day
- No race-hours re-prediction ever fired

The deploy is not the cause. The network outage is the sole cause of the guard failure.

---

## Coverage: Full (All 48 Races Predicted, Just Pre-Odds)

All 48 NAR races on 6/18 were predicted (501 horse rows). Coverage was complete. The problem is
entirely timing/odds quality, not missing races.

---

## Verdict: CHRONIC SERVE-SKEW + ACUTE NETWORK OUTAGE

This incident is the combination of two factors:

1. **Chronic structural issue**: The 03:00 JST NAR cron fires pre-odds. For NAR (where odds open
   09:00-10:00 JST), this is always a null-odds run. The guard was explicitly designed to re-kick
   during race hours to fix this. Under normal conditions (network up), the guard would have fired
   10+ times during 10:00-20:00 JST and refreshed predictions with real odds.

2. **Acute trigger**: Mac network went down at ~09:01 JST, just before odds opened. This prevented
   all 31 guard runs from reaching the D1 query step, and the guard exits on D1 error before
   kicking the finish-position pipeline.

**This is NOT a new chronic problem.** Prior days with normal network had the guard successfully
kicking race-hours refreshes. For example, 6/17 NAR predictions were generated at 20:45 JST
(post-race) with full odds — evidence of a successful late-day guard kick.

The underlying scheduling risk (03:00 cron always pre-odds for NAR) is mitigated entirely by the
guard. When the network is up, the guard provides the fix. When the network is down for the full
race day, the fix does not run.

---

## Recommended Fix

### Immediate (next race day with network issues)

If network was down: manually re-run finish-position predictions post-odds once network restores:

```bash
RUN_DATE=YYYYMMDD PREDICT_DAYS_AHEAD=0 bash scripts/launchd/finish-position-predict-daily.sh
```

### Structural Fix 1: Guard should not hard-exit on D1 error for finish-position (HIGH PRIORITY)

Current behavior: `guard_target` returns 1 on D1 parse error, skipping both running-style AND
finish-position kicks. This means a D1 outage blocks the finish-position refresh even though
the finish-position kick doesn't use D1 at all — it queries Neon for actual/expected counts.

Fix: decouple the finish-position guard from the D1 `expected_count` query. For the race-hours
freshness re-prediction path, the guard should kick the finish-position pipeline regardless of
whether D1 is available, since the sole purpose is to refresh with current odds/bataiju.

Specifically: when `is_race_hours=1`, do not require a valid `expected_count` from D1. Instead,
always attempt the finish-position kick (the FINISH_LOCK_DIR lock still prevents concurrent runs).

```bash
# In guard_target(), for is_race_hours=1:
# Skip the D1 query and always kick finish-position
if [ "$is_race_hours" = "1" ]; then
  log "race-hours mode: kicking finish-position regardless of D1 status"
  # ... kick finish-position ...
fi
```

### Structural Fix 2: Add a post-odds NAR re-run without D1 dependency (MEDIUM PRIORITY)

Add a dedicated launchd timer that fires at JST 09:30 and 10:00 specifically for NAR predictions,
independent of the guard. This timer queries Neon only (no D1) to check if fresh odds exist, then
kicks the pipeline. It is self-contained and not blocked by D1 failures.

### Note on 6/19 Wrangler Token Issue (Separate)

The OAuth token `expiration_time = 2026-06-19T07:11:12.385Z` means the guard is already failing on
6/19 for a different reason. Running `bunx wrangler login` (interactive) or setting
`CLOUDFLARE_API_TOKEN` in the environment will fix 6/19+. This is urgent before the next race day.
