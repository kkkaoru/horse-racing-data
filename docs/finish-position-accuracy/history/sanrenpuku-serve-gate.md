# NAR sanrenpuku serve-gate: 03:00 JST availability audit

**Date**: 2026-06-13
**Blocking**: NAR sanrenpuku exotic feature (exotic_sanrenpuku_p3) production deploy — task #288
**Verdict**: STOP — structural NULL mismatch at serve time

---

## 1. Worker storage findings

### Hot worker architecture (`apps/sync-realtime-data`)

- **Bet types scraped**: all 8 — `tansho`, `fukusho`, `umaren`, `umatan`, `wakuren`, `wide`, `3renpuku`, `3rentan`
- **Sanrenpuku storage key**: `odds_type = '3renpuku'` (not `sanrenpuku`)
- **Schema** (`odds_snapshots` D1 table):

  ```sql
  race_key TEXT        -- e.g. nar:2026:0612:30:11
  fetched_at TEXT      -- ISO-8601 JST, e.g. 2026-06-12T10:00:15+09:00
  odds_type TEXT       -- '3renpuku' for sanrenpuku
  combination TEXT     -- horse triplet e.g. '1-3-7'
  odds REAL / min_odds / max_odds / average_odds REAL
  rank INTEGER
  ```

- **Scraping trigger**: `getNarOddsFetchSlotAt` in `src/time.ts` — slots start at `NAR_ODDS_SALE_START` (same-day 10:00 JST default; 12:00 JST for night-race venues 48, 54), then hourly until 60 min pre-race, then 10-min intervals, then 1-min intervals, then final odds at race_start + 2 min.
- **No advance (前日) NAR odds**: `getJraAdvanceOddsFetchSlotAt` exists for JRA (−19 h pre-race-day) but has **no NAR equivalent**. NAR odds are same-day only.
- **Sources**: `src/time.ts` lines 8-15 (sale start), 131-156 (fetch slots); `src/keiba-go.ts` lines 33-51 (link-text→type map); `migrations/0001_init.sql` lines 22-40 (schema).

---

## 2. Empirical availability table

D1 database: `sync-realtime-data` (hot worker, remote).
Sample period: 2026-05 through latest available rows (~4.58 M rows total).
NAR tracks identified by `race_key LIKE 'nar:%'`.

### First-snapshot timing by venue (JST, same race day)

| Venue (keibajo_code) | First 3renpuku snapshot | Notes            |
| -------------------- | ----------------------- | ---------------- |
| 47 Morioka           | ~10:00 JST              | Standard NAR     |
| 42 Urawa             | ~10:00 JST              | Standard NAR     |
| 50 Kasamatsu         | ~10:00 JST              | Standard NAR     |
| 35 Mito              | ~10:03 JST              | Standard NAR     |
| 30 Ohi               | ~12:01 JST              | Night-race venue |
| 83 Ban-ei/Obihiro    | ~12:03 JST              | Night-race venue |

**Zero snapshots observed before 10:00 JST for any NAR track.**

### Availability at predict-time windows

| Time window              | NAR 3renpuku available? | Fraction of races with snapshot |
| ------------------------ | ----------------------- | ------------------------------- |
| 03:00 JST (launchd cron) | **NO**                  | ~0%                             |
| 09:00 JST                | NO                      | ~0%                             |
| 10:00–10:10 JST          | YES (standard venues)   | ~100% of non-night races        |
| 12:00–12:10 JST          | YES (all venues)        | ~100%                           |

---

## 3. Train vs. serve NULL comparison

| Context                                            | Sanrenpuku NULL rate            |
| -------------------------------------------------- | ------------------------------- |
| Training data (nvd_o5, data_kubun='5', FINAL odds) | ~0–1.4%                         |
| Serve at 03:00 JST launchd cron                    | **~100%** (not yet published)   |
| Serve at race-guard hourly cron (09:00–12:00 JST)  | ~100% until 10:00 JST; then ~0% |

**Mismatch magnitude**: training saw ~99% non-null; 03:00 JST serve sees ~0% non-null.
This is the same structural failure mode documented in the g1f1 lesson (NAR G-1+F1 combined adopt 2026-06-12): GBDT trained on informative-presence flips to informative-absence routing at serve → observed −0.63 pp top1 degradation pattern.

---

## 4. Root cause

NAR sanrenpuku odds are **not available before race day morning**. There are no advance odds (前日オッズ) for NAR on keiba.go.jp — the worker's scraping architecture reflects this correctly. The odds pool opens at 10:00 JST (standard) or 12:00 JST (night venues) on race day itself. The NAR prediction cron runs at **03:00 JST** (launchd `com.kkk4oru.finish-position-predict.plist`), which is 7–9 hours before any sanrenpuku data exists in D1.

The training set used `data_kubun='5'` (FINAL odds), captured after races run. Final odds have ~0% NULL because every completed race eventually has its odds published. At serve (03:00 JST), the feature is always NULL — a distribution shift the model has not been trained to handle as an uninformative absence.

---

## 5. Verdict: STOP

**exotic_sanrenpuku_p3 must NOT be deployed to NAR production.**

- Serve-time availability at predict time (03:00 JST): **~0%**
- Train-time NULL rate: **~0–1.4%**
- Mismatch: ~99 pp
- Risk: identical to g1f1 pattern — GBDT routes NULL as informative absence, hurting predictions

### Could a later cron rescue this?

A CONDITIONAL-GO would require a re-predict run after 10:00 JST when sanrenpuku odds are available. The race-guard hourly cron could in principle re-run predictions with non-null sanrenpuku features. However:

1. Most NAR races run in the afternoon/evening; a 10:00–12:00 JST refresh is feasible timing-wise.
2. But the cron infrastructure for a second NAR predict pass at 10:00 JST does not currently exist.
3. Even if built, night-race venues (Ohi keibajo_code=30, Ban-ei=83) do not have odds until 12:00 JST, requiring yet another pass.
4. Implementing, testing, and validating this multi-pass predict pipeline is out of scope for the current feature increment.

**Decision**: STOP. Do not deploy `exotic_sanrenpuku_p3` to NAR production until either (a) a 10:00 JST re-predict cron is implemented and validated, or (b) NAR advance sanrenpuku odds become available via an alternative data source.

---

## 6. JRA / Ban-ei status (out of scope but noted)

This audit was NAR-specific. JRA uses advance odds (`getJraAdvanceOddsFetchSlotAt`, −19 h) so JRA sanrenpuku is available at 03:00 JST. Ban-ei (keibajo_code=83) follows NAR scraping cadence (same-day 12:00 JST) and has the same NULL mismatch risk as NAR — also STOP until re-predict infrastructure exists.
