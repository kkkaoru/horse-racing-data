# Odds Dynamics (Drift/Momentum) — Data Feasibility — 2026-06-11

**Scope**: Can historical odds time-series (intraday snapshots) be acquired for the 21-year training set? Covers JRA O1-O6 (JV-Data), NAR nvd_o1-o6, hot D1 odds_snapshots, vote counts (H1/H6).
**Method**: PG read-only schema audit (127 tables, port 15432) + git history + web research (JRA-VAN developer forum, PC-Keiba docs).
**Investigator**: Feasibility SubAgent (claude-sonnet-4-6), 2026-06-11.

---

## Executive Summary

**DEFINITIVE: 21-year historical odds time-series is NOT reconstructable for training.**

The three data sources that contain any intraday odds snapshots are:

| Source                              | Table                     | History depth                                         | JRA | NAR    | Verdict                        |
| ----------------------------------- | ------------------------- | ----------------------------------------------------- | --- | ------ | ------------------------------ |
| JV-Link sokuho O1/O2 (backfillable) | `apd_sokuho_o1/o2`        | Max ~1 yr (official); ~2003 possible but unguaranteed | YES | **NO** | NO-GO training                 |
| Hot D1 odds_snapshots               | Cloudflare D1 (not in PG) | From 2026-05-29 only                                  | YES | YES    | ~2 weeks at investigation      |
| JV-Link final odds                  | `jvd_o1`, `nvd_o1`        | 1993/2005–2026                                        | YES | YES    | No drift, single snapshot only |

None of these can supply a drift signal for 21 years of JRA + NAR races jointly.

---

## 1. PG Schema Audit — Odds Tables Inventory

### 1a. Final odds (confirmed, data_kubun=5)

| Table                            | Rows      | Year range | Per-race   | Time-series? |
| -------------------------------- | --------- | ---------- | ---------- | ------------ |
| `jvd_o1` (JRA win/place/bracket) | 113,974   | 1993–2026  | 1 row/race | **NO**       |
| `jvd_o2` (JRA quinella)          | 111,413   | 1993–2026  | 1 row/race | NO           |
| `jvd_o3` (JRA wide)              | 91,174    | 1999–2026  | 1 row/race | NO           |
| `jvd_o4` (JRA umatan)            | 82,833    | 2002–2026  | 1 row/race | NO           |
| `jvd_o5` (JRA 3renpuku)          | 82,797    | 2002–2026  | 1 row/race | NO           |
| `jvd_o6` (JRA 3rentan)           | 66,321    | 2004–2026  | 1 row/race | NO           |
| `nvd_o1` (NAR win/place/bracket) | 292,309   | 2005–2026  | 1 row/race | NO           |
| `nvd_o2`                         | 292,311   | 2005–2026  | 1 row/race | NO           |
| `nvd_o3–o6`                      | 241K–292K | 2005–2026  | 1 row/race | NO           |

All `jvd_o*` and `nvd_o*` have `data_kubun=5` only (confirmed final odds). No intraday duplicates per race. The `happyo_tsukihi_jifun` field is `"00000000"` for all historical rows — meaning the timestamp was not recorded in these archived snapshots. **These tables contain zero drift information.**

The data is identical to what is already accessed via `jvd_se.tansho_odds` / `jvd_se.tansho_ninkijun` (the current `odds_score` / `popularity_score` source).

### 1b. Intraday time-series snapshots (PC-Keiba sokuho)

| Table              | Rows  | Race days      | Races | Notes                          |
| ------------------ | ----- | -------------- | ----- | ------------------------------ |
| `apd_sokuho_o1`    | 2,772 | 1 (2026-05-10) | 37    | ~75 snaps/race, 5-min interval |
| `apd_sokuho_o2`    | 2,765 | 1 (2026-05-10) | 37    | same day                       |
| `apd_sokuho_o3–o6` | 0     | —              | —     | empty                          |

**Key findings:**

- `data_kubun=1` in `apd_sokuho_o1` = intermediate/live snapshot (correct for time-series)
- `happyo_tsukihi_jifun` encodes `MMDDHHII` (month/day/hour/minute): confirms true timestamp per snapshot
- Snapshots span from ticket-open to T-3h before race: drift window is **~2–2.4 days** for first race, ~0.5 days for later races
- Per-race snapshot count: 55–382 (median ~75), at ~5-minute intervals
- Only 1 race day in local PG: no historical backfill has been performed

### 1c. Vote counts (H1/H6)

| Table    | Rows  | Year range                    | Per-race time-series?                         |
| -------- | ----- | ----------------------------- | --------------------------------------------- |
| `jvd_h1` | **0** | —                             | — (JRA vote counts not imported)              |
| `jvd_h6` | **0** | —                             | —                                             |
| `nvd_h1` | 1,717 | 2026-05-02 to 2026-06-08 only | NO (1 row/race, data_kubun=4 = pre-confirmed) |
| `nvd_h6` | 0     | —                             | —                                             |

`nvd_h1` has per-horse vote counts (`hyosu_tansho`) for 38 NAR race days in 2026. No historical depth. No intraday time-series.

---

## 2. JV-Data O1–O6 Source Specification

**Record types:** O1 (win/place/bracket odds), O2 (quinella), O3 (wide), O4 (umatan), O5 (3renpuku), O6 (3rentan). All are populated into `apd_sokuho_o1–o6` by PC-Keiba via JV-Link `JVRTOpen` (real-time) and a separate "historical sokuho" function.

**PC-Keiba historical sokuho import rules** (sourced from pc-keiba.com/wp/jikeiretsu-odds/ and forum threads):

- Historical time-series is only available for **O1 (単複枠) and O2 (馬連)** — O3–O6 have NO historical archive.
- Official JV-Link provision period: **past 1 year**.
- Practical access: reportedly ~2003 onward, but JRA-VAN explicitly states "not guaranteed beyond 1 year" (developer.jra-van.jp/t/topic/470).
- NAR (nvd_o\*): **no historical sokuho** provided through JV-Link. NAR sokuho is real-time only on race day.
- Frequency: O1 updates approximately every 5–10 minutes per race (varies by venue count and active race window).

**Conclusion:** Even under best-case JV-Link backfill, the maximum reconstructable training window is:

- JRA: ~1 year (O1/O2 only), ~4,961 races, ~372K snapshots
- NAR: **0 years** — no historical sokuho path exists

---

## 3. Hot D1 odds_snapshots — Recent-Only Window

**System:** `sync-realtime-data-hot` Cloudflare Worker, D1 database `odds_snapshots`.

- Worker skeleton created: **2026-05-29** (git commit `0df0452`)
- Operational start: approximately 2026-05-29
- At investigation date (2026-06-11): **~13 days** of data
- Snapshot interval: ~1 minute during race hours (Gate 1–7 architecture)
- Coverage: JRA + NAR, all bet types
- Data is NOT mirrored to local PG; R2 archive preserves rows >7 days

**Window at investigation:** ~0.4 months. This system will grow forward but can never backfill 21 years.

---

## 4. Quantification: Recent-Only Model Variants

### Option A: Inference-time-only odds-drift signal (NO training needed)

The current model already receives `odds_score` (final odds at inference time) from the hot D1 worker. Drift features — e.g., `open_to_final_drift = final_odds / open_odds` or `rank_shift = final_rank - open_rank` — could be computed purely at inference using the hot D1 time-series and COALESCEd into the existing feature vector, **without model retrain**. This requires no training data.

- **Feasibility:** HIGH — inference pipeline already fetches from hot D1; adding 1–2 more derived scalars is a pipeline change, not a model change
- **Risk:** These features would be NULL for historical/backtest evaluation; the model never learned to use them
- **Benefit:** Captures smart-money signal at prediction time for upcoming races
- **Recommended path:** Encode as `odds_open_drift` and `odds_open_rank_shift` in the upcoming-race branch of `finish_position_features_duckdb.py`, COALESCE with NULL fallback for historical

### Option B: Forward-accumulate and train in 12–24 months

Start archiving all `apd_sokuho_o1` data from today forward using PC-Keiba's daily sokuho registration. After 12–18 months:

- JRA: ~5,000–7,000 races with full drift time-series (O1/O2)
- NAR: still no sokuho archive; can use hot D1 (1-min interval) as proxy going forward
- Drift features become trainable for JRA; NAR requires a separate model or imputation

Storage: ~372K rows/year for JRA O1+O2 (manageable). PC-Keiba daily update is already running.

### Option C: JV-Link 1-year backfill for JRA only

Execute PC-Keiba "過去の時系列オッズ登録" for O1+O2 from 2025-06-11 to 2026-06-10. Yields ~4,961 JRA races, ~372K snapshots. Can train a JRA-only drift feature. NAR (21,313 races/yr) would receive NaN → median imputation, diluting the signal.

**Effort:** Medium (one-time PC-Keiba operation, requires Windows Parallels). Mixed JRA/NAR training with 81% NAR NaN rate makes this a weak lever.

---

## 5. Ranked Candidates — GO/NO-GO

| Rank | Candidate                                    | 21yr?     | JRA     | NAR       | Effort        | Training GO?    | Inference GO? |
| ---- | -------------------------------------------- | --------- | ------- | --------- | ------------- | --------------- | ------------- |
| 1    | **A: Inference-time drift (hot D1)**         | N/A       | YES     | YES       | LOW           | —               | **GO**        |
| 2    | **B: Forward-accumulate sokuho**             | NO→grows  | YES     | partial   | LOW (ongoing) | GO in 12–18mo   | GO now (JRA)  |
| 3    | **C: 1yr JV-Link backfill O1/O2 (JRA only)** | NO (1yr)  | YES     | **NO**    | MEDIUM        | WEAK (JRA-only) | —             |
| 4    | jvd_h1 vote counts                           | NO        | NO      | NO (2026) | MEDIUM        | NO-GO           | NO-GO         |
| 5    | Third-party scraping                         | uncertain | partial | partial   | VERY HIGH     | NO-GO           | NO-GO         |

**Final verdict:**

- **C1 (training a 21-year drift feature): NO-GO.** No viable historical archive exists.
- **C2 (inference-time signal): GO.** Hot D1 already running; drift ratio computable from existing endpoint with minimal code change.
- **C3 (forward accumulate): PROCEED IN BACKGROUND.** Enable daily PC-Keiba sokuho registration; revisit as a training feature when 12+ months of JRA data exists.

---

## 6. Supporting Sources

- JRA-VAN developer forum — sokuho 1-year limit confirmed: https://developer.jra-van.jp/t/topic/470
- JRA-VAN developer forum — O1 timestamp field usage: https://developer.jra-van.jp/t/topic/613
- PC-Keiba manual — apd_sokuho_o1–o6 table mapping + historical limit: https://pc-keiba.com/wp/jikeiretsu-odds/
- PC-Keiba forum — O3–O6 no historical data confirmed by admin: https://pc-keiba.com/wp/forums/topic/速報系データのオッズ情報更新について/
- TARGET frontierJV FAQ — ~2003 practical limit, not guaranteed: https://targetfaq.jra-van.jp/faq/detail?site=SVKNEGBV&category=47&id=667
- PG direct audit (2026-06-11): apd_sokuho_o1 = 1 race day; jvd_o1/nvd_o1 = final only; jvd_h1 = empty
- git log: hot worker 0df0452 created 2026-05-29
