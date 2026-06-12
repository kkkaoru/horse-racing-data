# Exotic Odds Availability Audit

**Date:** 2026-06-12  
**Scope:** 馬連/wide/馬単/3連複/3連単 — warehouse coverage + production-serve availability  
**Verdict at a glance:** All 5 exotic types exist in warehouse with good coverage. All 5 are already stored and served by the hot DO/D1 worker. The **only gap is in the prediction container's Python fetcher**, which discards them today. Zero infrastructure changes needed — all required work is in `realtime_odds_fetcher.py` and the feature builder.

---

## 1. Table → Bet Type Mapping

### JVD (JRA, keibajo_code 01–10)

| Table    | Bet type (JP)      | Bet type (EN)              | Notes                                |
| -------- | ------------------ | -------------------------- | ------------------------------------ |
| `jvd_o1` | 単勝 / 複勝 / 枠連 | Tansho / Fukusho / Wakuren | Three types packed in one table      |
| `jvd_o2` | 馬連               | Umaren (Quinella)          | Unordered pair                       |
| `jvd_o3` | ワイド             | Wide (Quinella-place)      | Unordered pair, range odds (min/max) |
| `jvd_o4` | 馬単               | Umatan (Exacta)            | Ordered pair                         |
| `jvd_o5` | 3連複              | Sanrenpuku (Trio)          | Unordered triple                     |
| `jvd_o6` | 3連単              | Sanrentan (Trifecta)       | Ordered triple                       |

### NVD (NAR + Ban-ei, keibajo_code 30–48, 83 + some 50–58, 81–84)

| Table    | Bet type (JP)      | Bet type (EN)              | Notes                                       |
| -------- | ------------------ | -------------------------- | ------------------------------------------- |
| `nvd_o1` | 単勝 / 複勝 / 枠連 | Tansho / Fukusho / Wakuren | Same packing as jvd_o1                      |
| `nvd_o2` | 馬連               | Umaren (Quinella)          |                                             |
| `nvd_o3` | ワイド             | Wide (Quinella-place)      | Not all NAR venues; coverage grew 2012–2013 |
| `nvd_o4` | 馬単               | Umatan (Exacta)            |                                             |
| `nvd_o5` | 3連複              | Sanrenpuku (Trio)          | Introduced at NAR ~2010                     |
| `nvd_o6` | 3連単              | Sanrentan (Trifecta)       | Introduced at NAR ~2010                     |
| `nvd_oa` | 枠単               | Wakutan (Frame-Exacta)     | NAR only, ~81k rows 2005–2026               |

---

## 2. Odds Encoding

All odds are stored as a **single fixed-width packed string** in one column per bet type — there are no per-combination rows. Widths:

| Table | Column            | Chars/combo | Max combos (18-horse field)          |
| ----- | ----------------- | ----------- | ------------------------------------ |
| o2    | `odds_umaren`     | 13          | C(18,2) = 153                        |
| o3    | `odds_wide`       | 17          | C(18,2) = 153 (carries min+max odds) |
| o4    | `odds_umatan`     | 13          | P(18,2) = 306                        |
| o5    | `odds_sanrenpuku` | 15          | C(18,3) = 816                        |
| o6    | `odds_sanrentan`  | 17          | P(18,3) = 4896                       |

Decode: `SUBSTRING(odds_col, 1 + (k-1)*N, N)` for combination index k (0-based). Horse numbers are zero-padded 2-char strings within each entry.

---

## 3. Warehouse Coverage Matrix

### JRA (jvd_o1–o6)

| Bet type            | First year | Last year | Confirmed rows (data_kubun≠9) |
| ------------------- | ---------- | --------- | ----------------------------- |
| 単勝/複勝/枠連 (o1) | 1993       | 2026      | 113,974                       |
| 馬連 (o2)           | 1993       | 2026      | 111,311                       |
| ワイド (o3)         | **1999**   | 2026      | 91,072                        |
| 馬単 (o4)           | **2002**   | 2026      | 82,731                        |
| 3連複 (o5)          | **2002**   | 2026      | 82,696                        |
| 3連単 (o6)          | **2004**   | 2026      | 66,226 (partial 2004–2008)    |

Full density for jvd_o6 begins 2009.

### NAR + Ban-ei (nvd_o1–o6)

| Bet type            | First year | Last year | Total rows (data_kubun=5) | Notes                                     |
| ------------------- | ---------- | --------- | ------------------------- | ----------------------------------------- |
| 単勝/複勝/枠連 (o1) | 2005       | 2026      | 291,737                   | **2024 missing** (sync gap)               |
| 馬連 (o2)           | 2005       | 2026      | 291,729                   | **2024 missing** (sync gap)               |
| ワイド (o3)         | 2005       | 2026      | 244,459                   | **2024 missing**; not all venues offer it |
| 馬単 (o4)           | 2005       | 2026      | 291,729                   | **2024 missing** (sync gap)               |
| 3連複 (o5)          | **2010**   | 2026      | 240,920                   | 2024 present                              |
| 3連単 (o6)          | **2010**   | 2026      | 241,096                   | 2024 present                              |

Note: nvd_o1–o4 are missing 2024 entirely (ingest gap, not a bet-type gap). nvd_o5/o6 are intact through 2026.

### Ban-ei (keibajo_code = 83, within nvd tables)

Ban-ei is a subset of the nvd tables. Ban-ei has ワイド/3連複/3連単 introduced later (~2011); 馬連/馬単 present from 2005.

---

## 4. Final Odds vs Snapshots

**All warehouse tables hold final (kakutei) confirmed odds only.** Evidence:

- `happyo_tsukihi_jifun = '00000000'` on all confirmed rows (the intra-day announcement timestamp field is zeroed, meaning these are post-confirmation records).
- Exactly one row per race per table (no time-series duplication).
- `data_kubun = '5'` = normal confirmed; `'9'` = cancelled race.

Live intra-day snapshots would go into `apd_sokuho_o1`–`apd_sokuho_o6`. Those tables **currently have 0 rows** — the live feed is not running.

---

## 5. Production Serve Path — Current State

The daily prediction pipeline (launchd cron, runs via Docker):

```
pipeline_runner.py
  └─ build_upcoming_feature_rows()
       └─ fetch_realtime_odds_parquet()
            └─ fetch_odds_for_race(race_key)
                 └─ GET https://sync-realtime-data-hot.kkk4oru.com/api/odds/{raceKey}
                      → response["latest"] contains ALL 8 types
                 └─ extract_rows()  ← reads ONLY response["latest"]["tansho"]
                      → output: tansho_odds_realtime, ninkijun_realtime, bataiju_realtime
```

**The hot worker already stores and serves all 8 types.** The gap is exclusively in `extract_rows()` in `realtime_odds_fetcher.py`.

### Hot Worker Storage (already live)

| Layer               | What's stored                                                                                     |
| ------------------- | ------------------------------------------------------------------------------------------------- |
| D1 `odds_snapshots` | `odds_type TEXT` column — all 8 `OddsType` values written                                         |
| OddsCacheHot DO     | `latest: Partial<Record<OddsType, OddsData[]>>` — all 8 types in DO storage                       |
| JRA scraper         | `ODDS_PAGE_LABELS` iterates all 8 types via Playwright                                            |
| NAR scraper         | `ODDS_TYPES` array lists all 8: tansho, fukusho, wakuren, umaren, umatan, wide, 3renpuku, 3rentan |

---

## 6. Summary Matrix

| Bet type         | Warehouse (JRA)     | Warehouse (NAR)                     | Warehouse (Ban-ei) | Serve-time (hot DO API) | Served to model today |
| ---------------- | ------------------- | ----------------------------------- | ------------------ | ----------------------- | --------------------- |
| 単勝 tansho      | ✓ full (1993–)      | ✓ (2005–, 2024 gap)                 | ✓ (2005–)          | ✓                       | ✓                     |
| 複勝 fukusho     | ✓ full (1993–)      | ✓ (2005–, 2024 gap)                 | ✓ (2005–)          | ✓                       | ✗                     |
| 馬連 umaren      | ✓ full (1993–)      | ✓ (2005–, 2024 gap)                 | ✓ (2005–)          | ✓                       | ✗                     |
| ワイド wide      | ✓ (1999–)           | ✓ (2005–, partial venues, 2024 gap) | ✓ (~2011–)         | ✓                       | ✗                     |
| 馬単 umatan      | ✓ (2002–)           | ✓ (2005–, 2024 gap)                 | ✓ (2005–)          | ✓                       | ✗                     |
| 3連複 sanrenpuku | ✓ (2002–)           | ✓ (2010–)                           | ✓ (~2011–)         | ✓                       | ✗                     |
| 3連単 sanrentan  | ✓ (2004/full 2009–) | ✓ (2010–)                           | ✓ (~2011–)         | ✓                       | ✗                     |
| 枠連 wakuren     | ✓ full (1993–)      | ✓ (2005–, 2024 gap)                 | ✓ (2005–)          | ✓                       | ✗                     |

Legend: ✓ = available, ✗ = not used/fetched

---

## 7. Verdict

### (a) Buildable for training (warehouse)?

**Yes, for all 5 exotic types.** Final confirmed odds for umaren, wide, umatan, sanrenpuku, sanrentan all exist in the warehouse. Coverage is dense from 2002+ (JRA) and 2010+ (NAR for sanrenpuku/sanrentan). Training data can be joined to `jvd_se`/`nvd_se` on `(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)` with `WHERE data_kubun IN ('5')`.

Key caveats for feature design:

- Odds are stored as fixed-width packed strings — decoding requires substring arithmetic, not a simple column join. A SQL UDF or Python unpacking step is needed.
- ワイド (wide) carries min+max odds per combination, not a single value.
- sanrentan has up to 4896 combinations per race — aggregating to a per-horse summary (e.g. min odds among all triples containing this horse in pos-1/2/3) is feasible but compute-intensive.
- The 2024 NAR gap in nvd_o1–o4 (馬連/馬単/ワイド) is a known ingest issue, not structural.

### (b) Serveable in production?

**Yes, with a Python-only change — no infrastructure work needed.**

The hot DO/D1 worker already scrapes, stores, and serves all 8 bet types on every odds update cycle. The API response `GET /api/odds/{raceKey}` already returns `latest.umaren`, `latest.wide`, `latest.umatan`, `latest["3renpuku"]`, `latest["3rentan"]`.

Required changes (all in the prediction container):

1. **`apps/finish-position-predict-container/src/realtime_odds_fetcher.py`**  
   Extend `extract_rows()` to also parse pair/triple types from `response["latest"]`. Add new output columns (e.g. `umaren_top1_odds`, `umaren_top1_ninkijun`, per-horse exotic min-odds aggregates, etc.). Extend `_RealtimeRow` namedtuple / output parquet schema.

2. **`apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py`**  
   Expose the new parquet columns in the base SELECT that feeds the model. Add `COALESCE(rt.umaren_xxx, ...)` columns.

3. **Retrain** with new features.

No changes to `apps/sync-realtime-data-hot/`, no D1 schema migrations, no worker deploys.

---

## 8. Key File Paths

| File                                                                  | Relevance                                                                          |
| --------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `apps/finish-position-predict-container/src/realtime_odds_fetcher.py` | The only file that needs changing for serve-side exotic odds                       |
| `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py` | Feature builder — add new realtime columns                                         |
| `apps/sync-realtime-data/migrations/0001_init.sql`                    | D1 schema: `odds_snapshots` with `odds_type TEXT` (already stores all types)       |
| `apps/sync-realtime-data-hot/src/odds-cache.ts`                       | Hot DO: `latest: Partial<Record<OddsType, OddsData[]>>` (all types already served) |
| `apps/sync-realtime-data-hot/src/jra.ts`                              | JRA Playwright scraper — 8 types in `ODDS_PAGE_LABELS`                             |
| `apps/sync-realtime-data-hot/src/keiba-go.ts`                         | NAR scraper — 8 types in `ODDS_TYPES` array                                        |
| `packages/horse-racing-realtime/src/types.ts`                         | Shared `OddsType` union definition                                                 |
| `apps/local-postgresql/docs/pc-keiba-postgresql-reference.md`         | Full warehouse schema reference (jvd_o1–o6, nvd_o1–o6 column specs)                |
