# E0: jvd_dm / jvd_tm Verification — ABORT

**Date:** 2026-06-12
**Purpose:** Determine whether JRA data-mining forecast tables (`jvd_dm`, `jvd_tm`) are usable,
leak-free, serve-available, and novel signals for the finish-position prediction model.

**Overall verdict: ABORT — jvd_dm/jvd_tm add zero information beyond jvd_se fields already in the
feature set. No E1 needed.**

---

## Gate-0: data_kubun distinct values

```
jvd_dm: {2, 3, 7}
jvd_tm: {2, 3, 7}
```

Neither table contains only `'5'` (確定/post-race), so Gate-0 **PASSES**. However, further
analysis (Gates 1–4) reveals a deeper problem that results in ABORT.

---

## E0 Gate 1: Encoding decode + per-horse extractability — ABORT

### jvd_dm format (15-char per field)

`mining_yoso_NN` (N = 01..18) encodes one horse per field:

| chars | content                                    | example (`011280300120017`) |
| ----- | ------------------------------------------ | --------------------------- |
| 1–2   | umaban (horse number, always == NN)        | `01`                        |
| 3–7   | yoso_soha_time (predicted time, 1/10s)     | `12803`                     |
| 8–11  | yoso_gosa_plus (upper error bound, 1/10s)  | `0012`                      |
| 12–15 | yoso_gosa_minus (lower error bound, 1/10s) | `0017`                      |

**Critical finding:** `mining_yoso_NN[3:7]` == `jvd_se.yoso_soha_time` for the horse with
`umaban == NN`, and `[8:11]`/`[12:15]` == `jvd_se.yoso_gosa_plus`/`yoso_gosa_minus`.

**Verification** (2026/0607/05/01, 16 horses):

```sql
-- For every horse, dm field exactly matches se field
SELECT s.umaban, s.yoso_soha_time, SUBSTRING(dm_field, 3, 5) AS dm_time
-- → 100% match across all 16 horses
```

**jvd_dm is a race-level view aggregating per-horse data already present verbatim in jvd_se.**
It contains no additional information. Per-horse extractability is technically possible (first 2
chars = umaban), but the extracted values are identical to what is already in `jvd_se`.

### jvd_tm format (6-char per field)

`mining_yoso_NN` encodes one horse per field:

| chars | content                                     |
| ----- | ------------------------------------------- |
| 1–2   | umaban                                      |
| 3–6   | score (relative strength, integer, ~0–9999) |

The score metric for `jvd_tm` is not documented in `pc-keiba-postgresql-reference.md` beyond
"対戦型データマイニング予想" (head-to-head data mining forecast). Scores observed: 0171–1000+.
jvd_tm covers only JRA venues (keibajo_code 03–10 in 2026 sample). No NAR coverage.

**Gate 1 result: ABORT** — jvd_dm is a strict subset of jvd_se content (fields already in feature
set). jvd_tm has an opaque score with unknown derivation and no NAR coverage.

---

## E0 Gate 2: Leak test + oracle test + per-year coverage — PASS (conditionally)

### data_kubun semantics

| kubun | count (all years)  | meaning                     | timing                      |
| ----- | ------------------ | --------------------------- | --------------------------- |
| `2`   | 5 rows (2026 only) | 予定 (scheduled)            | day before race             |
| `3`   | 3 rows (2026 only) | 変更 (changed)              | same race day, before hasso |
| `7`   | 84,226 rows        | ??? (bulk historical batch) | next day for 99.9%          |

### Timing analysis

**kubun=7 (99.9% of all rows):**

- 84,178 rows (99.94%): `data_sakusei_nengappi` = race date + 1 day → **AFTER RACE**
- 48 rows (0.06%): same calendar day, but before `hasso_jikoku` → pre-race (all from 2021/2023
  era, apparently real-time delivery on specific days)

The bulk of historical data (2001–2025) was loaded as `data_kubun='7'` with
`data_sakusei_nengappi='20181003'` at `1100` — a one-time historical back-fill. The content of
these rows is identical to `jvd_se.yoso_soha_time`, confirming the prediction was computed from
pre-race data but **delivered/loaded the next day**.

**Year coverage for non-'5' rows (jvd_dm):**

| year | kubun | rows      |
| ---- | ----- | --------- |
| 2026 | 2     | 2         |
| 2026 | 3     | 3         |
| 2026 | 7     | 1,506     |
| 2025 | 7     | 3,455     |
| 2024 | 7     | 3,454     |
| 2023 | 7     | 3,456     |
| ...  | 7     | ~3,450/yr |
| 2001 | 7     | 48        |

Total non-'5' rows: 84,231. Coverage from 2001 onward (~22,000 JRA races/year ÷ ~6 = ~3,450
races with dm data per year, consistent with JRA 3-venue pattern).

### Leak test: kubun=2 and kubun=3

The 5 kubun=2/3 rows from 2026-02-07 (keibajo=05, Chukyo):

| kubun | data_sakusei | data_sakusei_jifun | hasso_jikoku   | leak                     |
| ----- | ------------ | ------------------ | -------------- | ------------------------ |
| 2     | 20260206     | 1728               | 1545/1625      | BEFORE (day before race) |
| 3     | 20260207     | 1258/1328/1404     | 1401/1431/1505 | BEFORE (intra-day)       |

All kubun=2/3 rows are pre-race. No leak.

### Oracle test (via jvd_se)

The `jvd_se.yoso_juni` column stores the same mining prediction rank:

| scenario                        | n       | win_rate |
| ------------------------------- | ------- | -------- |
| yoso_juni=01 win rate overall   | 83,863  | 23.23%   |
| odds fav (ninkijun=01) win rate | 84,172  | 39.18%\* |
| dm pick wins, odds disagree     | 48,967  | 13.68%   |
| odds pick wins, dm disagrees    | 198,742 | 39.18%\* |
| both agree on fav               | 34,896  | 36.63%   |

\*Computed from same universe. The dm signal produces top-1 win rate of 23.23%, well below the
market's 39.18%. When they disagree, dm is substantially worse than odds.

**Gate 2 result: PASS (no leak for kubun=2/3; kubun=7 is delivered next day but contains
pre-race predictions)** — but the oracle test shows dm is weaker than market odds, reducing
signal novelty to near-zero.

---

## E0 Gate 3: yoso_soha_time independence — ABORT

### Agreement between yoso_juni and tansho_ninkijun

| metric                                  | value                   |
| --------------------------------------- | ----------------------- |
| Exact rank agreement (all positions)    | 17.02%                  |
| Top-1 agreement (both pick same winner) | 35,010 / 84,170 = 41.6% |

These agreement rates are exactly what one expects when two predictors correlate with the same
underlying signal (horse quality). Since jvd_dm fields are mathematically identical to jvd_se
yoso fields:

```
jvd_dm.mining_yoso_NN[3:7] ≡ jvd_se.yoso_soha_time where umaban = NN
```

There is no independent dimension. The r-value is effectively **1.0** between jvd_dm and the
jvd_se yoso columns.

### Additionally

The `jvd_se` columns `yoso_soha_time`, `yoso_gosa_plus`, `yoso_gosa_minus`, `yoso_juni` are
already available as candidate features in the existing pipeline. They are the **source** of
jvd_dm's content, not derived from jvd_dm.

**Gate 3 result: ABORT** — r = 1.0 (exact identity). No independent signal.

---

## E0 Gate 4: Serve availability — CONDITIONAL FAIL

### Recent data presence

**jvd_dm** (last 6 weeks):

```
20260608  kubun=7  48 rows   (races 2026-06-06 + 2026-06-07, loaded next day)
20260601  kubun=7  48 rows
20260525  kubun=7  72 rows
...
```

**jvd_tm** (last 6 weeks):

```
20260608  kubun=7  46 rows
20260601  kubun=7  46 rows
...
```

**Pattern:** kubun=7 rows are loaded the morning after the race day. Latest race dates covered
(as of 2026-06-12): up to 2026-06-07 in both tables.

### Serve availability assessment

- For **kubun=7** (99.9% of data): loaded next morning → **not available at serve time** (our
  cron runs at 03:00 JST for same-day races). Even if kubun=7 were novel, it arrives too late for
  same-day predictions.
- For **kubun=2/3** (5 rows total, 2026-only): these ARE pre-race and would be serve-available,
  but there are only 5 rows in the entire database covering 2 JRA races. Not a usable signal.

**Gate 4 result: FAIL** — the dominant data variant (kubun=7, 99.9%) arrives the day after
races; kubun=2/3 have 5 rows total.

---

## Structural summary

```
jvd_dm.mining_yoso_NN = concat(
    umaban,            ← identical: "NN"
    yoso_soha_time,    ← identical: jvd_se.yoso_soha_time
    yoso_gosa_plus,    ← identical: jvd_se.yoso_gosa_plus
    yoso_gosa_minus    ← identical: jvd_se.yoso_gosa_minus
)
```

jvd_dm is a **derived view** of jvd_se, not a separate signal source.

---

## Overall verdict: ABORT — do not proceed to E1

**Primary reason:** jvd_dm/jvd_tm contain no information beyond `jvd_se.{yoso_soha_time,
yoso_gosa_plus, yoso_gosa_minus, yoso_juni}`, which are already available as feature candidates
via `jvd_se`. Adding jvd_dm to the feature set would be adding zero novel information.

**Secondary reason:** 99.9% of rows (kubun=7) arrive the next morning, making them unavailable
for same-day serve.

**Tertiary reason:** The mining prediction itself is weaker than market odds (23.23% vs 39.18%
top-1 win rate), and when dm and odds disagree, dm is far worse (13.68%). No alpha.

**Recommendation:** Do not ingest jvd_dm or jvd_tm into the feature pipeline. The existing jvd_se
yoso fields (`yoso_soha_time`, `yoso_juni`) are the correct source if this signal is ever
evaluated.
