# JRA first-race weight lead (D1, read-only)

Compared 08-15 (only other JRA card in this D1 window) to 08-16.
08-14 / 08-10 / 08-11 / 08-12 / 08-13: **zero** `realtime_race_sources`
rows with `source=jra` in this database. No first-race lead for those
dates here.

## First JRA post vs first `fetch-weights` ok

| day   | first post | race    | first `fetch-weights` ok |       lead |
| ----- | ---------- | ------- | ------------------------ | ---------: |
| 08-15 | 09:40 JST  | `04/01` | **09:03:53**             | **36 min** |
| 08-15 | 09:50      | `07/01` | 09:13:55                 |     36 min |
| 08-15 | 10:00      | `01/01` | 09:23:55                 |     36 min |
| 08-16 | 09:40      | `04/01` | **09:10:29**             | **30 min** |

Same first-race clock on 08-15 was **36 min**, not the 68-race p50 of 40. Today's first ok is **6 min later** than yesterday's first race,
still in the ~30 min band. Not a silent fetch failure: empty polls ran
from 06:40, then `ok` at 09:10.

`realtime_race_sources.last_weight_fetch_at` for 08-15 `04/01` is
09:03:49 (matches the log). For 08-16 `04/01` it is 09:10:24.

## Stage check at 09:11:46 JST (after that first ok)

1. **D1 weight write:** `fetch-weights` ok ×2 on `04/01` (09:10:29,
   09:10:55). Neon `jvd_se.bataiju` still 0/490 — weights land in D1
   first, not in Neon JV.
2. **Trigger:** `weight-rescore-trigger` **ok** `04/01` 09:10:29 and
   `07/01` 09:10:33. Stage 2 fired.
3. **Neon predictions:** still **80/940**, max stamp 07:16:43 JST.
   `01/01` and `04/04` still 05:04 host. Stage 3 **not** moved ~1 min
   after trigger (container / queue). Watch this, not stage 1.

08-14 first-race lead: **no data** in this D1 (no JRA card stored).
