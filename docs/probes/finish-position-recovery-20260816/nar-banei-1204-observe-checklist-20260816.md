# NAR 12:04 / Ban-ei 13:54 — observe only (2026-08-16)

Same three questions as JRA. No new hypothesis. No deploy / PUT /
POST `/run`.

Baselines: `nar-banei-weight-baselines-20260816.md` (09:10).
Sheet: `weight-rescore-missed-post-table-20260816.md`.
JRA close: `weight-rescore-cause-unknown-20260816.md`.

## Windows

| venue            | first post | weight earliest |
| ---------------- | ---------- | --------------- |
| NAR 35 / 44 / 55 | 12:35      | ~12:04          |
| Ban-ei 83        | 14:25      | ~13:54          |

Contrast races if a gen moves: 35/06, 44/06, 55/06, 83/06.

## 1. Did weights land?

Neon `nvd_se.bataiju` for `kaisai_tsukihi='0816'` venues 35/44/55/83.
08:25 baseline was 0/450 real weights. A real value is not three
spaces and not `000`.

## 2. Did the trigger fire?

Remote D1 `sync-realtime-data`:

```sql
SELECT race_key, status, created_at
FROM fetch_logs
WHERE job_type = 'weight-rescore-trigger'
  AND created_at >= '2026-08-16 11:30:00+09:00'
  AND race_key LIKE 'nar:2026:0816:%'
ORDER BY created_at;
```

Ban-ei keys are `nar:2026:0816:83:*`. Pair with `fetch-weights`
status=ok for the same `race_key`.

## 3. Did Neon `prediction_generated_at` move?

Compare to 09:10 baselines (JST): 35/01 **05:31:18**, 44/01
**05:31:24**, 55/01 **05:31:30**, 83/01 **05:50:12**. Coverage must
stay 80/940 unless a new race appears.

If unchanged at/after 12:35, say posted without rescore (same
sentence as JRA). If it moved, record new stamp and delay from
that race’s trigger. Do not infer HIT/MISS from Last-Modified.

## Optional clocks (do not interpret)

- `wrangler queues list` depth for `finish-position-predict-queue`
- GraphQL only if the three questions already have numbers

Stop after writing the numbers. Do not invent a mechanism.
