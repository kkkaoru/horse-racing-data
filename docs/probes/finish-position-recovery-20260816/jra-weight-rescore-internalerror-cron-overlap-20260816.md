# internalError minutes vs finish-position-cron crons (2026-08-16)

Read-only. Compared advisor 10:15 list against committed
`apps/finish-position-cron/wrangler.jsonc` `triggers.crons` and the
GraphQL minute buckets already recorded.

## Correction: Last-Modified

GET does not change R2 Last-Modified. “Still 07:49” means **no PUT**,
not unread. This file never used Last-Modified as unread. The notes
this agent committed (`jra-0401-posted-without-weight-rescore`,
`jra-first-three-posted-without-weight-rescore`,
`jra-weight-rescore-worker-invocations`,
`jra-weight-rescore-timeout-hypothesis`) also do not.

## Error minutes

GraphQL `datetimeMinute` (UTC) with `internalError`:

- **00:26–00:28** = 09:26–09:28 JST (8 errors)
- **00:41–00:42** = 09:41–09:42 JST (6 errors)

## Crons that fire _inside_ those minutes

None on **this** Worker.

| expression                        | JST                                      | inside 09:26–28 / 09:41–42? |
| --------------------------------- | ---------------------------------------- | --------------------------- |
| `*/5 0-13 * * *` canary           | :00 :05 … **09:25, 09:30, 09:40**, 09:45 | **no** — adjacent           |
| `25 0 * * *` Neon pre-wake        | **09:25**                                | **no**                      |
| `30 0 * * *` day-base PREWARM     | **09:30**                                | **no**                      |
| `*/30 1-11 * * *` keep-warm       | first **10:00**                          | no (`hour` is 1–11 UTC)     |
| `*/10 1-11 * * *` coordinator     | first **10:00**                          | no                          |
| `7,22,37,52 1-11 * * *` self-heal | first **10:07**                          | no                          |
| `15 0 * * *` corner morning       | 09:15                                    | no                          |
| RS kicks                          | 00:00–08:00 and 22:00/23:00              | no                          |

`*/10 0-14 * * *` is **sync-realtime-data**
`RUNNING_STYLE_INFERENCE_CRON`, not this Worker. It does not appear in
this `wrangler.jsonc`. HTTP from that Worker _into_
`finish-position-cron` would still show here, but that cron is not a
scheduled tick **of** this script.

## Adjacent GraphQL minutes (success, not the error cluster)

| UTC   | JST   | GraphQL                         | scheduled here              |
| ----- | ----- | ------------------------------- | --------------------------- |
| 00:25 | 09:25 | success 3 / sub 9, **0 errors** | canary + JRA pre-wake       |
| 00:30 | 09:30 | success 3 / sub 3, **0 errors** | PREWARM (trivial signature) |
| 00:40 | 09:40 | success 1 / sub 0, **0 errors** | canary                      |

Those ticks ran and did **not** carry today’s `internalError`s.
08-14 PREWARM `internalError` was in the **00:30** bucket. Today 00:30
is clean.

## Disagreement with “both windows have overlapping crons”

Adjacent is not the same minute. A <1s pre-wake / canary at 09:25 or
09:40 does not explain:

- 00:26 **success 22 / sub 162 + internalError 4**
- 00:41 **internalError 5** with no cron in that minute

22 requests in one minute is not a cron signature (those are 1–3
requests). That burst is **more like HTTP + queue + retries**, still
unlabeled.

Cannot prove the 14 errors are rescore. Cannot drop them as “cron”
either: **no finish-position-cron expression fires in the error
minutes**. Correlation with 09:20 `01/01` trigger (+6 min) and 09:40
post remains only a clock alignment.

Neon at **10:16:00 JST** still unchanged: `04/01` 07:07, `07/01` 05:04,
`01/01` 05:03 (posted 10:00). 80/940 max 07:16. Consume result still
unproven. No production change.
