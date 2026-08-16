# Weight rescore: cause unknown (2026-08-16)

Today’s honest close for the JRA first three. No new hypothesis.

## What is known

| fact                                                        | clock / source                                           |
| ----------------------------------------------------------- | -------------------------------------------------------- |
| Weights wrote; trigger claimed; `PREDICT_QUEUE.send` ran    | `04/01` 09:10:29, `07/01` 09:10:33, `01/01` 09:20:54     |
| Queue `finish-position-predict-queue` is **ACTIVE**         | advisor `check-queue-delivery.sh` 09:53, not 08-12 pause |
| `finish-position-cron` **was invoked** 09:00–09:55          | GraphQL 74 req / 14 internalError / 322 sub (`c73ee70f`) |
| Neon did **not** rewrite the three R1s before post          | `04/01` 07:07:10, `07/01` 05:04:08, `01/01` 05:03:55     |
| Same at **10:16:00** (after all three posts)                | 80/940, max still 07:16:43                               |
| Queue depth still **2** at 09:51                            | `wrangler queues list`                                   |
| No 0816 rescore row in retry_errors / DLQ at 09:22          | D1                                                       |
| GET does not change R2 Last-Modified                        | 07:49 still means **no PUT**, not unread                 |
| `processContainerPerRaceRescore` waits for NDJSON last line | optimize, code                                           |
| focused-full does not wait                                  | same                                                     |
| Worker-native JRA scorer is unwired                         | `rescore-consumer.ts` STALE                              |

## What is not known

- Whether the two (then three) rescore messages were consumed.
- Whether HIT or MISS ran (no GET log, no cache flag on Neon).
- Which of the 14 `internalError`s belong to those messages.
  Error minutes 09:26–28 and 09:41–42 have **no**
  `finish-position-cron` cron expression (`713dcc38`). Adjacent
  09:25 / 09:30 / 09:40 ticks were success with 0 errors. That
  does not identify the errors.
- Isolate CPU / wall limit for a held `stub.fetch`.
- Eventual late landing time (none by 10:16).

**Cause of “enqueued, Worker ran, Neon unchanged at post” is
unknown.** That is the conclusion. Do not replace it with a
story.

## Next observation only

NAR first post **12:35** (weight earliest ~12:04). Ban-ei **14:25**
(~13:54). Same three questions, no new mechanism hunt:
`nar-banei-1204-observe-checklist-20260816.md`.
Baselines already saved: `nar-banei-weight-baselines-20260816.md`.

No deploy. No PUT. No POST `/run`.
