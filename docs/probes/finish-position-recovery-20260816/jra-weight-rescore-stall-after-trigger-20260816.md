# Weight rescore stall after 09:10 trigger (2026-08-16 09:22 JST)

Read-only. No deploy. First post 09:40.

`weight-rescore-trigger` ok means the cron Worker returned
`{claimed:true}` and **`PREDICT_QUEUE.send` already ran**
(`handleInternalRescoreRace`). That is enqueue, not Neon write.

## Where it stopped

| step                     | evidence                                                                                                                         | status at 09:20–09:22              |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------- |
| 1. Weights exist         | D1 `fetch-weights` ok `04/01` 09:10:29, `07/01` 09:10:33 (also 09:15)                                                            | yes (D1, not `jvd_se`)             |
| 2. Trigger               | `weight-rescore-trigger` ok those two keys                                                                                       | **enqueued**                       |
| 3a. Consumer / container | `finish_position_predict_retry_errors` for `20260816` rescore **empty**. Latest 0816 rows are yesterday 18:05 **mode=full** 503s | **no consume error logged**        |
| 3b. DLQ                  | no `run_ymd=20260816` rescore. Latest DLQ is `20260815` `07/02` rescore **Network connection lost** at 00:11:30 UTC              | not dead-lettered yet              |
| 3c. Neon                 | 80/940, max gen still **07:16:43 JST**. `04/01` still 07:07, `07/01` still 05:04                                                 | **no write ~12 min after enqueue** |
| Lifecycle table          | `finish_position_delivery_lifecycle` **does not exist** on remote D1 (migration 0006 not applied)                                | cannot see consume clock           |

`wrangler queues list`: `finish-position-predict-queue` consumers=1
(`finish-position-cron`), **messages=2**. That is a backlog of two,
consistent with the two claimed rescored races sitting unconsumed or
in-flight without a result log.

`finish_position_delivery_canaries` also missing on remote (same
undeployed 0006). No canary proof of consumer health this morning.

## What this is not

- Not "weights never landed". Stage 1 via `jvd_se` was the wrong
  series (advisor correction). D1 wrote.
- Not `RESCORE_ENABLED=0` — that would log `skip:rescore-disabled`.
- Not `skip:not-claimed` — claimed=true.
- Not a finished container HIT (that would move Neon in seconds) and
  not a recorded 503 (those show up in retry_errors / DLQ).

## Allowed conclusion

Stopped **after enqueue, before a durable consume result**. Either the
two queued messages are waiting behind an in-flight job, or the
consumer started and went silent (same family as last night's
`Network connection lost` on NDJSON). Cannot tell which without
container stdout or the missing lifecycle table.

Do not POST `/run`. Do not deploy mid-card.
