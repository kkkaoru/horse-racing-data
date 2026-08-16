# Queue settings vs 65 min / 227 min dwell (2026-08-16)

Read-only. No deploy. New landings at **13:11**:

| race    | trigger  | Neon         | dwell       |
| ------- | -------- | ------------ | ----------- |
| `01/02` | 09:49:59 | 10:54:34     | **65 min**  |
| `07/01` | 09:10:33 | **12:57:05** | **227 min** |

`07/01` retry row at 12:55:24: Container **503** max instances.
Then a write ~2 min later. Same shape as `01/02` (10:19 lost /
10:20 503 → 10:54). Queue depth still **2**.

Not dead. Dwell is real. Settings **cannot** pick tomorrow’s
priority over 0006.

## What the numbers allow

| knob                  | live    | what it does                                                        |
| --------------------- | ------- | ------------------------------------------------------------------- |
| `max_batch_size`      | 1       | one message per invoke                                              |
| `max_batch_timeout`   | 5s      | batch wait, **not** handler wall                                    |
| `max_concurrency`     | 9       | up to 9 consumer isolates                                           |
| `max_retries`         | 16      | then DLQ. 16 × focused-full 150s = 40 min **if** that delay is used |
| `max_instances`       | 10      | running containers. sleepAfter **45m still counts**                 |
| `RACE_SHARDED_DO`     | 1       | 3 shards × 3 cats = 9 names. Matches LIVE 9                         |
| `_PIPELINE_EXEC_LOCK` | process | **one** predict/rescore at a time **per container process**         |

A HIT rescore should be seconds. A MISS `LAYER_CHAIN` is ~10 min
on one shard. Nine shards in parallel is the design ceiling.

## What they do **not** explain

**227 min is not 16 × anything on the rescore path.**
`processContainerPerRaceRescore` holds `stub.fetch` until NDJSON
ends. A throw goes to `retryAfterFailure` (platform retry, not
the 150s focused-full delay). 16 platform retries do not add to
3.8 h unless each attempt sat ~14 min — possible for MISS, not
proven. Empty 0816 DLQ: messages did not exhaust 16.

**`max_concurrency` 9 is not the observed limit.** LIVE 9 means
the **instance cap / shard names** are full, not that 9
rescored. One process lock per live container: 9 in-process
slots if all 9 are actually executing. We cannot see that.

Raising concurrency or retries **without** a consume row would
not tell us whether the three silent R1s are queued, hung on
held-fetch, or never entered Python. Same as this morning.

`sleepAfter` 45m explains **why slots stay occupied after a
job**, not a 227 min wait by itself.

## Priority

0006 + a **weight `deliveryTrackingId`** still come first.
Tuning 9 / 16 / 10 is a **later** lever, and only after we see
per-message start/ack. Shipping a bigger `max_instances` tonight
would change the card we are still watching.

No production change.
