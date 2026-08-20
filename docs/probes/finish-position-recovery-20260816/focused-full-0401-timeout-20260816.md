# jra 04/01 focused-full: accept+20min no write (2026-08-16)

Measurement, not a retry. No second POST. Remaining MISS races not expanded.

## Timeline (facts)

| clock                    | fact                                                                                                                         |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| 20:51:12Z / 05:51:12 JST | `POST /api/admin/run-focused-full-race` jra 04/01 HTTP 200 `accepted`, `racesPredicted: 0`                                   |
| 05:56:19–05:56:46 JST    | Neon writes `55/10`, `55/09` (advisor)                                                                                       |
| 06:00:31–06:00:38 JST    | Neon writes `55/03`, `07/09` (advisor)                                                                                       |
| 06:04:47 JST             | Neon writes `01/01` (advisor) — **after** our 04/01 accept                                                                   |
| 21:11:24Z / 06:11:24 JST | HEAD `feat-cache/catalog-v1/jra/20260816/04/01/features.parquet` **404**; Neon 04/01 still **13 rows**, `last_gen=20:04:02Z` |

Elapsed **1213s**. Memory free 47% at fail check. No extra host `predict_upcoming`.

**Fact:** the container processed at least five other races after our accept; the requested 04/01 did not land in R2 or Neon.

## Queue answers (from code, not dashboard)

### (1) FIFO? Priority?

Cloudflare Queues has **no message priority** in this Worker. All producers call `PREDICT_QUEUE.send(body)` with no priority field (`worker.ts` `sendRescoreRaceMessage`, `race-coordinator.ts` `PREDICT_QUEUE.send`).

Consumer: `max_concurrency: 9` (`wrangler.jsonc`). Messages can run in parallel across shards (`RACE_SHARDED_DO=1`, default 3 shards × category). That alone can **overtake** a race on another shard.

Focused-full `accepted` then `message.retry({ delaySeconds: FOCUSED_FULL_RETRY_DELAY_SECONDS })` — the original message is **not held at the front**; redelivery is delayed. Meanwhile other messages consume slots.

Per-process `_FOCUSED_FULL_IN_FLIGHT` / `_PIPELINE_EXEC_LOCK` is **one pipeline per container process** (category work dirs). A different race holding that shard returns `busy` and requeues with growing delay.

**Not a single FIFO line.** Overtake is expected from sharding + concurrency + delayed retry.

### (2) Weight rescore same queue?

**Yes.** `triggerRescoreAfterWeights` → `/api/internal/rescore-race` → `sendRescoreRaceMessage` → **same** `PREDICT_QUEUE` (`mode=rescore`). Same consumer, same shard hash (`keibajo:raceBango`), same lack of priority.

### (3) In-meeting backlog?

Not measured as a queue depth number tonight (no Queue metrics pull). Evidence of load: after 05:51, at least five Neon writes for other races before 06:11. Coordinator cron is `*/10 1-11 * * *` but `COORDINATOR_ENABLED=0` so that path is a no-op. Coverage self-heal and RS-complete `skipDedup` full messages still share the same queue.

### (4) Near-post processing priority?

**Enqueue** uses `race_start_at_jst` (coordinator window, self-heal `order by race_start_at_jst`). **Consume order does not.** No `delaySeconds` / priority based on time-to-post on `send()`. Shard index hashes `keibajo:raceBango` only — not `runYmd`, not minutes-to-post.

So “T-X enqueue” exists; “process nearer posts first” does **not**.

## Implication for betting / weight trigger

Weight arrives ~31–40 min before post. A `mode=rescore` on this queue can wait behind other shards’ full jobs and behind `accepted` retries. Cache HIT only helps **after** the message is dequeued. Tonight 04/01 waited >20 min without dequeue-complete.

Do not flood remaining MISS races until consume-order is understood or a nearer-post priority exists.
