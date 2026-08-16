# Built, not used (observability / safety that stayed off)

Tonight’s stall was expensive because several instruments exist in git
and do not write in production. First checklist after the next window.

**Same pattern:** code or cron is live enough to _look_ ready; the
sink is empty, gated, or never migrated. Do not flip any of these
tonight.

## Confirmed (7)

| #   | built                                                | why it does not help today                                                                                                         | tonight’s cost                                                               |
| --- | ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| 1   | Day-base PREWARM cron `30 0 * * *`                   | Fires; 08-14/15 objects 404. Success without `parquetKey` was logged success (`cd90cb73` / `3cd71358` git-only)                    | Split cannot HIT a day-base. First race still pays DAY or `--target-race`    |
| 2   | `DAY_BASE_SPLIT_ENABLED`                             | Implemented ~07-12; secret empty; 08-14 restore “keep split off”. Catalog `ensure_day_base` was always-`None` 07-15 (`e6111ca6`)   | JRA per-race pedigree dies; Mac seed was the workaround                      |
| 3   | `_debug_finish_position_layer_timing`                | Table exists; `record_layer_timing_row` no-ops unless `PREDICT_DEBUG_LOGS` (unset). Last write 2026-07-18. `41676f7c` not deployed | 04/01 had no start/end clock. 9.9 min vs queue wait indistinguishable        |
| 4   | `finish_position_delivery_lifecycle`                 | migration **0006**, code writes best-effort                                                                                        | **Not on remote D1.** Consume clock invisible after 09:10 trigger            |
| 5   | `finish_position_delivery_canaries`                  | same 0006; cron still `enqueueDeliveryCanary`                                                                                      | **Not on remote D1.** Cannot prove consumer health vs enqueue                |
| 6   | `PREDICT_DEBUG_LOGS` / child stream                  | Tests require it **off** for silence. HTTP progress exists; Python internals stay in the container                                 | `wrangler tail` is the Worker. focused-full `accepted` then dark             |
| 7   | `PIPELINE_FORCE_MEMORY_GB` / `_THREADS` (`9007a6e6`) | Viewer / host only. Container DO `envVars` does not forward them                                                                   | Host one-shot needs FORCE 8/4; production container cannot use the same knob |

## Related, not the same pattern

| item                                        | why it is not #8                                                                                                                 |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `COORDINATOR_ENABLED=0`                     | **Intentional** shadow no-op (T-25 enqueue). Weight path uses `RESCORE_ENABLED=1` instead. Off by decision, not a forgotten sink |
| `STAGE1_PRESERVED_ODDS_GATE` default-off    | Feature flag, not an instrument. `bfba0c25` still default-off                                                                    |
| Worker-native `scoring/rescore-consumer.ts` | JRA/NAR/Ban-ei all go through the container (`CONTAINER_PER_RACE_CATEGORIES`). Dead path, not a monitor we paid for tonight      |
| feat-cache seed                             | One-shot host PUT, not a built-in that failed to enable                                                                          |

## Enable: what happens / cost (do not do tonight)

**(b) records only** — scoring / enqueue / cache keys stay the same.
**(a) changes production behavior** — race-day window required.

| #   | class   | if enabled                                                                                                                         | cost / risk                                                                                                                                                            |
| --- | ------- | ---------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 3   | **(b)** | Each layer INSERT to Neon `_debug_finish_position_layer_timing` (`41676f7c` already always-on in git)                              | Extra Neon writes (~17/race JRA). Must not share a read-only pooler session with scoring DML. Image deploy, off-hours ok                                               |
| 4   | **(b)** | D1 upserts on consume/complete **if** `deliveryTrackingId` is on the message                                                       | D1 write per message. Best-effort catch already. **Apply 0006 alone** (no other secret). Off-hours ok                                                                  |
| 6   | \*_(b)_ | `PREDICT_DEBUG_LOGS=1` streams child DuckDB/Python on the NDJSON / stderr                                                          | Log volume. Tests lock default-off. Do **not** set on a race day; noise can hide the one line we want. Off-hours image/secret                                          |
| 5   | **(a)** | Cron already `PREDICT_QUEUE.send`s a canary every 5 min (00–13 UTC). 0006 makes the **insert** succeed so the cron no longer fails | **Queue traffic + one consumer slot** per canary. Fix-dev: canary insert is a side effect. Apply 0006 **off-hours**; watch depth                                       |
| 1   | **(a)** | PREWARM PUT of a whole-day parquet (~22 min JRA)                                                                                   | CPU/R2. Fixes `cd90cb73`/`3cd71358` change **logged status** (can flip GraphQL success→failed) and, once PUT works, later split HITs. Image + next 09:30. Not mid-card |
| 2   | **(a)** | First race builds day-base without `--target-race`; later races reuse                                                              | Pedigree quality **and** path change. Needs proven PREWARM HIT first. Team-lead + user. **Never** same deploy as first image                                           |
| 7   | **(a)** | Forward FORCE into `container-class.ts` `envVars`                                                                                  | Container RSS 8GB / 4 threads vs today’s auto cap. Can OOM the instance or change runtime. Image + memory headroom. Not a race-day first                               |

\*6 is records/logs only if we never treat log failure as pipeline failure. Do not pair with a behavior change.

### Tomorrow’s order

1. **(b) 4** — D1 0006 for **lifecycle only** if canary cron can be paused; otherwise treat 0006 as **(a) 5** (same file enables both tables).
2. **(b) 3** — layer-timing image off-hours (`41676f7c`).
3. **(a) 1** — PREWARM image; prove next-morning HEAD HIT.
4. **(a) 2** — split secret, only after (3).
5. **(a) 7** / **(b) 6** — last; 6 only for a short debug window.

**0006 is one migration.** Applying it turns on **4 and 5 together**.
If canary enqueue must not touch `PREDICT_QUEUE` on a race morning,
apply 0006 only when that cron is disabled or after last post.

No deploy, no migration apply, no secret flip from this list.
