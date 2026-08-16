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

## First inspection next window (do not do tonight)

1. Apply D1 migration **0006** or stop calling the writers.
2. Confirm PREWARM PUT + `parquetKey` on one morning **before** any
   split flip.
3. Deploy always-on layer-timing (`41676f7c`) **or** accept we will
   stay blind on the next +20 min wait.
4. If FORCE is required in the container, add the two names to
   `container-class.ts` `envVars` — do not assume viewer env leaks in.

No deploy, no migration apply, no secret flip from this list.
