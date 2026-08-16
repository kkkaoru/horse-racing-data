# Nine undeployed commits vs today’s stall (2026-08-16)

Today’s known fact: weight trigger fired, queue is ACTIVE, Worker
ran, Neon `prediction_generated_at` did not move before (or 77 min
after) the first three JRA posts. Cause unknown. This page only
asks whether a commit sits on **that** write path.

It does **not** say any commit would have fixed 08-16.

## Might change “trigger ok, Neon unchanged”

Only if consume actually reached the container. Unproven today.

| commit                  | one line                                                                                                                                                                |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `85bfba82` writable txn | Only change on the prediction UPSERT (`predict_upcoming.execute`). If a started rescore died on pooler `25006`, this is the one that could land the row.                |
| `67440b8b` DuckDB 1.5.5 | Irrelevant on a HIT rescore (no Iceberg). On a MISS → `LAYER_CHAIN` fallback, live 1.5.3 dies at IcebergScan, so a fallback could never finish. HIT vs MISS is unknown. |

Do not ship only the other seven and then say “the stall is fixed”.
Those two are the only ones that can make a **started** rescore write
or finish a **MISS** rebuild.

## Does not move today’s Neon stamp

| commit                        | one line                                                                                           |
| ----------------------------- | -------------------------------------------------------------------------------------------------- |
| `2139645b` `PIPELINE_DIR`     | Host override. Production default stays `/app/pipeline`.                                           |
| `cd90cb73` PREWARM error      | `/prewarm-day-base` payload. Not `mode=rescore`.                                                   |
| `3cd71358` parquetKey failed  | Worker PREWARM logger. Not the weight queue.                                                       |
| `41676f7c` layer-timing loud  | Writes `_debug_finish_position_layer_timing`. Never the predictions table. Does not ack the queue. |
| `6793ad7f` no miss PUT        | Stops a polluted feat-cache PUT after scoped MISS. Quality. Does not create a prediction row.      |
| `9007a6e6` `PIPELINE_FORCE_*` | Viewer / host scripts. Container `envVars` does not forward it.                                    |
| `8228ce00` `sync-failed`      | Running-style D1 state. Different Worker, different table.                                         |

## What `41676f7c` would show next time

Today we stopped six times on “cannot see inside the container”.
This commit does **not** put Python stdout on `wrangler tail`.
It removes the `debug_logs_enabled()` gate, forces a writable txn,
and returns False (stderr + no raise) if the timing insert is
skipped.

If the **next** stall is a MISS / full `LAYER_CHAIN`:

- rows appear in `_debug_finish_position_layer_timing` → consume
  started a rebuild, and we get per-layer clocks
- still 0 rows after a known MISS start → pipeline never reached
  a layer, or the timing write itself failed (False, stderr only)

If the next stall is a HIT rescore, the table stays empty on
purpose (no layers). 0 rows then still cannot tell HIT from
“consumer never entered Python”.

So it removes **one** proof-gap (MISS/full started or not). It
does not identify today’s three messages, and it does not fix
them.

## Deploy-window use

To test the stall: `85bfba82` (and `67440b8b` if a MISS is in
play). To see more next time: add `41676f7c`. The rest can ride
along as other fixes; they will not move this clock.

No production change.
