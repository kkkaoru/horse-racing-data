# Host NAR 0817: timing writer fails visible, pipeline continues

Started 16:22:30 JST, FORCE 8/4, `SOURCE_DATABASE_URL=r2-catalog://pc-keiba`.
`NEON_DATABASE_URL` ← `NEON_PRIMARY_URL`. Values not logged here.

## What 41676f7c did

Layer-0 and layer-1 both printed:

`debug-timing write failed ... ProgrammingError('missing "=" after
"r2-catalog://pc-keiba" in connection info string')`

The writer used the **catalog** URL as a Postgres DSN. It **did not**
stop the pipeline. That is the intended fail-loud change vs 48h of
silent 0 rows.

**Host one-shot cannot use `_debug_finish_position_layer_timing`** while
`SOURCE_DATABASE_URL` is `r2-catalog://`. Neon writes for scores still
use `NEON_DATABASE_URL`. Timing is a separate connection that picked
the wrong URL in this context.

## Layer clocks so far (dir mtime, not the timing table)

| step                           | clock (JST)         | note                              |
| ------------------------------ | ------------------- | --------------------------------- |
| process start                  | 16:22:30            | PID 20538                         |
| DuckDB base child peak         | ~16:26–16:28        | RSS ~3.1 then ~4.6GB              |
| `feat-nar-base` / layer-0 dirs | 16:29:35 / 16:29:36 | timing fail index 0,1 status=done |
| layer-1 near-miss child        | 16:29:36+           | RSS ~97MB                         |

Base wall ≈ **7 min** (16:22:30 → 16:29:35).

**Aborted 16:32:37.** `add-near-miss-features.py` child RSS **6.83GB**
(agreed stop if >6GB). Parent + child killed. Apple PG left running.
No Neon 0817 flush from this run (still 8 races). Partial dirs:
`feat-nar-base`, `feat-nar-layer-0` only.

Do not treat this as PREWARM or cache HIT. 0816 `feat-nar-*` is in
`/tmp/predict-upcoming-0816-preserve`.
