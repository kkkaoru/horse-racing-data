# Mac-less image deploy (2026-08-16 15:55 JST)

User asked directly to deploy so 8/17+ can generate feat without a Mac
and HIT. Not inferred from advisor chat.

## Versions

| role                      | id                                                        |
| ------------------------- | --------------------------------------------------------- |
| **live 100%**             | `cd0c3975-e264-46b7-ad35-d8b35ef7c68e` (upload 06:49:25Z) |
| previous / first rollback | `0c76062e-03c6-4b8b-9a25-a501a4f4c9cc`                    |
| emergency rollback        | `953d086b-4342-42ce-a146-0a5061d51575`                    |

Image tag `finish-position-cron-finishpositionpredictcontainer:cd0c3975`.
`DAY_BASE_SPLIT_ENABLED` **not** flipped (secret name exists; value
untouched). `COORDINATOR_ENABLED` still `0`.

On the image: PREWARM fail-loud (`cd90cb73` `3cd71358`), no PUT of
scoped CacheMiss (`6793ad7f`), DuckDB 1.5.5, writable txn.

## Confirmed today (not broken)

- Health `{"ok":true}`.
- Neon **0816** still **80 races / 951 rows**. 04/01 still 13 runners,
  gen 07:07:10Z (unchanged).
- Advisor 16:01: 0817 still **8/32 / 76 rows** including the 15:48:27
  write. **No data loss.**
- This deploy does **not** fill the other 24 races. Morning host
  one-shot is still required (`morning-host-oneshot-replay-20260817.md`).

## Not confirmed today (do not write “it works”)

| piece                                         | earliest proof                                                 |
| --------------------------------------------- | -------------------------------------------------------------- |
| PREWARM actually PUTs day-base                | next cron **09:30 JST 08-17** HEAD `feat-daybase/.../20260817` |
| CacheMiss fallback does not PUT dead pedigree | next scoped **MISS** (need GraphQL / no new degenerate object) |
| Weight rescore HIT + in-time UPSERT           | NAR window **~11:14 JST 08-17**                                |
| Split reuse of day-base                       | **not enabled**. Do not flip until PREWARM HEAD is HIT         |

## Probe in flight (not a success)

15:56:50 JST POST focused-full `nar 20260817 46/01` → HTTP 200
`accepted`. At 16:00 (+3.5 min) Neon **0 rows**, feat-cache **404**,
day-base **404** (expected: split off, PREWARM not due). 0816 still 80.
No second POST. If empty at accepted+15 min, that is a **queue/stall**
fact, not proof the image “works” or “failed PREWARM”.
