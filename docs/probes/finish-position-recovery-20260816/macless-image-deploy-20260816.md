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

## Probe (not a success, not a PREWARM proof)

|          |                                                                                                                       |
| -------- | --------------------------------------------------------------------------------------------------------------------- |
| POST     | **15:56:50 JST** focused-full `nar 20260817 46/01` HTTP 200 `accepted`                                                |
| 16:10:45 | elapsed **13 min**. Neon **0**. feat-cache **404**. 0816 still 80/951                                                 |
| 16:58    | 46/01 still **0** / cache 404. elapsed **61 min** (midpoint 17:02 not yet)                                            |
| 16:53:09 | **46/02 landed** (POST 16:16:45 → **36 min**). n=10. odds flat 0.5048. model marketfree. cache still **404** at 16:58 |

**17:02 is a midpoint, not a deploy verdict.** 65 min is only today’s
_shortest_ landing (01/02). Today also had 199 and 227. One race past
65 min is normal in that spread — **not** “deploy did not help”. Faster
than 65 min is only “faster than today’s shortest”. Slower than 227 min
(19:44) is only “slower than today’s longest”. Effect needs **several**
races tomorrow. Do not flood POST.

0817 remains **8/32**. This deploy does not generate the other 24.
