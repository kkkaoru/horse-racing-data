# JRA weight window observation at 09:03 JST (2026-08-16)

Read-only. Compared to `weight-rescore-observe-plan-20260816.md` (08:02)
and the 09:01 live check.

Clock: **2026-08-16 09:03:15 JST**. First likely publish was ~09:09; this
sample is a few minutes early and already shows the same empty state.

## 1. Trigger

D1 `fetch_logs` since 09:00 JST:

| job_type                 | status                       |     n | latest   |
| ------------------------ | ---------------------------- | ----: | -------- |
| `weight-watchdog`        | ok                           |   272 | 09:02:32 |
| `fetch-weights`          | `skip:weights-empty`         |   110 | 09:01:24 |
| `fetch-weights`          | `queued:weights-empty-retry` |   110 | 09:01:24 |
| `weight-rescore-trigger` | —                            | **0** | —        |
| `fetch-weights` `ok`     | —                            | **0** | —        |

Same as 08:02: watchdog alive, empty polls, **no 0816 write, no trigger**.

## 2. Neon `prediction_generated_at`

0816 still **80 races / 940 rows**. Max stamp **2026-08-15 22:16:43 UTC =
07:16:43 JST**. No JRA row after 09:00 JST.

Controls vs 08:02 baseline: unchanged.

| race    | gen (UTC) | note                |
| ------- | --------- | ------------------- |
| `01/01` | 20:03:55  | 05:04 host          |
| `04/04` | 20:04:03  | 05:04 host          |
| `04/01` | 22:07:10  | already dirty 07:07 |
| `04/02` | 21:33:28  | already dirty 06:33 |
| `04/03` | 21:14:03  | already dirty 06:14 |

Healthy 05:04 host rows were **not** overwritten. Nothing new to degrade.

## 3. Coverage / stage

`jvd_se` JRA 01/04/07: **490 rows, 0 with a real `bataiju`**.

Stopped at **stage 1**. Trigger and Neon cannot move until a weight
write exists. 09:20 remeasure is still scheduled.
