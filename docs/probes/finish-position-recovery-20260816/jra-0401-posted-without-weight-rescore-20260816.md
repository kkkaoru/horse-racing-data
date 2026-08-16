# 04/01 posted without the 09:10 weight rescore (2026-08-16)

Read-only. Clock of this Neon read: **2026-08-16 09:40:59 JST**.
Venue 04 race 1 official post: **09:40 JST**.

Facts only.

| event                                                               | clock                                                                                       |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `fetch-weights` / `weight-rescore-trigger` ok `jra:2026:0816:04:01` | **09:10:29 JST** (30 min before post)                                                       |
| Trigger meaning                                                     | cron returned `{claimed:true}` → `PREDICT_QUEUE.send` already ran                           |
| Queue depth at 09:22 (and still 2 at 09:40 list)                    | **2**                                                                                       |
| Neon `04/01`                                                        | 13 rows, **one** `prediction_generated_at` **2026-08-15 22:07:10.675167+00 = 07:07:10 JST** |
| Neon `07/01`                                                        | 10 rows, **05:04:08 JST**                                                                   |
| Neon `01/01`                                                        | 14 rows, **05:03:55 JST**                                                                   |
| Neon 0816 coverage                                                  | **80 / 940**, max gen still **07:16:43 JST**                                                |
| Later triggers (not 04/01)                                          | `01/01` 09:20:54, `04/02` 09:31:37, `07/02` 09:39:30 — all ok                               |

At post time, `04/01` Neon rows were **not** rewritten after 09:10.
The race started on the 07:07 stamp.

No 0816 `mode=rescore` row in `finish_position_predict_retry_errors` or
DLQ at 09:22. `finish_position_delivery_lifecycle` is not on remote D1.
Container stdout is not on `wrangler tail`.

Stopped **after enqueue, before a durable consume result**. Whether the
consumer never started, or started and went silent, is **unproven**.
This is the same observability gap as earlier tonight.

Do not interpret beyond that. No production write.
