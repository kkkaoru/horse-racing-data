# JRA weight window at 09:20 JST (2026-08-16)

Read-only. Clock **2026-08-16 09:20:14 JST**. First post 09:40.

## Three stages

### 1. Neon `jvd_se.bataiju`

JRA 01/04/07: **490 rows, 0 with a real weight**. Same as 09:01.
Weights that arrived are in **D1** (`fetch-weights` ok), not mirrored
into Neon JV yet. Do not read stage 1 as "no weights exist".

### 2. D1 trigger (since 09:00 JST)

| job                      | status              |     n | latest   |
| ------------------------ | ------------------- | ----: | -------- |
| `fetch-weights`          | ok                  | **6** | 09:15:06 |
| `fetch-weights`          | skip:already-stored |     4 | 09:15:06 |
| `fetch-weights`          | skip:weights-empty  |   148 | 09:15:31 |
| `weight-rescore-trigger` | **ok**              | **2** | 09:10:33 |

Trigger rows:

- `jra:2026:0816:04:01` ok **09:10:29**
- `jra:2026:0816:07:01` ok **09:10:33**

No further trigger after 09:10:33. `01/01` has no ok write yet.

### 3. Neon predictions

Still **80 / 940**. Max `prediction_generated_at` **07:16:43 JST**.
No JRA row after 09:09. Controls unchanged: `01/01` and `04/04` still
05:04 host; `04/01` still 07:07.

**~10 minutes after trigger, Neon has not moved.** Stage 2 fired;
stage 3 did not. That is container / queue, not "weights never landed".

## vs 09:01 plan

The 09:01 instruction said "if nothing moved, weights never landed".
That is **false at 09:20**. Weights wrote to D1 and the trigger
returned ok. The stall is **after** enqueue.
