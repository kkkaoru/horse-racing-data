# 01/02 late landing vs three silent R1s (2026-08-16 11:12)

Read-only. Advisor clock: only `01/02` moved after 10:00 JST
(`10:54:34`). Confirming the three questions. No new mechanism.

Neon at **11:11:10 JST**:

| race    | rows | gen              | after 09:10 JST?   |
| ------- | ---: | ---------------- | ------------------ |
| `01/02` |    8 | **10:54:34 JST** | yes, only this one |
| `04/01` |   13 | 07:07:10         | no                 |
| `07/01` |   10 | 05:04:08         | no                 |
| `01/01` |   14 | 05:03:55         | no                 |

0816 still **80 / 940**. Max gen is now `01/02`.

## 1. Did `01/02` trigger?

Yes. One `weight-rescore-trigger` **ok** at **09:49:59 JST**.
First `fetch-weights` ok **09:50:10**. Same shape as the R1s
(one claimed trigger each).

| race    | trigger ok   | first weights ok |
| ------- | ------------ | ---------------- |
| `04/01` | 09:10:29     | 09:10:29         |
| `07/01` | 09:10:33     | 09:10:33         |
| `01/01` | 09:20:54     | 09:20:54         |
| `01/02` | **09:49:59** | 09:50:10         |

`01/02` delay trigger → Neon: **64 min 35 s**. Post 10:30, so
**+24 min** after post. Not in time.

## 2. Did R2 Last-Modified move?

No. Signed HEAD, bucket `pc-keiba-features-archive`:

| key                        | bytes | Last-Modified                           |
| -------------------------- | ----: | --------------------------------------- |
| `…/01/02/features.parquet` | 72866 | **2026-08-15 22:49:06Z = 07:49:06 JST** |
| `…/01/01/…`                | 84451 | 07:49:05                                |
| `…/04/01/…`                | 81153 | 07:49:20                                |
| `…/07/01/…`                | 73697 | 07:49:35                                |

Same seed window as 09:31. **No PUT.** A later overwrite would
have moved this. GET would not. HIT is possible and **not
proven**. MISS-then-PUT is ruled out.

## 3. D1 difference vs the three R1s

`finish_position_predict_retry_errors` `run_ymd=20260816`:

| race                    | mode    | recorded_at UTC             | error                              |
| ----------------------- | ------- | --------------------------- | ---------------------------------- |
| `01/02`                 | rescore | 01:19:56 = **10:19:56 JST** | `Network connection lost.`         |
| `01/02`                 | rescore | 01:20:37 = **10:20:37 JST** | Container DO **503** max instances |
| `04/01` `07/01` `01/01` | —       | **no row**                  | —                                  |

DLQ 0816: **empty**.

Visible difference: `01/02` reached `catch` twice, then a Neon
row 34 min later. The three R1s have trigger ok and **no** retry
row. That is not a reason. It is the only D1 contrast.

Why this race and not `01/01` remains unknown.

No production change.
