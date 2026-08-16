# finish-position-cron invocations 09:00–09:55 JST (2026-08-16)

Read-only GraphQL `workersInvocationsAdaptive`,
`scriptName=finish-position-cron`, window **2026-08-16T00:00:00Z–
00:55:00Z**. Token not printed.

Queue is **ACTIVE** (advisor `check-queue-delivery.sh`, exit 0).
This is not the 08-12 paused-delivery outage.

## Totals

| status        | requests | errors | subrequests | minute-buckets |
| ------------- | -------: | -----: | ----------: | -------------: |
| success       |       60 |      0 |         308 |             20 |
| internalError |       14 |     14 |          14 |              5 |
| **sum**       |   **74** | **14** |     **322** |             25 |

The Worker **was invoked**. Not “consumer registered but never called”.

## Minutes that matter

| UTC       | JST                     | success req/sub | internalError |
| --------- | ----------------------- | --------------- | ------------- |
| 00:10     | **09:10** (trigger)     | 2 / 2           | 0             |
| 00:11     | 09:11                   | 1 / 0           | 0             |
| 00:15     | 09:15                   | 3 / 25          | 0             |
| 00:20     | 09:20 (`01/01` trigger) | 2 / 1           | 0             |
| 00:25     | 09:25                   | 3 / 9           | 0             |
| **00:26** | **09:26**               | **22 / 162**    | **4 / 4**     |
| **00:27** | **09:27**               | **9 / 71**      | **3 / 3**     |
| 00:28     | 09:28                   | 2 / 16          | 1 / 1         |
| 00:30     | 09:30 PREWARM cron      | 3 / 3           | 0             |
| **00:41** | **09:41** (04/01 post)  | 1 / 8           | **5 / 5**     |
| 00:42     | 09:42                   | 1 / 8           | 1 / 1         |

GraphQL does not label queue vs cron vs HTTP. Trivial crons in this
repo have historically been `requests=1, subrequests=1`. The 09:26
burst (`sub=162`) is not that signature.

## What this can and cannot say

- Consumer / Worker isolate **did run** after the 09:10 enqueue.
- **14 internalErrors** in the hour, clustered 09:26–09:28 and 09:41.
- Neon still had no new 04/01 / 07/01 / 01/01 stamp at 09:51. Invoke
  ≠ successful rescore write.
- Cannot see whether those errors are the two rescore messages, a
  container 503, or NDJSON `Network connection lost` (yesterday’s DLQ
  family). No 0816 rescore row in retry_errors / DLQ at 09:22.

Stopped after enqueue with Worker activity and platform errors, not
with a silent unused consumer. Still no Neon write. No production
change.
