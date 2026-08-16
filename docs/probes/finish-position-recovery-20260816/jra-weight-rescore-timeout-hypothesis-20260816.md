# Timeout hypothesis vs settings (2026-08-16)

Advisor hypothesis: consume → wait on container → Worker/DO wall
limit → `internalError` → retry to tail → never finishes. Grounded in
per-race p50 ~9.9 min vs a much shorter Worker limit.

**Do not treat that as proven.** Parts match. Parts do not.

## What GraphQL can and cannot say

`workersInvocationsAdaptive` has **no queue-vs-cron field**. Status is
`success` / `internalError` only.

Time clustering (not identity):

| JST         |                                   requests | note                                                 |
| ----------- | -----------------------------------------: | ---------------------------------------------------- |
| 09:10       |                          success 2 / sub 2 | same minute as trigger enqueue (HTTP to this Worker) |
| 09:26–09:28 | success 33 / sub 249 + **internalError 8** | first large burst after enqueue                      |
| 09:30       |                          success 3 / sub 3 | matches PREWARM cron `30 0 * * *` trivial signature  |
| 09:41–09:42 |   success 2 / sub 16 + **internalError 6** | 04/01 post                                           |

Cannot prove the 14 `internalError`s are the two rescore messages.
Cannot prove they are not.

## Queue consumer settings (committed `wrangler.jsonc`)

Primary `finish-position-predict-queue`:

- `max_batch_size`: **1**
- `max_batch_timeout`: **5** (seconds to wait for a batch, **not**
  handler wall time)
- `max_concurrency`: **9**
- `max_retries`: **16**
- DLQ: `finish-position-predict-dlq`

A 9.9 min handler is **allowed by these numbers**. They do not cap
consume at 30s. Retry-to-tail after one `internalError` is **not**
what 16 retries describe; the message stays in-flight / delayed, then
DLQ. At 09:51 there was **no 0816 rescore DLQ/retry row**.

## How long rescore waits on the container

JRA per-race rescore is **not** the stale Worker-native scorer
(`rescore-consumer.ts` is explicitly unwired). It is
`processContainerPerRaceRescore`: `stub.fetch` held `/predict`
`mode=rescore`, then `parseNdjsonStream` until the last line.

There is **no application-level abort** on that fetch in this file.
`sleepAfter` on the Container class is **45m** (idle after last
request activity, not a 9.9 min kill). `renewActivityTimeout` runs
while NDJSON chunks arrive.

**Not in repo, not measured tonight:** Cloudflare Queue **consumer
isolate CPU / wall** limit, or DO `stub.fetch` platform timeout. If
those are ~30s–few minutes, a **MISS** full rebuild (~10 min) sitting
on an open held fetch would match the hypothesis. A **HIT** rescore
that only refreshes five late-binding columns should finish in
seconds and would **not** need that long wait — unless HIT never
happened or the stream never started.

08-14 PREWARM `internalError` is the same GraphQL status. Same
**class of platform error**, not proof of the same code path
(PREWARM is cron `30 0`, not this queue consumer).

## Allowed conclusion

Hypothesis is **plausible for a MISS / hung held-fetch**, **not
established**. Counter-evidence: `max_batch_timeout` is not a
handler deadline; 16 retries + empty 0816 DLQ argue against
“one error then forever last”. Missing: isolate limit number, and
whether 09:26/09:41 errors are these two messages.

No production change.
