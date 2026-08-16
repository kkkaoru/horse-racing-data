# Queue consumer: wait vs detach (code, 2026-08-16)

Question: does the consumer wait for the container to finish, so a
9.9 min `LAYER_CHAIN --target-race` fallback could blow a Worker
limit (`internalError`)?

**Answer: it depends on `mode`. Do not collapse the two paths.**

## `mode=rescore` (weight trigger) — **waits**

`POST /api/internal/rescore-race` only `PREDICT_QUEUE.send`s
`mode=rescore` (`worker.ts` `sendRescoreRaceMessage`). HTTP returns 202.

`processContainerPerRaceRescore` (`queue-consumer.ts`):

- `await stub.fetch(/predict?mode=rescore&…)`
- `await parseNdjsonStream` until the **final** result line
- then `ack` or `retryAfterFailure`

`serve.py`: race-scoped `mode=rescore` keeps the **held-response**
keepalive (progress chunks + `renewActivityTimeout`). Focused-full
detach/`accepted` is **not** used.

CacheMiss fallback runs `LAYER_CHAIN` + `--target-race` **inside that
same held request** (`rescore-fallback-to-full`). `6793ad7f` only
stops a later `FEATURES_CACHE.put` of that rebuild; it does not change
wait-vs-detach.

A long fallback **can** occupy one of 9 consumer slots for the whole
rebuild. That is structural. It is **not** proof that today’s 14
GraphQL `internalError`s (09:00–09:55) are a Worker wall-clock timeout:
this file does not state the queue-consumer limit, and a successful
9.9 min wait would still UPSERT Neon.

## `mode=full` skipDedup (focused-full) — **does not wait**

Container claims the slot, starts a detached thread, returns
`accepted`. Consumer `message.retry({ delaySeconds: 150 })` and later
polls Neon (`ackIfFocusedFullAlreadyComplete`). Pipeline can run 15–27
min without holding the Worker request.

## Today’s 09:10 weight messages

Those are **rescore**, so the wait path. Last-Modified staying 07:49
does **not** prove unread (GET does not rewrite the object). Neon still
on morning gens through 10:01 **does** prove no successful UPSERT. HIT
vs MISS is not in Neon/D1/R2 metadata — see the observation table.
`internalError` after consumer start is a failed invocation, not a
finished long wait.

NAR 12:04 / Ban-ei 13:54 use the same rescore wait path.
