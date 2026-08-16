# Uncomfortable fact: tonight’s nine commits do not fix this

Do not read `ee01fef0` or `tonight-commit-summary-20260816.md` as
“nine fixes, so tomorrow is fine”.

## The fact

Deploying **all nine** undeployed commits
(`85bfba82` `2139645b` `cd90cb73` `3cd71358` `41676f7c`
`6793ad7f` `67440b8b` `9007a6e6` `8228ce00`)
**does not guarantee** that a weight trigger will update Neon
before post.

Only two even sit on that path, and both are conditional:

- `85bfba82` only if consume already reached the container and the
  UPSERT died on pooler `25006`
- `67440b8b` only if the job was a MISS `LAYER_CHAIN` that died on
  IcebergScan 1.5.3

Neither condition is known for 08-16. The other seven do not write
`race_finish_position_model_predictions` for this stall.

**There is no root-cause fix for today’s stall in tonight’s
commits.** Shipping the pile and watching the next card is not a
test of a fix. A later “still unchanged” would not tell us the
patches were wrong. It would tell us we deployed the wrong object.

## What is actually missing

Not “more GraphQL” and not layer-timing alone.

We can prove enqueue (`weight-rescore-trigger` claimed) and we can
prove the Neon stamp did not move. Everything between those two
clocks is unlabeled. Empty retry_errors / empty DLQ only say the
handler did not `catch` and the message did not exhaust 16 retries.
They do not say consume started, hung on `stub.fetch`, or never
entered Python.

What is required is a **durable per-message outcome** written by
the queue consumer: consume started, then ack / retry / died
before Python. That row has to exist even when the held container
fetch never returns. `finish_position_delivery_lifecycle`
(migration 0006) is that table and is **not on remote D1**.
`41676f7c` does not substitute: it is empty on a HIT and empty
when Python never starts.

Until that outcome exists, we cannot choose a code change. Cause
remains unknown.

No production change.
