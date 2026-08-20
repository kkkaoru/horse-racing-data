# Pipeline monitor queue-pause rehearsal (2026-08-15)

This rehearsal runs the production signal-building and incident-delivery code
locally with an in-memory KV and captured webhook. It does not contact the
production Discord webhook or change any queue.

## Reproduced conditions

- A canary is enqueued at `t=0`.
- The producer reports no error.
- `consumedAt` remains `null` at every poll.
- No prediction rows exist for a race (`predictionCount=0`,
  `missingCount=expectedCount`, `complete=false`).
- The simulated clock is injected as `t=0`, `t=5m`, `t=10m`, then the
  readiness deadline moves from `T-120` to `T-60`.

## Result

At `t=0` and `t=5m`, the canary signal is healthy and emits no alert. At
`t=10m`, the signal becomes critical and exactly one Discord embed is
constructed. The captured run printed:

```text
producer enqueue at 0m: ok=true sentCount=0
5m poll while consumer receives nothing: ok=true sentCount=0
10m SLO deadline: ok=false severity=critical sentCount=1
```

The canary branch that decides this is based on `consumedAt === null` and the
elapsed time since `enqueuedAt`; producer `send()` success is not consulted.
This proves enqueue success cannot make the paused queue healthy.

The readiness rehearsal emitted a warning at `T-120` and a critical alert at
`T-60` for `0/10` coverage. The critical stage transition sends immediately;
it does not wait for three consecutive failures.

## Captured critical canary message

```text
Title: [CRITICAL] finish-position queue delivery canary overdue
Description: Canary rehearsal-canary has not been consumed within 10 minutes.
Runbook: apps/finish-position-cron/docs/prediction-readiness-monitor-design-2026-08-15.md
First detected (JST): 2026-08-15T09:10:00+09:00
Duration: 0m
Stage: 10-minute-delivery-slo
Canary ID: rehearsal-canary
Enqueued: 2026-08-15T00:00:00.000Z
Color: 15158332
```

The captured readiness critical message had:

```text
Title: [CRITICAL] finish-position predictions incomplete jra:01:01
Description: 10 of 10 required predictions are missing.
First detected (JST): 2026-08-15T09:00:00+09:00
Duration: 1h
Stage: T-60
Race: jra:01:01
Post: 2026-08-15T09:40:00+09:00
Coverage: 0/10
Minutes to post: 60
Color: 15158332
```

## Continued-absence resend rehearsal

Using the same unresolved canary and the same real incident engine, the resend
schedule was exercised with an injected clock:

```text
elapsed 0m   ok=true   sent=0
elapsed 5m   ok=true   sent=0
elapsed 10m  ok=false  sent=1  Duration=0m
elapsed 20m  ok=false  sent=2  Duration=10m
elapsed 40m  ok=false  sent=3  Duration=30m
elapsed 70m  ok=false  sent=3  (no new send; only 30m since last send)
elapsed 100m ok=false  sent=4  Duration=1h 30m
```

This is one initial alert at the 10-minute boundary, a 10-minute resend, a
30-minute resend, and then hourly resends measured from the last send. The
incident ID remains stable while `sendCount`, `lastSentAt`, and `Duration`
make the continued absence visible. It neither floods Discord every five
minutes nor stops after one alert.

Critical Action text was made explicit so an operator can act without first
finding a tail log:

```text
Immediately inspect delivery_paused; if true run `bunx wrangler queues resume-delivery finish-position-predict-queue`, then verify canary consumption and prediction rows before acknowledging this incident.
```

## Interpretation

- **A — 10-minute detection:** passed. No alert before 10 minutes; critical at
  the 10-minute SLO boundary.
- **B — enqueue trap:** passed. A successful enqueue with no consumption stayed
  unhealthy and produced a critical alert.
- **C — continuous absence:** passed for both canary consumption and prediction
  coverage. Persistent zero rows reach critical at the deadline instead of
  being treated as healthy.
- **D — actionable notification:** passed after the Action text update; the
  message names `delivery_paused`, the exact resume command, and the required
  post-resume verification.

The rehearsal is local evidence for monitor logic. Production readiness still
requires deploying the finish-position endpoint implementation after the
approved race-day window and then observing a live recovery signal.
