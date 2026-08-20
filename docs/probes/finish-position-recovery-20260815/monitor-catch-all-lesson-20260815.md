# Monitor catch-all route lesson (2026-08-15)

## Observed production state

The `pipeline-health-monitor` was deployed before the readiness/canary routes
were deployed to `finish-position-cron`. Requests made with the correct bearer
token returned the Worker health fallback instead of the requested payload:

```json
{ "cron": "0 18 * * *", "name": "finish-position-cron", "ok": true }
```

The same body and HTTP 200 status were returned for both:

- `/api/internal/prediction-readiness`
- `/api/internal/delivery-canaries`
- a deliberately nonexistent route
- requests without authorization and with an invalid token

Therefore HTTP 200 did not prove route existence or authentication. The
readiness/canary implementation exists in source, but was not live until the
finish-position Worker is deployed after the race-day change window.

## Required monitor behavior

A health fallback must never be interpreted as healthy readiness or canary
state. The monitor client now validates the response shape before returning it:

- readiness requires string `checkedAt`, string `runYmd`, and array `races`;
- delivery canaries require string `checkedAt` and array `canaries`.

An unexpected shape raises an endpoint incident with an explicit message that
the corresponding finish-position endpoint may not be deployed. This fails
closed and reaches the monitor's direct Discord notifier rather than silently
reporting `ok`.

## Operational lessons

1. HTTP 200 is not evidence that an endpoint exists when a catch-all returns
   200 for unknown routes.
2. Endpoint probes need a negative-route control experiment and an expected
   payload-shape assertion.
3. A monitor must treat "cannot evaluate" as an incident, never as healthy.
4. Committing monitoring code does not activate it; the target Worker must be
   deployed before the monitor can call the new route.
5. A producer enqueue success is not delivery success. Queue health requires an
   independent consumed canary and prediction-readiness output.

## Live monitor follow-up (2026-08-15)

A live tail of the first quarter-hour tick initially found that
`/api/internal/queue-health` returned HTTP 403. The request is sent to
`https://sync-realtime-data.kkk4oru.com/api/internal/queue-health` with the
`REALTIME_ADMIN_TOKEN` secret. The sync Worker compares that bearer value to
its own `env.REALTIME_ADMIN_TOKEN`; this is an application secret mismatch (or
missing value), not proof of a Cloudflare API token's Queue permission.

The scheduled handler was changed so each inspection is isolated. Queue-health,
canary, and readiness failures each become critical incident signals; one
rejection no longer cancels the other inspections or escapes the scheduled
handler. Incident-delivery failures are contained as well, while durable
outbox state remains the retry record.

After deployment, a live `wrangler tail` tick completed with `outcome: ok`, no
exceptions, and signals for all three unavailable checks:

```text
finish-position-monitor-endpoint:delivery-canaries ok=false severity=critical
finish-position-monitor-endpoint:prediction-readiness ok=false severity=critical
finish-position-monitor-endpoint:queue-health ok=false severity=critical
```

The monitor KV recorded critical sends for queue-health and readiness at the
same scheduled tick. The queue-health credential was then reconciled safely
from the local `sync-realtime-data/.dev.vars` source into the monitor's
`REALTIME_ADMIN_TOKEN` secret. No token value is recorded here. A direct
production probe returned:

```json
{
  "lastSuccessfulFetchResultsAt": "2026-08-15T09:51:06+09:00",
  "lastSuccessfulFetchWeightsAt": "2026-08-15T09:50:59+09:00",
  "racesQueuedNotFetchedToday": 0,
  "racesStuckOverThirtyMin": 0
}
```

The next quarter-hour live tick completed with `outcome: ok`, logged
`queue-health {"ok":true}`, emitted a queue-health recovery signal, and logged
all four checks. Finish-position readiness/canary routes remained unavailable
until their separately approved post-race Worker deployment.

Discord channel visual confirmation was not available in this session. The
forced messages and live incident delivery were confirmed through Discord's
successful webhook responses and the incident state update that occurs only
after direct delivery completes; this is indirect evidence, not a human visual
check.
