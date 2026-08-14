# pipeline-health-monitor

Cloudflare Worker that polls realtime queue health and finish-position
prediction readiness during the JST race-day window. The queue-delivery canary
runs every 5 minutes; the existing realtime checks and batched prediction
readiness run every 15 minutes.

Legacy realtime alerts use `pipeline-health-alerts`. Finish-position incidents
write durable KV state/outbox first and notify Discord directly, independently
of the prediction queue that is being monitored.

This replaces a previously rejected Mac launchd health monitor — the worker
is fully Cloudflare-hosted and survives laptop sleep, network drops, and
account-level outages of the sync workers themselves.

## Architecture

```
                +----------------------+
                |   Cron Trigger       |
                | every 15 min, JST    |
                |  09:00 - 22:45       |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | runScheduled         |
                |  - fetch queue-health|
                |  - evaluateChecks    |
                |  - update KV counter |
                +----+------------+----+
                     |            |
       healthy       |            |   3rd failure
       (counter=0)   |            v
                     |  +----------------------+
                     |  | Queue: pipeline-     |
                     |  | health-alerts        |
                     |  +----------+-----------+
                     v             |
              KV reset            v
                          +----------------------+
                          |  runQueue (consumer) |
                          |  - notifyDiscord     |
                          |  - notifySlack       |
                          |  - notifyCustom      |
                          |  - ack / retry       |
                          +----------------------+
```

## Finish-position incident behavior

- End-to-end readiness alerts at T-120 (warning), T-60 (critical), and T-30
  (critical) when eligible entries lack current prediction rows.
- A dedicated message traverses `finish-position-predict-queue` every 5 minutes.
  An enqueue without consumption after 10 minutes is critical.
- Critical incidents carry an incident ID. Operators acknowledge with
  `POST /api/internal/incidents/<incident-id>/ack` and
  `Authorization: Bearer <ALERT_ACK_TOKEN>`.
- Unacknowledged incidents resend after 10 minutes, after 30 minutes, and then
  hourly. Acknowledged but unresolved incidents remind hourly. Recovery closes
  the incident, removes pending outbox state, and sends one recovery message.
- Endpoint failures are incidents; a failed readiness query can never be
  interpreted as zero missing races.

## Checks evaluated each tick

| Check                          | Threshold | Active window (JST) |
| ------------------------------ | --------- | ------------------- |
| fetch-results-staleness        | 30 min    | 13:00 - 21:30       |
| fetch-weights-staleness        | 30 min    | 11:00 - 21:30       |
| races-queued-not-fetched-today | < 10      | always              |
| races-stuck-over-thirty-min    | < 10      | always              |

A check that fails 3 times in a row triggers a `critical` alert. Once
failing, a `still-failing` reminder is re-sent every 4 ticks. When the
check returns to ok, a `recovery` alert is sent and the counter is reset.

## Deploy

1. Create the KV namespace and copy the returned `id`:

   ```sh
   wrangler kv namespace create pipeline-health-monitor-state
   ```

   Replace `REPLACE_WITH_KV_ID` in `wrangler.jsonc` with that id.

2. Apply `apps/finish-position-cron/migrations/0006_create_prediction_monitoring.sql`
   and deploy `finish-position-cron` only in the approved post-meeting window.
   The canary and readiness endpoints must exist before this monitor is enabled.

3. Set service authentication and acknowledgement secrets. The
   `FINISH_POSITION_CRON_TOKEN` value must match `finish-position-cron`'s
   `TRIGGER_TOKEN`.

   ```sh
   wrangler secret put REALTIME_ADMIN_TOKEN
   wrangler secret put FINISH_POSITION_CRON_TOKEN
   wrangler secret put ALERT_ACK_TOKEN
   ```

4. Set webhook URLs. `DISCORD_ALERT_WEBHOOK_URL` is mandatory for the selected
   finish-position incident path; Slack/custom are optional secondary paths:

   ```sh
   wrangler secret put DISCORD_ALERT_WEBHOOK_URL
   wrangler secret put SLACK_ALERT_WEBHOOK_URL
   wrangler secret put CUSTOM_ALERT_WEBHOOK_URL
   ```

5. Create the queue once (idempotent):

   ```sh
   wrangler queues create pipeline-health-alerts
   ```

6. Before deploy, record these values in the private race-operations runbook
   (they cannot be proven from this repository):

   - exact private Discord channel name;
   - primary and backup responder names;
   - critical acknowledgement expectation (10 minutes during operations);
   - witnessed forced-critical, acknowledgement, resend, and recovery test.

7. Deploy:

   ```sh
   bun run --filter pipeline-health-monitor deploy
   ```

## Manual test

```sh
# Trigger one cron tick locally against a real queue-health response:
wrangler dev --test-scheduled

# In another terminal:
curl http://127.0.0.1:8787/cdn-cgi/handler/scheduled
```

The fetch handler also responds to a plain `GET /` with `{"ok": true}` and
is used purely as a Workers liveness probe. Do not deploy the monitoring
changes until the operator completion gate above is satisfied and the current
race meetings have ended.
