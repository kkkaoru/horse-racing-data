# pipeline-health-monitor

Cloudflare Worker that polls `sync-realtime-data /api/internal/queue-health`
every 15 min during the JST race-day window, tracks consecutive failures in
KV, and on the 3rd consecutive failure produces an alert message to a
Cloudflare Queue. A consumer in the same worker fans the alert out to
Discord, Slack, and/or a custom HTTP webhook.

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

2. Set the `sync-realtime-data` admin token (used by the worker to call the
   `/api/internal/queue-health` endpoint via service binding):

   ```sh
   wrangler secret put REALTIME_ADMIN_TOKEN
   ```

3. Set any subset of webhook URLs. None are required, but at least one must
   be configured for alerts to reach a destination:

   ```sh
   wrangler secret put DISCORD_ALERT_WEBHOOK_URL
   wrangler secret put SLACK_ALERT_WEBHOOK_URL
   wrangler secret put CUSTOM_ALERT_WEBHOOK_URL
   ```

4. Create the queue once (idempotent):

   ```sh
   wrangler queues create pipeline-health-alerts
   ```

5. Deploy:

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
is used purely as a Workers liveness probe.
