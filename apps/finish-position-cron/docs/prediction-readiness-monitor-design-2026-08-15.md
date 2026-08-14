# Finish-position prediction readiness monitoring design

Date: 2026-08-15 JST

Status: A/B/D implementation committed for validation; migration, secrets, deploy, and forced human-notification test remain deferred until the 2026-08-15 meetings finish

## Incident lesson

A three-day pause of `finish-position-predict-queue` delivery was not detected. Producers could still enqueue, so enqueue success looked like recovery while no consumer received the work. The useful health signal is therefore not "could enqueue" but "are the predictions that users need present by their deadline?"

## Current controls and the structural gap

### Coverage self-heal

`src/coverage-self-heal.ts` runs every 15 minutes from 10:00 through 20:59 JST. It has two paths:

- pre-race readiness scans incomplete races up to 180 minutes before post;
- post-race self-heal scans from post time +15 minutes.

The pre-race path initially enqueues at most twice per race, escalates, and then retries at 30-minute intervals. It caps pre-race work at 16 races per tick. Both paths enqueue onto the same `PREDICT_QUEUE` whose delivery had been paused.

Consequently, all of the following can be true together:

1. the prediction rows are absent;
2. self-heal successfully calls `send()` and records an enqueue event;
3. the primary consumer processes zero messages;
4. subsequent self-heal attempts only add messages to the stopped path.

`PRE_RACE_READY_ESCALATE` is a `console.error`; it has no notifier. Observability uses `head_sampling_rate: 0.1`, so logs are neither a reliable state store nor a human notification. The D1 coverage-gap events record enqueue attempts, not consumption or completed predictions. The DLQ consumer also cannot help when primary delivery is paused: no primary delivery means no retries and no DLQ landing.

### Existing health monitor

`apps/pipeline-health-monitor` already provides a separate scheduled Worker, consecutive-failure/recovery state, and Discord, Slack, and custom webhook notifiers. It currently checks only `sync-realtime-data /api/internal/queue-health`; it does not inspect finish-position predictions or the predict queue. Its alert fan-out uses a separate queue, `pipeline-health-alerts`.

This is the preferred host for detection because it is independent of the stopped primary queue. Existing repository configuration proves notifier support, but does **not** prove that a production webhook secret is configured or that a person is watching its destination.

## Signals to implement

### A. End-to-end prediction readiness (primary signal)

Every 15 minutes during the race-day window, the health monitor asks an authenticated internal endpoint on `finish-position-cron` for race-level readiness. The endpoint compares upcoming entries from `REALTIME_DB` with usable rows in `race_finish_position_model_predictions` from Neon. It returns aggregate/race-key status only, never connection details.

A race is complete when every eligible current entry has the required current prediction row. Report at least:

- race key and post time;
- eligible entry count and usable prediction count;
- missing count;
- oldest/newest prediction timestamp where useful;
- readiness deadline crossed (`T-120`, `T-60`, or `T-30`).

Alert rules:

| Deadline | Action                                               |
| -------- | ---------------------------------------------------- |
| T-120    | warning on incomplete coverage; durable state starts |
| T-60     | critical immediately if any required row is missing  |
| T-30     | critical reminder if still incomplete                |
| recovery | notify once when all required rows appear            |

The T-60 check does not wait for three consecutive failures. Deadline crossings are discrete SLO failures, and waiting 30-45 minutes would defeat the deadline. Deduplicate by race key + deadline in durable state. A changed missing count updates state but does not notify repeatedly except at the next deadline. Query in batches for all races in the lead window; never issue one Neon query per race.

This signal catches queue, consumer, container, Neon, model, and coordinator failures without diagnosing their cause first.

### B. Queue delivery canary (early cause signal)

Every five minutes, `finish-position-cron` scheduled code sends a small message with a dedicated discriminator such as:

```json
{ "type": "delivery-canary", "id": "<uuid>", "enqueuedAt": "<iso>" }
```

The primary queue consumer handles it before prediction dispatch, writes `consumedAt`, message ID, and delivery lag to D1, and immediately acknowledges it. It must not claim a race, contact Neon, or start a container.

A separate scheduled check reads canary state through an authenticated internal endpoint. Alert when a successfully enqueued canary has not been consumed within 10 minutes, and send one recovery notice after delivery resumes. Persist `enqueued_at` and `consumed_at` separately. Never interpret enqueue success as consumer health.

Five-minute canaries are intentional: a check only a few times per day cannot meet the approved pause-to-alert SLO of 10 minutes or detect an overnight pause before race readiness becomes urgent.

### C. Control-plane status (lower-priority supplement)

An external scheduled job may query the Cloudflare Queues control-plane API for `delivery_paused`. This can provide a more direct diagnosis, but it is not the source of truth: API/token failures and undocumented state changes must not suppress A or B.

### D. Self-heal accounting

Extend coverage-gap state to distinguish:

- detected incomplete;
- enqueued;
- consumed;
- prediction complete;
- notified/escalated.

Escalation must create a durable alert/outbox record, not only a sampled console message. Self-heal may remain a recovery mechanism, but monitoring evaluates the resulting prediction rows independently.

## Notification delivery and ownership

### Selected path

Use a **private Discord race-operations alert channel** as the primary human destination, reusing the tested Discord notifier in `apps/pipeline-health-monitor`. The recipients are the repository owner/race-day operator and the designated backup operator. The critical message must identify the race/deadline, missing/expected counts, first failure time, and a runbook link. Recovery goes to the same channel.

Do not send readiness alerts through `finish-position-predict-queue`. Prefer direct HTTPS delivery from `pipeline-health-monitor` with a short timeout, while first writing alert state/outbox durably. Retry notifier failures from the durable outbox. The existing separate `pipeline-health-alerts` queue can remain a secondary fan-out path, but must not be the only path because queue delivery itself is the incident class being monitored.

### Acknowledgement and resend escalation

Every critical alert has an incident ID and remains unacknowledged until the primary or backup operator uses an authenticated acknowledgement action recorded in durable state. Discord delivery success is not acknowledgement.

While the failure persists and no human has acknowledged it, resend the critical alert after 10 minutes, again 30 minutes after the initial alert, and then hourly. Send it through direct Discord and the configured secondary destination; notifier failure leaves the outbox pending for bounded delivery retries. Once acknowledged, stop acknowledgement-driven resends but send an hourly still-failing reminder while the SLO remains violated. This prevents a silent multi-day incident without producing a message every monitor tick.

Recovery closes the incident, cancels pending resends, and sends one direct recovery notification to the same destinations. If recovery delivery fails, keep retrying it from the outbox until delivered. A later recurrence creates a new incident ID rather than reopening an acknowledged/closed incident.

### Completion gate

The repository cannot reveal secret values or prove readership. Before enabling alerts, an operator must record in the runbook:

1. the exact Discord channel name;
2. the primary and backup human responders;
3. their acknowledgement expectation (critical within 10 minutes during race operations);
4. a successful forced critical and recovery test witnessed by both responders.

If no suitable operations channel currently exists, create a private Discord channel and webhook. Slack/custom webhook remain optional secondary destinations. Email is not the initial choice because this repository has no generic operational email sender, while Discord webhook support and tests already exist.

## Cost controls

- End-to-end readiness: reuse the existing 15-minute monitor window, about 56 ticks/day, with one batched endpoint request and batched SQL per tick.
- Canary: 288 tiny messages/day plus D1 writes; it never starts a container or queries Neon.
- Log only state transitions and errors. Keep 0.1 head sampling; do not increase observability sampling for this feature.
- Durable deduplication prevents per-race/per-tick alert storms.
- Return aggregates from internal endpoints and cap diagnostic race details.

Compared with container starts and Neon prediction work, canary cost is negligible. Exact Cloudflare request/queue/D1 deltas should be checked from the first seven days before changing frequency.

## Failure modes

- If the prediction queue is paused, B alerts and A eventually alerts; Discord delivery bypasses that queue.
- If the container hangs, A alerts; optional container-duration telemetry aids diagnosis but is not required for detection.
- If Neon is unavailable, the readiness endpoint fails and the monitor treats endpoint failure as critical rather than healthy/zero.
- If the monitor cannot reach `finish-position-cron`, that is a monitor failure and alerts after a short bounded retry.
- If Discord delivery fails, the durable outbox remains pending and the secondary notifier path is attempted.
- If no races exist, A is healthy while B still tests delivery.

## Acceptance criteria

1. Pausing primary queue delivery produces a human-visible canary alert within 10 minutes.
2. Missing predictions produce a human-visible critical alert no later than T-60.
3. Alerts arrive while the primary prediction queue remains stopped.
4. Resuming delivery/completing predictions produces one recovery notification.
5. Enqueue-only events cannot make either check healthy.
6. Canary handling never starts a container or contacts Neon.
7. Forced critical and recovery tests are acknowledged by the named primary and backup recipients.
8. No DSN, hostname, credential, or webhook URL appears in responses or logs.

## Implemented components (not deployed)

- `finish-position-cron` exposes authenticated batched prediction-readiness and
  delivery-canary endpoints.
- A dedicated five-minute canary traverses the primary prediction queue, records
  enqueue and consume timestamps separately in D1, and never enters container or
  Neon prediction dispatch.
- Self-heal messages carry a durable tracking ID with detected, enqueued,
  consumed, and prediction-completed timestamps.
- `pipeline-health-monitor` polls canary state every five minutes and readiness
  every fifteen minutes through a service binding.
- Critical incidents use a direct notifier with KV incident/outbox state,
  acknowledgement API, 10/30-minute then hourly unacknowledged resends, hourly
  acknowledged reminders, and one recovery notification.
- Endpoint failures fail closed as incidents.

Migration `0006_create_prediction_monitoring.sql`, secrets, responder names,
Discord channel ownership, witnessed forced tests, and both Worker deploys are
still blocked by the completion gate and post-meeting deployment decision.

## Implementation order after meetings

1. Confirm Discord channel and two responders; execute a notifier smoke test.
2. Add direct durable notification/outbox support to `pipeline-health-monitor`.
3. Add end-to-end readiness endpoint and deadline check.
4. Add canary discriminator, D1 state, producer, consumer fast path, and monitor check.
5. Split self-heal enqueue/consume/complete accounting.
6. Add optional control-plane polling last.

No implementation or production configuration change is part of this design commit.
