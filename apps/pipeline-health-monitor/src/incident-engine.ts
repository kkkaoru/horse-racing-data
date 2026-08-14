// Run with bun. Direct, durable incident delivery with acknowledgement-aware resend policy.

import { notifyCustom, notifyDiscord, notifySlack } from "./notifiers";
import {
  closeIncident,
  getIncident,
  incidentOutboxKey,
  openIncident,
  putIncident,
  type IncidentState,
} from "./incident-state";
import type { AlertField, AlertMessage, AlertSeverity, Env } from "./types";

const TEN_MINUTES_MS = 10 * 60 * 1000;
const THIRTY_MINUTES_MS = 30 * 60 * 1000;
const ONE_HOUR_MS = 60 * 60 * 1000;
const OUTBOX_TTL_SECONDS = 7 * 24 * 60 * 60;
const RUNBOOK_URL =
  "apps/finish-position-cron/docs/prediction-readiness-monitor-design-2026-08-15.md";

export interface IncidentSignal {
  key: string;
  ok: boolean;
  severity: "critical" | "warning";
  stage: string;
  title: string;
  description: string;
  fields: AlertField[];
}

const elapsedSince = (iso: string, now: Date): number => now.getTime() - Date.parse(iso);

export const shouldSendIncident = (
  state: IncidentState,
  signal: IncidentSignal,
  now: Date,
): boolean => {
  if (state.lastSentAt === null) return true;
  if (state.lastSeverity !== signal.severity || state.lastStage !== signal.stage) return true;
  // T-120 warnings are advisory state transitions, not pages. One warning is
  // enough; acknowledgement-driven resend begins only if the race reaches a
  // critical deadline while still incomplete.
  if (signal.severity === "warning") return false;
  const sinceOpen = elapsedSince(state.openedAt, now);
  const sinceLast = elapsedSince(state.lastSentAt, now);
  if (state.acknowledgedAt !== null) return sinceLast >= ONE_HOUR_MS;
  if (state.sendCount === 1) return sinceOpen >= TEN_MINUTES_MS;
  if (state.sendCount === 2) return sinceOpen >= THIRTY_MINUTES_MS;
  return sinceLast >= ONE_HOUR_MS;
};

const buildMessage = (
  signal: IncidentSignal,
  incidentId: string,
  severity: AlertSeverity,
  now: Date,
): AlertMessage => ({
  checkName: signal.key,
  description: `${signal.description}\nRunbook: ${RUNBOOK_URL}`,
  fields: [
    { name: "Incident ID", value: incidentId },
    { name: "Stage", value: signal.stage },
    ...signal.fields,
  ],
  incidentId,
  severity,
  timestampJst: now.toISOString(),
  title: `${severity === "recovery" ? "[RECOVERY]" : severity === "critical" ? "[CRITICAL]" : "[WARNING]"} ${signal.title}`,
});

const directNotificationPromises = (env: Env, message: AlertMessage): Promise<void>[] => {
  const promises: Promise<void>[] = [];
  if (env.DISCORD_ALERT_WEBHOOK_URL) {
    promises.push(notifyDiscord({ message, webhookUrl: env.DISCORD_ALERT_WEBHOOK_URL }));
  }
  if (env.SLACK_ALERT_WEBHOOK_URL) {
    promises.push(notifySlack({ message, webhookUrl: env.SLACK_ALERT_WEBHOOK_URL }));
  }
  if (env.CUSTOM_ALERT_WEBHOOK_URL) {
    promises.push(notifyCustom({ message, webhookUrl: env.CUSTOM_ALERT_WEBHOOK_URL }));
  }
  return promises;
};

const deliver = async (env: Env, message: AlertMessage): Promise<void> => {
  const outboxKey = incidentOutboxKey(message.incidentId ?? message.checkName);
  await env.STATE_KV.put(outboxKey, JSON.stringify(message), {
    expirationTtl: OUTBOX_TTL_SECONDS,
  });
  const promises = directNotificationPromises(env, message);
  if (promises.length === 0) {
    throw new Error("no direct incident notifier configured");
  }
  await Promise.all(promises);
  await env.STATE_KV.delete(outboxKey);
};

const sendFailure = async (
  env: Env,
  state: IncidentState,
  signal: IncidentSignal,
  now: Date,
): Promise<IncidentState> => {
  const message = buildMessage(signal, state.incidentId, signal.severity, now);
  await deliver(env, message);
  const sent: IncidentState = {
    ...state,
    lastSentAt: now.toISOString(),
    lastSeverity: signal.severity,
    lastStage: signal.stage,
    sendCount: state.sendCount + 1,
  };
  await putIncident(env, sent);
  return sent;
};

const JST_OFFSET_MS = 9 * 60 * 60 * 1000;
const HEARTBEAT_PREFIX = "monitor-heartbeat:";
const HEARTBEAT_TTL_SECONDS = 3 * 24 * 60 * 60;

const heartbeatDateJst = (now: Date): string =>
  new Date(now.getTime() + JST_OFFSET_MS).toISOString().slice(0, 10);

export const sendDailyMonitorHeartbeat = async (env: Env, now: Date): Promise<void> => {
  const date = heartbeatDateJst(now);
  const key = `${HEARTBEAT_PREFIX}${date}`;
  if ((await env.STATE_KV.get(key)) !== null) return;
  const message: AlertMessage = {
    checkName: "pipeline-health-monitor-heartbeat",
    description:
      "Scheduled prediction monitoring is running. Operators must investigate if this daily message is absent.",
    fields: [{ name: "JST date", value: date }],
    incidentId: `heartbeat-${date}`,
    severity: "recovery",
    timestampJst: now.toISOString(),
    title: "[HEALTHY] pipeline health monitor daily heartbeat",
  };
  await deliver(env, message);
  await env.STATE_KV.put(key, now.toISOString(), { expirationTtl: HEARTBEAT_TTL_SECONDS });
};

export const processIncidentSignal = async (
  env: Env,
  signal: IncidentSignal,
  now: Date,
): Promise<void> => {
  const existing = await getIncident(env, signal.key);
  if (signal.ok) {
    if (existing === null || existing.closedAt !== null) return;
    const message = buildMessage(signal, existing.incidentId, "recovery", now);
    await deliver(env, message);
    await closeIncident(env, existing, now);
    return;
  }
  const state = await openIncident(env, signal.key, now);
  if (shouldSendIncident(state, signal, now)) {
    await sendFailure(env, state, signal, now);
  }
};
