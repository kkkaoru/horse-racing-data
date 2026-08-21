// Run with bun. Direct, durable incident delivery with acknowledgement-aware resend policy.

import { notifyCustom, notifyDiscord, notifySlack } from "./notifiers";
import {
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
const OUTBOX_DRAIN_LIMIT = 25;
const OUTBOX_VERSION = 1;
const HEARTBEAT_TTL_SECONDS = 3 * 24 * 60 * 60;
const HEARTBEAT_PREFIX = "monitor-heartbeat:";
const RUNBOOK_URL =
  "apps/finish-position-cron/docs/prediction-readiness-monitor-design-2026-08-15.md";
const JST_OFFSET_MS = 9 * 60 * 60 * 1000;
const JST_ISO_FRACTIONAL_TRIM_LENGTH = 19;
const JST_ISO_SUFFIX = "+09:00";
const ACTION_BY_SEVERITY: Record<AlertSeverity, string> = {
  warning:
    "Verify prediction coverage for the named race and confirm generation is progressing before the next deadline.",
  critical:
    "Immediately inspect delivery_paused; if true run `bunx wrangler queues resume-delivery finish-position-predict-queue`, then verify canary consumption and prediction rows before acknowledging this incident.",
  recovery:
    "Verify prediction coverage and queue delivery are restored; take no further action unless the incident recurs.",
};
const TERMINAL_CLOSE_ACTION =
  "Record the unresolved readiness miss for follow-up; this race is no longer observable by the active-day endpoint.";
const TITLE_PREFIX_BY_SEVERITY: Record<AlertSeverity, string> = {
  warning: "[WARNING]",
  critical: "[CRITICAL]",
  recovery: "[RECOVERY]",
};

export interface IncidentSignal {
  key: string;
  ok: boolean;
  severity: "critical" | "warning";
  stage: string;
  title: string;
  description: string;
  fields: AlertField[];
}

type NotificationDestination = "custom" | "discord" | "slack";

interface FailureCompletion {
  kind: "failure";
  incidentId: string;
  signalKey: string;
  sentAt: string;
  severity: "critical" | "warning";
  stage: string;
  sendCount: number;
}

interface RecoveryCompletion {
  kind: "recovery";
  incidentId: string;
  signalKey: string;
  closedAt: string;
}

interface HeartbeatCompletion {
  kind: "heartbeat";
  key: string;
  value: string;
}

type DeliveryCompletion = FailureCompletion | RecoveryCompletion | HeartbeatCompletion;

interface IncidentOutbox {
  version: number;
  message: AlertMessage;
  pendingDestinations: NotificationDestination[];
  completion: DeliveryCompletion;
}

interface IncidentAlertMessage extends AlertMessage {
  incidentId: string;
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

const formatJstIso = (date: Date): string => {
  const shifted = new Date(date.getTime() + JST_OFFSET_MS);
  return `${shifted.toISOString().slice(0, JST_ISO_FRACTIONAL_TRIM_LENGTH)}${JST_ISO_SUFFIX}`;
};

const MS_PER_SECOND = 1_000;
const MS_PER_MINUTE = 60 * MS_PER_SECOND;
const MS_PER_HOUR = 60 * MS_PER_MINUTE;
const MS_PER_DAY = 24 * MS_PER_HOUR;

const formatElapsedDuration = (openedAt: string, now: Date): string => {
  const elapsedMs = Math.max(0, now.getTime() - Date.parse(openedAt));
  const days = Math.floor(elapsedMs / MS_PER_DAY);
  const hours = Math.floor((elapsedMs % MS_PER_DAY) / MS_PER_HOUR);
  const minutes = Math.floor((elapsedMs % MS_PER_HOUR) / MS_PER_MINUTE);
  const seconds = Math.floor((elapsedMs % MS_PER_MINUTE) / MS_PER_SECOND);
  const parts = [
    days > 0 ? `${days}d` : null,
    hours > 0 ? `${hours}h` : null,
    minutes > 0 ? `${minutes}m` : null,
    seconds > 0 ? `${seconds}s` : null,
  ].filter((part): part is string => part !== null);
  return parts.length > 0 ? parts.join(" ") : "0m";
};

const titlePrefix = (severity: AlertSeverity): string => TITLE_PREFIX_BY_SEVERITY[severity];

const buildMessage = (
  signal: IncidentSignal,
  incidentId: string,
  openedAt: string,
  severity: AlertSeverity,
  now: Date,
): IncidentAlertMessage => ({
  checkName: signal.key,
  description: `${signal.description}\nRunbook: ${RUNBOOK_URL}`,
  fields: [
    { name: "Incident ID", value: incidentId },
    { name: "First detected (JST)", value: formatJstIso(new Date(openedAt)) },
    { name: "Duration", value: formatElapsedDuration(openedAt, now) },
    { name: "Action", value: ACTION_BY_SEVERITY[severity] },
    { name: "Stage", value: signal.stage },
    ...signal.fields,
  ],
  incidentId,
  severity,
  timestampJst: formatJstIso(now),
  title: `${titlePrefix(severity)} ${signal.title}`,
});

const buildTerminalCloseMessage = (
  state: IncidentState,
  now: Date,
  reason: string,
): IncidentAlertMessage => ({
  checkName: state.signalKey,
  description: `${reason}\nRunbook: ${RUNBOOK_URL}`,
  fields: [
    { name: "Incident ID", value: state.incidentId },
    { name: "First detected (JST)", value: formatJstIso(new Date(state.openedAt)) },
    { name: "Duration", value: formatElapsedDuration(state.openedAt, now) },
    { name: "Action", value: TERMINAL_CLOSE_ACTION },
    { name: "Stage", value: "monitoring-window-ended-unresolved" },
  ],
  incidentId: state.incidentId,
  severity: "recovery",
  timestampJst: formatJstIso(now),
  title: `[CLOSED UNRESOLVED] ${state.signalKey}`,
});

const configuredDestinations = (env: Env): NotificationDestination[] => {
  const destinations: NotificationDestination[] = [];
  if (env.DISCORD_ALERT_WEBHOOK_URL) destinations.push("discord");
  if (env.SLACK_ALERT_WEBHOOK_URL) destinations.push("slack");
  if (env.CUSTOM_ALERT_WEBHOOK_URL) destinations.push("custom");
  return destinations.length === 0 ? ["discord"] : destinations;
};

const notifyDestination = async (
  env: Env,
  message: AlertMessage,
  destination: NotificationDestination,
): Promise<void> => {
  if (destination === "discord") {
    if (!env.DISCORD_ALERT_WEBHOOK_URL) throw new Error("discord notifier is not configured");
    await notifyDiscord({ message, webhookUrl: env.DISCORD_ALERT_WEBHOOK_URL });
    return;
  }
  if (destination === "slack") {
    if (!env.SLACK_ALERT_WEBHOOK_URL) throw new Error("slack notifier is not configured");
    await notifySlack({ message, webhookUrl: env.SLACK_ALERT_WEBHOOK_URL });
    return;
  }
  if (!env.CUSTOM_ALERT_WEBHOOK_URL) throw new Error("custom notifier is not configured");
  await notifyCustom({ message, webhookUrl: env.CUSTOM_ALERT_WEBHOOK_URL });
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isIncidentOutbox = (value: unknown): value is IncidentOutbox =>
  isRecord(value) && value.version === OUTBOX_VERSION;

const finalizeDelivery = async (env: Env, completion: DeliveryCompletion): Promise<void> => {
  if (completion.kind === "heartbeat") {
    await env.STATE_KV.put(completion.key, completion.value, {
      expirationTtl: HEARTBEAT_TTL_SECONDS,
    });
    return;
  }
  const state = await getIncident(env, completion.signalKey);
  if (state === null || state.incidentId !== completion.incidentId) return;
  if (completion.kind === "recovery") {
    await putIncident(env, { ...state, closedAt: completion.closedAt });
    return;
  }
  await putIncident(env, {
    ...state,
    lastSentAt: completion.sentAt,
    lastSeverity: completion.severity,
    lastStage: completion.stage,
    sendCount: Math.max(state.sendCount, completion.sendCount),
  });
};

const attemptOutbox = async (env: Env, key: string, outbox: IncidentOutbox): Promise<void> => {
  const results = await Promise.all(
    outbox.pendingDestinations.map(async (destination) => {
      try {
        await notifyDestination(env, outbox.message, destination);
        return null;
      } catch (error) {
        console.error(
          "pipeline-health-monitor outbox destination delivery failed",
          destination,
          String(error),
        );
        return destination;
      }
    }),
  );
  const pendingDestinations = results.filter(
    (destination): destination is NotificationDestination => destination !== null,
  );
  if (pendingDestinations.length > 0) {
    await env.STATE_KV.put(key, JSON.stringify({ ...outbox, pendingDestinations }), {
      expirationTtl: OUTBOX_TTL_SECONDS,
    });
    throw new Error(`incident delivery pending destinations=${pendingDestinations.join(",")}`);
  }
  await finalizeDelivery(env, outbox.completion);
  await env.STATE_KV.delete(key);
};

const deliver = async (
  env: Env,
  message: IncidentAlertMessage,
  completion: DeliveryCompletion,
): Promise<void> => {
  const outboxKey = incidentOutboxKey(message.incidentId);
  const outbox: IncidentOutbox = {
    completion,
    message,
    pendingDestinations: configuredDestinations(env),
    version: OUTBOX_VERSION,
  };
  await env.STATE_KV.put(outboxKey, JSON.stringify(outbox), {
    expirationTtl: OUTBOX_TTL_SECONDS,
  });
  await attemptOutbox(env, outboxKey, outbox);
};

const sendFailure = async (
  env: Env,
  state: IncidentState,
  signal: IncidentSignal,
  now: Date,
): Promise<IncidentState> => {
  const message = buildMessage(signal, state.incidentId, state.openedAt, signal.severity, now);
  const sent: IncidentState = {
    ...state,
    lastSentAt: now.toISOString(),
    lastSeverity: signal.severity,
    lastStage: signal.stage,
    sendCount: state.sendCount + 1,
  };
  await deliver(env, message, {
    incidentId: state.incidentId,
    kind: "failure",
    sendCount: sent.sendCount,
    sentAt: now.toISOString(),
    severity: signal.severity,
    signalKey: state.signalKey,
    stage: signal.stage,
  });
  return sent;
};

const heartbeatDateJst = (now: Date): string =>
  new Date(now.getTime() + JST_OFFSET_MS).toISOString().slice(0, 10);

export const sendDailyMonitorHeartbeat = async (env: Env, now: Date): Promise<void> => {
  const date = heartbeatDateJst(now);
  const key = `${HEARTBEAT_PREFIX}${date}`;
  if ((await env.STATE_KV.get(key)) !== null) return;
  const message: IncidentAlertMessage = {
    checkName: "pipeline-health-monitor-heartbeat",
    description:
      "Scheduled prediction monitoring is running. Operators must investigate if this daily message is absent.",
    fields: [
      { name: "JST date", value: date },
      {
        name: "Action",
        value:
          "If this heartbeat is absent by the end of the JST race day, verify the monitor Worker and alert delivery path.",
      },
    ],
    incidentId: `heartbeat-${date}`,
    severity: "recovery",
    timestampJst: formatJstIso(now),
    title: "[HEALTHY] pipeline health monitor daily heartbeat",
  };
  await deliver(env, message, { key, kind: "heartbeat", value: now.toISOString() });
};

export const drainIncidentOutbox = async (env: Env): Promise<void> => {
  const listed = await env.STATE_KV.list({ limit: OUTBOX_DRAIN_LIMIT, prefix: "incident-outbox:" });
  await Promise.all(
    listed.keys.map(async ({ name }) => {
      const value: unknown = await env.STATE_KV.get(name, "json");
      if (!isIncidentOutbox(value)) {
        console.error("pipeline-health-monitor discarding unreadable legacy outbox", name);
        await env.STATE_KV.delete(name);
        return;
      }
      try {
        await attemptOutbox(env, name, value);
      } catch (error) {
        console.error("pipeline-health-monitor outbox retry remains pending", name, String(error));
      }
    }),
  );
};

export const terminalCloseIncident = async (
  env: Env,
  state: IncidentState,
  now: Date,
  reason: string,
): Promise<void> => {
  if (state.closedAt !== null) return;
  await deliver(env, buildTerminalCloseMessage(state, now, reason), {
    closedAt: now.toISOString(),
    incidentId: state.incidentId,
    kind: "recovery",
    signalKey: state.signalKey,
  });
};

export const processIncidentSignal = async (
  env: Env,
  signal: IncidentSignal,
  now: Date,
): Promise<void> => {
  const existing = await getIncident(env, signal.key);
  if (signal.ok) {
    if (existing === null || existing.closedAt !== null) return;
    const message = buildMessage(signal, existing.incidentId, existing.openedAt, "recovery", now);
    await deliver(env, message, {
      closedAt: now.toISOString(),
      incidentId: existing.incidentId,
      kind: "recovery",
      signalKey: existing.signalKey,
    });
    return;
  }
  const state = await openIncident(env, signal.key, now);
  if (shouldSendIncident(state, signal, now)) {
    await sendFailure(env, state, signal, now);
  }
};
