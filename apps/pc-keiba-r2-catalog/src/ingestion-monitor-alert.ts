// Runs with bun; notification planning only, never sends, acknowledges or claims complete archival.
import { createHash } from "node:crypto";
import type { AlertMessage } from "../../pipeline-health-monitor/src/types";
import type { ArchiveAttemptIdentity } from "./ingestion-archive-attempt";
import { assessIngestionBacklog } from "./ingestion-backlog";
import { INGESTION_DLQ_NAME } from "./ingestion-dead-letter-envelope";

interface MonitorField {
  name: string;
  value: string;
}
interface MonitorObservation {
  metrics: unknown;
  pending: readonly ArchiveAttemptIdentity[];
  observedAtMs: number;
  nowMs: number;
}
export interface IngestionMonitorNotification {
  eventId: string;
  observedAtMs: number;
  message: AlertMessage;
}
const HOUR_MS: number = 3600000;
const JST_OFFSET_MS: number = 9 * HOUR_MS;
const JST_TIMESTAMP_LENGTH: number = 29;
const POLICY: Parameters<typeof assessIngestionBacklog>[0]["policy"] = {
  retentionSeconds: 1209600,
  warningAgeSeconds: 3600,
  criticalAgeSeconds: 1036800,
  maxObservationAgeSeconds: 300,
};

const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const text = (value: unknown): value is string => typeof value === "string" && value.length <= 2048;
const formatJstTimestamp = (nowMs: number): string =>
  new Date(nowMs + JST_OFFSET_MS).toISOString().replace("Z", "+09:00");
const jstTimestamp = (value: unknown): value is string =>
  typeof value === "string" &&
  value.length === JST_TIMESTAMP_LENGTH &&
  Number.isFinite(Date.parse(value)) &&
  formatJstTimestamp(Date.parse(value)) === value;
const parseField = (value: unknown): MonitorField => {
  if (!record(value) || !text(value.name) || !text(value.value))
    throw new Error("Invalid monitor notification field");
  return { name: value.name, value: value.value };
};
export const parseIngestionMonitorNotification = (value: unknown): IngestionMonitorNotification => {
  if (
    !record(value) ||
    typeof value.eventId !== "string" ||
    value.eventId.length !== 64 ||
    !/^[a-f0-9]{64}$/u.test(value.eventId) ||
    typeof value.observedAtMs !== "number" ||
    !Number.isSafeInteger(value.observedAtMs) ||
    value.observedAtMs < 0 ||
    !record(value.message)
  )
    throw new Error("Invalid monitor notification");
  const message = value.message;
  if (
    (message.checkName !== "hot-ingestion-archive:backlog" &&
      message.checkName !== "hot-ingestion-archive:observation") ||
    (message.severity !== "warning" && message.severity !== "critical") ||
    !text(message.title) ||
    !text(message.description) ||
    !jstTimestamp(message.timestampJst) ||
    !Array.isArray(message.fields) ||
    message.fields.length > 10
  )
    throw new Error("Invalid monitor notification message");
  return {
    eventId: value.eventId,
    observedAtMs: value.observedAtMs,
    message: {
      checkName: message.checkName,
      severity: message.severity,
      title: message.title,
      description: message.description,
      timestampJst: message.timestampJst,
      fields: message.fields.map(parseField),
    },
  };
};

export const buildIngestionMonitorNotification = (
  input: MonitorObservation,
): IngestionMonitorNotification | null => {
  if (
    !Number.isSafeInteger(input.nowMs) ||
    input.nowMs < 0 ||
    !Number.isFinite(new Date(input.nowMs + JST_OFFSET_MS).getTime()) ||
    formatJstTimestamp(input.nowMs).length !== JST_TIMESTAMP_LENGTH
  )
    throw new Error("Invalid monitor clock");
  const gauge = assessIngestionBacklog({ ...input, policy: POLICY });
  const ages: number[] = input.pending.map(
    (entry) => (input.nowMs - Date.parse(entry.queuedAt)) / 1000,
  );
  const invalidPending: boolean =
    input.pending.length > 100 || ages.some((age) => !Number.isFinite(age) || age < 0);
  const oldestAge: number = Math.max(0, ...ages);
  const unknown: boolean = gauge.status === "unknown" || invalidPending;
  const critical: boolean =
    unknown || gauge.status === "critical" || oldestAge >= POLICY.criticalAgeSeconds;
  const warning: boolean = gauge.status === "warning" || oldestAge >= POLICY.warningAgeSeconds;
  if (!critical && !warning) return null;
  const severity: "critical" | "warning" = critical ? "critical" : "warning";
  const cause: string = unknown ? "observation" : "backlog";
  const eventId: string = createHash("sha256")
    .update(
      JSON.stringify([INGESTION_DLQ_NAME, cause, severity, Math.floor(input.nowMs / HOUR_MS)]),
    )
    .digest("hex");
  return {
    eventId,
    observedAtMs: input.observedAtMs,
    message: {
      checkName: `hot-ingestion-archive:${cause}`,
      severity,
      title: `Hot ingestion archive ${severity}`,
      description:
        "Archive backlog or observation requires attention. Queue gauges and tracked attempts do not certify complete coverage; no business job has been replayed.",
      fields: [
        { name: "Queue", value: INGESTION_DLQ_NAME },
        { name: "Gauge assessment", value: `${gauge.status}: ${gauge.reason}` },
        { name: "Known pending entries (bounded)", value: String(input.pending.length) },
        {
          name: "Oldest tracked age (seconds)",
          value: invalidPending ? "unknown" : String(oldestAge),
        },
        {
          name: "Coverage",
          value: "Unverified; delayed/in-flight messages and pre-tracking gaps are not resolved.",
        },
      ],
      timestampJst: formatJstTimestamp(input.nowMs),
    },
  };
};
