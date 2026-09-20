// Runs with bun; private archival-attempt journal, not a business-completion or source-coverage authority.
import { DurableObject } from "cloudflare:workers";
import type { ArchiveAttemptIdentity } from "./ingestion-archive-attempt";
import type { IngestionAcceptance } from "./ingestion-buffer";
import {
  buildIngestionMonitorNotification,
  parseIngestionMonitorNotification,
  type IngestionMonitorNotification,
} from "./ingestion-monitor-alert";

interface ArchiveRow extends Record<string, SqlStorageValue> {
  request_id: string;
  queued_at: string;
  receipt: string | null;
}
interface NotificationRow extends Record<string, SqlStorageValue> {
  event_id: string;
  payload: string;
}
interface PendingArchives {
  entries: ArchiveAttemptIdentity[];
  coverageVerified: false;
}
interface StoredAssessment {
  observedAtMs: number;
  assessment: "quiet" | "backlog" | "unknown";
}
export interface ArchiveMonitorStatus {
  sampledAtMs: number;
  lastObservedAtMs: number | null;
  assessment: "quiet" | "backlog" | "unknown" | "unclassified";
  oldestPendingArchiveAt: string | null;
  oldestPendingNotificationAtMs: number | null;
  coverageVerified: false;
  notificationDeliveryVerified: false;
}
const ASSESSMENT_KEY: string = "archive-monitor/last-assessment-v1";
const OBSERVATION_KEY: string = "archive-monitor/last-observed-v1";
const MAX_STORAGE_BYTES: number = 8_000_000_000;
const MAX_SEQUENCE: bigint = 9223372036854775807n;
const ID_PATTERN: RegExp = /^dlq_[a-f0-9]{64}$/u;
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const storedAssessment = (
  value: unknown,
  observedAtMs: number | null,
): ArchiveMonitorStatus["assessment"] => {
  if (value === undefined) return "unclassified";
  if (!record(value) || observedAtMs === null || value.observedAtMs !== observedAtMs)
    throw new Error("Invalid archive monitor assessment");
  const assessment: unknown = value.assessment;
  if (assessment !== "quiet" && assessment !== "backlog" && assessment !== "unknown")
    throw new Error("Invalid archive monitor assessment");
  return assessment;
};
const identity = (value: unknown): ArchiveAttemptIdentity => {
  if (
    !record(value) ||
    typeof value.requestId !== "string" ||
    value.requestId.length !== 68 ||
    !ID_PATTERN.test(value.requestId) ||
    typeof value.queuedAt !== "string" ||
    value.queuedAt.length !== 24 ||
    !Number.isFinite(Date.parse(value.queuedAt)) ||
    new Date(value.queuedAt).toISOString() !== value.queuedAt
  )
    throw new Error("Invalid archive attempt identity");
  return { requestId: value.requestId, queuedAt: value.queuedAt };
};
const acceptance = (value: unknown): IngestionAcceptance => {
  if (
    !record(value) ||
    value.source !== "sync-realtime-data-hot-v2" ||
    typeof value.requestId !== "string" ||
    value.requestId.length !== 68 ||
    !ID_PATTERN.test(value.requestId) ||
    typeof value.digest !== "string" ||
    value.digest.length !== 64 ||
    !/^[a-f0-9]{64}$/u.test(value.digest) ||
    typeof value.sequence !== "string" ||
    !/^[1-9]\d{0,18}$/u.test(value.sequence) ||
    BigInt(value.sequence) > MAX_SEQUENCE ||
    BigInt(value.sequence).toString() !== value.sequence ||
    value.accepted !== true
  )
    throw new Error("Invalid archive receipt");
  return {
    source: value.source,
    requestId: value.requestId,
    digest: value.digest,
    sequence: value.sequence,
    accepted: true,
  };
};

export class IngestionArchiveJournal extends DurableObject<unknown> {
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
    void this.ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(
        "CREATE TABLE IF NOT EXISTS __archive_attempts_v1 (request_id TEXT PRIMARY KEY, queued_at TEXT NOT NULL, receipt TEXT)",
      );
      this.ctx.storage.sql.exec(
        "CREATE INDEX IF NOT EXISTS __archive_pending_v1 ON __archive_attempts_v1 (queued_at, request_id) WHERE receipt IS NULL",
      );
      this.ctx.storage.sql.exec(
        "CREATE TABLE IF NOT EXISTS __archive_alert_outbox_v1 (sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL, broker_accepted_at INTEGER)",
      );
      this.ctx.storage.sql.exec(
        "CREATE INDEX IF NOT EXISTS __archive_alert_pending_v1 ON __archive_alert_outbox_v1 (sequence) WHERE broker_accepted_at IS NULL",
      );
    });
  }

  begin(value: unknown): void {
    const attempt: ArchiveAttemptIdentity = identity(value);
    this.ctx.storage.transactionSync(() => {
      const previous: ArchiveRow | undefined = this.#lookup(attempt.requestId);
      if (previous !== undefined) {
        if (previous.queued_at !== attempt.queuedAt)
          throw new Error("Archive attempt identity conflict");
        return;
      }
      if (this.ctx.storage.sql.databaseSize >= MAX_STORAGE_BYTES)
        throw new Error("Archive journal storage budget reached");
      this.ctx.storage.sql.exec(
        "INSERT INTO __archive_attempts_v1 (request_id, queued_at) VALUES (?, ?)",
        attempt.requestId,
        attempt.queuedAt,
      );
    });
  }

  recordReceipt(value: unknown): void {
    const receipt: IngestionAcceptance = acceptance(value);
    const serialized: string = JSON.stringify(receipt);
    this.ctx.storage.transactionSync(() => {
      const previous: ArchiveRow | undefined = this.#lookup(receipt.requestId);
      if (previous === undefined) throw new Error("Archive attempt not found");
      if (previous.receipt !== null && previous.receipt !== serialized)
        throw new Error("Archive receipt conflict");
      if (previous.receipt === serialized) return;
      this.ctx.storage.sql.exec(
        "UPDATE __archive_attempts_v1 SET receipt = ? WHERE request_id = ? AND receipt IS NULL",
        serialized,
        receipt.requestId,
      );
    });
  }

  pending(limit: unknown): PendingArchives {
    if (typeof limit !== "number" || !Number.isInteger(limit) || limit < 1 || limit > 100)
      throw new Error("Invalid archive pending limit");
    const entries: ArchiveAttemptIdentity[] = this.ctx.storage.sql
      .exec<ArchiveRow>(
        "SELECT request_id, queued_at FROM __archive_attempts_v1 WHERE receipt IS NULL ORDER BY queued_at, request_id LIMIT ?",
        limit,
      )
      .toArray()
      .map((row) => identity({ requestId: row.request_id, queuedAt: row.queued_at }));
    return { entries, coverageVerified: false };
  }

  observe(metrics: unknown, observedAtMs: unknown): void {
    if (typeof observedAtMs !== "number" || !Number.isSafeInteger(observedAtMs) || observedAtMs < 0)
      throw new Error("Invalid archive observation time");
    const nowMs: number = Date.now();
    this.ctx.storage.transactionSync(() => {
      const previous: number | null = this.#lastObserved();
      if (previous !== null && observedAtMs < previous) return;
      const notification = buildIngestionMonitorNotification({
        metrics,
        pending: this.pending(100).entries,
        observedAtMs,
        nowMs,
      });
      if (observedAtMs <= nowMs) {
        const cause: "backlog" | "unknown" =
          notification?.message.checkName === "hot-ingestion-archive:backlog"
            ? "backlog"
            : "unknown";
        const assessment: StoredAssessment = {
          observedAtMs,
          assessment: notification === null ? "quiet" : cause,
        };
        this.ctx.storage.kv.put(OBSERVATION_KEY, observedAtMs);
        this.ctx.storage.kv.put(ASSESSMENT_KEY, assessment);
      }
      if (notification === null) return;
      const existing = this.ctx.storage.sql
        .exec<NotificationRow>(
          "SELECT event_id, payload FROM __archive_alert_outbox_v1 WHERE event_id = ?",
          notification.eventId,
        )
        .toArray()[0];
      // Coalesce by hour/cause/severity, keeping the first immutable message, including after broker acceptance.
      if (existing !== undefined) return;
      if (this.ctx.storage.sql.databaseSize >= MAX_STORAGE_BYTES)
        throw new Error("Archive notification storage budget reached");
      this.ctx.storage.sql.exec(
        "INSERT INTO __archive_alert_outbox_v1 (event_id, payload) VALUES (?, ?)",
        notification.eventId,
        JSON.stringify(notification),
      );
    });
  }

  pendingNotifications(limit: unknown): IngestionMonitorNotification[] {
    if (typeof limit !== "number" || !Number.isInteger(limit) || limit < 1 || limit > 10)
      throw new Error("Invalid archive notification limit");
    return this.ctx.storage.sql
      .exec<NotificationRow>(
        "SELECT event_id, payload FROM __archive_alert_outbox_v1 WHERE broker_accepted_at IS NULL ORDER BY sequence LIMIT ?",
        limit,
      )
      .toArray()
      .map((row) => {
        const value: unknown = JSON.parse(row.payload);
        const notification: IngestionMonitorNotification = parseIngestionMonitorNotification(value);
        if (notification.eventId !== row.event_id)
          throw new Error("Archive notification identity mismatch");
        return notification;
      });
  }

  markNotificationEnqueued(eventId: unknown): void {
    if (typeof eventId !== "string" || eventId.length !== 64 || !/^[a-f0-9]{64}$/u.test(eventId))
      throw new Error("Invalid archive notification identity");
    const rows = this.ctx.storage.sql
      .exec(
        "UPDATE __archive_alert_outbox_v1 SET broker_accepted_at = COALESCE(broker_accepted_at, ?) WHERE event_id = ? RETURNING event_id",
        Date.now(),
        eventId,
      )
      .toArray();
    if (rows.length !== 1) throw new Error("Archive notification not found");
  }

  monitorStatus(): ArchiveMonitorStatus {
    return this.ctx.storage.transactionSync(() => {
      const lastObservedAtMs: number | null = this.#lastObserved();
      const stored: unknown = this.ctx.storage.kv.get(ASSESSMENT_KEY);
      const assessment = storedAssessment(stored, lastObservedAtMs);
      return {
        sampledAtMs: Date.now(),
        lastObservedAtMs,
        assessment,
        oldestPendingArchiveAt: this.pending(1).entries[0]?.queuedAt ?? null,
        oldestPendingNotificationAtMs: this.pendingNotifications(1)[0]?.observedAtMs ?? null,
        coverageVerified: false,
        notificationDeliveryVerified: false,
      };
    });
  }

  #lastObserved(): number | null {
    const value: unknown = this.ctx.storage.kv.get(OBSERVATION_KEY);
    if (value === undefined) return null;
    if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
      throw new Error("Invalid archive monitor watermark");
    return value;
  }

  #lookup(requestId: string): ArchiveRow | undefined {
    return this.ctx.storage.sql
      .exec<ArchiveRow>(
        "SELECT request_id, queued_at, receipt FROM __archive_attempts_v1 WHERE request_id = ?",
        requestId,
      )
      .toArray()[0];
  }
}
