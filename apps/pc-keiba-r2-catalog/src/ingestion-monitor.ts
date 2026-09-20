// Runs with bun; bounded, at-least-once notification draining. Broker acceptance is not destination delivery.
import { INGESTION_DLQ_NAME } from "./ingestion-dead-letter-envelope";
import type { ArchiveMonitorStatus } from "./ingestion-archive-journal";
import {
  parseIngestionMonitorNotification,
  type IngestionMonitorNotification,
} from "./ingestion-monitor-alert";

interface RestQueueGauges {
  backlog_count: unknown;
  backlog_bytes: unknown;
  oldest_message_timestamp_ms: number;
}
export interface IngestionMonitorPorts {
  now: () => number;
  readMetrics: () => Promise<unknown>;
  observe: (metrics: unknown, observedAtMs: number) => Promise<void>;
  pending: () => Promise<IngestionMonitorNotification[]>;
  enqueue: (notification: IngestionMonitorNotification) => Promise<void>;
  markEnqueued: (eventId: string) => Promise<void>;
}
export const INGESTION_MONITOR_CRON: string = "2-57/5 * * * *";
const DEADLINE_MS: number = 30000;
const MAX_NOTIFICATIONS: number = 10;
const bounded = async <T>(operation: () => Promise<T>): Promise<T> => {
  const deadline = Promise.withResolvers<never>();
  const timer = setTimeout(
    () => deadline.reject(new Error("Ingestion monitor operation timed out")),
    DEADLINE_MS,
  );
  try {
    return await Promise.race([operation(), deadline.promise]);
  } finally {
    clearTimeout(timer);
  }
};
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const oldestTimestamp = (value: unknown): number => {
  const timestamp: number = value instanceof Date ? value.getTime() : Number.NaN;
  return value === undefined ? 0 : timestamp;
};
const normalizeMetrics = (value: unknown): unknown =>
  record(value)
    ? ({
        backlog_count: value.backlogCount,
        backlog_bytes: value.backlogBytes,
        oldest_message_timestamp_ms: oldestTimestamp(value.oldestMessageTimestamp),
      } satisfies RestQueueGauges)
    : null;

export const runIngestionMonitor = async (ports: IngestionMonitorPorts): Promise<void> => {
  const observedAtMs: number = ports.now();
  const metrics: unknown = await bounded(ports.readMetrics).then(normalizeMetrics, () => null);
  const observed: boolean = await bounded(
    async () => await ports.observe(metrics, observedAtMs),
  ).then(
    () => true,
    () => false,
  );
  // Still drain already retained alerts if the new observation cannot be recorded.
  const notifications = await bounded(ports.pending).catch(() => {
    throw new Error("Ingestion monitor outbox unavailable");
  });
  if (notifications.length > MAX_NOTIFICATIONS)
    throw new Error("Ingestion monitor outbox page exceeds bound");
  const results = await Promise.allSettled(
    notifications.map(async (notification) => {
      await bounded(async () => await ports.enqueue(notification));
      await bounded(async () => await ports.markEnqueued(notification.eventId));
    }),
  );
  if (!observed || results.some((result) => result.status === "rejected"))
    throw new Error("Ingestion monitor work remains unresolved");
};

export const readIngestionMonitorStatus = async (
  env: Pick<CatalogBindings, "INGESTION_ARCHIVE_JOURNAL">,
): Promise<ArchiveMonitorStatus> =>
  await bounded(async () => {
    const journal = env.INGESTION_ARCHIVE_JOURNAL.getByName(INGESTION_DLQ_NAME);
    using snapshot = await journal.monitorStatus();
    return {
      sampledAtMs: snapshot.sampledAtMs,
      lastObservedAtMs: snapshot.lastObservedAtMs,
      assessment: snapshot.assessment,
      oldestPendingArchiveAt: snapshot.oldestPendingArchiveAt,
      oldestPendingNotificationAtMs: snapshot.oldestPendingNotificationAtMs,
      coverageVerified: snapshot.coverageVerified,
      notificationDeliveryVerified: snapshot.notificationDeliveryVerified,
    };
  });

export const monitorIngestionArchives = async (
  env: Pick<
    CatalogBindings,
    "INGESTION_ARCHIVE_JOURNAL" | "INGESTION_DLQ_METRICS" | "INGESTION_ALERTS"
  >,
): Promise<void> => {
  const journal = env.INGESTION_ARCHIVE_JOURNAL.getByName(INGESTION_DLQ_NAME);
  await runIngestionMonitor({
    now: Date.now,
    readMetrics: async () => await env.INGESTION_DLQ_METRICS.metrics(),
    observe: async (metrics, observedAtMs) => {
      await journal.observe(metrics, observedAtMs);
    },
    pending: async () => {
      using notifications = await journal.pendingNotifications(MAX_NOTIFICATIONS);
      return notifications.map(parseIngestionMonitorNotification);
    },
    enqueue: async (notification) => {
      await env.INGESTION_ALERTS.send(notification.message);
    },
    markEnqueued: async (eventId) => {
      await journal.markNotificationEnqueued(eventId);
    },
  });
};
