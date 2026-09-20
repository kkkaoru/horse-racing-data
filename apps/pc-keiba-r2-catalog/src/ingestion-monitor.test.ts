// Runs with bun via Vitest; Queue, clock and journal effects are mocked.
import { afterEach, expect, test, vi } from "vitest";
import { mockDeep } from "vitest-mock-extended";
import type { IngestionArchiveJournal } from "./ingestion-archive-journal";
import type { IngestionMonitorNotification } from "./ingestion-monitor-alert";
import {
  monitorIngestionArchives,
  readIngestionMonitorStatus,
  runIngestionMonitor,
  type IngestionMonitorPorts,
} from "./ingestion-monitor";

const notification: IngestionMonitorNotification = {
  eventId: "a".repeat(64),
  observedAtMs: 10000000,
  message: {
    checkName: "hot-ingestion-archive:observation",
    severity: "critical",
    title: "Archive",
    description: "Coverage unknown",
    fields: [],
    timestampJst: "2026-09-16T12:00:00.000+09:00",
  },
};
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

test("status copies scalar fields and disposes the RPC result without modifying the journal", async () => {
  const env = mockDeep<Pick<CatalogBindings, "INGESTION_ARCHIVE_JOURNAL">>();
  const journal = mockDeep<DurableObjectStub<IngestionArchiveJournal>>();
  env.INGESTION_ARCHIVE_JOURNAL.getByName.mockReturnValue(journal);
  const dispose = vi.fn();
  journal.monitorStatus.mockResolvedValue({
    sampledAtMs: 2,
    lastObservedAtMs: 1,
    assessment: "quiet",
    oldestPendingArchiveAt: null,
    oldestPendingNotificationAtMs: null,
    coverageVerified: false,
    notificationDeliveryVerified: false,
    [Symbol.dispose]: dispose,
  });
  expect(await readIngestionMonitorStatus(env)).toStrictEqual({
    sampledAtMs: 2,
    lastObservedAtMs: 1,
    assessment: "quiet",
    oldestPendingArchiveAt: null,
    oldestPendingNotificationAtMs: null,
    coverageVerified: false,
    notificationDeliveryVerified: false,
  });
  expect(dispose).toHaveBeenCalledOnce();
  expect(journal.observe).not.toHaveBeenCalled();
  expect(journal.markNotificationEnqueued).not.toHaveBeenCalled();
});
test("a timed-out status read still disposes its late RPC result", async () => {
  vi.useFakeTimers();
  const env = mockDeep<Pick<CatalogBindings, "INGESTION_ARCHIVE_JOURNAL">>();
  const journal = mockDeep<DurableObjectStub<IngestionArchiveJournal>>();
  env.INGESTION_ARCHIVE_JOURNAL.getByName.mockReturnValue(journal);
  const pending = Promise.withResolvers<Awaited<ReturnType<typeof journal.monitorStatus>>>();
  journal.monitorStatus.mockReturnValue(
    Object.assign(pending.promise, {
      sampledAtMs: pending.promise.then(() => 2),
      lastObservedAtMs: pending.promise.then(() => null),
      assessment: pending.promise.then((): "unclassified" => "unclassified"),
      oldestPendingArchiveAt: pending.promise.then(() => null),
      oldestPendingNotificationAtMs: pending.promise.then(() => null),
      coverageVerified: pending.promise.then((): false => false),
      notificationDeliveryVerified: pending.promise.then((): false => false),
    }),
  );
  const dispose = vi.fn();
  const result = expect(readIngestionMonitorStatus(env)).rejects.toThrow(
    "Ingestion monitor operation timed out",
  );
  await vi.advanceTimersByTimeAsync(30000);
  await result;
  pending.resolve({
    sampledAtMs: 2,
    lastObservedAtMs: null,
    assessment: "unclassified",
    oldestPendingArchiveAt: null,
    oldestPendingNotificationAtMs: null,
    coverageVerified: false,
    notificationDeliveryVerified: false,
    [Symbol.dispose]: dispose,
  });
  await vi.advanceTimersByTimeAsync(1);
  expect(dispose).toHaveBeenCalledOnce();
  expect(vi.getTimerCount()).toBe(0);
});

test.each([
  { metrics: { backlogCount: 0, backlogBytes: 0 }, oldest: 0 },
  {
    metrics: { backlogCount: 1, backlogBytes: 1, oldestMessageTimestamp: new Date(1000) },
    oldest: 1000,
  },
  {
    metrics: { backlogCount: 1, backlogBytes: 1, oldestMessageTimestamp: "invalid" },
    oldest: Number.NaN,
  },
])(
  "normalizes SDK gauges without reading or sending business messages",
  async ({ metrics, oldest }) => {
    const ports = {
      now: vi.fn(() => 10000000),
      readMetrics: vi.fn<IngestionMonitorPorts["readMetrics"]>().mockResolvedValue(metrics),
      observe: vi.fn<IngestionMonitorPorts["observe"]>().mockResolvedValue(undefined),
      pending: vi.fn<IngestionMonitorPorts["pending"]>().mockResolvedValue([]),
      enqueue: vi.fn<IngestionMonitorPorts["enqueue"]>(),
      markEnqueued: vi.fn<IngestionMonitorPorts["markEnqueued"]>(),
    } satisfies IngestionMonitorPorts;
    await runIngestionMonitor(ports);
    expect(ports.observe).toHaveBeenCalledWith(
      {
        backlog_count: metrics.backlogCount,
        backlog_bytes: metrics.backlogBytes,
        oldest_message_timestamp_ms: oldest,
      },
      10000000,
    );
    expect(ports.enqueue).not.toHaveBeenCalled();
  },
);
test.each([null, new Error("provider details must not escape")])(
  "records unavailable or malformed gauges as unknown",
  async (value) => {
    const ports = {
      now: vi.fn(() => 10000000),
      readMetrics: vi.fn<IngestionMonitorPorts["readMetrics"]>(async () => {
        if (value instanceof Error) throw value;
        return value;
      }),
      observe: vi.fn<IngestionMonitorPorts["observe"]>().mockResolvedValue(undefined),
      pending: vi.fn<IngestionMonitorPorts["pending"]>().mockResolvedValue([]),
      enqueue: vi.fn<IngestionMonitorPorts["enqueue"]>(),
      markEnqueued: vi.fn<IngestionMonitorPorts["markEnqueued"]>(),
    } satisfies IngestionMonitorPorts;
    await runIngestionMonitor(ports);
    expect(ports.observe).toHaveBeenCalledWith(null, 10000000);
  },
);
test("waits for broker acceptance before marking the retained outbox entry", async () => {
  const events: string[] = [];
  const ports = {
    now: vi.fn(() => 10000000),
    readMetrics: vi.fn<IngestionMonitorPorts["readMetrics"]>().mockResolvedValue(null),
    observe: vi.fn<IngestionMonitorPorts["observe"]>().mockResolvedValue(undefined),
    pending: vi.fn<IngestionMonitorPorts["pending"]>().mockResolvedValue([notification]),
    enqueue: vi.fn<IngestionMonitorPorts["enqueue"]>(async () => {
      events.push("enqueue");
    }),
    markEnqueued: vi.fn<IngestionMonitorPorts["markEnqueued"]>(async () => {
      events.push("mark");
    }),
  } satisfies IngestionMonitorPorts;
  await runIngestionMonitor(ports);
  expect(events).toStrictEqual(["enqueue", "mark"]);
});
test.each<{ stage: "enqueue" | "markEnqueued" }>([{ stage: "enqueue" }, { stage: "markEnqueued" }])(
  "uncertain $stage fails the tick without claiming notification delivery",
  async ({ stage }) => {
    const ports = {
      now: vi.fn(() => 10000000),
      readMetrics: vi.fn<IngestionMonitorPorts["readMetrics"]>().mockResolvedValue(null),
      observe: vi.fn<IngestionMonitorPorts["observe"]>().mockResolvedValue(undefined),
      pending: vi.fn<IngestionMonitorPorts["pending"]>().mockResolvedValue([notification]),
      enqueue: vi.fn<IngestionMonitorPorts["enqueue"]>().mockResolvedValue(undefined),
      markEnqueued: vi.fn<IngestionMonitorPorts["markEnqueued"]>().mockResolvedValue(undefined),
    } satisfies IngestionMonitorPorts;
    ports[stage].mockRejectedValue(new Error("uncertain acknowledgement"));
    await expect(runIngestionMonitor(ports)).rejects.toThrow("remains unresolved");
    if (stage === "enqueue") expect(ports.markEnqueued).not.toHaveBeenCalled();
  },
);
test("observation failure still drains previously retained alerts but leaves the tick failed", async () => {
  const ports = {
    now: vi.fn(() => 10000000),
    readMetrics: vi.fn<IngestionMonitorPorts["readMetrics"]>().mockResolvedValue(null),
    observe: vi
      .fn<IngestionMonitorPorts["observe"]>()
      .mockRejectedValue(new Error("write unavailable")),
    pending: vi.fn<IngestionMonitorPorts["pending"]>().mockResolvedValue([notification]),
    enqueue: vi.fn<IngestionMonitorPorts["enqueue"]>().mockResolvedValue(undefined),
    markEnqueued: vi.fn<IngestionMonitorPorts["markEnqueued"]>().mockResolvedValue(undefined),
  } satisfies IngestionMonitorPorts;
  await expect(runIngestionMonitor(ports)).rejects.toThrow("remains unresolved");
  expect(ports.markEnqueued).toHaveBeenCalledOnce();
});
test.each(["failed", "oversized"])(
  "outbox %s never sends unbounded notifications",
  async (mode) => {
    const ports = {
      now: vi.fn(() => 10000000),
      readMetrics: vi.fn<IngestionMonitorPorts["readMetrics"]>().mockResolvedValue(null),
      observe: vi.fn<IngestionMonitorPorts["observe"]>().mockResolvedValue(undefined),
      pending: vi.fn<IngestionMonitorPorts["pending"]>(async () => {
        if (mode === "failed") throw new Error("private provider details");
        return Array.from({ length: 11 }, () => notification);
      }),
      enqueue: vi.fn<IngestionMonitorPorts["enqueue"]>(),
      markEnqueued: vi.fn<IngestionMonitorPorts["markEnqueued"]>(),
    } satisfies IngestionMonitorPorts;
    await expect(runIngestionMonitor(ports)).rejects.toThrow("Ingestion monitor outbox");
    expect(ports.enqueue).not.toHaveBeenCalled();
  },
);
test("stalled metrics reads time out and create an unknown observation", async () => {
  vi.useFakeTimers();
  const ports = {
    now: vi.fn(() => 10000000),
    readMetrics: vi.fn<IngestionMonitorPorts["readMetrics"]>(() => new Promise(() => undefined)),
    observe: vi.fn<IngestionMonitorPorts["observe"]>().mockResolvedValue(undefined),
    pending: vi.fn<IngestionMonitorPorts["pending"]>().mockResolvedValue([]),
    enqueue: vi.fn<IngestionMonitorPorts["enqueue"]>(),
    markEnqueued: vi.fn<IngestionMonitorPorts["markEnqueued"]>(),
  } satisfies IngestionMonitorPorts;
  const result = runIngestionMonitor(ports);
  await vi.advanceTimersByTimeAsync(30000);
  await result;
  expect(ports.observe).toHaveBeenCalledWith(null, 10000000);
  expect(vi.getTimerCount()).toBe(0);
});
test("binding adapter only reads DLQ metrics and disposes the RPC outbox snapshot", async () => {
  const env = mockDeep<CatalogBindings>();
  const journal = mockDeep<DurableObjectStub<IngestionArchiveJournal>>();
  env.INGESTION_ARCHIVE_JOURNAL.getByName.mockReturnValue(journal);
  env.INGESTION_DLQ_METRICS.metrics.mockResolvedValue({ backlogCount: 0, backlogBytes: 0 });
  const dispose = vi.fn();
  journal.pendingNotifications.mockResolvedValue(
    Object.assign([notification], { [Symbol.dispose]: dispose }),
  );
  await monitorIngestionArchives(env);
  expect(dispose).toHaveBeenCalledOnce();
  expect(env.INGESTION_DLQ_METRICS.send).not.toHaveBeenCalled();
  expect(env.INGESTION_ALERTS.send).toHaveBeenCalledWith(notification.message);
  expect(journal.markNotificationEnqueued).toHaveBeenCalledWith(notification.eventId);
});
