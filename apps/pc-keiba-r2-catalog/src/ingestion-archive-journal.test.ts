// Runs with bun via Vitest; SQL/KV and RPC context are mocked here; native SQLite is tested separately.
import { afterEach, expect, test, vi } from "vitest";
import { mockDeep } from "vitest-mock-extended";
import { IngestionArchiveJournal } from "./ingestion-archive-journal";
import { buildIngestionMonitorNotification } from "./ingestion-monitor-alert";
afterEach(() => {
  vi.restoreAllMocks();
});

const id: string = `dlq_${"a".repeat(64)}`;
const attempt = { requestId: id, queuedAt: "2026-09-16T00:00:00.000Z" };
const receipt = {
  source: "sync-realtime-data-hot-v2",
  requestId: id,
  digest: "b".repeat(64),
  sequence: "9007199254740993",
  accepted: true,
};

test.each([undefined, "quiet", "backlog", "unknown"])(
  "bounded read-only monitor status preserves assessment %s without claiming health",
  (assessment) => {
    vi.spyOn(Date, "now").mockReturnValue(10000000);
    const ctx = mockDeep<DurableObjectState>();
    const cursor = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
    ctx.storage.sql.exec.mockReturnValue(cursor);
    cursor.toArray.mockReturnValue([]);
    ctx.storage.transactionSync.mockImplementation((action) => action());
    ctx.storage.kv.get
      .mockReturnValueOnce(9999999)
      .mockReturnValueOnce(
        assessment === undefined ? undefined : { observedAtMs: 9999999, assessment },
      );
    const status = new IngestionArchiveJournal(ctx, {}).monitorStatus();
    expect(status.lastObservedAtMs).toBe(9999999);
    expect(status.sampledAtMs).toBe(10000000);
    expect(status.oldestPendingArchiveAt).toBeNull();
    expect(status.oldestPendingNotificationAtMs).toBeNull();
    expect(status.coverageVerified).toBe(false);
    expect(status.notificationDeliveryVerified).toBe(false);
    expect(status).toMatchObject({ assessment: assessment ?? "unclassified" });
    expect(ctx.storage.kv.put).not.toHaveBeenCalled();
    expect(ctx.storage.sql.exec).toHaveBeenCalledTimes(2);
    expect(ctx.storage.sql.exec).toHaveBeenLastCalledWith(
      "SELECT event_id, payload FROM __archive_alert_outbox_v1 WHERE broker_accepted_at IS NULL ORDER BY sequence LIMIT ?",
      1,
    );
  },
);
test.each([null, -1, "100", Number.NaN, 1.5])(
  "invalid stored watermark fails closed: %s",
  (watermark) => {
    const ctx = mockDeep<DurableObjectState>();
    ctx.storage.transactionSync.mockImplementation((action) => action());
    ctx.storage.kv.get.mockReturnValue(watermark);
    expect(() => new IngestionArchiveJournal(ctx, {}).monitorStatus()).toThrow(
      "Invalid archive monitor watermark",
    );
  },
);
test.each([
  null,
  { observedAtMs: 1, assessment: "quiet" },
  { observedAtMs: 2, assessment: "healthy" },
])("corrupt or mismatched assessments fail closed", (assessment) => {
  const ctx = mockDeep<DurableObjectState>();
  ctx.storage.transactionSync.mockImplementation((action) => action());
  ctx.storage.kv.get.mockReturnValueOnce(2).mockReturnValueOnce(assessment);
  expect(() => new IngestionArchiveJournal(ctx, {}).monitorStatus()).toThrow(
    "Invalid archive monitor assessment",
  );
});
test("assessment without an observation watermark is not accepted", () => {
  const ctx = mockDeep<DurableObjectState>();
  ctx.storage.transactionSync.mockImplementation((action) => action());
  ctx.storage.kv.get
    .mockReturnValueOnce(undefined)
    .mockReturnValueOnce({ observedAtMs: null, assessment: "quiet" });
  expect(() => new IngestionArchiveJournal(ctx, {}).monitorStatus()).toThrow(
    "Invalid archive monitor assessment",
  );
});

test("monitor status includes oldest known pending records but does not write", () => {
  vi.spyOn(Date, "now").mockReturnValue(10000000);
  const ctx = mockDeep<DurableObjectState>();
  const cursor = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
  ctx.storage.sql.exec.mockReturnValue(cursor);
  ctx.storage.transactionSync.mockImplementation((action) => action());
  const notification = buildIngestionMonitorNotification({
    metrics: null,
    pending: [],
    observedAtMs: 10000000,
    nowMs: 10000000,
  });
  if (notification === null) throw new Error("Missing notification");
  cursor.toArray
    .mockReturnValueOnce([{ request_id: id, queued_at: "2026-09-16T00:00:00.000Z" }])
    .mockReturnValueOnce([
      { event_id: notification.eventId, payload: JSON.stringify(notification) },
    ]);
  const status = new IngestionArchiveJournal(ctx, {}).monitorStatus();
  expect(status).toStrictEqual({
    sampledAtMs: 10000000,
    lastObservedAtMs: null,
    assessment: "unclassified",
    oldestPendingArchiveAt: "2026-09-16T00:00:00.000Z",
    oldestPendingNotificationAtMs: 10000000,
    coverageVerified: false,
    notificationDeliveryVerified: false,
  });
  expect(ctx.storage.kv.put).not.toHaveBeenCalled();
});

test("observation and first coalesced alert are recorded transactionally", () => {
  vi.spyOn(Date, "now").mockReturnValue(10000000);
  const ctx = mockDeep<DurableObjectState>();
  const cursor = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
  ctx.storage.sql.exec.mockReturnValue(cursor);
  ctx.storage.transactionSync.mockImplementation((action) => action());
  Object.defineProperty(ctx.storage.sql, "databaseSize", { value: 4096 });
  cursor.toArray.mockReturnValue([]);
  const journal = new IngestionArchiveJournal(ctx, {});
  journal.observe(null, 10000000);
  expect(ctx.storage.kv.put).toHaveBeenCalledWith("archive-monitor/last-observed-v1", 10000000);
  expect(ctx.storage.kv.put).toHaveBeenCalledWith("archive-monitor/last-assessment-v1", {
    observedAtMs: 10000000,
    assessment: "unknown",
  });
  expect(ctx.storage.sql.exec).toHaveBeenLastCalledWith(
    "INSERT INTO __archive_alert_outbox_v1 (event_id, payload) VALUES (?, ?)",
    expect.stringMatching(/^[a-f0-9]{64}$/u),
    expect.any(String),
  );
  cursor.toArray
    .mockReturnValueOnce([])
    .mockReturnValueOnce([{ event_id: "existing", payload: "first immutable notification" }]);
  ctx.storage.sql.exec.mockClear();
  journal.observe(null, 10000000);
  expect(ctx.storage.sql.exec).toHaveBeenCalledTimes(2);
});
test("quiet samples do not enqueue recovery and older samples do not replace newer observations", () => {
  vi.spyOn(Date, "now").mockReturnValue(10000000);
  const ctx = mockDeep<DurableObjectState>();
  const cursor = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
  ctx.storage.sql.exec.mockReturnValue(cursor);
  cursor.toArray.mockReturnValue([]);
  ctx.storage.transactionSync.mockImplementation((action) => action());
  const journal = new IngestionArchiveJournal(ctx, {});
  journal.observe({ backlog_count: 0, backlog_bytes: 0, oldest_message_timestamp_ms: 0 }, 10000000);
  expect(ctx.storage.sql.exec).toHaveBeenCalledTimes(1);
  expect(ctx.storage.kv.put).toHaveBeenCalledWith("archive-monitor/last-assessment-v1", {
    observedAtMs: 10000000,
    assessment: "quiet",
  });
  ctx.storage.kv.get.mockReturnValue(10000000);
  ctx.storage.sql.exec.mockClear();
  journal.observe(null, 9999999);
  expect(ctx.storage.sql.exec).not.toHaveBeenCalled();
});
test("assessable backlog is recorded as backlog rather than a healthy observation", () => {
  vi.spyOn(Date, "now").mockReturnValue(10000000);
  const ctx = mockDeep<DurableObjectState>();
  const cursor = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
  ctx.storage.sql.exec.mockReturnValue(cursor);
  cursor.toArray.mockReturnValue([]);
  ctx.storage.transactionSync.mockImplementation((action) => action());
  Object.defineProperty(ctx.storage.sql, "databaseSize", { value: 4096 });
  new IngestionArchiveJournal(ctx, {}).observe(
    { backlog_count: 1, backlog_bytes: 10, oldest_message_timestamp_ms: 1000000 },
    10000000,
  );
  expect(ctx.storage.kv.put).toHaveBeenCalledWith("archive-monitor/last-assessment-v1", {
    observedAtMs: 10000000,
    assessment: "backlog",
  });
});

test("future samples cannot advance the watermark and new alerts obey the storage budget", () => {
  vi.spyOn(Date, "now").mockReturnValue(10000000);
  const ctx = mockDeep<DurableObjectState>();
  const cursor = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
  ctx.storage.sql.exec.mockReturnValue(cursor);
  cursor.toArray.mockReturnValue([]);
  ctx.storage.transactionSync.mockImplementation((action) => action());
  Object.defineProperty(ctx.storage.sql, "databaseSize", { value: 8000000000 });
  const journal = new IngestionArchiveJournal(ctx, {});
  expect(() => journal.observe(null, 10000001)).toThrow("notification storage budget");
  expect(ctx.storage.kv.put).not.toHaveBeenCalled();
});
test("outbox decoding checks identities and marks broker acceptance without deleting messages", () => {
  const ctx = mockDeep<DurableObjectState>();
  const cursor = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
  ctx.storage.sql.exec.mockReturnValue(cursor);
  const notification = buildIngestionMonitorNotification({
    metrics: null,
    pending: [],
    observedAtMs: 10000000,
    nowMs: 10000000,
  });
  if (notification === null) throw new Error("Missing test notification");
  cursor.toArray.mockReturnValue([
    { event_id: notification.eventId, payload: JSON.stringify(notification) },
  ]);
  const journal = new IngestionArchiveJournal(ctx, {});
  expect(journal.pendingNotifications(10)).toMatchObject([{ message: { severity: "critical" } }]);
  journal.markNotificationEnqueued(notification.eventId);
  expect(ctx.storage.sql.exec).toHaveBeenLastCalledWith(
    "UPDATE __archive_alert_outbox_v1 SET broker_accepted_at = COALESCE(broker_accepted_at, ?) WHERE event_id = ? RETURNING event_id",
    expect.any(Number),
    notification.eventId,
  );
  cursor.toArray.mockReturnValue([{ event_id: "mismatch", payload: JSON.stringify(notification) }]);
  expect(() => journal.pendingNotifications(1)).toThrow("identity mismatch");
  cursor.toArray.mockReturnValue([]);
  expect(() => journal.markNotificationEnqueued(notification.eventId)).toThrow("not found");
});
test.each([null, -1, 1.5])("rejects invalid observation timestamps", (time) => {
  expect(() =>
    new IngestionArchiveJournal(mockDeep<DurableObjectState>(), {}).observe(null, time),
  ).toThrow("observation time");
});
test.each([null, 0, 11, 1.5])("rejects unsafe outbox read limits", (limit) => {
  expect(() =>
    new IngestionArchiveJournal(mockDeep<DurableObjectState>(), {}).pendingNotifications(limit),
  ).toThrow("notification limit");
});
test.each([null, "bad", "x".repeat(64), `${"a".repeat(64)}\n`])(
  "rejects invalid broker acceptance identifiers",
  (value) => {
    expect(() =>
      new IngestionArchiveJournal(mockDeep<DurableObjectState>(), {}).markNotificationEnqueued(
        value,
      ),
    ).toThrow("notification identity");
  },
);
test("rejects trailing-newline identities and noncanonical receipt sequences", () => {
  const journal = new IngestionArchiveJournal(mockDeep<DurableObjectState>(), {});
  expect(() => journal.begin({ ...attempt, requestId: `${id}\n` })).toThrow(
    "Invalid archive attempt",
  );
  expect(() => journal.recordReceipt({ ...receipt, requestId: `${id}\n` })).toThrow(
    "Invalid archive receipt",
  );
  expect(() => journal.recordReceipt({ ...receipt, digest: `${"b".repeat(64)}\n` })).toThrow(
    "Invalid archive receipt",
  );
  expect(() => journal.recordReceipt({ ...receipt, sequence: "1\n" })).toThrow(
    "Invalid archive receipt",
  );
});

test("idempotent begin/completion preserve identity and reject conflicts", () => {
  const ctx = mockDeep<DurableObjectState>();
  const cursor = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
  ctx.storage.sql.exec.mockReturnValue(cursor);
  ctx.storage.transactionSync.mockImplementation((action) => action());
  ctx.blockConcurrencyWhile.mockImplementation(async (action) => action());
  Object.defineProperty(ctx.storage.sql, "databaseSize", { value: 4096 });
  cursor.toArray.mockReturnValue([]);
  const journal = new IngestionArchiveJournal(ctx, {});
  expect("lookup" in journal).toBe(false);
  journal.begin(attempt);
  expect(ctx.storage.sql.exec).toHaveBeenLastCalledWith(
    "INSERT INTO __archive_attempts_v1 (request_id, queued_at) VALUES (?, ?)",
    id,
    "2026-09-16T00:00:00.000Z",
  );
  cursor.toArray.mockReturnValue([{ request_id: id, queued_at: attempt.queuedAt, receipt: null }]);
  journal.begin(attempt);
  expect(() => journal.begin({ ...attempt, queuedAt: "2026-09-17T00:00:00.000Z" })).toThrow(
    "identity conflict",
  );
  journal.recordReceipt(receipt);
  expect(ctx.storage.sql.exec).toHaveBeenLastCalledWith(
    "UPDATE __archive_attempts_v1 SET receipt = ? WHERE request_id = ? AND receipt IS NULL",
    JSON.stringify(receipt),
    id,
  );
  cursor.toArray.mockReturnValue([
    { request_id: id, queued_at: attempt.queuedAt, receipt: JSON.stringify(receipt) },
  ]);
  journal.recordReceipt(receipt);
  expect(() => journal.recordReceipt({ ...receipt, sequence: "2" })).toThrow("receipt conflict");
  cursor.toArray.mockReturnValue([]);
  expect(() => journal.recordReceipt(receipt)).toThrow("not found");
});
test("budget protects new entries but permits existing attempts", () => {
  const ctx = mockDeep<DurableObjectState>();
  const cursor = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
  ctx.storage.sql.exec.mockReturnValue(cursor);
  ctx.storage.transactionSync.mockImplementation((action) => action());
  Object.defineProperty(ctx.storage.sql, "databaseSize", { value: 8000000000 });
  cursor.toArray.mockReturnValue([]);
  const journal = new IngestionArchiveJournal(ctx, {});
  expect(() => journal.begin(attempt)).toThrow("storage budget");
  cursor.toArray.mockReturnValue([{ request_id: id, queued_at: attempt.queuedAt, receipt: null }]);
  expect(() => journal.begin(attempt)).not.toThrow();
});
test("bounded pending observations never certify coverage", () => {
  const ctx = mockDeep<DurableObjectState>();
  const cursor = mockDeep<SqlStorageCursor<Record<string, SqlStorageValue>>>();
  ctx.storage.sql.exec.mockReturnValue(cursor);
  cursor.toArray.mockReturnValue([{ request_id: id, queued_at: attempt.queuedAt }]);
  const journal = new IngestionArchiveJournal(ctx, {});
  expect(journal.pending(1)).toMatchObject({
    entries: [{ queuedAt: "2026-09-16T00:00:00.000Z" }],
    coverageVerified: false,
  });
  cursor.toArray.mockReturnValue([]);
  expect(journal.pending(100)).toStrictEqual({ entries: [], coverageVerified: false });
});
test.each([
  null,
  [],
  {},
  { ...attempt, requestId: 1 },
  { ...attempt, requestId: "invalid" },
  { ...attempt, requestId: `dlq_${"x".repeat(64)}` },
  { ...attempt, queuedAt: 1 },
  { ...attempt, queuedAt: "bad" },
  { ...attempt, queuedAt: "2026-99-99T00:00:00.000Z" },
  { ...attempt, queuedAt: "2026-02-30T00:00:00.000Z" },
])("rejects invalid identity", (value) => {
  expect(() =>
    new IngestionArchiveJournal(mockDeep<DurableObjectState>(), {}).begin(value),
  ).toThrow("Invalid archive attempt");
});
test.each([
  null,
  { ...receipt, source: "other" },
  { ...receipt, requestId: 1 },
  { ...receipt, requestId: "other" },
  { ...receipt, requestId: `dlq_${"x".repeat(64)}` },
  { ...receipt, digest: 1 },
  { ...receipt, digest: "bad" },
  { ...receipt, digest: "x".repeat(64) },
  { ...receipt, sequence: 1 },
  { ...receipt, sequence: "01" },
  { ...receipt, sequence: "9223372036854775808" },
  { ...receipt, accepted: false },
])("rejects invalid receipt", (value) => {
  expect(() =>
    new IngestionArchiveJournal(mockDeep<DurableObjectState>(), {}).recordReceipt(value),
  ).toThrow("Invalid archive receipt");
});
test.each([null, "1", 0, 101, 1.5])("rejects invalid pending limit", (limit) => {
  expect(() =>
    new IngestionArchiveJournal(mockDeep<DurableObjectState>(), {}).pending(limit),
  ).toThrow("Invalid archive pending limit");
});
