// Runs with bun via Vitest; real SQLite transactions, RPC acknowledgement and restart persistence.
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { afterAll, beforeAll, expect, test, vi } from "vitest";
import { prepareIngestionEnvelope } from "./ingestion-buffer";
import { parseIngestionMonitorNotification } from "./ingestion-monitor-alert";

interface InboxRequest {
  scope: string;
  operation: string;
  args: unknown[];
}
const runtime: { worker: Miniflare | null; directory: string | null; script: string } = {
  worker: null,
  directory: null,
  script: "",
};
const instance = (): Miniflare => {
  if (runtime.worker === null) throw new Error("Inbox worker is not started");
  return runtime.worker;
};
const createWorker = (): Miniflare => {
  if (runtime.directory === null) throw new Error("Missing inbox persistence directory");
  return new Miniflare({
    name: "inbox-native",
    modules: true,
    script: runtime.script,
    compatibilityDate: "2026-06-01",
    compatibilityFlags: ["nodejs_compat"],
    durableObjects: {
      ARCHIVE_JOURNAL: { className: "TestArchiveJournal", useSQLite: true },
      INGESTION_ARCHIVE_JOURNAL: { className: "TestArchiveJournal", useSQLite: true },
      INBOX: { className: "TestInbox", useSQLite: true },
      INGESTION_INBOX: { className: "TestInbox", useSQLite: true },
    },
    bindings: { ADMIN_TOKEN: "native-status-secret" },
    serviceBindings: { BUFFER: { name: "inbox-native", entrypoint: "IngestionBufferService" } },
    queueProducers: {
      DLQ_PRODUCER: "sync-realtime-data-hot-ingestion-dlq",
      INGESTION_DLQ_METRICS: "sync-realtime-data-hot-ingestion-dlq",
      INGESTION_ALERTS: "native-monitor-alerts",
    },
    queueConsumers: {
      "sync-realtime-data-hot-ingestion-dlq": { maxBatchSize: 1, maxBatchTimeout: 0 },
      "native-monitor-alerts": { maxBatchSize: 1, maxBatchTimeout: 0 },
    },
    r2Buckets: ["CATALOG_OBJECTS"],
    r2Persist: join(runtime.directory, "r2"),
    durableObjectsPersist: runtime.directory,
  });
};
const call = async (input: InboxRequest): Promise<unknown> => {
  const response = await instance().dispatchFetch("https://inbox.test", {
    method: "POST",
    body: JSON.stringify(input),
  });
  if (!response.ok) throw new Error(await response.text());
  return await response.json();
};

beforeAll(async () => {
  const entry = new URL("./ingestion-inbox.ts", import.meta.url).pathname;
  const serviceEntry = new URL("./ingestion-service.ts", import.meta.url).pathname;
  const deadLetterEntry = new URL("./ingestion-dead-letter.ts", import.meta.url).pathname;
  const journalEntry = new URL("./ingestion-archive-journal.ts", import.meta.url).pathname;
  const attemptEntry = new URL("./ingestion-archive-attempt.ts", import.meta.url).pathname;
  const monitorEntry = new URL("./ingestion-monitor.ts", import.meta.url).pathname;
  const gatewayEntry = new URL("./vector-gateway.ts", import.meta.url).pathname;
  const bundle = await build({
    stdin: {
      contents: `import { IngestionInbox } from ${JSON.stringify(entry)};
export { IngestionBufferService } from ${JSON.stringify(serviceEntry)};
import { acceptIngestion } from ${JSON.stringify(serviceEntry)};
import { retainTrackedDeadLetter } from ${JSON.stringify(attemptEntry)};
import { monitorIngestionArchives } from ${JSON.stringify(monitorEntry)};
import gateway from ${JSON.stringify(gatewayEntry)};
import { handleIngestionDeadLetters } from ${JSON.stringify(deadLetterEntry)};
import { IngestionArchiveJournal } from ${JSON.stringify(journalEntry)};
export class TestArchiveJournal extends IngestionArchiveJournal {
  rejectAlertInsert() { this.ctx.storage.sql.exec("CREATE TRIGGER reject_alert BEFORE INSERT ON __archive_alert_outbox_v1 BEGIN SELECT RAISE(ABORT, 'alert fixture rejection'); END"); }
  lastObserved() { return this.ctx.storage.kv.get('archive-monitor/last-observed-v1'); }
  archivedCount() { return this.ctx.storage.sql.exec("SELECT COUNT(*) AS count FROM __archive_attempts_v1 WHERE receipt IS NOT NULL").one().count; }
  rejectReceipt() { this.ctx.storage.sql.exec("CREATE TRIGGER reject_receipt BEFORE UPDATE ON __archive_attempts_v1 BEGIN SELECT RAISE(ABORT, 'receipt fixture rejection'); END"); }
}
export class TestInbox extends IngestionInbox {
  armReject() { this.ctx.storage.sql.exec("CREATE TRIGGER test_reject BEFORE INSERT ON __ingestion_inbox_v1 WHEN NEW.request_id = 'reject' BEGIN SELECT RAISE(ABORT, 'fixture rejection'); END"); return true; }
  seedLarge(pointer) { this.accept(pointer); this.ctx.storage.sql.exec("UPDATE __ingestion_inbox_v1 SET sequence = 9007199254740993 WHERE request_id = ?", pointer.requestId); return true; }
}
export default { async queue(batch, env) {
  if (batch.queue === 'native-monitor-alerts') {
    await Promise.all(batch.messages.map(message => env.CATALOG_OBJECTS.put('test-only/alerts/' + message.id, JSON.stringify(message.body))));
    return;
  }
  await handleIngestionDeadLetters(batch, env);
}, async fetch(request, env) {
  if (new URL(request.url).pathname === '/v1/internal/ingestion/status') return await gateway.fetch(request, env);
  try {
    const input = await request.json();
    if (input.operation === 'monitorTick') { await monitorIngestionArchives(env); return Response.json({ complete: true }); }
    if (input.operation === 'trackedArchive') {
      const message = input.args[0];
      const journal = env.ARCHIVE_JOURNAL.getByName(input.scope);
      return Response.json(await retainTrackedDeadLetter({ id: message.id, timestamp: new Date(message.timestamp), body: message.body }, {
        begin: async identity => { await journal.begin(identity); },
        accept: async envelope => await acceptIngestion(envelope, env),
        recordReceipt: async receipt => { await journal.recordReceipt(receipt); },
      }));
    }
    if (['begin', 'recordReceipt', 'pending', 'rejectReceipt', 'archivedCount', 'observe', 'pendingNotifications', 'markNotificationEnqueued', 'rejectAlertInsert', 'lastObserved', 'monitorStatus'].includes(input.operation)) {
      const result = await env.ARCHIVE_JOURNAL.getByName(input.scope)[input.operation](...input.args);
      return Response.json(result === undefined ? null : result);
    }
    if (input.operation === 'stage') return Response.json(await env.BUFFER.accept(input.args[0]));
    if (!['accept', 'entries', 'armReject', 'seedLarge'].includes(input.operation)) return new Response(null, {status:400});
    return Response.json(await env.INBOX.getByName(input.scope)[input.operation](...input.args));
  } catch(error) { return Response.json({error:String(error)}, {status:409}); }
} };`,
      loader: "ts",
      resolveDir: process.cwd(),
    },
    bundle: true,
    format: "esm",
    platform: "neutral",
    conditions: ["workerd", "worker", "browser"],
    mainFields: ["browser", "module", "main"],
    write: false,
    external: ["cloudflare:workers", "node:*"],
    logLevel: "silent",
  });
  const output = bundle.outputFiles[0];
  if (output === undefined) throw new Error("Missing inbox bundle");
  runtime.script = output.text;
  runtime.directory = await mkdtemp(join(tmpdir(), "ingestion-inbox-workerd-"));
  runtime.worker = createWorker();
  await instance().ready;
}, 20000);

afterAll(async () => {
  await runtime.worker?.dispose();
  if (runtime.directory !== null) await rm(runtime.directory, { recursive: true, force: true });
});

test("native authenticated HTTP diagnostics reach the SQLite journal without observing or enqueuing", async () => {
  const unauthorized = await instance().dispatchFetch(
    "https://inbox.test/v1/internal/ingestion/status",
  );
  expect(unauthorized.status).toBe(401);
  expect(unauthorized.headers.get("cache-control")).toBe("no-store");
  const wrongMethod = await instance().dispatchFetch(
    "https://inbox.test/v1/internal/ingestion/status",
    { method: "POST", headers: { Authorization: "Bearer native-status-secret" } },
  );
  expect(wrongMethod.status).toBe(405);
  const response = await instance().dispatchFetch(
    "https://inbox.test/v1/internal/ingestion/status",
    { headers: { Authorization: "Bearer native-status-secret" } },
  );
  expect(response.status).toBe(200);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(await response.json()).toMatchObject({
    lastObservedAtMs: null,
    assessment: "unclassified",
    oldestPendingArchiveAt: null,
    oldestPendingNotificationAtMs: null,
    coverageVerified: false,
    notificationDeliveryVerified: false,
  });
  expect(
    await call({
      scope: "sync-realtime-data-hot-ingestion-dlq",
      operation: "lastObserved",
      args: [],
    }),
  ).toBeNull();
  expect(
    await call({
      scope: "sync-realtime-data-hot-ingestion-dlq",
      operation: "pendingNotifications",
      args: [1],
    }),
  ).toStrictEqual([]);
});

test("concurrent retries yield one receipt, content conflicts and wrong-source calls fail closed", async () => {
  const pointer = prepareIngestionEnvelope({
    source: "hot",
    requestId: "message",
    payload: "{}",
  }).pointer;
  const request: InboxRequest = { scope: "concurrent", operation: "accept", args: [pointer] };
  expect(await Promise.all([call(request), call(request)])).toMatchObject([
    { sequence: "1", accepted: true },
    { sequence: "1", accepted: true },
  ]);
  expect(await call({ scope: "concurrent", operation: "entries", args: ["0", 100] })).toHaveLength(
    1,
  );
  await expect(
    call({
      ...request,
      args: [
        prepareIngestionEnvelope({ source: "hot", requestId: "message", payload: "[]" }).pointer,
      ],
    }),
  ).rejects.toThrow("reused with different content");
  await expect(
    call({
      ...request,
      args: [
        prepareIngestionEnvelope({ source: "other", requestId: "another", payload: "{}" }).pointer,
      ],
    }),
  ).rejects.toThrow("source mismatch");
});

test("failed insert rolls back initial source binding and does not consume a sequence", async () => {
  await call({ scope: "rollback", operation: "armReject", args: [] });
  await expect(
    call({
      scope: "rollback",
      operation: "accept",
      args: [
        prepareIngestionEnvelope({ source: "first", requestId: "reject", payload: "{}" }).pointer,
      ],
    }),
  ).rejects.toThrow("fixture rejection");
  expect(await call({ scope: "rollback", operation: "entries", args: ["0", 10] })).toStrictEqual(
    [],
  );
  expect(
    await call({
      scope: "rollback",
      operation: "accept",
      args: [
        prepareIngestionEnvelope({ source: "second", requestId: "ok", payload: "{}" }).pointer,
      ],
    }),
  ).toMatchObject({ source: "second", sequence: "1", accepted: true });
});

test("named Worker RPC retains R2 input and deduplicates through the real SQLite inbox", async () => {
  const input = {
    source: "sync-realtime-data-hot-v2",
    requestId: "service-message",
    payload: '{"rowid":9007199254740993}',
  };
  const request: InboxRequest = { scope: "unused", operation: "stage", args: [input] };
  expect(await call(request)).toMatchObject({
    source: "sync-realtime-data-hot-v2",
    requestId: "service-message",
    sequence: "1",
    accepted: true,
  });
  expect(await call(request)).toMatchObject({ sequence: "1", accepted: true });
  expect(
    await call({ scope: "sync-realtime-data-hot-v2", operation: "entries", args: ["0", 100] }),
  ).toHaveLength(1);
  const bucket = await instance().getR2Bucket("CATALOG_OBJECTS");
  expect(
    (await bucket.list({ prefix: "ingestion/inbox/v1/sync-realtime-data-hot-v2/service-message/" }))
      .objects,
  ).toHaveLength(1);
  await expect(call({ ...request, args: [{ ...input, payload: "[]" }] })).rejects.toThrow(
    "reused with different content",
  );
});

test("native Queue delivery archives input through R2 and SQLite without executing its job", async () => {
  const producer = await instance().getQueueProducer("DLQ_PRODUCER");
  await producer.send({ type: "fetch-odds", raceKey: "native-only-never-executed" });
  await vi.waitFor(
    async () => {
      expect(
        await call({ scope: "sync-realtime-data-hot-v2", operation: "entries", args: ["1", 100] }),
      ).toMatchObject([
        {
          sequence: "2",
          pointer: {
            source: "sync-realtime-data-hot-v2",
            requestId: expect.stringMatching(/^dlq_[a-f0-9]{64}$/u),
          },
        },
      ]);
    },
    { timeout: 5000, interval: 20 },
  );
  const bucket = await instance().getR2Bucket("CATALOG_OBJECTS");
  expect(
    (await bucket.list({ prefix: "ingestion/inbox/v1/sync-realtime-data-hot-v2/dlq_" })).objects,
  ).toHaveLength(1);
  await vi.waitFor(
    async () => {
      expect(
        await call({
          scope: "sync-realtime-data-hot-ingestion-dlq",
          operation: "archivedCount",
          args: [],
        }),
      ).toBe(1);
    },
    { timeout: 5000, interval: 20 },
  );
  expect(
    await call({
      scope: "sync-realtime-data-hot-ingestion-dlq",
      operation: "pending",
      args: [100],
    }),
  ).toStrictEqual({ entries: [], coverageVerified: false });
});

test("native tracked retention commits R2 and inbox before clearing pending; outcome failure stays pending", async () => {
  const args = [
    {
      id: "tracked-success",
      timestamp: "2026-09-16T00:00:00.000Z",
      body: { type: "fetch-odds", raceKey: "local-only" },
    },
  ];
  const receipts = await Promise.all([
    call({ scope: "tracked", operation: "trackedArchive", args }),
    call({ scope: "tracked", operation: "trackedArchive", args }),
  ]);
  expect(receipts).toMatchObject([
    { accepted: true, sequence: "3" },
    { accepted: true, sequence: "3" },
  ]);
  expect(await call({ scope: "tracked", operation: "pending", args: [100] })).toStrictEqual({
    entries: [],
    coverageVerified: false,
  });
  await call({ scope: "tracked-failed", operation: "rejectReceipt", args: [] });
  await expect(
    call({
      scope: "tracked-failed",
      operation: "trackedArchive",
      args: [
        {
          id: "tracked-failed",
          timestamp: "2026-09-16T00:00:00.000Z",
          body: { type: "fetch-odds", raceKey: "local-only" },
        },
      ],
    }),
  ).rejects.toThrow("receipt fixture rejection");
  expect(await call({ scope: "tracked-failed", operation: "pending", args: [1] })).toMatchObject({
    entries: [{ queuedAt: "2026-09-16T00:00:00.000Z" }],
    coverageVerified: false,
  });
  expect(
    await call({ scope: "sync-realtime-data-hot-v2", operation: "entries", args: ["3", 100] }),
  ).toMatchObject([{ sequence: "4" }]);
  const bucket = await instance().getR2Bucket("CATALOG_OBJECTS");
  expect(
    (await bucket.list({ prefix: "ingestion/inbox/v1/sync-realtime-data-hot-v2/dlq_" })).objects,
  ).toHaveLength(3);
});

test("native archival journal replays concurrent begin/receipts and retains failed completion as pending", async () => {
  const identity = { requestId: `dlq_${"a".repeat(64)}`, queuedAt: "2026-09-16T00:00:00.000Z" };
  await Promise.all([
    call({ scope: "journal", operation: "begin", args: [identity] }),
    call({ scope: "journal", operation: "begin", args: [identity] }),
  ]);
  expect(await call({ scope: "journal", operation: "pending", args: [100] })).toMatchObject({
    entries: [{ queuedAt: "2026-09-16T00:00:00.000Z" }],
    coverageVerified: false,
  });
  await expect(
    call({
      scope: "journal",
      operation: "begin",
      args: [{ ...identity, queuedAt: "2026-09-17T00:00:00.000Z" }],
    }),
  ).rejects.toThrow("identity conflict");
  const receipt = {
    source: "sync-realtime-data-hot-v2",
    requestId: identity.requestId,
    digest: "b".repeat(64),
    sequence: "9007199254740993",
    accepted: true,
  };
  await Promise.all([
    call({ scope: "journal", operation: "recordReceipt", args: [receipt] }),
    call({ scope: "journal", operation: "recordReceipt", args: [receipt] }),
  ]);
  expect(await call({ scope: "journal", operation: "pending", args: [100] })).toStrictEqual({
    entries: [],
    coverageVerified: false,
  });
  await expect(
    call({ scope: "journal", operation: "recordReceipt", args: [{ ...receipt, sequence: "2" }] }),
  ).rejects.toThrow("receipt conflict");
  await call({ scope: "journal-reject", operation: "begin", args: [identity] });
  await call({ scope: "journal-reject", operation: "rejectReceipt", args: [] });
  await expect(
    call({ scope: "journal-reject", operation: "recordReceipt", args: [receipt] }),
  ).rejects.toThrow("receipt fixture rejection");
  expect(await call({ scope: "journal-reject", operation: "pending", args: [1] })).toMatchObject({
    entries: [{ queuedAt: "2026-09-16T00:00:00.000Z" }],
    coverageVerified: false,
  });
});

test("native alert outbox coalesces, retains pending alerts through quiet observations, and rolls back failed inserts", async () => {
  const observedAt: number = Date.now();
  await Promise.all([
    call({ scope: "monitor-outbox", operation: "observe", args: [null, observedAt] }),
    call({ scope: "monitor-outbox", operation: "observe", args: [null, observedAt] }),
  ]);
  const notifications: unknown = await call({
    scope: "monitor-outbox",
    operation: "pendingNotifications",
    args: [10],
  });
  expect(notifications).toMatchObject([{ message: { severity: "critical" } }]);
  expect(
    await call({ scope: "monitor-outbox", operation: "monitorStatus", args: [] }),
  ).toMatchObject({
    assessment: "unknown",
    coverageVerified: false,
    notificationDeliveryVerified: false,
  });
  if (!Array.isArray(notifications) || notifications.length !== 1)
    throw new Error("Expected one coalesced alert");
  const notification = parseIngestionMonitorNotification(notifications[0]);
  await call({
    scope: "monitor-outbox",
    operation: "observe",
    args: [{ backlog_count: 0, backlog_bytes: 0, oldest_message_timestamp_ms: 0 }, Date.now()],
  });
  expect(
    await call({ scope: "monitor-outbox", operation: "pendingNotifications", args: [10] }),
  ).toHaveLength(1);
  await Promise.all([
    call({
      scope: "monitor-outbox",
      operation: "markNotificationEnqueued",
      args: [notification.eventId],
    }),
    call({
      scope: "monitor-outbox",
      operation: "markNotificationEnqueued",
      args: [notification.eventId],
    }),
  ]);
  expect(
    await call({ scope: "monitor-outbox", operation: "pendingNotifications", args: [10] }),
  ).toStrictEqual([]);
  await call({ scope: "monitor-reject", operation: "rejectAlertInsert", args: [] });
  await expect(
    call({ scope: "monitor-reject", operation: "observe", args: [null, Date.now()] }),
  ).rejects.toThrow("alert fixture rejection");
  expect(await call({ scope: "monitor-reject", operation: "lastObserved", args: [] })).toBe(null);
  expect(
    await call({ scope: "monitor-reject", operation: "monitorStatus", args: [] }),
  ).toMatchObject({
    lastObservedAtMs: null,
    assessment: "unclassified",
    oldestPendingArchiveAt: null,
    oldestPendingNotificationAtMs: null,
    coverageVerified: false,
    notificationDeliveryVerified: false,
  });
  expect(
    await call({ scope: "monitor-reject", operation: "pendingNotifications", args: [10] }),
  ).toStrictEqual([]);
});

test("native diagnostic status retains quiet assessment without asserting notification delivery", async () => {
  expect(
    await call({ scope: "monitor-outbox", operation: "monitorStatus", args: [] }),
  ).toMatchObject({
    assessment: "quiet",
    oldestPendingNotificationAtMs: null,
    coverageVerified: false,
    notificationDeliveryVerified: false,
  });
});

test("native monitor reads real Queue gauges and drains retained alerts to an isolated local Queue", async () => {
  await call({
    scope: "sync-realtime-data-hot-ingestion-dlq",
    operation: "begin",
    args: [{ requestId: `dlq_${"c".repeat(64)}`, queuedAt: "2000-01-01T00:00:00.000Z" }],
  });
  expect(await call({ scope: "unused", operation: "monitorTick", args: [] })).toStrictEqual({
    complete: true,
  });
  const bucket = await instance().getR2Bucket("CATALOG_OBJECTS");
  await vi.waitFor(
    async () => {
      expect((await bucket.list({ prefix: "test-only/alerts/" })).objects).toHaveLength(1);
    },
    { timeout: 5000, interval: 20 },
  );
  const key: string | undefined = (await bucket.list({ prefix: "test-only/alerts/" })).objects[0]
    ?.key;
  if (key === undefined) throw new Error("Missing local notification");
  const retained = await bucket.get(key);
  expect(await retained?.json()).toMatchObject({
    checkName: "hot-ingestion-archive:backlog",
    severity: "critical",
  });
  expect(
    await call({
      scope: "sync-realtime-data-hot-ingestion-dlq",
      operation: "pendingNotifications",
      args: [10],
    }),
  ).toStrictEqual([]);
  expect(
    await call({
      scope: "sync-realtime-data-hot-ingestion-dlq",
      operation: "pending",
      args: [100],
    }),
  ).toMatchObject({ entries: [{ queuedAt: "2000-01-01T00:00:00.000Z" }], coverageVerified: false });
});

test("native int64 receipt and page cursors survive a cold worker restart", async () => {
  const pointer = prepareIngestionEnvelope({
    source: "hot",
    requestId: "large",
    payload: "{}",
  }).pointer;
  await call({ scope: "persistent", operation: "seedLarge", args: [pointer] });
  expect(
    await call({ scope: "persistent", operation: "entries", args: ["9007199254740992", 1] }),
  ).toMatchObject([{ sequence: "9007199254740993", pointer: { requestId: "large" } }]);
  expect(
    await call({ scope: "persistent", operation: "entries", args: ["9007199254740993", 1] }),
  ).toStrictEqual([]);
  await instance().dispose();
  runtime.worker = createWorker();
  await instance().ready;
  expect(
    await call({ scope: "monitor-outbox", operation: "monitorStatus", args: [] }),
  ).toMatchObject({
    assessment: "quiet",
    oldestPendingNotificationAtMs: null,
    coverageVerified: false,
    notificationDeliveryVerified: false,
  });
  expect(
    await call({
      scope: "sync-realtime-data-hot-ingestion-dlq",
      operation: "monitorStatus",
      args: [],
    }),
  ).toMatchObject({
    assessment: "backlog",
    oldestPendingArchiveAt: "2000-01-01T00:00:00.000Z",
    coverageVerified: false,
    notificationDeliveryVerified: false,
  });
  expect(await call({ scope: "persistent", operation: "accept", args: [pointer] })).toMatchObject({
    sequence: "9007199254740993",
    accepted: true,
  });
  expect(
    await call({
      scope: "persistent",
      operation: "accept",
      args: [prepareIngestionEnvelope({ source: "hot", requestId: "next", payload: "{}" }).pointer],
    }),
  ).toMatchObject({ sequence: "9007199254740994", accepted: true });
  expect(
    await call({ scope: "monitor-outbox", operation: "pendingNotifications", args: [10] }),
  ).toStrictEqual([]);
  expect(await call({ scope: "journal", operation: "pending", args: [100] })).toStrictEqual({
    entries: [],
    coverageVerified: false,
  });
  expect(await call({ scope: "journal-reject", operation: "pending", args: [1] })).toMatchObject({
    entries: [{ queuedAt: "2026-09-16T00:00:00.000Z" }],
    coverageVerified: false,
  });
});
