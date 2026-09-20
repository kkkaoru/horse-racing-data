// Runs with bun via Vitest; Queue acknowledgements, storage/RPC and logs are mocked.
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { mockDeep } from "vitest-mock-extended";
import type { IngestionArchiveJournal } from "./ingestion-archive-journal";
import {
  deadLetterEnvelope,
  deadLetterRequestId,
  handleIngestionDeadLetters,
} from "./ingestion-dead-letter";

const mocks = vi.hoisted(() => ({
  accept: vi.fn<typeof import("./ingestion-service").acceptIngestion>(),
}));
vi.mock("./ingestion-service", () => ({ acceptIngestion: mocks.accept }));
beforeEach(() => {
  vi.clearAllMocks();
  vi.spyOn(console, "error").mockImplementation(() => undefined);
});
afterEach(() => {
  vi.restoreAllMocks();
});

test("records the received identity/time/body but not changing retry attempts", () => {
  const message = {
    id: "message-1",
    timestamp: new Date("2026-09-16T00:00:00.000Z"),
    body: { type: "fetch-odds", raceKey: "nar:202609165401" },
    attempts: 1,
  };
  const first = deadLetterEnvelope(message);
  expect(first.source).toBe("sync-realtime-data-hot-v2");
  expect(first.requestId).toMatch(/^dlq_[a-f0-9]{64}$/u);
  expect(first.payload).toBe(
    '{"formatVersion":1,"kind":"dead-letter","queue":"sync-realtime-data-hot-ingestion-dlq","messageId":"message-1","queuedAt":"2026-09-16T00:00:00.000Z","body":{"type":"fetch-odds","raceKey":"nar:202609165401"}}',
  );
  const retried = { ...message, attempts: 9 };
  const repeated = deadLetterEnvelope(retried);
  expect(first.requestId === repeated.requestId && first.payload === repeated.payload).toBe(true);
});

test.each([
  undefined,
  { omitted: undefined },
  Number.NaN,
  Number.POSITIVE_INFINITY,
  -0,
  new Map(),
  new Date(),
  1n,
])("rejects lossy or unsupported decoded message values", (body) => {
  expect(() =>
    deadLetterEnvelope({ id: "message", timestamp: new Date("2026-09-16"), body }),
  ).toThrow();
});

test.each([null, 1, undefined])("rejects invalid identity before pending-journal access", (id) => {
  expect(() => deadLetterRequestId(id)).toThrow("message identity");
});

test.each(["", "x".repeat(129)])("rejects invalid message identifiers", (id) => {
  expect(() => deadLetterEnvelope({ id, timestamp: new Date("2026-09-16"), body: {} })).toThrow(
    "message identity",
  );
});

test("a journal completion failure retries without acknowledging retained input", async () => {
  const message = {
    id: "completion-failed",
    timestamp: new Date("2026-09-16"),
    body: {},
    attempts: 1,
    ack: vi.fn(),
    retry: vi.fn(),
  } satisfies Message<unknown>;
  const batch = {
    queue: "sync-realtime-data-hot-ingestion-dlq",
    messages: [message],
    metadata: { metrics: { backlogCount: 1, backlogBytes: 2 } },
    ackAll: vi.fn(),
    retryAll: vi.fn(),
  } satisfies MessageBatch<unknown>;
  const env = mockDeep<CatalogBindings>();
  const journal = mockDeep<DurableObjectStub<IngestionArchiveJournal>>();
  env.INGESTION_ARCHIVE_JOURNAL.getByName.mockReturnValue(journal);
  journal.recordReceipt.mockRejectedValue(new Error("completion unavailable"));
  mocks.accept.mockResolvedValueOnce({
    source: "sync-realtime-data-hot-v2",
    requestId: "retained",
    digest: "digest",
    sequence: "1",
    accepted: true,
  });
  await handleIngestionDeadLetters(batch, env);
  expect(journal.begin).toHaveBeenCalledOnce();
  expect(mocks.accept).toHaveBeenCalledOnce();
  expect(message.ack).not.toHaveBeenCalled();
  expect(message.retry).toHaveBeenCalledWith({ delaySeconds: 86400 });
});

test("rejects a misrouted batch before storage or acknowledgements", async () => {
  const batch = mockDeep<MessageBatch<unknown>>({ queue: "another-queue" });
  await expect(handleIngestionDeadLetters(batch, mockDeep<CatalogBindings>())).rejects.toThrow(
    "Unexpected",
  );
  expect(mocks.accept).not.toHaveBeenCalled();
  expect(batch.ackAll).not.toHaveBeenCalled();
});

test("acknowledges only after verified archival, retries failures without exposing payloads", async () => {
  // Keep decoded bodies and Dates as real values: deep proxies invent toJSON methods.
  const success = {
    id: "ok",
    timestamp: new Date("2026-09-16"),
    body: { type: "fetch-odds", raceKey: "race" },
    attempts: 1,
    ack: vi.fn(),
    retry: vi.fn(),
  } satisfies Message<unknown>;
  const failed = {
    id: "failed",
    timestamp: new Date("2026-09-16"),
    body: { privatePayload: "do-not-log" },
    attempts: 1,
    ack: vi.fn(),
    retry: vi.fn(),
  } satisfies Message<unknown>;
  const malformed = {
    id: "bad",
    timestamp: new Date("invalid"),
    body: {},
    attempts: 1,
    ack: vi.fn(),
    retry: vi.fn(),
  } satisfies Message<unknown>;
  const batch = {
    queue: "sync-realtime-data-hot-ingestion-dlq",
    messages: [success, failed, malformed],
    metadata: { metrics: { backlogCount: 3, backlogBytes: 256 } },
    ackAll: vi.fn(),
    retryAll: vi.fn(),
  } satisfies MessageBatch<unknown>;
  mocks.accept
    .mockImplementationOnce(async () => {
      expect(success.ack).not.toHaveBeenCalled();
      return {
        source: "sync-realtime-data-hot-v2",
        requestId: "retained",
        digest: "digest",
        sequence: "1",
        accepted: true,
      };
    })
    .mockRejectedValueOnce(new Error("do-not-log sensitive error"));
  const env = mockDeep<CatalogBindings>();
  const journal = mockDeep<DurableObjectStub<IngestionArchiveJournal>>();
  env.INGESTION_ARCHIVE_JOURNAL.getByName.mockReturnValue(journal);
  journal.recordReceipt.mockImplementation(async () => {
    expect(success.ack).not.toHaveBeenCalled();
  });
  await handleIngestionDeadLetters(batch, env);
  expect(journal.begin).toHaveBeenCalledTimes(2);
  expect(journal.recordReceipt).toHaveBeenCalledOnce();
  expect(success.ack).toHaveBeenCalledOnce();
  expect(success.retry).not.toHaveBeenCalled();
  expect(failed.ack).not.toHaveBeenCalled();
  expect(failed.retry).toHaveBeenCalledWith({ delaySeconds: 86400 });
  expect(malformed.retry).toHaveBeenCalledWith({ delaySeconds: 86400 });
  expect(mocks.accept).toHaveBeenCalledTimes(2);
  expect(console.error).toHaveBeenCalledWith(
    '{"event":"ingestion_dlq_archive_failed","queue":"sync-realtime-data-hot-ingestion-dlq","messageId":"failed"}',
  );
  expect(batch.ackAll).not.toHaveBeenCalled();
});
