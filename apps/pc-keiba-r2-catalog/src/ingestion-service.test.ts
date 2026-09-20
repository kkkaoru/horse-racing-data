// Runs with bun via Vitest; service binding and storage calls are mocked.
import { expect, test, vi } from "vitest";
import { mock, mockDeep } from "vitest-mock-extended";
import { prepareIngestionEnvelope } from "./ingestion-buffer";
import type { IngestionInbox } from "./ingestion-inbox";
import { acceptIngestion, IngestionBufferService } from "./ingestion-service";

const input = { source: "sync-realtime-data-hot-v2", requestId: "message-1", payload: "{}" };

test.each([
  null,
  [],
  {},
  { ...input, source: "other" },
  { source: input.source },
  { ...input, requestId: 1 },
  { source: input.source, requestId: "message-1" },
  { ...input, payload: 1 },
  { ...input, payload: "not JSON" },
  { ...input, requestId: "../other" },
])("rejects invalid or non-pilot envelopes before storage access", async (value) => {
  const env = mockDeep<Pick<CatalogBindings, "CATALOG_OBJECTS" | "INGESTION_INBOX">>();
  const service = new IngestionBufferService(mockDeep<ExecutionContext>(), env);
  await expect(service.accept(value)).rejects.toThrow();
  expect(env.CATALOG_OBJECTS.put).not.toHaveBeenCalled();
  expect(env.INGESTION_INBOX.getByName).not.toHaveBeenCalled();
});

test("uses a source-specific private inbox only after retaining and verifying bytes", async () => {
  const env = mockDeep<Pick<CatalogBindings, "CATALOG_OBJECTS" | "INGESTION_INBOX">>();
  const stub = mockDeep<DurableObjectStub<IngestionInbox>>();
  const prepared = prepareIngestionEnvelope(input);
  env.CATALOG_OBJECTS.put.mockResolvedValue(mock<R2Object>());
  env.CATALOG_OBJECTS.get.mockResolvedValue(
    mock<R2ObjectBody>({
      size: prepared.pointer.bytes,
      arrayBuffer: async () => new Response(prepared.serialized).arrayBuffer(),
    }),
  );
  env.INGESTION_INBOX.getByName.mockReturnValue(stub);
  const dispose = vi.fn<() => void>();
  stub.accept.mockResolvedValue({
    [Symbol.dispose]: dispose,
    source: input.source,
    requestId: input.requestId,
    digest: prepared.pointer.digest,
    sequence: "1",
    accepted: true,
  });
  const service = new IngestionBufferService(mockDeep<ExecutionContext>(), env);
  expect(await service.accept(input)).toMatchObject({
    source: "sync-realtime-data-hot-v2",
    requestId: "message-1",
    sequence: "1",
    accepted: true,
  });
  expect(env.INGESTION_INBOX.getByName).toHaveBeenCalledWith("sync-realtime-data-hot-v2");
  expect(dispose).toHaveBeenCalledTimes(1);
  expect(stub.accept).toHaveBeenCalledWith(
    expect.objectContaining({
      source: "sync-realtime-data-hot-v2",
      requestId: "message-1",
      bytes: expect.any(Number),
    }),
  );
});

test("shared acceptance helper applies the same pilot and storage gate without an RPC wrapper", async () => {
  const env = mockDeep<Pick<CatalogBindings, "CATALOG_OBJECTS" | "INGESTION_INBOX">>();
  await expect(acceptIngestion({ ...input, source: "other" }, env)).rejects.toThrow(
    "Invalid ingestion service envelope",
  );
  expect(env.CATALOG_OBJECTS.put).not.toHaveBeenCalled();
});

test("R2 failure prevents durable-inbox access and propagates to the caller", async () => {
  const env = mockDeep<Pick<CatalogBindings, "CATALOG_OBJECTS" | "INGESTION_INBOX">>();
  env.CATALOG_OBJECTS.put.mockRejectedValue(new Error("R2 unavailable"));
  const service = new IngestionBufferService(mockDeep<ExecutionContext>(), env);
  await expect(service.accept(input)).rejects.toThrow("R2 unavailable");
  expect(env.INGESTION_INBOX.getByName).not.toHaveBeenCalled();
});
