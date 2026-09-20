// Runs with bun via Vitest; mocked fault boundaries and native R2 immutable-write semantics.
import { Miniflare } from "miniflare";
import { expect, test, vi } from "vitest";
import { mock, mockDeep } from "vitest-mock-extended";
import {
  bufferIngestion,
  parseIngestionPointer,
  prepareIngestionEnvelope,
  type IngestionBufferDependencies,
  type IngestionEnvelope,
} from "./ingestion-buffer";

const input: IngestionEnvelope = {
  source: "hot-logs",
  requestId: "message-1",
  payload: '{"rowid":9007199254740993}',
};

test("retains exact payload text including int64 JSON literals", () => {
  const prepared = prepareIngestionEnvelope(input);
  expect(prepared.serialized).toBe(
    '{"formatVersion":1,"source":"hot-logs","requestId":"message-1","payload":"{\\"rowid\\":9007199254740993}"}',
  );
  expect(prepared.pointer.key).toMatch(
    /^ingestion\/inbox\/v1\/hot-logs\/message-1\/[a-f0-9]{64}\.json$/u,
  );
  expect(parseIngestionPointer(prepared.pointer)).toMatchObject({
    source: "hot-logs",
    requestId: "message-1",
    formatVersion: 1,
  });
});

test.each([
  { source: "" },
  { source: "hot-logs\n" },
  { requestId: "../other" },
  { requestId: "message-1\n" },
  { payload: "invalid JSON" },
  { payload: '"' + "x".repeat(1048576) + '"' },
  { payload: '"' + "x".repeat(1048570) + '"' },
  { payload: '"' + "馬".repeat(350000) + '"' },
])("rejects unsafe envelopes and byte overflow including wrapper overhead", (invalid) => {
  expect(() => prepareIngestionEnvelope({ ...input, ...invalid })).toThrow();
});

test.each([
  null,
  [],
  { formatVersion: 2 },
  { source: 1 },
  { source: "x\n" },
  { requestId: 1 },
  { requestId: "../x" },
  { digest: 1 },
  { digest: "bad" },
  { digest: "0".repeat(64) + "\n" },
  { bytes: "1" },
  { bytes: 0 },
  { bytes: 1.5 },
  { bytes: 1048577 },
  { key: "outside" },
  { key: 1 },
])("rejects malformed or misrouted pointers", (invalid) => {
  const pointer = prepareIngestionEnvelope(input).pointer;
  expect(() =>
    parseIngestionPointer(
      typeof invalid === "object" && invalid !== null && !Array.isArray(invalid)
        ? { ...pointer, ...invalid }
        : invalid,
    ),
  ).toThrow("Invalid ingestion pointer");
});

test("does not acknowledge before immutable put and complete readback", async () => {
  const prepared = prepareIngestionEnvelope(input);
  const calls: string[] = [];
  const bucket = mockDeep<R2Bucket>();
  bucket.put.mockImplementation(async () => {
    calls.push("put");
    return mock<R2Object>();
  });
  bucket.get.mockImplementation(async () => {
    calls.push("get");
    return mock<R2ObjectBody>({
      size: prepared.pointer.bytes,
      arrayBuffer: async () => {
        calls.push("body");
        return new Response(prepared.serialized).arrayBuffer();
      },
    });
  });
  const accept: IngestionBufferDependencies["accept"] = async (pointer) => {
    calls.push("accept");
    return {
      source: pointer.source,
      requestId: pointer.requestId,
      digest: pointer.digest,
      sequence: "9007199254740993",
      accepted: true,
    };
  };
  expect(await bufferIngestion(input, { bucket, accept })).toMatchObject({
    source: "hot-logs",
    requestId: "message-1",
    sequence: "9007199254740993",
    accepted: true,
  });
  expect(calls).toStrictEqual(["put", "get", "body", "accept"]);
  expect(bucket.put).toHaveBeenCalledWith(
    expect.any(String),
    expect.any(String),
    expect.objectContaining({
      onlyIf: { etagDoesNotMatch: "*" },
      sha256: expect.stringMatching(/^[a-f0-9]{64}$/u),
    }),
  );
});

test("storage failures and corrupt/missing readback never reach the inbox", async () => {
  const prepared = prepareIngestionEnvelope(input);
  const bucket = mockDeep<R2Bucket>();
  const accept = vi.fn<IngestionBufferDependencies["accept"]>();
  bucket.put.mockRejectedValueOnce(new Error("put failed"));
  await expect(bufferIngestion(input, { bucket, accept })).rejects.toThrow("put failed");
  bucket.put.mockResolvedValue(mock<R2Object>());
  bucket.get.mockResolvedValueOnce(null);
  await expect(bufferIngestion(input, { bucket, accept })).rejects.toThrow("readback mismatch");
  bucket.get.mockResolvedValueOnce(mock<R2ObjectBody>({ size: prepared.pointer.bytes + 1 }));
  await expect(bufferIngestion(input, { bucket, accept })).rejects.toThrow("readback mismatch");
  bucket.get.mockResolvedValueOnce(
    mock<R2ObjectBody>({
      size: prepared.pointer.bytes,
      arrayBuffer: async () => new Response("corrupt").arrayBuffer(),
    }),
  );
  await expect(bufferIngestion(input, { bucket, accept })).rejects.toThrow("readback mismatch");
  expect(accept).not.toHaveBeenCalled();
});

test.each([
  null,
  { accepted: false },
  { source: "other" },
  { requestId: "other" },
  { digest: "other" },
  { sequence: 1 },
  { sequence: "0" },
  { sequence: "1\n" },
  { sequence: "9223372036854775808" },
])("rejects incomplete or changed durable acknowledgements", async (override) => {
  const prepared = prepareIngestionEnvelope(input);
  const bucket = mockDeep<R2Bucket>();
  bucket.put.mockResolvedValue(mock<R2Object>());
  bucket.get.mockResolvedValue(
    mock<R2ObjectBody>({
      size: prepared.pointer.bytes,
      arrayBuffer: async () => new Response(prepared.serialized).arrayBuffer(),
    }),
  );
  const receipt: unknown =
    override === null
      ? null
      : {
          source: input.source,
          requestId: input.requestId,
          digest: prepared.pointer.digest,
          sequence: "1",
          accepted: true,
          ...override,
        };
  await expect(bufferIngestion(input, { bucket, accept: async () => receipt })).rejects.toThrow(
    "acceptance mismatch",
  );
});

test("lost inbox acknowledgement propagates after retaining immutable bytes", async () => {
  const prepared = prepareIngestionEnvelope(input);
  const bucket = mockDeep<R2Bucket>();
  bucket.put.mockResolvedValue(mock<R2Object>());
  bucket.get.mockResolvedValue(
    mock<R2ObjectBody>({
      size: prepared.pointer.bytes,
      arrayBuffer: async () => new Response(prepared.serialized).arrayBuffer(),
    }),
  );
  const accept = vi
    .fn<IngestionBufferDependencies["accept"]>()
    .mockRejectedValue(new Error("lost ack"));
  await expect(bufferIngestion(input, { bucket, accept })).rejects.toThrow("lost ack");
  expect(accept).toHaveBeenCalledTimes(1);
});

test("rejects changed raw bytes even when UTF-8 replacement decoding yields identical text", async () => {
  const envelope = { ...input, payload: '"�"' };
  const prepared = prepareIngestionEnvelope(envelope);
  const corrupted = new TextEncoder().encode(prepared.serialized);
  const offset = corrupted.indexOf(239);
  if (offset < 0) throw new Error("Missing replacement character fixture");
  corrupted.set([240, 144, 128], offset);
  expect(new TextDecoder().decode(corrupted) === prepared.serialized).toBe(true);
  const bucket = mockDeep<R2Bucket>();
  bucket.put.mockResolvedValue(mock<R2Object>());
  bucket.get.mockResolvedValue(
    mock<R2ObjectBody>({
      size: prepared.pointer.bytes,
      arrayBuffer: async () => new Uint8Array(corrupted).buffer,
    }),
  );
  const accept = vi.fn<IngestionBufferDependencies["accept"]>();
  await expect(bufferIngestion(envelope, { bucket, accept })).rejects.toThrow("readback mismatch");
  expect(accept).not.toHaveBeenCalled();
});

test("native R2 does not overwrite corrupt retained content or acknowledge it", async () => {
  const mf = new Miniflare({
    modules: true,
    script: "export default {fetch(){return new Response('ok')}}",
    r2Buckets: ["BUCKET"],
  });
  try {
    const bucket = await mf.getR2Bucket("BUCKET");
    const prepared = prepareIngestionEnvelope(input);
    await bucket.put(prepared.pointer.key, "x".repeat(prepared.pointer.bytes));
    const accept = vi.fn<IngestionBufferDependencies["accept"]>();
    await expect(bufferIngestion(input, { bucket, accept })).rejects.toThrow("readback mismatch");
    expect(accept).not.toHaveBeenCalled();
    const retained = await bucket.get(prepared.pointer.key);
    if (retained === null) throw new Error("Expected retained object");
    expect(await retained.text()).toMatch(/^x+$/u);
  } finally {
    await mf.dispose();
  }
});

test("native R2 SHA validation and conditional put preserve the existing object on replay", async () => {
  const mf = new Miniflare({
    modules: true,
    script: "export default {fetch(){return new Response('ok')}}",
    r2Buckets: ["BUCKET"],
  });
  try {
    const bucket = await mf.getR2Bucket("BUCKET");
    const accept: IngestionBufferDependencies["accept"] = async (pointer) => ({
      source: pointer.source,
      requestId: pointer.requestId,
      digest: pointer.digest,
      sequence: "1",
      accepted: true,
    });
    await bufferIngestion(input, { bucket, accept });
    const key = prepareIngestionEnvelope(input).pointer.key;
    const first = await bucket.head(key);
    await bufferIngestion(input, { bucket, accept });
    const replay = await bucket.head(key);
    expect(first !== null && replay !== null && first.version === replay.version).toBe(true);
    const original = await bucket.get(key);
    if (original === null) throw new Error("Expected retained object");
    expect(await original.text()).toBe(
      '{"formatVersion":1,"source":"hot-logs","requestId":"message-1","payload":"{\\"rowid\\":9007199254740993}"}',
    );
  } finally {
    await mf.dispose();
  }
});
