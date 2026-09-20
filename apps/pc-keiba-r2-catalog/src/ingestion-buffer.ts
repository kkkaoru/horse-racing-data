// Runs with bun; retain immutable input before a durable inbox can acknowledge acceptance.
import { createHash } from "node:crypto";

export interface IngestionEnvelope {
  source: string;
  requestId: string;
  payload: string;
}
interface IngestionObjectIdentity {
  source: string;
  requestId: string;
  digest: string;
}
export interface IngestionPointer extends IngestionObjectIdentity {
  formatVersion: 1;
  key: string;
  bytes: number;
}
export interface PreparedIngestion {
  pointer: IngestionPointer;
  serialized: string;
}
export interface IngestionAcceptance {
  source: string;
  requestId: string;
  digest: string;
  sequence: string;
  accepted: true;
}
interface IngestionPutCondition {
  etagDoesNotMatch: string;
}
interface IngestionContentMetadata {
  contentType: string;
}
interface IngestionPutOptions {
  onlyIf: IngestionPutCondition;
  sha256: string;
  httpMetadata: IngestionContentMetadata;
}
// Restrict the binding contract to operations actually used, not unrelated platform/Node Headers overloads.
interface IngestionBucket {
  put: (
    key: string,
    value: string,
    options: IngestionPutOptions,
  ) => Promise<Pick<R2Object, "size"> | null>;
  get: (key: string) => Promise<Pick<R2ObjectBody, "size" | "arrayBuffer"> | null>;
}
export interface IngestionBufferDependencies {
  bucket: IngestionBucket;
  accept: (pointer: IngestionPointer) => Promise<unknown>;
}
const MAX_BYTES: number = 1048576;
const MAX_SEQUENCE: bigint = 9223372036854775807n;
const SOURCE_PATTERN: RegExp = /^[a-zA-Z0-9_-]{1,64}$/u;
const REQUEST_PATTERN: RegExp = /^[a-zA-Z0-9_-]{1,128}$/u;
const DIGEST_PATTERN: RegExp = /^[a-f0-9]{64}$/u;
const encoder: TextEncoder = new TextEncoder();
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const digest = (value: string | Uint8Array): string =>
  createHash("sha256").update(value).digest("hex");
const validName = (value: string, pattern: RegExp): boolean =>
  pattern.test(value) && value.trim() === value;
const objectKey = (input: IngestionObjectIdentity): string =>
  `ingestion/inbox/v1/${input.source}/${input.requestId}/${input.digest}.json`;

export const prepareIngestionEnvelope = (input: IngestionEnvelope): PreparedIngestion => {
  if (
    !validName(input.source, SOURCE_PATTERN) ||
    !validName(input.requestId, REQUEST_PATTERN) ||
    encoder.encode(input.payload).byteLength > MAX_BYTES
  )
    throw new Error("Invalid ingestion envelope");
  // Validate syntax, but never parse/re-serialize payload numbers or otherwise change its bytes.
  JSON.parse(input.payload);
  const serialized: string = JSON.stringify({
    formatVersion: 1,
    source: input.source,
    requestId: input.requestId,
    payload: input.payload,
  });
  const bytes: number = encoder.encode(serialized).byteLength;
  if (bytes > MAX_BYTES) throw new Error("Ingestion envelope exceeds byte limit");
  const hash: string = digest(serialized);
  return {
    serialized,
    pointer: {
      formatVersion: 1,
      source: input.source,
      requestId: input.requestId,
      key: objectKey({ source: input.source, requestId: input.requestId, digest: hash }),
      digest: hash,
      bytes,
    },
  };
};

export const parseIngestionPointer = (value: unknown): IngestionPointer => {
  if (
    !record(value) ||
    value.formatVersion !== 1 ||
    typeof value.source !== "string" ||
    !validName(value.source, SOURCE_PATTERN) ||
    typeof value.requestId !== "string" ||
    !validName(value.requestId, REQUEST_PATTERN) ||
    typeof value.digest !== "string" ||
    !DIGEST_PATTERN.test(value.digest) ||
    value.digest.length !== 64 ||
    typeof value.bytes !== "number" ||
    !Number.isSafeInteger(value.bytes) ||
    value.bytes < 1 ||
    value.bytes > MAX_BYTES ||
    typeof value.key !== "string" ||
    value.key !==
      objectKey({ source: value.source, requestId: value.requestId, digest: value.digest })
  )
    throw new Error("Invalid ingestion pointer");
  return {
    formatVersion: 1,
    source: value.source,
    requestId: value.requestId,
    digest: value.digest,
    bytes: value.bytes,
    key: value.key,
  };
};

const acceptance = (value: unknown, pointer: IngestionPointer): IngestionAcceptance => {
  if (
    !record(value) ||
    value.accepted !== true ||
    value.source !== pointer.source ||
    value.requestId !== pointer.requestId ||
    value.digest !== pointer.digest ||
    typeof value.sequence !== "string" ||
    !/^[1-9]\d{0,18}$/u.test(value.sequence) ||
    BigInt(value.sequence).toString() !== value.sequence ||
    BigInt(value.sequence) > MAX_SEQUENCE
  )
    throw new Error("Durable ingestion acceptance mismatch");
  return {
    source: pointer.source,
    requestId: pointer.requestId,
    digest: pointer.digest,
    sequence: value.sequence,
    accepted: true,
  };
};

/** An inbox error/lost acknowledgement must propagate: retain the same bytes and retry, never ack early. */
export const bufferIngestion = async (
  input: IngestionEnvelope,
  dependencies: IngestionBufferDependencies,
): Promise<IngestionAcceptance> => {
  const prepared: PreparedIngestion = prepareIngestionEnvelope(input);
  await dependencies.bucket.put(prepared.pointer.key, prepared.serialized, {
    onlyIf: { etagDoesNotMatch: "*" },
    sha256: prepared.pointer.digest,
    httpMetadata: { contentType: "application/json" },
  });
  const restored: Pick<R2ObjectBody, "size" | "arrayBuffer"> | null = await dependencies.bucket.get(
    prepared.pointer.key,
  );
  if (
    restored === null ||
    restored.size !== prepared.pointer.bytes ||
    digest(new Uint8Array(await restored.arrayBuffer())) !== prepared.pointer.digest
  )
    throw new Error("Immutable ingestion readback mismatch");
  return acceptance(await dependencies.accept(prepared.pointer), prepared.pointer);
};
