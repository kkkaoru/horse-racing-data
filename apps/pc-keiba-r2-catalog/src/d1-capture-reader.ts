// Runs with bun; bounded, lossless journal reads, not historical source-row reconstruction.
import { D1_CAPTURE_TABLE } from "./d1-change-capture";
import type { D1SnapshotQuery } from "./d1-snapshot";

export type D1CaptureOperation = "insert" | "update" | "delete" | "touch";
export interface D1CaptureRegistration {
  captureId: string;
  table: string;
  schemaHash: string;
}
export interface D1CapturePageRequest {
  afterSequence: string;
  throughSequence: string;
  limit: number;
}
export interface D1CaptureKeys {
  beforeKey: string | null;
  afterKey: string | null;
}
export interface D1CaptureEvent extends D1CaptureRegistration, D1CaptureKeys {
  sequence: string;
  operation: D1CaptureOperation;
  capturedAt: string;
}
export interface D1CapturePage {
  events: readonly D1CaptureEvent[];
  lastSequence: string;
  rangeExhausted: boolean;
}
export interface D1CapturePageInput {
  rows: readonly Record<string, unknown>[];
  request: D1CapturePageRequest;
  registrations: readonly D1CaptureRegistration[];
}
interface KeyValidators {
  insert: (keys: D1CaptureKeys) => boolean;
  update: (keys: D1CaptureKeys) => boolean;
  delete: (keys: D1CaptureKeys) => boolean;
  touch: (keys: D1CaptureKeys) => boolean;
}
const MAX_SEQUENCE: bigint = 9223372036854775807n;
const MIN_KEY: bigint = -9223372036854775808n;
const MAX_PAGE_ROWS: number = 1000;
const MAX_TABLE_LENGTH: number = 256;
const MAX_INTEGER_LENGTH: number = 20;
const MAX_TIMESTAMP_LENGTH: number = 24;
const HASH_LENGTH: number = 64;
const HASH_PATTERN: RegExp = /^[a-f0-9]{64}$/u;
const CAPTURE_PATTERN: RegExp = /^[a-zA-Z0-9_-]{1,64}$/u;
const INTEGER_PATTERN: RegExp = /^(?:0|-?[1-9]\d*)$/u;
const KEY_VALIDATORS: KeyValidators = {
  insert: ({ beforeKey, afterKey }) => beforeKey === null && afterKey !== null,
  update: ({ beforeKey, afterKey }) => beforeKey !== null && afterKey !== null,
  delete: ({ beforeKey, afterKey }) => beforeKey !== null && afterKey === null,
  touch: ({ beforeKey, afterKey }) => beforeKey !== null && beforeKey === afterKey,
};
const isInteger = (value: unknown): value is string =>
  typeof value === "string" &&
  value.length <= MAX_INTEGER_LENGTH &&
  INTEGER_PATTERN.test(value) &&
  BigInt(value).toString() === value &&
  BigInt(value) >= MIN_KEY &&
  BigInt(value) <= MAX_SEQUENCE;
const isSequence = (value: unknown): value is string => isInteger(value) && BigInt(value) >= 0n;
const isKey = (value: unknown): value is string | null => value === null || isInteger(value);
const isOperation = (value: unknown): value is D1CaptureOperation =>
  value === "insert" || value === "update" || value === "delete" || value === "touch";
const validRegistration = (registration: D1CaptureRegistration): boolean =>
  CAPTURE_PATTERN.test(registration.captureId) &&
  registration.captureId.trim() === registration.captureId &&
  registration.table.length > 0 &&
  registration.table.length <= MAX_TABLE_LENGTH &&
  registration.schemaHash.length === HASH_LENGTH &&
  HASH_PATTERN.test(registration.schemaHash);
const registrationKey = (
  registration: Pick<D1CaptureRegistration, "captureId" | "table">,
): string => `${registration.captureId}/${registration.table}`;

export const buildD1CapturePageQuery = (request: D1CapturePageRequest): D1SnapshotQuery => {
  if (
    !isSequence(request.afterSequence) ||
    !isSequence(request.throughSequence) ||
    BigInt(request.afterSequence) > BigInt(request.throughSequence) ||
    !Number.isInteger(request.limit) ||
    request.limit < 1 ||
    request.limit > MAX_PAGE_ROWS
  )
    throw new Error("Invalid capture page range");
  // Do not filter by table/capture: preserve a single indexed database-wide sequence scan.
  return {
    sql: `SELECT CAST(sequence AS TEXT) AS sequence, capture_id, table_name, schema_hash, operation, before_key, after_key, captured_at FROM "${D1_CAPTURE_TABLE}" WHERE sequence > CAST(? AS INTEGER) AND sequence <= CAST(? AS INTEGER) ORDER BY sequence LIMIT ?`,
    params: [request.afterSequence, request.throughSequence, request.limit],
  };
};

const parseEvent = (row: Record<string, unknown>): D1CaptureEvent => {
  if (
    !isSequence(row.sequence) ||
    typeof row.capture_id !== "string" ||
    typeof row.table_name !== "string" ||
    typeof row.schema_hash !== "string" ||
    !isOperation(row.operation) ||
    !isKey(row.before_key) ||
    !isKey(row.after_key) ||
    typeof row.captured_at !== "string" ||
    row.captured_at.length !== MAX_TIMESTAMP_LENGTH ||
    !Number.isFinite(Date.parse(row.captured_at)) ||
    new Date(row.captured_at).toISOString() !== row.captured_at
  )
    throw new Error("Invalid capture event");
  const keys: D1CaptureKeys = { beforeKey: row.before_key, afterKey: row.after_key };
  if (!KEY_VALIDATORS[row.operation](keys)) throw new Error("Invalid capture operation keys");
  return {
    captureId: row.capture_id,
    table: row.table_name,
    schemaHash: row.schema_hash,
    sequence: row.sequence,
    operation: row.operation,
    capturedAt: row.captured_at,
    ...keys,
  };
};

/** Gaps may be legitimate; exhaustion is not proof of source fencing or safe journal pruning. */
export const parseD1CapturePage = (input: D1CapturePageInput): D1CapturePage => {
  buildD1CapturePageQuery(input.request);
  if (input.rows.length > input.request.limit)
    throw new Error("Capture page exceeded requested limit");
  const registry: Map<string, string> = new Map();
  for (const registration of input.registrations) {
    const key: string = registrationKey(registration);
    if (!validRegistration(registration) || registry.has(key))
      throw new Error("Invalid capture registration");
    registry.set(key, registration.schemaHash);
  }
  const cursor: { sequence: string } = { sequence: input.request.afterSequence };
  const events: D1CaptureEvent[] = input.rows.map((row) => {
    const event: D1CaptureEvent = parseEvent(row);
    if (registry.get(registrationKey(event)) !== event.schemaHash)
      throw new Error("Unregistered capture schema");
    if (
      BigInt(event.sequence) <= BigInt(cursor.sequence) ||
      BigInt(event.sequence) > BigInt(input.request.throughSequence)
    )
      throw new Error("Capture sequence outside ordered range");
    cursor.sequence = event.sequence;
    return event;
  });
  return {
    events,
    lastSequence: cursor.sequence,
    rangeExhausted: events.length < input.request.limit,
  };
};
