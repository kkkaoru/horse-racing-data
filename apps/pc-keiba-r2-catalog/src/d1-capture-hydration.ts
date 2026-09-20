// Runs with bun; bounded dirty-key observations, not fenced images or confirmed tombstones.
import {
  buildD1SnapshotRowEncoding,
  parseD1SnapshotPage,
  type D1SnapshotColumn,
  type D1SnapshotPageRow,
  type D1SnapshotQuery,
} from "./d1-snapshot";

export interface D1CaptureKeyRequest {
  table: string;
  columns: readonly D1SnapshotColumn[];
  keys: readonly string[];
}
export interface D1CaptureKeyRead {
  request: D1CaptureKeyRequest;
  rows: readonly Record<string, unknown>[];
}
export interface D1CaptureKeyObservation {
  rows: readonly D1SnapshotPageRow[];
  absentAtRead: readonly string[];
  sourceFenceVerified: false;
}
const MAX_KEYS: number = 100;
const MIN_KEY: bigint = -9223372036854775808n;
const MAX_KEY: bigint = 9223372036854775807n;
const KEY_PATTERN: RegExp = /^-?(?:0|[1-9]\d*)$/u;
const MAX_KEY_LENGTH: number = 20;
const validKey = (key: string): boolean =>
  key.length <= MAX_KEY_LENGTH &&
  KEY_PATTERN.test(key) &&
  BigInt(key).toString() === key &&
  BigInt(key) >= MIN_KEY &&
  BigInt(key) <= MAX_KEY;
// Only the sign is used for sorting; every int64 difference is finite as a JS number.
const compareKeys = (left: string, right: string): number => Number(BigInt(left) - BigInt(right));
const orderedKeys = (input: D1CaptureKeyRequest): string[] => {
  if (
    input.keys.length < 1 ||
    input.keys.length > MAX_KEYS ||
    input.keys.some((key) => !validKey(key))
  )
    throw new Error("Invalid bounded capture key list");
  return [...new Set(input.keys)].sort(compareKeys);
};

export const buildD1CaptureKeyQuery = (input: D1CaptureKeyRequest): D1SnapshotQuery => {
  const keys: string[] = orderedKeys(input);
  const encoding = buildD1SnapshotRowEncoding(input);
  const table: string = `"${input.table.replaceAll('"', '""')}"`;
  return {
    sql: `SELECT CAST(${encoding.rowId} AS TEXT) AS row_key, ${encoding.payloadSql} AS payload FROM ${table} WHERE ${encoding.rowId} IN (${keys.map(() => "CAST(? AS INTEGER)").join(", ")}) ORDER BY ${encoding.rowId}`,
    params: keys,
  };
};

/** Absence can be transient or rowid reuse; require an external source fence before reconciliation. */
export const parseD1CaptureKeyRead = (input: D1CaptureKeyRead): D1CaptureKeyObservation => {
  const keys: readonly string[] = orderedKeys(input.request);
  buildD1SnapshotRowEncoding(input.request);
  const requested: ReadonlySet<string> = new Set(keys);
  if (input.rows.some((row) => typeof row.row_key !== "string" || !requested.has(row.row_key)))
    throw new Error("Capture lookup returned an unrequested identity");
  const rows: D1SnapshotPageRow[] = parseD1SnapshotPage([...input.rows], {
    table: input.request.table,
    columns: input.request.columns,
    afterRowId: null,
    limit: keys.length,
  });
  const found: ReadonlySet<string> = new Set(rows.map((row) => row.row_key));
  return { rows, absentAtRead: keys.filter((key) => !found.has(key)), sourceFenceVerified: false };
};
