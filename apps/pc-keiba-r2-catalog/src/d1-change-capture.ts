// Runs with bun; generates capture DDL only. Installing it is a separate, audited step.
import { createHash } from "node:crypto";
import { buildD1SnapshotRowEncoding, type D1SnapshotColumn } from "./d1-snapshot";

export const D1_CAPTURE_TABLE: string = "__pc_keiba_catalog_cdc_v1";
export interface D1CaptureUniqueKey {
  columns: readonly { name: string; collation: string }[];
  partial: boolean;
}
export interface D1CaptureInput {
  table: string;
  columns: readonly D1SnapshotColumn[];
  captureId: string;
  // Must include every unique index from index_list/index_xinfo; unsupported indexes fail closed.
  uniqueKeys: readonly D1CaptureUniqueKey[];
}
export interface D1CapturePlan {
  createOutbox: string;
  triggers: { name: string; sql: string }[];
  schemaHash: string;
  planHash: string;
}
const quote = (value: string): string => `"${value.replaceAll('"', '""')}"`;
const literal = (value: string): string => `'${value.replaceAll("'", "''")}'`;
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const isList = (value: unknown): value is readonly unknown[] => Array.isArray(value);

/**
 * Dirty-key journal, NOT an image-based change stream. No source values are duplicated:
 * encoding a large source BLOB/image in a trigger can exceed D1's row limit and reject writes.
 * Start a watermark only after all five triggers are verified. Rehydrate both old/new keys
 * under a writer fence; source absence is the tombstone, including rowid reuse. This stream
 * alone cannot reconstruct historical row images or an online snapshot at an event sequence.
 * Trigger writes change D1 meta.changes, including BEFORE-trigger writes on ignored inserts.
 * All affected CAS/claim consumers must use RETURNING (and be drained) before installation.
 */
export const buildD1CapturePlan = (input: D1CaptureInput): D1CapturePlan => {
  if (!/^[a-zA-Z0-9_-]{1,64}$/u.test(input.captureId)) throw new Error("Invalid capture identity");
  if (
    input.table.toLowerCase().startsWith("__pc_keiba_catalog_cdc") ||
    input.table.toLowerCase().startsWith("_cf_") ||
    input.table.toLowerCase().startsWith("sqlite_")
  )
    throw new Error("Cannot capture internal tables");
  const before = buildD1SnapshotRowEncoding(input, "OLD");
  const after = buildD1SnapshotRowEncoding(input, "NEW");
  if (!isList(input.uniqueKeys)) throw new Error("Unique index metadata is required");
  for (const key of input.uniqueKeys) {
    if (
      key.partial !== false ||
      !isList(key.columns) ||
      key.columns.length === 0 ||
      key.columns.some(
        (column) =>
          !input.columns.some(({ name }) => name === column.name) ||
          !["BINARY", "NOCASE", "RTRIM"].includes(column.collation.toUpperCase()),
      )
    )
      throw new Error("Unsupported unique index for change capture");
  }
  const schemaHash = hash(
    JSON.stringify({ table: input.table, columns: input.columns, uniqueKeys: input.uniqueKeys }),
  );
  const prefix = `__pc_keiba_catalog_cdc_${hash(input.table)}`;
  const changed: string = [
    `${before.rowId} IS NOT ${after.rowId}`,
    ...input.columns.map(
      ({ name }) =>
        `(OLD.${quote(name)} COLLATE BINARY IS NOT NEW.${quote(name)} COLLATE BINARY OR typeof(OLD.${quote(name)}) IS NOT typeof(NEW.${quote(name)}))`,
    ),
  ].join(" OR ");
  const variants = [
    {
      operation: "insert",
      event: "INSERT",
      condition: "",
      oldKey: "NULL",
      newKey: `CAST(${after.rowId} AS TEXT)`,
    },
    {
      operation: "update",
      event: "UPDATE",
      condition: ` WHEN ${changed}`,
      oldKey: `CAST(${before.rowId} AS TEXT)`,
      newKey: `CAST(${after.rowId} AS TEXT)`,
    },
    {
      operation: "delete",
      event: "DELETE",
      condition: "",
      oldKey: `CAST(${before.rowId} AS TEXT)`,
      newKey: "NULL",
    },
  ];
  const triggers = variants.map((variant) => {
    const name = `${prefix}_${variant.operation}`;
    const sql = `CREATE TRIGGER ${quote(name)} AFTER ${variant.event} ON ${quote(input.table)}${variant.condition} BEGIN INSERT INTO ${quote(D1_CAPTURE_TABLE)} (capture_id, table_name, schema_hash, operation, before_key, after_key) VALUES (${literal(input.captureId)}, ${literal(input.table)}, ${literal(schemaHash)}, ${literal(variant.operation)}, ${variant.oldKey}, ${variant.newKey}); END`;
    return { name, sql };
  });
  // REPLACE's implicit deletes need not fire DELETE triggers. Record candidate conflict keys
  // before either INSERT or UPDATE; IGNORE/no-op false positives are safe for fenced rehydration.
  const rowId = buildD1SnapshotRowEncoding(input).rowId;
  const conflicts = [
    `${rowId} = ${after.rowId}`,
    ...input.uniqueKeys.map(
      (key) =>
        `(${key.columns.map((column) => `${quote(column.name)} COLLATE ${quote(column.collation.toUpperCase())} = NEW.${quote(column.name)} COLLATE ${quote(column.collation.toUpperCase())}`).join(" AND ")})`,
    ),
  ];
  for (const event of ["INSERT", "UPDATE"]) {
    const name = `${prefix}_before_${event.toLowerCase()}`;
    const exclude = event === "UPDATE" ? ` AND ${rowId} IS NOT ${before.rowId}` : "";
    // Separate indexable probes: the live D1 planner scanned the whole source for OR.
    // UNION also deduplicates a row that conflicts through multiple unique keys.
    const candidates = conflicts
      .map(
        (condition) =>
          `SELECT ${rowId} AS candidate_key FROM ${quote(input.table)} WHERE ${condition}${exclude}`,
      )
      .join(" UNION ");
    triggers.push({
      name,
      sql: `CREATE TRIGGER ${quote(name)} BEFORE ${event} ON ${quote(input.table)} BEGIN INSERT INTO ${quote(D1_CAPTURE_TABLE)} (capture_id, table_name, schema_hash, operation, before_key, after_key) SELECT ${literal(input.captureId)}, ${literal(input.table)}, ${literal(schemaHash)}, 'touch', CAST(candidate_key AS TEXT), CAST(candidate_key AS TEXT) FROM (${candidates}); END`,
    });
  }
  if (triggers.some(({ sql }) => new TextEncoder().encode(sql).byteLength > 100000))
    throw new Error("Capture trigger exceeds D1 SQL byte limit");
  const createOutbox = `CREATE TABLE ${quote(D1_CAPTURE_TABLE)} (sequence INTEGER PRIMARY KEY AUTOINCREMENT, capture_id TEXT NOT NULL, table_name TEXT NOT NULL, schema_hash TEXT NOT NULL, operation TEXT NOT NULL CHECK (operation IN ('insert', 'update', 'delete', 'touch')), before_key TEXT, after_key TEXT, captured_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')))`;
  return {
    schemaHash,
    triggers,
    createOutbox,
    planHash: hash(JSON.stringify({ createOutbox, triggers })),
  };
};
