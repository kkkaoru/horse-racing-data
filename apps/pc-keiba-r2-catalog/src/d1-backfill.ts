// Runs with bun; crash-safe orchestration for D1-to-Iceberg snapshot migration.
import type { D1SnapshotPageRow } from "./d1-snapshot";

export interface D1BackfillIdentity {
  snapshotId: string;
  databaseName: string;
  tableName: string;
}

export interface D1BatchArtifact {
  path: string;
  digest: string;
}

export interface D1PendingBatch extends D1BatchArtifact {
  lastRowId: string;
  rows: number;
}

export interface D1BackfillState extends D1BackfillIdentity {
  afterRowId: string | null;
  copiedRows: number;
  pending: D1PendingBatch | null;
  phase: "copying" | "copied";
}

export interface D1SnapshotBatchDocument {
  snapshot_id: string;
  database_name: string;
  table_name: string;
  rows: D1SnapshotPageRow[];
}

export interface D1BackfillDependencies {
  read(afterRowId: string | null, limit: number): Promise<D1SnapshotPageRow[]>;
  stage(batch: D1SnapshotBatchDocument): Promise<D1BatchArtifact>;
  publish(artifact: D1BatchArtifact): Promise<{ rows: number; reconciled: boolean }>;
  checkpoint(state: D1BackfillState): Promise<void>;
}

export interface D1BackfillStep {
  state: D1BackfillState;
  dependencies: D1BackfillDependencies;
  batchRows: number;
  batchBytes: number;
  pageRows: number;
}

const MAX_BATCH_ROWS: number = 50_000;
const MAX_BATCH_BYTES: number = 64 * 1024 * 1024;
const MAX_PAGE_ROWS: number = 1000;
const MIN_CURSOR: bigint = -9223372036854775809n;
const encoder: TextEncoder = new TextEncoder();

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const rowIdText = (value: unknown): value is string =>
  typeof value === "string" &&
  /^-?\d+$/u.test(value) &&
  BigInt(value) > MIN_CURSOR &&
  BigInt(value) <= 9223372036854775807n;

const parsePending = (value: unknown): D1PendingBatch | null => {
  if (value === null) return null;
  if (
    !isRecord(value) ||
    typeof value.path !== "string" ||
    value.path.length === 0 ||
    typeof value.digest !== "string" ||
    !/^[a-f0-9]{64}$/u.test(value.digest) ||
    !rowIdText(value.lastRowId) ||
    typeof value.rows !== "number" ||
    !Number.isSafeInteger(value.rows) ||
    value.rows < 1 ||
    value.rows > MAX_BATCH_ROWS
  )
    throw new Error("Invalid pending D1 batch checkpoint");
  return { path: value.path, digest: value.digest, lastRowId: value.lastRowId, rows: value.rows };
};

export const parseD1BackfillState = (
  value: unknown,
  identity: D1BackfillIdentity,
): D1BackfillState => {
  if (
    !isRecord(value) ||
    value.snapshotId !== identity.snapshotId ||
    value.databaseName !== identity.databaseName ||
    value.tableName !== identity.tableName ||
    (value.afterRowId !== null && !rowIdText(value.afterRowId)) ||
    typeof value.copiedRows !== "number" ||
    !Number.isSafeInteger(value.copiedRows) ||
    value.copiedRows < 0 ||
    (value.phase !== "copying" && value.phase !== "copied")
  )
    throw new Error("Invalid D1 backfill checkpoint");
  if ((value.afterRowId === null) !== (value.copiedRows === 0))
    throw new Error("D1 checkpoint cursor and row count disagree");
  const pending: D1PendingBatch | null = parsePending(value.pending);
  if (
    pending !== null &&
    value.afterRowId !== null &&
    BigInt(pending.lastRowId) <= BigInt(value.afterRowId)
  )
    throw new Error("Pending D1 checkpoint cursor did not advance");
  if (value.phase === "copied" && pending !== null)
    throw new Error("Completed D1 checkpoint still has pending data");
  return {
    ...identity,
    afterRowId: value.afterRowId,
    copiedRows: value.copiedRows,
    phase: value.phase,
    pending,
  };
};

const batchDocument = (state: D1BackfillState): D1SnapshotBatchDocument => ({
  snapshot_id: state.snapshotId,
  database_name: state.databaseName,
  table_name: state.tableName,
  rows: [],
});

const captureBatch = async (input: D1BackfillStep): Promise<D1SnapshotBatchDocument> => {
  const batch: D1SnapshotBatchDocument = batchDocument(input.state);
  const progress = {
    after: input.state.afterRowId,
    bytes: encoder.encode(JSON.stringify(batch)).byteLength,
    full: false,
  };
  while (batch.rows.length < input.batchRows && !progress.full) {
    const limit: number = Math.min(input.pageRows, input.batchRows - batch.rows.length);
    const page: D1SnapshotPageRow[] = await input.dependencies.read(progress.after, limit);
    if (page.length > limit) throw new Error("D1 backfill source exceeded page limit");
    if (page.length === 0) break;
    for (const row of page) {
      const previous: bigint = progress.after === null ? MIN_CURSOR : BigInt(progress.after);
      if (BigInt(row.row_key) <= previous) throw new Error("D1 backfill cursor did not advance");
      const bytes: number = encoder.encode(JSON.stringify(row)).byteLength + 1;
      if (progress.bytes + bytes > input.batchBytes) {
        if (batch.rows.length === 0) throw new Error("D1 snapshot row exceeds batch byte limit");
        progress.full = true;
        break;
      }
      batch.rows.push(row);
      progress.after = row.row_key;
      progress.bytes += bytes;
    }
  }
  return batch;
};

const finishPendingBatch = async (
  input: D1BackfillStep,
  pending: D1PendingBatch,
): Promise<D1BackfillState> => {
  const result = await input.dependencies.publish({ path: pending.path, digest: pending.digest });
  if (!result.reconciled || result.rows !== pending.rows)
    throw new Error("D1 Catalog batch reconciliation did not match checkpoint");
  const next: D1BackfillState = {
    ...input.state,
    afterRowId: pending.lastRowId,
    copiedRows: input.state.copiedRows + pending.rows,
    pending: null,
  };
  await input.dependencies.checkpoint(next);
  return next;
};

export const copyD1BackfillStep = async (input: D1BackfillStep): Promise<D1BackfillState> => {
  if (
    !Number.isInteger(input.batchRows) ||
    input.batchRows < 1 ||
    input.batchRows > MAX_BATCH_ROWS ||
    !Number.isInteger(input.batchBytes) ||
    input.batchBytes < 1 ||
    input.batchBytes > MAX_BATCH_BYTES ||
    !Number.isInteger(input.pageRows) ||
    input.pageRows < 1 ||
    input.pageRows > MAX_PAGE_ROWS
  )
    throw new Error("Invalid D1 backfill batch configuration");
  if (input.state.phase === "copied") return input.state;
  if (input.state.pending !== null) return await finishPendingBatch(input, input.state.pending);
  const batch: D1SnapshotBatchDocument = await captureBatch(input);
  const last: D1SnapshotPageRow | undefined = batch.rows.at(-1);
  if (last === undefined) {
    const next: D1BackfillState = { ...input.state, phase: "copied" };
    await input.dependencies.checkpoint(next);
    return next;
  }
  const artifact: D1BatchArtifact = await input.dependencies.stage(batch);
  const pending: D1PendingBatch = { ...artifact, lastRowId: last.row_key, rows: batch.rows.length };
  const staged: D1BackfillState = { ...input.state, pending };
  // Crash before or after external commit resumes this exact artifact, never mutable source rows.
  await input.dependencies.checkpoint(staged);
  return await finishPendingBatch({ ...input, state: staged }, pending);
};
