// Runs with bun; resumable offline backfill orchestration with injected I/O.
import {
  cornerMigrationCursor,
  cornerMigrationParameters,
  mapCornerMigrationRow,
  type CornerMigrationCursor,
} from "./vector-migration";

export interface VectorBackfillCheckpoint {
  namespace: string;
  cursor: CornerMigrationCursor;
  submittedRows: number;
  lastMutationId: string | null;
  phase: "submitting" | "submitted";
}

export interface VectorBackfillDependencies {
  load(parameters: readonly (string | number)[]): Promise<Record<string, unknown>[]>;
  upsert(vectors: VectorizeVector[]): Promise<VectorizeAsyncMutation>;
  checkpoint(value: VectorBackfillCheckpoint): Promise<void>;
}

export interface VectorBackfillBatch {
  batchSize: number;
  state: VectorBackfillCheckpoint;
  dependencies: VectorBackfillDependencies;
}

const MAX_BATCH_SIZE: number = 500;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const parseVectorBackfillCheckpoint = (
  value: unknown,
  namespace: string,
): VectorBackfillCheckpoint => {
  if (
    !isRecord(value) ||
    value.namespace !== namespace ||
    !isRecord(value.cursor) ||
    typeof value.submittedRows !== "number" ||
    !Number.isSafeInteger(value.submittedRows) ||
    value.submittedRows < 0 ||
    (value.lastMutationId !== null && typeof value.lastMutationId !== "string") ||
    (value.phase !== "submitting" && value.phase !== "submitted")
  )
    throw new Error("Invalid vector backfill checkpoint");
  const cursor = value.cursor;
  if (
    typeof cursor.source !== "string" ||
    typeof cursor.year !== "string" ||
    typeof cursor.monthDay !== "string" ||
    typeof cursor.venue !== "string" ||
    typeof cursor.race !== "string" ||
    typeof cursor.horse !== "string"
  )
    throw new Error("Invalid vector backfill cursor");
  return {
    namespace,
    cursor: {
      source: cursor.source,
      year: cursor.year,
      monthDay: cursor.monthDay,
      venue: cursor.venue,
      race: cursor.race,
      horse: cursor.horse,
    },
    submittedRows: value.submittedRows,
    lastMutationId: value.lastMutationId,
    phase: value.phase,
  };
};

export const submitVectorBackfillBatch = async (
  input: VectorBackfillBatch,
): Promise<VectorBackfillCheckpoint> => {
  if (!Number.isInteger(input.batchSize) || input.batchSize < 1 || input.batchSize > MAX_BATCH_SIZE)
    throw new Error("Invalid vector backfill batch size");
  if (input.state.phase === "submitted") return input.state;
  const rows: Record<string, unknown>[] = await input.dependencies.load([
    ...cornerMigrationParameters(input.state.cursor),
    input.batchSize,
  ]);
  if (rows.length > input.batchSize)
    throw new Error("Source exceeded the vector migration batch limit");
  const last: Record<string, unknown> | undefined = rows.at(-1);
  if (last === undefined) {
    const completed: VectorBackfillCheckpoint = { ...input.state, phase: "submitted" };
    await input.dependencies.checkpoint(completed);
    return completed;
  }
  const vectors: VectorizeVector[] = await Promise.all(
    rows.map((row) => mapCornerMigrationRow(row, input.state.namespace)),
  );
  const receipt: VectorizeAsyncMutation = await input.dependencies.upsert(vectors);
  if (!receipt.mutationId) throw new Error("Missing Vectorize backfill mutation receipt");
  const next: VectorBackfillCheckpoint = {
    namespace: input.state.namespace,
    cursor: cornerMigrationCursor(last),
    submittedRows: input.state.submittedRows + rows.length,
    lastMutationId: receipt.mutationId,
    phase: "submitting",
  };
  // Advancing only after acceptance makes crashes replay the same deterministic identifiers.
  await input.dependencies.checkpoint(next);
  return next;
};
