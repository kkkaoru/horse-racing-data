// Runs with bun; crash-safe capture publication steps. The caller owns the exclusive writer lock.
import { prepareD1CaptureBatch, type D1CaptureBatchArtifact } from "./d1-capture-batch";
import { buildD1CapturePageQuery, type D1CaptureRegistration } from "./d1-capture-reader";

export interface D1CaptureExportConfig {
  databaseName: string;
  databaseId: string;
  registrations: readonly D1CaptureRegistration[];
}
export interface D1CaptureExportState {
  databaseName: string;
  databaseId: string;
  afterSequence: string;
  pending: D1CaptureBatchArtifact | null;
}
export interface D1CaptureLoadedRange {
  throughSequence: string;
  rows: readonly Record<string, unknown>[];
}
export interface D1CaptureExportDependencies {
  load: (afterSequence: string) => Promise<D1CaptureLoadedRange>;
  retainArtifact: (artifact: D1CaptureBatchArtifact) => Promise<void>;
  checkpoint: (state: D1CaptureExportState) => Promise<void>;
  publish: (artifact: D1CaptureBatchArtifact) => Promise<unknown>;
}
export interface D1CaptureExportInput {
  config: D1CaptureExportConfig;
  state: D1CaptureExportState;
  pageSize: number;
  dependencies: D1CaptureExportDependencies;
}
const MAX_PAGE_SIZE: number = 1000;
const MAX_ARTIFACT_BYTES: number = 1048576;
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const list = (value: unknown): value is readonly unknown[] => Array.isArray(value);

const rawEvent = (value: unknown): Record<string, unknown> => {
  if (!record(value)) throw new Error("Invalid pending capture event");
  return {
    sequence: value.sequence,
    capture_id: value.captureId,
    table_name: value.table,
    schema_hash: value.schemaHash,
    operation: value.operation,
    before_key: value.beforeKey,
    after_key: value.afterKey,
    captured_at: value.capturedAt,
  };
};

/** Rebuild canonical bytes to reject checkpoint edits, wrong databases and advanced pending cursors. */
export const parseD1CaptureExportState = (
  value: unknown,
  config: D1CaptureExportConfig,
): D1CaptureExportState => {
  if (
    !record(value) ||
    value.databaseId !== config.databaseId ||
    value.databaseName !== config.databaseName ||
    typeof value.afterSequence !== "string"
  )
    throw new Error("Invalid capture checkpoint");
  prepareD1CaptureBatch({
    ...config,
    rows: [],
    request: {
      afterSequence: value.afterSequence,
      throughSequence: value.afterSequence,
      limit: MAX_PAGE_SIZE,
    },
  });
  if (value.pending === null)
    return {
      databaseName: config.databaseName,
      databaseId: config.databaseId,
      afterSequence: value.afterSequence,
      pending: null,
    };
  const pending = value.pending;
  if (
    !record(pending) ||
    typeof pending.serialized !== "string" ||
    typeof pending.lastSequence !== "string" ||
    new TextEncoder().encode(pending.serialized).byteLength > MAX_ARTIFACT_BYTES
  )
    throw new Error("Invalid pending capture checkpoint");
  const document: unknown = JSON.parse(pending.serialized);
  if (
    !record(document) ||
    document.formatVersion !== 1 ||
    document.databaseName !== config.databaseName ||
    document.databaseId !== config.databaseId ||
    !list(document.events)
  )
    throw new Error("Pending capture database mismatch");
  const rebuilt = prepareD1CaptureBatch({
    ...config,
    rows: document.events.map(rawEvent),
    request: {
      afterSequence: value.afterSequence,
      throughSequence: pending.lastSequence,
      limit: MAX_PAGE_SIZE,
    },
  });
  if (
    rebuilt === null ||
    rebuilt.serialized !== pending.serialized ||
    rebuilt.batchId !== pending.batchId ||
    rebuilt.lastSequence !== pending.lastSequence ||
    rebuilt.eventCount !== pending.eventCount
  )
    throw new Error("Pending capture artifact mismatch");
  return {
    databaseName: config.databaseName,
    databaseId: config.databaseId,
    afterSequence: value.afterSequence,
    pending: rebuilt,
  };
};

const acknowledge = async (
  input: D1CaptureExportInput,
  artifact: D1CaptureBatchArtifact,
): Promise<D1CaptureExportState> => {
  const receipt = await input.dependencies.publish(artifact);
  if (
    !record(receipt) ||
    receipt.reconciled !== true ||
    receipt.batchId !== artifact.batchId ||
    receipt.lastSequence !== artifact.lastSequence ||
    receipt.eventCount !== artifact.eventCount
  )
    throw new Error("Capture publication receipt mismatch");
  const next: D1CaptureExportState = {
    databaseName: input.config.databaseName,
    databaseId: input.config.databaseId,
    afterSequence: artifact.lastSequence,
    pending: null,
  };
  await input.dependencies.checkpoint(next);
  return next;
};

export const exportD1CaptureStep = async (
  input: D1CaptureExportInput,
): Promise<D1CaptureExportState> => {
  const state = parseD1CaptureExportState(input.state, input.config);
  buildD1CapturePageQuery({
    afterSequence: state.afterSequence,
    throughSequence: state.afterSequence,
    limit: input.pageSize,
  });
  // Never regenerate pending input from a moving source, including after an uncertain commit.
  if (state.pending !== null) return acknowledge(input, state.pending);
  const range = await input.dependencies.load(state.afterSequence);
  const artifact = prepareD1CaptureBatch({
    ...input.config,
    rows: range.rows,
    request: {
      afterSequence: state.afterSequence,
      throughSequence: range.throughSequence,
      limit: input.pageSize,
    },
  });
  if (artifact === null) return state;
  await input.dependencies.retainArtifact(artifact);
  await input.dependencies.checkpoint({ ...state, pending: artifact });
  return acknowledge(input, artifact);
};
