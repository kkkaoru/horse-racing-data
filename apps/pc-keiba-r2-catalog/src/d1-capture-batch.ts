// Runs with bun; immutable event artifacts precede publication and checkpoint advancement.
import { createHash } from "node:crypto";
import {
  parseD1CapturePage,
  type D1CaptureEvent,
  type D1CapturePageInput,
} from "./d1-capture-reader";

export interface D1CaptureBatchInput extends D1CapturePageInput {
  databaseName: string;
  databaseId: string;
}
export interface D1CaptureBatchContent {
  formatVersion: 1;
  databaseName: string;
  databaseId: string;
  events: readonly D1CaptureEvent[];
}
export interface D1CaptureBatchArtifact {
  batchId: string;
  serialized: string;
  lastSequence: string;
  eventCount: number;
}
const DATABASE_NAME: RegExp = /^[a-zA-Z0-9_-]{1,64}$/u;
const DATABASE_ID: RegExp = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/u;
const MAX_BATCH_BYTES: number = 1048576;
const DATABASE_ID_LENGTH: number = 36;

/** One writer per database; consumers must retain and replay these exact bytes on uncertain acknowledgements. */
export const prepareD1CaptureBatch = (
  input: D1CaptureBatchInput,
): D1CaptureBatchArtifact | null => {
  if (
    !DATABASE_NAME.test(input.databaseName) ||
    input.databaseName.trim() !== input.databaseName ||
    !DATABASE_ID.test(input.databaseId) ||
    input.databaseId.length !== DATABASE_ID_LENGTH
  )
    throw new Error("Invalid capture database identity");
  const page = parseD1CapturePage(input);
  if (page.events.length === 0) return null;
  const content: D1CaptureBatchContent = {
    formatVersion: 1,
    databaseName: input.databaseName,
    databaseId: input.databaseId,
    events: page.events,
  };
  // Fixed field order, UTF-8 JSON, no whitespace. Cursor bounds are not content identity:
  // the same events may be retried after more events have arrived above the original bound.
  const batchId: string = createHash("sha256").update(JSON.stringify(content)).digest("hex");
  const serialized: string = JSON.stringify({ ...content, batchId });
  if (new TextEncoder().encode(serialized).byteLength > MAX_BATCH_BYTES)
    throw new Error(
      "Capture artifact exceeds byte limit; reduce page size before persisting intent",
    );
  return { batchId, serialized, lastSequence: page.lastSequence, eventCount: page.events.length };
};
