// Runs with bun; lossless decoded Queue envelope preparation, without runtime service bindings.
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import type { IngestionEnvelope } from "./ingestion-buffer";

interface DeadLetterPayload {
  formatVersion: 1;
  kind: "dead-letter";
  queue: string;
  messageId: string;
  queuedAt: string;
  body: unknown;
}
export const INGESTION_DLQ_NAME: string = "sync-realtime-data-hot-ingestion-dlq";
const SOURCE: string = "sync-realtime-data-hot-v2";
export const deadLetterRequestId = (messageId: unknown): string => {
  if (typeof messageId !== "string" || messageId.length === 0 || messageId.length > 128)
    throw new Error("Invalid dead-letter message identity");
  return `dlq_${createHash("sha256")
    .update(JSON.stringify([INGESTION_DLQ_NAME, messageId]))
    .digest("hex")}`;
};
export const deadLetterEnvelope = (
  message: Pick<Message<unknown>, "id" | "timestamp" | "body">,
): IngestionEnvelope => {
  const requestId: string = deadLetterRequestId(message.id);
  const payload: DeadLetterPayload = {
    formatVersion: 1,
    kind: "dead-letter",
    queue: INGESTION_DLQ_NAME,
    messageId: message.id,
    queuedAt: message.timestamp.toISOString(),
    body: message.body,
  };
  const serialized: string = JSON.stringify(payload);
  const restored: unknown = JSON.parse(serialized);
  if (!isDeepStrictEqual(restored, payload))
    throw new Error("Dead-letter body requires lossless transport encoding");
  return { source: SOURCE, requestId, payload: serialized };
};
