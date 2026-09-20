// Runs with bun; unbound archival journal protocol, never acknowledges Queue messages itself.
import type { IngestionAcceptance, IngestionEnvelope } from "./ingestion-buffer";
import { deadLetterEnvelope, deadLetterRequestId } from "./ingestion-dead-letter-envelope";

export interface ArchiveAttemptIdentity {
  requestId: string;
  queuedAt: string;
}
export interface ArchiveAttemptPorts {
  begin: (identity: ArchiveAttemptIdentity) => Promise<void>;
  accept: (envelope: IngestionEnvelope) => Promise<IngestionAcceptance>;
  recordReceipt: (receipt: IngestionAcceptance) => Promise<void>;
}

/** Ports must durably/idempotently journal begin and receipt; accept must verify R2 and inbox identity. */
export const retainTrackedDeadLetter = async (
  message: Pick<Message<unknown>, "id" | "timestamp" | "body">,
  ports: ArchiveAttemptPorts,
): Promise<IngestionAcceptance> => {
  const identity: ArchiveAttemptIdentity = {
    requestId: deadLetterRequestId(message.id),
    queuedAt: message.timestamp.toISOString(),
  };
  // Track before body conversion: unsupported bodies remain known pending, not silently "healthy".
  await ports.begin(identity);
  const receipt: IngestionAcceptance = await ports.accept(deadLetterEnvelope(message));
  await ports.recordReceipt(receipt);
  return receipt;
};
