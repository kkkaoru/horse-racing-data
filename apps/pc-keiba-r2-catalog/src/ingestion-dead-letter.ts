// Runs with bun; archive failed Queue inputs only, never execute their business jobs.
import { retainTrackedDeadLetter } from "./ingestion-archive-attempt";
import { INGESTION_DLQ_NAME } from "./ingestion-dead-letter-envelope";
import { acceptIngestion } from "./ingestion-service";
export {
  deadLetterEnvelope,
  deadLetterRequestId,
  INGESTION_DLQ_NAME,
} from "./ingestion-dead-letter-envelope";

const RETRY_SECONDS: number = 86400;
export const handleIngestionDeadLetters = async (
  batch: MessageBatch<unknown>,
  env: Pick<CatalogBindings, "CATALOG_OBJECTS" | "INGESTION_INBOX" | "INGESTION_ARCHIVE_JOURNAL">,
): Promise<void> => {
  if (batch.queue !== INGESTION_DLQ_NAME) throw new Error("Unexpected ingestion dead-letter queue");
  const journal = env.INGESTION_ARCHIVE_JOURNAL.getByName(INGESTION_DLQ_NAME);
  await Promise.all(
    batch.messages.map(async (message) => {
      try {
        await retainTrackedDeadLetter(message, {
          begin: async (identity) => {
            await journal.begin(identity);
          },
          accept: async (envelope) => await acceptIngestion(envelope, env),
          recordReceipt: async (receipt) => {
            await journal.recordReceipt(receipt);
          },
        });
        message.ack();
      } catch {
        message.retry({ delaySeconds: RETRY_SECONDS });
        console.error(
          JSON.stringify({
            event: "ingestion_dlq_archive_failed",
            queue: batch.queue,
            messageId: message.id,
          }),
        );
      }
    }),
  );
};
