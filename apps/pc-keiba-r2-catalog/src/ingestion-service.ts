// Runs with bun; named service-binding RPC only, not a public HTTP ingestion route.
import { WorkerEntrypoint } from "cloudflare:workers";
import {
  bufferIngestion,
  type IngestionAcceptance,
  type IngestionEnvelope,
} from "./ingestion-buffer";

const PILOT_SOURCE: string = "sync-realtime-data-hot-v2";
const envelope = (value: unknown): IngestionEnvelope => {
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value) ||
    !("source" in value) ||
    value.source !== PILOT_SOURCE ||
    !("requestId" in value) ||
    typeof value.requestId !== "string" ||
    !("payload" in value) ||
    typeof value.payload !== "string"
  )
    throw new Error("Invalid ingestion service envelope");
  return { source: value.source, requestId: value.requestId, payload: value.payload };
};

export const acceptIngestion = async (
  value: unknown,
  env: Pick<CatalogBindings, "CATALOG_OBJECTS" | "INGESTION_INBOX">,
): Promise<IngestionAcceptance> =>
  await bufferIngestion(envelope(value), {
    bucket: env.CATALOG_OBJECTS,
    accept: async (pointer) => {
      using result = await env.INGESTION_INBOX.getByName(pointer.source).accept(pointer);
      return {
        source: result.source,
        requestId: result.requestId,
        digest: result.digest,
        sequence: result.sequence,
        accepted: result.accepted,
      } satisfies IngestionAcceptance;
    },
  });

/** Binding holders are trusted callers. Acceptance is storage only, never delivery or routing permission. */
export class IngestionBufferService extends WorkerEntrypoint<
  Pick<CatalogBindings, "CATALOG_OBJECTS" | "INGESTION_INBOX">
> {
  async accept(value: unknown): Promise<IngestionAcceptance> {
    return await acceptIngestion(value, this.env);
  }
}
