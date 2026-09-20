// Runs with bun; private source-sharded receipt journal, called only after verified R2 retention.
import { DurableObject } from "cloudflare:workers";
import {
  parseIngestionPointer,
  type IngestionAcceptance,
  type IngestionPointer,
} from "./ingestion-buffer";

interface InboxRow extends Record<string, SqlStorageValue> {
  sequence: string;
  descriptor: string;
}
export interface IngestionInboxEntry {
  sequence: string;
  pointer: IngestionPointer;
}
const SOURCE_KEY: string = "ingestion-inbox/source-v1";
const MAX_STORAGE_BYTES: number = 8_000_000_000;
const MAX_SEQUENCE: bigint = 9223372036854775807n;
const SCHEMA: string =
  "CREATE TABLE IF NOT EXISTS __ingestion_inbox_v1 (sequence INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL UNIQUE, descriptor TEXT NOT NULL)";
const receipt = (entry: IngestionInboxEntry): IngestionAcceptance => ({
  source: entry.pointer.source,
  requestId: entry.pointer.requestId,
  digest: entry.pointer.digest,
  sequence: entry.sequence,
  accepted: true,
});

/** No delivery, deletion, routing promotion or lease-expiry inference is exposed by this journal. */
export class IngestionInbox extends DurableObject<unknown> {
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
    void this.ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(SCHEMA);
    });
  }

  accept(value: unknown): IngestionAcceptance {
    const pointer: IngestionPointer = parseIngestionPointer(value);
    const descriptor: string = JSON.stringify(pointer);
    return this.ctx.storage.transactionSync(() => {
      const source: string | undefined = this.ctx.storage.kv.get<string>(SOURCE_KEY);
      if (source !== undefined && source !== pointer.source)
        throw new Error("Ingestion inbox source mismatch");
      const previous = this.ctx.storage.sql
        .exec<InboxRow>(
          "SELECT CAST(sequence AS TEXT) AS sequence, descriptor FROM __ingestion_inbox_v1 WHERE request_id = ?",
          pointer.requestId,
        )
        .toArray()[0];
      if (previous !== undefined) {
        if (previous.descriptor !== descriptor)
          throw new Error("Ingestion request identifier reused with different content");
        return receipt({ sequence: previous.sequence, pointer });
      }
      if (this.ctx.storage.sql.databaseSize >= MAX_STORAGE_BYTES)
        throw new Error("Ingestion inbox storage budget reached");
      if (source === undefined) this.ctx.storage.kv.put(SOURCE_KEY, pointer.source);
      const inserted = this.ctx.storage.sql
        .exec<{ sequence: string }>(
          "INSERT INTO __ingestion_inbox_v1 (request_id, descriptor) VALUES (?, ?) RETURNING CAST(sequence AS TEXT) AS sequence",
          pointer.requestId,
          descriptor,
        )
        .one();
      return receipt({ sequence: inserted.sequence, pointer });
    });
  }

  entries(afterSequence: unknown, limit: unknown): IngestionInboxEntry[] {
    if (
      typeof afterSequence !== "string" ||
      !/^(?:0|[1-9]\d{0,18})$/u.test(afterSequence) ||
      BigInt(afterSequence).toString() !== afterSequence ||
      BigInt(afterSequence) > MAX_SEQUENCE ||
      typeof limit !== "number" ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 100
    )
      throw new Error("Invalid ingestion inbox cursor");
    const source: string | undefined = this.ctx.storage.kv.get<string>(SOURCE_KEY);
    return this.ctx.storage.sql
      .exec<InboxRow>(
        "SELECT CAST(sequence AS TEXT) AS sequence, descriptor FROM __ingestion_inbox_v1 WHERE sequence > CAST(? AS INTEGER) ORDER BY sequence LIMIT ?",
        afterSequence,
        limit,
      )
      .toArray()
      .map((row) => {
        const pointer: IngestionPointer = parseIngestionPointer(JSON.parse(row.descriptor));
        if (pointer.source !== source) throw new Error("Ingestion inbox stored source mismatch");
        return { sequence: row.sequence, pointer };
      });
  }
}
