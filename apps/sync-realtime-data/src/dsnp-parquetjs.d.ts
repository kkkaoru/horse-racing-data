declare module "@dsnp/parquetjs" {
  import type { Writable } from "node:stream";

  export class ParquetSchema {
    constructor(schema: Record<string, Record<string, unknown>>);
  }

  export class ParquetWriter {
    static openStream(
      schema: ParquetSchema,
      stream: Writable,
      options?: Record<string, unknown>,
    ): Promise<ParquetWriter>;
    appendRow(row: Record<string, unknown>): Promise<void>;
    close(): Promise<void>;
  }

  export class ParquetReader {
    static openBuffer(buffer: Buffer): Promise<ParquetReader>;
    getCursor(): { next(): Promise<Record<string, unknown> | null> };
    close(): Promise<void>;
  }
}
