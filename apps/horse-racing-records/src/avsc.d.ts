// Run with: bun
// Type declarations for avsc library (no bundled types)

declare module "avsc" {
  import type { Readable, Writable, Transform } from "node:stream";

  interface AvroSchema {
    readonly type: string;
    readonly name?: string;
    readonly fields?: ReadonlyArray<AvroFieldSchema>;
    readonly items?: string | AvroSchema;
    readonly values?: string;
    readonly default?: unknown;
  }

  interface AvroFieldSchema {
    readonly name: string;
    readonly type: string | AvroSchema | ReadonlyArray<string | AvroSchema>;
    readonly default?: unknown;
  }

  interface AvroType {
    toBuffer: (val: unknown) => Buffer;
    fromBuffer: (buf: Buffer) => unknown;
    isValid: (val: unknown) => boolean;
  }

  interface BlockDecoderInstance extends Transform {
    on(event: "data", listener: (val: unknown) => void): this;
    on(event: "end", listener: () => void): this;
    on(event: "error", listener: (err: Error) => void): this;
    end(buf?: Buffer | Uint8Array): this;
  }

  interface BlockEncoderInstance extends Transform {
    on(event: "data", listener: (chunk: Buffer) => void): this;
    on(event: "end", listener: () => void): this;
    on(event: "error", listener: (err: Error) => void): this;
    write(val: unknown): boolean;
    end(): this;
  }

  const Type: {
    forSchema(schema: AvroSchema | string): AvroType;
  };

  interface BlockDecoderOptions {
    readonly writerType?: AvroType;
    readonly readerType?: AvroType;
    readonly noDecode?: boolean;
  }

  const streams: {
    BlockDecoder: new (opts?: BlockDecoderOptions) => BlockDecoderInstance;
    BlockEncoder: new (type: AvroType) => BlockEncoderInstance;
    RawDecoder: new (type: AvroType) => Transform;
    RawEncoder: new (type: AvroType) => Transform;
  };

  function createFileDecoder(path: string): Readable;
  function createFileEncoder(path: string, schema: AvroType): Writable;
  function parse(schema: AvroSchema | string): AvroType;

  export { Type, streams, createFileDecoder, createFileEncoder, parse };
  export type { AvroSchema, AvroFieldSchema, AvroType, BlockDecoderInstance, BlockEncoderInstance };
}
