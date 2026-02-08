// Run with: bun
// Avro read/write for Iceberg manifest files

import avro from "avsc";

const CONTENT_DATA = 0;
const CONTENT_EQUALITY_DELETES = 2;
const STATUS_ADDED = 1;
const FILE_FORMAT_PARQUET = "PARQUET";

interface ManifestEntry {
  readonly status: number;
  readonly snapshot_id: number;
  readonly sequence_number: number;
  readonly data_file: DataFile;
}

interface DataFile {
  readonly content: number;
  readonly file_path: string;
  readonly file_format: string;
  readonly record_count: number;
  readonly file_size_in_bytes: number;
  readonly equality_ids?: ReadonlyArray<number> | null;
  readonly partition: Record<string, unknown>;
}

const MANIFEST_ENTRY_AVRO_SCHEMA = avro.Type.forSchema({
  type: "record",
  name: "manifest_entry",
  fields: [
    { name: "status", type: "int" },
    { name: "snapshot_id", type: "long" },
    { name: "sequence_number", type: "long" },
    {
      name: "data_file",
      type: {
        type: "record",
        name: "r2",
        fields: [
          { name: "content", type: "int" },
          { name: "file_path", type: "string" },
          { name: "file_format", type: "string" },
          { name: "record_count", type: "long" },
          { name: "file_size_in_bytes", type: "long" },
          {
            name: "equality_ids",
            type: ["null", { type: "array", items: "int" }],
            default: null,
          },
          {
            name: "partition",
            type: { type: "map", values: "string" },
            default: {},
          },
        ],
      },
    },
  ],
});

interface BuildDeleteManifestEntryArgs {
  readonly snapshotId: number;
  readonly sequenceNumber: number;
  readonly filePath: string;
  readonly fileSize: number;
  readonly recordCount: number;
  readonly equalityFieldIds: ReadonlyArray<number>;
}

const buildDeleteManifestEntry = (args: BuildDeleteManifestEntryArgs): ManifestEntry => ({
  status: STATUS_ADDED,
  snapshot_id: args.snapshotId,
  sequence_number: args.sequenceNumber,
  data_file: {
    content: CONTENT_EQUALITY_DELETES,
    file_path: args.filePath,
    file_format: FILE_FORMAT_PARQUET,
    record_count: args.recordCount,
    file_size_in_bytes: args.fileSize,
    equality_ids: [...args.equalityFieldIds],
    partition: {},
  },
});

const parseManifest = (buffer: Buffer): Promise<ReadonlyArray<ManifestEntry>> =>
  new Promise<ReadonlyArray<ManifestEntry>>((resolve, reject) => {
    const results: Array<ManifestEntry> = [];
    const decoder = new avro.streams.BlockDecoder();
    decoder.on("data", (val: unknown) => {
      results.push(val as ManifestEntry);
    });
    decoder.on("end", () => {
      resolve(results);
    });
    decoder.on("error", (err: Error) => {
      reject(err);
    });
    decoder.end(buffer);
  });

const serializeManifest = (entries: ReadonlyArray<ManifestEntry>): Promise<Buffer> =>
  new Promise<Buffer>((resolve, reject) => {
    const encoder = new avro.streams.BlockEncoder(MANIFEST_ENTRY_AVRO_SCHEMA);
    const chunks: Array<Buffer> = [];
    encoder.on("data", (chunk: Buffer) => {
      chunks.push(chunk);
    });
    encoder.on("end", () => {
      resolve(Buffer.concat(chunks));
    });
    encoder.on("error", (err: Error) => {
      reject(err);
    });
    entries.forEach((entry) => encoder.write(entry));
    encoder.end();
  });

export {
  buildDeleteManifestEntry,
  parseManifest,
  serializeManifest,
  MANIFEST_ENTRY_AVRO_SCHEMA,
  CONTENT_DATA,
  CONTENT_EQUALITY_DELETES,
  STATUS_ADDED,
  FILE_FORMAT_PARQUET,
};
export type { ManifestEntry, DataFile, BuildDeleteManifestEntryArgs };
