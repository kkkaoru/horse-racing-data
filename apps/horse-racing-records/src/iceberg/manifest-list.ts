// Run with: bun
// Avro read/write for Iceberg manifest list files

import avro from "avsc";

interface ManifestListEntry {
  readonly manifest_path: string;
  readonly manifest_length: number;
  readonly partition_spec_id: number;
  readonly content: number;
  readonly sequence_number: number;
  readonly min_sequence_number: number;
  readonly added_snapshot_id: number;
  readonly added_data_files_count: number;
  readonly existing_data_files_count: number;
  readonly deleted_data_files_count: number;
  readonly added_rows_count: number;
  readonly existing_rows_count: number;
  readonly deleted_rows_count: number;
}

const MANIFEST_LIST_AVRO_SCHEMA = avro.Type.forSchema({
  type: "record",
  name: "manifest_file",
  fields: [
    { name: "manifest_path", type: "string" },
    { name: "manifest_length", type: "long" },
    { name: "partition_spec_id", type: "int" },
    { name: "content", type: "int" },
    { name: "sequence_number", type: "long" },
    { name: "min_sequence_number", type: "long" },
    { name: "added_snapshot_id", type: "long" },
    { name: "added_data_files_count", type: "int", default: 0 },
    { name: "existing_data_files_count", type: "int", default: 0 },
    { name: "deleted_data_files_count", type: "int", default: 0 },
    { name: "added_rows_count", type: "long", default: 0 },
    { name: "existing_rows_count", type: "long", default: 0 },
    { name: "deleted_rows_count", type: "long", default: 0 },
  ],
});

const parseManifestList = (buffer: Buffer): Promise<ReadonlyArray<ManifestListEntry>> =>
  new Promise<ReadonlyArray<ManifestListEntry>>((resolve, reject) => {
    const results: Array<ManifestListEntry> = [];
    const decoder = new avro.streams.BlockDecoder();
    decoder.on("data", (val: unknown) => {
      results.push(val as ManifestListEntry);
    });
    decoder.on("end", () => {
      resolve(results);
    });
    decoder.on("error", (err: Error) => {
      reject(err);
    });
    decoder.end(buffer);
  });

const serializeManifestList = (entries: ReadonlyArray<ManifestListEntry>): Promise<Buffer> =>
  new Promise<Buffer>((resolve, reject) => {
    const encoder = new avro.streams.BlockEncoder(MANIFEST_LIST_AVRO_SCHEMA);
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

export { parseManifestList, serializeManifestList, MANIFEST_LIST_AVRO_SCHEMA };
export type { ManifestListEntry };
