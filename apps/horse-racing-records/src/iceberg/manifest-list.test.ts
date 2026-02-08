// Run with: bun
// Tests for iceberg/manifest-list.ts

import { describe, it, expect } from "vitest";
import { serializeManifestList, parseManifestList } from "./manifest-list.ts";
import type { ManifestListEntry } from "./manifest-list.ts";

const createTestEntry = (): ManifestListEntry => ({
  manifest_path: "s3://bucket/metadata/manifest-12345.avro",
  manifest_length: 2048,
  partition_spec_id: 0,
  content: 1,
  sequence_number: 5,
  min_sequence_number: 5,
  added_snapshot_id: 12345,
  added_data_files_count: 0,
  existing_data_files_count: 0,
  deleted_data_files_count: 1,
  added_rows_count: 0,
  existing_rows_count: 0,
  deleted_rows_count: 10,
});

describe("serializeManifestList and parseManifestList roundtrip", () => {
  it("should serialize and parse a single entry", async () => {
    const entry = createTestEntry();
    const serialized = await serializeManifestList([entry]);
    expect(serialized.byteLength > 0).toStrictEqual(true);

    const parsed = await parseManifestList(serialized);
    expect(parsed.length).toStrictEqual(1);

    const parsedEntry = parsed[0];
    expect(parsedEntry?.manifest_path).toStrictEqual("s3://bucket/metadata/manifest-12345.avro");
    expect(parsedEntry?.manifest_length).toStrictEqual(2048);
    expect(parsedEntry?.partition_spec_id).toStrictEqual(0);
    expect(parsedEntry?.content).toStrictEqual(1);
    expect(parsedEntry?.sequence_number).toStrictEqual(5);
    expect(parsedEntry?.added_snapshot_id).toStrictEqual(12345);
    expect(parsedEntry?.deleted_data_files_count).toStrictEqual(1);
    expect(parsedEntry?.deleted_rows_count).toStrictEqual(10);
  });

  it("should serialize and parse multiple entries", async () => {
    const entry1 = createTestEntry();
    const entry2: ManifestListEntry = {
      manifest_path: "s3://bucket/metadata/manifest-99999.avro",
      manifest_length: 4096,
      partition_spec_id: 0,
      content: 0,
      sequence_number: 4,
      min_sequence_number: 1,
      added_snapshot_id: 99999,
      added_data_files_count: 3,
      existing_data_files_count: 0,
      deleted_data_files_count: 0,
      added_rows_count: 100,
      existing_rows_count: 0,
      deleted_rows_count: 0,
    };

    const serialized = await serializeManifestList([entry1, entry2]);
    const parsed = await parseManifestList(serialized);

    expect(parsed.length).toStrictEqual(2);
    expect(parsed[0]?.manifest_path).toStrictEqual("s3://bucket/metadata/manifest-12345.avro");
    expect(parsed[1]?.manifest_path).toStrictEqual("s3://bucket/metadata/manifest-99999.avro");
    expect(parsed[1]?.added_data_files_count).toStrictEqual(3);
    expect(parsed[1]?.added_rows_count).toStrictEqual(100);
  });

  it("should produce non-empty buffer", async () => {
    const entry = createTestEntry();
    const serialized = await serializeManifestList([entry]);
    expect(serialized.byteLength > 10).toStrictEqual(true);
  });
});
