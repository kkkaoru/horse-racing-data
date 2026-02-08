// Run with: bun
// Tests for iceberg/manifest.ts

import { describe, it, expect } from "vitest";
import {
  buildDeleteManifestEntry,
  serializeManifest,
  parseManifest,
  CONTENT_DATA,
  CONTENT_EQUALITY_DELETES,
  STATUS_ADDED,
  FILE_FORMAT_PARQUET,
} from "./manifest.ts";

describe("constants", () => {
  it("should have correct CONTENT_DATA value", () => {
    expect(CONTENT_DATA).toStrictEqual(0);
  });

  it("should have correct CONTENT_EQUALITY_DELETES value", () => {
    expect(CONTENT_EQUALITY_DELETES).toStrictEqual(2);
  });

  it("should have correct STATUS_ADDED value", () => {
    expect(STATUS_ADDED).toStrictEqual(1);
  });

  it("should have correct FILE_FORMAT_PARQUET value", () => {
    expect(FILE_FORMAT_PARQUET).toStrictEqual("PARQUET");
  });
});

describe("buildDeleteManifestEntry", () => {
  it("should create a manifest entry for equality deletes", () => {
    const entry = buildDeleteManifestEntry({
      snapshotId: 12345,
      sequenceNumber: 5,
      filePath: "s3://bucket/data/delete-12345.parquet",
      fileSize: 1024,
      recordCount: 10,
      equalityFieldIds: [1],
    });

    expect(entry.status).toStrictEqual(1);
    expect(entry.snapshot_id).toStrictEqual(12345);
    expect(entry.sequence_number).toStrictEqual(5);
    expect(entry.data_file.content).toStrictEqual(2);
    expect(entry.data_file.file_path).toStrictEqual("s3://bucket/data/delete-12345.parquet");
    expect(entry.data_file.file_format).toStrictEqual("PARQUET");
    expect(entry.data_file.record_count).toStrictEqual(10);
    expect(entry.data_file.file_size_in_bytes).toStrictEqual(1024);
    expect(entry.data_file.equality_ids).toStrictEqual([1]);
    expect(entry.data_file.partition).toStrictEqual({});
  });

  it("should handle multiple equality field IDs", () => {
    const entry = buildDeleteManifestEntry({
      snapshotId: 99999,
      sequenceNumber: 10,
      filePath: "s3://bucket/data/delete-99999.parquet",
      fileSize: 2048,
      recordCount: 50,
      equalityFieldIds: [1, 2, 3],
    });

    expect(entry.data_file.equality_ids).toStrictEqual([1, 2, 3]);
    expect(entry.data_file.record_count).toStrictEqual(50);
  });
});

describe("serializeManifest and parseManifest roundtrip", () => {
  it("should serialize and parse manifest entries correctly", async () => {
    const entry = buildDeleteManifestEntry({
      snapshotId: 55555,
      sequenceNumber: 3,
      filePath: "s3://bucket/data/delete-55555.parquet",
      fileSize: 512,
      recordCount: 5,
      equalityFieldIds: [1],
    });

    const serialized = await serializeManifest([entry]);
    expect(serialized.byteLength > 0).toStrictEqual(true);

    const parsed = await parseManifest(serialized);
    expect(parsed.length).toStrictEqual(1);

    const parsedEntry = parsed[0];
    expect(parsedEntry?.status).toStrictEqual(1);
    expect(parsedEntry?.snapshot_id).toStrictEqual(55555);
    expect(parsedEntry?.sequence_number).toStrictEqual(3);
    expect(parsedEntry?.data_file.content).toStrictEqual(2);
    expect(parsedEntry?.data_file.file_path).toStrictEqual("s3://bucket/data/delete-55555.parquet");
  });
});
