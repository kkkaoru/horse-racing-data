// Run with: bun
// Tests for iceberg/snapshot.ts

import { describe, it, expect, vi, beforeEach } from "vitest";
import { generateSnapshotId, buildDeleteSnapshot } from "./snapshot.ts";

beforeEach(() => {
  vi.restoreAllMocks();
});

describe("generateSnapshotId", () => {
  it("should return a string", () => {
    const id = generateSnapshotId();
    expect(typeof id).toStrictEqual("string");
  });

  it("should generate different IDs on each call", () => {
    const id1 = generateSnapshotId();
    const id2 = generateSnapshotId();
    expect(id1 === id2).toStrictEqual(false);
  });

  it("should generate IDs within safe integer range", () => {
    const id = generateSnapshotId();
    expect(Number(id) <= Number.MAX_SAFE_INTEGER).toStrictEqual(true);
    expect(Number(id) >= 0).toStrictEqual(true);
  });
});

describe("buildDeleteSnapshot", () => {
  it("should build a delete snapshot without parent", () => {
    vi.spyOn(Date, "now").mockReturnValue(1700000000000);

    const snapshot = buildDeleteSnapshot({
      snapshotId: "55555",
      sequenceNumber: 5,
      manifestListLocation: "s3://bucket/metadata/snap-123.avro",
      schemaId: 0,
      deletedRowCount: 10,
    });

    expect(snapshot["snapshot-id"]).toStrictEqual("55555");
    expect(snapshot["sequence-number"]).toStrictEqual(5);
    expect(snapshot["manifest-list"]).toStrictEqual("s3://bucket/metadata/snap-123.avro");
    expect(snapshot["schema-id"]).toStrictEqual(0);
    expect(snapshot["timestamp-ms"]).toStrictEqual(1700000000000);
    expect(snapshot.summary.operation).toStrictEqual("delete");
    expect(snapshot.summary["deleted-records"]).toStrictEqual("10");
    expect(snapshot.summary["added-delete-files"]).toStrictEqual("1");
    expect(snapshot.summary["deleted-data-files"]).toStrictEqual("0");
    expect(snapshot["parent-snapshot-id"]).toStrictEqual(undefined);
  });

  it("should build a delete snapshot with parent", () => {
    vi.spyOn(Date, "now").mockReturnValue(1700000001000);

    const snapshot = buildDeleteSnapshot({
      snapshotId: "66666",
      parentSnapshotId: "99999",
      sequenceNumber: 6,
      manifestListLocation: "s3://bucket/metadata/snap-456.avro",
      schemaId: 1,
      deletedRowCount: 25,
    });

    expect(snapshot["snapshot-id"]).toStrictEqual("66666");
    expect(snapshot["parent-snapshot-id"]).toStrictEqual("99999");
    expect(snapshot["sequence-number"]).toStrictEqual(6);
    expect(snapshot["timestamp-ms"]).toStrictEqual(1700000001000);
    expect(snapshot.summary["deleted-records"]).toStrictEqual("25");
  });

  it("should use the provided snapshot ID", () => {
    const snapshot = buildDeleteSnapshot({
      snapshotId: "12345",
      sequenceNumber: 1,
      manifestListLocation: "test1",
      schemaId: 0,
      deletedRowCount: 1,
    });

    expect(snapshot["snapshot-id"]).toStrictEqual("12345");
  });
});
