// Run with: bun
// Tests for catalog/metadata-types.ts - verifying type structures

import { describe, it, expect } from "vitest";
import type {
  TableMetadata,
  SnapshotJson,
  LoadTableResponse,
  CommitTableRequest,
  IcebergSchema,
} from "./metadata-types.ts";

describe("metadata types", () => {
  it("should create a valid TableMetadata object", () => {
    const metadata: TableMetadata = {
      "format-version": 2,
      "table-uuid": "abc-123",
      location: "s3://bucket/table",
      "last-sequence-number": 5,
      "last-updated-ms": 1700000000000,
      "last-column-id": 20,
      "current-schema-id": 0,
      schemas: [],
      "default-spec-id": 0,
      "partition-specs": [],
      "last-partition-id": 0,
      "default-sort-order-id": 0,
      "sort-orders": [],
    };
    expect(metadata["format-version"]).toStrictEqual(2);
    expect(metadata["table-uuid"]).toStrictEqual("abc-123");
    expect(metadata.location).toStrictEqual("s3://bucket/table");
  });

  it("should create a valid SnapshotJson object", () => {
    const snapshot: SnapshotJson = {
      "snapshot-id": "12345",
      "sequence-number": 1,
      "timestamp-ms": 1700000000000,
      summary: { operation: "append" },
      "manifest-list": "s3://bucket/metadata/snap-12345.avro",
      "schema-id": 0,
    };
    expect(snapshot["snapshot-id"]).toStrictEqual("12345");
    expect(snapshot.summary.operation).toStrictEqual("append");
  });

  it("should create a valid SnapshotJson with parent", () => {
    const snapshot: SnapshotJson = {
      "snapshot-id": "12346",
      "parent-snapshot-id": "12345",
      "sequence-number": 2,
      "timestamp-ms": 1700000001000,
      summary: { operation: "delete" },
      "manifest-list": "s3://bucket/metadata/snap-12346.avro",
      "schema-id": 0,
    };
    expect(snapshot["parent-snapshot-id"]).toStrictEqual("12345");
  });

  it("should create a valid LoadTableResponse", () => {
    const response: LoadTableResponse = {
      "metadata-location": "s3://bucket/metadata/v1.metadata.json",
      metadata: {
        "format-version": 2,
        "table-uuid": "test-uuid",
        location: "s3://bucket/table",
        "last-sequence-number": 1,
        "last-updated-ms": 1700000000000,
        "last-column-id": 10,
        "current-schema-id": 0,
        schemas: [],
        "default-spec-id": 0,
        "partition-specs": [],
        "last-partition-id": 0,
        "default-sort-order-id": 0,
        "sort-orders": [],
      },
    };
    expect(response["metadata-location"]).toStrictEqual("s3://bucket/metadata/v1.metadata.json");
  });

  it("should create a valid CommitTableRequest", () => {
    const request: CommitTableRequest = {
      requirements: [{ type: "assert-current-snapshot-id", "snapshot-id": "100" }],
      updates: [{ action: "add-snapshot" }],
    };
    expect(request.requirements[0]?.type).toStrictEqual("assert-current-snapshot-id");
    expect(request.updates[0]?.action).toStrictEqual("add-snapshot");
  });

  it("should create a valid IcebergSchema", () => {
    const schema: IcebergSchema = {
      type: "struct",
      "schema-id": 0,
      fields: [
        { id: 1, name: "id", required: true, type: "string" },
        { id: 2, name: "horse_name", required: true, type: "string" },
      ],
    };
    expect(schema["schema-id"]).toStrictEqual(0);
    expect(schema.fields[0]?.name).toStrictEqual("id");
    expect(schema.fields[1]?.name).toStrictEqual("horse_name");
  });
});
