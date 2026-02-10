// Run with: bun
// Tests for iceberg/equality-delete.ts

import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../catalog/rest-client.ts", () => ({
  loadTable: vi.fn(),
  commitTable: vi.fn(),
  fetchCatalogPrefix: vi.fn().mockResolvedValue("test-prefix"),
  CONFLICT_STATUS: 409,
}));

vi.mock("../query/r2-sql-client.ts", () => ({
  executeR2SqlQuery: vi.fn(),
}));

vi.mock("../duckdb/connection.ts", () => ({
  createDuckDBInstance: vi.fn(),
}));

vi.mock("../duckdb/parquet-writer.ts", () => ({
  writeDeleteParquet: vi.fn(),
}));

vi.mock("./manifest.ts", () => ({
  buildDeleteManifestEntry: vi.fn().mockReturnValue({
    status: 1,
    snapshot_id: 12345,
    sequence_number: 2,
    data_file: {
      content: 2,
      file_path: "test-path",
      file_format: "PARQUET",
      record_count: 1,
      file_size_in_bytes: 100,
      equality_ids: [1],
      partition: {},
    },
  }),
  serializeManifest: vi.fn().mockResolvedValue(Buffer.from("manifest")),
}));

vi.mock("./manifest-list.ts", () => ({
  parseManifestList: vi.fn().mockResolvedValue([]),
  serializeManifestList: vi.fn().mockResolvedValue(Buffer.from("manifest-list")),
}));

import {
  executeDeleteAttempt,
  executeEqualityDelete,
  buildCatalogConfig,
  buildSqlConfig,
  buildTableId,
  generateDeleteFilePath,
  generateManifestPath,
  generateManifestListPath,
  fetchMatchingIds,
  loadExistingManifestEntries,
  toR2Key,
  MAX_DELETE_ROWS,
  MAX_RETRY_COUNT,
  ID_COLUMN_NAME,
  ID_FIELD_ID,
} from "./equality-delete.ts";
import { loadTable, commitTable, fetchCatalogPrefix } from "../catalog/rest-client.ts";
import { executeR2SqlQuery } from "../query/r2-sql-client.ts";
import { createDuckDBInstance } from "../duckdb/connection.ts";
import { writeDeleteParquet } from "../duckdb/parquet-writer.ts";
import { serializeManifest, buildDeleteManifestEntry } from "./manifest.ts";
import { parseManifestList, serializeManifestList } from "./manifest-list.ts";
import type { CloudflareBindings } from "../types.ts";

const createMockEnv = (): CloudflareBindings => ({
  R2_BUCKET: {
    put: vi.fn().mockResolvedValue(undefined),
    get: vi.fn().mockResolvedValue(null),
  } as unknown as R2Bucket,
  CLOUDFLARE_API_TOKEN: "test-token",
  CLOUDFLARE_ACCOUNT_ID: "test-account",
  R2_BUCKET_NAME: "test-bucket",
  ICEBERG_NAMESPACE: "horse_racing",
  CATALOG_URI: "https://catalog.example.com",
  R2_SQL_ENDPOINT: "https://api.sql.cloudflarestorage.com/api/v1/accounts",
  R2_ACCESS_KEY_ID: "test-key",
  R2_SECRET_ACCESS_KEY: "test-secret",
});

const createMockTableMetadata = () => ({
  "metadata-location": "s3://bucket/metadata.json",
  metadata: {
    "format-version": 2,
    "table-uuid": "test-uuid",
    location: "s3://bucket/table",
    "last-sequence-number": 1,
    "last-updated-ms": 1000,
    "last-column-id": 10,
    "current-schema-id": 0,
    schemas: [],
    "default-spec-id": 0,
    "partition-specs": [],
    "last-partition-id": 0,
    "default-sort-order-id": 0,
    "sort-orders": [],
    "current-snapshot-id": "100",
    snapshots: [
      {
        "snapshot-id": "100",
        "sequence-number": 1,
        "timestamp-ms": 1000,
        summary: { operation: "append" },
        "manifest-list": "s3://bucket/metadata/snap-100.avro",
        "schema-id": 0,
      },
    ],
  },
});

const setupManifestMocks = (): void => {
  vi.mocked(buildDeleteManifestEntry).mockReturnValue({
    status: 1,
    snapshot_id: 12345,
    sequence_number: 2,
    data_file: {
      content: 2,
      file_path: "test-path",
      file_format: "PARQUET",
      record_count: 1,
      file_size_in_bytes: 100,
      equality_ids: [1],
      partition: {},
    },
  });
  vi.mocked(serializeManifest).mockResolvedValue(Buffer.from("manifest"));
  vi.mocked(parseManifestList).mockResolvedValue([]);
  vi.mocked(serializeManifestList).mockResolvedValue(Buffer.from("manifest-list"));
};

const setupDuckDBMock = (): void => {
  vi.mocked(createDuckDBInstance).mockResolvedValue({
    connect: () =>
      ({
        query: vi.fn().mockResolvedValue([]),
        execute: vi.fn().mockResolvedValue(0),
        close: vi.fn(),
      }) as never,
    close: vi.fn(),
  });
  vi.mocked(writeDeleteParquet).mockResolvedValue({
    bytes: new Uint8Array([1, 2, 3]),
    recordCount: 2,
  });
};

beforeEach(() => {
  vi.restoreAllMocks();
});

describe("toR2Key", () => {
  it("should strip s3://bucket-name/ prefix", () => {
    const result = toR2Key("s3://horse-racing-data/__r2_data_catalog/table/data/file.parquet");
    expect(result).toStrictEqual("__r2_data_catalog/table/data/file.parquet");
  });

  it("should return path unchanged if no s3 prefix", () => {
    const result = toR2Key("__r2_data_catalog/table/data/file.parquet");
    expect(result).toStrictEqual("__r2_data_catalog/table/data/file.parquet");
  });
});

describe("constants", () => {
  it("should have MAX_DELETE_ROWS of 50000", () => {
    expect(MAX_DELETE_ROWS).toStrictEqual(50000);
  });

  it("should have MAX_RETRY_COUNT of 3", () => {
    expect(MAX_RETRY_COUNT).toStrictEqual(3);
  });

  it("should have ID_COLUMN_NAME of id", () => {
    expect(ID_COLUMN_NAME).toStrictEqual("id");
  });

  it("should have ID_FIELD_ID of 2", () => {
    expect(ID_FIELD_ID).toStrictEqual(2);
  });
});

describe("buildCatalogConfig", () => {
  it("should build catalog config from env with fetched prefix", async () => {
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    const env = createMockEnv();
    const config = await buildCatalogConfig(env);
    expect(config).toStrictEqual({
      catalogUri: "https://catalog.example.com",
      warehouse: "test-prefix",
      apiToken: "test-token",
    });
  });
});

describe("buildSqlConfig", () => {
  it("should build SQL config from env", () => {
    const env = createMockEnv();
    const config = buildSqlConfig(env);
    expect(config).toStrictEqual({
      endpoint: "https://api.sql.cloudflarestorage.com/api/v1/accounts",
      accountId: "test-account",
      bucketName: "test-bucket",
      apiToken: "test-token",
    });
  });
});

describe("buildTableId", () => {
  it("should build table identifier from env and table name", () => {
    const env = createMockEnv();
    const tableId = buildTableId(env, "horse_info");
    expect(tableId).toStrictEqual({
      namespace: "horse_racing",
      table: "horse_info",
    });
  });

  it("should build table identifier for race_info", () => {
    const env = createMockEnv();
    const tableId = buildTableId(env, "race_info");
    expect(tableId).toStrictEqual({
      namespace: "horse_racing",
      table: "race_info",
    });
  });
});

describe("generateDeleteFilePath", () => {
  it("should generate correct delete file path", () => {
    const path = generateDeleteFilePath("s3://bucket/table", "12345");
    expect(path).toStrictEqual("s3://bucket/table/data/equality-delete-12345.parquet");
  });
});

describe("generateManifestPath", () => {
  it("should generate correct manifest path", () => {
    const path = generateManifestPath("s3://bucket/table", "12345");
    expect(path).toStrictEqual("s3://bucket/table/metadata/manifest-12345.avro");
  });
});

describe("generateManifestListPath", () => {
  it("should generate correct manifest list path", () => {
    const path = generateManifestListPath("s3://bucket/table", "12345");
    expect(path).toStrictEqual("s3://bucket/table/metadata/snap-12345.avro");
  });
});

describe("fetchMatchingIds", () => {
  it("should return matching IDs from R2 SQL", async () => {
    vi.mocked(executeR2SqlQuery).mockResolvedValue({
      success: true,
      data: [{ id: "id-001" }, { id: "id-002" }],
    });

    const sqlConfig = {
      endpoint: "https://sql.example.com",
      accountId: "account",
      bucketName: "bucket",
      apiToken: "token",
    };

    const ids = await fetchMatchingIds(sqlConfig, "horse_racing", "horse_info", [
      { column: "horse_name", op: "eq" as const, value: "Deep Impact" },
    ]);

    expect(ids).toStrictEqual(["id-001", "id-002"]);
  });

  it("should throw on query failure", async () => {
    vi.mocked(executeR2SqlQuery).mockResolvedValue({
      success: false,
      data: [],
      error: "R2 SQL request failed: 500 Error",
    });

    const sqlConfig = {
      endpoint: "https://sql.example.com",
      accountId: "account",
      bucketName: "bucket",
      apiToken: "token",
    };

    await expect(fetchMatchingIds(sqlConfig, "horse_racing", "horse_info", [])).rejects.toThrow(
      "Failed to query matching rows",
    );
  });

  it("should return empty array when no matches", async () => {
    vi.mocked(executeR2SqlQuery).mockResolvedValue({
      success: true,
      data: [],
    });

    const sqlConfig = {
      endpoint: "https://sql.example.com",
      accountId: "account",
      bucketName: "bucket",
      apiToken: "token",
    };

    const ids = await fetchMatchingIds(sqlConfig, "horse_racing", "horse_info", [
      { column: "id", op: "eq" as const, value: "nonexistent" },
    ]);

    expect(ids).toStrictEqual([]);
  });
});

describe("loadExistingManifestEntries", () => {
  it("should return empty array when no current snapshot", async () => {
    const env = createMockEnv();
    const metadata = { "current-snapshot-id": undefined, snapshots: [] };
    const result = await loadExistingManifestEntries(env, metadata);
    expect(result).toStrictEqual([]);
  });

  it("should return empty array when manifest list not found in R2", async () => {
    const env = createMockEnv();
    const metadata = {
      "current-snapshot-id": "100",
      snapshots: [{ "snapshot-id": "100", "manifest-list": "s3://bucket/snap-100.avro" }],
    };
    const result = await loadExistingManifestEntries(env, metadata);
    expect(result).toStrictEqual([]);
  });
});

describe("executeDeleteAttempt with filters", () => {
  it("should return success with 0 count when no matching rows", async () => {
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    vi.mocked(loadTable).mockResolvedValue(createMockTableMetadata());
    vi.mocked(executeR2SqlQuery).mockResolvedValue({
      success: true,
      data: [],
    });

    const env = createMockEnv();
    const result = await executeDeleteAttempt({
      env,
      table: "horse_info",
      filters: [{ column: "id", op: "eq", value: "nonexistent" }],
    });

    expect(result).toStrictEqual({ success: true, deletedCount: 0 });
  });

  it("should return error when too many rows match", async () => {
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    vi.mocked(loadTable).mockResolvedValue(createMockTableMetadata());

    const manyIds = Array.from({ length: MAX_DELETE_ROWS + 2 }, (_, i) => ({
      id: `id-${String(i)}`,
    }));
    vi.mocked(executeR2SqlQuery).mockResolvedValue({
      success: true,
      data: manyIds,
    });

    const env = createMockEnv();
    const result = await executeDeleteAttempt({
      env,
      table: "horse_info",
      filters: [{ column: "horse_name", op: "like", value: "%" }],
    });

    expect(result.success).toStrictEqual(false);
    expect(result.deletedCount).toStrictEqual(0);
    if (!result.success) {
      expect(result.error).toStrictEqual(
        `Delete would affect ${String(MAX_DELETE_ROWS + 2)} rows, exceeding limit of ${String(MAX_DELETE_ROWS)}`,
      );
    }
  });

  it("should complete delete flow successfully with filters", async () => {
    setupManifestMocks();
    setupDuckDBMock();
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    vi.mocked(loadTable).mockResolvedValue(createMockTableMetadata());
    vi.mocked(executeR2SqlQuery).mockResolvedValue({
      success: true,
      data: [{ id: "id-001" }, { id: "id-002" }],
    });
    vi.mocked(commitTable).mockResolvedValue({ success: true });

    const env = createMockEnv();
    const result = await executeDeleteAttempt({
      env,
      table: "horse_info",
      filters: [{ column: "id", op: "in", value: ["id-001", "id-002"] }],
    });

    expect(result.success).toStrictEqual(true);
    expect(result.deletedCount).toStrictEqual(2);
  });

  it("should return error when commit fails", async () => {
    setupManifestMocks();
    setupDuckDBMock();
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    vi.mocked(loadTable).mockResolvedValue(createMockTableMetadata());
    vi.mocked(executeR2SqlQuery).mockResolvedValue({
      success: true,
      data: [{ id: "id-001" }],
    });
    vi.mocked(writeDeleteParquet).mockResolvedValue({
      bytes: new Uint8Array([1]),
      recordCount: 1,
    });
    vi.mocked(commitTable).mockResolvedValue({
      success: false,
      error: "Commit failed: 500 Server Error",
      status: 500,
    });

    const env = createMockEnv();
    const result = await executeDeleteAttempt({
      env,
      table: "horse_info",
      filters: [{ column: "id", op: "eq", value: "id-001" }],
    });

    expect(result.success).toStrictEqual(false);
    if (!result.success) {
      expect(result.error).toStrictEqual("Commit failed: 500 Server Error");
    }
  });
});

describe("executeDeleteAttempt with ids", () => {
  it("should delete by direct ids without querying R2 SQL", async () => {
    setupManifestMocks();
    setupDuckDBMock();
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    vi.mocked(loadTable).mockResolvedValue(createMockTableMetadata());
    vi.mocked(commitTable).mockResolvedValue({ success: true });

    const env = createMockEnv();
    const result = await executeDeleteAttempt({
      env,
      table: "horse_info",
      ids: ["id-001", "id-002", "id-003"],
    });

    expect(result.success).toStrictEqual(true);
    expect(result.deletedCount).toStrictEqual(3);
    expect(vi.mocked(executeR2SqlQuery)).not.toHaveBeenCalled();
  });

  it("should return success with 0 count when ids array is empty", async () => {
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    vi.mocked(loadTable).mockResolvedValue(createMockTableMetadata());
    vi.mocked(executeR2SqlQuery).mockResolvedValue({
      success: true,
      data: [],
    });

    const env = createMockEnv();
    const result = await executeDeleteAttempt({
      env,
      table: "horse_info",
      ids: [],
    });

    expect(result).toStrictEqual({ success: true, deletedCount: 0 });
  });

  it("should reject when ids exceed MAX_DELETE_ROWS", async () => {
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    vi.mocked(loadTable).mockResolvedValue(createMockTableMetadata());

    const manyIds = Array.from({ length: MAX_DELETE_ROWS + 1 }, (_, i) => `id-${String(i)}`);

    const env = createMockEnv();
    const result = await executeDeleteAttempt({
      env,
      table: "horse_info",
      ids: manyIds,
    });

    expect(result.success).toStrictEqual(false);
    expect(result.deletedCount).toStrictEqual(0);
    if (!result.success) {
      expect(result.error).toStrictEqual(
        `Delete would affect ${String(MAX_DELETE_ROWS + 1)} rows, exceeding limit of ${String(MAX_DELETE_ROWS)}`,
      );
    }
  });

  it("should pass ids to writeDeleteParquet correctly", async () => {
    setupManifestMocks();
    setupDuckDBMock();
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    vi.mocked(loadTable).mockResolvedValue(createMockTableMetadata());
    vi.mocked(commitTable).mockResolvedValue({ success: true });

    const env = createMockEnv();
    await executeDeleteAttempt({
      env,
      table: "race_info",
      ids: ["ri-001", "ri-002"],
    });

    expect(vi.mocked(writeDeleteParquet)).toHaveBeenCalledWith({
      conn: expect.anything(),
      deleteIds: ["ri-001", "ri-002"],
      columnName: "id",
    });
  });
});

describe("executeEqualityDelete", () => {
  it("should return success on first attempt with filters", async () => {
    setupManifestMocks();
    setupDuckDBMock();
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    vi.mocked(loadTable).mockResolvedValue(createMockTableMetadata());
    vi.mocked(executeR2SqlQuery).mockResolvedValue({
      success: true,
      data: [{ id: "id-001" }],
    });
    vi.mocked(writeDeleteParquet).mockResolvedValue({
      bytes: new Uint8Array([1]),
      recordCount: 1,
    });
    vi.mocked(commitTable).mockResolvedValue({ success: true });

    const env = createMockEnv();
    const result = await executeEqualityDelete({
      env,
      table: "horse_info",
      filters: [{ column: "id", op: "eq", value: "id-001" }],
    });

    expect(result.success).toStrictEqual(true);
    expect(result.deletedCount).toStrictEqual(1);
  });

  it("should return success on first attempt with ids", async () => {
    setupManifestMocks();
    setupDuckDBMock();
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    vi.mocked(loadTable).mockResolvedValue(createMockTableMetadata());
    vi.mocked(commitTable).mockResolvedValue({ success: true });

    const env = createMockEnv();
    const result = await executeEqualityDelete({
      env,
      table: "horse_info",
      ids: ["id-001", "id-002"],
    });

    expect(result.success).toStrictEqual(true);
    expect(result.deletedCount).toStrictEqual(2);
    expect(vi.mocked(executeR2SqlQuery)).not.toHaveBeenCalled();
  });

  it("should retry on conflict and eventually succeed", async () => {
    setupManifestMocks();
    setupDuckDBMock();
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    vi.mocked(loadTable).mockResolvedValue(createMockTableMetadata());
    vi.mocked(executeR2SqlQuery).mockResolvedValue({
      success: true,
      data: [{ id: "id-001" }],
    });
    vi.mocked(writeDeleteParquet).mockResolvedValue({
      bytes: new Uint8Array([1]),
      recordCount: 1,
    });
    vi.mocked(commitTable)
      .mockResolvedValueOnce({
        success: false,
        error: "Conflict: table was modified concurrently",
        status: 409,
      })
      .mockResolvedValueOnce({ success: true });

    const env = createMockEnv();
    const result = await executeEqualityDelete({
      env,
      table: "horse_info",
      filters: [{ column: "id", op: "eq", value: "id-001" }],
    });

    expect(result.success).toStrictEqual(true);
    expect(result.deletedCount).toStrictEqual(1);
  });

  it("should fail after max retries on repeated conflicts", async () => {
    setupManifestMocks();
    setupDuckDBMock();
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    vi.mocked(loadTable).mockResolvedValue(createMockTableMetadata());
    vi.mocked(executeR2SqlQuery).mockResolvedValue({
      success: true,
      data: [{ id: "id-001" }],
    });
    vi.mocked(writeDeleteParquet).mockResolvedValue({
      bytes: new Uint8Array([1]),
      recordCount: 1,
    });
    vi.mocked(commitTable).mockResolvedValue({
      success: false,
      error: "Conflict: table was modified concurrently",
      status: 409,
    });

    const env = createMockEnv();
    const result = await executeEqualityDelete({
      env,
      table: "horse_info",
      filters: [{ column: "id", op: "eq", value: "id-001" }],
    });

    expect(result.success).toStrictEqual(false);
    if (!result.success) {
      expect(result.error).toStrictEqual(
        "Max retry count exceeded due to concurrent modifications",
      );
    }
  });

  it("should not retry on non-conflict errors", async () => {
    setupManifestMocks();
    setupDuckDBMock();
    vi.mocked(fetchCatalogPrefix).mockResolvedValue("test-prefix");
    vi.mocked(loadTable).mockResolvedValue(createMockTableMetadata());
    vi.mocked(executeR2SqlQuery).mockResolvedValue({
      success: true,
      data: [{ id: "id-001" }],
    });
    vi.mocked(writeDeleteParquet).mockResolvedValue({
      bytes: new Uint8Array([1]),
      recordCount: 1,
    });
    vi.mocked(commitTable).mockResolvedValue({
      success: false,
      error: "Commit failed: 500 Server Error",
      status: 500,
    });

    const env = createMockEnv();
    const result = await executeEqualityDelete({
      env,
      table: "horse_info",
      filters: [{ column: "id", op: "eq", value: "id-001" }],
    });

    expect(result.success).toStrictEqual(false);
    if (!result.success) {
      expect(result.error).toStrictEqual("Commit failed: 500 Server Error");
    }
  });
});
