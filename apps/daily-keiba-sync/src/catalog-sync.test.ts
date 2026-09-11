import { Miniflare } from "miniflare";
import { afterAll, beforeAll, beforeEach, describe, expect, test, vi } from "vitest";
import { layoutByTable } from "./layouts";
import { syncCatalogTable, toCatalogRecord } from "./catalog-sync";
import type { RecordRow, TableStage } from "./types";

interface FakeTransaction {
  append(options: { records: readonly Record<string, unknown>[] }): Promise<void>;
  delete(options: {
    deletes: readonly { file_path: string; pos: bigint }[];
    mode: string;
  }): Promise<void>;
}

const mocks = vi.hoisted(() => ({
  append: vi.fn(),
  connect: vi.fn(),
  createResolver: vi.fn(),
  delete: vi.fn(),
  getOperation: vi.fn(),
  getPartition: vi.fn(),
  load: vi.fn(),
  manifests: vi.fn(),
  markCommitted: vi.fn(),
  markIndexed: vi.fn(),
  positions: vi.fn(),
  readObjects: vi.fn(),
  startOperation: vi.fn(),
  updatePartitionSnapshots: vi.fn(),
  transaction: vi.fn(
    async (
      _options: unknown,
      callback: (transaction: FakeTransaction) => Promise<void>,
    ): Promise<{ "current-snapshot-id"?: bigint }> => {
      await callback({ append: mocks.append, delete: mocks.delete });
      return { "current-snapshot-id": 99n };
    },
  ),
  update: vi.fn(),
  upsertIndex: vi.fn(),
}));

vi.mock("icebird", () => ({
  icebergManifests: mocks.manifests,
  icebergTransaction: mocks.transaction,
  restCatalogConnect: mocks.connect,
  restCatalogLoadTable: mocks.load,
}));

vi.mock("icebird/src/catalog/rest.js", () => ({ restCatalogUpdateTable: mocks.update }));

vi.mock("hyparquet", () => ({ parquetReadObjects: mocks.readObjects }));
vi.mock("./r2-resolver", () => ({ createR2Resolver: mocks.createResolver }));
vi.mock("./state", () => ({
  findCatalogPositions: mocks.positions,
  getCatalogOperation: mocks.getOperation,
  getIndexPartition: mocks.getPartition,
  markCatalogOperationCommitted: mocks.markCommitted,
  markCatalogOperationIndexed: mocks.markIndexed,
  startCatalogOperation: mocks.startOperation,
  updateReadyIndexPartitionSnapshots: mocks.updatePartitionSnapshots,
  upsertCatalogIndex: mocks.upsertIndex,
}));

let miniflare: Miniflare;
let bucket: R2Bucket;
let db: D1Database;

const completeRow = (tableName: string): RecordRow => {
  const row: Record<string, string> = {};
  for (const column of layoutByTable(tableName).columns) row[column.name] = "";
  row.record_id = tableName.slice(4).toUpperCase();
  row.kaisai_nen = "2026";
  row.kaisai_tsukihi = "0903";
  row.keibajo_code = "30";
  row.race_bango = "01";
  return row;
};

const tableStage = (
  tableName = "nvd_ra",
  records: readonly RecordRow[] = [completeRow(tableName)],
): TableStage => ({
  formatVersion: 1,
  provider: tableName.startsWith("jvd") ? "jv" : "nv",
  records,
  runId: "run-1",
  tableName,
});

const metadata = (tableName = "nvd_ra", formatVersion = 2, partition = true) => {
  const layout = layoutByTable(tableName);
  const fields = layout.columns.map((column, index) => ({
    id: index + 1,
    name: column.name,
    type: "string",
  }));
  const year = fields.find((field) => field.name === "kaisai_nen");
  return {
    "current-schema-id": 0,
    "current-snapshot-id": 10,
    "default-spec-id": 0,
    "format-version": formatVersion,
    "partition-specs": [
      {
        "spec-id": 0,
        fields:
          partition && year !== undefined
            ? [{ "source-id": year.id, name: "kaisai_nen", transform: "identity" }]
            : [],
      },
    ],
    schemas: [{ "schema-id": 0, fields }],
  };
};

const env = () => ({
  CATALOG_BUCKET: bucket,
  DB: db,
  R2_BUCKET_NAME: "pc-keiba-r2-catalog",
  R2_CATALOG_NAMESPACE: "pc_keiba",
  R2_CATALOG_TOKEN: "token",
  R2_CATALOG_URI: "https://catalog.example",
  R2_CATALOG_WAREHOUSE: "warehouse",
});

beforeAll(async () => {
  miniflare = new Miniflare({
    compatibilityDate: "2026-06-18",
    d1Databases: { DB: "catalog-test" },
    modules: true,
    r2Buckets: { BUCKET: "catalog-test" },
    script: "export default {}",
  });
  const bindings = await miniflare.getBindings<{ BUCKET: R2Bucket; DB: D1Database }>();
  bucket = bindings.BUCKET;
  db = bindings.DB;
});

afterAll(async () => {
  await miniflare.dispose();
});

beforeEach(() => {
  mocks.append.mockReset().mockResolvedValue(undefined);
  mocks.connect.mockReset().mockResolvedValue({ type: "rest" });
  mocks.createResolver.mockReset().mockReturnValue({ reader: vi.fn() });
  mocks.getOperation.mockReset().mockResolvedValueOnce(null).mockResolvedValue({
    base_snapshot_id: "10",
    committed_snapshot_id: null,
    deleted_rows: 0,
    status: "planned",
  });
  mocks.getPartition.mockReset().mockResolvedValue({
    catalog_snapshot_id: "10",
    completed_chunks: 1,
    status: "ready",
    total_chunks: 1,
  });
  mocks.delete.mockReset().mockResolvedValue(undefined);
  mocks.load.mockReset().mockResolvedValue({ metadata: metadata() });
  mocks.manifests.mockReset().mockResolvedValue([
    {
      entries: [
        {
          data_file: {
            content: 0,
            file_path: "s3://pc-keiba-r2-catalog/data.parquet",
            file_size_in_bytes: 100n,
            partition: { kaisai_nen: "2026" },
            record_count: 1n,
          },
          snapshot_id: 99n,
          status: 1,
        },
      ],
      url: "manifest.avro",
    },
  ]);
  mocks.markCommitted.mockReset().mockResolvedValue(undefined);
  mocks.markIndexed.mockReset().mockResolvedValue(undefined);
  mocks.positions
    .mockReset()
    .mockResolvedValue([
      { file_path: "s3://pc-keiba-r2-catalog/data.parquet", row_key: "key", row_position: 0 },
    ]);
  mocks.readObjects
    .mockReset()
    .mockResolvedValue([
      { kaisai_nen: "2026", kaisai_tsukihi: "0903", keibajo_code: "30", race_bango: "01" },
    ]);
  mocks.startOperation.mockReset().mockResolvedValue(undefined);
  mocks.transaction.mockClear();
  mocks.update.mockReset();
  mocks.updatePartitionSnapshots.mockReset().mockResolvedValue(undefined);
  mocks.upsertIndex.mockReset().mockResolvedValue(undefined);
});

describe("Worker-native Iceberg table sync", () => {
  test("normalizes an unsupported table write codec before syncing", async () => {
    const zstdMetadata = {
      ...metadata(),
      properties: { "write.parquet.compression-codec": "zstd" },
      "table-uuid": "table-uuid",
    };
    mocks.load.mockResolvedValue({ metadata: zstdMetadata });
    mocks.update.mockResolvedValue({
      metadata: {
        ...zstdMetadata,
        properties: { "write.parquet.compression-codec": "snappy" },
      },
    });

    await expect(syncCatalogTable(tableStage(), env())).resolves.toMatchObject({ records: 1 });
    expect(mocks.update).toHaveBeenCalledWith(expect.anything(), {
      namespace: "pc_keiba",
      requirements: [{ type: "assert-table-uuid", uuid: "table-uuid" }],
      table: "nvd_ra",
      updates: [
        {
          action: "set-properties",
          updates: { "write.parquet.compression-codec": "snappy" },
        },
      ],
    });
  });

  test("fails safely when an unsupported codec cannot be normalized", async () => {
    const zstdMetadata = {
      ...metadata(),
      properties: { "write.parquet.compression-codec": "zstd" },
    };
    mocks.load.mockResolvedValueOnce({ metadata: zstdMetadata });
    await expect(syncCatalogTable(tableStage(), env())).rejects.toMatchObject({
      safeStage: "catalog-metadata",
    });

    mocks.load.mockResolvedValueOnce({
      metadata: { ...zstdMetadata, "table-uuid": "table-uuid" },
    });
    mocks.update.mockRejectedValueOnce(new Error("private commit detail"));
    await expect(syncCatalogTable(tableStage(), env())).rejects.toMatchObject({
      safeStage: "catalog-write-properties",
    });
  });

  test("position-deletes an existing key and appends the replacement atomically", async () => {
    await expect(syncCatalogTable(tableStage(), env())).resolves.toEqual({
      deletedRows: 1,
      records: 1,
      snapshotId: "99",
    });
    expect(mocks.delete).toHaveBeenCalledWith({
      deletes: [{ file_path: "s3://pc-keiba-r2-catalog/data.parquet", pos: 0n }],
      mode: "parquet",
    });
    expect(mocks.append).toHaveBeenCalledTimes(1);
  });

  test("position-deletes both confirmed and provisional keys for a domestic JRA runner", async () => {
    const row = completeRow("jvd_se");
    Object.assign(row, {
      kaisai_nen: "2026",
      kaisai_tsukihi: "0912",
      keibajo_code: "06",
      race_bango: "01",
      umaban: "03",
      ketto_toroku_bango: "2023100001",
    });
    mocks.load.mockResolvedValue({ metadata: metadata("jvd_se") });
    mocks.readObjects.mockResolvedValue([row]);

    await expect(syncCatalogTable(tableStage("jvd_se", [row]), env())).resolves.toMatchObject({
      records: 1,
    });

    const keys = mocks.positions.mock.calls[0]?.[2] as string[];
    expect(keys).toHaveLength(2);
    expect(keys.some((key) => key.includes("\u001f03\u001f"))).toBe(true);
    expect(keys.some((key) => key.includes("\u001f00\u001f"))).toBe(true);
  });

  test("rejects a stage row without its partition value", async () => {
    const row = Object.fromEntries(
      Object.entries(completeRow("nvd_ra")).filter(([column]) => column !== "kaisai_nen"),
    );
    await expect(syncCatalogTable(tableStage("nvd_ra", [row]), env())).rejects.toThrow(
      "no partition value",
    );
    expect(mocks.transaction).not.toHaveBeenCalled();
  });

  test("retries a stale index before recording any commit attempt", async () => {
    mocks.getPartition.mockResolvedValue({
      catalog_snapshot_id: "9",
      completed_chunks: 1,
      status: "ready",
      total_chunks: 1,
    });
    await expect(syncCatalogTable(tableStage(), env())).rejects.toMatchObject({
      safeStage: "catalog-index-stale",
    });
    expect(mocks.transaction).not.toHaveBeenCalled();
  });

  test("a file-rewrite snapshot can be retried after rebuilding without a pinned old operation", async () => {
    mocks.getPartition.mockResolvedValue({ catalog_snapshot_id: "9", status: "ready" });
    await expect(syncCatalogTable(tableStage(), env())).rejects.toMatchObject({
      name: "TransientJobError",
      safeStage: "catalog-index-stale",
    });
    expect(mocks.startOperation).not.toHaveBeenCalled();
    expect(mocks.transaction).not.toHaveBeenCalled();
    mocks.getOperation.mockReset().mockResolvedValueOnce(null).mockResolvedValue({
      base_snapshot_id: "10",
      committed_snapshot_id: null,
      deleted_rows: 0,
      status: "planned",
    });
    mocks.getPartition.mockResolvedValue({ catalog_snapshot_id: "10", status: "ready" });
    await expect(syncCatalogTable(tableStage(), env())).resolves.toMatchObject({
      snapshotId: "99",
    });
    expect(mocks.startOperation).toHaveBeenCalledTimes(1);
    expect(mocks.transaction).toHaveBeenCalledTimes(1);
  });

  test("still rejects snapshot advancement after a recorded commit attempt", async () => {
    mocks.getOperation.mockReset().mockResolvedValue({
      base_snapshot_id: "9",
      committed_snapshot_id: null,
      deleted_rows: 0,
      status: "planned",
    });
    await expect(syncCatalogTable(tableStage(), env())).rejects.toMatchObject({
      name: "PermanentJobError",
      safeStage: "catalog-commit-uncertain",
    });
    expect(mocks.transaction).not.toHaveBeenCalled();
  });

  test.each([
    null,
    { catalog_snapshot_id: "10", completed_chunks: 0, status: "building", total_chunks: 1 },
  ])("does not pin a commit snapshot while an index is unavailable", async (partition) => {
    mocks.getPartition.mockResolvedValue(partition);
    await expect(syncCatalogTable(tableStage(), env())).rejects.toMatchObject({
      name: "TransientJobError",
      safeStage: "catalog-index-stale",
    });
    expect(mocks.startOperation).not.toHaveBeenCalled();
    expect(mocks.transaction).not.toHaveBeenCalled();
  });

  test("fails closed when an indexed delete target is absent or out of range", async () => {
    mocks.manifests.mockResolvedValueOnce([]);
    await expect(syncCatalogTable(tableStage(), env())).rejects.toMatchObject({
      safeStage: "catalog-index-target",
    });
    expect(mocks.transaction).not.toHaveBeenCalled();

    mocks.manifests.mockResolvedValueOnce([
      {
        entries: [
          {
            data_file: {
              content: 0,
              file_path: "s3://pc-keiba-r2-catalog/data.parquet",
              record_count: 0n,
            },
            status: 1,
          },
        ],
      },
    ]);
    await expect(syncCatalogTable(tableStage(), env())).rejects.toMatchObject({
      safeStage: "catalog-index-position",
    });
    expect(mocks.transaction).not.toHaveBeenCalled();
  });

  test("classifies delete, append, and transaction failures without exposing causes", async () => {
    mocks.delete.mockRejectedValueOnce(new Error("private delete detail"));
    await expect(syncCatalogTable(tableStage(), env())).rejects.toMatchObject({
      safeStage: "catalog-delete-prepare",
    });

    mocks.positions.mockResolvedValueOnce([]);
    mocks.append.mockRejectedValueOnce(new Error("private append detail"));
    await expect(syncCatalogTable(tableStage(), env())).rejects.toMatchObject({
      safeStage: "catalog-append-stage",
    });

    mocks.transaction.mockRejectedValueOnce(new Error("private transaction detail"));
    await expect(syncCatalogTable(tableStage(), env())).rejects.toMatchObject({
      safeStage: "catalog-transaction",
    });
  });

  test("appends without a delete when no indexed key matches", async () => {
    mocks.positions.mockResolvedValue([]);
    await expect(syncCatalogTable(tableStage(), env())).resolves.toMatchObject({ deletedRows: 0 });
    expect(mocks.delete).not.toHaveBeenCalled();
  });

  test("indexes only data files added by the committed snapshot", async () => {
    mocks.positions.mockResolvedValue([]);
    mocks.manifests.mockResolvedValue([
      {
        entries: [
          { data_file: { content: 0, partition: { kaisai_nen: "2025" } }, status: 1 },
          { data_file: { content: 0, partition: { kaisai_nen: "2026" } }, status: 2 },
          { data_file: { content: 1, partition: { kaisai_nen: "2026" } }, status: 1 },
        ],
        url: "manifest.avro",
      },
    ]);
    await expect(syncCatalogTable(tableStage(), env())).rejects.toThrow(
      "Committed Iceberg data file was not found",
    );
    expect(mocks.readObjects).not.toHaveBeenCalled();
  });

  test("scans unpartitioned tables and validates physical key rows", async () => {
    mocks.load.mockResolvedValue({ metadata: metadata("nvd_ra", 2, false) });
    mocks.readObjects.mockResolvedValue([null]);
    await expect(syncCatalogTable(tableStage(), env())).rejects.toThrow("primary-key row");
  });

  test("rejects incompatible table metadata and stages", async () => {
    await expect(syncCatalogTable(tableStage("nvd_ra", []), env())).rejects.toThrow(
      "catalog-stage",
    );
    mocks.load.mockResolvedValue({ metadata: metadata("nvd_ra", 1) });
    await expect(syncCatalogTable(tableStage(), env())).rejects.toThrow("catalog-schema");
    const missingSchema = metadata();
    missingSchema["current-schema-id"] = 99;
    mocks.load.mockResolvedValue({ metadata: missingSchema });
    await expect(syncCatalogTable(tableStage(), env())).rejects.toThrow("catalog-schema");
    const wrongCount = metadata();
    wrongCount.schemas[0]?.fields.pop();
    mocks.load.mockResolvedValue({ metadata: wrongCount });
    await expect(syncCatalogTable(tableStage(), env())).rejects.toThrow("catalog-schema");
    const wrongType = metadata();
    if (wrongType.schemas[0]?.fields[0] !== undefined) wrongType.schemas[0].fields[0].type = "int";
    mocks.load.mockResolvedValue({ metadata: wrongType });
    await expect(syncCatalogTable(tableStage(), env())).rejects.toThrow("catalog-schema");
  });

  test("rejects a non-string physical primary-key value", async () => {
    mocks.readObjects.mockResolvedValue([
      { kaisai_nen: 2026, kaisai_tsukihi: "0903", keibajo_code: "30", race_bango: "01" },
    ]);
    await expect(syncCatalogTable(tableStage(), env())).rejects.toThrow("primary-key value");
  });

  test("resumes index publication after an already committed operation", async () => {
    mocks.getOperation.mockReset().mockResolvedValue({
      base_snapshot_id: "10",
      committed_snapshot_id: "99",
      deleted_rows: 3,
      status: "committed",
    });
    await expect(syncCatalogTable(tableStage(), env())).resolves.toMatchObject({
      deletedRows: 3,
      snapshotId: "99",
    });
    expect(mocks.transaction).not.toHaveBeenCalled();
    expect(mocks.markIndexed).toHaveBeenCalledTimes(1);
  });

  test("fails closed instead of duplicating rows after an uncertain commit", async () => {
    const advanced = metadata();
    advanced["current-snapshot-id"] = 11;
    mocks.load.mockResolvedValue({ metadata: advanced });
    mocks.getOperation.mockReset().mockResolvedValue({
      base_snapshot_id: "10",
      committed_snapshot_id: null,
      deleted_rows: 0,
      status: "planned",
    });
    await expect(syncCatalogTable(tableStage(), env())).rejects.toThrow("catalog-commit-uncertain");
    expect(mocks.transaction).not.toHaveBeenCalled();
  });

  test("classifies only Catalog 404 as permanent", async () => {
    mocks.load.mockRejectedValue(Object.assign(new Error("hidden"), { status: 404 }));
    await expect(syncCatalogTable(tableStage(), env())).rejects.toThrow("catalog-not-found");
    const transient = Object.assign(new Error("temporary"), { status: 503 });
    mocks.load.mockRejectedValue(transient);
    await expect(syncCatalogTable(tableStage(), env())).rejects.toBe(transient);
    const network = new Error("network");
    mocks.load.mockRejectedValue(network);
    await expect(syncCatalogTable(tableStage(), env())).rejects.toBe(network);
  });

  test("fails closed when operation state or base snapshot is missing", async () => {
    const missingSnapshot = metadata();
    Reflect.deleteProperty(missingSnapshot, "current-snapshot-id");
    mocks.load.mockResolvedValue({ metadata: missingSnapshot });
    await expect(syncCatalogTable(tableStage(), env())).rejects.toThrow("catalog-metadata");

    mocks.load.mockResolvedValue({ metadata: metadata() });
    mocks.getOperation.mockReset().mockResolvedValue(null);
    await expect(syncCatalogTable(tableStage(), env())).rejects.toThrow("catalog-operation");
  });

  test("converts typed external rows for the Catalog writer", () => {
    const layout = layoutByTable("netkeiba_training_workouts");
    const row = Object.fromEntries(
      layout.columns.map((column) => [
        column.name,
        column.catalogType === "int"
          ? 1
          : column.catalogType === "timestamptz"
            ? "2026-09-04T19:30:00.000Z"
            : "value",
      ]),
    );
    const converted = toCatalogRecord("netkeiba_training_workouts", row);
    expect(converted.workout_index).toBe(1);
    expect(converted.fetched_at).toStrictEqual(new Date("2026-09-04T19:30:00.000Z"));
  });

  test("rejects malformed typed external Catalog values", () => {
    const layout = layoutByTable("netkeiba_training_workouts");
    const valid = Object.fromEntries(
      layout.columns.map((column) => [
        column.name,
        column.catalogType === "int"
          ? 1
          : column.catalogType === "timestamptz"
            ? "2026-09-04T19:30:00.000Z"
            : "value",
      ]),
    );
    expect(() =>
      toCatalogRecord("netkeiba_training_workouts", { ...valid, workout_index: "one" }),
    ).toThrow("integer value");
    expect(() =>
      toCatalogRecord("netkeiba_training_workouts", { ...valid, fetched_at: "invalid" }),
    ).toThrow("timestamp value");
    expect(() => toCatalogRecord("netkeiba_training_workouts", { ...valid, bamei: 1 })).toThrow(
      "string value",
    );
    expect(() => {
      const missing = { ...valid };
      Reflect.deleteProperty(missing, "course");
      toCatalogRecord("netkeiba_training_workouts", missing);
    }).toThrow("missing a column");
    expect(
      toCatalogRecord("netkeiba_training_workouts", { ...valid, course: null }).course,
    ).toBeNull();
  });

  test("rejects commits that do not return a snapshot", async () => {
    mocks.transaction.mockImplementationOnce(
      async (_options: unknown, callback: (transaction: FakeTransaction) => Promise<void>) => {
        await callback({ append: mocks.append, delete: mocks.delete });
        return {};
      },
    );
    await expect(syncCatalogTable(tableStage(), env())).rejects.toThrow("no current snapshot");
  });
});
