import {
  icebergManifests,
  icebergTransaction,
  restCatalogConnect,
  restCatalogLoadTable,
} from "icebird";
import { restCatalogUpdateTable } from "icebird/src/catalog/rest.js";
import { parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import { isTransientJobError, permanentFailure, transientFailure } from "./errors";
import { layoutByTable, rowKey } from "./layouts";
import { createR2Resolver } from "./r2-resolver";
import {
  findCatalogPositions,
  getCatalogOperation,
  getIndexPartition,
  markCatalogOperationCommitted,
  markCatalogOperationIndexed,
  startCatalogOperation,
  updateReadyIndexPartitionSnapshots,
  upsertCatalogIndex,
} from "./state";
import type { CatalogIndexRow } from "./state";
import type { Env, RecordRow, TableStage } from "./types";

export type CatalogSyncEnvironment = Pick<
  Env,
  | "CATALOG_BUCKET"
  | "DB"
  | "R2_BUCKET_NAME"
  | "R2_CATALOG_NAMESPACE"
  | "R2_CATALOG_TOKEN"
  | "R2_CATALOG_URI"
  | "R2_CATALOG_WAREHOUSE"
>;

export interface CatalogSyncResult {
  deletedRows: number;
  records: number;
  snapshotId: string;
}

type CatalogMetadata = Parameters<typeof icebergManifests>[0]["metadata"];
type CatalogResolver = ReturnType<typeof createR2Resolver>;

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const currentSchema = (metadata: CatalogMetadata) => {
  const schema = metadata.schemas.find(
    (entry) => entry["schema-id"] === metadata["current-schema-id"],
  );
  if (schema === undefined) throw new Error("Iceberg current schema is missing");
  return schema;
};

const validateSchema = (tableName: string, metadata: CatalogMetadata): void => {
  const layout = layoutByTable(tableName);
  const fields = currentSchema(metadata).fields;
  if (fields.length !== layout.columns.length)
    throw new Error("Iceberg schema column count mismatch");
  for (let index = 0; index < fields.length; index += 1) {
    const field = fields[index];
    const column = layout.columns[index];
    if (
      field === undefined ||
      column === undefined ||
      field.name !== column.name ||
      field.type !== (column.catalogType ?? "string")
    )
      throw new Error("Iceberg schema does not match fixed-record layout");
  }
};

const partitionValue = (row: RecordRow): string => {
  const value = row.kaisai_nen;
  if (typeof value !== "string") throw new Error("Catalog row has no partition value");
  return value.trim();
};

export const toCatalogRecord = (tableName: string, row: RecordRow): Record<string, unknown> => {
  const layout = layoutByTable(tableName);
  return Object.fromEntries(
    layout.columns.map((column) => {
      const value = row[column.name];
      if (value === undefined) throw new Error("Catalog row is missing a column");
      if (value === null) return [column.name, null];
      if (column.catalogType === "int") {
        if (typeof value !== "number" || !Number.isInteger(value))
          throw new Error("Catalog integer value is invalid");
        return [column.name, value];
      }
      if (column.catalogType === "timestamptz") {
        if (typeof value !== "string") throw new Error("Catalog timestamp value is invalid");
        const timestamp = new Date(value);
        if (Number.isNaN(timestamp.getTime()))
          throw new Error("Catalog timestamp value is invalid");
        return [column.name, timestamp];
      }
      if (typeof value !== "string") throw new Error("Catalog string value is invalid");
      return [column.name, value];
    }),
  );
};

const parsePhysicalRow = (value: unknown, columns: readonly string[]): RecordRow => {
  if (!isObject(value)) throw new Error("Invalid Iceberg primary-key row");
  const row: Record<string, string> = {};
  for (const column of columns) {
    const field = value[column];
    if (typeof field !== "string") throw new Error("Invalid Iceberg primary-key value");
    row[column] = field;
  }
  return row;
};

export const connectCatalog = async (env: CatalogSyncEnvironment) =>
  await restCatalogConnect({
    requestInit: {
      headers: {
        Authorization: `Bearer ${env.R2_CATALOG_TOKEN}`,
        "X-Iceberg-Access-Delegation": "vended-credentials",
      },
    },
    url: env.R2_CATALOG_URI,
    warehouse: env.R2_CATALOG_WAREHOUSE,
  });

export const loadCatalogTable = async (
  env: CatalogSyncEnvironment,
  tableName: string,
): Promise<{ metadata: CatalogMetadata; resolver: CatalogResolver }> => {
  const catalog = await connectCatalog(env);
  let loaded: Awaited<ReturnType<typeof restCatalogLoadTable>>;
  try {
    loaded = await restCatalogLoadTable(catalog, {
      namespace: env.R2_CATALOG_NAMESPACE,
      table: tableName,
    });
  } catch (error: unknown) {
    const status = isObject(error) && typeof error.status === "number" ? error.status : undefined;
    if (status === 404) throw permanentFailure("catalog-not-found", error);
    throw error;
  }
  try {
    if (loaded.metadata["format-version"] !== 2)
      throw new Error("Worker-native upsert requires an Iceberg v2 table");
    validateSchema(tableName, loaded.metadata);
  } catch (error: unknown) {
    throw permanentFailure("catalog-schema", error);
  }
  return {
    metadata: loaded.metadata,
    resolver: createR2Resolver(env.CATALOG_BUCKET, env.R2_BUCKET_NAME),
  };
};

const indexCommittedRows = async (
  env: CatalogSyncEnvironment,
  tableName: string,
  metadata: CatalogMetadata,
  resolver: CatalogResolver,
  baseSnapshotId: string,
  snapshotId: string,
  now: Date,
): Promise<void> => {
  const layout = layoutByTable(tableName);
  const targetSnapshot = BigInt(snapshotId);
  const manifests = await icebergManifests({ metadata, resolver });
  const entries = manifests.flatMap((manifest) =>
    manifest.entries.filter(
      (entry) =>
        entry.status === 1 && entry.data_file.content === 0 && entry.snapshot_id === targetSnapshot,
    ),
  );
  const indexed: CatalogIndexRow[] = [];
  for (const entry of entries) {
    const file = await resolver.reader(
      entry.data_file.file_path,
      Number(entry.data_file.file_size_in_bytes),
    );
    const columns = [...new Set([...layout.primaryKey, "kaisai_nen"])];
    const values: readonly unknown[] = await parquetReadObjects({ columns, compressors, file });
    for (let position = 0; position < values.length; position += 1) {
      const row = parsePhysicalRow(values[position], columns);
      indexed.push({
        filePath: entry.data_file.file_path,
        partitionValue: partitionValue(row),
        position,
        rowKey: rowKey(layout, row),
      });
    }
  }
  if (indexed.length === 0) throw new Error("Committed Iceberg data file was not found");
  await upsertCatalogIndex(env.DB, tableName, snapshotId, indexed, now);
  await updateReadyIndexPartitionSnapshots(env.DB, tableName, baseSnapshotId, snapshotId, now);
};

const ensureSupportedWriteCodec = async (
  env: CatalogSyncEnvironment,
  tableName: string,
  metadata: CatalogMetadata,
): Promise<CatalogMetadata> => {
  const codec = metadata.properties?.["write.parquet.compression-codec"]?.toLowerCase();
  if (codec === undefined || codec === "snappy" || codec === "none" || codec === "uncompressed")
    return metadata;
  if (metadata["table-uuid"] === undefined)
    throw permanentFailure("catalog-metadata", new Error("missing table uuid"));
  try {
    const catalog = await connectCatalog(env);
    const updated = await restCatalogUpdateTable(catalog, {
      namespace: env.R2_CATALOG_NAMESPACE,
      requirements: [{ type: "assert-table-uuid", uuid: metadata["table-uuid"] }],
      table: tableName,
      updates: [
        {
          action: "set-properties",
          updates: { "write.parquet.compression-codec": "snappy" },
        },
      ],
    });
    return updated.metadata;
  } catch (error: unknown) {
    throw transientFailure("catalog-write-properties", error);
  }
};

const validateDeletePositions = async (
  metadata: CatalogMetadata,
  resolver: CatalogResolver,
  positions: readonly { file_path: string; row_position: number }[],
): Promise<void> => {
  if (positions.length === 0) return;
  const manifests = await icebergManifests({ metadata, resolver });
  const currentFiles = new Map<string, bigint>();
  for (const manifest of manifests)
    for (const entry of manifest.entries)
      if (entry.status !== 2 && entry.data_file.content === 0)
        currentFiles.set(entry.data_file.file_path, entry.data_file.record_count);
  for (const position of positions) {
    const recordCount = currentFiles.get(position.file_path);
    if (recordCount === undefined)
      throw permanentFailure("catalog-index-target", new Error("indexed file is not current"));
    if (position.row_position < 0 || BigInt(position.row_position) >= recordCount)
      throw permanentFailure("catalog-index-position", new Error("indexed position is invalid"));
  }
};

export const syncCatalogTable = async (
  stage: TableStage,
  env: CatalogSyncEnvironment,
  now = new Date(),
): Promise<CatalogSyncResult> => {
  if (stage.records.length === 0) throw permanentFailure("catalog-stage", new Error("empty"));
  const loaded = await loadCatalogTable(env, stage.tableName);
  const metadata = await ensureSupportedWriteCodec(env, stage.tableName, loaded.metadata);
  const { resolver } = loaded;
  const baseSnapshotId = metadata["current-snapshot-id"];
  if (baseSnapshotId === undefined)
    throw permanentFailure("catalog-metadata", new Error("missing snapshot"));
  let operation = await getCatalogOperation(env.DB, stage.runId, stage.tableName);
  if (operation === null) {
    // Validate before recording a commit attempt: rebuilding an index must not
    // leave a planned operation pinned to an obsolete snapshot.
    for (const partition of new Set(stage.records.map(partitionValue))) {
      const index = await getIndexPartition(env.DB, stage.tableName, partition);
      if (index?.status !== "ready" || index.catalog_snapshot_id !== String(baseSnapshotId))
        throw transientFailure("catalog-index-stale", new Error("snapshot mismatch"));
    }
    await startCatalogOperation(env.DB, stage.runId, stage.tableName, String(baseSnapshotId), now);
    operation = await getCatalogOperation(env.DB, stage.runId, stage.tableName);
  }
  if (operation === null)
    throw permanentFailure("catalog-operation", new Error("operation missing"));
  let committed = metadata;
  let snapshotId = operation.committed_snapshot_id;
  let deletedRows = operation.deleted_rows;
  if (snapshotId === null) {
    if (String(baseSnapshotId) !== operation.base_snapshot_id)
      throw permanentFailure("catalog-commit-uncertain", new Error("snapshot advanced"));
    const layout = layoutByTable(stage.tableName);
    const partitions = [...new Set(stage.records.map(partitionValue))];
    for (const partition of partitions) {
      const index = await getIndexPartition(env.DB, stage.tableName, partition);
      if (
        index === null ||
        index.status !== "ready" ||
        index.catalog_snapshot_id !== String(baseSnapshotId)
      )
        throw transientFailure("catalog-index-stale", new Error("snapshot mismatch"));
    }
    const positions = await findCatalogPositions(
      env.DB,
      stage.tableName,
      stage.records.map((row) => rowKey(layout, row)),
    );
    await validateDeletePositions(metadata, resolver, positions);
    const deletes = positions.map((position) => ({
      file_path: position.file_path,
      pos: BigInt(position.row_position),
    }));
    const catalog = await connectCatalog(env);
    let deleteStage = "catalog-delete-prepare";
    const transactionResolver: CatalogResolver = {
      ...resolver,
      writer(path, options) {
        if (path.endsWith("-deletes.parquet")) deleteStage = "catalog-delete-parquet";
        else if (path.includes("-m")) deleteStage = "catalog-delete-manifest";
        else deleteStage = "catalog-delete-manifest-list";
        return resolver.writer(path, options);
      },
    };
    try {
      committed = await icebergTransaction(
        {
          catalog,
          namespace: env.R2_CATALOG_NAMESPACE,
          resolver: transactionResolver,
          table: stage.tableName,
        },
        async (transaction) => {
          try {
            if (deletes.length > 0) await transaction.delete({ deletes, mode: "parquet" });
          } catch (error: unknown) {
            throw transientFailure(deleteStage, error);
          }
          try {
            await transaction.append({
              records: stage.records.map((row) => toCatalogRecord(stage.tableName, row)),
            });
          } catch (error: unknown) {
            throw transientFailure("catalog-append-stage", error);
          }
        },
      );
    } catch (error: unknown) {
      if (isTransientJobError(error)) throw error;
      throw transientFailure("catalog-transaction", error);
    }
    const currentSnapshotId = committed["current-snapshot-id"];
    if (currentSnapshotId === undefined)
      throw new Error("Iceberg commit returned no current snapshot");
    snapshotId = String(currentSnapshotId);
    deletedRows = deletes.length;
    await markCatalogOperationCommitted(
      env.DB,
      stage.runId,
      stage.tableName,
      snapshotId,
      deletedRows,
      now,
    );
  }
  await indexCommittedRows(
    env,
    stage.tableName,
    committed,
    resolver,
    operation.base_snapshot_id,
    snapshotId,
    now,
  );
  await markCatalogOperationIndexed(env.DB, stage.runId, stage.tableName, now);
  return { deletedRows, records: stage.records.length, snapshotId };
};
