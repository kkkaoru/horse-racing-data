// Run with: bun
// Orchestrates the full Iceberg equality delete flow using @ducklings/workers

import type { TableName, CloudflareBindings, QueryFilter } from "../types.ts";
import type { CatalogConfig, TableIdentifier } from "../catalog/rest-client.ts";
import type { R2SqlConfig } from "../query/r2-sql-client.ts";
import { loadTable, commitTable, fetchCatalogPrefix } from "../catalog/rest-client.ts";
import { executeR2SqlQuery } from "../query/r2-sql-client.ts";
import { buildSelectQuery } from "../query/sql-builder.ts";
import { buildDeleteSnapshot, generateSnapshotId } from "./snapshot.ts";
import { buildDeleteManifestEntry } from "./manifest.ts";
import { serializeManifest } from "./manifest.ts";
import { parseManifestList, serializeManifestList } from "./manifest-list.ts";
import { writeDeleteParquet } from "../duckdb/parquet-writer.ts";
import { createDuckDBInstance } from "../duckdb/connection.ts";

const MAX_DELETE_ROWS = 50000;
const MAX_RETRY_COUNT = 3;
const ID_COLUMN_NAME = "id";
const ID_FIELD_ID = 2;
const CONFLICT_KEYWORD = "Conflict";
const S3_PREFIX_PATTERN = /^s3:\/\/[^/]+\//;

const toR2Key = (s3Path: string): string => s3Path.replace(S3_PREFIX_PATTERN, "");

interface EqualityDeleteArgs {
  readonly env: CloudflareBindings;
  readonly table: TableName;
  readonly filters?: ReadonlyArray<QueryFilter>;
  readonly ids?: ReadonlyArray<string>;
}

interface EqualityDeleteResult {
  readonly success: boolean;
  readonly deletedCount: number;
  readonly error?: string;
}

const buildCatalogConfig = async (env: CloudflareBindings): Promise<CatalogConfig> => {
  const warehouse = `${env.CLOUDFLARE_ACCOUNT_ID}_${env.R2_BUCKET_NAME}`;
  const prefix = await fetchCatalogPrefix(env.CATALOG_URI, warehouse, env.CLOUDFLARE_API_TOKEN);
  return {
    catalogUri: env.CATALOG_URI,
    warehouse: prefix,
    apiToken: env.CLOUDFLARE_API_TOKEN,
  };
};

const buildSqlConfig = (env: CloudflareBindings): R2SqlConfig => ({
  endpoint: env.R2_SQL_ENDPOINT,
  accountId: env.CLOUDFLARE_ACCOUNT_ID,
  bucketName: env.R2_BUCKET_NAME,
  apiToken: env.CLOUDFLARE_API_TOKEN,
});

const buildTableId = (env: CloudflareBindings, table: TableName): TableIdentifier => ({
  namespace: env.ICEBERG_NAMESPACE,
  table,
});

const fetchMatchingIds = async (
  sqlConfig: R2SqlConfig,
  namespace: string,
  table: TableName,
  filters: ReadonlyArray<QueryFilter>,
): Promise<ReadonlyArray<string>> => {
  const query = buildSelectQuery({
    table,
    namespace,
    filters,
    columns: [ID_COLUMN_NAME],
    limit: MAX_DELETE_ROWS + 1,
  });

  const result = await executeR2SqlQuery(sqlConfig, query);

  if (!result.success) {
    throw new Error(`Failed to query matching rows: ${result.error ?? "unknown error"}`);
  }

  return result.data.map((row) => String(row[ID_COLUMN_NAME]));
};

const generateDeleteFilePath = (tableLocation: string, snapshotId: string): string =>
  `${tableLocation}/data/equality-delete-${snapshotId}.parquet`;

const generateManifestPath = (tableLocation: string, snapshotId: string): string =>
  `${tableLocation}/metadata/manifest-${snapshotId}.avro`;

const generateManifestListPath = (tableLocation: string, snapshotId: string): string =>
  `${tableLocation}/metadata/snap-${snapshotId}.avro`;

const loadExistingManifestEntries = async (
  env: CloudflareBindings,
  metadata: {
    readonly snapshots?: ReadonlyArray<{
      readonly "snapshot-id": string;
      readonly "manifest-list": string;
    }>;
    readonly "current-snapshot-id"?: string;
  },
): Promise<ReadonlyArray<import("./manifest-list.ts").ManifestListEntry>> => {
  const currentSnapshot = metadata.snapshots?.find(
    (s) => s["snapshot-id"] === metadata["current-snapshot-id"],
  );

  if (!currentSnapshot) return [];

  const manifestListObj = await env.R2_BUCKET.get(toR2Key(currentSnapshot["manifest-list"]));
  if (!manifestListObj) return [];

  const manifestListBuffer = Buffer.from(await manifestListObj.arrayBuffer());
  return parseManifestList(manifestListBuffer);
};

const executeDeleteAttempt = async (args: EqualityDeleteArgs): Promise<EqualityDeleteResult> => {
  const catalogConfig = await buildCatalogConfig(args.env);
  const sqlConfig = buildSqlConfig(args.env);
  const tableId = buildTableId(args.env, args.table);

  // Step 1: Load table metadata
  const tableResponse = await loadTable(catalogConfig, tableId);
  const metadata = tableResponse.metadata;

  // Step 2: Resolve IDs (direct ids or query via filters)
  const matchingIds =
    args.ids && args.ids.length > 0
      ? args.ids
      : await fetchMatchingIds(
          sqlConfig,
          args.env.ICEBERG_NAMESPACE,
          args.table,
          args.filters ?? [],
        );

  // Step 3: Guard - no matching rows
  if (matchingIds.length === 0) {
    return { success: true, deletedCount: 0 };
  }

  // Step 4: Guard - too many rows
  if (matchingIds.length > MAX_DELETE_ROWS) {
    return {
      success: false,
      deletedCount: 0,
      error: `Delete would affect ${String(matchingIds.length)} rows, exceeding limit of ${String(MAX_DELETE_ROWS)}`,
    };
  }

  // Step 5: Create equality delete Parquet via @ducklings/workers
  const db = await createDuckDBInstance();
  const conn = db.connect();

  try {
    const parquetResult = await writeDeleteParquet({
      conn,
      deleteIds: matchingIds,
      columnName: ID_COLUMN_NAME,
    });

    const currentSnapshotId = metadata["current-snapshot-id"];
    const sequenceNumber = metadata["last-sequence-number"] + 1;
    const schemaId = metadata["current-schema-id"];
    const snapshotId = generateSnapshotId();

    // Step 6: Upload delete Parquet to R2
    const deleteFilePath = generateDeleteFilePath(metadata.location, snapshotId);
    await args.env.R2_BUCKET.put(toR2Key(deleteFilePath), parquetResult.bytes);

    // Step 7: Create manifest entry
    const manifestEntry = buildDeleteManifestEntry({
      snapshotId: Number(snapshotId),
      sequenceNumber,
      filePath: deleteFilePath,
      fileSize: parquetResult.bytes.byteLength,
      recordCount: parquetResult.recordCount,
      equalityFieldIds: [ID_FIELD_ID],
    });

    // Step 8: Serialize and upload manifest
    const manifestPath = generateManifestPath(metadata.location, snapshotId);
    const manifestBytes = await serializeManifest([manifestEntry]);
    await args.env.R2_BUCKET.put(toR2Key(manifestPath), manifestBytes);

    // Step 9: Load existing manifest list entries
    const existingEntries = await loadExistingManifestEntries(args.env, metadata);

    const newManifestListEntry = {
      manifest_path: manifestPath,
      manifest_length: manifestBytes.byteLength,
      partition_spec_id: metadata["default-spec-id"],
      content: 1,
      sequence_number: sequenceNumber,
      min_sequence_number: sequenceNumber,
      added_snapshot_id: Number(snapshotId),
      added_data_files_count: 0,
      existing_data_files_count: 0,
      deleted_data_files_count: 1,
      added_rows_count: 0,
      existing_rows_count: 0,
      deleted_rows_count: matchingIds.length,
    };

    const allManifestEntries = [...existingEntries, newManifestListEntry];

    // Step 10: Upload manifest list
    const manifestListPath = generateManifestListPath(metadata.location, snapshotId);
    const manifestListBytes = await serializeManifestList(allManifestEntries);
    await args.env.R2_BUCKET.put(toR2Key(manifestListPath), manifestListBytes);

    // Step 11: Build and commit snapshot
    const snapshot = buildDeleteSnapshot({
      snapshotId,
      parentSnapshotId: currentSnapshotId,
      sequenceNumber,
      manifestListLocation: manifestListPath,
      schemaId,
      deletedRowCount: matchingIds.length,
    });

    const commitResult = await commitTable(catalogConfig, tableId, {
      requirements: [
        {
          type: "assert-ref-snapshot-id",
          ref: "main",
          "snapshot-id": currentSnapshotId ?? "-1",
        },
      ],
      updates: [
        { action: "add-snapshot", snapshot },
        {
          action: "set-snapshot-ref",
          "ref-name": "main",
          type: "branch",
          "snapshot-id": snapshotId,
        },
      ],
    });

    if (!commitResult.success) {
      return { success: false, deletedCount: 0, error: commitResult.error };
    }

    return { success: true, deletedCount: matchingIds.length };
  } finally {
    conn.close();
    db.close();
  }
};

const executeEqualityDelete = async (args: EqualityDeleteArgs): Promise<EqualityDeleteResult> => {
  const retryAttempt = async (attempt: number): Promise<EqualityDeleteResult> => {
    if (attempt >= MAX_RETRY_COUNT) {
      return {
        success: false,
        deletedCount: 0,
        error: "Max retry count exceeded due to concurrent modifications",
      };
    }

    const result = await executeDeleteAttempt(args);

    if (!result.success && result.error?.includes(CONFLICT_KEYWORD)) {
      return retryAttempt(attempt + 1);
    }

    return result;
  };

  return retryAttempt(0);
};

export {
  executeEqualityDelete,
  executeDeleteAttempt,
  fetchMatchingIds,
  buildCatalogConfig,
  buildSqlConfig,
  buildTableId,
  generateDeleteFilePath,
  generateManifestPath,
  generateManifestListPath,
  loadExistingManifestEntries,
  toR2Key,
  MAX_DELETE_ROWS,
  MAX_RETRY_COUNT,
  ID_COLUMN_NAME,
  ID_FIELD_ID,
};
export type { EqualityDeleteArgs, EqualityDeleteResult };
