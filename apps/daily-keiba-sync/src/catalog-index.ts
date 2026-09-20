// Runs in Workers; verification runs with bun package scripts.
import { icebergManifests } from "icebird";
import { fetchDeleteMaps } from "icebird/src/fetch.js";
import { applicablePositionDeletes } from "icebird/src/delete.js";
import { parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";
import { permanentFailure } from "./errors";
import { layoutByTable, rowKey } from "./layouts";
import {
  beginIndexPartition,
  clearIndexPartition,
  completeIndexChunk,
  finalizeIndexPartition,
  getIndexChunk,
  getIndexPartition,
  listCatalogWaiters,
  listPendingIndexChunks,
  saveIndexChunks,
  setRunTableCatalogStatus,
  upsertCatalogIndex,
} from "./state";
import { loadCatalogTable } from "./catalog-sync";
import type { CatalogIndexRow, IndexChunkRow } from "./state";
import type { Env, IndexFileJob, IndexPlanJob, RecordRow, SyncJob } from "./types";

const MAX_INDEX_DELETE_FILES = 128;
const MAX_INDEX_DELETE_BYTES = 8n * 1024n * 1024n;
const MAX_INDEX_DELETE_ROWS = 100_000n;

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const matchesPartition = (job: IndexPlanJob, partition: Record<string, unknown>): boolean => {
  if (job.tableName !== "jvd_cs") return String(partition.kaisai_nen) === job.partitionValue;
  if (Object.keys(partition).length !== 0)
    throw permanentFailure("index-partition", new Error("course file is partitioned"));
  return true;
};

const deletedPositions = async (
  chunk: IndexChunkRow,
  loaded: Awaited<ReturnType<typeof loadCatalogTable>>,
): Promise<ReadonlySet<bigint>> => {
  const manifests = await icebergManifests({
    ...loaded,
    snapshotId: BigInt(chunk.snapshot_id),
  });
  const entries = manifests.flatMap((manifest) =>
    manifest.entries.filter((entry) => entry.status !== 2),
  );
  const data = entries.find(
    (entry) => entry.data_file.content === 0 && entry.data_file.file_path === chunk.file_path,
  );
  if (data === undefined)
    throw permanentFailure("index-file-missing", new Error("file absent from pinned snapshot"));
  const deletes = entries.filter((entry) => entry.data_file.content !== 0);
  if (deletes.some((entry) => entry.data_file.content === 2))
    throw permanentFailure(
      "index-equality-delete",
      new Error("equality delete indexing is not supported"),
    );
  if (
    deletes.length > MAX_INDEX_DELETE_FILES ||
    deletes.some(
      (entry) => entry.data_file.file_size_in_bytes < 0n || entry.data_file.record_count < 0n,
    ) ||
    deletes.reduce((total, entry) => total + entry.data_file.file_size_in_bytes, 0n) >
      MAX_INDEX_DELETE_BYTES ||
    deletes.reduce((total, entry) => total + entry.data_file.record_count, 0n) >
      MAX_INDEX_DELETE_ROWS
  )
    throw permanentFailure(
      "index-delete-budget",
      new Error("delete scan exceeds bounded index budget"),
    );
  const maps = await fetchDeleteMaps(deletes, loaded.resolver);
  return applicablePositionDeletes(
    data,
    maps.positionDeletesMap.get(chunk.file_path),
    loaded.metadata,
  );
};

const physicalRow = (value: unknown, columns: readonly string[]): RecordRow => {
  if (!isObject(value)) throw new Error("Invalid catalog index row");
  const row: Record<string, string> = {};
  for (const column of columns) {
    const field = value[column];
    if (typeof field !== "string") throw new Error("Invalid catalog index value");
    row[column] = field;
  }
  return row;
};

const sendJobs = async (queue: Queue<SyncJob>, jobs: readonly SyncJob[]): Promise<void> => {
  for (let offset = 0; offset < jobs.length; offset += 50)
    await queue.sendBatch(jobs.slice(offset, offset + 50).map((body) => ({ body })));
};

const wakeCatalogWaiters = async (env: Env, tableName: string, now: Date): Promise<void> => {
  const waiters = await listCatalogWaiters(env.DB, tableName);
  for (const waiter of waiters)
    await setRunTableCatalogStatus(env.DB, waiter.run_id, waiter.table_name, "pending", now);
  await sendJobs(
    env.R2_CATALOG_JOBS,
    waiters.map((waiter) => ({
      provider: waiter.provider,
      runDate: waiter.run_date,
      runId: waiter.run_id,
      tableName: waiter.table_name,
      tableStagingKey: waiter.staging_key,
      type: "catalog-table",
    })),
  );
};

export const planCatalogIndex = async (
  job: IndexPlanJob,
  env: Env,
  now = new Date(),
): Promise<void> => {
  if (job.tableName === "jvd_cs" && job.partitionValue !== "__all__")
    throw permanentFailure("index-partition", new Error("invalid course partition"));
  const started = await beginIndexPartition(env.DB, job.tableName, job.partitionValue, now);
  if (!started) {
    const partition = await getIndexPartition(env.DB, job.tableName, job.partitionValue);
    if (partition?.status === "ready") {
      await wakeCatalogWaiters(env, job.tableName, now);
      return;
    }
    if (partition?.status !== "building") throw new Error("Catalog index planning is active");
    const chunks = await listPendingIndexChunks(env.DB, job.tableName, job.partitionValue);
    if (chunks.length > 0) {
      await sendJobs(
        env.R2_CATALOG_JOBS,
        chunks.map((chunk) => ({
          chunkId: chunk.chunk_id,
          partitionValue: job.partitionValue,
          provider: job.provider,
          runDate: job.runDate,
          runId: job.runId,
          tableName: job.tableName,
          type: "index-file",
        })),
      );
      return;
    }
    if (partition.completed_chunks !== partition.total_chunks)
      throw new Error("Catalog index chunks are incomplete");
    await finalizeIndexPartition(env.DB, job.tableName, job.partitionValue, now);
    await wakeCatalogWaiters(env, job.tableName, now);
    return;
  }
  await clearIndexPartition(env.DB, job.tableName, job.partitionValue);
  const { metadata, resolver } = await loadCatalogTable(env, job.tableName);
  const manifests = await icebergManifests({ metadata, resolver });
  const entries = manifests.flatMap((manifest) =>
    manifest.entries.filter(
      (entry) =>
        entry.status !== 2 &&
        entry.data_file.content === 0 &&
        matchesPartition(job, entry.data_file.partition),
    ),
  );
  const snapshotId = metadata["current-snapshot-id"];
  if (snapshotId === undefined)
    throw permanentFailure("index-metadata", new Error("missing snapshot"));
  const chunks: IndexChunkRow[] = [];
  for (const entry of entries)
    chunks.push({
      chunk_id: crypto.randomUUID(),
      file_path: entry.data_file.file_path,
      file_size: Number(entry.data_file.file_size_in_bytes),
      partition_value: job.partitionValue,
      row_end: Number(entry.data_file.record_count),
      row_start: 0,
      snapshot_id: String(snapshotId),
      status: "pending",
      table_name: job.tableName,
    });
  await saveIndexChunks(env.DB, job.tableName, job.partitionValue, String(snapshotId), chunks, now);
  if (chunks.length === 0) {
    await wakeCatalogWaiters(env, job.tableName, now);
    return;
  }
  await sendJobs(
    env.R2_CATALOG_JOBS,
    chunks.map((chunk) => ({
      chunkId: chunk.chunk_id,
      partitionValue: job.partitionValue,
      provider: job.provider,
      runDate: job.runDate,
      runId: job.runId,
      tableName: job.tableName,
      type: "index-file",
    })),
  );
};

export const indexCatalogFile = async (
  job: IndexFileJob,
  env: Env,
  now = new Date(),
): Promise<void> => {
  const chunk = await getIndexChunk(env.DB, job.chunkId);
  if (chunk.status === "succeeded") return;
  if (chunk.table_name !== job.tableName || chunk.partition_value !== job.partitionValue)
    throw permanentFailure("index-identity", new Error("mismatch"));
  const layout = layoutByTable(job.tableName);
  const loaded = await loadCatalogTable(env, job.tableName);
  const deleted = await deletedPositions(chunk, loaded);
  const file = await loaded.resolver.reader(chunk.file_path, chunk.file_size);
  const columns =
    job.tableName === "jvd_cs"
      ? [...layout.primaryKey]
      : [...new Set([...layout.primaryKey, "kaisai_nen"])];
  const values: readonly unknown[] = await parquetReadObjects({
    columns,
    compressors,
    file,
    rowEnd: chunk.row_end,
    rowStart: chunk.row_start,
  });
  const rows: CatalogIndexRow[] = values.flatMap((value, index) => {
    if (deleted.has(BigInt(chunk.row_start + index))) return [];
    const row = physicalRow(value, columns);
    return [
      {
        filePath: chunk.file_path,
        partitionValue: job.partitionValue,
        position: chunk.row_start + index,
        rowKey: rowKey(layout, row),
      },
    ];
  });
  await upsertCatalogIndex(env.DB, job.tableName, chunk.snapshot_id, rows, now);
  if (await completeIndexChunk(env.DB, chunk, now))
    await wakeCatalogWaiters(env, job.tableName, now);
};
