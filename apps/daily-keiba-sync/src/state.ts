import type { Provider, RunRow, TriggerKind } from "./types";

export interface CatalogTargetRow {
  partition_field: string;
  table_name: string;
}

export interface RunTableRow {
  catalog_status: string;
  neon_status: string;
  source_records: number;
  staging_key: string;
  table_name: string;
}

export interface RunTablePlan extends RunTableRow {
  partitions: readonly string[];
}

export interface IndexChunkRow {
  chunk_id: string;
  file_path: string;
  file_size: number;
  partition_value: string;
  row_end: number;
  row_start: number;
  snapshot_id: string;
  status: string;
  table_name: string;
}

const nowIso = (now: Date): string => now.toISOString();

export const createRun = async (
  db: D1Database,
  provider: Provider,
  runDate: string,
  trigger: TriggerKind,
  lookbackDays: number,
  now: Date,
  force: boolean,
  fromTime: string | null = null,
  toTime: string | null = null,
  cursorTime: string | null = null,
  advanceCursor = false,
): Promise<{ created: boolean; run: RunRow }> => {
  const runId = crypto.randomUUID();
  const dedupeKey = force
    ? `${provider}:${runDate}:${trigger}:${runId}`
    : `${provider}:${runDate}:daily`;
  const timestamp = nowIso(now);
  const result = await db
    .prepare(
      `insert or ignore into sync_runs (
         run_id, dedupe_key, provider, run_date, trigger_kind, lookback_days,
         from_time, to_time, cursor_time, advance_cursor, status, created_at, updated_at
       ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)`,
    )
    .bind(
      runId,
      dedupeKey,
      provider,
      runDate,
      trigger,
      lookbackDays,
      fromTime,
      toTime,
      cursorTime,
      advanceCursor ? 1 : 0,
      timestamp,
      timestamp,
    )
    .run();
  const run = await db
    .prepare("select * from sync_runs where dedupe_key = ?")
    .bind(dedupeKey)
    .first<RunRow>();
  if (run === null) throw new Error("D1 did not return the created sync run");
  return { created: result.meta.changes === 1, run };
};

export const getRun = async (db: D1Database, runId: string): Promise<RunRow> => {
  const run = await db
    .prepare("select * from sync_runs where run_id = ?")
    .bind(runId)
    .first<RunRow>();
  if (run === null) throw new Error("Sync run not found");
  return run;
};

export const getDailyRun = async (
  db: D1Database,
  provider: Provider,
  runDate: string,
): Promise<RunRow | null> =>
  await db
    .prepare("select * from sync_runs where dedupe_key = ?")
    .bind(`${provider}:${runDate}:daily`)
    .first<RunRow>();

export const getLatestRun = async (
  db: D1Database,
  provider: Provider,
  runDate: string,
): Promise<RunRow | null> =>
  await db
    .prepare(
      "select * from sync_runs where provider = ? and run_date = ? order by created_at desc, rowid desc limit 1",
    )
    .bind(provider, runDate)
    .first<RunRow>();

export const getProviderAcquisitionCursor = async (
  db: D1Database,
  provider: Provider,
): Promise<string | null> => {
  const row = await db
    .prepare("select last_acquired_at from provider_acquisition_cursors where provider = ?")
    .bind(provider)
    .first<{ last_acquired_at: string }>();
  return row?.last_acquired_at ?? null;
};

export const advanceProviderAcquisitionCursor = async (
  db: D1Database,
  provider: Provider,
  cursorTime: string,
  now: Date,
): Promise<void> => {
  await db
    .prepare(
      `insert into provider_acquisition_cursors (provider, last_acquired_at, updated_at)
       values (?, ?, ?)
       on conflict(provider) do update set
         last_acquired_at = excluded.last_acquired_at,
         updated_at = excluded.updated_at
       where excluded.last_acquired_at > provider_acquisition_cursors.last_acquired_at`,
    )
    .bind(provider, cursorTime, nowIso(now))
    .run();
};

export const updateRunStatus = async (
  db: D1Database,
  runId: string,
  status: string,
  now: Date,
  errorStage: string | null = null,
): Promise<void> => {
  await db
    .prepare("update sync_runs set status = ?, error_stage = ?, updated_at = ? where run_id = ?")
    .bind(status, errorStage, nowIso(now), runId)
    .run();
};

export const markAcquired = async (
  db: D1Database,
  runId: string,
  stagingKey: string,
  now: Date,
): Promise<void> => {
  await db
    .prepare(
      "update sync_runs set status = 'acquired', staging_key = ?, error_stage = null, updated_at = ? where run_id = ?",
    )
    .bind(stagingKey, nowIso(now), runId)
    .run();
};

export const markParsed = async (
  db: D1Database,
  runId: string,
  files: number,
  records: number,
  tables: readonly RunTablePlan[],
  now: Date,
): Promise<void> => {
  const timestamp = nowIso(now);
  const statements = tables.map((table) =>
    db
      .prepare(
        `insert into sync_run_tables (
           run_id, table_name, staging_key, source_records, catalog_status, neon_status, updated_at
         ) values (?, ?, ?, ?, ?, ?, ?)
         on conflict(run_id, table_name) do update set
           staging_key = excluded.staging_key,
           source_records = excluded.source_records,
           catalog_status = excluded.catalog_status,
           neon_status = excluded.neon_status,
           updated_at = excluded.updated_at`,
      )
      .bind(
        runId,
        table.table_name,
        table.staging_key,
        table.source_records,
        table.catalog_status,
        table.neon_status,
        timestamp,
      ),
  );
  statements.push(
    ...tables.flatMap((table) =>
      table.partitions.map((partition) =>
        db
          .prepare(
            `insert or ignore into sync_run_table_partitions
             (run_id, table_name, partition_value) values (?, ?, ?)`,
          )
          .bind(runId, table.table_name, partition),
      ),
    ),
  );
  statements.push(
    db
      .prepare(
        `update sync_runs set status = 'catalog_pending', files = ?, records = ?,
         catalog_tables = ?, error_stage = null, updated_at = ? where run_id = ?`,
      )
      .bind(
        files,
        records,
        tables.filter((table) => table.catalog_status !== "not_configured").length,
        timestamp,
        runId,
      ),
  );
  await db.batch(statements);
};

export const getRunTable = async (
  db: D1Database,
  runId: string,
  tableName: string,
): Promise<RunTableRow> => {
  const row = await db
    .prepare(
      `select table_name, staging_key, source_records, catalog_status, neon_status
       from sync_run_tables where run_id = ? and table_name = ?`,
    )
    .bind(runId, tableName)
    .first<RunTableRow>();
  if (row === null) throw new Error("Sync run table not found");
  return row;
};

export const listRunTables = async (db: D1Database, runId: string): Promise<RunTableRow[]> => {
  const result = await db
    .prepare(
      `select table_name, staging_key, source_records, catalog_status, neon_status
       from sync_run_tables where run_id = ? order by table_name`,
    )
    .bind(runId)
    .all<RunTableRow>();
  return result.results;
};

export const listCatalogTargets = async (
  db: D1Database,
  provider: Provider,
): Promise<CatalogTargetRow[]> => {
  const result = await db
    .prepare(
      `select table_name, partition_field from catalog_targets
       where provider = ? and enabled = 1 order by table_name`,
    )
    .bind(provider)
    .all<CatalogTargetRow>();
  return result.results;
};

export const getRunTablePartitions = async (
  db: D1Database,
  runId: string,
  tableName: string,
): Promise<string[]> => {
  const result = await db
    .prepare(
      `select partition_value as value from sync_run_table_partitions
       where run_id = ? and table_name = ? order by partition_value`,
    )
    .bind(runId, tableName)
    .all<{ value: string }>();
  return result.results.map((row) => row.value);
};

export const getReadyIndexPartitions = async (
  db: D1Database,
  tableName: string,
  partitions: readonly string[],
): Promise<Set<string>> => {
  if (partitions.length === 0) return new Set<string>();
  const placeholders = partitions.map(() => "?").join(", ");
  const result = await db
    .prepare(
      `select partition_value from catalog_index_partitions
       where table_name = ? and status = 'ready' and partition_value in (${placeholders})`,
    )
    .bind(tableName, ...partitions)
    .all<{ partition_value: string }>();
  return new Set(result.results.map((row) => row.partition_value));
};

export interface CatalogWaiterRow {
  provider: Provider;
  run_date: string;
  run_id: string;
  staging_key: string;
  table_name: string;
}

export const listCatalogWaiters = async (
  db: D1Database,
  tableName: string,
): Promise<CatalogWaiterRow[]> => {
  const result = await db
    .prepare(
      `select r.provider, r.run_date, t.run_id, t.table_name, t.staging_key
       from sync_run_tables t join sync_runs r on r.run_id = t.run_id
       where t.table_name = ? and t.catalog_status = 'index_pending'
       and not exists (
         select 1 from sync_run_table_partitions p
         left join catalog_index_partitions i
           on i.table_name = p.table_name and i.partition_value = p.partition_value
         where p.run_id = t.run_id and p.table_name = t.table_name
           and (i.status is null or i.status != 'ready')
       )`,
    )
    .bind(tableName)
    .all<CatalogWaiterRow>();
  return result.results;
};

export const setRunTableCatalogStatus = async (
  db: D1Database,
  runId: string,
  tableName: string,
  status: string,
  now: Date,
): Promise<void> => {
  await db
    .prepare(
      `update sync_run_tables set catalog_status = ?, error_stage = null, updated_at = ?
       where run_id = ? and table_name = ?`,
    )
    .bind(status, nowIso(now), runId, tableName)
    .run();
};

export const setRunTableNeonStatus = async (
  db: D1Database,
  runId: string,
  tableName: string,
  status: string,
  now: Date,
): Promise<void> => {
  await db
    .prepare(
      `update sync_run_tables set neon_status = ?, updated_at = ?
       where run_id = ? and table_name = ?`,
    )
    .bind(status, nowIso(now), runId, tableName)
    .run();
};

export const markCatalogTable = async (
  db: D1Database,
  runId: string,
  tableName: string,
  snapshotId: string,
  deletedRows: number,
  now: Date,
): Promise<void> => {
  await db
    .prepare(
      `update sync_run_tables set catalog_status = 'succeeded', catalog_snapshot_id = ?,
       catalog_deleted_rows = ?, error_stage = null, updated_at = ?
       where run_id = ? and table_name = ?`,
    )
    .bind(snapshotId, deletedRows, nowIso(now), runId, tableName)
    .run();
};

export const markNeonTable = async (
  db: D1Database,
  runId: string,
  tableName: string,
  now: Date,
): Promise<void> => {
  await db
    .prepare(
      `update sync_run_tables set neon_status = 'succeeded', error_stage = null, updated_at = ?
       where run_id = ? and table_name = ?`,
    )
    .bind(nowIso(now), runId, tableName)
    .run();
};

export const markTableFailure = async (
  db: D1Database,
  runId: string,
  tableName: string,
  stage: "catalog" | "neon",
  now: Date,
): Promise<void> => {
  await db
    .prepare(
      `update sync_run_tables set ${stage}_status = 'failed', error_stage = ?, updated_at = ?
       where run_id = ? and table_name = ?`,
    )
    .bind(stage, nowIso(now), runId, tableName)
    .run();
  await updateRunStatus(db, runId, `${stage}_failed`, now, stage);
};

export interface CatalogOperationRow {
  base_snapshot_id: string;
  committed_snapshot_id: string | null;
  deleted_rows: number;
  status: string;
}

export const getCatalogOperation = async (
  db: D1Database,
  runId: string,
  tableName: string,
): Promise<CatalogOperationRow | null> =>
  await db
    .prepare(
      `select base_snapshot_id, committed_snapshot_id, deleted_rows, status from catalog_operations
       where run_id = ? and table_name = ?`,
    )
    .bind(runId, tableName)
    .first<CatalogOperationRow>();

export const startCatalogOperation = async (
  db: D1Database,
  runId: string,
  tableName: string,
  baseSnapshotId: string,
  now: Date,
): Promise<void> => {
  await db
    .prepare(
      `insert or ignore into catalog_operations
       (run_id, table_name, base_snapshot_id, status, updated_at)
       values (?, ?, ?, 'planned', ?)`,
    )
    .bind(runId, tableName, baseSnapshotId, nowIso(now))
    .run();
};

export const markCatalogOperationCommitted = async (
  db: D1Database,
  runId: string,
  tableName: string,
  snapshotId: string,
  deletedRows: number,
  now: Date,
): Promise<void> => {
  await db
    .prepare(
      `update catalog_operations set committed_snapshot_id = ?, deleted_rows = ?,
       status = 'committed', updated_at = ? where run_id = ? and table_name = ?`,
    )
    .bind(snapshotId, deletedRows, nowIso(now), runId, tableName)
    .run();
};

export const markCatalogOperationIndexed = async (
  db: D1Database,
  runId: string,
  tableName: string,
  now: Date,
): Promise<void> => {
  await db
    .prepare(
      `update catalog_operations set status = 'indexed', updated_at = ?
       where run_id = ? and table_name = ?`,
    )
    .bind(nowIso(now), runId, tableName)
    .run();
};

export const acquireCatalogLease = async (
  db: D1Database,
  tableName: string,
  ownerId: string,
  now: Date,
): Promise<boolean> => {
  const expiresAt = new Date(now.getTime() + 14 * 60 * 1000).toISOString();
  const result = await db
    .prepare(
      `insert into catalog_table_leases (table_name, owner_id, expires_at, updated_at)
       values (?, ?, ?, ?)
       on conflict(table_name) do update set
         owner_id = excluded.owner_id,
         expires_at = excluded.expires_at,
         updated_at = excluded.updated_at
       where catalog_table_leases.expires_at <= excluded.updated_at
          or catalog_table_leases.owner_id = excluded.owner_id`,
    )
    .bind(tableName, ownerId, expiresAt, nowIso(now))
    .run();
  return result.meta.changes === 1;
};

export const releaseCatalogLease = async (
  db: D1Database,
  tableName: string,
  ownerId: string,
): Promise<void> => {
  await db
    .prepare("delete from catalog_table_leases where table_name = ? and owner_id = ?")
    .bind(tableName, ownerId)
    .run();
};

// Do not interrupt an in-flight rebuild. Only ready indexes are invalidated.
export const invalidateReadyIndexPartitions = async (
  db: D1Database,
  tableName: string,
  partitions: readonly string[],
  now: Date,
): Promise<void> => {
  for (const partition of partitions)
    await db
      .prepare(
        `update catalog_index_partitions set status = 'pending', updated_at = ?
         where table_name = ? and partition_value = ? and status = 'ready'`,
      )
      .bind(nowIso(now), tableName, partition)
      .run();
};

export const beginIndexPartition = async (
  db: D1Database,
  tableName: string,
  partitionValue: string,
  now: Date,
): Promise<boolean> => {
  const timestamp = nowIso(now);
  const staleBefore = new Date(now.getTime() - 20 * 60 * 1000).toISOString();
  const result = await db
    .prepare(
      `insert into catalog_index_partitions
       (table_name, partition_value, status, updated_at)
       values (?, ?, 'planning', ?)
       on conflict(table_name, partition_value) do update set
         status = 'planning', updated_at = excluded.updated_at
       where catalog_index_partitions.status in ('pending', 'failed')
          or catalog_index_partitions.updated_at <= ?`,
    )
    .bind(tableName, partitionValue, timestamp, staleBefore)
    .run();
  return result.meta.changes === 1;
};

export const clearIndexPartition = async (
  db: D1Database,
  tableName: string,
  partitionValue: string,
): Promise<void> => {
  await db.batch([
    db
      .prepare("delete from catalog_index_chunks where table_name = ? and partition_value = ?")
      .bind(tableName, partitionValue),
    db
      .prepare("delete from catalog_row_index where table_name = ? and partition_value = ?")
      .bind(tableName, partitionValue),
  ]);
};

export const saveIndexChunks = async (
  db: D1Database,
  tableName: string,
  partitionValue: string,
  snapshotId: string,
  chunks: readonly IndexChunkRow[],
  now: Date,
): Promise<void> => {
  const timestamp = nowIso(now);
  const statements = chunks.map((chunk) =>
    db
      .prepare(
        `insert or ignore into catalog_index_chunks
         (chunk_id, table_name, partition_value, file_path, file_size, row_start, row_end,
          snapshot_id, status, updated_at)
         values (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)`,
      )
      .bind(
        chunk.chunk_id,
        tableName,
        partitionValue,
        chunk.file_path,
        chunk.file_size,
        chunk.row_start,
        chunk.row_end,
        snapshotId,
        timestamp,
      ),
  );
  for (let offset = 0; offset < statements.length; offset += 50)
    await db.batch(statements.slice(offset, offset + 50));
  await db
    .prepare(
      `update catalog_index_partitions set status = ?, catalog_snapshot_id = ?,
       total_chunks = ?, completed_chunks = 0, updated_at = ?
       where table_name = ? and partition_value = ?`,
    )
    .bind(
      chunks.length === 0 ? "ready" : "building",
      snapshotId,
      chunks.length,
      timestamp,
      tableName,
      partitionValue,
    )
    .run();
};

export interface IndexPartitionRow {
  catalog_snapshot_id: string | null;
  completed_chunks: number;
  status: string;
  total_chunks: number;
}

export const getIndexPartition = async (
  db: D1Database,
  tableName: string,
  partitionValue: string,
): Promise<IndexPartitionRow | null> =>
  await db
    .prepare(
      `select catalog_snapshot_id, status, total_chunks, completed_chunks
       from catalog_index_partitions
       where table_name = ? and partition_value = ?`,
    )
    .bind(tableName, partitionValue)
    .first<IndexPartitionRow>();

export const updateReadyIndexPartitionSnapshots = async (
  db: D1Database,
  tableName: string,
  baseSnapshotId: string,
  snapshotId: string,
  now: Date,
): Promise<void> => {
  // Snapshots are table-wide. Unchanged partitions remain valid after our
  // atomic upsert, but only indexes proven valid at its base may advance.
  await db
    .prepare(
      `update catalog_index_partitions set catalog_snapshot_id = ?, updated_at = ?
       where table_name = ? and status = 'ready' and catalog_snapshot_id = ?`,
    )
    .bind(snapshotId, now.toISOString(), tableName, baseSnapshotId)
    .run();
};

export const listPendingIndexChunks = async (
  db: D1Database,
  tableName: string,
  partitionValue: string,
): Promise<IndexChunkRow[]> => {
  const result = await db
    .prepare(
      `select * from catalog_index_chunks
       where table_name = ? and partition_value = ? and status = 'pending'
       order by file_path, row_start`,
    )
    .bind(tableName, partitionValue)
    .all<IndexChunkRow>();
  return result.results;
};

export const finalizeIndexPartition = async (
  db: D1Database,
  tableName: string,
  partitionValue: string,
  now: Date,
): Promise<boolean> => {
  const result = await db
    .prepare(
      `update catalog_index_partitions set status = 'ready', updated_at = ?
       where table_name = ? and partition_value = ? and total_chunks = completed_chunks`,
    )
    .bind(nowIso(now), tableName, partitionValue)
    .run();
  return result.meta.changes === 1;
};

export const getIndexChunk = async (db: D1Database, chunkId: string): Promise<IndexChunkRow> => {
  const chunk = await db
    .prepare("select * from catalog_index_chunks where chunk_id = ?")
    .bind(chunkId)
    .first<IndexChunkRow>();
  if (chunk === null) throw new Error("Catalog index chunk not found");
  return chunk;
};

export interface CatalogIndexRow {
  filePath: string;
  partitionValue: string;
  position: number;
  rowKey: string;
}

export const upsertCatalogIndex = async (
  db: D1Database,
  tableName: string,
  snapshotId: string,
  rows: readonly CatalogIndexRow[],
  now: Date,
): Promise<void> => {
  const timestamp = nowIso(now);
  for (let pageOffset = 0; pageOffset < rows.length; pageOffset += 1_000) {
    const statements: D1PreparedStatement[] = [];
    const page = rows.slice(pageOffset, pageOffset + 1_000);
    for (let offset = 0; offset < page.length; offset += 10) {
      const batch = page.slice(offset, offset + 10);
      const placeholders = batch.map(() => "(?, ?, ?, ?, ?, ?, ?)").join(", ");
      statements.push(
        db
          .prepare(
            `insert into catalog_row_index
             (table_name, row_key, partition_value, file_path, row_position,
              catalog_snapshot_id, updated_at) values ${placeholders}
             on conflict(table_name, row_key) do update set
               partition_value = excluded.partition_value,
               file_path = excluded.file_path,
               row_position = excluded.row_position,
               catalog_snapshot_id = excluded.catalog_snapshot_id,
               updated_at = excluded.updated_at`,
          )
          .bind(
            ...batch.flatMap((row) => [
              tableName,
              row.rowKey,
              row.partitionValue,
              row.filePath,
              row.position,
              snapshotId,
              timestamp,
            ]),
          ),
      );
    }
    await db.batch(statements);
  }
};

export const completeIndexChunk = async (
  db: D1Database,
  chunk: IndexChunkRow,
  now: Date,
): Promise<boolean> => {
  const timestamp = nowIso(now);
  await db.batch([
    db
      .prepare(
        `update catalog_index_chunks set status = 'succeeded', updated_at = ?
         where chunk_id = ? and status != 'succeeded'`,
      )
      .bind(timestamp, chunk.chunk_id),
    db
      .prepare(
        `update catalog_index_partitions set completed_chunks = (
           select count(*) from catalog_index_chunks
           where table_name = ? and partition_value = ? and status = 'succeeded'
         ), updated_at = ? where table_name = ? and partition_value = ?`,
      )
      .bind(
        chunk.table_name,
        chunk.partition_value,
        timestamp,
        chunk.table_name,
        chunk.partition_value,
      ),
  ]);
  const remaining = await db
    .prepare(
      `select count(*) as count from catalog_index_chunks
       where table_name = ? and partition_value = ? and status != 'succeeded'`,
    )
    .bind(chunk.table_name, chunk.partition_value)
    .first<{ count: number }>();
  if ((remaining?.count ?? 0) !== 0) return false;
  await db
    .prepare(
      `update catalog_index_partitions set status = 'ready', updated_at = ?
       where table_name = ? and partition_value = ?`,
    )
    .bind(timestamp, chunk.table_name, chunk.partition_value)
    .run();
  return true;
};

export const findCatalogPositions = async (
  db: D1Database,
  tableName: string,
  rowKeys: readonly string[],
): Promise<{ file_path: string; row_position: number; row_key: string }[]> => {
  const positions: { file_path: string; row_position: number; row_key: string }[] = [];
  for (let offset = 0; offset < rowKeys.length; offset += 50) {
    const keys = rowKeys.slice(offset, offset + 50);
    const placeholders = keys.map(() => "?").join(", ");
    const result = await db
      .prepare(
        `select row_key, file_path, row_position from catalog_row_index
         where table_name = ? and row_key in (${placeholders})`,
      )
      .bind(tableName, ...keys)
      .all<{ file_path: string; row_position: number; row_key: string }>();
    positions.push(...result.results);
  }
  return positions;
};

export const completeRun = async (
  db: D1Database,
  runId: string,
  status: "succeeded" | "succeeded_empty",
  neonTables: number,
  now: Date,
): Promise<void> => {
  const timestamp = nowIso(now);
  await db
    .prepare(
      `update sync_runs set status = ?, neon_tables = ?, error_stage = null,
       completed_at = ?, updated_at = ? where run_id = ?`,
    )
    .bind(status, neonTables, timestamp, timestamp, runId)
    .run();
};
