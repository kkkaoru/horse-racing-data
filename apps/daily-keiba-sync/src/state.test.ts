import { readFile } from "node:fs/promises";
import { Miniflare } from "miniflare";
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import {
  acquireCatalogLease,
  advanceProviderAcquisitionCursor,
  beginIndexPartition,
  clearIndexPartition,
  completeIndexChunk,
  completeRun,
  createRun,
  finalizeIndexPartition,
  findCatalogPositions,
  getCatalogOperation,
  getDailyRun,
  getIndexChunk,
  getIndexPartition,
  getLatestRun,
  getProviderAcquisitionCursor,
  getReadyIndexPartitions,
  getRun,
  getRunTable,
  getRunTablePartitions,
  listCatalogTargets,
  listCatalogWaiters,
  listRunTables,
  markAcquired,
  markCatalogOperationCommitted,
  markCatalogOperationIndexed,
  markCatalogTable,
  markNeonTable,
  markParsed,
  markTableFailure,
  releaseCatalogLease,
  saveIndexChunks,
  setRunTableCatalogStatus,
  startCatalogOperation,
  updateReadyIndexPartitionSnapshots,
  updateRunStatus,
  upsertCatalogIndex,
} from "./state";
import type { IndexChunkRow } from "./state";

let miniflare: Miniflare;
let db: D1Database;
const now = new Date("2026-09-03T11:00:00.000Z");

beforeAll(async () => {
  miniflare = new Miniflare({
    compatibilityDate: "2026-06-18",
    d1Databases: { DB: "state-test" },
    modules: true,
    script: "export default {}",
  });
  db = await miniflare.getD1Database("DB");
  for (const name of [
    "0001_initial.sql",
    "0002_catalog_index.sql",
    "0003_acquisition_window.sql",
  ]) {
    const migration = await readFile(new URL(`../migrations/${name}`, import.meta.url), "utf8");
    await db.exec(
      migration
        .split("\n")
        .filter((line) => !line.trimStart().startsWith("--"))
        .join(" "),
    );
  }
});

beforeEach(async () => {
  await db.exec(
    "delete from catalog_table_leases; delete from catalog_operations; delete from catalog_row_index; delete from catalog_index_chunks; delete from catalog_index_partitions; delete from sync_run_table_partitions; delete from sync_run_tables; delete from sync_runs; delete from provider_acquisition_cursors;",
  );
});

afterAll(async () => {
  await miniflare.dispose();
});

describe("D1 sync state", () => {
  test("deduplicates daily runs and permits forced manual runs", async () => {
    const first = await createRun(
      db,
      "nv",
      "20260903",
      "daily",
      2,
      now,
      false,
      "20260901000000",
      null,
      "20260903010000",
      true,
    );
    const duplicate = await createRun(db, "nv", "20260903", "daily", 2, now, false);
    const manual = await createRun(
      db,
      "nv",
      "20260903",
      "manual",
      7,
      now,
      true,
      "20260902123456",
      null,
      "20260903123456",
      false,
    );

    expect(first.created).toBe(true);
    expect(duplicate.created).toBe(false);
    expect(duplicate.run.run_id).toBe(first.run.run_id);
    expect(manual.created).toBe(true);
    expect(manual.run.run_id).not.toBe(first.run.run_id);
    expect(manual.run).toMatchObject({
      advance_cursor: 0,
      cursor_time: "20260903123456",
      from_time: "20260902123456",
      to_time: null,
    });
    expect((await getDailyRun(db, "nv", "20260903"))?.run_id).toBe(first.run.run_id);
    expect((await getLatestRun(db, "nv", "20260903"))?.run_id).toBe(manual.run.run_id);
    expect(await getDailyRun(db, "jv", "20260903")).toBeNull();
  });

  test("advances each provider cursor monotonically", async () => {
    expect(await getProviderAcquisitionCursor(db, "nv")).toBeNull();
    await advanceProviderAcquisitionCursor(db, "nv", "20260903010000", now);
    await advanceProviderAcquisitionCursor(db, "nv", "20260902010000", now);
    expect(await getProviderAcquisitionCursor(db, "nv")).toBe("20260903010000");
    expect(await getProviderAcquisitionCursor(db, "jv")).toBeNull();
  });

  test("tracks acquisition, table publication, Neon, and completion", async () => {
    const { run } = await createRun(db, "jv", "20260903", "daily", 2, now, false);
    await updateRunStatus(db, run.run_id, "acquiring", now);
    await markAcquired(db, run.run_id, "source.ndjson", now);
    await markParsed(
      db,
      run.run_id,
      2,
      3,
      [
        {
          catalog_status: "pending",
          neon_status: "pending",
          partitions: ["2026"],
          source_records: 3,
          staging_key: "ra.json",
          table_name: "jvd_ra",
        },
      ],
      now,
    );
    expect(await listRunTables(db, run.run_id)).toEqual([
      {
        catalog_status: "pending",
        neon_status: "pending",
        source_records: 3,
        staging_key: "ra.json",
        table_name: "jvd_ra",
      },
    ]);

    await markCatalogTable(db, run.run_id, "jvd_ra", "123", 2, now);
    await markNeonTable(db, run.run_id, "jvd_ra", now);
    await completeRun(db, run.run_id, "succeeded", 1, now);
    const completed = await getRun(db, run.run_id);
    expect(completed).toMatchObject({
      catalog_tables: 1,
      completed_at: "2026-09-03T11:00:00.000Z",
      files: 2,
      neon_tables: 1,
      records: 3,
      staging_key: "source.ndjson",
      status: "succeeded",
    });
  });

  test("records catalog and Neon failures without storing error details", async () => {
    const { run } = await createRun(db, "nv", "20260903", "daily", 2, now, false);
    await markParsed(
      db,
      run.run_id,
      1,
      1,
      [
        {
          catalog_status: "pending",
          neon_status: "pending",
          partitions: ["2026"],
          source_records: 1,
          staging_key: "ra.json",
          table_name: "nvd_ra",
        },
      ],
      now,
    );
    await markTableFailure(db, run.run_id, "nvd_ra", "catalog", now);
    expect((await getRun(db, run.run_id)).status).toBe("catalog_failed");
    await markTableFailure(db, run.run_id, "nvd_ra", "neon", now);
    expect((await getRun(db, run.run_id)).status).toBe("neon_failed");
    await updateRunStatus(db, run.run_id, "parse_failed", now, "parse");
    expect(await getRun(db, run.run_id)).toMatchObject({
      error_stage: "parse",
      status: "parse_failed",
    });
  });

  test("tracks configured targets, required partitions, and waiting runs", async () => {
    const { run } = await createRun(db, "nv", "20260903", "daily", 2, now, false);
    await markParsed(
      db,
      run.run_id,
      1,
      1,
      [
        {
          catalog_status: "index_pending",
          neon_status: "pending",
          partitions: ["2025", "2026"],
          source_records: 1,
          staging_key: "ra.json",
          table_name: "nvd_ra",
        },
      ],
      now,
    );
    expect(await listCatalogTargets(db, "nv")).toHaveLength(2);
    expect((await getRunTable(db, run.run_id, "nvd_ra")).catalog_status).toBe("index_pending");
    expect(await getRunTablePartitions(db, run.run_id, "nvd_ra")).toEqual(["2025", "2026"]);
    expect(await getReadyIndexPartitions(db, "nvd_ra", [])).toEqual(new Set());
    expect(await listCatalogWaiters(db, "nvd_ra")).toEqual([]);
    for (const partition of ["2025", "2026"])
      await db
        .prepare(
          `insert into catalog_index_partitions
           (table_name, partition_value, status, updated_at) values ('nvd_ra', ?, 'ready', ?)`,
        )
        .bind(partition, now.toISOString())
        .run();
    expect(await getReadyIndexPartitions(db, "nvd_ra", ["2025", "2026"])).toEqual(
      new Set(["2025", "2026"]),
    );
    expect(await listCatalogWaiters(db, "nvd_ra")).toHaveLength(1);
    await setRunTableCatalogStatus(db, run.run_id, "nvd_ra", "pending", now);
    expect((await getRunTable(db, run.run_id, "nvd_ra")).catalog_status).toBe("pending");
    await expect(getRunTable(db, run.run_id, "missing")).rejects.toThrow("not found");
  });

  test("advances ready partition snapshots after an in-Worker commit", async () => {
    await db
      .prepare(
        `insert into catalog_index_partitions
         (table_name, partition_value, status, catalog_snapshot_id, updated_at)
         values ('nvd_ra', '2026', 'ready', '10', '2026-09-03T00:01:00.000Z'),
                ('nvd_ra', '2025', 'ready', '10', '2026-09-03T00:01:00.000Z'),
                ('nvd_ra', '2024', 'ready', '9', '2026-09-03T00:01:00.000Z'),
                ('nvd_ra', '2023', 'building', '10', '2026-09-03T00:01:00.000Z')`,
      )
      .run();
    await updateReadyIndexPartitionSnapshots(
      db,
      "nvd_ra",
      "10",
      "11",
      new Date("2026-09-03T00:02:00Z"),
    );
    expect(await getIndexPartition(db, "nvd_ra", "2026")).toMatchObject({
      catalog_snapshot_id: "11",
      status: "ready",
    });
    expect(await getIndexPartition(db, "nvd_ra", "2025")).toMatchObject({
      catalog_snapshot_id: "11",
      status: "ready",
    });
    expect(await getIndexPartition(db, "nvd_ra", "2024")).toMatchObject({
      catalog_snapshot_id: "9",
      status: "ready",
    });
    expect(await getIndexPartition(db, "nvd_ra", "2023")).toMatchObject({
      catalog_snapshot_id: "10",
      status: "building",
    });
  });

  test("serializes table leases and permits takeover only after expiry", async () => {
    expect(await acquireCatalogLease(db, "nvd_ra", "owner-1", now)).toBe(true);
    expect(await acquireCatalogLease(db, "nvd_ra", "owner-2", now)).toBe(false);
    expect(
      await acquireCatalogLease(db, "nvd_ra", "owner-2", new Date(now.getTime() + 15 * 60_000)),
    ).toBe(true);
    await releaseCatalogLease(db, "nvd_ra", "wrong-owner");
    expect(await acquireCatalogLease(db, "nvd_ra", "owner-3", now)).toBe(false);
    await releaseCatalogLease(db, "nvd_ra", "owner-2");
    expect(await acquireCatalogLease(db, "nvd_ra", "owner-3", now)).toBe(true);
  });

  test("persists resumable catalog operations", async () => {
    const { run } = await createRun(db, "nv", "20260903", "daily", 2, now, false);
    await markParsed(
      db,
      run.run_id,
      1,
      1,
      [
        {
          catalog_status: "pending",
          neon_status: "pending",
          partitions: ["2026"],
          source_records: 1,
          staging_key: "ra.json",
          table_name: "nvd_ra",
        },
      ],
      now,
    );
    expect(await getCatalogOperation(db, run.run_id, "nvd_ra")).toBeNull();
    await startCatalogOperation(db, run.run_id, "nvd_ra", "10", now);
    await startCatalogOperation(db, run.run_id, "nvd_ra", "11", now);
    await markCatalogOperationCommitted(db, run.run_id, "nvd_ra", "12", 3, now);
    expect(await getCatalogOperation(db, run.run_id, "nvd_ra")).toEqual({
      base_snapshot_id: "10",
      committed_snapshot_id: "12",
      deleted_rows: 3,
      status: "committed",
    });
    await markCatalogOperationIndexed(db, run.run_id, "nvd_ra", now);
    expect((await getCatalogOperation(db, run.run_id, "nvd_ra"))?.status).toBe("indexed");
  });

  test("builds partition index chunks and primary-key positions idempotently", async () => {
    expect(await beginIndexPartition(db, "nvd_ra", "2026", now)).toBe(true);
    expect(await beginIndexPartition(db, "nvd_ra", "2026", now)).toBe(false);
    const chunk = (id: string, start: number, end: number): IndexChunkRow => ({
      chunk_id: id,
      file_path: "s3://catalog/data.parquet",
      file_size: 100,
      partition_value: "2026",
      row_end: end,
      row_start: start,
      snapshot_id: "10",
      status: "pending",
      table_name: "nvd_ra",
    });
    const firstChunk = chunk("chunk-1", 0, 2);
    const secondChunk = chunk("chunk-2", 2, 4);
    const chunks = [firstChunk, secondChunk];
    await saveIndexChunks(db, "nvd_ra", "2026", "10", chunks, now);
    expect((await getIndexChunk(db, "chunk-1")).row_end).toBe(2);
    await upsertCatalogIndex(
      db,
      "nvd_ra",
      "10",
      [
        { filePath: "file-1", partitionValue: "2026", position: 0, rowKey: "key-1" },
        { filePath: "file-1", partitionValue: "2026", position: 1, rowKey: "key-2" },
      ],
      now,
    );
    await upsertCatalogIndex(
      db,
      "nvd_ra",
      "11",
      [{ filePath: "file-2", partitionValue: "2026", position: 2, rowKey: "key-1" }],
      now,
    );
    expect(await findCatalogPositions(db, "nvd_ra", ["key-1", "missing"])).toEqual([
      { file_path: "file-2", row_key: "key-1", row_position: 2 },
    ]);
    expect(await completeIndexChunk(db, firstChunk, now)).toBe(false);
    expect(await completeIndexChunk(db, secondChunk, now)).toBe(true);
    expect(await completeIndexChunk(db, secondChunk, now)).toBe(true);
    await expect(getIndexChunk(db, "missing")).rejects.toThrow("not found");
    await clearIndexPartition(db, "nvd_ra", "2026");
    expect(await findCatalogPositions(db, "nvd_ra", ["key-1"])).toEqual([]);
  });

  test("marks an index with no files ready and restarts failed planning", async () => {
    expect(await beginIndexPartition(db, "nvd_ra", "2026", now)).toBe(true);
    await saveIndexChunks(db, "nvd_ra", "2026", "10", [], now);
    expect(await getReadyIndexPartitions(db, "nvd_ra", ["2026"])).toEqual(new Set(["2026"]));
    expect(await finalizeIndexPartition(db, "nvd_ra", "2026", now)).toBe(true);
    await db
      .prepare(
        "update catalog_index_partitions set status = 'failed' where table_name = 'nvd_ra' and partition_value = '2026'",
      )
      .run();
    expect(await beginIndexPartition(db, "nvd_ra", "2026", now)).toBe(true);
    await upsertCatalogIndex(db, "nvd_ra", "10", [], now);
  });

  test("throws for an unknown run", async () => {
    await expect(getRun(db, "missing")).rejects.toThrow("not found");
  });
});
