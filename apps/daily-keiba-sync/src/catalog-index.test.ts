// Runs with bun via the package Vitest scripts.
import { readFile } from "node:fs/promises";
import { Miniflare } from "miniflare";
import { afterAll, beforeAll, beforeEach, describe, expect, test, vi } from "vitest";
import { indexCatalogFile, planCatalogIndex } from "./catalog-index";
import { PermanentJobError } from "./errors";
import { createRun, markParsed } from "./state";
import type { Env, IndexPlanJob, SyncJob } from "./types";

const mocks = vi.hoisted(() => ({
  load: vi.fn(),
  deleteMaps: vi.fn(),
  manifests: vi.fn(),
  readObjects: vi.fn(),
}));

vi.mock("./catalog-sync", () => ({ loadCatalogTable: mocks.load }));
vi.mock("icebird", () => ({ icebergManifests: mocks.manifests }));
vi.mock("hyparquet", () => ({ parquetReadObjects: mocks.readObjects }));
vi.mock("icebird/src/fetch.js", () => ({ fetchDeleteMaps: mocks.deleteMaps }));

class RecordingQueue implements Queue<SyncJob> {
  readonly messages: SyncJob[] = [];
  async metrics(): Promise<QueueMetrics> {
    return { backlogBytes: 0, backlogCount: this.messages.length };
  }
  async send(message: SyncJob): Promise<QueueSendResponse> {
    this.messages.push(message);
    return { metadata: { metrics: await this.metrics() } };
  }
  async sendBatch(
    messages: Iterable<MessageSendRequest<SyncJob>>,
  ): Promise<QueueSendBatchResponse> {
    for (const message of messages) this.messages.push(message.body);
    return { metadata: { metrics: await this.metrics() } };
  }
}

let miniflare: Miniflare;
let env: Env;
let queue: RecordingQueue;
const now = new Date("2026-09-03T16:00:00.000Z");

beforeAll(async () => {
  miniflare = new Miniflare({
    bindings: {
      ADMIN_TOKEN: "admin",
      JRA_VAN_WORKER_API_TOKEN: "jv",
      NEON_DATABASE_URL: "postgresql://example",
      R2_BUCKET_NAME: "catalog",
      R2_CATALOG_NAMESPACE: "pc_keiba",
      R2_CATALOG_TOKEN: "token",
      R2_CATALOG_URI: "https://catalog.example",
      R2_CATALOG_WAREHOUSE: "warehouse",
      UMMACON_WORKER_API_TOKEN: "nv",
    },
    compatibilityDate: "2026-06-18",
    d1Databases: { DB: "catalog-index-test" },
    modules: true,
    r2Buckets: { CATALOG_BUCKET: "catalog", SOURCE_STAGING: "staging" },
    script: "export default {}",
    serviceBindings: {
      JV_SOURCE: async () => new Response(),
      NV_SOURCE: async () => new Response(),
    },
  });
  env = await miniflare.getBindings<Env>();
  for (const name of [
    "0001_initial.sql",
    "0002_catalog_index.sql",
    "0003_acquisition_window.sql",
    "0005_acquisition_data_spec.sql",
  ]) {
    const migration = await readFile(new URL(`../migrations/${name}`, import.meta.url), "utf8");
    await env.DB.exec(
      migration
        .split("\n")
        .filter((line) => !line.trimStart().startsWith("--"))
        .join(" "),
    );
  }
  await env.DB.prepare(
    "insert into catalog_targets (table_name, provider, partition_field, created_at) values ('jvd_cs', 'jv', '__unpartitioned__', ?)",
  )
    .bind(now.toISOString())
    .run();
});

beforeEach(async () => {
  await env.DB.exec(
    "delete from catalog_table_leases; delete from catalog_operations; delete from catalog_row_index; delete from catalog_index_chunks; delete from catalog_index_partitions; delete from sync_run_table_partitions; delete from sync_run_tables; delete from sync_runs;",
  );
  queue = new RecordingQueue();
  env = {
    ...env,
    JV_RAW_STAGE_JOBS: queue,
    NEON_JOBS: queue,
    NV_RAW_STAGE_JOBS: queue,
    R2_CATALOG_JOBS: queue,
  };
  mocks.load
    .mockReset()
    .mockResolvedValue({ metadata: { "current-snapshot-id": 10n }, resolver: { reader: vi.fn() } });
  mocks.deleteMaps
    .mockReset()
    .mockResolvedValue({ positionDeletesMap: new Map(), equalityDeleteGroups: [] });
  mocks.manifests.mockReset().mockResolvedValue([
    {
      entries: [
        {
          data_file: {
            content: 0,
            file_path: "s3://catalog/data.parquet",
            file_size_in_bytes: 100n,
            partition: { kaisai_nen: "2026" },
            record_count: 2_500n,
          },
          sequence_number: 1n,
          status: 1,
        },
      ],
    },
  ]);
  mocks.readObjects
    .mockReset()
    .mockResolvedValue([
      { kaisai_nen: "2026", kaisai_tsukihi: "0903", keibajo_code: "30", race_bango: "01" },
    ]);
});

afterAll(async () => {
  await miniflare.dispose();
});

const prepareWaitingRun = async (): Promise<string> => {
  const { run } = await createRun(env.DB, "nv", "20260904", "daily", 2, now, false);
  await markParsed(
    env.DB,
    run.run_id,
    1,
    1,
    [
      {
        catalog_status: "index_pending",
        neon_status: "pending",
        partitions: ["2026"],
        source_records: 1,
        staging_key: "table.json",
        table_name: "nvd_ra",
      },
    ],
    now,
  );
  return run.run_id;
};

const planJob = (runId: string): IndexPlanJob => ({
  partitionValue: "2026",
  provider: "nv",
  runDate: "20260904",
  runId,
  tableName: "nvd_ra",
  type: "index-plan",
});

describe("partition catalog index", () => {
  test("plans and indexes unpartitioned course files without a physical year column", async () => {
    const job: IndexPlanJob = {
      ...planJob("course-run"),
      provider: "jv",
      tableName: "jvd_cs",
      partitionValue: "__all__",
    };
    mocks.manifests.mockResolvedValue([
      {
        entries: [
          {
            data_file: {
              content: 0,
              file_path: "s3://catalog/course.parquet",
              file_size_in_bytes: 100n,
              partition: {},
              record_count: 1n,
            },
            status: 1,
          },
        ],
      },
    ]);
    mocks.readObjects.mockResolvedValue([
      {
        keibajo_code: "05",
        kyori: "1600",
        track_code: "11",
        course_kaishu_nengappi: "20240106",
      },
    ]);
    await planCatalogIndex(job, env, now);
    expect(queue.messages).toHaveLength(1);
    const fileJob = queue.messages[0];
    if (fileJob?.type !== "index-file") throw new Error("Expected course index file job");
    await indexCatalogFile(fileJob, env, now);
    expect(mocks.readObjects).toHaveBeenCalledWith(
      expect.objectContaining({
        columns: ["keibajo_code", "kyori", "track_code", "course_kaishu_nengappi"],
      }),
    );
    expect(
      await env.DB.prepare(
        "select partition_value, row_key, row_position from catalog_row_index where table_name = 'jvd_cs'",
      ).first(),
    ).toStrictEqual({
      partition_value: "__all__",
      row_key: "05\u001f1600\u001f11\u001f20240106",
      row_position: 0,
    });
    expect(
      await env.DB.prepare(
        "select status, catalog_snapshot_id from catalog_index_partitions where table_name = 'jvd_cs'",
      ).first(),
    ).toStrictEqual({ status: "ready", catalog_snapshot_id: "10" });
    await indexCatalogFile(fileJob, env, now);
    expect(mocks.readObjects).toHaveBeenCalledTimes(1);
  });

  test("rejects a race-year course index request before recording state", async () => {
    await expect(
      planCatalogIndex(
        {
          ...planJob("course-run"),
          provider: "jv",
          tableName: "jvd_cs",
        },
        env,
        now,
      ),
    ).rejects.toMatchObject({ safeStage: "index-partition" });
    expect(mocks.load).not.toHaveBeenCalled();
    expect(
      await env.DB.prepare(
        "select count(*) as count from catalog_index_partitions where table_name = 'jvd_cs'",
      ).first(),
    ).toStrictEqual({ count: 0 });
  });

  test("fails closed on course files carrying unexpected partition values", async () => {
    await expect(
      planCatalogIndex(
        {
          ...planJob("course-run"),
          provider: "jv",
          tableName: "jvd_cs",
          partitionValue: "__all__",
        },
        env,
        now,
      ),
    ).rejects.toMatchObject({ safeStage: "index-partition" });
    expect(queue.messages).toHaveLength(0);
    expect(
      await env.DB.prepare(
        "select count(*) as count from catalog_index_partitions where table_name = 'jvd_cs' and status = 'ready'",
      ).first(),
    ).toStrictEqual({ count: 0 });
  });

  test("marks an actually empty course snapshot ready", async () => {
    mocks.manifests.mockResolvedValue([]);
    await planCatalogIndex(
      {
        ...planJob("course-run"),
        provider: "jv",
        tableName: "jvd_cs",
        partitionValue: "__all__",
      },
      env,
      now,
    );
    expect(queue.messages).toHaveLength(0);
    expect(
      await env.DB.prepare(
        "select status, catalog_snapshot_id, total_chunks from catalog_index_partitions where table_name = 'jvd_cs'",
      ).first(),
    ).toStrictEqual({ status: "ready", catalog_snapshot_id: "10", total_chunks: 0 });
  });

  test("plans one bounded job per Parquet file in the indexed partition", async () => {
    const runId = await prepareWaitingRun();
    await planCatalogIndex(planJob(runId), env, now);
    expect(queue.messages).toHaveLength(1);
    expect(queue.messages[0]).toMatchObject({ partitionValue: "2026", type: "index-file" });
    const state = await env.DB.prepare(
      "select status, total_chunks from catalog_index_partitions where table_name = 'nvd_ra'",
    ).first<{ status: string; total_chunks: number }>();
    expect(state).toEqual({ status: "building", total_chunks: 1 });
  });

  test("indexes absolute positions and wakes waiters after the final chunk", async () => {
    const runId = await prepareWaitingRun();
    await planCatalogIndex(planJob(runId), env, now);
    const jobs = queue.messages.filter((job) => job.type === "index-file");
    queue.messages.length = 0;
    for (const job of jobs) {
      if (job.type === "index-file") await indexCatalogFile(job, env, now);
    }
    const indexed = await env.DB.prepare(
      "select file_path, row_position from catalog_row_index where table_name = 'nvd_ra'",
    ).first<{ file_path: string; row_position: number }>();
    expect(indexed?.file_path).toBe("s3://catalog/data.parquet");
    expect(queue.messages.at(-1)).toMatchObject({ tableName: "nvd_ra", type: "catalog-table" });
  });

  test("marks an empty partition ready without file jobs", async () => {
    const runId = await prepareWaitingRun();
    mocks.manifests.mockResolvedValue([{ entries: [] }]);
    await planCatalogIndex(planJob(runId), env, now);
    expect(queue.messages).toEqual([
      expect.objectContaining({ tableName: "nvd_ra", type: "catalog-table" }),
    ]);
  });

  test("requeues pending file jobs for an index plan already in progress", async () => {
    const runId = await prepareWaitingRun();
    await planCatalogIndex(planJob(runId), env, now);
    const before = queue.messages.length;
    await planCatalogIndex(planJob(runId), env, now);
    expect(queue.messages).toHaveLength(before + 1);
  });

  test("ignores deleted, delete-file, and other-partition manifest entries", async () => {
    const runId = await prepareWaitingRun();
    mocks.manifests.mockResolvedValue([
      {
        entries: [
          { data_file: { content: 0, partition: { kaisai_nen: "2025" } }, status: 1 },
          { data_file: { content: 0, partition: { kaisai_nen: "2026" } }, status: 2 },
          { data_file: { content: 1, partition: { kaisai_nen: "2026" } }, status: 1 },
        ],
      },
    ]);
    await planCatalogIndex(planJob(runId), env, now);
    expect(queue.messages.at(-1)?.type).toBe("catalog-table");
  });

  test("rejects missing snapshots and malformed Parquet primary keys", async () => {
    const runId = await prepareWaitingRun();
    mocks.load.mockResolvedValueOnce({ metadata: {}, resolver: { reader: vi.fn() } });
    await expect(planCatalogIndex(planJob(runId), env, now)).rejects.toBeInstanceOf(
      PermanentJobError,
    );

    await env.DB.prepare(
      "update catalog_index_partitions set status = 'failed' where table_name = 'nvd_ra' and partition_value = '2026'",
    ).run();
    mocks.load.mockResolvedValue({
      metadata: { "current-snapshot-id": 10n },
      resolver: { reader: vi.fn() },
    });
    await planCatalogIndex(planJob(runId), env, now);
    const job = queue.messages.find((message) => message.type === "index-file");
    if (job?.type !== "index-file") throw new Error("Missing test index job");
    mocks.readObjects.mockResolvedValue([null]);
    await expect(indexCatalogFile(job, env, now)).rejects.toThrow("catalog index row");
    mocks.readObjects.mockResolvedValue([{ kaisai_nen: 2026 }]);
    await expect(indexCatalogFile(job, env, now)).rejects.toThrow("catalog index value");
  });

  test("returns immediately for a duplicate completed chunk", async () => {
    const runId = await prepareWaitingRun();
    await planCatalogIndex(planJob(runId), env, now);
    const job = queue.messages.find((message) => message.type === "index-file");
    if (job?.type !== "index-file") throw new Error("Missing test index job");
    await indexCatalogFile(job, env, now);
    mocks.readObjects.mockClear();
    await indexCatalogFile(job, env, now);
    expect(mocks.readObjects).not.toHaveBeenCalled();
  });

  test("handles ready, actively planning, and inconsistent index states", async () => {
    const runId = await prepareWaitingRun();
    mocks.manifests.mockResolvedValue([{ entries: [] }]);
    await planCatalogIndex(planJob(runId), env, now);
    await planCatalogIndex(planJob(runId), env, now);
    expect(queue.messages.at(-1)?.type).toBe("catalog-table");

    await env.DB.prepare(
      `update catalog_index_partitions set status = 'planning', total_chunks = 0,
         completed_chunks = 0, updated_at = ? where table_name = 'nvd_ra' and partition_value = '2026'`,
    )
      .bind(now.toISOString())
      .run();
    await expect(planCatalogIndex(planJob(runId), env, now)).rejects.toThrow("planning is active");

    await env.DB.prepare(
      `update catalog_index_partitions set status = 'building', total_chunks = 2,
         completed_chunks = 1 where table_name = 'nvd_ra' and partition_value = '2026'`,
    ).run();
    await expect(planCatalogIndex(planJob(runId), env, now)).rejects.toThrow(
      "chunks are incomplete",
    );
  });

  test("does not wake Catalog until every file job finishes", async () => {
    const runId = await prepareWaitingRun();
    const original = (await mocks.manifests()).at(0)?.entries.at(0);
    if (original === undefined) throw new Error("Missing test manifest entry");
    mocks.manifests.mockResolvedValue([
      {
        entries: [
          original,
          {
            ...original,
            data_file: { ...original.data_file, file_path: "s3://catalog/second.parquet" },
          },
        ],
      },
    ]);
    await planCatalogIndex(planJob(runId), env, now);
    const first = queue.messages.find((message) => message.type === "index-file");
    if (first?.type !== "index-file") throw new Error("Missing test index job");
    queue.messages.length = 0;
    await indexCatalogFile(first, env, now);
    expect(queue.messages).toEqual([]);
  });

  test.each([0n, 1n, 2n])(
    "applies only same-or-newer position deletes, preserving absolute positions: %s",
    async (sequence) => {
      const runId = await prepareWaitingRun();
      await planCatalogIndex(planJob(runId), env, now);
      const job = queue.messages.find((message) => message.type === "index-file");
      if (job?.type !== "index-file") throw new Error("Missing index job");
      mocks.deleteMaps.mockResolvedValue({
        positionDeletesMap: new Map([
          [
            "s3://catalog/data.parquet",
            [
              {
                deleteEntry: {
                  sequence_number: sequence,
                  data_file: { file_format: "PARQUET", partition: { kaisai_nen: "2026" } },
                },
                positions: new Set([0n]),
              },
            ],
          ],
        ]),
        equalityDeleteGroups: [],
      });
      mocks.readObjects.mockResolvedValue([
        { kaisai_nen: "2026", kaisai_tsukihi: "0903", keibajo_code: "30", race_bango: "01" },
        { kaisai_nen: "2026", kaisai_tsukihi: "0903", keibajo_code: "30", race_bango: "02" },
      ]);
      await indexCatalogFile(job, env, now);
      const rows = await env.DB.prepare(
        "select row_position from catalog_row_index order by row_position",
      ).all();
      if (sequence === 0n)
        expect(rows.results).toStrictEqual([{ row_position: 0 }, { row_position: 1 }]);
      else expect(rows.results).toStrictEqual([{ row_position: 1 }]);
      expect(mocks.manifests).toHaveBeenLastCalledWith(
        expect.objectContaining({ snapshotId: 10n }),
      );
    },
  );

  test("does not overwrite a replacement key when the deleted old file finishes last", async () => {
    const deleteEntry = {
      status: 1,
      sequence_number: 2n,
      partition_spec_id: 0,
      data_file: {
        content: 1,
        file_format: "PARQUET",
        file_path: "s3://catalog/deletes.parquet",
        file_size_in_bytes: 100n,
        partition: {},
        record_count: 1n,
      },
    };
    mocks.manifests.mockResolvedValue([
      {
        entries: [
          {
            status: 1,
            sequence_number: 2n,
            partition_spec_id: 0,
            data_file: {
              content: 0,
              file_path: "s3://catalog/new.parquet",
              file_size_in_bytes: 100n,
              partition: {},
              record_count: 1n,
            },
          },
          {
            status: 1,
            sequence_number: 1n,
            partition_spec_id: 0,
            data_file: {
              content: 0,
              file_path: "s3://catalog/old.parquet",
              file_size_in_bytes: 100n,
              partition: {},
              record_count: 1n,
            },
          },
          deleteEntry,
        ],
      },
    ]);
    mocks.deleteMaps.mockResolvedValue({
      positionDeletesMap: new Map([
        ["s3://catalog/old.parquet", [{ deleteEntry, positions: new Set([0n]) }]],
      ]),
      equalityDeleteGroups: [],
    });
    mocks.readObjects.mockResolvedValue([
      {
        keibajo_code: "05",
        kyori: "1600",
        track_code: "11",
        course_kaishu_nengappi: "20240106",
      },
    ]);
    await planCatalogIndex(
      {
        ...planJob("course-run"),
        provider: "jv",
        tableName: "jvd_cs",
        partitionValue: "__all__",
      },
      env,
      now,
    );
    expect(queue.messages).toHaveLength(2);
    const replacement = queue.messages[0];
    const obsolete = queue.messages[1];
    if (replacement?.type !== "index-file" || obsolete?.type !== "index-file")
      throw new Error("Missing index file jobs");
    // Metadata can advance between planning and delivery; each chunk must still read snapshot 10.
    mocks.load.mockResolvedValue({
      metadata: { "current-snapshot-id": 11n },
      resolver: { reader: vi.fn() },
    });
    await indexCatalogFile(replacement, env, now);
    await indexCatalogFile(obsolete, env, now);
    expect(mocks.manifests).toHaveBeenLastCalledWith(expect.objectContaining({ snapshotId: 10n }));
    expect(mocks.deleteMaps).toHaveBeenLastCalledWith(
      [
        expect.objectContaining({
          data_file: expect.objectContaining({ file_path: "s3://catalog/deletes.parquet" }),
        }),
      ],
      expect.anything(),
    );
    expect(
      await env.DB.prepare(
        "select file_path, row_position, catalog_snapshot_id from catalog_row_index where table_name = 'jvd_cs'",
      ).all(),
    ).toMatchObject({
      results: [
        {
          file_path: "s3://catalog/new.parquet",
          row_position: 0,
          catalog_snapshot_id: "10",
        },
      ],
    });
    expect(
      await env.DB.prepare(
        "select status, catalog_snapshot_id from catalog_index_partitions where table_name = 'jvd_cs'",
      ).first(),
    ).toStrictEqual({ status: "ready", catalog_snapshot_id: "10" });
  });

  test.each([
    { bytes: 8_388_609n, rows: 1n, files: 1 },
    { bytes: 1n, rows: 100_001n, files: 1 },
    { bytes: -1n, rows: 1n, files: 1 },
    { bytes: 1n, rows: -1n, files: 1 },
    { bytes: 1n, rows: 1n, files: 129 },
  ])(
    "bounds delete-file IO before fetching ($files files, $bytes bytes, $rows rows)",
    async ({ bytes, rows, files }) => {
      const runId = await prepareWaitingRun();
      await planCatalogIndex(planJob(runId), env, now);
      const job = queue.messages.find((message) => message.type === "index-file");
      if (job?.type !== "index-file") throw new Error("Missing index job");
      mocks.manifests.mockResolvedValue([
        {
          entries: [
            { status: 1, data_file: { content: 0, file_path: "s3://catalog/data.parquet" } },
            ...Array.from({ length: files }, () => ({
              status: 1,
              data_file: {
                content: 1,
                file_size_in_bytes: bytes,
                record_count: rows,
              },
            })),
          ],
        },
      ]);
      await expect(indexCatalogFile(job, env, now)).rejects.toMatchObject({
        safeStage: "index-delete-budget",
      });
      expect(mocks.deleteMaps).not.toHaveBeenCalled();
      expect(mocks.readObjects).not.toHaveBeenCalled();
    },
  );

  test("rejects a data file missing from its pinned snapshot", async () => {
    const runId = await prepareWaitingRun();
    await planCatalogIndex(planJob(runId), env, now);
    const job = queue.messages.find((message) => message.type === "index-file");
    if (job?.type !== "index-file") throw new Error("Missing index job");
    mocks.manifests.mockResolvedValue([]);
    await expect(indexCatalogFile(job, env, now)).rejects.toMatchObject({
      safeStage: "index-file-missing",
    });
    expect(mocks.readObjects).not.toHaveBeenCalled();
  });

  test("fails closed rather than ignoring equality deletes", async () => {
    const runId = await prepareWaitingRun();
    await planCatalogIndex(planJob(runId), env, now);
    const job = queue.messages.find((message) => message.type === "index-file");
    if (job?.type !== "index-file") throw new Error("Missing index job");
    mocks.manifests.mockResolvedValue([
      {
        entries: [
          { status: 1, data_file: { content: 0, file_path: "s3://catalog/data.parquet" } },
          { status: 1, data_file: { content: 2 } },
        ],
      },
    ]);
    await expect(indexCatalogFile(job, env, now)).rejects.toMatchObject({
      safeStage: "index-equality-delete",
    });
    expect(mocks.readObjects).not.toHaveBeenCalled();
  });

  test("rejects a chunk whose queue identity was changed", async () => {
    const runId = await prepareWaitingRun();
    await planCatalogIndex(planJob(runId), env, now);
    const job = queue.messages.find((message) => message.type === "index-file");
    if (job?.type !== "index-file") throw new Error("Missing test index job");
    await expect(
      indexCatalogFile({ ...job, partitionValue: "2025" }, env, now),
    ).rejects.toBeInstanceOf(PermanentJobError);
  });
});
