import { readFile } from "node:fs/promises";
import { Miniflare } from "miniflare";
import { afterAll, beforeAll, beforeEach, describe, expect, test, vi } from "vitest";
import { indexCatalogFile, planCatalogIndex } from "./catalog-index";
import { PermanentJobError } from "./errors";
import { createRun, markParsed } from "./state";
import type { Env, IndexPlanJob, SyncJob } from "./types";

const mocks = vi.hoisted(() => ({
  load: vi.fn(),
  manifests: vi.fn(),
  readObjects: vi.fn(),
}));

vi.mock("./catalog-sync", () => ({ loadCatalogTable: mocks.load }));
vi.mock("icebird", () => ({ icebergManifests: mocks.manifests }));
vi.mock("hyparquet", () => ({ parquetReadObjects: mocks.readObjects }));

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
  ]) {
    const migration = await readFile(new URL(`../migrations/${name}`, import.meta.url), "utf8");
    await env.DB.exec(
      migration
        .split("\n")
        .filter((line) => !line.trimStart().startsWith("--"))
        .join(" "),
    );
  }
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
