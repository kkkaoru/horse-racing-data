// Run with bun via the package test scripts.
import { readFile } from "node:fs/promises";
import { Miniflare } from "miniflare";
import { afterAll, beforeAll, beforeEach, describe, expect, test, vi } from "vitest";
import { PermanentJobError, TransientJobError } from "./errors";
import { acquireRun, handleJob, recordJobFailure } from "./jobs";
import { makeRecord } from "./layouts.test";
import {
  completeRun,
  createRun,
  createRunWithDataSpec,
  getProviderAcquisitionCursor,
  getRun,
  listRunTables,
  markAcquired,
  markCatalogTable,
  markNeonTable,
  markParsed,
} from "./state";
import { createTableStage } from "./source-stream";
import type { AcquisitionDataSpec, Env, SyncJob } from "./types";

const mocks = vi.hoisted(() => ({
  catalog: vi.fn(),
  neon: vi.fn(),
  purgeCursorCache: vi.fn(),
}));

vi.mock("./cache", () => ({ purgeProviderCursorCache: mocks.purgeCursorCache }));
vi.mock("./catalog-sync", () => ({ syncCatalogTable: mocks.catalog }));
vi.mock("./neon-sync", () => ({ syncNeonTable: mocks.neon }));

class RecordingQueue implements Queue<SyncJob> {
  readonly messages: SyncJob[] = [];

  async metrics(): Promise<QueueMetrics> {
    return { backlogBytes: 0, backlogCount: this.messages.length };
  }

  async send(message: SyncJob): Promise<QueueSendResponse> {
    this.messages.push(message);
    return { metadata: { metrics: { backlogBytes: 0, backlogCount: this.messages.length } } };
  }

  async sendBatch(
    messages: Iterable<MessageSendRequest<SyncJob>>,
  ): Promise<QueueSendBatchResponse> {
    for (const message of messages) this.messages.push(message.body);
    return { metadata: { metrics: { backlogBytes: 0, backlogCount: this.messages.length } } };
  }
}

const toBase64 = (bytes: Uint8Array): string => {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
};

const sourceBody = (records: readonly Uint8Array[]): string =>
  [
    JSON.stringify({ event: "open" }),
    JSON.stringify({ event: "file" }),
    ...records.map((record) =>
      JSON.stringify({
        bytes: record.length,
        data: toBase64(record),
        encoding: "base64",
        event: "record",
      }),
    ),
    JSON.stringify({ event: "close", files: 1, records: records.length }),
    "",
  ].join("\n");

const tableRow = (runId: string, tableName = "nvd_ra", stagingKey = "table.json") => ({
  catalog_status: "pending",
  neon_status: "pending",
  partitions: ["2026"],
  source_records: 1,
  staging_key: stagingKey,
  table_name: tableName,
  runId,
});

let miniflare: Miniflare;
let baseEnv: Env;
let providerBody: string;
let queue: RecordingQueue;
let realtimeRequests: Request[];
const now = new Date("2026-09-03T16:00:00.000Z");

beforeAll(async () => {
  providerBody = sourceBody([makeRecord("nvd_ra", { kaisai_nen: "2026" })]);
  miniflare = new Miniflare({
    bindings: {
      ADMIN_TOKEN: "admin",
      JRA_VAN_WORKER_API_TOKEN: "jv-token",
      NEON_DATABASE_URL: "postgresql://example",
      R2_BUCKET_NAME: "pc-keiba-r2-catalog",
      REALTIME_ADMIN_TOKEN: "realtime-token",
      R2_CATALOG_NAMESPACE: "pc_keiba",
      R2_CATALOG_TOKEN: "catalog-token",
      R2_CATALOG_URI: "https://catalog.example",
      R2_CATALOG_WAREHOUSE: "warehouse",
      UMMACON_WORKER_API_TOKEN: "nv-token",
    },
    compatibilityDate: "2026-06-18",
    d1Databases: { DB: "jobs-test" },
    modules: true,
    r2Buckets: { CATALOG_BUCKET: "catalog-test", SOURCE_STAGING: "staging-test" },
    script: "export default {}",
    serviceBindings: {
      JV_SOURCE: async () =>
        new Response(providerBody, { headers: { "Content-Type": "application/x-ndjson" } }),
      NV_SOURCE: async () =>
        new Response(providerBody, { headers: { "Content-Type": "application/x-ndjson" } }),
      REALTIME_SYNC: async () => Response.json({ ok: true }),
    },
  });
  baseEnv = await miniflare.getBindings<Env>();
  for (const name of [
    "0001_initial.sql",
    "0002_catalog_index.sql",
    "0003_acquisition_window.sql",
    "0005_acquisition_data_spec.sql",
  ]) {
    const migration = await readFile(new URL(`../migrations/${name}`, import.meta.url), "utf8");
    await baseEnv.DB.exec(
      migration
        .split("\n")
        .filter((line) => !line.trimStart().startsWith("--"))
        .join(" "),
    );
  }
});

beforeEach(async () => {
  await baseEnv.DB.exec(
    "delete from catalog_table_leases; delete from catalog_operations; delete from catalog_row_index; delete from catalog_index_chunks; delete from catalog_index_partitions; delete from sync_run_table_partitions; delete from sync_run_tables; delete from sync_runs; delete from provider_acquisition_cursors;",
  );
  await baseEnv.DB.exec("delete from catalog_targets where table_name = 'jvd_cs'");
  queue = new RecordingQueue();
  realtimeRequests = [];
  const source = {
    fetch: async () =>
      new Response(providerBody, { headers: { "Content-Type": "application/x-ndjson" } }),
  };
  baseEnv = {
    ...baseEnv,
    JV_RAW_STAGE_JOBS: queue,
    JV_SOURCE: source,
    NEON_JOBS: queue,
    NV_RAW_STAGE_JOBS: queue,
    NV_SOURCE: source,
    R2_CATALOG_JOBS: queue,
    REALTIME_SYNC: {
      fetch: async (request) => {
        realtimeRequests.push(request instanceof Request ? request : new Request(request));
        return Response.json({ ok: true });
      },
    },
  };
  mocks.catalog.mockReset().mockResolvedValue({ deletedRows: 1, records: 1, snapshotId: "99" });
  mocks.neon.mockReset().mockResolvedValue(1);
  mocks.purgeCursorCache.mockReset().mockResolvedValue(undefined);
});

afterAll(async () => {
  await miniflare.dispose();
});

const createTestRun = async () =>
  (await createRun(baseEnv.DB, "nv", "20260904", "daily", 2, now, false)).run;

const prepareTable = async (
  runId: string,
  tableName = "nvd_ra",
  stagingKey = "table.json",
  indexReady = true,
) => {
  await markParsed(baseEnv.DB, runId, 1, 1, [tableRow(runId, tableName, stagingKey)], now);
  if (indexReady)
    await baseEnv.DB.prepare(
      `insert into catalog_index_partitions
         (table_name, partition_value, status, catalog_snapshot_id, updated_at)
         values (?, '2026', 'ready', '1', ?)`,
    )
      .bind(tableName, now.toISOString())
      .run();
  await baseEnv.SOURCE_STAGING.put(
    stagingKey,
    JSON.stringify(createTableStage("nv", runId, tableName, [{ record_shubetsu_id: "RA" }])),
  );
};

describe("sync queue jobs", () => {
  test.each([false, true])(
    "stages course rows using the virtual partition (ready=%s)",
    async (ready) => {
      const { run } = await createRun(baseEnv.DB, "jv", "20260904", "manual", 2, now, false);
      await baseEnv.DB.prepare(
        "insert into catalog_targets (table_name, provider, partition_field, created_at) values ('jvd_cs', 'jv', '__unpartitioned__', ?)",
      )
        .bind(now.toISOString())
        .run();
      if (ready)
        await baseEnv.DB.prepare(
          "insert into catalog_index_partitions (table_name, partition_value, status, catalog_snapshot_id, updated_at) values ('jvd_cs', '__all__', 'ready', '10', ?)",
        )
          .bind(now.toISOString())
          .run();
      await baseEnv.SOURCE_STAGING.put(
        "course-source.ndjson",
        sourceBody([
          makeRecord("jvd_cs", {
            data_kubun: "0",
            keibajo_code: "05",
            kyori: "1600",
            track_code: "11",
            course_kaishu_nengappi: "20240106",
          }),
        ]),
      );
      await handleJob(
        {
          type: "r2-bucket-jvlink",
          provider: "jv",
          runId: run.run_id,
          runDate: "20260904",
          advanceCursor: false,
          stagingKey: "course-source.ndjson",
        },
        baseEnv,
        now,
      );
      expect(queue.messages).toHaveLength(1);
      if (ready)
        expect(queue.messages[0]).toMatchObject({ type: "catalog-table", tableName: "jvd_cs" });
      else
        expect(queue.messages[0]).toMatchObject({
          type: "index-plan",
          tableName: "jvd_cs",
          partitionValue: "__all__",
        });
      expect(
        await baseEnv.DB.prepare(
          "select partition_value from sync_run_table_partitions where run_id = ?",
        )
          .bind(run.run_id)
          .all(),
      ).toMatchObject({ results: [{ partition_value: "__all__" }] });
      const tables = await listRunTables(baseEnv.DB, run.run_id);
      const stage = await baseEnv.SOURCE_STAGING.get(tables[0]?.staging_key ?? "missing");
      if (stage === null) throw new Error("Expected durable course stage");
      expect(await stage.json()).toMatchObject({
        tableName: "jvd_cs",
        records: [{ data_kubun: "0" }],
      });
      expect(mocks.catalog).not.toHaveBeenCalled();
      expect(mocks.neon).not.toHaveBeenCalled();
    },
  );

  test("rejects a course target configured with a race-year partition", async () => {
    const { run } = await createRun(baseEnv.DB, "jv", "20260904", "manual", 2, now, false);
    await baseEnv.DB.prepare(
      "insert into catalog_targets (table_name, provider, partition_field, created_at) values ('jvd_cs', 'jv', 'kaisai_nen', ?)",
    )
      .bind(now.toISOString())
      .run();
    await baseEnv.SOURCE_STAGING.put("course-source.ndjson", sourceBody([makeRecord("jvd_cs")]));
    await expect(
      handleJob(
        {
          type: "r2-bucket-jvlink",
          provider: "jv",
          runId: run.run_id,
          runDate: "20260904",
          advanceCursor: false,
          stagingKey: "course-source.ndjson",
        },
        baseEnv,
        now,
      ),
    ).rejects.toThrow("Course target requires an unpartitioned configuration");
    expect(queue.messages).toHaveLength(0);
    expect(mocks.catalog).not.toHaveBeenCalled();
    expect(mocks.neon).not.toHaveBeenCalled();
  });

  test.each<AcquisitionDataSpec>(["COMM", "RACECOMM"])(
    "retries a %s acquisition with the persisted spec and no race cursor advance",
    async (dataSpec) => {
      const { run } = await createRunWithDataSpec({
        db: baseEnv.DB,
        provider: "jv",
        dataSpec,
        runDate: "20260904",
        trigger: "manual",
        lookbackDays: 2,
        now,
        force: true,
        fromTime: "20260902000000",
        toTime: "20260904200000",
        cursorTime: "20260904200000",
        advanceCursor: false,
      });
      const fetch = vi
        .fn(
          async (_input: RequestInfo | URL) =>
            new Response('{"event":"open"}\n{"event":"close","files":0,"records":0}\n', {
              headers: { "Content-Type": "application/x-ndjson" },
            }),
        )
        .mockRejectedValueOnce(new Error("temporary source failure"));
      const env: Env = { ...baseEnv, JV_SOURCE: { fetch } };
      await expect(
        acquireRun(
          {
            advanceCursor: false,
            cursorTime: "20260904200000",
            fromTime: "20260902000000",
            provider: "jv",
            runDate: "20260904",
            runId: run.run_id,
            toTime: "20260904200000",
          },
          env,
          now,
        ),
      ).rejects.toThrow("temporary source failure");
      await acquireRun(
        {
          advanceCursor: false,
          cursorTime: "20260904200000",
          fromTime: "20260902000000",
          provider: "jv",
          runDate: "20260904",
          runId: run.run_id,
          toTime: "20260904200000",
        },
        env,
        now,
      );
      expect(
        await Promise.all(fetch.mock.calls.map(async ([input]) => await new Request(input).json())),
      ).toMatchObject([
        { dataSpec, from: "20260902000000", to: "20260904200000" },
        { dataSpec, from: "20260902000000", to: "20260904200000" },
      ]);
      expect(fetch).toHaveBeenCalledTimes(2);
      const staged = queue.messages[0];
      if (staged?.type !== "r2-bucket-jvlink") throw new Error("Missing JV stage job");
      await handleJob(staged, env, now);
      expect(await getProviderAcquisitionCursor(baseEnv.DB, "jv")).toBeNull();
      expect(await getRun(baseEnv.DB, run.run_id)).toMatchObject({
        status: "succeeded_empty",
        data_spec: dataSpec,
        advance_cursor: 0,
      });
    },
  );

  test("rejects a mismatched provider or COMM cursor request before source I/O", async () => {
    const { run } = await createRunWithDataSpec({
      db: baseEnv.DB,
      provider: "jv",
      dataSpec: "COMM",
      runDate: "20260904",
      trigger: "manual",
      lookbackDays: 2,
      now,
      force: true,
      fromTime: "20260902000000",
      toTime: "20260904200000",
      cursorTime: "20260904200000",
      advanceCursor: false,
    });
    const fetch = vi.fn(async () => new Response("unexpected"));
    const env: Env = { ...baseEnv, JV_SOURCE: { fetch }, NV_SOURCE: { fetch } };
    await expect(
      acquireRun(
        {
          advanceCursor: false,
          cursorTime: "20260904200000",
          fromTime: "20260902000000",
          provider: "nv",
          runDate: "20260904",
          runId: run.run_id,
          toTime: null,
        },
        env,
        now,
      ),
    ).rejects.toThrow("Source acquisition provider mismatch");
    await expect(
      acquireRun(
        {
          advanceCursor: true,
          cursorTime: "20260904200000",
          fromTime: "20260902000000",
          provider: "jv",
          runDate: "20260904",
          runId: run.run_id,
          toTime: "20260904200000",
        },
        env,
        now,
      ),
    ).rejects.toThrow("COMM-only acquisition cannot advance the race cursor");
    expect(fetch).not.toHaveBeenCalled();
    expect(queue.messages).toStrictEqual([]);
  });

  test("rejects COMM cursor advancement from a replayed staging message before touching storage", async () => {
    const { run } = await createRunWithDataSpec({
      db: baseEnv.DB,
      provider: "jv",
      dataSpec: "COMM",
      runDate: "20260904",
      trigger: "manual",
      lookbackDays: 2,
      now,
      force: true,
      fromTime: "20260902000000",
      toTime: "20260904200000",
      cursorTime: "20260904200000",
      advanceCursor: false,
    });
    const get = vi.spyOn(baseEnv.SOURCE_STAGING, "get");
    await expect(
      handleJob(
        {
          type: "r2-bucket-jvlink",
          provider: "jv",
          runDate: "20260904",
          runId: run.run_id,
          stagingKey: "must-not-be-read",
          advanceCursor: true,
          cursorTime: "20260904200000",
        },
        baseEnv,
        now,
      ),
    ).rejects.toThrow("COMM-only acquisition cannot advance the race cursor");
    expect(get).not.toHaveBeenCalled();
    get.mockRestore();
    expect(await getProviderAcquisitionCursor(baseEnv.DB, "jv")).toBeNull();
    expect((await getRun(baseEnv.DB, run.run_id)).status).toBe("queued");
  });

  test("acquires the provider stream directly into R2 staging", async () => {
    const run = await createTestRun();
    await acquireRun(
      {
        advanceCursor: true,
        cursorTime: "20260904010000",
        fromTime: "20260902000000",
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        toTime: null,
      },
      baseEnv,
      now,
    );

    const updated = await getRun(baseEnv.DB, run.run_id);
    expect(updated.status).toBe("acquired");
    expect(updated.staging_key).toContain("source.ndjson");
    expect(await getProviderAcquisitionCursor(baseEnv.DB, "nv")).toBeNull();
    expect(queue.messages).toEqual([
      {
        advanceCursor: true,
        cursorTime: "20260904010000",
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        stagingKey: updated.staging_key,
        type: "r2-bucket-nvlink",
      },
    ]);
  });

  test("does not advance the daily cursor for manual acquisition", async () => {
    const run = await createTestRun();
    await acquireRun(
      {
        advanceCursor: false,
        cursorTime: "20260905010000",
        fromTime: "20260901000000",
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        toTime: null,
      },
      baseEnv,
      now,
    );
    expect(await getProviderAcquisitionCursor(baseEnv.DB, "nv")).toBeNull();
  });

  test("treats a valid zero-record response as success and advances the cursor", async () => {
    const run = await createTestRun();
    const emptyEvents = `${JSON.stringify({ event: "open" })}\n${JSON.stringify({ event: "close", files: 0, records: 0 })}\n`;
    const emptyEnv: Env = {
      ...baseEnv,
      NV_SOURCE: {
        fetch: async () =>
          new Response(emptyEvents, {
            headers: { "Content-Type": "application/x-ndjson" },
          }),
      },
    };
    await acquireRun(
      {
        advanceCursor: true,
        cursorTime: "20260904010000",
        fromTime: "20260903010000",
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        toTime: null,
      },
      emptyEnv,
      now,
    );
    const bucketJob = queue.messages[0];
    if (bucketJob?.type !== "r2-bucket-nvlink") throw new Error("Missing R2 Bucket job");
    await handleJob(bucketJob, emptyEnv, now);
    expect((await getRun(baseEnv.DB, run.run_id)).status).toBe("succeeded_empty");
    expect(await getProviderAcquisitionCursor(baseEnv.DB, "nv")).toBe("20260904010000");
    expect(mocks.purgeCursorCache).toHaveBeenCalledWith(emptyEnv.SYNC_CACHE, "nv");
  });

  test("selects the JV source and rejects unsafe upstream responses", async () => {
    const { run: jvRun } = await createRun(baseEnv.DB, "jv", "20260904", "daily", 2, now, false);
    await acquireRun(
      {
        advanceCursor: true,
        cursorTime: "20260904200000",
        fromTime: "20260902000000",
        provider: "jv",
        runDate: "20260904",
        runId: jvRun.run_id,
        toTime: "20260904235959",
      },
      baseEnv,
      now,
    );
    expect(queue.messages[0]).toMatchObject({
      provider: "jv",
      type: "r2-bucket-jvlink",
    });

    const failed = await createTestRun();
    const badStatus: Env = {
      ...baseEnv,
      NV_SOURCE: { fetch: async () => new Response("no", { status: 502 }) },
    };
    await expect(
      acquireRun(
        {
          advanceCursor: true,
          cursorTime: "20260904010000",
          fromTime: "20260902000000",
          provider: "nv",
          runDate: "20260904",
          runId: failed.run_id,
          toTime: null,
        },
        badStatus,
        now,
      ),
    ).rejects.toThrow("Source acquisition failed");
    const emptyBody: Env = {
      ...baseEnv,
      NV_SOURCE: { fetch: async () => new Response(null) },
    };
    await expect(
      acquireRun(
        {
          advanceCursor: true,
          cursorTime: "20260904010000",
          fromTime: "20260902000000",
          provider: "nv",
          runDate: "20260904",
          runId: failed.run_id,
          toTime: null,
        },
        emptyBody,
        now,
      ),
    ).rejects.toThrow("Source acquisition failed");
    const badType: Env = {
      ...baseEnv,
      NV_SOURCE: {
        fetch: async () => new Response("no", { headers: { "Content-Type": "text/plain" } }),
      },
    };
    await expect(
      acquireRun(
        {
          advanceCursor: true,
          cursorTime: "20260904010000",
          fromTime: "20260902000000",
          provider: "nv",
          runDate: "20260904",
          runId: failed.run_id,
          toTime: null,
        },
        badType,
        now,
      ),
    ).rejects.toThrow("Invalid source content type");
    expect(await getProviderAcquisitionCursor(baseEnv.DB, "nv")).toBeNull();
    expect(mocks.purgeCursorCache).not.toHaveBeenCalledWith(baseEnv.SYNC_CACHE, "nv");
  });

  test("rejects a missing raw staging object", async () => {
    const run = await createTestRun();
    await expect(
      handleJob(
        {
          provider: "nv",
          runDate: "20260904",
          runId: run.run_id,
          stagingKey: "missing-raw",
          type: "r2-bucket-nvlink",
        },
        baseEnv,
        now,
      ),
    ).rejects.toThrow("Staging object not found");

    const { run: jvRun } = await createRun(baseEnv.DB, "jv", "20260904", "daily", 2, now, false);
    await expect(
      handleJob(
        {
          provider: "jv",
          runDate: "20260904",
          runId: jvRun.run_id,
          stagingKey: "missing-jv-raw",
          type: "r2-bucket-jvlink",
        },
        baseEnv,
        now,
      ),
    ).rejects.toThrow("Staging object not found");
  });

  test("parses raw staging, writes table stages, and fans out R2 jobs", async () => {
    const run = await createTestRun();
    const key = "raw.ndjson";
    await baseEnv.SOURCE_STAGING.put(
      key,
      sourceBody([makeRecord("nvd_ra", { kaisai_nen: "2026" })]),
    );
    await markAcquired(baseEnv.DB, run.run_id, key, now);
    await handleJob(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        stagingKey: key,
        type: "r2-bucket-nvlink",
      },
      baseEnv,
      now,
    );

    expect((await getRun(baseEnv.DB, run.run_id)).status).toBe("catalog_pending");
    expect(await listRunTables(baseEnv.DB, run.run_id)).toHaveLength(1);
    expect(queue.messages[0]).toMatchObject({
      partitionValue: "2026",
      tableName: "nvd_ra",
      type: "index-plan",
    });
  });

  test("stages unsupported official records as not_configured without retrying them", async () => {
    const run = await createTestRun();
    const key = "mixed.ndjson";
    await baseEnv.SOURCE_STAGING.put(
      key,
      sourceBody([
        makeRecord("nvd_ra", { kaisai_nen: "2026" }),
        makeRecord("nvd_h1", { kaisai_nen: "2026" }),
      ]),
    );
    await handleJob(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        stagingKey: key,
        type: "r2-bucket-nvlink",
      },
      baseEnv,
      now,
    );
    const tables = await listRunTables(baseEnv.DB, run.run_id);
    expect(tables.find((table) => table.table_name === "nvd_h1")).toMatchObject({
      catalog_status: "not_configured",
      neon_status: "not_configured",
    });
    expect(
      await baseEnv.SOURCE_STAGING.head(
        `source-staging/v1/nv/20260904/${run.run_id}/tables/nvd_h1.json`,
      ),
    ).not.toBeNull();
    expect(queue.messages).toHaveLength(1);
    expect(queue.messages[0]?.type).toBe("index-plan");
  });

  test("completes when every staged official record is not configured", async () => {
    const run = await createTestRun();
    const key = "unsupported-only.ndjson";
    await baseEnv.SOURCE_STAGING.put(key, sourceBody([makeRecord("nvd_h1")]));
    await handleJob(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        stagingKey: key,
        type: "r2-bucket-nvlink",
      },
      baseEnv,
      now,
    );
    expect((await getRun(baseEnv.DB, run.run_id)).status).toBe("succeeded");
    expect(queue.messages).toEqual([]);
  });

  test("dispatches Catalog directly when every required partition index is ready", async () => {
    const run = await createTestRun();
    await baseEnv.DB.prepare(
      `insert into catalog_index_partitions
         (table_name, partition_value, status, catalog_snapshot_id, updated_at)
         values ('nvd_ra', '2026', 'ready', '1', ?)`,
    )
      .bind(now.toISOString())
      .run();
    const key = "ready.ndjson";
    await baseEnv.SOURCE_STAGING.put(
      key,
      sourceBody([makeRecord("nvd_ra", { kaisai_nen: "2026" })]),
    );
    await handleJob(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        stagingKey: key,
        type: "r2-bucket-nvlink",
      },
      baseEnv,
      now,
    );
    expect(queue.messages[0]).toMatchObject({ tableName: "nvd_ra", type: "catalog-table" });
  });

  test("rejects a configured record with a blank partition", async () => {
    const run = await createTestRun();
    const key = "blank-partition.ndjson";
    await baseEnv.SOURCE_STAGING.put(key, sourceBody([makeRecord("nvd_ra")]));
    await expect(
      handleJob(
        {
          provider: "nv",
          runDate: "20260904",
          runId: run.run_id,
          stagingKey: key,
          type: "r2-bucket-nvlink",
        },
        baseEnv,
        now,
      ),
    ).rejects.toThrow("partition is missing");
  });

  test("completes an empty source run without catalog or Neon jobs", async () => {
    const run = await createTestRun();
    const key = "empty.ndjson";
    await baseEnv.SOURCE_STAGING.put(
      key,
      `${JSON.stringify({ event: "open" })}\n${JSON.stringify({ event: "close", files: 0, records: 0 })}\n`,
    );
    await handleJob(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        stagingKey: key,
        type: "r2-bucket-nvlink",
      },
      baseEnv,
      now,
    );
    expect((await getRun(baseEnv.DB, run.run_id)).status).toBe("succeeded_empty");
    expect(queue.messages).toEqual([]);
  });

  test("publishes a catalog table before dispatching Neon", async () => {
    const run = await createTestRun();
    await prepareTable(run.run_id);
    await handleJob(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        tableStagingKey: "table.json",
        type: "catalog-table",
      },
      baseEnv,
      now,
    );
    expect(mocks.catalog).toHaveBeenCalledTimes(1);
    expect((await getRun(baseEnv.DB, run.run_id)).status).toBe("catalog_succeeded");
    expect(queue.messages).toEqual([
      { provider: "nv", runDate: "20260904", runId: run.run_id, type: "neon-dispatch" },
    ]);
  });

  test("serializes Catalog commits and skips duplicate completed messages", async () => {
    const run = await createTestRun();
    await prepareTable(run.run_id);
    await baseEnv.DB.prepare(
      `insert into catalog_table_leases (table_name, owner_id, expires_at, updated_at)
         values ('nvd_ra', 'other', '2099-01-01T00:00:00.000Z', ?)`,
    )
      .bind(now.toISOString())
      .run();
    const job: SyncJob = {
      provider: "nv",
      runDate: "20260904",
      runId: run.run_id,
      tableName: "nvd_ra",
      tableStagingKey: "table.json",
      type: "catalog-table",
    };
    await expect(handleJob(job, baseEnv, now)).rejects.toThrow("lease is held");
    await baseEnv.DB.prepare("delete from catalog_table_leases where table_name = 'nvd_ra'").run();
    await markCatalogTable(baseEnv.DB, run.run_id, "nvd_ra", "99", 0, now);
    await handleJob(job, baseEnv, now);
    expect(mocks.catalog).not.toHaveBeenCalled();
  });

  test("returns a Catalog table to index_pending when its index is not ready", async () => {
    const run = await createTestRun();
    await prepareTable(run.run_id, "nvd_ra", "table.json", false);
    await handleJob(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        tableStagingKey: "table.json",
        type: "catalog-table",
      },
      baseEnv,
      now,
    );
    expect((await listRunTables(baseEnv.DB, run.run_id))[0]?.catalog_status).toBe("index_pending");
    expect(mocks.catalog).not.toHaveBeenCalled();
  });

  test("rebuilds stale ready indexes without dispatching Neon or retaining the lease", async () => {
    const run = await createTestRun();
    await prepareTable(run.run_id);
    mocks.catalog.mockRejectedValueOnce(new TransientJobError("catalog-index-stale"));
    await handleJob(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        tableStagingKey: "table.json",
        type: "catalog-table",
      },
      baseEnv,
      now,
    );
    expect((await listRunTables(baseEnv.DB, run.run_id))[0]?.catalog_status).toBe("index_pending");
    expect(queue.messages).toEqual([
      {
        partitionValue: "2026",
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        type: "index-plan",
      },
    ]);
    expect(
      await baseEnv.DB.prepare(
        "select status from catalog_index_partitions where table_name = 'nvd_ra'",
      ).first("status"),
    ).toBe("pending");
    expect(
      await baseEnv.DB.prepare("select owner_id from catalog_table_leases").first(),
    ).toBeNull();
  });

  test("waits for every catalog table before dispatching Neon", async () => {
    const run = await createTestRun();
    await prepareTable(run.run_id);
    await markParsed(
      baseEnv.DB,
      run.run_id,
      2,
      2,
      [tableRow(run.run_id), tableRow(run.run_id, "nvd_se", "se.json")],
      now,
    );
    await baseEnv.SOURCE_STAGING.put(
      "table.json",
      JSON.stringify(createTableStage("nv", run.run_id, "nvd_ra", [{}])),
    );
    await handleJob(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        tableStagingKey: "table.json",
        type: "catalog-table",
      },
      baseEnv,
      now,
    );
    expect(queue.messages).toEqual([]);
  });

  test("dispatches and completes Neon only after the catalog barrier", async () => {
    const run = await createTestRun();
    await prepareTable(run.run_id);
    await markCatalogTable(baseEnv.DB, run.run_id, "nvd_ra", "99", 0, now);
    await handleJob(
      { provider: "nv", runDate: "20260904", runId: run.run_id, type: "neon-dispatch" },
      baseEnv,
      now,
    );
    expect(queue.messages[0]).toMatchObject({ tableName: "nvd_ra", type: "neon-table" });

    queue.messages.length = 0;
    await handleJob(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        tableStagingKey: "table.json",
        type: "neon-table",
      },
      baseEnv,
      now,
    );
    expect(mocks.neon).toHaveBeenCalledTimes(1);
    expect((await getRun(baseEnv.DB, run.run_id)).status).toBe("succeeded");
  });

  test("does not rewrite an already backed-up table on duplicate queue delivery", async () => {
    const run = await createTestRun();
    await prepareTable(run.run_id);
    await markCatalogTable(baseEnv.DB, run.run_id, "nvd_ra", "99", 0, now);
    await markNeonTable(baseEnv.DB, run.run_id, "nvd_ra", now);

    await handleJob(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        tableStagingKey: "missing-stage.json",
        type: "neon-table",
      },
      baseEnv,
      now,
    );

    expect(mocks.neon).not.toHaveBeenCalled();
    expect((await getRun(baseEnv.DB, run.run_id)).status).toBe("succeeded");
  });

  test("rejects backup jobs for a table absent from the committed run", async () => {
    const run = await createTestRun();
    await prepareTable(run.run_id);
    await markCatalogTable(baseEnv.DB, run.run_id, "nvd_ra", "99", 0, now);

    await expect(
      handleJob(
        {
          provider: "nv",
          runDate: "20260904",
          runId: run.run_id,
          tableName: "nvd_se",
          tableStagingKey: "missing-stage.json",
          type: "neon-table",
        },
        baseEnv,
        now,
      ),
    ).rejects.toThrow("Neon backup requires a committed Catalog table");
    expect(mocks.neon).not.toHaveBeenCalled();
  });

  test("rejects backup jobs for unconfigured Catalog tables", async () => {
    const run = await createTestRun();
    await markParsed(
      baseEnv.DB,
      run.run_id,
      1,
      1,
      [
        {
          ...tableRow(run.run_id),
          catalog_status: "not_configured",
          neon_status: "not_configured",
        },
      ],
      now,
    );

    await expect(
      handleJob(
        {
          provider: "nv",
          runDate: "20260904",
          runId: run.run_id,
          tableName: "nvd_ra",
          tableStagingKey: "missing-stage.json",
          type: "neon-table",
        },
        baseEnv,
        now,
      ),
    ).rejects.toThrow("Neon backup requires a committed Catalog table");
    expect(mocks.neon).not.toHaveBeenCalled();
  });

  test("publishes JV race days from Catalog while Neon is unavailable and does not repeat publication", async () => {
    const run = (
      await createRun(
        baseEnv.DB,
        "jv",
        "20260904",
        "daily",
        2,
        now,
        true,
        "20260904100000",
        "20260904200000",
        "20260904200000",
        true,
      )
    ).run;
    await prepareTable(run.run_id, "jvd_ra");
    await baseEnv.SOURCE_STAGING.put(
      "table.json",
      JSON.stringify(
        createTableStage("jv", run.run_id, "jvd_ra", [
          { kaisai_nen: "2026", kaisai_tsukihi: "0905" },
        ]),
      ),
    );
    mocks.neon.mockRejectedValue(new Error("Neon unavailable"));
    const job: SyncJob = {
      provider: "jv",
      runDate: "20260904",
      runId: run.run_id,
      tableName: "jvd_ra",
      tableStagingKey: "table.json",
      type: "catalog-table",
    };

    await handleJob(job, baseEnv, now);
    expect(realtimeRequests).toHaveLength(1);
    expect(await realtimeRequests[0]?.json()).toStrictEqual({
      date: "20260905",
      type: "discover-urls",
    });
    expect(mocks.neon).not.toHaveBeenCalled();
    expect((await listRunTables(baseEnv.DB, run.run_id))[0]?.neon_status).toBe("pending");

    await handleJob(job, baseEnv, now);
    expect(realtimeRequests).toHaveLength(1);
    expect(mocks.catalog).toHaveBeenCalledTimes(1);
    await completeRun(baseEnv.DB, run.run_id, "succeeded", 1, now);
    queue.messages.length = 0;
    await handleJob(job, baseEnv, now);
    expect(queue.messages).toStrictEqual([]);
    expect((await getRun(baseEnv.DB, run.run_id)).status).toBe("succeeded");
  });

  test("retries failed Catalog publication without recommitting or writing Neon", async () => {
    const run = (
      await createRun(
        baseEnv.DB,
        "jv",
        "20260904",
        "daily",
        2,
        now,
        true,
        "20260904100000",
        "20260904200000",
        "20260904200000",
        true,
      )
    ).run;
    await prepareTable(run.run_id, "jvd_ra");
    await baseEnv.SOURCE_STAGING.put(
      "table.json",
      JSON.stringify(
        createTableStage("jv", run.run_id, "jvd_ra", [
          { kaisai_nen: "2026", kaisai_tsukihi: "0905" },
        ]),
      ),
    );
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response(null, { status: 503 }))
      .mockResolvedValue(Response.json({ ok: true }));
    baseEnv.REALTIME_SYNC = { fetch };
    const job: SyncJob = {
      provider: "jv",
      runDate: "20260904",
      runId: run.run_id,
      tableName: "jvd_ra",
      tableStagingKey: "table.json",
      type: "catalog-table",
    };

    await expect(handleJob(job, baseEnv, now)).rejects.toThrow(
      "Realtime scheduling notification failed",
    );
    expect(queue.messages).toStrictEqual([]);
    await handleJob(job, baseEnv, now);
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(mocks.catalog).toHaveBeenCalledTimes(1);
    expect(mocks.neon).not.toHaveBeenCalled();
    expect(queue.messages[0]?.type).toBe("neon-dispatch");
  });

  test("notifies realtime discovery for legacy JV messages after Neon succeeds", async () => {
    const run = (
      await createRun(
        baseEnv.DB,
        "jv",
        "20260904",
        "daily",
        2,
        now,
        true,
        "20260904100000",
        "20260904200000",
        "20260904200000",
        true,
      )
    ).run;
    await markParsed(baseEnv.DB, run.run_id, 1, 2, [tableRow(run.run_id, "jvd_ra")], now);
    await baseEnv.SOURCE_STAGING.put(
      "table.json",
      JSON.stringify(
        createTableStage("jv", run.run_id, "jvd_ra", [
          { kaisai_nen: "2026", kaisai_tsukihi: "0905" },
          { kaisai_nen: "2026", kaisai_tsukihi: "0906" },
        ]),
      ),
    );
    await markCatalogTable(baseEnv.DB, run.run_id, "jvd_ra", "99", 0, now);

    await handleJob(
      {
        provider: "jv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "jvd_ra",
        tableStagingKey: "table.json",
        type: "neon-table",
      },
      baseEnv,
      now,
    );

    expect(
      await Promise.all(realtimeRequests.map(async (request) => await request.json())),
    ).toEqual([
      { date: "20260905", type: "discover-urls" },
      { date: "20260906", type: "discover-urls" },
    ]);
    expect(realtimeRequests[0]?.headers.get("Authorization")).toBe("Bearer realtime-token");
    expect((await getRun(baseEnv.DB, run.run_id)).status).toBe("succeeded");
  });

  test("rejects invalid JV race dates and failed realtime scheduling", async () => {
    const run = (
      await createRun(
        baseEnv.DB,
        "jv",
        "20260904",
        "daily",
        1,
        now,
        true,
        "20260904100000",
        "20260904200000",
        "20260904200000",
        true,
      )
    ).run;
    await markParsed(baseEnv.DB, run.run_id, 1, 1, [tableRow(run.run_id, "jvd_ra")], now);
    await markCatalogTable(baseEnv.DB, run.run_id, "jvd_ra", "99", 0, now);
    await baseEnv.SOURCE_STAGING.put(
      "table.json",
      JSON.stringify(
        createTableStage("jv", run.run_id, "jvd_se", [
          { kaisai_nen: "2026", kaisai_tsukihi: "0905" },
        ]),
      ),
    );
    const job = {
      provider: "jv" as const,
      runDate: "20260904",
      runId: run.run_id,
      tableName: "jvd_ra",
      tableStagingKey: "table.json",
      type: "neon-table" as const,
    };

    await markNeonTable(baseEnv.DB, run.run_id, "jvd_ra", now);
    await expect(
      handleJob(
        {
          provider: "jv",
          runDate: "20260904",
          runId: run.run_id,
          type: "neon-dispatch",
        },
        baseEnv,
        now,
      ),
    ).rejects.toThrow("Realtime scheduling stage identity mismatch");

    await baseEnv.SOURCE_STAGING.put(
      "table.json",
      JSON.stringify(createTableStage("jv", run.run_id, "jvd_ra", [{ kaisai_tsukihi: "0905" }])),
    );
    await expect(handleJob(job, baseEnv, now)).rejects.toThrow("JV race date is invalid");

    await baseEnv.SOURCE_STAGING.put(
      "table.json",
      JSON.stringify(createTableStage("jv", run.run_id, "jvd_ra", [{ kaisai_nen: "2026" }])),
    );
    await expect(handleJob(job, baseEnv, now)).rejects.toThrow("JV race date is invalid");

    await baseEnv.SOURCE_STAGING.put(
      "table.json",
      JSON.stringify(
        createTableStage("jv", run.run_id, "jvd_ra", [
          { kaisai_nen: "invalid", kaisai_tsukihi: "0905" },
        ]),
      ),
    );
    await expect(handleJob(job, baseEnv, now)).rejects.toThrow("JV race date is invalid");

    await baseEnv.SOURCE_STAGING.put(
      "table.json",
      JSON.stringify(
        createTableStage("jv", run.run_id, "jvd_ra", [
          { kaisai_nen: "2026", kaisai_tsukihi: "0905" },
        ]),
      ),
    );
    baseEnv.REALTIME_SYNC = {
      fetch: () => Promise.resolve(new Response(null, { status: 503 })),
    };
    await expect(handleJob(job, baseEnv, now)).rejects.toThrow(
      "Realtime scheduling notification failed",
    );
    expect((await getRun(baseEnv.DB, run.run_id)).status).not.toBe("succeeded");
  });

  test("completes a Neon dispatch with no pending tables", async () => {
    const run = await createTestRun();
    await prepareTable(run.run_id);
    await markCatalogTable(baseEnv.DB, run.run_id, "nvd_ra", "99", 0, now);
    await markNeonTable(baseEnv.DB, run.run_id, "nvd_ra", now);
    await handleJob(
      { provider: "nv", runDate: "20260904", runId: run.run_id, type: "neon-dispatch" },
      baseEnv,
      now,
    );
    expect((await getRun(baseEnv.DB, run.run_id)).status).toBe("succeeded");
  });

  test("rejects Neon work when the R2 barrier is incomplete", async () => {
    const run = await createTestRun();
    await prepareTable(run.run_id);
    await expect(
      handleJob(
        { provider: "nv", runDate: "20260904", runId: run.run_id, type: "neon-dispatch" },
        baseEnv,
        now,
      ),
    ).rejects.toThrow("before R2 Catalog");
    await expect(
      handleJob(
        {
          provider: "nv",
          runDate: "20260904",
          runId: run.run_id,
          tableName: "nvd_ra",
          tableStagingKey: "table.json",
          type: "neon-table",
        },
        baseEnv,
        now,
      ),
    ).rejects.toThrow("before R2 Catalog");
  });

  test("recovery re-notifies index waiters even when every index is already ready", async () => {
    const run = await createTestRun();
    await markAcquired(baseEnv.DB, run.run_id, "raw.ndjson", now);
    await prepareTable(run.run_id);
    await baseEnv.DB.prepare(
      "update sync_run_tables set catalog_status = 'index_pending' where run_id = ?",
    )
      .bind(run.run_id)
      .run();
    await handleJob(
      { provider: "nv", runDate: "20260904", runId: run.run_id, type: "recover" },
      baseEnv,
      now,
    );
    expect(queue.messages).toEqual([
      {
        partitionValue: "2026",
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        type: "index-plan",
      },
    ]);
  });

  test("recovery routes staged data to R2 Bucket, R2 Catalog, or Neon queues", async () => {
    const missing = await createTestRun();
    await expect(
      handleJob(
        { provider: "nv", runDate: "20260904", runId: missing.run_id, type: "recover" },
        baseEnv,
        now,
      ),
    ).rejects.toThrow("scheduled acquisition");

    await markAcquired(baseEnv.DB, missing.run_id, "raw.ndjson", now);
    await handleJob(
      { provider: "nv", runDate: "20260904", runId: missing.run_id, type: "recover" },
      baseEnv,
      now,
    );
    expect(queue.messages.at(-1)?.type).toBe("r2-bucket-nvlink");

    await prepareTable(missing.run_id);
    await handleJob(
      { provider: "nv", runDate: "20260904", runId: missing.run_id, type: "recover" },
      baseEnv,
      now,
    );
    expect(queue.messages.at(-1)?.type).toBe("catalog-table");

    await markCatalogTable(baseEnv.DB, missing.run_id, "nvd_ra", "99", 0, now);
    await handleJob(
      { provider: "nv", runDate: "20260904", runId: missing.run_id, type: "recover" },
      baseEnv,
      now,
    );
    expect(queue.messages.at(-1)?.type).toBe("neon-dispatch");

    await markNeonTable(baseEnv.DB, missing.run_id, "nvd_ra", now);
    await handleJob(
      { provider: "nv", runDate: "20260904", runId: missing.run_id, type: "recover" },
      baseEnv,
      now,
    );
    expect(queue.messages.at(-1)?.type).toBe("neon-dispatch");

    const before = queue.messages.length;
    await completeRun(baseEnv.DB, missing.run_id, "succeeded", 1, now);
    await handleJob(
      { provider: "nv", runDate: "20260904", runId: missing.run_id, type: "recover" },
      baseEnv,
      now,
    );
    expect(queue.messages).toHaveLength(before);

    const { run: empty } = await createRun(baseEnv.DB, "nv", "20260905", "daily", 2, now, false);
    await completeRun(baseEnv.DB, empty.run_id, "succeeded_empty", 0, now);
    await handleJob(
      { provider: "nv", runDate: "20260905", runId: empty.run_id, type: "recover" },
      baseEnv,
      now,
    );
    expect(queue.messages).toHaveLength(before);
  });

  test("rejects missing or mismatched staging objects", async () => {
    const run = await createTestRun();
    await expect(
      handleJob(
        {
          provider: "nv",
          runDate: "20260904",
          runId: run.run_id,
          tableName: "nvd_ra",
          tableStagingKey: "missing",
          type: "catalog-table",
        },
        baseEnv,
        now,
      ),
    ).rejects.toThrow("not found");
    await prepareTable(run.run_id);
    await baseEnv.SOURCE_STAGING.put(
      "wrong.json",
      JSON.stringify(createTableStage("nv", "another-run", "nvd_ra", [{}])),
    );
    await expect(
      handleJob(
        {
          provider: "nv",
          runDate: "20260904",
          runId: run.run_id,
          tableName: "nvd_ra",
          tableStagingKey: "wrong.json",
          type: "catalog-table",
        },
        baseEnv,
        now,
      ),
    ).rejects.toThrow("identity mismatch");
  });

  test.each(["catalog-table", "neon-table"] satisfies ReadonlyArray<SyncJob["type"]>)(
    "preserves a committed table when %s downstream publication fails",
    async (type) => {
      const run = await createTestRun();
      await prepareTable(run.run_id);
      await markCatalogTable(baseEnv.DB, run.run_id, "nvd_ra", "99", 0, now);
      await markNeonTable(baseEnv.DB, run.run_id, "nvd_ra", now);
      await recordJobFailure(
        {
          provider: "nv",
          runDate: "20260904",
          runId: run.run_id,
          tableName: "nvd_ra",
          tableStagingKey: "table.json",
          type,
        },
        baseEnv,
        new Error("Notification unavailable"),
        now,
      );

      const tables = await listRunTables(baseEnv.DB, run.run_id);
      expect(tables[0]?.catalog_status).toBe("succeeded");
      expect(tables[0]?.neon_status).toBe("succeeded");
      expect((await getRun(baseEnv.DB, run.run_id)).status).toBe("publication_failed");
    },
  );

  test("records safe job failure stages", async () => {
    const run = await createTestRun();
    await prepareTable(run.run_id);
    await recordJobFailure(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        tableStagingKey: "table.json",
        type: "catalog-table",
      },
      baseEnv,
      new Error("catalog"),
      now,
    );
    expect((await getRun(baseEnv.DB, run.run_id)).error_stage).toBe("catalog");
    await recordJobFailure(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        tableStagingKey: "table.json",
        type: "catalog-table",
      },
      baseEnv,
      new TransientJobError("catalog-append-stage"),
      now,
    );
    expect((await getRun(baseEnv.DB, run.run_id)).error_stage).toBe("catalog-append-stage");
    await recordJobFailure(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        tableStagingKey: "table.json",
        type: "catalog-table",
      },
      baseEnv,
      new PermanentJobError("catalog-schema"),
      now,
    );
    expect((await listRunTables(baseEnv.DB, run.run_id))[0]?.catalog_status).toBe(
      "failed_permanent",
    );
    await recordJobFailure(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        tableStagingKey: "table.json",
        type: "neon-table",
      },
      baseEnv,
      new Error("neon"),
      now,
    );
    expect((await getRun(baseEnv.DB, run.run_id)).error_stage).toBe("neon");
    await recordJobFailure(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        stagingKey: "raw",
        type: "r2-bucket-nvlink",
      },
      baseEnv,
      new Error("parse"),
      now,
    );
    expect((await getRun(baseEnv.DB, run.run_id)).error_stage).toBe("r2-bucket-nvlink");
    await recordJobFailure(
      {
        partitionValue: "2026",
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        type: "index-plan",
      },
      baseEnv,
      new PermanentJobError("catalog-not-found"),
      now,
    );
    expect((await getRun(baseEnv.DB, run.run_id)).error_stage).toBe("catalog-not-found");
    expect((await listRunTables(baseEnv.DB, run.run_id))[0]?.catalog_status).toBe(
      "failed_permanent",
    );
    await recordJobFailure(
      {
        chunkId: "chunk",
        partitionValue: "2026",
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        type: "index-file",
      },
      baseEnv,
      new PermanentJobError("index-identity"),
      now,
    );
    await recordJobFailure(
      {
        provider: "nv",
        runDate: "20260904",
        runId: run.run_id,
        tableName: "nvd_ra",
        tableStagingKey: "table.json",
        type: "neon-table",
      },
      baseEnv,
      new PermanentJobError("neon-schema"),
      now,
    );
    expect((await listRunTables(baseEnv.DB, run.run_id))[0]?.neon_status).toBe("failed_permanent");
    await markAcquired(baseEnv.DB, run.run_id, "raw.ndjson", now);
    const queued = queue.messages.length;
    await handleJob(
      { provider: "nv", runDate: "20260904", runId: run.run_id, type: "recover" },
      baseEnv,
      now,
    );
    expect(queue.messages).toHaveLength(queued);
  });
});
