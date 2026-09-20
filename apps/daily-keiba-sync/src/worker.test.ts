// Runs with bun; verifies Worker routing and read-only readiness responses.
import { readFile } from "node:fs/promises";
import { Miniflare } from "miniflare";
import { afterAll, beforeAll, beforeEach, describe, expect, test, vi } from "vitest";
import { PermanentJobError } from "./errors";
import { layoutByTable } from "./layouts";
import {
  adminRun,
  adminTrigger,
  fetchHandler,
  jobQueueName,
  monitor,
  parseExternalRecord,
  queueHandler,
  scheduled,
} from "./worker";
import type { Env, RunRow, SyncJob } from "./types";

const mocks = vi.hoisted(() => ({
  acquireRun: vi.fn(),
  cachedCursor: vi.fn(),
  createRun: vi.fn(),
  createRunWithDataSpec: vi.fn(),
  dailyRun: vi.fn(),
  handleJob: vi.fn(),
  latestRun: vi.fn(),
  getRunReadiness: vi.fn(),
  getReadyIndexPartitions: vi.fn(),
  listTargets: vi.fn(),
  markParsed: vi.fn(),
  providerCursor: vi.fn(),
  purgeAllCaches: vi.fn(),
  purgeCursorCache: vi.fn(),
  recordFailure: vi.fn(),
  updateRunStatus: vi.fn(),
}));

vi.mock("./cache", () => ({
  getCachedProviderCursor: mocks.cachedCursor,
  purgeAllProviderCursorCaches: mocks.purgeAllCaches,
  purgeProviderCursorCache: mocks.purgeCursorCache,
}));
vi.mock("./run-readiness", () => ({ getRunReadiness: mocks.getRunReadiness }));
vi.mock("./state", () => ({
  createRun: mocks.createRun,
  createRunWithDataSpec: mocks.createRunWithDataSpec,
  getDailyRun: mocks.dailyRun,
  getLatestRun: mocks.latestRun,
  getProviderAcquisitionCursor: mocks.providerCursor,
  getReadyIndexPartitions: mocks.getReadyIndexPartitions,
  listCatalogTargets: mocks.listTargets,
  markParsed: mocks.markParsed,
  updateRunStatus: mocks.updateRunStatus,
}));
vi.mock("./jobs", () => ({
  acquireRun: mocks.acquireRun,
  handleJob: mocks.handleJob,
  recordJobFailure: mocks.recordFailure,
}));

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

class TestMessage implements Message<SyncJob> {
  readonly id = "message-1";
  readonly timestamp = new Date("2026-09-03T11:00:00Z");
  readonly attempts = 2;
  acked = false;
  retryDelay: number | undefined;

  constructor(readonly body: SyncJob) {}

  ack(): void {
    this.acked = true;
  }

  retry(options?: QueueRetryOptions): void {
    this.retryDelay = options?.delaySeconds;
  }
}

class TestBatch implements MessageBatch<SyncJob> {
  readonly queue: string;
  readonly metadata = { metrics: { backlogBytes: 0, backlogCount: 1 } };

  constructor(
    readonly messages: readonly Message<SyncJob>[],
    queue = "daily-keiba-sync-r2-catalog-jobs",
  ) {
    this.queue = queue;
  }

  ackAll(): void {}
  retryAll(): void {}
}

const run = (status = "queued", updatedAt = "2026-09-03T10:00:00.000Z"): RunRow => ({
  advance_cursor: 1,
  data_spec: "RACE",
  catalog_tables: 0,
  completed_at: null,
  cursor_time: "20260903190000",
  error_stage: null,
  files: 0,
  from_time: null,
  neon_tables: 0,
  provider: "jv",
  records: 0,
  run_date: "20260903",
  run_id: "run-1",
  staging_key: null,
  status,
  to_time: null,
  updated_at: updatedAt,
});

let miniflare: Miniflare;
let env: Env;
let queue: RecordingQueue;

beforeAll(async () => {
  miniflare = new Miniflare({
    bindings: {
      ADMIN_TOKEN: "admin",
      JRA_VAN_WORKER_API_TOKEN: "jv-token",
      NEON_DATABASE_URL: "postgresql://example",
      R2_BUCKET_NAME: "pc-keiba-r2-catalog",
      R2_CATALOG_NAMESPACE: "pc_keiba",
      R2_CATALOG_TOKEN: "catalog-token",
      R2_CATALOG_URI: "https://catalog.example",
      R2_CATALOG_WAREHOUSE: "warehouse",
      REALTIME_ADMIN_TOKEN: "realtime-admin",
      UMMACON_WORKER_API_TOKEN: "nv-token",
    },
    compatibilityDate: "2026-06-18",
    d1Databases: { DB: "worker-test" },
    modules: true,
    r2Buckets: { CATALOG_BUCKET: "catalog-test", SOURCE_STAGING: "staging-test" },
    script: "export default {}",
    serviceBindings: {
      JV_SOURCE: async () => new Response("ok"),
      NV_SOURCE: async () => new Response("ok"),
    },
  });
  env = await miniflare.getBindings<Env>();
  const migrations: readonly string[] = await Promise.all(
    [
      "0001_initial.sql",
      "0002_catalog_index.sql",
      "0003_acquisition_window.sql",
      "0005_acquisition_data_spec.sql",
    ].map((name) => readFile(new URL(`../migrations/${name}`, import.meta.url), "utf8")),
  );
  await env.DB.exec(
    migrations
      .join("\n")
      .split("\n")
      .filter((line) => !line.trimStart().startsWith("--"))
      .join(" "),
  );
});

beforeEach(() => {
  queue = new RecordingQueue();
  env = {
    ...env,
    JV_RAW_STAGE_JOBS: queue,
    NEON_JOBS: queue,
    NV_RAW_STAGE_JOBS: queue,
    R2_CATALOG_JOBS: queue,
  };
  mocks.acquireRun.mockReset().mockResolvedValue(undefined);
  mocks.cachedCursor.mockReset().mockResolvedValue(null);
  mocks.createRun.mockReset().mockResolvedValue({ created: true, run: run() });
  mocks.createRunWithDataSpec.mockReset().mockResolvedValue({ created: true, run: run() });
  mocks.dailyRun.mockReset().mockResolvedValue(run("succeeded"));
  mocks.latestRun.mockReset().mockResolvedValue(run());
  mocks.getRunReadiness.mockReset().mockResolvedValue({
    ...run(),
    catalog_ready: true,
    neon_backup_complete: false,
  });
  mocks.getReadyIndexPartitions.mockReset().mockResolvedValue(new Set(["2026"]));
  mocks.markParsed.mockReset().mockResolvedValue(undefined);
  mocks.providerCursor.mockReset().mockResolvedValue(null);
  mocks.purgeAllCaches.mockReset().mockResolvedValue(undefined);
  mocks.purgeCursorCache.mockReset().mockResolvedValue(undefined);
  mocks.listTargets
    .mockReset()
    .mockResolvedValue([{ partition_field: "kaisai_nen", table_name: "nvd_ra" }]);
  mocks.handleJob.mockReset().mockResolvedValue(undefined);
  mocks.recordFailure.mockReset().mockResolvedValue(undefined);
  mocks.updateRunStatus.mockReset().mockResolvedValue(undefined);
});

afterAll(async () => {
  await miniflare.dispose();
});

describe("daily sync Worker", () => {
  test("routes R2 bucket, R2 Catalog, and Neon jobs to distinct queues", () => {
    expect(
      jobQueueName({
        provider: "jv",
        runDate: "20260903",
        runId: "run",
        stagingKey: "raw",
        type: "r2-bucket-jvlink",
      }),
    ).toBe("daily-keiba-sync-jv-raw-stage-jobs");
    expect(
      jobQueueName({
        provider: "nv",
        runDate: "20260903",
        runId: "run",
        stagingKey: "raw",
        type: "r2-bucket-nvlink",
      }),
    ).toBe("daily-keiba-sync-nv-raw-stage-jobs");
    expect(
      jobQueueName({ provider: "jv", runDate: "20260903", runId: "run", type: "recover" }),
    ).toBe("daily-keiba-sync-r2-catalog-jobs");
    expect(
      jobQueueName({ provider: "jv", runDate: "20260903", runId: "run", type: "neon-dispatch" }),
    ).toBe("daily-keiba-sync-neon-jobs");
  });
  test("exposes only a public health response", async () => {
    const health = await fetchHandler(new Request("https://example/health"), env);
    expect(await health.json()).toEqual({
      ok: true,
      runtime: "cloudflare-workers-native-daily-keiba-sync",
    });
    expect((await fetchHandler(new Request("https://example/admin/status"), env)).status).toBe(401);
    expect(
      (
        await fetchHandler(
          new Request("https://example/admin/status", {
            headers: { Authorization: "Bearer admix" },
          }),
          env,
        )
      ).status,
    ).toBe(401);
    expect(
      (
        await fetchHandler(
          new Request("https://example/missing", { headers: { Authorization: "Bearer admin" } }),
          env,
        )
      ).status,
    ).toBe(404);
  });

  test("acquires and stages a validated manual run directly", async () => {
    const response = await fetchHandler(
      new Request("https://example/admin/run", {
        body: JSON.stringify({ lookbackDays: 5, provider: "nv", runDate: "20260903" }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(response.status).toBe(200);
    expect(mocks.acquireRun).toHaveBeenCalledWith(
      {
        advanceCursor: false,
        cursorTime: expect.stringMatching(/^20\d{12}$/),
        fromTime: "20260829000000",
        provider: "nv",
        runDate: "20260903",
        runId: "run-1",
        toTime: null,
      },
      env,
      expect.any(Date),
    );
    expect(queue.messages).toEqual([]);
    expect(mocks.createRunWithDataSpec).toHaveBeenCalledWith({
      db: expect.anything(),
      provider: "nv",
      dataSpec: "RACE",
      runDate: "20260903",
      trigger: "manual",
      lookbackDays: 5,
      now: expect.any(Date),
      force: true,
      fromTime: "20260829000000",
      toTime: null,
      cursorTime: expect.stringMatching(/^20\d{12}$/),
      advanceCursor: false,
    });
  });

  test.each(["COMM", "RACECOMM"])(
    "pins manual %s requests without advancing the race cursor",
    async (dataSpec) => {
      mocks.listTargets.mockResolvedValueOnce([
        { table_name: "jvd_cs", partition_field: "__unpartitioned__" },
      ]);
      mocks.createRunWithDataSpec.mockResolvedValueOnce({
        created: true,
        run: { ...run(), data_spec: dataSpec, advance_cursor: 0 },
      });
      const response = await adminRun(
        new Request("https://example/admin/run", {
          method: "POST",
          body: JSON.stringify({
            provider: "jv",
            dataSpec,
            fromTime: "20260901000000",
            toTime: "20260904090000",
          }),
        }),
        env,
        new Date("2026-09-04T00:00:00Z"),
      );
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({
        dataSpec,
        fromTime: "20260901000000",
        toTime: "20260904090000",
      });
      expect(mocks.createRunWithDataSpec).toHaveBeenCalledWith({
        db: expect.anything(),
        provider: "jv",
        dataSpec,
        runDate: "20260904",
        trigger: "manual",
        lookbackDays: 7,
        now: expect.any(Date),
        force: true,
        fromTime: "20260901000000",
        toTime: "20260904090000",
        cursorTime: "20260904090000",
        advanceCursor: false,
      });
      expect(mocks.acquireRun).toHaveBeenCalledWith(
        expect.objectContaining({ provider: "jv", advanceCursor: false }),
        env,
        expect.any(Date),
      );
      expect(mocks.cachedCursor).not.toHaveBeenCalled();
      expect(mocks.createRun).not.toHaveBeenCalled();
    },
  );

  test("the authenticated COMM endpoint persists the spec in actual D1 state", async () => {
    const state = await vi.importActual<typeof import("./state")>("./state");
    mocks.createRunWithDataSpec.mockImplementationOnce(state.createRunWithDataSpec);
    mocks.listTargets.mockResolvedValueOnce([
      { table_name: "jvd_cs", partition_field: "__unpartitioned__" },
    ]);
    const response = await fetchHandler(
      new Request("https://example/admin/run", {
        method: "POST",
        headers: { Authorization: "Bearer admin" },
        body: JSON.stringify({
          provider: "jv",
          dataSpec: "COMM",
          runDate: "20260904",
          fromTime: "20260901000000",
          toTime: "20260904090000",
        }),
      }),
      env,
    );
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ dataSpec: "COMM" });
    expect(
      await env.DB.prepare(
        "select data_spec, advance_cursor, from_time, to_time from sync_runs where data_spec = 'COMM'",
      ).all(),
    ).toMatchObject({
      results: [
        {
          data_spec: "COMM",
          advance_cursor: 0,
          from_time: "20260901000000",
          to_time: "20260904090000",
        },
      ],
    });
    expect(await state.getProviderAcquisitionCursor(env.DB, "jv")).toBeNull();
  });

  test.each([
    { dataSpec: "BAD", provider: "jv", fromTime: "20260901000000", toTime: "20260904090000" },
    { dataSpec: null, provider: "jv", fromTime: "20260901000000", toTime: "20260904090000" },
    { dataSpec: 1, provider: "jv", fromTime: "20260901000000", toTime: "20260904090000" },
    { dataSpec: "COMM", provider: "nv", fromTime: "20260901000000", toTime: "20260904090000" },
    { dataSpec: "RACECOMM", provider: "nv", fromTime: "20260901000000", toTime: "20260904090000" },
    { dataSpec: "COMM", provider: "jv", toTime: "20260904090000" },
    { dataSpec: "COMM", provider: "jv", fromTime: "20260901000000" },
    { dataSpec: "COMM", provider: "jv", fromTime: "20260901000000", toTime: "20260904100000" },
    { dataSpec: "COMM", provider: "jv", fromTime: "20260901000000", toTime: null },
  ])("rejects unsafe COMM/spec requests before state or source I/O: %j", async (body) => {
    const response = await adminRun(
      new Request("https://example/admin/run", { method: "POST", body: JSON.stringify(body) }),
      env,
      new Date("2026-09-04T00:00:00Z"),
    );
    expect(response.status).toBe(400);
    expect(mocks.listTargets).not.toHaveBeenCalled();
    expect(mocks.createRunWithDataSpec).not.toHaveBeenCalled();
    expect(mocks.acquireRun).not.toHaveBeenCalled();
  });

  test.each([
    { targets: [] },
    { targets: [{ table_name: "jvd_cs", partition_field: "kaisai_nen" }] },
    { targets: [{ table_name: "jvd_ra", partition_field: "__unpartitioned__" }] },
  ])(
    "requires an enabled unpartitioned CS target before COMM creation: %j",
    async ({ targets }) => {
      mocks.listTargets.mockResolvedValueOnce(targets);
      const response = await adminRun(
        new Request("https://example/admin/run", {
          method: "POST",
          body: JSON.stringify({
            provider: "jv",
            dataSpec: "COMM",
            fromTime: "20260901000000",
            toTime: "20260904090000",
          }),
        }),
        env,
        new Date("2026-09-04T00:00:00Z"),
      );
      expect(response.status).toBe(409);
      expect(await response.json()).toStrictEqual({
        error: "COMM Catalog target is not configured",
      });
      expect(mocks.createRunWithDataSpec).not.toHaveBeenCalled();
      expect(mocks.acquireRun).not.toHaveBeenCalled();
    },
  );

  test("COMM acquisition remains authenticated before reading configuration", async () => {
    const response = await fetchHandler(
      new Request("https://example/admin/run", {
        method: "POST",
        body: JSON.stringify({
          provider: "jv",
          dataSpec: "COMM",
          fromTime: "20260901000000",
          toTime: "20260904090000",
        }),
      }),
      env,
    );
    expect(response.status).toBe(401);
    expect(mocks.listTargets).not.toHaveBeenCalled();
    expect(mocks.createRunWithDataSpec).not.toHaveBeenCalled();
  });

  test("records acquisition failures without misreporting them as invalid input", async () => {
    mocks.acquireRun.mockRejectedValueOnce(new Error("private upstream failure"));
    const response = await fetchHandler(
      new Request("https://example/admin/run", {
        body: JSON.stringify({
          fromTime: "20260904070000",
          provider: "jv",
          runDate: "20260904",
          toTime: "20260904070100",
        }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );

    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({ error: "Acquisition failed" });
    expect(mocks.updateRunStatus).toHaveBeenCalledWith(
      expect.anything(),
      "run-1",
      "acquisition_failed",
      expect.any(Date),
      "acquisition",
    );
  });

  test("accepts an exact provider update timestamp for manual differential acquisition", async () => {
    const response = await fetchHandler(
      new Request("https://example/admin/run", {
        body: JSON.stringify({
          fromTime: "20260903123456",
          provider: "jv",
          runDate: "20260904",
          toTime: "20260904112233",
        }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      fromTime: "20260903123456",
      toTime: "20260904112233",
    });
    expect(mocks.acquireRun).toHaveBeenCalledWith(
      expect.objectContaining({
        advanceCursor: false,
        fromTime: "20260903123456",
        toTime: "20260904112233",
      }),
      env,
      expect.any(Date),
    );
  });

  test("uses safe manual defaults and avoids requeueing an existing run", async () => {
    mocks.createRunWithDataSpec.mockResolvedValueOnce({ created: false, run: run() });
    const response = await fetchHandler(
      new Request("https://example/admin/run", {
        body: JSON.stringify({ provider: "jv" }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(response.status).toBe(200);
    expect(queue.messages).toEqual([]);
    expect(mocks.acquireRun).not.toHaveBeenCalled();
  });

  test("rejects malformed manual run requests", async () => {
    const response = await fetchHandler(
      new Request("https://example/admin/run", {
        body: JSON.stringify({ lookbackDays: 0, provider: "bad" }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(response.status).toBe(400);
    const arrayResponse = await fetchHandler(
      new Request("https://example/admin/run", {
        body: "[]",
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(arrayResponse.status).toBe(400);
    for (const body of [
      { fromTime: 123, provider: "nv" },
      { fromTime: "bad", provider: "nv" },
      { fromTime: "20260230000000", provider: "nv" },
      { lookbackDays: 0, provider: "nv" },
      { lookbackDays: 32, provider: "nv" },
      { lookbackDays: 1.5, provider: "nv" },
      { lookbackDays: "2", provider: "nv" },
      { fromTime: "20260903000000", provider: "nv", toTime: "20260904000000" },
      { fromTime: "20260904000000", provider: "jv", toTime: "20260903000000" },
    ]) {
      const invalidWindow = await fetchHandler(
        new Request("https://example/admin/run", {
          body: JSON.stringify(body),
          headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
          method: "POST",
        }),
        env,
      );
      expect(invalidWindow.status).toBe(400);
    }
  });

  test("runs and monitors the exact daily acquisition path through an authenticated trigger", async () => {
    const runResponse = await fetchHandler(
      new Request("https://example/admin/trigger", {
        body: JSON.stringify({ action: "run", provider: "nv", runDate: "20260904" }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(await runResponse.json()).toEqual({
      action: "run",
      provider: "nv",
      runDate: "20260904",
      runId: "run-1",
    });
    expect(mocks.createRunWithDataSpec).toHaveBeenCalledWith({
      db: expect.anything(),
      provider: "nv",
      dataSpec: "RACE",
      runDate: "20260904",
      trigger: "daily",
      lookbackDays: 2,
      now: expect.any(Date),
      force: false,
      fromTime: "20260902000000",
      toTime: null,
      cursorTime: expect.any(String),
      advanceCursor: true,
    });

    const futureRunResponse = await fetchHandler(
      new Request("https://example/admin/trigger", {
        body: JSON.stringify({ action: "run", provider: "jv", runDate: "20260905" }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(await futureRunResponse.json()).toMatchObject({ runDate: "20260905" });
    expect(mocks.createRunWithDataSpec).toHaveBeenLastCalledWith({
      db: expect.anything(),
      provider: "jv",
      dataSpec: "RACE",
      runDate: "20260905",
      trigger: "daily",
      lookbackDays: 2,
      now: expect.any(Date),
      force: false,
      fromTime: "20260903000000",
      toTime: expect.any(String),
      cursorTime: expect.any(String),
      advanceCursor: true,
    });

    const forcedRunResponse = await fetchHandler(
      new Request("https://example/admin/trigger", {
        body: JSON.stringify({
          action: "run",
          force: true,
          provider: "nv",
          runDate: "20260904",
        }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(forcedRunResponse.status).toBe(200);
    expect(mocks.createRunWithDataSpec).toHaveBeenLastCalledWith({
      db: expect.anything(),
      provider: "nv",
      dataSpec: "RACE",
      runDate: "20260904",
      trigger: "daily",
      lookbackDays: 2,
      now: expect.any(Date),
      force: true,
      fromTime: "20260902000000",
      toTime: null,
      cursorTime: expect.any(String),
      advanceCursor: true,
    });

    const monitorResponse = await fetchHandler(
      new Request("https://example/admin/trigger", {
        body: JSON.stringify({ action: "monitor", provider: "jv", runDate: "20260904" }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(await monitorResponse.json()).toEqual({
      action: "monitor",
      provider: "jv",
      runDate: "20260904",
      runId: "run-1",
    });

    mocks.latestRun.mockResolvedValueOnce(null);
    const missingResponse = await fetchHandler(
      new Request("https://example/admin/trigger", {
        body: JSON.stringify({ action: "monitor", provider: "jv", runDate: "20260904" }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(await missingResponse.json()).toEqual({
      action: "monitor",
      provider: "jv",
      runDate: "20260904",
      runId: null,
    });
  });

  test.each(["run", "monitor"])(
    "rejects future %s dates before reserving a scheduled run",
    async (action) => {
      const response = await adminTrigger(
        new Request("https://example/admin/trigger", {
          body: JSON.stringify({ action, provider: "jv", runDate: "20260906" }),
          method: "POST",
        }),
        env,
        new Date("2026-09-04T01:34:31Z"),
      );
      expect(response.status).toBe(400);
      expect(mocks.createRunWithDataSpec).not.toHaveBeenCalled();
      expect(mocks.dailyRun).not.toHaveBeenCalled();
      expect(mocks.acquireRun).not.toHaveBeenCalled();
    },
  );

  test("accepts the current JST acquisition date at the UTC date boundary", async () => {
    const response = await adminTrigger(
      new Request("https://example/admin/trigger", {
        body: JSON.stringify({ action: "run", provider: "jv", runDate: "20260906" }),
        method: "POST",
      }),
      env,
      new Date("2026-09-05T15:00:00Z"),
    );
    expect(response.status).toBe(200);
    expect(mocks.acquireRun).toHaveBeenCalledTimes(1);
  });

  test("rejects invalid or failed daily admin triggers", async () => {
    for (const body of [
      "[]",
      JSON.stringify({ action: "bad", provider: "jv" }),
      JSON.stringify({ action: "run", provider: "bad" }),
      JSON.stringify({ action: "run", provider: "jv", runDate: "bad" }),
      JSON.stringify({ action: "run", force: "yes", provider: "jv" }),
    ]) {
      const response = await fetchHandler(
        new Request("https://example/admin/trigger", {
          body,
          headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
          method: "POST",
        }),
        env,
      );
      expect(response.status).toBe(400);
    }

    mocks.createRunWithDataSpec.mockRejectedValueOnce(new Error("private failure"));
    const failed = await fetchHandler(
      new Request("https://example/admin/trigger", {
        body: JSON.stringify({ action: "run", provider: "jv" }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(failed.status).toBe(502);
    expect(await failed.json()).toEqual({ error: "Trigger failed" });
  });

  test("purges one or all provider cursor caches", async () => {
    const all = await fetchHandler(
      new Request("https://example/admin/cache/purge", {
        body: "{}",
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(await all.json()).toEqual({ purged: ["jv", "nv"] });
    expect(mocks.purgeAllCaches).toHaveBeenCalledWith(env.SYNC_CACHE);

    const one = await fetchHandler(
      new Request("https://example/admin/cache/purge", {
        body: JSON.stringify({ provider: "nv" }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(await one.json()).toEqual({ purged: ["nv"] });
    expect(mocks.purgeCursorCache).toHaveBeenCalledWith(env.SYNC_CACHE, "nv");

    for (const body of ["[]", JSON.stringify({ provider: "bad" })]) {
      const invalid = await fetchHandler(
        new Request("https://example/admin/cache/purge", {
          body,
          headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
          method: "POST",
        }),
        env,
      );
      expect(invalid.status).toBe(400);
    }
  });

  test("queues the explicit unpartitioned course target", async () => {
    mocks.listTargets.mockResolvedValueOnce([
      { table_name: "jvd_cs", partition_field: "__unpartitioned__" },
    ]);
    const response = await fetchHandler(
      new Request("https://example/admin/index", {
        body: JSON.stringify({ partitionValue: "__all__", provider: "jv", tableName: "jvd_cs" }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(response.status).toBe(200);
    expect(queue.messages).toHaveLength(1);
    expect(queue.messages[0]).toMatchObject({
      partitionValue: "__all__",
      provider: "jv",
      tableName: "jvd_cs",
      type: "index-plan",
    });
  });

  test.each([
    {
      tableName: "jvd_cs",
      partitionField: "__unpartitioned__",
      partitionValue: "2026",
      provider: "jv",
    },
    {
      tableName: "jvd_cs",
      partitionField: "kaisai_nen",
      partitionValue: "__all__",
      provider: "jv",
    },
    {
      tableName: "jvd_cs",
      partitionField: "__unpartitioned__",
      partitionValue: "__all__",
      provider: "nv",
    },
    {
      tableName: "nvd_ra",
      partitionField: "kaisai_nen",
      partitionValue: "__all__",
      provider: "nv",
    },
  ])("rejects a mismatched unpartitioned index request: %j", async (entry) => {
    mocks.listTargets.mockResolvedValueOnce([
      { table_name: entry.tableName, partition_field: entry.partitionField },
    ]);
    const response = await fetchHandler(
      new Request("https://example/admin/index", {
        body: JSON.stringify({
          partitionValue: entry.partitionValue,
          provider: entry.provider,
          tableName: entry.tableName,
        }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(response.status).toBe(400);
    expect(queue.messages).toHaveLength(0);
  });

  test("queues only an explicitly configured partition index", async () => {
    const response = await fetchHandler(
      new Request("https://example/admin/index", {
        body: JSON.stringify({ partitionValue: "2026", provider: "nv", tableName: "nvd_ra" }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(response.status).toBe(200);
    expect(queue.messages[0]).toMatchObject({
      partitionValue: "2026",
      provider: "nv",
      tableName: "nvd_ra",
      type: "index-plan",
    });
    const rejected = await fetchHandler(
      new Request("https://example/admin/index", {
        body: JSON.stringify({ partitionValue: "bad", provider: "nv", tableName: "nvd_h1" }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(rejected.status).toBe(400);
    mocks.listTargets.mockResolvedValueOnce([]);
    const unconfigured = await fetchHandler(
      new Request("https://example/admin/index", {
        body: JSON.stringify({ partitionValue: "2026", provider: "nv", tableName: "nvd_h1" }),
        headers: { Authorization: "Bearer admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(unconfigured.status).toBe(400);
  });

  test("returns latest status and validates query parameters", async () => {
    const status = await fetchHandler(
      new Request("https://example/admin/status?provider=jv&runDate=20260903", {
        headers: { Authorization: "Bearer admin" },
      }),
      env,
    );
    expect(status.status).toBe(200);
    expect(
      (
        await fetchHandler(
          new Request("https://example/admin/status?provider=jv", {
            headers: { Authorization: "Bearer admin" },
          }),
          env,
        )
      ).status,
    ).toBe(200);
    mocks.latestRun.mockResolvedValueOnce(null);
    expect(
      (
        await fetchHandler(
          new Request("https://example/admin/status?provider=jv&runDate=20260903", {
            headers: { Authorization: "Bearer admin" },
          }),
          env,
        )
      ).status,
    ).toBe(404);
    expect(
      (
        await fetchHandler(
          new Request("https://example/admin/status?provider=x", {
            headers: { Authorization: "Bearer admin" },
          }),
          env,
        )
      ).status,
    ).toBe(400);
  });

  test("uses the D1 provider cursor for daily scheduled acquisition", async () => {
    mocks.cachedCursor.mockResolvedValueOnce("20260903193000");
    await scheduled(
      { cron: "0 11 * * *", noRetry: vi.fn(), scheduledTime: Date.parse("2026-09-03T11:00:00Z") },
      env,
    );
    expect(mocks.createRunWithDataSpec).toHaveBeenCalledWith({
      db: expect.anything(),
      provider: "jv",
      dataSpec: "RACE",
      runDate: "20260903",
      trigger: "daily",
      lookbackDays: 2,
      now: expect.any(Date),
      force: false,
      fromTime: "20260903193000",
      toTime: "20260903200000",
      cursorTime: "20260903200000",
      advanceCursor: true,
    });
    await scheduled(
      { cron: "0 17 * * *", noRetry: vi.fn(), scheduledTime: Date.parse("2026-09-03T17:00:00Z") },
      env,
    );
    expect(mocks.dailyRun).toHaveBeenCalledWith(expect.anything(), "nv", "20260904");
    await expect(
      scheduled({ cron: "bad", noRetry: vi.fn(), scheduledTime: 0 }, env),
    ).rejects.toThrow("Unknown cron");
  });

  test("monitor creates missing runs, ignores success/recent work, and recovers stale work", async () => {
    const now = new Date("2026-09-03T12:00:00Z");
    mocks.dailyRun.mockResolvedValueOnce(null);
    await monitor(env, "jv", "20260903", now);
    expect(mocks.createRunWithDataSpec).toHaveBeenCalledTimes(1);

    mocks.dailyRun.mockResolvedValueOnce(run("succeeded"));
    await monitor(env, "jv", "20260903", now);
    mocks.dailyRun.mockResolvedValueOnce(run("succeeded_empty"));
    await monitor(env, "jv", "20260903", now);
    mocks.dailyRun.mockResolvedValueOnce(run("processing", "2026-09-03T11:50:00Z"));
    await monitor(env, "jv", "20260903", now);
    expect(queue.messages).toHaveLength(0);

    mocks.dailyRun.mockResolvedValueOnce(run("failed", "2026-09-03T10:00:00Z"));
    await monitor(env, "jv", "20260903", now);
    expect(mocks.acquireRun).toHaveBeenLastCalledWith(
      expect.objectContaining({
        advanceCursor: true,
        cursorTime: "20260903190000",
        fromTime: "20260827000000",
        toTime: "20260903235959",
      }),
      env,
      now,
    );

    mocks.dailyRun.mockResolvedValueOnce({
      ...run("failed", "2026-09-03T10:00:00Z"),
      advance_cursor: 0,
      cursor_time: null,
      from_time: "20260902030405",
      to_time: "20260903060708",
    });
    await monitor(env, "jv", "20260903", now);
    expect(mocks.acquireRun).toHaveBeenLastCalledWith(
      expect.objectContaining({
        advanceCursor: false,
        cursorTime: "20260903210000",
        fromTime: "20260902030405",
        toTime: "20260903060708",
      }),
      env,
      now,
    );

    mocks.dailyRun.mockResolvedValueOnce({
      ...run("failed", "2026-09-03T10:00:00Z"),
      staging_key: "raw.ndjson",
    });
    await monitor(env, "jv", "20260903", now);
    expect(queue.messages.at(-1)).toEqual({
      provider: "jv",
      runDate: "20260903",
      runId: "run-1",
      type: "recover",
    });
    mocks.dailyRun.mockResolvedValueOnce({
      ...run("failed", "not-a-date"),
      staging_key: "raw.ndjson",
    });
    await monitor(env, "jv", "20260903", now);
    expect(queue.messages.at(-1)?.type).toBe("recover");
  });

  test("strictly validates every typed external record field", () => {
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
    expect(parseExternalRecord(valid).workout_index).toBe(1);
    expect(() => parseExternalRecord(null)).toThrow("external record");
    expect(() => parseExternalRecord({})).toThrow("record columns");
    expect(() => {
      const wrong = { ...valid, extra: "value" };
      Reflect.deleteProperty(wrong, "course");
      parseExternalRecord(wrong);
    }).toThrow("record columns");
    expect(() => parseExternalRecord({ ...valid, kaisai_nen: null })).toThrow("required value");
    expect(parseExternalRecord({ ...valid, course: null }).course).toBeNull();
    expect(() => parseExternalRecord({ ...valid, workout_index: 1.5 })).toThrow("integer value");
    expect(() => parseExternalRecord({ ...valid, course: 1 })).toThrow("string value");
    expect(() => parseExternalRecord({ ...valid, fetched_at: "invalid" })).toThrow(
      "timestamp value",
    );
  });

  test("stages typed netkeiba training rows for Catalog-first synchronization", async () => {
    const layout = layoutByTable("netkeiba_training_workouts");
    const record = Object.fromEntries(
      layout.columns.map((column) => [
        column.name,
        column.name === "kaisai_nen"
          ? "2026"
          : column.catalogType === "int"
            ? 1
            : column.catalogType === "timestamptz"
              ? "2026-09-04T19:30:00.000Z"
              : "value",
      ]),
    );
    mocks.listTargets.mockResolvedValueOnce([
      { partition_field: "kaisai_nen", table_name: "netkeiba_training_workouts" },
    ]);
    const response = await fetchHandler(
      new Request("https://daily.test/internal/stage-netkeiba-training", {
        body: JSON.stringify({
          records: [record],
          runDate: "20260905",
          tableName: "netkeiba_training_workouts",
        }),
        headers: {
          Authorization: "Bearer realtime-admin",
          "Content-Type": "application/json",
        },
        method: "POST",
      }),
      env,
    );
    expect(response.status).toBe(202);
    expect(await response.json()).toStrictEqual({ records: 1, runId: "run-1" });
    expect(mocks.markParsed).toHaveBeenCalledTimes(1);
    expect(queue.messages.at(-1)?.type).toBe("catalog-table");
  });

  test("requires realtime auth and validates external staging payloads", async () => {
    const unauthorized = await fetchHandler(
      new Request("https://daily.test/internal/stage-netkeiba-training", { method: "POST" }),
      env,
    );
    expect(unauthorized.status).toBe(401);
    const invalid = await fetchHandler(
      new Request("https://daily.test/internal/stage-netkeiba-training", {
        body: JSON.stringify({ records: [], runDate: "20260905", tableName: "wrong" }),
        headers: { Authorization: "Bearer realtime-admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(invalid.status).toBe(400);
  });

  test("rejects empty and partition-mismatched external stages", async () => {
    const headers = { Authorization: "Bearer realtime-admin", "Content-Type": "application/json" };
    const empty = await fetchHandler(
      new Request("https://daily.test/internal/stage-netkeiba-training", {
        body: JSON.stringify({
          records: [],
          runDate: "20260905",
          tableName: "netkeiba_training_workouts",
        }),
        headers,
        method: "POST",
      }),
      env,
    );
    expect(empty.status).toBe(400);

    const layout = layoutByTable("netkeiba_training_workouts");
    const record = Object.fromEntries(
      layout.columns.map((column) => [
        column.name,
        column.name === "kaisai_nen"
          ? "2025"
          : column.catalogType === "int"
            ? 1
            : column.catalogType === "timestamptz"
              ? "2026-09-04T19:30:00.000Z"
              : "value",
      ]),
    );
    const mismatch = await fetchHandler(
      new Request("https://daily.test/internal/stage-netkeiba-training", {
        body: JSON.stringify({
          records: [record],
          runDate: "20260905",
          tableName: "netkeiba_training_workouts",
        }),
        headers,
        method: "POST",
      }),
      env,
    );
    expect(mismatch.status).toBe(400);
  });

  test("queues index construction before an external Catalog commit", async () => {
    const layout = layoutByTable("netkeiba_training_workouts");
    const record = Object.fromEntries(
      layout.columns.map((column) => [
        column.name,
        column.name === "kaisai_nen"
          ? "2026"
          : column.catalogType === "int"
            ? 1
            : column.catalogType === "timestamptz"
              ? "2026-09-04T19:30:00.000Z"
              : "value",
      ]),
    );
    mocks.listTargets.mockResolvedValueOnce([
      { partition_field: "kaisai_nen", table_name: "netkeiba_training_workouts" },
    ]);
    mocks.getReadyIndexPartitions.mockResolvedValueOnce(new Set());
    const response = await fetchHandler(
      new Request("https://daily.test/internal/stage-netkeiba-training", {
        body: JSON.stringify({
          records: [record],
          runDate: "20260905",
          tableName: "netkeiba_training_workouts",
        }),
        headers: { Authorization: "Bearer realtime-admin", "Content-Type": "application/json" },
        method: "POST",
      }),
      env,
    );
    expect(response.status).toBe(202);
    expect(queue.messages.at(-1)?.type).toBe("index-plan");
  });

  test("rejects an internal run-status request without an exact run id", async () => {
    const response = await fetchHandler(
      new Request("https://daily.test/internal/run-status", {
        headers: { Authorization: "Bearer realtime-admin" },
      }),
      env,
    );
    expect(response.status).toBe(400);
  });

  test("returns an exact internal run status", async () => {
    const response = await fetchHandler(
      new Request(
        "https://daily.test/internal/run-status?runId=12345678-1234-1234-1234-123456789abc",
        {
          headers: { Authorization: "Bearer realtime-admin" },
        },
      ),
      env,
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(await response.json()).toStrictEqual(
      expect.objectContaining({
        run_id: "run-1",
        catalog_ready: true,
        neon_backup_complete: false,
      }),
    );
    expect(mocks.getRunReadiness).toHaveBeenCalledTimes(1);
    expect(mocks.getRunReadiness.mock.calls[0]?.[1]).toBe("12345678-1234-1234-1234-123456789abc");
  });

  test("serves real SQLite Catalog receipts through the authenticated HTTP handler", async () => {
    const state = await vi.importActual<typeof import("./state")>("./state");
    const readiness = await vi.importActual<typeof import("./run-readiness")>("./run-readiness");
    const now: Date = new Date("2026-09-16T00:00:00Z");
    const { run: stored } = await state.createRun(env.DB, "jv", "20260916", "manual", 1, now, true);
    await state.markParsed(
      env.DB,
      stored.run_id,
      1,
      1,
      [
        {
          table_name: "netkeiba_training_workouts",
          staging_key: "training-stage",
          source_records: 1,
          catalog_status: "succeeded",
          neon_status: "failed_permanent",
          partitions: [],
        },
      ],
      now,
    );
    await state.updateRunStatus(env.DB, stored.run_id, "neon_failed", now, "neon-schema");
    mocks.getRunReadiness.mockImplementationOnce(readiness.getRunReadiness);
    const url: URL = new URL("https://daily.test/internal/run-status");
    url.searchParams.set("runId", stored.run_id);
    const response = await fetchHandler(
      new Request(url, { headers: { Authorization: "Bearer realtime-admin" } }),
      env,
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(await response.json()).toStrictEqual(
      expect.objectContaining({
        catalog_ready: true,
        neon_backup_complete: false,
        status: "neon_failed",
        error_stage: "neon-schema",
      }),
    );
    expect((await state.getRun(env.DB, stored.run_id)).status).toBe("neon_failed");
    expect(
      (await state.getRunTable(env.DB, stored.run_id, "netkeiba_training_workouts")).neon_status,
    ).toBe("failed_permanent");
    expect(queue.messages).toStrictEqual([]);
  });

  test("authenticates before reading internal readiness", async () => {
    const response = await fetchHandler(
      new Request(
        "https://daily.test/internal/run-status?runId=12345678-1234-1234-1234-123456789abc",
      ),
      env,
    );
    expect(response.status).toBe(401);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(mocks.getRunReadiness).not.toHaveBeenCalled();
  });

  test("returns an uncached 404 only for an absent run", async () => {
    mocks.getRunReadiness.mockResolvedValueOnce(null);
    const response = await fetchHandler(
      new Request(
        "https://daily.test/internal/run-status?runId=12345678-1234-1234-1234-123456789abc",
        { headers: { Authorization: "Bearer realtime-admin" } },
      ),
      env,
    );
    expect(response.status).toBe(404);
    expect(response.headers.get("cache-control")).toBe("no-store");
  });

  test("does not disguise storage failures as a missing run or leak details", async () => {
    mocks.getRunReadiness.mockRejectedValueOnce(new Error("private storage detail"));
    const response = await fetchHandler(
      new Request(
        "https://daily.test/internal/run-status?runId=12345678-1234-1234-1234-123456789abc",
        { headers: { Authorization: "Bearer realtime-admin" } },
      ),
      env,
    );
    expect(response.status).toBe(503);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(await response.json()).toStrictEqual({ error: "Run status unavailable" });
  });

  test("acks successful queue messages and retries safe failures", async () => {
    const job: SyncJob = { provider: "jv", runDate: "20260903", runId: "run-1", type: "recover" };
    const success = new TestMessage(job);
    await queueHandler(new TestBatch([success]), env);
    expect(success.acked).toBe(true);

    mocks.handleJob.mockRejectedValueOnce(new Error("secret detail"));
    const failure = new TestMessage(job);
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    await queueHandler(new TestBatch([failure]), env);
    expect(mocks.recordFailure.mock.calls[0]?.[0]).toEqual(job);
    expect(mocks.recordFailure.mock.calls[0]?.[2]).toBeInstanceOf(Error);
    expect(failure.retryDelay).toBe(120);
    expect(consoleError.mock.calls[0]?.[0]).not.toContain("secret detail");
    const misrouted = new TestMessage(job);
    await queueHandler(new TestBatch([misrouted], "daily-keiba-sync-neon-jobs"), env);
    expect(misrouted.acked).toBe(true);
    mocks.handleJob.mockRejectedValueOnce(new PermanentJobError("catalog-schema"));
    const permanent = new TestMessage(job);
    await queueHandler(new TestBatch([permanent]), env);
    expect(permanent.acked).toBe(true);
    expect(permanent.retryDelay).toBeUndefined();
    consoleError.mockRestore();
  });
});
