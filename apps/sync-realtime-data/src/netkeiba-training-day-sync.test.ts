import { Miniflare } from "miniflare";
import { afterAll, beforeAll, beforeEach, expect, it, vi } from "vitest";
import type { Env, Job } from "./types";

const mocks = vi.hoisted(() => ({
  buildPremiumUrl: vi.fn(),
  bust: vi.fn(),
  fetchPremiumHtml: vi.fn(),
  getPremiumRaceConfig: vi.fn(),
  parseWorkouts: vi.fn(),
}));

vi.mock("./viewer-race-cache-bust", () => ({ triggerRaceCacheBust: mocks.bust }));
vi.mock("./premium-race", () => ({
  buildPremiumUrl: mocks.buildPremiumUrl,
  fetchPremiumHtml: mocks.fetchPremiumHtml,
  getPremiumRaceConfig: mocks.getPremiumRaceConfig,
  mergeNetkeibaTrainingWorkouts: vi.fn((groups: unknown[][]) => groups[0]),
  parseNetkeibaTrainingWorkouts: mocks.parseWorkouts,
}));

import {
  finalizeNetkeibaTrainingDay,
  netkeibaTrainingDaySyncInternals,
  syncNetkeibaTrainingDay,
} from "./netkeiba-training-day-sync";

class RecordingQueue implements Queue<Job> {
  messages: Job[] = [];
  async send(message: Job): Promise<QueueSendResponse> {
    this.messages.push(message);
    return { metadata: { metrics: { backlogBytes: 0, backlogCount: this.messages.length } } };
  }
  async sendBatch(): Promise<QueueSendBatchResponse> {
    return { metadata: { metrics: { backlogBytes: 0, backlogCount: this.messages.length } } };
  }
  async metrics(): Promise<QueueMetrics> {
    return { backlogBytes: 0, backlogCount: this.messages.length };
  }
}

const workout = {
  commentText: "好調",
  course: "美浦W",
  courseDirection: "右",
  evaluationGrade: "A",
  evaluationText: "伸びる",
  horseName: "テスト馬",
  horseNumber: "1",
  lapTime10f: null,
  lapTime1f: "123",
  lapTime2f: "245",
  lapTime3f: null,
  lapTime4f: null,
  lapTime5f: null,
  lapTime6f: null,
  lapTime7f: null,
  lapTime8f: null,
  lapTime9f: null,
  riderName: "助手",
  timeGokei10f: null,
  timeGokei2f: "245",
  timeGokei3f: null,
  timeGokei4f: null,
  timeGokei5f: null,
  timeGokei6f: null,
  timeGokei7f: null,
  timeGokei8f: null,
  timeGokei9f: null,
  tracenKubun: "1",
  trainingDate: "20260903",
  trainingTime: "0610",
  trainingType: "ウッド",
  workoutIndex: 1,
};

let mf: Miniflare;
let db: D1Database;
let queue: RecordingQueue;
let dailyStatus = "catalog_succeeded";
let stagedBody: Record<string, unknown> | null;
let viewerRequests: string[];
let env: Env;

const dailyBinding = (): NonNullable<Env["DAILY_KEIBA_SYNC"]> => {
  const binding = env.DAILY_KEIBA_SYNC;
  if (binding === undefined) throw new Error("test Daily Sync binding is missing");
  return binding;
};

beforeAll(async () => {
  mf = new Miniflare({
    compatibilityDate: "2026-06-18",
    d1Databases: { DB: "netkeiba-training-test" },
    modules: true,
    script: "export default {}",
  });
  const bindings = await mf.getBindings<{ DB: D1Database }>();
  db = bindings.DB;
  await db
    .prepare(
      "create table premium_race_links (race_key text primary key, source_race_id text not null)",
    )
    .run();
  await db
    .prepare(
      `create table netkeiba_training_day_sync_state (
        race_date text primary key,
        status text not null,
        catalog_run_id text,
        workout_count integer not null default 0,
        error_message text,
        attempted_at text not null,
        completed_at text
      )`,
    )
    .run();
});

beforeEach(async () => {
  await db.prepare("delete from premium_race_links").run();
  await db.prepare("delete from netkeiba_training_day_sync_state").run();
  await db
    .prepare("insert into premium_race_links (race_key, source_race_id) values (?, ?)")
    .bind("jra:2026:0905:01:01", "202601010101")
    .run();
  queue = new RecordingQueue();
  stagedBody = null;
  viewerRequests = [];
  dailyStatus = "catalog_succeeded";
  mocks.buildPremiumUrl
    .mockReset()
    .mockReturnValue("https://race.netkeiba.com/race/oikiri.html?race_id=202601010101");
  mocks.bust.mockReset().mockResolvedValue({ attempts: 1, status: "ok" });
  mocks.fetchPremiumHtml.mockReset().mockResolvedValue("html");
  mocks.getPremiumRaceConfig.mockReset().mockReturnValue({
    origin: "https://race.netkeiba.com",
    workPathTemplate: "/race/oikiri.html?race_id={sourceRaceId}",
  });
  mocks.parseWorkouts.mockReset().mockReturnValue([workout]);
  env = {
    DAILY_KEIBA_SYNC: {
      fetch: vi.fn(async (request: Request) => {
        const url = new URL(request.url);
        if (url.pathname === "/internal/stage-netkeiba-training") {
          stagedBody = (await request.json()) as Record<string, unknown>;
          return Response.json(
            { records: 1, runId: "12345678-1234-1234-1234-123456789abc" },
            { status: 202 },
          );
        }
        return Response.json({ error_stage: null, status: dailyStatus });
      }),
    },
    PC_KEIBA_R2_CATALOG: {
      fetch: vi.fn(async (request: Request) => {
        const url = new URL(request.url);
        if (url.pathname === "/admin/purge") return Response.json({ ok: true, purged: 1 });
        return Response.json({
          entries: [
            {
              keibajoCode: "01",
              kettoTorokuBango: "2023100001",
              raceBango: "01",
              source: "jra",
              umaban: 1,
            },
          ],
        });
      }),
    },
    PC_KEIBA_VIEWER: {
      fetch: vi.fn(async (request: Request) => {
        viewerRequests.push(request.url);
        return request.url.includes("premium-data-top")
          ? Response.json({ dataTopHorses: [{ horseNumber: "1" }] })
          : Response.json({
              trainings: [{ chokyoNengappi: "20260903", lapTime1f: "123", umaban: "01" }],
            });
      }),
    },
    R2_CATALOG_INGESTION_TOKEN: "catalog-token",
    REALTIME_ADMIN_TOKEN: "admin-token",
    REALTIME_DB: db,
    REALTIME_JOBS: queue,
  } as unknown as Env;
});

afterAll(async () => {
  await mf.dispose();
});

it("strictly validates Catalog entry envelopes and race keys", () => {
  const { parseCatalogEntries, raceParts } = netkeibaTrainingDaySyncInternals;
  expect(() => parseCatalogEntries(null)).toThrow("entry response");
  expect(() => parseCatalogEntries({ entries: null })).toThrow("entry response");
  expect(() => parseCatalogEntries({ entries: [null] })).toThrow("entry row");
  expect(() => parseCatalogEntries({ entries: [{ source: "nar", umaban: 1 }] })).toThrow(
    "entry row",
  );
  expect(() => parseCatalogEntries({ entries: [{ source: "jra", umaban: 1.5 }] })).toThrow(
    "entry row",
  );
  expect(() =>
    parseCatalogEntries({
      entries: [
        {
          keibajoCode: 1,
          kettoTorokuBango: "2023100001",
          raceBango: "01",
          source: "jra",
          umaban: 1,
        },
      ],
    }),
  ).toThrow("keibajoCode");
  expect(() =>
    parseCatalogEntries({
      entries: [
        {
          keibajoCode: "",
          kettoTorokuBango: "2023100001",
          raceBango: "01",
          source: "jra",
          umaban: 1,
        },
      ],
    }),
  ).toThrow("keibajoCode");
  expect(raceParts("jra:2026:0905:01:01")).toStrictEqual({
    date: "20260905",
    keibajoCode: "01",
    raceBango: "01",
  });
  expect(() => raceParts("invalid")).toThrow("race key");
});

it("recognizes only non-empty Viewer workout and data-top payloads", () => {
  const { hasDataTop, hasWorkoutData } = netkeibaTrainingDaySyncInternals;
  expect(hasWorkoutData(null)).toBe(false);
  expect(hasWorkoutData({ trainings: null })).toBe(false);
  expect(hasWorkoutData({ trainings: [null, { chokyoNengappi: 1 }] })).toBe(false);
  expect(
    hasWorkoutData({
      trainings: [{ chokyoNengappi: "2026/09/03", lapTime1f: "123", umaban: "01" }],
    }),
  ).toBe(false);
  expect(
    hasWorkoutData({
      trainings: [{ chokyoNengappi: "20260903", lapTime1f: "123", umaban: "01" }],
    }),
  ).toBe(true);
  expect(hasWorkoutData({ trainings: [{ chokyoNengappi: "20260903", umaban: "01" }] })).toBe(false);
  expect(
    hasWorkoutData({
      trainings: [{ chokyoNengappi: "20260903", lapTime1f: "0000", umaban: "01" }],
    }),
  ).toBe(false);
  expect(
    hasWorkoutData({
      trainings: [
        { chokyoNengappi: "20260903", lapTime1f: "123", umaban: "01" },
        { chokyoNengappi: "", lapTime1f: "123", umaban: "02" },
      ],
    }),
  ).toBe(false);
  expect(hasWorkoutData({ trainings: [{ chokyoNengappi: "20260903", umaban: "" }] })).toBe(false);
  expect(hasDataTop(null)).toBe(false);
  expect(hasDataTop({ dataTopHorses: null })).toBe(false);
  expect(hasDataTop({ dataTopHorses: [] })).toBe(false);
  expect(hasDataTop({ dataTopHorses: [{}] })).toBe(true);
});

it("stages one day through daily-keiba-sync after reading entrants from R2 Catalog", async () => {
  expect(await syncNetkeibaTrainingDay(env, "20260905")).toBe(1);
  expect(stagedBody?.tableName).toBe("netkeiba_training_workouts");
  const records = stagedBody?.records as Array<Record<string, unknown>>;
  expect(records[0]?.ketto_toroku_bango).toBe("2023100001");
  expect(records[0]?.workout_index).toBe(1);
  expect(queue.messages).toStrictEqual([
    {
      catalogRunId: "12345678-1234-1234-1234-123456789abc",
      date: "20260905",
      type: "finalize-netkeiba-training-day",
    },
  ]);
  expect(await syncNetkeibaTrainingDay(env, "20260905")).toBe(0);
});

it("fails closed for missing Catalog inputs and empty or unmatched workouts", async () => {
  const catalog = env.PC_KEIBA_R2_CATALOG;
  const originalToken = env.R2_CATALOG_INGESTION_TOKEN;
  env.R2_CATALOG_INGESTION_TOKEN = undefined;
  await expect(syncNetkeibaTrainingDay(env, "20260905")).rejects.toThrow("token is missing");
  env.R2_CATALOG_INGESTION_TOKEN = originalToken;

  catalog.fetch = vi.fn(async () => new Response("error", { status: 503 }));
  await expect(syncNetkeibaTrainingDay(env, "20260905")).rejects.toThrow("HTTP 503");
  catalog.fetch = vi.fn(async () => Response.json({ entries: [] }));
  await expect(syncNetkeibaTrainingDay(env, "20260905")).rejects.toThrow("source rows are missing");

  catalog.fetch = vi.fn(async () =>
    Response.json({
      entries: [
        {
          keibajoCode: "01",
          kettoTorokuBango: "2023100001",
          raceBango: "01",
          source: "jra",
          umaban: 1,
        },
      ],
    }),
  );
  mocks.parseWorkouts.mockReturnValue([]);
  await expect(syncNetkeibaTrainingDay(env, "20260905")).rejects.toThrow("returned no workouts");
  mocks.parseWorkouts.mockReturnValue([workout, { ...workout, horseNumber: "2" }]);
  await expect(syncNetkeibaTrainingDay(env, "20260905")).rejects.toThrow("did not match");
});

it("fails closed when premium workout configuration cannot produce a URL", async () => {
  mocks.getPremiumRaceConfig.mockReturnValue({ origin: "", workPathTemplate: undefined });
  await expect(syncNetkeibaTrainingDay(env, "20260905")).rejects.toThrow("not configured");
  mocks.getPremiumRaceConfig.mockReturnValue({
    origin: "https://race.netkeiba.com",
    workPathTemplate: undefined,
  });
  mocks.buildPremiumUrl.mockReturnValue(null);
  await expect(syncNetkeibaTrainingDay(env, "20260905")).rejects.toThrow("URL is unavailable");
});

it("records non-Error ingestion failures without losing the original rejection", async () => {
  env.PC_KEIBA_R2_CATALOG.fetch = vi.fn(async () => Promise.reject("catalog rejected"));
  await expect(syncNetkeibaTrainingDay(env, "20260905")).rejects.toBe("catalog rejected");
});

it("fails closed when Daily Sync staging is missing or rejects the day", async () => {
  const daily = dailyBinding();
  env.DAILY_KEIBA_SYNC = undefined;
  await expect(syncNetkeibaTrainingDay(env, "20260905")).rejects.toThrow(
    "staging is not configured",
  );
  env.DAILY_KEIBA_SYNC = daily;
  daily.fetch = vi.fn(async () => new Response("error", { status: 502 }));
  await expect(syncNetkeibaTrainingDay(env, "20260905")).rejects.toThrow("HTTP 502");
  daily.fetch = vi.fn(async () => Response.json([]));
  await expect(syncNetkeibaTrainingDay(env, "20260905")).rejects.toThrow("staging response");
});

it("keeps finalization queued until the Catalog-first run succeeds", async () => {
  dailyStatus = "catalog_succeeded";
  expect(
    await finalizeNetkeibaTrainingDay(env, "20260905", "12345678-1234-1234-1234-123456789abc"),
  ).toBe("catalog_succeeded");
  expect(queue.messages).toStrictEqual([
    {
      catalogRunId: "12345678-1234-1234-1234-123456789abc",
      date: "20260905",
      type: "finalize-netkeiba-training-day",
    },
  ]);
});

it("fails finalization when the Catalog-first run reports an error", async () => {
  dailyStatus = "failed";
  await expect(
    finalizeNetkeibaTrainingDay(env, "20260905", "12345678-1234-1234-1234-123456789abc"),
  ).rejects.toThrow("training run failed");

  dailyStatus = "running";
  dailyBinding().fetch = vi.fn(async () =>
    Response.json({ error_stage: "catalog-transaction", status: dailyStatus }),
  );
  await expect(
    finalizeNetkeibaTrainingDay(env, "20260905", "12345678-1234-1234-1234-123456789abc"),
  ).rejects.toThrow("training run failed");
});

it("rejects malformed Daily Sync status responses", async () => {
  dailyBinding().fetch = vi.fn(async () => Response.json([]));
  await expect(
    finalizeNetkeibaTrainingDay(env, "20260905", "12345678-1234-1234-1234-123456789abc"),
  ).rejects.toThrow("status response");
  dailyBinding().fetch = vi.fn(async () => new Response("error", { status: 503 }));
  await expect(
    finalizeNetkeibaTrainingDay(env, "20260905", "12345678-1234-1234-1234-123456789abc"),
  ).rejects.toThrow("HTTP 503");
});

it("reclaims a stale processing lease", async () => {
  await db
    .prepare(
      `insert into netkeiba_training_day_sync_state
       (race_date, status, workout_count, attempted_at)
       values (?, 'processing', 0, ?)`,
    )
    .bind("20260905", "2026-09-01T00:00:00.000Z")
    .run();
  expect(await syncNetkeibaTrainingDay(env, "20260905")).toBe(1);
});

it("fails finalization before warm when Catalog purge fails", async () => {
  dailyStatus = "succeeded";
  env.PC_KEIBA_R2_CATALOG.fetch = vi.fn(async (request: Request) =>
    new URL(request.url).pathname === "/admin/purge"
      ? new Response("error", { status: 503 })
      : Response.json({ entries: [] }),
  );
  await expect(
    finalizeNetkeibaTrainingDay(env, "20260905", "12345678-1234-1234-1234-123456789abc"),
  ).rejects.toThrow("R2 Catalog purge failed");
});

it("fails finalization when Viewer binding is missing", async () => {
  dailyStatus = "succeeded";
  env.PC_KEIBA_VIEWER = undefined;
  await expect(
    finalizeNetkeibaTrainingDay(env, "20260905", "12345678-1234-1234-1234-123456789abc"),
  ).rejects.toThrow("Viewer service binding is missing");
});

it("fails finalization when Viewer cache bust does not succeed", async () => {
  dailyStatus = "succeeded";
  mocks.bust.mockResolvedValue({ attempts: 1, status: "failed" });
  await expect(
    finalizeNetkeibaTrainingDay(env, "20260905", "12345678-1234-1234-1234-123456789abc"),
  ).rejects.toThrow("Viewer cache bust failed");
});

it("purges Catalog and Viewer caches and warms only non-empty sections after success", async () => {
  dailyStatus = "succeeded";
  expect(
    await finalizeNetkeibaTrainingDay(env, "20260905", "12345678-1234-1234-1234-123456789abc"),
  ).toBe("succeeded");
  expect(mocks.bust).toHaveBeenCalledTimes(1);
  expect(env.PC_KEIBA_VIEWER?.fetch).toHaveBeenCalledTimes(2);
  expect(viewerRequests).toStrictEqual([
    "https://pc-keiba-viewer.internal/api/races/2026/09/05/01/01/sections/training?__cacheWarm=1",
    "https://pc-keiba-viewer.internal/api/races/2026/09/05/01/01/sections/premium-data-top?__cacheWarm=1",
  ]);
  const state = await db
    .prepare("select status from netkeiba_training_day_sync_state where race_date = ?")
    .bind("20260905")
    .first<{ status: string }>();
  expect(state?.status).toBe("succeeded");
});

it("fails closed when Viewer warm returns an empty data section", async () => {
  dailyStatus = "succeeded";
  env.PC_KEIBA_VIEWER = { fetch: vi.fn(async () => Response.json({ trainings: [] })) };
  await expect(
    finalizeNetkeibaTrainingDay(env, "20260905", "12345678-1234-1234-1234-123456789abc"),
  ).rejects.toThrow("incomplete data");
  const state = await db
    .prepare(
      "select status, error_message from netkeiba_training_day_sync_state where race_date = ?",
    )
    .bind("20260905")
    .first<{ error_message: string | null; status: string }>();
  expect(state).toStrictEqual({
    error_message: "Viewer cache warm returned incomplete data for jra:2026:0905:01:01",
    status: "failed",
  });
});
