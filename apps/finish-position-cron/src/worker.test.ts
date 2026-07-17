// Run with bun. Tests for the Worker fetch (health + on-demand trigger) +
// scheduled handlers with mocked Container binding and D1.

import { beforeEach, expect, test, vi } from "vitest";

const {
  startMock,
  getContainerMock,
  warmNeonMock,
  enqueueMock,
  handleQueueMock,
  handleDlqQueueMock,
  coordinatorTickMock,
  claimRescoreRaceMock,
  completeFocusedFullRaceMock,
  runDayBasePrewarmMock,
  resolveCardMaxRaceBangoForKochiMock,
  runCoverageSelfHealMock,
  refreshCornerFeaturesMock,
} = vi.hoisted(() => {
  const start = vi.fn(async () => undefined);
  const warmNeon = vi.fn(async () => undefined);
  const enqueuePredict = vi.fn(async (_p: Record<string, unknown>) => ["jra", "nar", "ban-ei"]);
  const handleQueue = vi.fn(async () => undefined);
  const handleDlqQueue = vi.fn(async () => undefined);
  const runRaceCoordinatorTick = vi.fn(async () => []);
  const claimRescoreRace = vi.fn(async () => ({ proceed: true }));
  const completeFocusedFullRace = vi.fn(async () => undefined);
  const runDayBasePrewarm = vi.fn(async () => undefined);
  const refreshCornerFeatures = vi.fn(async () => undefined);
  const resolveCardMaxRaceBangoForKochi = vi.fn(async (): Promise<number | undefined> => undefined);
  const runCoverageSelfHeal = vi.fn(async () => ({
    alreadyComplete: 0,
    alreadyInFlight: 0,
    candidates: 0,
    enqueued: 0,
    errors: 0,
    escalated: 0,
    scanned: 0,
  }));
  return {
    getContainerMock: vi.fn(() => ({ start })),
    startMock: start,
    warmNeonMock: warmNeon,
    enqueueMock: enqueuePredict,
    handleQueueMock: handleQueue,
    handleDlqQueueMock: handleDlqQueue,
    coordinatorTickMock: runRaceCoordinatorTick,
    claimRescoreRaceMock: claimRescoreRace,
    completeFocusedFullRaceMock: completeFocusedFullRace,
    runDayBasePrewarmMock: runDayBasePrewarm,
    resolveCardMaxRaceBangoForKochiMock: resolveCardMaxRaceBangoForKochi,
    runCoverageSelfHealMock: runCoverageSelfHeal,
    refreshCornerFeaturesMock: refreshCornerFeatures,
  };
});

vi.mock("@cloudflare/containers", () => ({
  Container: class {},
  getContainer: getContainerMock,
}));

vi.mock("./neon-warm", () => ({
  warmNeon: warmNeonMock,
}));

vi.mock("./queue-producer", () => ({ enqueuePredict: enqueueMock }));

vi.mock("./queue-consumer", () => ({ handleQueue: handleQueueMock }));

vi.mock("./dlq-consumer", () => ({
  DLQ_QUEUE_NAME: "finish-position-predict-dlq",
  handleDlqQueue: handleDlqQueueMock,
}));

vi.mock("./race-coordinator", () => ({
  DEFAULT_RESCORE_LEAD_MINUTES: 25,
  runRaceCoordinatorTick: coordinatorTickMock,
  resolveCardMaxRaceBangoForKochi: resolveCardMaxRaceBangoForKochiMock,
}));

vi.mock("./do-state", () => ({
  claimRescoreRace: claimRescoreRaceMock,
  completeFocusedFullRace: completeFocusedFullRaceMock,
}));

vi.mock("./day-base-prewarm", () => ({
  runDayBasePrewarm: runDayBasePrewarmMock,
}));

// shouldRunCornerFeaturesRefreshCron is a pure string comparison against the
// two real cron constants (CORNER_FEATURES_REFRESH_CRON_MORNING/_EVENING,
// corner-features-refresh.ts) -- inlined here as literals rather than
// re-derived, mirroring the shouldRunCoverageSelfHealCron mock below.
vi.mock("./corner-features-refresh", () => ({
  refreshCornerFeatures: refreshCornerFeaturesMock,
  shouldRunCornerFeaturesRefreshCron: (cron: string) =>
    cron === "15 0 * * *" || cron === "0 13 * * *",
}));

// shouldRunCoverageSelfHealCron is a pure string comparison against the real
// cron constant (COVERAGE_SELF_HEAL_CRON, coverage-self-heal.ts) -- inlined
// here as a literal rather than re-derived, mirroring the DLQ_QUEUE_NAME
// literal in the "./dlq-consumer" mock above.
vi.mock("./coverage-self-heal", () => ({
  runCoverageSelfHeal: runCoverageSelfHealMock,
  shouldRunCoverageSelfHealCron: (cron: string) => cron === "7,22,37,52 1-11 * * *",
}));

import workerDefault, { handleFetch, handleScheduled } from "./worker";
import type { Env } from "./types";

const runMock = vi.fn(async () => ({ success: true }));
const bindMock = vi.fn(() => ({ run: runMock }));
const prepareMock = vi.fn(() => ({ bind: bindMock }));
const predictQueueSendMock = vi.fn(async () => undefined);
const containerDoFetchMock = vi.fn(async () => Response.json({ ok: true }));
const containerDoGetMock = vi.fn(() => ({ fetch: containerDoFetchMock }));
const containerDoIdFromNameMock = vi.fn((name: string) => ({ name }));
const realtimeAllMock = vi.fn(async () => ({
  results: [{ keibajo_code: "05", race_bango: "11", source: "jra" }],
}));
const realtimeBindMock = vi.fn(() => ({ all: realtimeAllMock }));
const realtimePrepareMock = vi.fn(() => ({ bind: realtimeBindMock }));

const makeEnv = (): Env => ({
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: { prepare: prepareMock } as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {
    get: containerDoGetMock,
    idFromName: containerDoIdFromNameMock,
  } as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: { send: predictQueueSendMock } as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
  REALTIME_DB: { prepare: realtimePrepareMock } as unknown as D1Database,
  RESCORE_ENABLED: "1",
  TRIGGER_TOKEN: "secret-token",
});

const makeEvent = (cron: string): ScheduledEvent =>
  ({ cron, scheduledTime: Date.parse("2026-06-02T18:00:00.000Z") }) as ScheduledEvent;

const healthRequest = (): Request => new Request("https://cron.example/", { method: "GET" });

const triggerRequest = (token: string | null, body: string): Request =>
  new Request("https://cron.example/run", {
    body,
    headers: token === null ? {} : { authorization: `Bearer ${token}` },
    method: "POST",
  });

beforeEach(() => {
  startMock.mockClear();
  getContainerMock.mockClear();
  prepareMock.mockClear();
  bindMock.mockClear();
  runMock.mockClear();
  warmNeonMock.mockClear();
  enqueueMock.mockClear();
  handleQueueMock.mockClear();
  handleDlqQueueMock.mockClear();
  coordinatorTickMock.mockClear();
  claimRescoreRaceMock.mockClear();
  completeFocusedFullRaceMock.mockClear();
  runDayBasePrewarmMock.mockClear();
  refreshCornerFeaturesMock.mockClear();
  runCoverageSelfHealMock.mockClear();
  predictQueueSendMock.mockClear();
  containerDoFetchMock.mockClear();
  containerDoGetMock.mockClear();
  containerDoIdFromNameMock.mockClear();
  realtimeAllMock.mockClear();
  realtimeBindMock.mockClear();
  realtimePrepareMock.mockClear();
  resolveCardMaxRaceBangoForKochiMock.mockClear();
  enqueueMock.mockResolvedValue(["jra", "nar", "ban-ei"]);
  coordinatorTickMock.mockResolvedValue([]);
  claimRescoreRaceMock.mockResolvedValue({ proceed: true });
  resolveCardMaxRaceBangoForKochiMock.mockResolvedValue(undefined);
});

const internalRescoreRaceRequest = (token: string | null, body: string): Request =>
  new Request("https://cron.example/api/internal/rescore-race", {
    body,
    headers: token === null ? {} : { authorization: `Bearer ${token}` },
    method: "POST",
  });

const adminStopContainersRequest = (token: string | null, body: string): Request =>
  new Request("https://cron.example/api/admin/stop-predict-containers", {
    body,
    headers: token === null ? {} : { authorization: `Bearer ${token}` },
    method: "POST",
  });

const adminCompleteFocusedFullRaceRequest = (token: string | null, body: string): Request =>
  new Request("https://cron.example/api/admin/complete-focused-full-race", {
    body,
    headers: token === null ? {} : { authorization: `Bearer ${token}` },
    method: "POST",
  });

const adminRunFocusedFullRaceRequest = (token: string | null, body: string): Request =>
  new Request("https://cron.example/api/admin/run-focused-full-race", {
    body,
    headers: token === null ? {} : { authorization: `Bearer ${token}` },
    method: "POST",
  });

test("fetch returns a health payload for GET", async () => {
  const response = await workerDefault.fetch(healthRequest(), makeEnv());
  expect(response.status).toBe(200);
  const body = (await response.json()) as { ok: boolean; cron: string };
  expect(body.ok).toBe(true);
  expect(body.cron).toBe("0 18 * * *");
});

test("handleFetch rejects an unauthenticated trigger with 401", async () => {
  const response = await handleFetch(triggerRequest(null, ""), makeEnv());
  expect(response.status).toBe(401);
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleFetch rejects a wrong-token trigger with 401", async () => {
  const response = await handleFetch(triggerRequest("wrong-token", ""), makeEnv());
  expect(response.status).toBe(401);
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleFetch enqueues predict and returns 202 for an authorized explicit RUN_DATE", async () => {
  const response = await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  const body = (await response.json()) as { ok: boolean; runDate: string; queued: string[] };
  expect(body.ok).toBe(true);
  expect(body.runDate).toBe("2026-06-03");
  expect(body.queued).toStrictEqual(["jra", "nar", "ban-ei"]);
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(startMock).not.toHaveBeenCalled();
});

test("handleFetch defaults to mode full when body omits mode", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ mode: "full" }));
});

test("handleFetch passes mode rescore when body specifies mode rescore", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ mode: "rescore", runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ mode: "rescore" }));
});

test("handleFetch defaults to today's JST date when the body omits runDate", async () => {
  const response = await handleFetch(triggerRequest("secret-token", ""), makeEnv());
  expect(response.status).toBe(202);
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(startMock).not.toHaveBeenCalled();
});

test("handleFetch returns 400 for a malformed RUN_DATE", async () => {
  const response = await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "2026-06-03" })),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleScheduled is a no-op for an unmatched cron", async () => {
  await handleScheduled(makeEvent("*/10 * * * *"), makeEnv());
  expect(getContainerMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalled();
});

test("handleScheduled starts the container for the configured cron", async () => {
  await handleScheduled(makeEvent("0 18 * * *"), makeEnv());
  expect(getContainerMock).toHaveBeenCalledTimes(1);
  expect(startMock).toHaveBeenCalledTimes(1);
});

test("handleScheduled writes a started audit row", async () => {
  await handleScheduled(makeEvent("0 18 * * *"), makeEnv());
  expect(prepareMock).toHaveBeenCalledTimes(1);
  expect(runMock).toHaveBeenCalledTimes(1);
});

test("scheduled default handler delegates to handleScheduled", async () => {
  await workerDefault.scheduled(makeEvent("0 18 * * *"), makeEnv());
  expect(startMock).toHaveBeenCalledTimes(1);
});

test("handleScheduled calls warmNeon for the pre-NAR warm cron", async () => {
  await handleScheduled(makeEvent("55 17 * * *"), makeEnv());
  expect(warmNeonMock).toHaveBeenCalledTimes(1);
  expect(warmNeonMock).toHaveBeenCalledWith("postgres://example");
  expect(getContainerMock).not.toHaveBeenCalled();
});

test("handleScheduled calls warmNeon for the pre-JRA warm cron", async () => {
  await handleScheduled(makeEvent("25 0 * * *"), makeEnv());
  expect(warmNeonMock).toHaveBeenCalledTimes(1);
  expect(warmNeonMock).toHaveBeenCalledWith("postgres://example");
  expect(getContainerMock).not.toHaveBeenCalled();
});

test("handleScheduled calls warmNeon for the race-hours warm cron", async () => {
  await handleScheduled(makeEvent("*/30 1-11 * * *"), makeEnv());
  expect(warmNeonMock).toHaveBeenCalledTimes(1);
  expect(warmNeonMock).toHaveBeenCalledWith("postgres://example");
  expect(getContainerMock).not.toHaveBeenCalled();
});

test("handleScheduled does not call warmNeon for the predict cron", async () => {
  await handleScheduled(makeEvent("0 18 * * *"), makeEnv());
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(getContainerMock).toHaveBeenCalledTimes(1);
});

test("handleScheduled enqueues rescore for RESCORE_CRON_RACE_HOURS", async () => {
  await handleScheduled(makeEvent("*/20 1-11 * * *"), makeEnv());
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({ daysAhead: 0, mode: "rescore" }),
  );
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(getContainerMock).not.toHaveBeenCalled();
});

test("handleScheduled rescore enqueue does not start container", async () => {
  await handleScheduled(makeEvent("*/20 1-11 * * *"), makeEnv());
  expect(startMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalled();
});

test("handleScheduled runs the per-race coordinator for the coordinator cron", async () => {
  await handleScheduled(makeEvent("*/10 1-11 * * *"), makeEnv());
  expect(coordinatorTickMock).toHaveBeenCalledTimes(1);
  expect(coordinatorTickMock).toHaveBeenCalledWith(expect.objectContaining({ leadMinutes: 25 }));
});

test("handleScheduled coordinator cron does not start container or warm or enqueue per-category", async () => {
  await handleScheduled(makeEvent("*/10 1-11 * * *"), makeEnv());
  expect(startMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalled();
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleScheduled does not run the coordinator for the rescore cron", async () => {
  await handleScheduled(makeEvent("*/20 1-11 * * *"), makeEnv());
  expect(coordinatorTickMock).not.toHaveBeenCalled();
});

test("handleScheduled runs the coverage self-heal scan for the coverage self-heal cron", async () => {
  await handleScheduled(makeEvent("7,22,37,52 1-11 * * *"), makeEnv());
  expect(runCoverageSelfHealMock).toHaveBeenCalledTimes(1);
  expect(runCoverageSelfHealMock).toHaveBeenCalledWith(
    expect.objectContaining({ now: new Date("2026-06-02T18:00:00.000Z") }),
  );
});

test("handleScheduled coverage self-heal cron does not start container, warm, coordinate, or day-base prewarm", async () => {
  await handleScheduled(makeEvent("7,22,37,52 1-11 * * *"), makeEnv());
  expect(startMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalled();
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(coordinatorTickMock).not.toHaveBeenCalled();
  expect(runDayBasePrewarmMock).not.toHaveBeenCalled();
  expect(refreshCornerFeaturesMock).not.toHaveBeenCalled();
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleScheduled does not run the coverage self-heal scan for the coordinator cron", async () => {
  await handleScheduled(makeEvent("*/10 1-11 * * *"), makeEnv());
  expect(runCoverageSelfHealMock).not.toHaveBeenCalled();
});

test("handleScheduled dispatches the day-base prewarm for the feature-build cron", async () => {
  await handleScheduled(makeEvent("30 0 * * *"), makeEnv());
  expect(runDayBasePrewarmMock).toHaveBeenCalledTimes(1);
  expect(runDayBasePrewarmMock).toHaveBeenCalledWith(
    expect.objectContaining({ daysAhead: 2, runYmd: "20260603" }),
  );
});

test("handleScheduled does not refresh corner features on the feature-build cron", async () => {
  await handleScheduled(makeEvent("30 0 * * *"), makeEnv());
  expect(refreshCornerFeaturesMock).not.toHaveBeenCalled();
  expect(runDayBasePrewarmMock).toHaveBeenCalledTimes(1);
});

test("handleScheduled dispatches corner-features refresh for the morning cron", async () => {
  await handleScheduled(makeEvent("15 0 * * *"), makeEnv());
  expect(refreshCornerFeaturesMock).toHaveBeenCalledTimes(1);
  expect(refreshCornerFeaturesMock).toHaveBeenCalledWith(
    expect.objectContaining({ daysAhead: 2, lookbackDays: 0, runYmd: "20260603" }),
  );
});

test("handleScheduled dispatches corner-features refresh for the evening cron", async () => {
  await handleScheduled(makeEvent("0 13 * * *"), makeEnv());
  expect(refreshCornerFeaturesMock).toHaveBeenCalledTimes(1);
  expect(refreshCornerFeaturesMock).toHaveBeenCalledWith(
    expect.objectContaining({ daysAhead: 2, lookbackDays: 0, runYmd: "20260603" }),
  );
});

test("handleScheduled passes CORNER_FEATURES_LOOKBACK_DAYS through to the corner-features refresh", async () => {
  const env = { ...makeEnv(), CORNER_FEATURES_LOOKBACK_DAYS: "7" };
  await handleScheduled(makeEvent("0 13 * * *"), env);
  expect(refreshCornerFeaturesMock).toHaveBeenCalledWith(
    expect.objectContaining({ lookbackDays: 7 }),
  );
});

test("handleScheduled defaults corner-features lookbackDays to 0 when CORNER_FEATURES_LOOKBACK_DAYS is unset", async () => {
  await handleScheduled(makeEvent("15 0 * * *"), makeEnv());
  expect(refreshCornerFeaturesMock).toHaveBeenCalledWith(
    expect.objectContaining({ lookbackDays: 0 }),
  );
});

test("handleScheduled corner-features refresh cron does not start container, warm, coordinate, or self-heal", async () => {
  await handleScheduled(makeEvent("15 0 * * *"), makeEnv());
  expect(startMock).not.toHaveBeenCalled();
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(coordinatorTickMock).not.toHaveBeenCalled();
  expect(runDayBasePrewarmMock).not.toHaveBeenCalled();
  expect(runCoverageSelfHealMock).not.toHaveBeenCalled();
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleScheduled does not refresh corner features for the coverage self-heal cron", async () => {
  await handleScheduled(makeEvent("7,22,37,52 1-11 * * *"), makeEnv());
  expect(refreshCornerFeaturesMock).not.toHaveBeenCalled();
});

test("handleScheduled feature-build cron does not enqueue a direct full-mode predict", async () => {
  await handleScheduled(makeEvent("30 0 * * *"), makeEnv());
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleScheduled feature-build cron does not start container or warm or coordinate", async () => {
  await handleScheduled(makeEvent("30 0 * * *"), makeEnv());
  expect(startMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalled();
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(coordinatorTickMock).not.toHaveBeenCalled();
});

test("queue default handler delegates to handleQueue for the primary queue", async () => {
  const batch = { messages: [] } as unknown as MessageBatch<import("./types").PredictQueueMessage>;
  await workerDefault.queue(batch, makeEnv());
  expect(handleQueueMock).toHaveBeenCalledTimes(1);
  expect(handleQueueMock).toHaveBeenCalledWith(
    batch,
    expect.objectContaining({ NEON_DATABASE_URL: "postgres://example" }),
  );
  expect(handleDlqQueueMock).not.toHaveBeenCalled();
});

test("queue default handler routes the dead-letter queue name to handleDlqQueue", async () => {
  const batch = {
    messages: [],
    queue: "finish-position-predict-dlq",
  } as unknown as MessageBatch<import("./types").PredictQueueMessage>;
  await workerDefault.queue(batch, makeEnv());
  expect(handleDlqQueueMock).toHaveBeenCalledTimes(1);
  expect(handleDlqQueueMock).toHaveBeenCalledWith(
    batch,
    expect.objectContaining({ NEON_DATABASE_URL: "postgres://example" }),
  );
  expect(handleQueueMock).not.toHaveBeenCalled();
});

test("handleFetch passes category nar when body specifies category nar", async () => {
  enqueueMock.mockResolvedValue(["nar"]);
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ category: "nar", runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ category: "nar" }));
});

test("handleFetch omits category when body does not specify category", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ category: undefined }));
});

test("handleFetch ignores invalid category and calls enqueue without category", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ category: "invalid", runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ category: undefined }));
});

test("handleFetch does not write an audit row when enqueueing", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(prepareMock).not.toHaveBeenCalled();
  expect(enqueueMock).toHaveBeenCalledTimes(1);
});

test("handleFetch forwards keibajoCode and raceBango for a per-race NAR rescore", async () => {
  enqueueMock.mockResolvedValue(["nar"]);
  await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: "45",
        mode: "rescore",
        raceBango: "12",
        runDate: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "nar",
      keibajoCode: "45",
      mode: "rescore",
      raceBango: "12",
    }),
  );
});

test("handleFetch forwards downstream full per-race trigger fields with skipDedup", async () => {
  enqueueMock.mockResolvedValue(["jra"]);
  const response = await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        keibajoCode: "05",
        mode: "full",
        raceBango: "11",
        runDate: "20260628",
        skipDedup: true,
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  const body = (await response.json()) as { ok: boolean; queued: string[]; runDate: string };
  expect(body).toStrictEqual({ ok: true, queued: ["jra"], runDate: "2026-06-28" });
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "jra",
      daysAhead: 2,
      keibajoCode: "05",
      mode: "full",
      raceBango: "11",
      runDate: "2026-06-28",
      runYmd: "20260628",
      skipDedup: true,
    }),
  );
});

test("handleFetch forwards debug flag to downstream queue messages", async () => {
  enqueueMock.mockResolvedValue(["jra"]);
  const response = await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        debug: true,
        keibajoCode: "05",
        mode: "full",
        raceBango: "11",
        runDate: "20260628",
        skipDedup: true,
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ debug: true }));
});

test("handleFetch accepts string debug flags for downstream queue messages", async () => {
  enqueueMock.mockResolvedValue(["jra"]);
  const response = await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({ category: "jra", debug: "1", runDate: "20260628" }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ debug: true }));
});

test("handleFetch trims whitespace from keibajoCode and raceBango", async () => {
  enqueueMock.mockResolvedValue(["nar"]);
  await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: " 45 ",
        mode: "rescore",
        raceBango: " 12 ",
        runDate: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: "45", raceBango: "12" }),
  );
});

test("handleFetch treats a blank keibajoCode as absent", async () => {
  await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({ category: "nar", keibajoCode: "   ", raceBango: "12", runDate: "20260619" }),
    ),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: undefined, raceBango: "12" }),
  );
});

test("handleFetch treats a non-string raceBango as absent", async () => {
  await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({ category: "nar", keibajoCode: "45", raceBango: 12, runDate: "20260619" }),
    ),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: "45", raceBango: undefined }),
  );
});

test("handleFetch passes skipDedup true when body specifies skipDedup true", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603", skipDedup: true })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ skipDedup: true }));
});

test("handleFetch omits skipDedup when body specifies skipDedup as string true", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603", skipDedup: "true" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.not.objectContaining({ skipDedup: expect.anything() }),
  );
});

test("handleFetch omits skipDedup when body does not specify skipDedup", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.not.objectContaining({ skipDedup: expect.anything() }),
  );
});

test("handleFetch passes force true when body specifies force true", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ force: true, runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ force: true }));
});

test("handleFetch omits force when body specifies force as string true", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ force: "true", runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.not.objectContaining({ force: expect.anything() }),
  );
});

test("handleFetch omits force when body does not specify force", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.not.objectContaining({ force: expect.anything() }),
  );
});

test("handleFetch omits per-race fields for the per-category path", async () => {
  await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: undefined, raceBango: undefined }),
  );
});

test("admin stop containers endpoint rejects unauthenticated requests", async () => {
  const response = await handleFetch(
    adminStopContainersRequest(null, JSON.stringify({ names: ["predict-nar-20260702-50-01"] })),
    makeEnv(),
  );
  expect(response.status).toBe(401);
  expect(containerDoFetchMock).not.toHaveBeenCalled();
});

test("admin stop containers endpoint rejects non-predict names", async () => {
  const response = await handleFetch(
    adminStopContainersRequest("secret-token", JSON.stringify({ names: ["daily-predict"] })),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(containerDoFetchMock).not.toHaveBeenCalled();
});

test("admin stop containers endpoint rejects missing and empty names", async () => {
  const missingResponse = await handleFetch(
    adminStopContainersRequest("secret-token", JSON.stringify({})),
    makeEnv(),
  );
  const emptyResponse = await handleFetch(
    adminStopContainersRequest("secret-token", JSON.stringify({ names: [] })),
    makeEnv(),
  );
  expect(missingResponse.status).toBe(400);
  expect(emptyResponse.status).toBe(400);
  expect(containerDoFetchMock).not.toHaveBeenCalled();
});

test("admin stop containers endpoint returns 400 for malformed JSON", async () => {
  const response = await handleFetch(adminStopContainersRequest("secret-token", "{"), makeEnv());
  expect(response.status).toBe(400);
  const body = (await response.json()) as { ok: boolean; error: string };
  expect(body.ok).toBe(false);
  expect(body.error).toContain("SyntaxError");
  expect(containerDoFetchMock).not.toHaveBeenCalled();
});

test("admin stop containers endpoint destroys requested predict DO containers", async () => {
  const response = await handleFetch(
    adminStopContainersRequest(
      "secret-token",
      JSON.stringify({ names: ["predict-nar-20260702-50-01", "predict-nar-20260702-50-02"] }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(200);
  expect(containerDoIdFromNameMock).toHaveBeenNthCalledWith(1, "predict-nar-20260702-50-01");
  expect(containerDoIdFromNameMock).toHaveBeenNthCalledWith(2, "predict-nar-20260702-50-02");
  expect(containerDoFetchMock).toHaveBeenCalledTimes(2);
  const request = (containerDoFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(request.method).toBe("POST");
  expect(request.url).toBe("http://do/__admin/stop-container");
  expect(request.headers.get("authorization")).toBe("Bearer secret-token");
  const body = (await response.json()) as {
    ok: boolean;
    results: Array<{ name: string; ok: boolean; status: number }>;
  };
  expect(body).toStrictEqual({
    ok: true,
    results: [
      { name: "predict-nar-20260702-50-01", ok: true, status: 200 },
      { name: "predict-nar-20260702-50-02", ok: true, status: 200 },
    ],
  });
});

test("admin complete focused full race endpoint rejects unauthenticated requests", async () => {
  const response = await handleFetch(
    adminCompleteFocusedFullRaceRequest(
      null,
      JSON.stringify({
        category: "jra",
        keibajoCode: "02",
        raceBango: "01",
        runYmd: "20260621",
        status: "error",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(401);
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
});

test("admin complete focused full race endpoint rejects invalid requests", async () => {
  const missingStatusResponse = await handleFetch(
    adminCompleteFocusedFullRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        keibajoCode: "02",
        raceBango: "01",
        runYmd: "20260621",
      }),
    ),
    makeEnv(),
  );
  const invalidCategoryResponse = await handleFetch(
    adminCompleteFocusedFullRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "overseas",
        keibajoCode: "02",
        raceBango: "01",
        runYmd: "20260621",
        status: "error",
      }),
    ),
    makeEnv(),
  );
  expect(missingStatusResponse.status).toBe(400);
  expect(invalidCategoryResponse.status).toBe(400);
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
});

test("admin complete focused full race endpoint writes terminal focused state", async () => {
  const response = await handleFetch(
    adminCompleteFocusedFullRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        keibajoCode: "02",
        raceBango: "01",
        runYmd: "20260621",
        status: "error",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(200);
  expect(completeFocusedFullRaceMock).toHaveBeenCalledTimes(1);
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.any(Object),
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260621",
    status: "error",
  });
  const body = (await response.json()) as { ok: boolean };
  expect(body.ok).toBe(true);
});

test("admin run focused full race endpoint rejects unauthenticated requests", async () => {
  const response = await handleFetch(
    adminRunFocusedFullRaceRequest(
      null,
      JSON.stringify({
        category: "jra",
        keibajoCode: "10",
        raceBango: "07",
        runYmd: "20260705",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(401);
  expect(containerDoFetchMock).not.toHaveBeenCalled();
});

test("admin run focused full race endpoint rejects invalid requests", async () => {
  const response = await handleFetch(
    adminRunFocusedFullRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        keibajoCode: "10",
        raceBango: "",
        runYmd: "20260705",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(containerDoFetchMock).not.toHaveBeenCalled();
});

test("admin run focused full race endpoint proxies a held predict request", async () => {
  containerDoFetchMock.mockResolvedValueOnce(
    new Response('{"type":"result","status":"success","racesPredicted":1}\\n', {
      headers: { "Content-Type": "application/x-ndjson" },
      status: 200,
    }),
  );
  const response = await handleFetch(
    adminRunFocusedFullRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        keibajoCode: "10",
        raceBango: "07",
        runYmd: "20260705",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(200);
  expect(containerDoIdFromNameMock).toHaveBeenCalledWith("predict-jra");
  const request = (containerDoFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(request.url).toBe(
    "http://do/predict?category=jra&daysAhead=0&keibajoCode=10&mode=full&raceBango=07&runDate=20260705",
  );
  expect(await response.text()).toBe('{"type":"result","status":"success","racesPredicted":1}\\n');
});

test("admin run focused full race endpoint threads cardMaxRaceBango for a Kochi race", async () => {
  resolveCardMaxRaceBangoForKochiMock.mockResolvedValueOnce(10);
  containerDoFetchMock.mockResolvedValueOnce(
    new Response('{"type":"result","status":"success","racesPredicted":1}\\n', {
      headers: { "Content-Type": "application/x-ndjson" },
      status: 200,
    }),
  );
  const response = await handleFetch(
    adminRunFocusedFullRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: "54",
        raceBango: "10",
        runYmd: "20260712",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(200);
  expect(resolveCardMaxRaceBangoForKochiMock).toHaveBeenCalledWith({
    env: expect.anything(),
    keibajoCode: "54",
    runYmd: "20260712",
  });
  const request = (containerDoFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(request.url).toBe(
    "http://do/predict?category=nar&daysAhead=0&keibajoCode=54&mode=full&raceBango=10&runDate=20260712&cardMaxRaceBango=10",
  );
});

test("admin run focused full race endpoint forwards debug to the held predict request", async () => {
  containerDoFetchMock.mockResolvedValueOnce(
    new Response('{"type":"result","status":"success","racesPredicted":1}\\n', {
      headers: { "Content-Type": "application/x-ndjson" },
      status: 200,
    }),
  );
  const response = await handleFetch(
    adminRunFocusedFullRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        debug: true,
        keibajoCode: "10",
        raceBango: "07",
        runYmd: "20260705",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(200);
  const request = (containerDoFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(request.url).toBe(
    "http://do/predict?category=jra&daysAhead=0&keibajoCode=10&mode=full&raceBango=07&runDate=20260705&debug=1",
  );
});

test("internal rescore-race endpoint claims, enqueues a per-race rescore message, and returns 202", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: "45",
        raceBango: "12",
        runYmd: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  expect(claimRescoreRaceMock).toHaveBeenCalledTimes(1);
  expect(claimRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "nar",
      keibajoCode: "45",
      raceBango: "12",
      runYmd: "20260619",
    }),
  );
  expect(predictQueueSendMock).toHaveBeenCalledTimes(1);
  expect(predictQueueSendMock).toHaveBeenCalledWith({
    category: "nar",
    daysAhead: 0,
    keibajoCode: "45",
    mode: "rescore",
    raceBango: "12",
    runDate: "2026-06-19",
    runDateIso: "2026-06-19",
    runYmd: "20260619",
  });
});

test("internal rescore-race endpoint forwards debug to per-race queue messages", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        debug: "1",
        keibajoCode: "45",
        raceBango: "12",
        runYmd: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  expect(predictQueueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "nar",
      debug: true,
      keibajoCode: "45",
      raceBango: "12",
    }),
  );
});

test("internal rescore-race endpoint response body marks claimed true when proceed", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        keibajoCode: "05",
        raceBango: "11",
        runYmd: "20260620",
      }),
    ),
    makeEnv(),
  );
  const body = (await response.json()) as { ok: boolean; claimed: boolean };
  expect(body.ok).toBe(true);
  expect(body.claimed).toBe(true);
});

test("internal rescore-race endpoint returns 200 with claimed=false on claim collision", async () => {
  claimRescoreRaceMock.mockResolvedValueOnce({ proceed: false });
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        keibajoCode: "05",
        raceBango: "11",
        runYmd: "20260620",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(200);
  const body = (await response.json()) as { ok: boolean; claimed: boolean };
  expect(body.ok).toBe(true);
  expect(body.claimed).toBe(false);
  expect(predictQueueSendMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 401 when authorization header is missing", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      null,
      JSON.stringify({
        category: "nar",
        keibajoCode: "45",
        raceBango: "12",
        runYmd: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(401);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
  expect(predictQueueSendMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 401 when bearer token mismatches", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "wrong-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: "45",
        raceBango: "12",
        runYmd: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(401);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint is a no-op when rescore is disabled", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: "45",
        raceBango: "12",
        runYmd: "20260619",
      }),
    ),
    { ...makeEnv(), RESCORE_ENABLED: "0" },
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toStrictEqual({
    claimed: false,
    ok: true,
    rescoreEnabled: false,
  });
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
  expect(predictQueueSendMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 400 when category is missing", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({ keibajoCode: "45", raceBango: "12", runYmd: "20260619" }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 400 when category is invalid", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "garbage",
        keibajoCode: "45",
        raceBango: "12",
        runYmd: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 400 when keibajoCode is blank", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: "   ",
        raceBango: "12",
        runYmd: "20260619",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 400 when raceBango is missing", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({ category: "nar", keibajoCode: "45", runYmd: "20260619" }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 400 when runYmd is malformed", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "nar",
        keibajoCode: "45",
        raceBango: "12",
        runYmd: "2026-06-19",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint returns 400 when body is not parseable JSON", async () => {
  const response = await handleFetch(
    internalRescoreRaceRequest("secret-token", "{not-json"),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint trims whitespace from keibajoCode and raceBango", async () => {
  await handleFetch(
    internalRescoreRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "ban-ei",
        keibajoCode: "  83  ",
        raceBango: "  11  ",
        runYmd: "20260620",
      }),
    ),
    makeEnv(),
  );
  expect(claimRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: "83", raceBango: "11" }),
  );
  expect(predictQueueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ category: "ban-ei", keibajoCode: "83", raceBango: "11" }),
  );
});

test("non-trigger non-rescore request falls through to health response", async () => {
  const response = await handleFetch(
    new Request("https://cron.example/api/other", { method: "POST" }),
    makeEnv(),
  );
  expect(response.status).toBe(200);
  const body = (await response.json()) as { ok: boolean };
  expect(body.ok).toBe(true);
  expect(claimRescoreRaceMock).not.toHaveBeenCalled();
});
