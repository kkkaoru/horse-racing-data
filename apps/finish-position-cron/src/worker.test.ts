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
  checkContainerSlotStopMock,
  claimContainerSlotMock,
  claimRescoreRaceMock,
  clearContainerSlotMock,
  completeFocusedFullRaceMock,
  releaseContainerSlotMock,
  runDayBasePrewarmMock,
  enqueueDayBasePrewarmMock,
  prewarmCategoryMock,
  resolveCardMaxRaceBangoForKochiMock,
  runCoverageSelfHealMock,
  refreshCornerFeaturesMock,
  runRunningStyleKickMorningGapMock,
  runRunningStyleKickTomorrowPrewarmMock,
  enqueueDeliveryCanaryMock,
  listDeliveryCanariesMock,
  getPredictionReadinessMock,
  pickUpPrewarmDayBaseMock,
  headDayBaseObjectMock,
  fanOutPredictionsAfterDayBaseHitMock,
  getFocusedFullDayBaseReadinessMock,
  materializeDayBasePerRaceCacheMock,
} = vi.hoisted(() => {
  const start = vi.fn(async () => undefined);
  const warmNeon = vi.fn(async () => undefined);
  const enqueuePredict = vi.fn(async (_p: Record<string, unknown>) => ["jra", "nar", "ban-ei"]);
  const handleQueue = vi.fn(async () => undefined);
  const handleDlqQueue = vi.fn(async () => undefined);
  const runRaceCoordinatorTick = vi.fn(async () => []);
  const claimContainerSlot = vi.fn(
    async (): Promise<{ proceed: boolean; state?: string }> => ({ proceed: true }),
  );
  const claimRescoreRace = vi.fn(async () => ({ proceed: true }));
  const checkContainerSlotStop = vi.fn(async () => true);
  const completeFocusedFullRace = vi.fn(async () => undefined);
  const clearContainerSlot = vi.fn(async () => undefined);
  const releaseContainerSlot = vi.fn(async () => undefined);
  const runDayBasePrewarm = vi.fn(async () => true);
  const enqueueDayBasePrewarm = vi.fn(async () => undefined);
  const prewarmCategory = vi.fn(async () => true);
  const refreshCornerFeatures = vi.fn(async () => undefined);
  const runRunningStyleKickMorningGap = vi.fn(async () => undefined);
  const runRunningStyleKickTomorrowPrewarm = vi.fn(async () => undefined);
  const enqueueDeliveryCanary = vi.fn(async () => ({
    enqueuedAt: "2026-08-15T00:00:00Z",
    id: "canary-id",
    type: "delivery-canary" as const,
  }));
  const listDeliveryCanaries = vi.fn(
    async (): Promise<
      Array<{
        consumedAt: string | null;
        deliveryLagMs: number | null;
        enqueuedAt: string;
        id: string;
      }>
    > => [],
  );
  const getPredictionReadiness = vi.fn(async () => ({
    checkedAt: "2026-08-15T00:00:00Z",
    races: [],
    runYmd: "20260815",
  }));
  const pickUpPrewarmDayBase = vi.fn(async () => false);
  const headDayBaseObject = vi.fn(async (): Promise<unknown> => null);
  const fanOutPredictionsAfterDayBaseHit = vi.fn(async () => 0);
  const getFocusedFullDayBaseReadiness = vi.fn(async () => ({ ready: true, reason: "ready" }));
  const materializeDayBasePerRaceCache = vi.fn(
    async (): Promise<
      | {
          featureHash: string;
          manifestKey: string;
          raceCount: number;
          rowCount: number;
          status: "materialized";
        }
      | { reason: string; status: "fallback" }
    > => ({
      featureHash: "feature-hash",
      manifestKey: "manifest.json",
      raceCount: 36,
      rowCount: 466,
      status: "materialized",
    }),
  );
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
    claimContainerSlotMock: claimContainerSlot,
    claimRescoreRaceMock: claimRescoreRace,
    checkContainerSlotStopMock: checkContainerSlotStop,
    clearContainerSlotMock: clearContainerSlot,
    completeFocusedFullRaceMock: completeFocusedFullRace,
    releaseContainerSlotMock: releaseContainerSlot,
    runDayBasePrewarmMock: runDayBasePrewarm,
    enqueueDayBasePrewarmMock: enqueueDayBasePrewarm,
    prewarmCategoryMock: prewarmCategory,
    resolveCardMaxRaceBangoForKochiMock: resolveCardMaxRaceBangoForKochi,
    runCoverageSelfHealMock: runCoverageSelfHeal,
    refreshCornerFeaturesMock: refreshCornerFeatures,
    runRunningStyleKickMorningGapMock: runRunningStyleKickMorningGap,
    runRunningStyleKickTomorrowPrewarmMock: runRunningStyleKickTomorrowPrewarm,
    enqueueDeliveryCanaryMock: enqueueDeliveryCanary,
    listDeliveryCanariesMock: listDeliveryCanaries,
    getPredictionReadinessMock: getPredictionReadiness,
    pickUpPrewarmDayBaseMock: pickUpPrewarmDayBase,
    headDayBaseObjectMock: headDayBaseObject,
    fanOutPredictionsAfterDayBaseHitMock: fanOutPredictionsAfterDayBaseHit,
    getFocusedFullDayBaseReadinessMock: getFocusedFullDayBaseReadiness,
    materializeDayBasePerRaceCacheMock: materializeDayBasePerRaceCache,
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

vi.mock("./delivery-canary", () => ({
  enqueueDeliveryCanary: enqueueDeliveryCanaryMock,
  listDeliveryCanaries: listDeliveryCanariesMock,
  shouldRunDeliveryCanaryCron: (cron: string) => cron === "*/5 0-13 * * *",
}));

vi.mock("./prediction-readiness", () => ({
  getPredictionReadiness: getPredictionReadinessMock,
}));

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
  claimContainerSlot: claimContainerSlotMock,
  claimRescoreRace: claimRescoreRaceMock,
  checkContainerSlotStop: checkContainerSlotStopMock,
  clearContainerSlot: clearContainerSlotMock,
  completeFocusedFullRace: completeFocusedFullRaceMock,
  releaseContainerSlot: releaseContainerSlotMock,
}));

vi.mock("./day-base-prewarm", () => ({
  enqueueDayBasePrewarm: enqueueDayBasePrewarmMock,
  prewarmCategory: prewarmCategoryMock,
  runDayBasePrewarm: runDayBasePrewarmMock,
}));

vi.mock("./day-base-prewarm-pickup", () => ({
  headDayBaseObject: headDayBaseObjectMock,
  pickUpPrewarmDayBase: pickUpPrewarmDayBaseMock,
}));

vi.mock("./day-base-race-materializer", () => ({
  materializeDayBasePerRaceCache: materializeDayBasePerRaceCacheMock,
}));

vi.mock("./feature-hit-prediction", () => ({
  fanOutPredictionsAfterDayBaseHit: fanOutPredictionsAfterDayBaseHitMock,
}));

vi.mock("./focused-full-day-base-readiness", () => ({
  getFocusedFullDayBaseReadiness: getFocusedFullDayBaseReadinessMock,
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
  shouldRunCoverageSelfHealCron: (cron: string) => cron === "7,22,37,52 1-14 * * *",
}));

// shouldRunRunningStyleKick*Cron is a pure string comparison against the two
// real cron constants (RUNNING_STYLE_KICK_CRON_MORNING_GAP/_TOMORROW_PREWARM,
// running-style-kick.ts) -- inlined here as literals rather than re-derived,
// mirroring the shouldRunCornerFeaturesRefreshCron mock above.
vi.mock("./running-style-kick", () => ({
  runRunningStyleKickMorningGap: runRunningStyleKickMorningGapMock,
  runRunningStyleKickTomorrowPrewarm: runRunningStyleKickTomorrowPrewarmMock,
  shouldRunRunningStyleKickMorningGapCron: (cron: string) => cron === "0 15-23 * * *",
  shouldRunRunningStyleKickTomorrowPrewarmCron: (cron: string) => cron === "0 13,14 * * *",
}));

const { retryPopulateViewerDisplayCacheMock } = vi.hoisted(() => ({
  retryPopulateViewerDisplayCacheMock: vi.fn(async () => true),
}));

vi.mock("./prediction-cache-warm", () => ({
  retryPopulateViewerDisplayCache: retryPopulateViewerDisplayCacheMock,
}));

import { PER_RACE_SCOPE_REQUIRED_ERROR } from "./per-race-scope-guard";
import workerDefault, {
  handleFetch,
  handleQueueBatch,
  handleScheduled,
  isInternalDeliveryCanaryRequest,
  isInternalPredictionReadinessRequest,
} from "./worker";
import type { Env } from "./types";

const PER_RACE_SCOPE = { keibajoCode: "05", raceBango: "11" };

const perRaceTriggerBody = (extra: Record<string, unknown> = {}): string =>
  JSON.stringify({ ...PER_RACE_SCOPE, runDate: "20260603", ...extra });

const runMock = vi.fn(async () => ({ success: true }));
const bindMock = vi.fn(() => ({ run: runMock }));
const prepareMock = vi.fn(() => ({ bind: bindMock }));
const predictQueueSendMock = vi.fn(async () => undefined);
const weightRescoreQueueSendMock = vi.fn(async () => undefined);
const controlQueueSendMock = vi.fn(async () => undefined);
const containerDoFetchMock = vi.fn(async () => Response.json({ ok: true }));
const containerDoGetMock = vi.fn(() => ({ fetch: containerDoFetchMock }));
const containerDoIdFromNameMock = vi.fn((name: string) => ({ name }));
const realtimeAllMock = vi.fn(async () => ({
  results: [{ keibajo_code: "05", race_bango: "11", source: "jra" }],
}));
const realtimeBindMock = vi.fn(() => ({ all: realtimeAllMock }));
const realtimePrepareMock = vi.fn(() => ({ bind: realtimeBindMock }));

const makeEnv = (): Env => ({
  CONTAINER_CONTROL_QUEUE: { send: controlQueueSendMock } as unknown as NonNullable<
    Env["CONTAINER_CONTROL_QUEUE"]
  >,
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
  REALTIME_ADMIN_TOKEN: "admin-secret",
  REALTIME_DB: { prepare: realtimePrepareMock } as unknown as D1Database,
  RESCORE_ENABLED: "1",
  TRIGGER_TOKEN: "secret-token",
  WEIGHT_RESCORE_QUEUE: {
    send: weightRescoreQueueSendMock,
  } as unknown as NonNullable<Env["WEIGHT_RESCORE_QUEUE"]>,
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
  claimContainerSlotMock.mockClear();
  claimRescoreRaceMock.mockClear();
  checkContainerSlotStopMock.mockClear();
  checkContainerSlotStopMock.mockResolvedValue(true);
  completeFocusedFullRaceMock.mockClear();
  clearContainerSlotMock.mockClear();
  releaseContainerSlotMock.mockClear();
  runDayBasePrewarmMock.mockClear();
  prewarmCategoryMock.mockClear();
  pickUpPrewarmDayBaseMock.mockClear();
  headDayBaseObjectMock.mockClear();
  fanOutPredictionsAfterDayBaseHitMock.mockClear();
  getFocusedFullDayBaseReadinessMock.mockReset();
  getFocusedFullDayBaseReadinessMock.mockResolvedValue({
    ready: false,
    reason: "day-base-missing-or-invalid",
  });
  materializeDayBasePerRaceCacheMock.mockClear();
  refreshCornerFeaturesMock.mockClear();
  runCoverageSelfHealMock.mockClear();
  runRunningStyleKickMorningGapMock.mockClear();
  runRunningStyleKickTomorrowPrewarmMock.mockClear();
  enqueueDeliveryCanaryMock.mockClear();
  listDeliveryCanariesMock.mockClear();
  getPredictionReadinessMock.mockClear();
  predictQueueSendMock.mockClear();
  weightRescoreQueueSendMock.mockClear();
  controlQueueSendMock.mockClear();
  enqueueDayBasePrewarmMock.mockClear();
  containerDoFetchMock.mockClear();
  containerDoGetMock.mockClear();
  containerDoIdFromNameMock.mockClear();
  realtimeAllMock.mockClear();
  realtimeBindMock.mockClear();
  realtimePrepareMock.mockClear();
  resolveCardMaxRaceBangoForKochiMock.mockClear();
  retryPopulateViewerDisplayCacheMock.mockClear();
  retryPopulateViewerDisplayCacheMock.mockResolvedValue(true);
  enqueueMock.mockResolvedValue(["jra", "nar", "ban-ei"]);
  coordinatorTickMock.mockResolvedValue([]);
  claimRescoreRaceMock.mockResolvedValue({ proceed: true });
  claimContainerSlotMock.mockResolvedValue({ proceed: true });
  releaseContainerSlotMock.mockResolvedValue(undefined);
  resolveCardMaxRaceBangoForKochiMock.mockResolvedValue(undefined);
  pickUpPrewarmDayBaseMock.mockResolvedValue(false);
  headDayBaseObjectMock.mockResolvedValue(null);
  fanOutPredictionsAfterDayBaseHitMock.mockResolvedValue(0);
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

const adminPrewarmDayBaseRequest = (token: string | null, body: string): Request =>
  new Request("https://cron.example/api/admin/prewarm-day-base", {
    body,
    headers: token === null ? {} : { authorization: `Bearer ${token}` },
    method: "POST",
  });

const adminPickupDayBaseRequest = (token: string | null, body: string): Request =>
  new Request("https://cron.example/api/admin/pickup-day-base", {
    body,
    headers: token === null ? {} : { authorization: `Bearer ${token}` },
    method: "POST",
  });

const adminMaterializeDayBaseRequest = (token: string | null, body: string): Request =>
  new Request("https://cron.example/api/admin/materialize-day-base-races", {
    body,
    headers: token === null ? {} : { authorization: `Bearer ${token}` },
    method: "POST",
  });

test("admin materialize route is authenticated, scoped, and returns the idempotent result", async () => {
  expect(await handleFetch(adminMaterializeDayBaseRequest(null, "{}"), makeEnv())).toMatchObject({
    status: 401,
  });
  expect(
    await handleFetch(
      adminMaterializeDayBaseRequest("secret-token", JSON.stringify({ runYmd: "20260823" })),
      makeEnv(),
    ),
  ).toMatchObject({ status: 400 });

  const env = makeEnv();
  const response = await handleFetch(
    adminMaterializeDayBaseRequest(
      "secret-token",
      JSON.stringify({ category: "jra", runYmd: "20260823" }),
    ),
    env,
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toMatchObject({
    category: "jra",
    ok: true,
    result: { raceCount: 36, rowCount: 466, status: "materialized" },
  });
  expect(materializeDayBasePerRaceCacheMock).toHaveBeenCalledWith({
    category: "jra",
    env,
    runYmd: "20260823",
  });
});

test("admin materialize route returns 503 on a fail-closed fallback", async () => {
  materializeDayBasePerRaceCacheMock.mockResolvedValueOnce({
    reason: "source-size-limit",
    status: "fallback",
  });
  const response = await handleFetch(
    adminMaterializeDayBaseRequest(
      "secret-token",
      JSON.stringify({ category: "jra", runYmd: "20260823" }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(503);
  await expect(response.json()).resolves.toMatchObject({ ok: false });
});

test("admin pickup-day-base rejects unauthenticated and unscoped requests", async () => {
  const unauthorized = await handleFetch(adminPickupDayBaseRequest(null, "{}"), makeEnv());
  const unscoped = await handleFetch(
    adminPickupDayBaseRequest("secret-token", JSON.stringify({ runYmd: "20260817" })),
    makeEnv(),
  );
  expect(unauthorized.status).toBe(401);
  expect(unscoped.status).toBe(400);
  expect(pickUpPrewarmDayBaseMock).not.toHaveBeenCalled();
  expect(headDayBaseObjectMock).not.toHaveBeenCalled();
});

test("admin pickup-day-base reports a cache miss without touching container coordination", async () => {
  const env = makeEnv();
  const response = await handleFetch(
    adminPickupDayBaseRequest(
      "secret-token",
      JSON.stringify({ category: "jra", runYmd: "20260817" }),
    ),
    env,
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({
    category: "jra",
    ok: false,
    pickedUp: false,
    readiness: "pickup-missing",
    racesEnqueued: 0,
    runYmd: "20260817",
  });
  expect(pickUpPrewarmDayBaseMock).toHaveBeenCalledWith({
    category: "jra",
    env,
    runYmd: "20260817",
  });
  expect(getFocusedFullDayBaseReadinessMock).toHaveBeenCalledTimes(1);
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
  expect(containerDoFetchMock).not.toHaveBeenCalled();
  expect(fanOutPredictionsAfterDayBaseHitMock).not.toHaveBeenCalled();
});

test("admin pickup-day-base verifies R2 before ordered prediction fanout", async () => {
  const env = makeEnv();
  pickUpPrewarmDayBaseMock.mockResolvedValue(true);
  getFocusedFullDayBaseReadinessMock
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: true, reason: "ready" });
  headDayBaseObjectMock.mockResolvedValue({ customMetadata: {} });
  fanOutPredictionsAfterDayBaseHitMock.mockResolvedValue(36);
  const response = await handleFetch(
    adminPickupDayBaseRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        generatePredictionsAfterHit: true,
        runYmd: "20260817",
      }),
    ),
    env,
  );
  expect(response.status).toBe(200);
  await expect(response.json()).resolves.toStrictEqual({
    category: "jra",
    ok: true,
    pickedUp: true,
    readiness: "ready",
    racesEnqueued: 36,
    runYmd: "20260817",
  });
  expect(fanOutPredictionsAfterDayBaseHitMock).toHaveBeenCalledWith({
    category: "jra",
    env,
    runYmd: "20260817",
  });
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
});

test("admin pickup-day-base rejects a stale partial canonical object and blocks fanout", async () => {
  const env = makeEnv();
  pickUpPrewarmDayBaseMock.mockResolvedValue(true);
  getFocusedFullDayBaseReadinessMock
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({
      ready: false,
      reason: "source-row-count-26-of-392",
    });
  const response = await handleFetch(
    adminPickupDayBaseRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        generatePredictionsAfterHit: true,
        runYmd: "20260817",
      }),
    ),
    env,
  );

  await expect(response.json()).resolves.toStrictEqual({
    category: "jra",
    ok: false,
    pickedUp: false,
    readiness: "source-row-count-26-of-392",
    racesEnqueued: 0,
    runYmd: "20260817",
  });
  expect(fanOutPredictionsAfterDayBaseHitMock).not.toHaveBeenCalled();
});

test("admin pickup-day-base uses an already-ready R2 generation without waking its Container", async () => {
  const env = makeEnv();
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({ ready: true, reason: "ready" });

  const response = await handleFetch(
    adminPickupDayBaseRequest(
      "secret-token",
      JSON.stringify({ category: "ban-ei", runYmd: "20260817" }),
    ),
    env,
  );

  await expect(response.json()).resolves.toMatchObject({
    ok: true,
    pickedUp: true,
    readiness: "ready",
  });
  expect(pickUpPrewarmDayBaseMock).not.toHaveBeenCalled();
});

test("admin prewarm-day-base rejects unauthenticated requests", async () => {
  const response = await handleFetch(adminPrewarmDayBaseRequest(null, "{}"), makeEnv());
  expect(response.status).toBe(401);
  expect(runDayBasePrewarmMock).not.toHaveBeenCalled();
  expect(prewarmCategoryMock).not.toHaveBeenCalled();
});

test("admin prewarm-day-base rejects an invalid runYmd", async () => {
  const response = await handleFetch(
    adminPrewarmDayBaseRequest("secret-token", JSON.stringify({ runYmd: "2026-08-16" })),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(runDayBasePrewarmMock).not.toHaveBeenCalled();
});

test("admin prewarm-day-base rejects an invalid category", async () => {
  const response = await handleFetch(
    adminPrewarmDayBaseRequest(
      "secret-token",
      JSON.stringify({ category: "overseas", runYmd: "20260817" }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(prewarmCategoryMock).not.toHaveBeenCalled();
});

test("admin prewarm-day-base rejects an invalid or unscoped generation intent", async () => {
  const invalidFlag = await handleFetch(
    adminPrewarmDayBaseRequest(
      "secret-token",
      JSON.stringify({ category: "jra", generatePredictionsAfterHit: "yes", runYmd: "20260817" }),
    ),
    makeEnv(),
  );
  const unscopedFlag = await handleFetch(
    adminPrewarmDayBaseRequest(
      "secret-token",
      JSON.stringify({ generatePredictionsAfterHit: true, runYmd: "20260817" }),
    ),
    makeEnv(),
  );
  expect(invalidFlag.status).toBe(400);
  expect(unscopedFlag.status).toBe(400);
  expect(prewarmCategoryMock).not.toHaveBeenCalled();
});

test("admin prewarm-day-base dispatches one category", async () => {
  const response = await handleFetch(
    adminPrewarmDayBaseRequest(
      "secret-token",
      JSON.stringify({ category: "ban-ei", runYmd: "20260817" }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  await expect(response.json()).resolves.toStrictEqual({
    category: "ban-ei",
    ok: true,
    queued: true,
    runYmd: "20260817",
  });
  expect(enqueueDayBasePrewarmMock).toHaveBeenCalledWith(
    expect.objectContaining({ category: "ban-ei", daysAhead: 0, runYmd: "20260817" }),
  );
  expect(prewarmCategoryMock).not.toHaveBeenCalled();
  expect(runDayBasePrewarmMock).not.toHaveBeenCalled();
});

test("admin prewarm-day-base forwards the feature-hit generation intent", async () => {
  const response = await handleFetch(
    adminPrewarmDayBaseRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        generatePredictionsAfterHit: true,
        runYmd: "20260817",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  expect(enqueueDayBasePrewarmMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 0,
    env: expect.any(Object),
    generatePredictionsAfterHit: true,
    runYmd: "20260817",
  });
});

test("admin prewarm-day-base preserves an explicit category-scoped historical force", async () => {
  const response = await handleFetch(
    adminPrewarmDayBaseRequest(
      "secret-token",
      JSON.stringify({ category: "jra", force: true, runYmd: "20260817" }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  expect(enqueueDayBasePrewarmMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 0,
    env: expect.any(Object),
    force: true,
    runYmd: "20260817",
  });
});

test("admin prewarm-day-base rejects invalid and unscoped historical force", async () => {
  const invalid = await handleFetch(
    adminPrewarmDayBaseRequest(
      "secret-token",
      JSON.stringify({ category: "jra", force: "yes", runYmd: "20260817" }),
    ),
    makeEnv(),
  );
  const unscoped = await handleFetch(
    adminPrewarmDayBaseRequest("secret-token", JSON.stringify({ force: true, runYmd: "20260817" })),
    makeEnv(),
  );
  expect(invalid.status).toBe(400);
  expect(unscoped.status).toBe(400);
});

test("admin prewarm-day-base without category warms every scheduled category", async () => {
  const response = await handleFetch(
    adminPrewarmDayBaseRequest("secret-token", JSON.stringify({ runYmd: "20260817" })),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  await expect(response.json()).resolves.toStrictEqual({
    category: "all",
    ok: true,
    queued: true,
    runYmd: "20260817",
  });
  expect(runDayBasePrewarmMock).toHaveBeenCalledTimes(1);
  expect(runDayBasePrewarmMock).toHaveBeenCalledWith(
    expect.objectContaining({ daysAhead: 0, runYmd: "20260817" }),
  );
  expect(prewarmCategoryMock).not.toHaveBeenCalled();
});

test("monitor endpoint predicates require GET and exact paths", () => {
  expect(isInternalPredictionReadinessRequest("GET", "/api/internal/prediction-readiness")).toBe(
    true,
  );
  expect(isInternalPredictionReadinessRequest("POST", "/api/internal/prediction-readiness")).toBe(
    false,
  );
  expect(isInternalDeliveryCanaryRequest("GET", "/api/internal/delivery-canaries")).toBe(true);
  expect(isInternalDeliveryCanaryRequest("GET", "/wrong")).toBe(false);
});

test("monitor endpoints authenticate and return readiness/canary payloads", async () => {
  const env = makeEnv();
  const readinessUrl = "https://cron.example/api/internal/prediction-readiness";
  const unauthorized = await handleFetch(new Request(readinessUrl), env);
  expect(unauthorized.status).toBe(401);
  const readiness = await handleFetch(
    new Request(readinessUrl, { headers: { authorization: "Bearer secret-token" } }),
    env,
  );
  expect(readiness.status).toBe(200);
  await expect(readiness.json()).resolves.toMatchObject({ runYmd: "20260815" });
  expect(getPredictionReadinessMock).toHaveBeenCalledTimes(1);

  listDeliveryCanariesMock.mockResolvedValue([
    { consumedAt: null, deliveryLagMs: null, enqueuedAt: "now", id: "id" },
  ]);
  const canaryUrl = "https://cron.example/api/internal/delivery-canaries";
  const canaries = await handleFetch(
    new Request(canaryUrl, { headers: { authorization: "Bearer secret-token" } }),
    env,
  );
  expect(canaries.status).toBe(200);
  await expect(canaries.json()).resolves.toMatchObject({ canaries: [{ id: "id" }] });
});

test("fetch returns a health payload for GET", async () => {
  const response = await workerDefault.fetch(healthRequest(), makeEnv(), {
    waitUntil: vi.fn(),
  } as unknown as ExecutionContext);
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

test("handleFetch rejects missing keibajo and race on /run with 400 and PER_RACE_SCOPE_REQUIRED_ERROR", async () => {
  const response = await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ runDate: "20260603" })),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  const body = (await response.json()) as { ok: boolean; error: string };
  expect(body.ok).toBe(false);
  expect(body.error).toBe(PER_RACE_SCOPE_REQUIRED_ERROR);
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleFetch rejects keibajo-only partial scope on /run with 400", async () => {
  const response = await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ keibajoCode: "05", runDate: "20260603" })),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  const body = (await response.json()) as { ok: boolean; error: string };
  expect(body.error).toBe(PER_RACE_SCOPE_REQUIRED_ERROR);
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleFetch rejects race-only partial scope on /run with 400", async () => {
  const response = await handleFetch(
    triggerRequest("secret-token", JSON.stringify({ raceBango: "11", runDate: "20260603" })),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  const body = (await response.json()) as { ok: boolean; error: string };
  expect(body.error).toBe(PER_RACE_SCOPE_REQUIRED_ERROR);
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleFetch accepts per-race /run with both keibajoCode and raceBango", async () => {
  const response = await handleFetch(
    triggerRequest("secret-token", perRaceTriggerBody()),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  const body = (await response.json()) as { ok: boolean; runDate: string; queued: string[] };
  expect(body.ok).toBe(true);
  expect(body.runDate).toBe("2026-06-03");
  expect(body.queued).toStrictEqual(["jra", "nar", "ban-ei"]);
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: "05", raceBango: "11" }),
  );
});

test("handleFetch enqueues predict and returns 202 for an authorized explicit RUN_DATE", async () => {
  const response = await handleFetch(
    triggerRequest("secret-token", perRaceTriggerBody()),
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
  await handleFetch(triggerRequest("secret-token", perRaceTriggerBody()), makeEnv());
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ mode: "full" }));
});

test("handleFetch passes mode rescore when body specifies mode rescore", async () => {
  await handleFetch(
    triggerRequest("secret-token", perRaceTriggerBody({ mode: "rescore" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ mode: "rescore" }));
});

test("handleFetch rejects /run when the body omits runDate and per-race scope", async () => {
  const response = await handleFetch(triggerRequest("secret-token", ""), makeEnv());
  expect(response.status).toBe(400);
  const body = (await response.json()) as { ok: boolean; error: string };
  expect(body.error).toBe(PER_RACE_SCOPE_REQUIRED_ERROR);
  expect(enqueueMock).not.toHaveBeenCalled();
  expect(startMock).not.toHaveBeenCalled();
});

test("handleFetch defaults to today's JST date when the body omits runDate but has per-race scope", async () => {
  const response = await handleFetch(
    triggerRequest("secret-token", JSON.stringify(PER_RACE_SCOPE)),
    makeEnv(),
  );
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

test("handleScheduled predict cron no longer starts container", async () => {
  await handleScheduled(makeEvent("0 18 * * *"), makeEnv());
  expect(getContainerMock).not.toHaveBeenCalled();
  expect(startMock).not.toHaveBeenCalled();
});

test("handleScheduled predict cron no longer writes audit row", async () => {
  await handleScheduled(makeEvent("0 18 * * *"), makeEnv());
  expect(prepareMock).not.toHaveBeenCalled();
  expect(runMock).not.toHaveBeenCalled();
});

test("scheduled default handler delegates to handleScheduled without starting container for predict cron", async () => {
  await workerDefault.scheduled(makeEvent("0 18 * * *"), makeEnv());
  expect(startMock).not.toHaveBeenCalled();
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

test("handleScheduled ignores the retired race-hours keep-warm cron", async () => {
  await handleScheduled(makeEvent("*/30 1-11 * * *"), makeEnv());
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(getContainerMock).not.toHaveBeenCalled();
});

test("handleScheduled does not call warmNeon for the predict cron", async () => {
  await handleScheduled(makeEvent("0 18 * * *"), makeEnv());
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(getContainerMock).not.toHaveBeenCalled();
});

test("handleScheduled rescore cron no longer enqueues day-scoped rescore", async () => {
  await handleScheduled(makeEvent("*/20 1-11 * * *"), makeEnv());
  expect(enqueueMock).not.toHaveBeenCalled();
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(getContainerMock).not.toHaveBeenCalled();
});

test("handleScheduled rescore cron does not start container or write audit", async () => {
  await handleScheduled(makeEvent("*/20 1-11 * * *"), makeEnv());
  expect(startMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalled();
});

test("handleScheduled runs the per-race coordinator for the coordinator cron", async () => {
  await handleScheduled(makeEvent("*/10 1-11 * * *"), makeEnv());
  expect(coordinatorTickMock).toHaveBeenCalledTimes(1);
  expect(coordinatorTickMock).toHaveBeenCalledWith(expect.objectContaining({ leadMinutes: 25 }));
});

test("handleScheduled coordinator cron does not start container or enqueue canaries", async () => {
  await handleScheduled(makeEvent("*/10 1-11 * * *"), makeEnv());
  expect(startMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalled();
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(predictQueueSendMock).not.toHaveBeenCalled();
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleScheduled delivery-canary cron only persists and sends a canary", async () => {
  const env = makeEnv();
  await handleScheduled(makeEvent("*/5 0-13 * * *"), env);
  expect(enqueueDeliveryCanaryMock).toHaveBeenCalledWith(env, new Date("2026-06-02T18:00:00.000Z"));
  expect(coordinatorTickMock).not.toHaveBeenCalled();
  expect(startMock).not.toHaveBeenCalled();
  expect(warmNeonMock).not.toHaveBeenCalled();
});

test("handleScheduled does not run the coordinator for the rescore cron", async () => {
  await handleScheduled(makeEvent("*/20 1-11 * * *"), makeEnv());
  expect(coordinatorTickMock).not.toHaveBeenCalled();
});

test("handleScheduled runs the coverage self-heal scan for the coverage self-heal cron", async () => {
  await handleScheduled(makeEvent("7,22,37,52 1-14 * * *"), makeEnv());
  expect(runCoverageSelfHealMock).toHaveBeenCalledTimes(1);
  expect(runCoverageSelfHealMock).toHaveBeenCalledWith(
    expect.objectContaining({ now: new Date("2026-06-02T18:00:00.000Z") }),
  );
});

test("handleScheduled coverage self-heal cron does not start container, warm, coordinate, or day-base prewarm", async () => {
  await handleScheduled(makeEvent("7,22,37,52 1-14 * * *"), makeEnv());
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

test("handleScheduled dispatches the day-base prewarm for the early feature-build cron", async () => {
  await handleScheduled(makeEvent("0 21 * * *"), makeEnv());
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
  await handleScheduled(makeEvent("7,22,37,52 1-14 * * *"), makeEnv());
  expect(refreshCornerFeaturesMock).not.toHaveBeenCalled();
});

test("handleScheduled kicks the running-style morning-gap plan for the morning-gap cron", async () => {
  await handleScheduled(makeEvent("0 15-23 * * *"), makeEnv());
  expect(runRunningStyleKickMorningGapMock).toHaveBeenCalledTimes(1);
  expect(runRunningStyleKickMorningGapMock).toHaveBeenCalledWith(
    expect.objectContaining({ now: new Date("2026-06-02T18:00:00.000Z") }),
  );
  expect(runRunningStyleKickTomorrowPrewarmMock).not.toHaveBeenCalled();
});

test("handleScheduled morning-gap RS kick cron does not start container, warm, coordinate, self-heal, or refresh corner features", async () => {
  await handleScheduled(makeEvent("0 15-23 * * *"), makeEnv());
  expect(startMock).not.toHaveBeenCalled();
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(coordinatorTickMock).not.toHaveBeenCalled();
  expect(runCoverageSelfHealMock).not.toHaveBeenCalled();
  expect(refreshCornerFeaturesMock).not.toHaveBeenCalled();
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleScheduled kicks the running-style tomorrow-prewarm plan for the tomorrow-prewarm cron", async () => {
  await handleScheduled(makeEvent("0 13,14 * * *"), makeEnv());
  expect(runRunningStyleKickTomorrowPrewarmMock).toHaveBeenCalledTimes(1);
  expect(runRunningStyleKickTomorrowPrewarmMock).toHaveBeenCalledWith(
    expect.objectContaining({ now: new Date("2026-06-02T18:00:00.000Z") }),
  );
  expect(runRunningStyleKickMorningGapMock).not.toHaveBeenCalled();
});

test("handleScheduled tomorrow-prewarm RS kick cron does not start container, warm, coordinate, self-heal, or refresh corner features", async () => {
  await handleScheduled(makeEvent("0 13,14 * * *"), makeEnv());
  expect(startMock).not.toHaveBeenCalled();
  expect(warmNeonMock).not.toHaveBeenCalled();
  expect(coordinatorTickMock).not.toHaveBeenCalled();
  expect(runCoverageSelfHealMock).not.toHaveBeenCalled();
  expect(refreshCornerFeaturesMock).not.toHaveBeenCalled();
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleScheduled does not kick running-style for the coverage self-heal cron", async () => {
  await handleScheduled(makeEvent("7,22,37,52 1-14 * * *"), makeEnv());
  expect(runRunningStyleKickMorningGapMock).not.toHaveBeenCalled();
  expect(runRunningStyleKickTomorrowPrewarmMock).not.toHaveBeenCalled();
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

test("queue default handler delegates the dedicated weight-rescore queue to handleQueue", async () => {
  const batch = {
    messages: [],
    queue: "finish-position-weight-rescore-queue",
  } as unknown as MessageBatch<import("./types").PredictQueueMessage>;
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

test("control queue stops and acknowledges a named container", async () => {
  const ack = vi.fn();
  const retry = vi.fn();
  const batch = {
    messages: [
      {
        ack,
        body: {
          name: "predict-jra-0",
          requestedAt: "2026-08-22T00:00:00.000Z",
          type: "container-stop",
        },
        retry,
      },
    ],
    queue: "finish-position-container-control-queue",
  } as unknown as MessageBatch<import("./types").ContainerControlMessage>;
  await handleQueueBatch(batch, makeEnv());
  expect(containerDoFetchMock).toHaveBeenCalledTimes(1);
  expect(ack).toHaveBeenCalledTimes(1);
  expect(retry).not.toHaveBeenCalled();
});

test("control queue retries failed stops and ignores invalid control bodies", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  containerDoFetchMock.mockResolvedValueOnce(new Response("busy", { status: 503 }));
  const ack = vi.fn();
  const retry = vi.fn();
  const batch = {
    messages: [
      { ack, body: { type: "unknown" }, retry },
      {
        ack,
        body: {
          name: "predict-nar-0",
          requestedAt: "2026-08-22T00:00:00.000Z",
          type: "container-stop",
        },
        retry,
      },
    ],
    queue: "finish-position-container-control-queue",
  } as unknown as MessageBatch<import("./types").ContainerControlMessage>;
  await handleQueueBatch(batch, makeEnv());
  expect(ack).not.toHaveBeenCalled();
  expect(retry).toHaveBeenCalledWith({ delaySeconds: 30 });
  errorSpy.mockRestore();
});

test("handleFetch passes category nar when body specifies category nar", async () => {
  enqueueMock.mockResolvedValue(["nar"]);
  await handleFetch(
    triggerRequest(
      "secret-token",
      perRaceTriggerBody({ category: "nar", keibajoCode: "45", raceBango: "12" }),
    ),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ category: "nar" }));
});

test("handleFetch omits category when body does not specify category", async () => {
  await handleFetch(triggerRequest("secret-token", perRaceTriggerBody()), makeEnv());
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ category: undefined }));
});

test("handleFetch ignores invalid category and calls enqueue without category", async () => {
  await handleFetch(
    triggerRequest("secret-token", perRaceTriggerBody({ category: "invalid" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ category: undefined }));
});

test("handleFetch does not write an audit row when enqueueing", async () => {
  await handleFetch(triggerRequest("secret-token", perRaceTriggerBody()), makeEnv());
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
      JSON.stringify({
        category: "jra",
        debug: "1",
        keibajoCode: "05",
        raceBango: "11",
        runDate: "20260628",
      }),
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

test("handleFetch rejects a blank keibajoCode as missing per-race scope", async () => {
  const response = await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({ category: "nar", keibajoCode: "   ", raceBango: "12", runDate: "20260619" }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  const body = (await response.json()) as { ok: boolean; error: string };
  expect(body.error).toBe(PER_RACE_SCOPE_REQUIRED_ERROR);
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleFetch rejects a non-string raceBango as missing per-race scope", async () => {
  const response = await handleFetch(
    triggerRequest(
      "secret-token",
      JSON.stringify({ category: "nar", keibajoCode: "45", raceBango: 12, runDate: "20260619" }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  const body = (await response.json()) as { ok: boolean; error: string };
  expect(body.error).toBe(PER_RACE_SCOPE_REQUIRED_ERROR);
  expect(enqueueMock).not.toHaveBeenCalled();
});

test("handleFetch passes skipDedup true when body specifies skipDedup true", async () => {
  await handleFetch(
    triggerRequest("secret-token", perRaceTriggerBody({ skipDedup: true })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ skipDedup: true }));
});

test("handleFetch omits skipDedup when body specifies skipDedup as string true", async () => {
  await handleFetch(
    triggerRequest("secret-token", perRaceTriggerBody({ skipDedup: "true" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.not.objectContaining({ skipDedup: expect.anything() }),
  );
});

test("handleFetch omits skipDedup when body does not specify skipDedup", async () => {
  await handleFetch(triggerRequest("secret-token", perRaceTriggerBody()), makeEnv());
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.not.objectContaining({ skipDedup: expect.anything() }),
  );
});

test("handleFetch passes force true when body specifies force true", async () => {
  await handleFetch(triggerRequest("secret-token", perRaceTriggerBody({ force: true })), makeEnv());
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(expect.objectContaining({ force: true }));
});

test("handleFetch omits force when body specifies force as string true", async () => {
  await handleFetch(
    triggerRequest("secret-token", perRaceTriggerBody({ force: "true" })),
    makeEnv(),
  );
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.not.objectContaining({ force: expect.anything() }),
  );
});

test("handleFetch omits force when body does not specify force", async () => {
  await handleFetch(triggerRequest("secret-token", perRaceTriggerBody()), makeEnv());
  expect(enqueueMock).toHaveBeenCalledTimes(1);
  expect(enqueueMock).toHaveBeenCalledWith(
    expect.not.objectContaining({ force: expect.anything() }),
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

test("admin stop containers endpoint rejects a non-boolean active override", async () => {
  const response = await handleFetch(
    adminStopContainersRequest(
      "secret-token",
      JSON.stringify({ names: ["predict-jra-0"], overrideActive: "true" }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(400);
  expect(controlQueueSendMock).not.toHaveBeenCalled();
});

test("admin stop containers endpoint returns 400 for malformed JSON", async () => {
  const response = await handleFetch(adminStopContainersRequest("secret-token", "{"), makeEnv());
  expect(response.status).toBe(400);
  const body = (await response.json()) as { ok: boolean; error: string };
  expect(body.ok).toBe(false);
  expect(body.error).toContain("SyntaxError");
  expect(containerDoFetchMock).not.toHaveBeenCalled();
});

test("admin stop containers endpoint enqueues each requested container on the control queue", async () => {
  const response = await handleFetch(
    adminStopContainersRequest(
      "secret-token",
      JSON.stringify({
        names: ["predict-nar-20260702-50-01", "predict-nar-20260702-50-02"],
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  expect(controlQueueSendMock).toHaveBeenCalledTimes(2);
  expect(controlQueueSendMock).toHaveBeenNthCalledWith(
    1,
    expect.objectContaining({ name: "predict-nar-20260702-50-01", type: "container-stop" }),
  );
  expect(controlQueueSendMock).toHaveBeenNthCalledWith(
    2,
    expect.objectContaining({ name: "predict-nar-20260702-50-02", type: "container-stop" }),
  );
  expect(controlQueueSendMock).toHaveBeenNthCalledWith(
    1,
    expect.not.objectContaining({ force: expect.anything() }),
  );
  expect(controlQueueSendMock).toHaveBeenNthCalledWith(
    2,
    expect.not.objectContaining({ force: expect.anything() }),
  );
  expect(containerDoFetchMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
});

test("admin stop containers endpoint forces a stop only with an explicit active override", async () => {
  const response = await handleFetch(
    adminStopContainersRequest(
      "secret-token",
      JSON.stringify({ names: ["predict-jra-0"], overrideActive: true }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  expect(controlQueueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, name: "predict-jra-0", type: "container-stop" }),
  );
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
  expect(retryPopulateViewerDisplayCacheMock).not.toHaveBeenCalled();
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
  expect(retryPopulateViewerDisplayCacheMock).not.toHaveBeenCalled();
});

test("admin run focused full race endpoint enqueues a focused full prediction", async () => {
  const response = await handleFetch(
    adminRunFocusedFullRaceRequest(
      "secret-token",
      JSON.stringify({
        category: "jra",
        debug: true,
        force: true,
        keibajoCode: "10",
        raceBango: "07",
        runYmd: "20260705",
      }),
    ),
    makeEnv(),
  );
  expect(response.status).toBe(202);
  expect(enqueueMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 0,
    debug: true,
    env: expect.any(Object),
    force: true,
    keibajoCode: "10",
    mode: "full",
    raceBango: "07",
    runDate: "2026-07-05",
    runYmd: "20260705",
    skipDedup: true,
  });
  expect(containerDoFetchMock).not.toHaveBeenCalled();
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(retryPopulateViewerDisplayCacheMock).not.toHaveBeenCalled();
});

test("admin run focused full race endpoint omits optional debug and force fields", async () => {
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
  expect(response.status).toBe(202);
  expect(enqueueMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 0,
    env: expect.any(Object),
    keibajoCode: "10",
    mode: "full",
    raceBango: "07",
    runDate: "2026-07-05",
    runYmd: "20260705",
    skipDedup: true,
  });
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
  expect(weightRescoreQueueSendMock).toHaveBeenCalledTimes(1);
  expect(weightRescoreQueueSendMock).toHaveBeenCalledWith({
    category: "nar",
    daysAhead: 0,
    keibajoCode: "45",
    mode: "rescore",
    raceBango: "12",
    runDate: "2026-06-19",
    runDateIso: "2026-06-19",
    runYmd: "20260619",
  });
  expect(predictQueueSendMock).not.toHaveBeenCalled();
});

test("internal rescore-race endpoint safely falls back to the primary queue during a rolling deploy", async () => {
  const env = makeEnv();
  delete env.WEIGHT_RESCORE_QUEUE;
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
    env,
  );
  expect(response.status).toBe(202);
  expect(predictQueueSendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 0,
    keibajoCode: "05",
    mode: "rescore",
    raceBango: "11",
    runDate: "2026-06-20",
    runDateIso: "2026-06-20",
    runYmd: "20260620",
  });
  expect(weightRescoreQueueSendMock).not.toHaveBeenCalled();
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
  expect(weightRescoreQueueSendMock).toHaveBeenCalledWith(
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
  expect(weightRescoreQueueSendMock).toHaveBeenCalledWith(
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
