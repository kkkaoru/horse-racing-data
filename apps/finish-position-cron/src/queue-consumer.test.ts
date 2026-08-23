// Run with bun. Tests for the queue consumer (DO-backed dedup).

import { beforeEach, expect, test, vi } from "vitest";
import type { RaceEntry } from "./cron-decision";
import type { ParseNdjsonStreamOptions, PredictResultLine } from "./ndjson-stream";
import type { PredictionKvPublishResult } from "./prediction-kv-writer";
import type { Env, PredictQueueMessage } from "./types";

interface ClaimResult {
  proceed: boolean;
  state?: string;
}

interface RescoreResult {
  status: "ok" | "cache_miss" | "race_not_found";
  racesPredicted: number;
  predictionCount: number;
  modelVersion: string | null;
}

const {
  claimContainerSlotMock,
  claimFocusedFullRaceMock,
  claimRescoreExecutionMock,
  claimRunMock,
  clearContainerSlotMock,
  completeFocusedFullRaceMock,
  completeRescoreRaceMock,
  completeRunMock,
  failFocusedFullRaceEnqueueMock,
  releaseContainerSlotMock,
  reserveFocusedFullRaceEnqueueMock,
  touchContainerSlotMock,
  isOldDateRunYmdMock,
  parseNdjsonStreamMock,
  rescoreJraRaceMock,
  warmPredictionCacheForRaceMock,
  warmPredictionCacheForCategoryMock,
  publishFinishPositionPredictionCacheMock,
  publishFinishPositionPredictionCacheForCategoryMock,
  isFocusedFullPredictionCompleteMock,
  isPerRaceFeatureCachePresentMock,
  isPerRaceRescoreReadyMock,
  getRunningStyleRaceReadinessMock,
} = vi.hoisted(() => {
  const claimContainerSlot = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const claimFocusedFullRace = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const claimRescoreExecution = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const claimRun = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const clearContainerSlot = vi.fn(async () => undefined);
  const releaseContainerSlot = vi.fn(async () => undefined);
  const touchContainerSlot = vi.fn(async () => undefined);
  const completeFocusedFullRace = vi.fn(async () => undefined);
  const completeRescoreRace = vi.fn(async () => undefined);
  const completeRun = vi.fn(async () => undefined);
  const failFocusedFullRaceEnqueue = vi.fn(async () => undefined);
  const reserveFocusedFullRaceEnqueue = vi.fn(
    async (): Promise<ClaimResult> => ({ proceed: true }),
  );
  const isOldDateRunYmd = vi.fn((): boolean => false);
  const parseNdjsonStream = vi.fn(
    async (
      _body: ReadableStream<Uint8Array>,
      _options?: ParseNdjsonStreamOptions,
    ): Promise<PredictResultLine> => ({
      type: "result" as const,
      racesPredicted: 5,
      category: "jra",
      status: "success" as const,
    }),
  );
  const rescoreJraRace = vi.fn(
    async (): Promise<RescoreResult> => ({
      modelVersion: "jra-cb-v9-sim-2013-clean",
      predictionCount: 3,
      racesPredicted: 1,
      status: "ok",
    }),
  );
  const warmPredictionCacheForRace = vi.fn(async (): Promise<boolean> => true);
  const warmPredictionCacheForCategory = vi.fn(async (): Promise<number> => 0);
  const publishFinishPositionPredictionCache = vi.fn(
    async (): Promise<PredictionKvPublishResult> => ({
      busted: true,
      status: "written",
    }),
  );
  const publishFinishPositionPredictionCacheForCategory = vi.fn(async (): Promise<number> => 0);
  const isFocusedFullPredictionComplete = vi.fn(async (): Promise<boolean> => false);
  const isPerRaceFeatureCachePresent = vi.fn(async (): Promise<boolean> => true);
  const isPerRaceRescoreReady = vi.fn(async (): Promise<boolean> => true);
  const getRunningStyleRaceReadiness = vi.fn(
    async (params: {
      races: readonly RaceEntry[];
    }): Promise<readonly { race: RaceEntry; reason: string | null }[]> =>
      params.races.map((race) => ({ race, reason: null })),
  );
  return {
    claimContainerSlotMock: claimContainerSlot,
    claimFocusedFullRaceMock: claimFocusedFullRace,
    claimRescoreExecutionMock: claimRescoreExecution,
    claimRunMock: claimRun,
    clearContainerSlotMock: clearContainerSlot,
    completeFocusedFullRaceMock: completeFocusedFullRace,
    completeRescoreRaceMock: completeRescoreRace,
    failFocusedFullRaceEnqueueMock: failFocusedFullRaceEnqueue,
    releaseContainerSlotMock: releaseContainerSlot,
    reserveFocusedFullRaceEnqueueMock: reserveFocusedFullRaceEnqueue,
    touchContainerSlotMock: touchContainerSlot,
    completeRunMock: completeRun,
    isFocusedFullPredictionCompleteMock: isFocusedFullPredictionComplete,
    isPerRaceFeatureCachePresentMock: isPerRaceFeatureCachePresent,
    isPerRaceRescoreReadyMock: isPerRaceRescoreReady,
    getRunningStyleRaceReadinessMock: getRunningStyleRaceReadiness,
    isOldDateRunYmdMock: isOldDateRunYmd,
    parseNdjsonStreamMock: parseNdjsonStream,
    publishFinishPositionPredictionCacheForCategoryMock:
      publishFinishPositionPredictionCacheForCategory,
    publishFinishPositionPredictionCacheMock: publishFinishPositionPredictionCache,
    rescoreJraRaceMock: rescoreJraRace,
    warmPredictionCacheForCategoryMock: warmPredictionCacheForCategory,
    warmPredictionCacheForRaceMock: warmPredictionCacheForRace,
  };
});

const {
  clearDayBaseRepairReservationMock,
  enqueueDayBaseRepairOnceMock,
  getFocusedFullDayBaseReadinessMock,
} = vi.hoisted(() => ({
  clearDayBaseRepairReservationMock: vi.fn(async () => undefined),
  enqueueDayBaseRepairOnceMock: vi.fn(async () => "enqueued"),
  getFocusedFullDayBaseReadinessMock: vi.fn(async () => ({ ready: true, reason: "ready" })),
}));

const { addRescoreAttestationToUrlMock, createRescoreAttestationMock } = vi.hoisted(() => ({
  addRescoreAttestationToUrlMock: vi.fn((url: string) => url),
  createRescoreAttestationMock: vi.fn(async () => ({
    attestationIssuedAtMs: 1_777_000_000_000,
    entryCount: 2,
    entrySetHash: "a".repeat(64),
    featureCacheEtag: "feature-etag",
    featureCacheVersion: "feature-version",
  })),
}));

vi.mock("./day-base-repair", () => ({
  clearDayBaseRepairReservation: clearDayBaseRepairReservationMock,
  enqueueDayBaseRepairOnce: enqueueDayBaseRepairOnceMock,
}));

vi.mock("./focused-full-day-base-readiness", () => ({
  getFocusedFullDayBaseReadiness: getFocusedFullDayBaseReadinessMock,
}));

vi.mock("./rescore-attestation", () => ({
  addRescoreAttestationToUrl: addRescoreAttestationToUrlMock,
  createRescoreAttestation: createRescoreAttestationMock,
}));

vi.mock("./do-state", () => ({
  claimContainerSlot: claimContainerSlotMock,
  claimFocusedFullRace: claimFocusedFullRaceMock,
  claimRescoreExecution: claimRescoreExecutionMock,
  claimRun: claimRunMock,
  clearContainerSlot: clearContainerSlotMock,
  completeFocusedFullRace: completeFocusedFullRaceMock,
  completeRescoreRace: completeRescoreRaceMock,
  completeRun: completeRunMock,
  failFocusedFullRaceEnqueue: failFocusedFullRaceEnqueueMock,
  releaseContainerSlot: releaseContainerSlotMock,
  reserveFocusedFullRaceEnqueue: reserveFocusedFullRaceEnqueueMock,
  touchContainerSlot: touchContainerSlotMock,
}));

vi.mock("./ndjson-stream", () => ({
  parseNdjsonStream: parseNdjsonStreamMock,
}));

vi.mock("./old-date-guard", () => ({
  isOldDateRunYmd: isOldDateRunYmdMock,
  OLD_DATE_THRESHOLD_DAYS: 2,
}));

vi.mock("./scoring/rescore-consumer", () => ({
  rescoreJraRace: rescoreJraRaceMock,
}));

vi.mock("./prediction-cache-warm", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./prediction-cache-warm")>();
  return {
    ...actual,
    warmPredictionCacheForCategory: warmPredictionCacheForCategoryMock,
    warmPredictionCacheForRace: warmPredictionCacheForRaceMock,
    warmViewerDisplayForRace: warmPredictionCacheForRaceMock,
  };
});

vi.mock("./prediction-kv-writer", () => ({
  publishFinishPositionPredictionCache: publishFinishPositionPredictionCacheMock,
  publishFinishPositionPredictionCacheForCategory:
    publishFinishPositionPredictionCacheForCategoryMock,
}));

vi.mock("./focused-full-completion", () => ({
  isFocusedFullPredictionComplete: isFocusedFullPredictionCompleteMock,
  isPerRaceFeatureCachePresent: isPerRaceFeatureCachePresentMock,
  isPerRaceRescoreReady: isPerRaceRescoreReadyMock,
}));

vi.mock("./running-style-readiness", () => ({
  getRunningStyleRaceReadiness: getRunningStyleRaceReadinessMock,
}));

const { consumeDayBasePickupMock } = vi.hoisted(() => ({
  consumeDayBasePickupMock: vi.fn(async () => undefined),
}));

const { prewarmCategoryWithOutcomeMock } = vi.hoisted(() => ({
  prewarmCategoryWithOutcomeMock: vi.fn(async () => "landed"),
}));

vi.mock("./day-base-pickup", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./day-base-pickup")>();
  return {
    ...actual,
    consumeDayBasePickup: consumeDayBasePickupMock,
  };
});

vi.mock("./day-base-prewarm", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./day-base-prewarm")>();
  return { ...actual, prewarmCategoryWithOutcome: prewarmCategoryWithOutcomeMock };
});

import { PER_RACE_SCOPE_REQUIRED_ERROR } from "./per-race-scope-guard";
import { handleQueue } from "./queue-consumer";

const ackMock = vi.fn();
const retryMock = vi.fn();
const sendMock = vi.fn();
const controlSendMock = vi.fn();
const runMock = vi.fn(async () => ({ success: true }));
const bindMock = vi.fn(() => ({ run: runMock }));
const prepareMock = vi.fn(() => ({ bind: bindMock }));
const idFromNameMock = vi.fn(() => ({ name: "test-id" }));
const stubFetchMock = vi.fn(
  async () =>
    new Response(
      JSON.stringify({ type: "result", racesPredicted: 5, category: "jra", status: "success" }),
      {
        status: 200,
      },
    ),
);
const getMock = vi.fn(() => ({ fetch: stubFetchMock }));

const makeEnv = (): Env => ({
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: { prepare: prepareMock } as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {
    get: getMock,
    idFromName: idFromNameMock,
  } as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  CONTAINER_CONTROL_QUEUE: {
    send: controlSendMock,
  } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
  PREDICT_QUEUE: { send: sendMock } as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
  REALTIME_DB: {} as unknown as D1Database,
  TRIGGER_TOKEN: "secret-token",
});

// Happy-path messages are per-race only: production rejects day-scoped bodies.
const DEFAULT_KEIBAJO_CODE = "05";
const DEFAULT_RACE_BANGO = "11";

const makeMessage = (
  overrides: Partial<PredictQueueMessage> = {},
  attempts = 3,
): Message<PredictQueueMessage> =>
  ({
    ack: ackMock,
    attempts,
    body: {
      category: "jra",
      daysAhead: 2,
      keibajoCode: DEFAULT_KEIBAJO_CODE,
      mode: "full",
      raceBango: DEFAULT_RACE_BANGO,
      runDate: "2026-06-03",
      runDateIso: "2026-06-03",
      runYmd: "20260603",
      ...overrides,
    } satisfies PredictQueueMessage,
    id: "predict-msg-1",
    retry: retryMock,
    timestamp: new Date("2026-07-12T01:00:01.000Z"),
  }) as unknown as Message<PredictQueueMessage>;

const makeDayScopedMessage = (
  overrides: Partial<PredictQueueMessage> = {},
): Message<PredictQueueMessage> =>
  makeMessage({
    ...overrides,
    keibajoCode: undefined,
    raceBango: undefined,
  });

const expectSkippedMissingPerRaceScope = (): void => {
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(idFromNameMock).not.toHaveBeenCalled();
  expect(getMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(parseNdjsonStreamMock).not.toHaveBeenCalled();
};

const makeBatch = (messages: Message<PredictQueueMessage>[]): MessageBatch<PredictQueueMessage> =>
  ({ messages }) as unknown as MessageBatch<PredictQueueMessage>;

beforeEach(() => {
  ackMock.mockClear();
  retryMock.mockClear();
  prewarmCategoryWithOutcomeMock.mockClear();
  prewarmCategoryWithOutcomeMock.mockResolvedValue("landed");
  sendMock.mockClear();
  sendMock.mockResolvedValue(undefined);
  controlSendMock.mockClear();
  controlSendMock.mockResolvedValue(undefined);
  runMock.mockClear();
  runMock.mockResolvedValue({ success: true });
  bindMock.mockClear();
  prepareMock.mockClear();
  idFromNameMock.mockClear();
  getMock.mockClear();
  stubFetchMock.mockReset();
  claimContainerSlotMock.mockClear();
  claimFocusedFullRaceMock.mockClear();
  claimRescoreExecutionMock.mockClear();
  claimRunMock.mockClear();
  clearContainerSlotMock.mockClear();
  releaseContainerSlotMock.mockClear();
  touchContainerSlotMock.mockClear();
  completeFocusedFullRaceMock.mockClear();
  completeRescoreRaceMock.mockClear();
  completeRunMock.mockClear();
  failFocusedFullRaceEnqueueMock.mockClear();
  isOldDateRunYmdMock.mockClear();
  isOldDateRunYmdMock.mockReturnValue(false);
  parseNdjsonStreamMock.mockClear();
  rescoreJraRaceMock.mockClear();
  warmPredictionCacheForRaceMock.mockClear();
  warmPredictionCacheForCategoryMock.mockClear();
  publishFinishPositionPredictionCacheMock.mockClear();
  publishFinishPositionPredictionCacheForCategoryMock.mockClear();
  isFocusedFullPredictionCompleteMock.mockClear();
  isPerRaceFeatureCachePresentMock.mockClear();
  isPerRaceRescoreReadyMock.mockClear();
  getRunningStyleRaceReadinessMock.mockClear();
  clearDayBaseRepairReservationMock.mockClear();
  enqueueDayBaseRepairOnceMock.mockClear();
  getFocusedFullDayBaseReadinessMock.mockClear();
  getFocusedFullDayBaseReadinessMock.mockResolvedValue({ ready: true, reason: "ready" });
  addRescoreAttestationToUrlMock.mockClear();
  addRescoreAttestationToUrlMock.mockImplementation((url: string) => url);
  createRescoreAttestationMock.mockClear();
  createRescoreAttestationMock.mockResolvedValue({
    attestationIssuedAtMs: 1_777_000_000_000,
    entryCount: 2,
    entrySetHash: "a".repeat(64),
    featureCacheEtag: "feature-etag",
    featureCacheVersion: "feature-version",
  });
  consumeDayBasePickupMock.mockClear();
  warmPredictionCacheForRaceMock.mockResolvedValue(true);
  warmPredictionCacheForCategoryMock.mockResolvedValue(0);
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: true,
    status: "written",
  });
  publishFinishPositionPredictionCacheForCategoryMock.mockResolvedValue(0);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(false);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(true);
  isPerRaceRescoreReadyMock.mockResolvedValue(true);
  rescoreJraRaceMock.mockResolvedValue({
    modelVersion: "jra-cb-v9-sim-2013-clean",
    predictionCount: 3,
    racesPredicted: 1,
    status: "ok",
  });
  claimRunMock.mockResolvedValue({ proceed: true });
  clearContainerSlotMock.mockResolvedValue(undefined);
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: true });
  failFocusedFullRaceEnqueueMock.mockResolvedValue(undefined);
  reserveFocusedFullRaceEnqueueMock.mockClear();
  reserveFocusedFullRaceEnqueueMock.mockResolvedValue({ proceed: true });
  claimRescoreExecutionMock.mockResolvedValue({ proceed: true });
  claimContainerSlotMock.mockResolvedValue({ proceed: true });
  releaseContainerSlotMock.mockResolvedValue(undefined);
  touchContainerSlotMock.mockResolvedValue(undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 5,
    category: "jra",
    status: "success",
  });
  stubFetchMock.mockResolvedValue(
    new Response(
      JSON.stringify({ type: "result", racesPredicted: 5, category: "jra", status: "success" }),
      {
        status: 200,
      },
    ),
  );
});

test("defers an RS-incomplete focused-full before any Container or coordinator claim", async () => {
  getRunningStyleRaceReadinessMock.mockResolvedValueOnce([
    {
      race: { category: "jra", keibajoCode: "05", raceBango: "11" },
      reason: "prediction-count-7-of-14",
    },
  ]);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const message = makeMessage({ skipDedup: true });

  await handleQueue(makeBatch([message]), makeEnv());

  expect(getRunningStyleRaceReadinessMock).toHaveBeenCalledWith({
    category: "jra",
    db: expect.anything(),
    races: [{ category: "jra", keibajoCode: "05", raceBango: "11" }],
    runYmd: "20260603",
  });
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(idFromNameMock).not.toHaveBeenCalled();
  expect(getMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full deferred before claim category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=true busyRequeueCount=0 keibajo=05 race=11 reason=running-style-prediction-count-7-of-14 attempts=3 delaySeconds=70",
  );
  warnSpy.mockRestore();
});

test("yields the focused-full lane only for an actual same-container owner conflict", async () => {
  claimContainerSlotMock.mockResolvedValueOnce({ proceed: false, state: "busy" });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );

  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.any(Object),
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260628",
    status: "error",
  });
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
});

test("defers a partial day-base, enqueues one repair, and never claims a Container", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "source-row-count-26-of-392",
  });
  enqueueDayBaseRepairOnceMock.mockResolvedValueOnce("enqueued");
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await handleQueue(makeBatch([makeMessage({ force: true, skipDedup: true }, 2)]), makeEnv());

  expect(getFocusedFullDayBaseReadinessMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.anything(),
    runYmd: "20260603",
  });
  expect(enqueueDayBaseRepairOnceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.anything(),
    runYmd: "20260603",
  });
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 50 });
  expect(getRunningStyleRaceReadinessMock).not.toHaveBeenCalled();
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full day-base deferred before claim category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=true busyRequeueCount=0 keibajo=05 race=11 reason=source-row-count-26-of-392 repair=enqueued attempts=2 delaySeconds=50",
  );
  warnSpy.mockRestore();
});

test("retries a second partial-day-base race without duplicate repair enqueue", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "rs-row-count-26-of-392",
  });
  enqueueDayBaseRepairOnceMock.mockResolvedValueOnce("already-enqueued");

  await handleQueue(makeBatch([makeMessage({ skipDedup: true }, 1)]), makeEnv());

  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(enqueueDayBaseRepairOnceMock).toHaveBeenCalledTimes(1);
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
});

test("fails closed without claims when canonical day-base readiness throws", async () => {
  getFocusedFullDayBaseReadinessMock.mockRejectedValueOnce(new Error("Catalog unavailable"));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await handleQueue(makeBatch([makeMessage({ skipDedup: true }, 3)]), makeEnv());

  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(enqueueDayBaseRepairOnceMock).not.toHaveBeenCalled();
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full day-base readiness failed before claim category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=true busyRequeueCount=0 keibajo=05 race=11:",
    "Error: Catalog unavailable",
  );
  errorSpy.mockRestore();
});

test("continues a ready race when stale repair reservation cleanup fails", async () => {
  clearDayBaseRepairReservationMock.mockRejectedValueOnce(new Error("D1 unavailable"));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), makeEnv());

  expect(claimFocusedFullRaceMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "[predict-queue] failed to clear day-base repair reservation category=jra runYmd=20260603:",
    "Error: D1 unavailable",
  );
  errorSpy.mockRestore();
});

test("fails closed before claims when focused-full readiness returns no race", async () => {
  getRunningStyleRaceReadinessMock.mockResolvedValueOnce([]);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await handleQueue(makeBatch([makeMessage({ skipDedup: true }, 1)]), makeEnv());

  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full deferred before claim category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=true busyRequeueCount=0 keibajo=05 race=11 reason=running-style-state-missing attempts=1 delaySeconds=30",
  );
  warnSpy.mockRestore();
});

test("retries without claims when focused-full readiness query fails", async () => {
  getRunningStyleRaceReadinessMock.mockRejectedValueOnce(new Error("D1 unavailable"));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await handleQueue(makeBatch([makeMessage({ skipDedup: true }, 2)]), makeEnv());

  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 50 });
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(idFromNameMock).not.toHaveBeenCalled();
  expect(getMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full running-style readiness failed before claim category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=true busyRequeueCount=0 keibajo=05 race=11:",
    "Error: D1 unavailable",
  );
  errorSpy.mockRestore();
});

test("acks day-scoped messages without container fetch or claimRun", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  try {
    await handleQueue(makeBatch([makeDayScopedMessage()]), makeEnv());
    expectSkippedMissingPerRaceScope();
    expect(warnSpy).toHaveBeenCalledWith(
      `Skipping day-scoped predict message category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=false busyRequeueCount=0: ${PER_RACE_SCOPE_REQUIRED_ERROR}`,
    );
  } finally {
    warnSpy.mockRestore();
  }
});

test("acks keibajo-only partial scope without container fetch or claimRun", async () => {
  await handleQueue(makeBatch([makeMessage({ raceBango: undefined })]), makeEnv());
  expectSkippedMissingPerRaceScope();
});

test("acks race-only partial scope without container fetch or claimRun", async () => {
  await handleQueue(makeBatch([makeMessage({ keibajoCode: undefined })]), makeEnv());
  expectSkippedMissingPerRaceScope();
});

test("valid per-race message still claims, fetches the container, and acks", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "05",
        raceBango: "11",
        runYmd: "20260603",
      }),
    ]),
    makeEnv(),
  );
  expect(claimRunMock).toHaveBeenCalledWith(
    expect.objectContaining({ category: "jra", runYmd: "20260603" }),
  );
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=2&mode=full&runDate=20260603&keibajoCode=05&raceBango=11",
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
});

test("skips and acks when claimRun returns proceed:false", async () => {
  claimRunMock.mockResolvedValue({ proceed: false, state: "started" });
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).not.toHaveBeenCalled();
});

test("calls claimRun with correct params when processing", async () => {
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(claimRunMock).toHaveBeenCalledWith(
    expect.objectContaining({ category: "jra", runYmd: "20260603" }),
  );
});

test("calls stub.fetch with correct URL including mode=full using YYYYMMDD runDate", async () => {
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(idFromNameMock).toHaveBeenCalledWith("predict-jra");
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=2&mode=full&runDate=20260603&keibajoCode=05&raceBango=11",
  );
});

test("uses a stable category-scoped DO name for focused per-race full skipDedup messages", async () => {
  const randomUuidSpy = vi
    .spyOn(crypto, "randomUUID")
    .mockReturnValue("00000000-0000-4000-8000-000000000001");
  try {
    await handleQueue(
      makeBatch([
        makeMessage({
          daysAhead: 0,
          keibajoCode: "02",
          mode: "full",
          raceBango: "01",
          runYmd: "20260628",
          skipDedup: true,
        }),
      ]),
      makeEnv(),
    );
    expect(stubFetchMock).toHaveBeenCalledTimes(2);
    const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
    expect(fetchRequest.url).toBe(
      "http://do/predict?category=jra&daysAhead=0&mode=full&runDate=20260628&keibajoCode=02&raceBango=01",
    );
    expect(idFromNameMock).toHaveBeenCalledWith("predict-jra");
    expect(randomUuidSpy).not.toHaveBeenCalled();
    expect(claimRunMock).not.toHaveBeenCalled();
    expect(completeRunMock).not.toHaveBeenCalled();
    expect(ackMock).toHaveBeenCalledTimes(1);
  } finally {
    randomUuidSpy.mockRestore();
  }
});

test("keeps focused full detached when Worker debug logging is enabled", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        debug: true,
        keibajoCode: "07",
        mode: "full",
        raceBango: "11",
        runYmd: "20260822",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=2&mode=full&runDate=20260822&keibajoCode=07&raceBango=11",
  );
});

test("targets a race-sharded DO for a focused per-race full skipDedup message when RACE_SHARDED_DO is enabled", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );
  expect(idFromNameMock).toHaveBeenCalledWith("predict-jra-1");
});

test("routes only an allowlisted focused-full R2 day-base hit to the race-chain binding", async () => {
  const raceIdFromName = vi.fn(() => ({ name: "race-chain-id" }));
  const raceGet = vi.fn(() => ({ fetch: stubFetchMock }));
  const head = vi.fn(async () => ({
    customMetadata: {
      "max-data-sakusei-nengappi": "20260823090000",
      "row-count": "12",
      "rs-predicted-at-max": "20260823090500",
      "rs-row-count": "12",
    },
  }));
  const env: Env = {
    ...makeEnv(),
    FEATURES_CACHE: { head } as unknown as R2Bucket,
    FINISH_POSITION_RACE_CHAIN_CONTAINER: {
      get: raceGet,
      idFromName: raceIdFromName,
    } as unknown as NonNullable<Env["FINISH_POSITION_RACE_CHAIN_CONTAINER"]>,
    RACE_CHAIN_CONTAINER_CATEGORIES: "jra",
    RACE_CHAIN_CONTAINER_ENABLED: "1",
  };

  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "01",
        mode: "full",
        raceBango: "03",
        runYmd: "20260823",
        skipDedup: true,
      }),
    ]),
    env,
  );

  expect(head).toHaveBeenCalledWith("feat-daybase/catalog-v1/jra/20260823/features.parquet");
  expect(raceIdFromName).toHaveBeenCalledWith("race-chain-predict-jra");
  expect(raceGet).toHaveBeenCalledTimes(2);
  expect(idFromNameMock).not.toHaveBeenCalled();
});

test("hands DAY_BASE_REQUIRED from race-chain to one durable legacy replacement", async () => {
  parseNdjsonStreamMock.mockResolvedValueOnce({
    category: "jra",
    error: "DayBaseRequiredError: DAY_BASE_REQUIRED: source unavailable",
    racesPredicted: 0,
    status: "error",
    type: "result",
  });
  const raceIdFromName = vi.fn(() => ({ name: "race-chain-id" }));
  const raceGet = vi.fn(() => ({ fetch: stubFetchMock }));
  const env: Env = {
    ...makeEnv(),
    FEATURES_CACHE: {
      head: vi.fn(async () => ({
        customMetadata: {
          "max-data-sakusei-nengappi": "20260823090000",
          "row-count": "12",
          "rs-predicted-at-max": "20260823090500",
          "rs-row-count": "12",
        },
      })),
    } as unknown as R2Bucket,
    FINISH_POSITION_RACE_CHAIN_CONTAINER: {
      get: raceGet,
      idFromName: raceIdFromName,
    } as unknown as NonNullable<Env["FINISH_POSITION_RACE_CHAIN_CONTAINER"]>,
    RACE_CHAIN_CONTAINER_CATEGORIES: "jra",
    RACE_CHAIN_CONTAINER_ENABLED: "1",
  };
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const original = makeMessage({ skipDedup: true });

  await handleQueue(makeBatch([original]), env);

  expect(sendMock).toHaveBeenCalledWith(
    { ...original.body, forceLegacyContainer: true },
    { delaySeconds: 30 },
  );
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] race-chain requested legacy fallback category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=true busyRequeueCount=0 keibajo=05 race=11 delaySeconds=30",
  );
  warnSpy.mockRestore();
});

test("retries the original race-chain delivery when legacy replacement enqueue fails", async () => {
  parseNdjsonStreamMock.mockResolvedValueOnce({
    category: "jra",
    error: "DAY_BASE_REQUIRED: transient source failure",
    racesPredicted: 0,
    status: "error",
    type: "result",
  });
  sendMock.mockRejectedValueOnce(new Error("Queue unavailable"));
  const env: Env = {
    ...makeEnv(),
    FEATURES_CACHE: {
      head: vi.fn(async () => ({
        customMetadata: {
          "max-data-sakusei-nengappi": "20260823090000",
          "row-count": "12",
          "rs-predicted-at-max": "20260823090500",
          "rs-row-count": "12",
        },
      })),
    } as unknown as R2Bucket,
    FINISH_POSITION_RACE_CHAIN_CONTAINER: {
      get: vi.fn(() => ({ fetch: stubFetchMock })),
      idFromName: vi.fn(() => ({ name: "race-chain-id" })),
    } as unknown as NonNullable<Env["FINISH_POSITION_RACE_CHAIN_CONTAINER"]>,
    RACE_CHAIN_CONTAINER_CATEGORIES: "jra",
    RACE_CHAIN_CONTAINER_ENABLED: "1",
  };
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), env);

  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  errorSpy.mockRestore();
});

test("threads cardMaxRaceBango into a Kochi focused-full skipDedup query URL", async () => {
  const realtimeBindMock = vi.fn(() => ({
    first: vi.fn(async () => ({ max_race_bango: 10 })),
  }));
  const env: Env = {
    ...makeEnv(),
    REALTIME_DB: { prepare: vi.fn(() => ({ bind: realtimeBindMock })) } as unknown as D1Database,
  };
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "54",
        mode: "full",
        raceBango: "10",
        runYmd: "20260712",
        skipDedup: true,
      }),
    ]),
    env,
  );
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=nar&daysAhead=0&mode=full&runDate=20260712&keibajoCode=54&raceBango=10&cardMaxRaceBango=10",
  );
});

test("acks focused full skipDedup messages without container when Neon already has all rows", async () => {
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        keibajoCode: "50",
        mode: "full",
        raceBango: "12",
        runYmd: "20260701",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(isFocusedFullPredictionCompleteMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "nar",
      keibajoCode: "50",
      raceBango: "12",
      runYmd: "20260701",
    }),
  );
  // No full /predict re-run -- but exactly ONE fetch to the container's
  // GET /focused-full-cache pickup endpoint, since Neon completion means a
  // prior detached focused-full run may have left an unpicked R2 payload.
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  const pickupRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(pickupRequest.url).toBe(
    "http://do/focused-full-cache?category=nar&runDate=20260701&keibajoCode=50&raceBango=12",
  );
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledWith({
    day: "01",
    keibajoCode: "50",
    month: "07",
    raceNumber: "12",
    refresh: true,
    year: "2026",
  });
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledWith({
    bustCacheApi: true,
    category: "nar",
    env: expect.anything(),
    keibajoCode: "50",
    raceBango: "12",
    runYmd: "20260701",
  });
  expect(
    (publishFinishPositionPredictionCacheMock.mock.invocationCallOrder[0] ?? 0) <
      (warmPredictionCacheForRaceMock.mock.invocationCallOrder[0] ?? 0),
  ).toBe(true);
});

test("forces focused-full detail refresh when viewer cache bust is unavailable", async () => {
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    status: "written",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        keibajoCode: "50",
        mode: "full",
        raceBango: "12",
        runYmd: "20260701",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledWith({
    day: "01",
    keibajoCode: "50",
    month: "07",
    raceNumber: "12",
    refresh: true,
    year: "2026",
  });
});

test("does not acknowledge Neon-complete focused full until its per-race R2 cache exists", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  stubFetchMock.mockResolvedValueOnce(Response.json({ found: false })).mockResolvedValueOnce(
    Response.json({
      error: null,
      raceKey: "jra:20260822:01:06",
      status: "running",
    }),
  );

  await handleQueue(
    makeBatch([
      makeMessage({
        force: true,
        keibajoCode: "01",
        mode: "full",
        raceBango: "06",
        runYmd: "20260822",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );

  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(claimContainerSlotMock).toHaveBeenCalledWith(
    expect.objectContaining({
      allowSameOwner: true,
      workKey: "focused-full:20260822:jra:01:06",
    }),
  );
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(touchContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-jra",
    env: expect.anything(),
    staleAfterMs: 31 * 60 * 1000,
    workKey: "focused-full:20260822:jra:01:06",
  });
  expect(sendMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "Focused full cache still missing category=jra runYmd=20260822 keibajo=01 race=06 -- keeping completion message for recovery",
  );
  warnSpy.mockRestore();
});

test("short-retries Neon-complete cache pickup when detached status is success", async () => {
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  stubFetchMock.mockResolvedValueOnce(Response.json({ found: false })).mockResolvedValueOnce(
    Response.json({
      error: null,
      raceKey: "jra:20260822:01:06",
      status: "success",
    }),
  );

  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "01",
        mode: "full",
        raceBango: "06",
        runYmd: "20260822",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );

  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
});

test("replaces missing detached payload with one forced cache-repair message", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  stubFetchMock.mockResolvedValueOnce(Response.json({ found: false })).mockResolvedValueOnce(
    Response.json({
      error: null,
      raceKey: "jra:20260822:01:06",
      status: "missing",
    }),
  );
  const body = {
    keibajoCode: "01",
    mode: "full" as const,
    raceBango: "06",
    raceStartAtJst: "2026-08-22T10:00:00+09:00",
    runYmd: "20260822",
    skipDedup: true,
  };

  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());

  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra",
      type: "container-stop",
      workKey: "focused-full:20260822:jra:01:06",
    }),
  );
  expect(reserveFocusedFullRaceEnqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({
      keibajoCode: "01",
      raceBango: "06",
      raceStartAtJst: "2026-08-22T10:00:00+09:00",
      runYmd: "20260822",
      staleAfterMs: 31 * 60 * 1000,
    }),
  );
  expect(sendMock).toHaveBeenCalledWith(expect.objectContaining({ ...body, force: true }), {
    delaySeconds: 30,
  });
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("acks a missing-payload delivery when its cache repair is already reserved", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  reserveFocusedFullRaceEnqueueMock.mockResolvedValue({ proceed: false, state: "enqueued" });
  stubFetchMock.mockResolvedValueOnce(Response.json({ found: false })).mockResolvedValueOnce(
    Response.json({
      error: "lost payload",
      raceKey: "jra:20260822:01:06",
      status: "error",
    }),
  );

  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "01",
        mode: "full",
        raceBango: "06",
        runYmd: "20260822",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );

  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("retries the current message and releases its reservation when cache-repair send fails", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  sendMock.mockRejectedValueOnce(new Error("queue unavailable"));
  stubFetchMock.mockResolvedValueOnce(Response.json({ found: false })).mockResolvedValueOnce(
    Response.json({
      error: null,
      raceKey: "jra:20260822:01:06",
      status: "missing",
    }),
  );

  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "01",
        mode: "full",
        raceBango: "06",
        runYmd: "20260822",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );

  expect(failFocusedFullRaceEnqueueMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: "01", raceBango: "06", runYmd: "20260822" }),
  );
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("keeps a Neon-complete cache miss queued when detached status cannot be queried", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  stubFetchMock
    .mockResolvedValueOnce(Response.json({ found: false }))
    .mockResolvedValueOnce(new Response("unavailable", { status: 503 }));

  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "01",
        mode: "full",
        raceBango: "06",
        runYmd: "20260822",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );

  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("does not warm the viewer display when focused-full KV publish is empty", async () => {
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    status: "skipped-empty",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        keibajoCode: "50",
        mode: "full",
        raceBango: "12",
        runYmd: "20260701",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledTimes(1);
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
});

test("does not warm the viewer display when focused-full KV publish returns error", async () => {
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    status: "error",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        keibajoCode: "50",
        mode: "full",
        raceBango: "12",
        runYmd: "20260701",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
});

test("awaits viewer display warm after a successful focused-full KV write", async () => {
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  let releaseWarm!: () => void;
  const warmGate = new Promise<void>((resolve) => {
    releaseWarm = resolve;
  });
  let warmStarted!: () => void;
  const warmStartedPromise = new Promise<void>((resolve) => {
    warmStarted = resolve;
  });
  warmPredictionCacheForRaceMock.mockImplementation(async () => {
    warmStarted();
    await warmGate;
    return true;
  });
  let handlerDone = false;
  const running = handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        keibajoCode: "50",
        mode: "full",
        raceBango: "12",
        runYmd: "20260701",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  ).then(() => {
    handlerDone = true;
  });
  await warmStartedPromise;
  expect(ackMock).not.toHaveBeenCalled();
  expect(handlerDone).toBe(false);
  releaseWarm();
  await running;
  expect(handlerDone).toBe(true);
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("force:true bypasses the focused full completion guard and reaches the Container DO", async () => {
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          category: "jra",
          daysAhead: 0,
          force: true,
          forceRequestedAt: "2026-07-12T01:00:00.000Z",
          keibajoCode: "02",
          mode: "full",
          raceBango: "01",
          runYmd: "20260712",
          skipDedup: true,
        },
        1,
      ),
    ]),
    makeEnv(),
  );
  expect(isFocusedFullPredictionCompleteMock).not.toHaveBeenCalled();
  expect(stubFetchMock).toHaveBeenCalledTimes(2);
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=0&mode=full&runDate=20260712&keibajoCode=02&raceBango=01&force=1",
  );
  expect(claimFocusedFullRaceMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(claimFocusedFullRaceMock).toHaveBeenCalledWith(expect.objectContaining({ force: true }));
});

test("consumes force after the first delivery and picks up completed focused-full cache", async () => {
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          category: "jra",
          daysAhead: 0,
          force: true,
          forceRequestedAt: "2026-07-12T01:00:00.000Z",
          keibajoCode: "02",
          mode: "full",
          raceBango: "01",
          runYmd: "20260712",
          skipDedup: true,
        },
        2,
      ),
    ]),
    makeEnv(),
  );

  expect(isFocusedFullPredictionCompleteMock).toHaveBeenCalledTimes(1);
  expect(isFocusedFullPredictionCompleteMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.anything(),
    keibajoCode: "02",
    notBefore: "2026-07-12T01:00:00.000Z",
    raceBango: "01",
    runYmd: "20260712",
  });
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  const pickupRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(pickupRequest.url).toBe(
    "http://do/focused-full-cache?category=jra&runDate=20260712&keibajoCode=02&raceBango=01",
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("does not forward force to the Container on a redelivery when completion is not visible", async () => {
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          category: "jra",
          daysAhead: 0,
          force: true,
          keibajoCode: "02",
          mode: "full",
          raceBango: "01",
          runYmd: "20260712",
          skipDedup: true,
        },
        2,
      ),
    ]),
    makeEnv(),
  );

  expect(isFocusedFullPredictionCompleteMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.anything(),
    keibajoCode: "02",
    notBefore: "2026-07-12T01:00:01.000Z",
    raceBango: "01",
    runYmd: "20260712",
  });
  expect(claimFocusedFullRaceMock).toHaveBeenCalledWith(expect.objectContaining({ force: false }));
  const predictRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(predictRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=0&mode=full&runDate=20260712&keibajoCode=02&raceBango=01",
  );
});

test("focused full skipDedup message without force passes force:false to the DO claim", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "jra",
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260712",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(claimFocusedFullRaceMock).toHaveBeenCalledWith(expect.objectContaining({ force: false }));
});

test("continues to container when focused full completion guard fails", async () => {
  const consoleWarn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isFocusedFullPredictionCompleteMock.mockRejectedValue(new Error("neon unavailable"));
  try {
    await handleQueue(
      makeBatch([
        makeMessage({
          category: "nar",
          keibajoCode: "50",
          mode: "full",
          raceBango: "12",
          runYmd: "20260701",
          skipDedup: true,
        }),
      ]),
      makeEnv(),
    );
    expect(stubFetchMock).toHaveBeenCalledTimes(2);
    expect(ackMock).toHaveBeenCalledTimes(1);
    expect(consoleWarn).toHaveBeenCalledWith(
      "Focused full completion guard failed category=nar runYmd=20260701 keibajo=50 race=12:",
      "Error: neon unavailable",
    );
  } finally {
    consoleWarn.mockRestore();
  }
});

test("ignores requestId in the DO name for focused per-race full skipDedup messages", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 2,
        keibajoCode: "35",
        mode: "full",
        raceBango: "01",
        requestId: "request-123",
        runYmd: "20260629",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).toHaveBeenCalledTimes(2);
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=nar&daysAhead=2&mode=full&runDate=20260629&keibajoCode=35&raceBango=01",
  );
  expect(idFromNameMock).toHaveBeenCalledWith("predict-nar");
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(completeRunMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("polls and touches an active focused run without restarting its prediction", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: true, state: "resumed" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      deadlineAtMs: 2000,
      error: null,
      finishedAtMs: null,
      lastProgressAtMs: 1500,
      raceKey: "jra:20260628:02:01",
      startedAtMs: 1000,
      status: "running",
    }),
  );
  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  const statusRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(statusRequest.url).toBe(
    "http://do/focused-full-status?category=jra&keibajoCode=02&raceBango=01&runDate=20260628",
  );
  expect(parseNdjsonStreamMock).not.toHaveBeenCalled();
  expect(touchContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-jra",
    env: expect.anything(),
    staleAfterMs: 1_860_000,
    workKey: "focused-full:20260628:jra:02:01",
  });
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).not.toHaveBeenCalled();
  consoleSpy.mockRestore();
});

test("retries the same queued focused-full waiter without probing Python", async () => {
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: false, state: "queued" });
  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
});

test("reuses the category-scoped DO across multiple focused per-race full messages", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 2,
        keibajoCode: "35",
        mode: "full",
        raceBango: "01",
        runYmd: "20260629",
        skipDedup: true,
      }),
      makeMessage({
        category: "nar",
        daysAhead: 2,
        keibajoCode: "35",
        mode: "full",
        raceBango: "02",
        runYmd: "20260629",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(idFromNameMock).toHaveBeenCalledTimes(4);
  expect(idFromNameMock).toHaveBeenNthCalledWith(1, "predict-nar");
  expect(idFromNameMock).toHaveBeenNthCalledWith(2, "predict-nar");
  const firstRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  const secondRequest = (stubFetchMock.mock.calls[2] as unknown as [Request])[0];
  expect(firstRequest.url).toContain("raceBango=01");
  expect(secondRequest.url).toContain("raceBango=02");
  expect(ackMock).toHaveBeenCalledTimes(2);
});

test("keeps focused per-race full skipDedup messages on the category DO even when requestId is present", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        keibajoCode: "35",
        mode: "full",
        raceBango: "01",
        requestId: "request-123",
        runYmd: "20260629",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(idFromNameMock).toHaveBeenCalledWith("predict-nar");
  expect(completeRunMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("acks day-scoped skipDedup full messages without reaching the container", async () => {
  await handleQueue(
    makeBatch([
      makeDayScopedMessage({
        category: "nar",
        mode: "full",
        requestId: "request-123",
        runYmd: "20260629",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expectSkippedMissingPerRaceScope();
});

test("calls stub.fetch with mode=rescore when message has mode rescore using YYYYMMDD", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(idFromNameMock).toHaveBeenCalledWith("predict-jra");
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=0&mode=rescore&keibajoCode=05&raceBango=11&runDate=20260619",
  );
});

test("calls completeRun with success and acks on explicit result status success", async () => {
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(completeRunMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success", racesPredicted: 5 }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});

test("accepts legacy result lines without status for backward compatibility", async () => {
  parseNdjsonStreamMock.mockResolvedValue({ type: "result", racesPredicted: 4, category: "jra" });
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(completeRunMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success", racesPredicted: 4 }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});

test("logs container progress for per-race predict messages when debug is enabled", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockImplementationOnce(
    async (
      _body: ReadableStream<Uint8Array>,
      options?: ParseNdjsonStreamOptions,
    ): Promise<PredictResultLine> => {
      options?.onProgress?.({ type: "progress", stage: "predict", elapsed_s: 12.3 });
      options?.onProgress?.({ type: "progress" });
      return { type: "result", racesPredicted: 5, category: "jra", status: "success" };
    },
  );
  await handleQueue(makeBatch([makeMessage({ debug: true })]), makeEnv());
  expect(consoleSpy).toHaveBeenCalledWith(
    "Predict progress category=jra runYmd=20260603 keibajo=05 race=11 stage=predict elapsed=12.3",
  );
  expect(consoleSpy).toHaveBeenCalledWith(
    "Predict progress category=jra runYmd=20260603 keibajo=05 race=11 stage=- elapsed=-",
  );
  consoleSpy.mockRestore();
});

test("does not log container progress for normal predict messages when debug is off", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockImplementationOnce(
    async (
      _body: ReadableStream<Uint8Array>,
      options?: ParseNdjsonStreamOptions,
    ): Promise<PredictResultLine> => {
      options?.onProgress?.({ type: "progress", stage: "predict", elapsed_s: 12.3 });
      return { type: "result", racesPredicted: 5, category: "jra", status: "success" };
    },
  );
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(consoleSpy).not.toHaveBeenCalledWith(
    "Predict progress category=jra runYmd=20260603 keibajo=05 race=11 stage=predict elapsed=12.3",
  );
  consoleSpy.mockRestore();
});

test("marks the run failed and retries when final result status is error", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 2,
    category: "jra",
    status: "error",
    error: "ValueError: missing feature parquet",
  });
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(completeRunMock).toHaveBeenCalledTimes(1);
  expect(completeRunMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error", racesPredicted: 0 }),
  );
  expect(completeRunMock).not.toHaveBeenCalledWith(expect.objectContaining({ status: "success" }));
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(ackMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("calls completeRun with error and calls message.retry on failure", async () => {
  stubFetchMock.mockRejectedValue(new Error("network timeout"));
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(completeRunMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error", racesPredicted: 0 }),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(ackMock).not.toHaveBeenCalled();
});

test("consumes a delayed day-base pickup without claiming a run or starting a predict", async () => {
  const pickup = {
    ack: ackMock,
    body: {
      attempt: 1,
      category: "ban-ei",
      runYmd: "20260817",
      type: "day-base-pickup",
    },
    retry: retryMock,
  };
  await handleQueue({ messages: [pickup] } as never, makeEnv());
  expect(consumeDayBasePickupMock).toHaveBeenCalledWith({
    env: expect.any(Object),
    message: pickup.body,
  });
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
});

test("acks old non-force day-base pickup work and releases its repair and container ownership", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  const pickup = {
    ack: ackMock,
    body: {
      attempt: 9,
      category: "nar",
      runYmd: "20260823",
      type: "day-base-pickup",
    },
    retry: retryMock,
  };
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await handleQueue({ messages: [pickup] } as never, makeEnv());
  expect(consumeDayBasePickupMock).not.toHaveBeenCalled();
  expect(clearDayBaseRepairReservationMock).toHaveBeenCalledWith({
    category: "nar",
    env: expect.any(Object),
    runYmd: "20260823",
  });
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-nar",
      role: "legacy",
      type: "container-stop",
      workKey: "day-base:20260823:nar",
    }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("allows an explicitly forced historical day-base pickup", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  const pickup = {
    ack: ackMock,
    body: {
      attempt: 1,
      category: "jra",
      force: true,
      runYmd: "20260823",
      type: "day-base-pickup",
    },
    retry: retryMock,
  };
  await handleQueue({ messages: [pickup] } as never, makeEnv());
  expect(consumeDayBasePickupMock).toHaveBeenCalledWith({
    env: expect.any(Object),
    message: pickup.body,
  });
  expect(clearDayBaseRepairReservationMock).not.toHaveBeenCalled();
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("acks old day-base work even when its best-effort cleanup bindings fail", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  clearDayBaseRepairReservationMock.mockRejectedValueOnce(new Error("D1 unavailable"));
  controlSendMock.mockRejectedValueOnce(new Error("control queue unavailable"));
  sendMock.mockRejectedValueOnce(new Error("cleanup queue unavailable"));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const pickup = {
    ack: ackMock,
    body: {
      attempt: 9,
      category: "nar",
      runYmd: "20260823",
      type: "day-base-pickup",
    },
    retry: retryMock,
  };
  await handleQueue({ messages: [pickup] } as never, makeEnv());
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    expect.stringContaining("old day-base repair reservation cleanup failed"),
    "Error: D1 unavailable",
  );
  expect(errorSpy).toHaveBeenCalledWith(
    expect.stringContaining("old day-base container cleanup failed"),
    "Error: cleanup queue unavailable",
  );
  errorSpy.mockRestore();
  warnSpy.mockRestore();
});

test("consumes a delivery canary without claiming a run or starting a container", async () => {
  const canary = {
    ack: ackMock,
    body: {
      enqueuedAt: "2026-08-15T00:00:00Z",
      id: "canary-id",
      type: "delivery-canary",
    },
    retry: retryMock,
  };
  await handleQueue({ messages: [canary] } as never, makeEnv());
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
});

test("processes multiple messages in batch", async () => {
  const msg1 = makeMessage({ category: "jra" });
  const msg2 = makeMessage({ category: "nar" });
  const msg3 = makeMessage({ category: "ban-ei" });
  await handleQueue(makeBatch([msg1, msg2, msg3]), makeEnv());
  expect(stubFetchMock).toHaveBeenCalledTimes(3);
  expect(ackMock).toHaveBeenCalledTimes(3);
});

test("processes batch messages sequentially", async () => {
  let resolveFirstClaim!: (value: ClaimResult) => void;
  const firstClaim = new Promise<ClaimResult>((resolve) => {
    resolveFirstClaim = resolve;
  });
  claimRunMock.mockImplementationOnce(() => firstClaim);
  const processing = handleQueue(
    makeBatch([
      makeMessage({ category: "jra" }),
      makeMessage({ category: "nar" }),
      makeMessage({ category: "ban-ei" }),
    ]),
    makeEnv(),
  );
  await vi.waitFor(() => {
    expect(claimRunMock).toHaveBeenCalledTimes(1);
  });
  expect(stubFetchMock).not.toHaveBeenCalled();

  resolveFirstClaim({ proceed: true });
  await processing;
  expect(claimRunMock).toHaveBeenCalledTimes(3);
  expect(stubFetchMock).toHaveBeenCalledTimes(3);
  expect(ackMock).toHaveBeenCalledTimes(3);
});

test("calls completeRun with error and retries when response.body is null", async () => {
  stubFetchMock.mockResolvedValue(new Response(null, { status: 200 }));
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(completeRunMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error", racesPredicted: 0 }),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(ackMock).not.toHaveBeenCalled();
});

test("calls completeRun with error and retries when container DO returns 502", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockResolvedValue(
    Response.json({ error: "Container start failed", detail: "timeout" }, { status: 502 }),
  );
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(completeRunMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error", racesPredicted: 0 }),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("defers a per-race rescore until the initial prediction is complete", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isPerRaceRescoreReadyMock.mockResolvedValueOnce(false);
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(isPerRaceRescoreReadyMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.anything(),
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(warnSpy).toHaveBeenCalledWith(
    "Rescore deferred category=jra runYmd=20260619 keibajo=05 race=11 reason=initial-prediction-or-cache-incomplete attempts=3 delaySeconds=70",
  );
  warnSpy.mockRestore();
});

test("retries a per-race rescore when the initial-prediction readiness check fails", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  isPerRaceRescoreReadyMock.mockRejectedValueOnce(new Error("neon unavailable"));
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(errorSpy).toHaveBeenCalledWith(
    "Rescore readiness check failed category=jra runYmd=20260619 keibajo=05 race=11:",
    "Error: neon unavailable",
  );
  errorSpy.mockRestore();
});

test("persists a retry-error row before retrying a container 502", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockResolvedValue(
    Response.json({ error: "Container start failed", detail: "timeout" }, { status: 502 }),
  );
  await handleQueue(makeBatch([makeMessage({ keibajoCode: "83", raceBango: "06" })]), makeEnv());
  expect(prepareMock).toHaveBeenCalledWith(
    expect.stringContaining("insert into finish_position_predict_retry_errors"),
  );
  expect(bindMock).toHaveBeenCalledWith(
    "predict-msg-1",
    "20260603",
    "jra",
    "full",
    "83",
    "06",
    "Error",
    'Container DO returned 502: {"error":"Container start failed","detail":"timeout"}',
    expect.stringContaining("Container DO returned 502:"),
    502,
    '{"error":"Container start failed","detail":"timeout"}',
    3,
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  errorSpy.mockRestore();
});

test("still retries when persisting the retry-error row fails", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockRejectedValue(new Error("network timeout"));
  runMock.mockRejectedValue(new Error("d1 unavailable"));
  await handleQueue(makeBatch([makeMessage({}, 100)]), makeEnv());
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 300 });
  expect(ackMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    expect.stringContaining("[predict-queue] failed to persist retry error"),
    "Error: d1 unavailable",
  );
  errorSpy.mockRestore();
});

test("routes a JRA per-race rescore to the container held /predict", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(rescoreJraRaceMock).not.toHaveBeenCalled();
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).toHaveBeenCalledWith(
    new Request(
      "http://do/predict?category=jra&daysAhead=0&mode=rescore&keibajoCode=05&raceBango=11&runDate=20260619",
    ),
  );
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("retries without Container dispatch when fresh rescore attestation fails", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  createRescoreAttestationMock.mockRejectedValue(new Error("foundation entry-set hash mismatch"));
  const body: Partial<PredictQueueMessage> = {
    daysAhead: 0,
    keibajoCode: "05",
    mode: "rescore",
    raceBango: "11",
    runYmd: "20260619",
  };

  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());

  expect(createRescoreAttestationMock).toHaveBeenCalledTimes(1);
  expect(createRescoreAttestationMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "jra",
      keibajoCode: "05",
      raceBango: "11",
      runYmd: "20260619",
    }),
  );
  expect(addRescoreAttestationToUrlMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(releaseContainerSlotMock).toHaveBeenCalledTimes(1);
  errorSpy.mockRestore();
});

test("obtains a new rescore attestation on every Queue delivery", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const body: Partial<PredictQueueMessage> = {
    daysAhead: 0,
    keibajoCode: "05",
    mode: "rescore",
    raceBango: "11",
    runYmd: "20260619",
  };

  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());
  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());

  expect(createRescoreAttestationMock).toHaveBeenCalledTimes(2);
  expect(addRescoreAttestationToUrlMock).toHaveBeenCalledTimes(2);
  expect(stubFetchMock).toHaveBeenCalledTimes(2);
  consoleSpy.mockRestore();
});

test("acks a JRA container per-race rescore with zero races without retrying", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "jra",
    status: "success",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(rescoreJraRaceMock).not.toHaveBeenCalled();
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  consoleSpy.mockRestore();
});

test("runs an enabled JRA weight rescore entirely in the Worker", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    { ...makeEnv(), JRA_WORKER_RESCORE_ENABLED: "1" },
  );
  expect(rescoreJraRaceMock).toHaveBeenCalledTimes(1);
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledTimes(1);
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("falls back to the Container when enabled Worker rescore has no feature cache", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  rescoreJraRaceMock.mockResolvedValue({
    modelVersion: null,
    predictionCount: 0,
    racesPredicted: 0,
    status: "cache_miss",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    { ...makeEnv(), JRA_WORKER_RESCORE_ENABLED: "1" },
  );
  expect(rescoreJraRaceMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(completeRescoreRaceMock).toHaveBeenCalledTimes(2);
  expect(ackMock).toHaveBeenCalledTimes(1);
  warnSpy.mockRestore();
  consoleSpy.mockRestore();
});

test("acks an enabled Worker rescore whose semantic execution already succeeded", async () => {
  claimRescoreExecutionMock.mockResolvedValue({ proceed: false, state: "success" });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    { ...makeEnv(), JRA_WORKER_RESCORE_ENABLED: "1" },
  );
  expect(rescoreJraRaceMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});

test("retries an enabled Worker rescore while another semantic execution is active", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  claimRescoreExecutionMock.mockResolvedValue({ proceed: false, state: "started" });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    { ...makeEnv(), JRA_WORKER_RESCORE_ENABLED: "1" },
  );
  expect(rescoreJraRaceMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("retries an enabled Worker rescore when its semantic claim fails", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  claimRescoreExecutionMock.mockRejectedValue(new Error("coordinator unavailable"));
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    { ...makeEnv(), JRA_WORKER_RESCORE_ENABLED: "1" },
  );
  expect(rescoreJraRaceMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("keeps an enabled Worker rescore committed when viewer warm fails", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  warmPredictionCacheForRaceMock.mockRejectedValue(new Error("viewer unavailable"));
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    { ...makeEnv(), JRA_WORKER_RESCORE_ENABLED: "1" },
  );
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  errorSpy.mockRestore();
  consoleSpy.mockRestore();
});

test("retries a JRA container per-race rescore when the container fetch throws", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockRejectedValue(new Error("container down"));
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(rescoreJraRaceMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    expect.stringMatching(
      /^Container per-race rescore failed category=jra runYmd=20260619 keibajo=05 race=11 durationMs=\d+:$/u,
    ),
    "Error: container down",
  );
  errorSpy.mockRestore();
});

test("routes a NAR per-race rescore to the container held /predict (not Worker-native)", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(rescoreJraRaceMock).not.toHaveBeenCalled();
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("targets the per-race rescore at a category-scoped predict-nar DO with the exact query URL", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(idFromNameMock).toHaveBeenCalledWith("predict-nar");
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=nar&daysAhead=0&mode=rescore&keibajoCode=44&raceBango=01&runDate=20260619",
  );
  consoleSpy.mockRestore();
});

test("targets a bounded race shard for per-race rescore when RACE_SHARDED_DO is enabled", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );
  expect(idFromNameMock).toHaveBeenCalledWith("predict-nar-2");
  expect(claimContainerSlotMock).toHaveBeenCalledWith(
    expect.objectContaining({
      doName: "predict-nar-2",
      kind: "rescore",
    }),
  );
  consoleSpy.mockRestore();
});

test("threads cardMaxRaceBango into a Kochi per-race rescore query URL", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const realtimeBindMock = vi.fn(() => ({
    first: vi.fn(async () => ({ max_race_bango: 10 })),
  }));
  const env: Env = {
    ...makeEnv(),
    REALTIME_DB: { prepare: vi.fn(() => ({ bind: realtimeBindMock })) } as unknown as D1Database,
  };
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "54",
        mode: "rescore",
        raceBango: "10",
        runYmd: "20260712",
      }),
    ]),
    env,
  );
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=nar&daysAhead=0&mode=rescore&keibajoCode=54&raceBango=10&runDate=20260712&cardMaxRaceBango=10",
  );
  consoleSpy.mockRestore();
});

test("omits cardMaxRaceBango from a non-Kochi per-race rescore query URL", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "30",
        mode: "rescore",
        raceBango: "10",
        runYmd: "20260712",
      }),
    ]),
    makeEnv(),
  );
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=nar&daysAhead=0&mode=rescore&keibajoCode=30&raceBango=10&runDate=20260712",
  );
  consoleSpy.mockRestore();
});

test("keeps the per-race rescore DO name category-scoped when requestId is present", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        requestId: "request-123",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(idFromNameMock).toHaveBeenCalledWith("predict-nar");
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("acks a NAR per-race rescore when the container returns racesPredicted greater than zero", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  consoleSpy.mockRestore();
});

test("acks unsupported per-race rescore categories without container fetch", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "unsupported" as PredictQueueMessage["category"],
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(warnSpy).toHaveBeenCalledWith(
    "Skipping per-race rescore for unsupported category=unsupported runYmd=20260619 keibajo=44 race=01",
  );
  warnSpy.mockRestore();
});

test("logs container progress with race scope for debug container per-race rescore messages", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockImplementationOnce(
    async (
      _body: ReadableStream<Uint8Array>,
      options?: ParseNdjsonStreamOptions,
    ): Promise<PredictResultLine> => {
      options?.onProgress?.({ type: "progress", message: "halfway", elapsed: 4 });
      return { type: "result", racesPredicted: 1, category: "nar", status: "success" };
    },
  );
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        debug: true,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(consoleSpy).toHaveBeenCalledWith(
    "Predict progress category=nar runYmd=20260619 keibajo=44 race=01 stage=halfway elapsed=4",
  );
  consoleSpy.mockRestore();
});

test("acks a NAR per-race rescore when the container returns racesPredicted zero (no retry)", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    status: "success",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  consoleSpy.mockRestore();
});

test("retries a NAR per-race rescore when the container final result status is error", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    status: "error",
    error: "RuntimeError: rescore failed",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  expect(completeRunMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("retries a NAR per-race rescore when the container fetch throws", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockRejectedValue(new Error("container down"));
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("retries a NAR per-race rescore when the container response body is null", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockResolvedValue(new Response(null, { status: 200 }));
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("retries a NAR per-race rescore when the container DO returns 502", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockResolvedValue(
    Response.json({ error: "Container start failed", detail: "timeout" }, { status: 502 }),
  );
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("routes a Ban-ei per-race rescore to a category-scoped container DO (not Worker-native)", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "ban-ei",
        daysAhead: 0,
        keibajoCode: "83",
        mode: "rescore",
        raceBango: "07",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(rescoreJraRaceMock).not.toHaveBeenCalled();
  expect(idFromNameMock).toHaveBeenCalledWith("predict-ban-ei");
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("acks a day-scoped rescore without container fetch", async () => {
  await handleQueue(
    makeBatch([makeDayScopedMessage({ daysAhead: 0, mode: "rescore", runYmd: "20260619" })]),
    makeEnv(),
  );
  expectSkippedMissingPerRaceScope();
  expect(rescoreJraRaceMock).not.toHaveBeenCalled();
});

test("skips claimRun for focused per-race skipDedup full messages and still fetches the container", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "05",
        mode: "full",
        raceBango: "11",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(stubFetchMock).toHaveBeenCalledTimes(2);
  expect(completeRunMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("retries a focused skipDedup message when container fetch fails", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockRejectedValue(new Error("container down"));
  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "05",
        mode: "full",
        raceBango: "11",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(completeRunMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  errorSpy.mockRestore();
});

test("warms only the race cache for focused per-race skipDedup full messages", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runDateIso: "2026-06-28",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(completeRunMock).not.toHaveBeenCalled();
  expect(claimFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "jra",
      keibajoCode: "02",
      raceBango: "01",
      runYmd: "20260628",
      staleAfterMs: 1_860_000,
    }),
  );
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "jra",
      keibajoCode: "02",
      raceBango: "01",
      runYmd: "20260628",
      status: "success",
    }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(warmPredictionCacheForCategoryMock).not.toHaveBeenCalled();
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledWith({
    day: "28",
    keibajoCode: "02",
    month: "06",
    raceNumber: "01",
    refresh: true,
    year: "2026",
  });
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledWith({
    bustCacheApi: true,
    category: "jra",
    env: expect.anything(),
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260628",
  });
  expect(
    (publishFinishPositionPredictionCacheMock.mock.invocationCallOrder[0] ?? 0) <
      (warmPredictionCacheForRaceMock.mock.invocationCallOrder[0] ?? 0),
  ).toBe(true);
});

test("pads unpadded keibajo and race codes when warming after focused-full KV write", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "5",
        mode: "full",
        raceBango: "1",
        runDateIso: "2026-06-28",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledWith({
    day: "28",
    keibajoCode: "05",
    month: "06",
    raceNumber: "01",
    refresh: true,
    year: "2026",
  });
});

test("acks day-scoped skipDedup full messages without warming category cache", async () => {
  await handleQueue(
    makeBatch([
      makeDayScopedMessage({
        mode: "full",
        runDateIso: "2026-06-28",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expectSkippedMissingPerRaceScope();
  expect(warmPredictionCacheForCategoryMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheForCategoryMock).not.toHaveBeenCalled();
});

test("retries focused skipDedup full messages with result status error without category completion", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "jra",
    status: "error",
    error: "RuntimeError: focused build failed",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  expect(completeRunMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(warmPredictionCacheForCategoryMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    expect.stringMatching(
      /^Predict failed for category=jra runYmd=20260628 keibajo=02 race=01 durationMs=\d+:$/u,
    ),
    "Error: Container result status=error: RuntimeError: focused build failed",
  );
  errorSpy.mockRestore();
});

test("retries focused skipDedup full messages with a fixed delay when result status is accepted", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "jra",
    status: "accepted",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).not.toHaveBeenCalled();
  expect(completeRunMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  consoleSpy.mockRestore();
});

test("retries the original message at the base delay on the first busy encounter", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    status: "busy",
  });
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          category: "nar",
          keibajoCode: "35",
          mode: "full",
          raceBango: "01",
          runYmd: "20260629",
          skipDedup: true,
        },
        1,
      ),
    ]),
    makeEnv(),
  );
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith({
    category: "nar",
    env: expect.any(Object),
    keibajoCode: "35",
    raceBango: "01",
    runYmd: "20260629",
    status: "error",
  });
  consoleSpy.mockRestore();
});

test("grows the busy retry delay from the original message attempts", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    status: "busy",
  });
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          category: "nar",
          keibajoCode: "35",
          mode: "full",
          raceBango: "01",
          runYmd: "20260629",
          skipDedup: true,
        },
        6,
      ),
    ]),
    makeEnv(),
  );
  expect(sendMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 130 });
  expect(ackMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  consoleSpy.mockRestore();
});

test("caps the retry delay at the maximum once attempts are high", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    status: "busy",
  });
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          category: "nar",
          keibajoCode: "35",
          mode: "full",
          raceBango: "01",
          runYmd: "20260629",
          skipDedup: true,
        },
        20,
      ),
    ]),
    makeEnv(),
  );
  expect(sendMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 300 });
  expect(ackMock).not.toHaveBeenCalled();
  consoleSpy.mockRestore();
});

test("ignores the legacy busyRequeueCount when calculating retry delay", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    status: "busy",
  });
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          busyRequeueCount: 44,
          category: "nar",
          keibajoCode: "35",
          mode: "full",
          raceBango: "01",
          runYmd: "20260629",
          skipDedup: true,
        },
        2,
      ),
    ]),
    makeEnv(),
  );
  expect(sendMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 50 });
  expect(ackMock).not.toHaveBeenCalled();
  consoleSpy.mockRestore();
});

test("does not create a fresh busy message at the maximum Queue attempt", async () => {
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    status: "busy",
  });
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          busyRequeueCount: 45,
          category: "nar",
          keibajoCode: "35",
          mode: "full",
          raceBango: "01",
          runYmd: "20260629",
          skipDedup: true,
        },
        100,
      ),
    ]),
    makeEnv(),
  );
  expect(sendMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 300 });
  expect(ackMock).not.toHaveBeenCalled();
  expect(bindMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
});

test("acks focused skipDedup full messages when result status is already-complete", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "jra",
    status: "already-complete",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(completeRunMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(stubFetchMock).toHaveBeenCalledTimes(2);
  const alreadyCompletePickup = (stubFetchMock.mock.calls[1] as unknown as [Request])[0];
  expect(alreadyCompletePickup.url).toBe(
    "http://do/focused-full-cache?category=jra&runDate=20260628&keibajoCode=02&raceBango=01",
  );
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledWith({
    day: "28",
    keibajoCode: "02",
    month: "06",
    raceNumber: "01",
    refresh: true,
    year: "2026",
  });
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledWith({
    bustCacheApi: true,
    category: "jra",
    env: expect.anything(),
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260628",
  });
  expect(
    (publishFinishPositionPredictionCacheMock.mock.invocationCallOrder[0] ?? 0) <
      (warmPredictionCacheForRaceMock.mock.invocationCallOrder[0] ?? 0),
  ).toBe(true);
  consoleSpy.mockRestore();
});

test("retries container-complete focused full until its per-race R2 cache exists", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  parseNdjsonStreamMock.mockResolvedValue({
    category: "jra",
    racesPredicted: 0,
    status: "already-complete",
    type: "result",
  });

  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );

  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "Focused full cache still missing category=jra runYmd=20260628 keibajo=02 race=01 -- keeping completion message for recovery",
  );
  warnSpy.mockRestore();
});

test("falls through focused skipDedup full messages with result status success to the shared success path", async () => {
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 1,
    category: "jra",
    status: "success",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(completeRunMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(stubFetchMock).toHaveBeenCalledTimes(2);
  const successPickup = (stubFetchMock.mock.calls[1] as unknown as [Request])[0];
  expect(successPickup.url).toBe(
    "http://do/focused-full-cache?category=jra&runDate=20260628&keibajoCode=02&raceBango=01",
  );
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledWith({
    day: "28",
    keibajoCode: "02",
    month: "06",
    raceNumber: "01",
    refresh: true,
    year: "2026",
  });
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledWith({
    bustCacheApi: true,
    category: "jra",
    env: expect.anything(),
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260628",
  });
  expect(
    (publishFinishPositionPredictionCacheMock.mock.invocationCallOrder[0] ?? 0) <
      (warmPredictionCacheForRaceMock.mock.invocationCallOrder[0] ?? 0),
  ).toBe(true);
});

test("retries a successful focused full result until its per-race R2 cache exists", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  parseNdjsonStreamMock.mockResolvedValue({
    category: "jra",
    racesPredicted: 1,
    status: "success",
    type: "result",
  });

  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );

  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "Focused full cache still missing category=jra runYmd=20260628 keibajo=02 race=01 -- keeping completion message for recovery",
  );
  warnSpy.mockRestore();
});

test("does not treat a non-focused full message with status accepted as focused-full acceptance", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "jra",
    status: "accepted",
  });
  // Non-skipDedup per-race full still uses the shared success/error path, so
  // container "accepted" is treated as a failed result status.
  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "05",
        mode: "full",
        raceBango: "11",
      }),
    ]),
    makeEnv(),
  );
  expect(completeRunMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error", racesPredicted: 0 }),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(ackMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("does not treat a per-race rescore message with status accepted as focused-full acceptance", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "jra",
    status: "accepted",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(ackMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("does not treat per-race rescore as skipDedup even if skipDedup is set", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(rescoreJraRaceMock).not.toHaveBeenCalled();
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("pads unpadded keibajo and race codes when warming after a rescore KV write", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "5",
        mode: "rescore",
        raceBango: "1",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledWith({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "01",
    refresh: true,
    year: "2026",
  });
  consoleSpy.mockRestore();
});

test("warms the viewer cache for the race after a JRA per-race rescore succeeds", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledWith({
    day: "19",
    keibajoCode: "05",
    month: "06",
    raceNumber: "11",
    refresh: true,
    year: "2026",
  });
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledWith({
    bustCacheApi: true,
    category: "jra",
    env: expect.anything(),
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
  });
  expect(
    (publishFinishPositionPredictionCacheMock.mock.invocationCallOrder[0] ?? 0) <
      (warmPredictionCacheForRaceMock.mock.invocationCallOrder[0] ?? 0),
  ).toBe(true);
  consoleSpy.mockRestore();
});

test("awaits per-race KV publish before the rescore handler returns", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let publishStarted!: () => void;
  const publishStartedPromise = new Promise<void>((resolve) => {
    publishStarted = resolve;
  });
  publishFinishPositionPredictionCacheMock.mockImplementation(async () => {
    publishStarted();
    await gate;
    return { busted: true, status: "written" };
  });
  let handlerDone = false;
  const running = handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "04",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260809",
      }),
    ]),
    makeEnv(),
  ).then(() => {
    handlerDone = true;
  });
  await publishStartedPromise;
  expect(ackMock).not.toHaveBeenCalled();
  expect(handlerDone).toBe(false);
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  release();
  await running;
  expect(handlerDone).toBe(true);
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("awaits viewer display warm after a successful rescore KV write", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  let releaseWarm!: () => void;
  const warmGate = new Promise<void>((resolve) => {
    releaseWarm = resolve;
  });
  let warmStarted!: () => void;
  const warmStartedPromise = new Promise<void>((resolve) => {
    warmStarted = resolve;
  });
  warmPredictionCacheForRaceMock.mockImplementation(async () => {
    warmStarted();
    await warmGate;
    return true;
  });
  let handlerDone = false;
  const running = handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "04",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260809",
      }),
    ]),
    makeEnv(),
  ).then(() => {
    handlerDone = true;
  });
  await warmStartedPromise;
  expect(controlSendMock).toHaveBeenCalledTimes(1);
  expect(
    (controlSendMock.mock.invocationCallOrder[0] ?? 0) <
      (warmPredictionCacheForRaceMock.mock.invocationCallOrder[0] ?? 0),
  ).toBe(true);
  expect(ackMock).not.toHaveBeenCalled();
  expect(handlerDone).toBe(false);
  releaseWarm();
  await running;
  expect(handlerDone).toBe(true);
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("logs per-race KV publish status after a successful rescore", async () => {
  const logs: string[] = [];
  const consoleSpy = vi.spyOn(console, "log").mockImplementation((...args: unknown[]) => {
    logs.push(args.map(String).join(" "));
  });
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: true,
    status: "written",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "04",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260809",
      }),
    ]),
    makeEnv(),
  );
  expect(
    logs.some(
      (line) =>
        line.includes("prediction kv fp publish") &&
        line.includes("category=jra") &&
        line.includes("runYmd=20260809") &&
        line.includes("keibajo=04") &&
        line.includes("race=01") &&
        line.includes("status=written") &&
        line.includes("busted=true"),
    ),
  ).toBe(true);
  consoleSpy.mockRestore();
});

test("acks a successful rescore even when KV publish returns skipped-empty", async () => {
  const logs: string[] = [];
  const consoleSpy = vi.spyOn(console, "log").mockImplementation((...args: unknown[]) => {
    logs.push(args.map(String).join(" "));
  });
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    status: "skipped-empty",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "04",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260809",
      }),
    ]),
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  expect(logs.some((line) => line.includes("status=skipped-empty"))).toBe(true);
  consoleSpy.mockRestore();
});

test("does not warm the viewer display when a rescore KV publish returns error", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    status: "error",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "04",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260809",
      }),
    ]),
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  consoleSpy.mockRestore();
  warnSpy.mockRestore();
});

test("does not warm the race cache when a JRA container per-race rescore fetch throws", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockRejectedValue(new Error("container down"));
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("does not warm the category cache after a day-scoped skipDedup rescore is rejected", async () => {
  await handleQueue(
    makeBatch([
      makeDayScopedMessage({
        category: "nar",
        mode: "rescore",
        runDateIso: "2026-06-19",
        runYmd: "20260619",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expectSkippedMissingPerRaceScope();
  expect(warmPredictionCacheForCategoryMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheForCategoryMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
});

test("does not warm the category cache for a non-skipDedup container run", async () => {
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(warmPredictionCacheForCategoryMock).not.toHaveBeenCalled();
});

test("does not warm the category cache when a focused skipDedup run fails", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockRejectedValue(new Error("container down"));
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        keibajoCode: "44",
        mode: "full",
        raceBango: "01",
        runDateIso: "2026-06-19",
        runYmd: "20260619",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(warmPredictionCacheForCategoryMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("warms the viewer cache for the race after a NAR container per-race rescore succeeds", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260629",
      }),
    ]),
    makeEnv(),
  );
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledWith({
    day: "29",
    keibajoCode: "44",
    month: "06",
    raceNumber: "01",
    refresh: true,
    year: "2026",
  });
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledWith({
    bustCacheApi: true,
    category: "nar",
    env: expect.anything(),
    keibajoCode: "44",
    raceBango: "01",
    runYmd: "20260629",
  });
  consoleSpy.mockRestore();
});

test("warms the viewer cache for the race after a Ban-ei container per-race rescore succeeds", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "ban-ei",
        daysAhead: 0,
        keibajoCode: "83",
        mode: "rescore",
        raceBango: "07",
        runYmd: "20260629",
      }),
    ]),
    makeEnv(),
  );
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledWith({
    day: "29",
    keibajoCode: "83",
    month: "06",
    raceNumber: "07",
    refresh: true,
    year: "2026",
  });
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledWith({
    bustCacheApi: true,
    category: "ban-ei",
    env: expect.anything(),
    keibajoCode: "83",
    raceBango: "07",
    runYmd: "20260629",
  });
  consoleSpy.mockRestore();
});

test("does not warm the race cache when a container per-race rescore fetch throws", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockRejectedValue(new Error("container down"));
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260629",
      }),
    ]),
    makeEnv(),
  );
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("does not warm the race cache when a container per-race rescore response body is null", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockResolvedValue(new Response(null, { status: 200 }));
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260629",
      }),
    ]),
    makeEnv(),
  );
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("old-runYmd message is acked without touching the Container DO stub", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  await handleQueue(makeBatch([makeMessage({ runYmd: "20260101" })]), makeEnv());
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(idFromNameMock).not.toHaveBeenCalled();
  expect(getMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(prepareMock).toHaveBeenCalledTimes(1);
  expect(bindMock).toHaveBeenCalledWith("20260101", "jra", "full", "05", "11", 2);
  expect(runMock).toHaveBeenCalledTimes(1);
});

test("old-runYmd message with force:true bypasses the guard and reaches the Container DO", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  await handleQueue(makeBatch([makeMessage({ force: true, runYmd: "20260101" })]), makeEnv());
  expect(isOldDateRunYmdMock).not.toHaveBeenCalled();
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(prepareMock).not.toHaveBeenCalled();
});

test("old-runYmd focused-full skipDedup message completes the DO claim with status skipped-old-date", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  await handleQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260101",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.any(Object),
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260101",
    status: "skipped-old-date",
  });
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
});

test("old-runYmd per-race-rescore message is acked without dispatching to the container", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260101",
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
});

test("acks an old-runYmd message even when the D1 skip-event write fails, logging instead of retrying", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  isOldDateRunYmdMock.mockReturnValue(true);
  runMock.mockRejectedValue(new Error("d1 unavailable"));
  await handleQueue(makeBatch([makeMessage({ runYmd: "20260101" })]), makeEnv());
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    expect.stringMatching(/^Old-date skip bookkeeping failed /u),
    "Error: d1 unavailable",
  );
  errorSpy.mockRestore();
});

test("logs a warning describing the skip with category, runYmd, mode, and threshold", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isOldDateRunYmdMock.mockReturnValue(true);
  await handleQueue(makeBatch([makeMessage({ runYmd: "20260101" })]), makeEnv());
  expect(warnSpy).toHaveBeenCalledWith(
    "Skipping old-dated predict message category=jra runYmd=20260101 mode=full daysAhead=2 skipDedup=false busyRequeueCount=0 keibajo=05 race=11 thresholdDays=2",
  );
  warnSpy.mockRestore();
});

test("passes the message runYmd and a Date instance to isOldDateRunYmd", async () => {
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(isOldDateRunYmdMock).toHaveBeenCalledWith("20260603", expect.any(Date));
});

test("recent-runYmd messages proceed normally when isOldDateRunYmd returns false", async () => {
  isOldDateRunYmdMock.mockReturnValue(false);
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(prepareMock).not.toHaveBeenCalled();
});

test("retries the same per-race rescore and logs capped when the slot claim omits state", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  claimContainerSlotMock.mockResolvedValueOnce({ proceed: false });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "jra",
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] container slot capped doName=predict-jra kind=rescore category=jra runYmd=20260619 mode=rescore daysAhead=0 skipDedup=false busyRequeueCount=0 keibajo=05 race=11 -- will retry without starting a container",
  );
  warnSpy.mockRestore();
});

test("retries the same per-race rescore without starting a container when the slot is capped", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  claimContainerSlotMock.mockResolvedValueOnce({ proceed: false, state: "capped" });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "jra",
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("retries the same per-race rescore when the category DO is already busy", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  claimContainerSlotMock.mockResolvedValueOnce({ proceed: false, state: "busy" });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(idFromNameMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] container slot busy doName=predict-nar kind=rescore category=nar runYmd=20260619 mode=rescore daysAhead=0 skipDedup=false busyRequeueCount=0 keibajo=44 race=01 -- will retry without starting a container",
  );
  warnSpy.mockRestore();
});

test("keeps a busy per-race rescore bounded by the original Queue attempt budget", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  claimContainerSlotMock.mockResolvedValueOnce({ proceed: false, state: "busy" });
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          busyRequeueCount: 45,
          category: "jra",
          daysAhead: 0,
          keibajoCode: "05",
          mode: "rescore",
          raceBango: "11",
          runYmd: "20260619",
        },
        100,
      ),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 300 });
  expect(warnSpy).toHaveBeenCalledWith(
    "Rescore deferred category=jra runYmd=20260619 keibajo=05 race=11 reason=container-slot-unavailable attempts=100 delaySeconds=300",
  );
  warnSpy.mockRestore();
});

test("does not create a fresh rescore message when the legacy deferral deadline is old", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  claimContainerSlotMock.mockResolvedValueOnce({ proceed: false, state: "busy" });
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          busyRequeueCount: 45,
          category: "jra",
          daysAhead: 0,
          keibajoCode: "05",
          mode: "rescore",
          raceBango: "11",
          rescoreDeferredAt: "2026-06-19T00:00:00.000Z",
          runYmd: "20260619",
        },
        100,
      ),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(bindMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 300 });
  expect(warnSpy).toHaveBeenCalledWith(
    "Rescore deferred category=jra runYmd=20260619 keibajo=05 race=11 reason=container-slot-unavailable attempts=100 delaySeconds=300",
  );
  warnSpy.mockRestore();
});

test("queues the rescore container stop before acknowledging a successful rescore", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "jra",
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(claimContainerSlotMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "jra",
      doName: "predict-jra",
      kind: "rescore",
      staleAfterMs: 1_200_000,
    }),
  );
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra",
      type: "container-stop",
      workKey: "rescore:20260619:jra:05:11",
    }),
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("stops the rescore container after a failed rescore fetch", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockRejectedValueOnce(new Error("container 503"));
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "30",
        mode: "rescore",
        raceBango: "02",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-nar",
      type: "container-stop",
      workKey: "rescore:20260619:nar:30:02",
    }),
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  errorSpy.mockRestore();
});

test("retries a focused-full without starting a container when the slot is capped", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  claimContainerSlotMock.mockResolvedValueOnce({ proceed: false, state: "capped" });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(claimContainerSlotMock).toHaveBeenCalledWith(
    expect.objectContaining({
      allowSameOwner: true,
      doName: "predict-jra-1",
      kind: "focused-full",
    }),
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] container slot capped doName=predict-jra-1 kind=focused-full category=jra runYmd=20260628 mode=full daysAhead=0 skipDedup=true busyRequeueCount=0 keibajo=02 race=01 -- will retry without starting a container",
  );
  warnSpy.mockRestore();
});

test("resumed focused-full redelivery reclaims its same-work slot without yielding priority", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: true, state: "resumed" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: null,
      raceKey: "jra:20260628:02:01",
      status: "running",
    }),
  );
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        raceStartAtJst: "2026-06-28T10:10:00+09:00",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(touchContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-jra-1",
    env: expect.any(Object),
    staleAfterMs: 1_860_000,
    workKey: "focused-full:20260628:jra:02:01",
  });
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
});

test("rescore slot claims never receive focused-full same-owner permission", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(claimContainerSlotMock).toHaveBeenCalledWith(
    expect.not.objectContaining({ allowSameOwner: true }),
  );
});

test("keeps the focused-full slot after the container accepts a detached pipeline", async () => {
  parseNdjsonStreamMock.mockResolvedValueOnce({
    type: "result",
    racesPredicted: 0,
    category: "jra",
    status: "accepted",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );
  expect(claimContainerSlotMock).toHaveBeenCalledWith(
    expect.objectContaining({
      doName: "predict-jra-1",
      kind: "focused-full",
    }),
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
});

test("queues a focused-full stop when the container reports already-complete", async () => {
  parseNdjsonStreamMock.mockResolvedValueOnce({
    type: "result",
    racesPredicted: 8,
    category: "jra",
    status: "already-complete",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra-1",
      type: "container-stop",
      workKey: "focused-full:20260628:jra:02:01",
    }),
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("reclaims and queues a stop for the focused-full container when Neon is complete", async () => {
  isFocusedFullPredictionCompleteMock.mockResolvedValueOnce(true);
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );
  expect(claimContainerSlotMock).toHaveBeenCalledWith(
    expect.objectContaining({
      allowSameOwner: true,
      doName: "predict-jra-1",
      workKey: "focused-full:20260628:jra:02:01",
    }),
  );
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra-1",
      type: "container-stop",
      workKey: "focused-full:20260628:jra:02:01",
    }),
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("polls legacy started claims and refreshes their exact focused-full slot", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: null,
      raceKey: "jra:20260628:02:01",
      status: "running",
    }),
  );
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );
  expect(touchContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-jra-1",
    env: expect.anything(),
    staleAfterMs: 1_860_000,
    workKey: "focused-full:20260628:jra:02:01",
  });
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
});

test("keeps claims and slots unchanged when focused-full status query fails", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  stubFetchMock.mockResolvedValueOnce(new Response("unavailable", { status: 503 }));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(touchContainerSlotMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full status query failed category=jra runYmd=20260628 mode=full daysAhead=0 skipDedup=true busyRequeueCount=0 keibajo=02 race=01:",
    "Error: Focused-full status returned 503",
  );
  warnSpy.mockRestore();
});

test("persists detached errors and stops the exact container before retry", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: "RuntimeError: Catalog unavailable",
      raceKey: "jra:20260628:02:01",
      status: "error",
    }),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.anything(),
    keibajoCode: "02",
    raceBango: "01",
    runYmd: "20260628",
    status: "error",
  });
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra-1",
      type: "container-stop",
      workKey: "focused-full:20260628:jra:02:01",
    }),
  );
  expect(bindMock).toHaveBeenCalledWith(
    "predict-msg-1",
    "20260628",
    "jra",
    "full",
    "02",
    "01",
    "Error",
    "RuntimeError: Catalog unavailable",
    expect.stringContaining("RuntimeError: Catalog unavailable"),
    null,
    null,
    3,
  );
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full status error; cleanup handed off before retry category=jra runYmd=20260628 mode=full daysAhead=0 skipDedup=true busyRequeueCount=0 keibajo=02 race=01; error=RuntimeError: Catalog unavailable",
  );
  warnSpy.mockRestore();
});

test("hands detached DAY_BASE_REQUIRED race-chain errors to one legacy replacement", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: "DayBaseRequiredError: DAY_BASE_REQUIRED: layer subprocess terminated",
      raceKey: "jra:20260628:02:01",
      status: "error",
    }),
  );
  const original = makeMessage({
    daysAhead: 0,
    keibajoCode: "02",
    mode: "full",
    raceBango: "01",
    runYmd: "20260628",
    skipDedup: true,
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const raceGet = vi.fn(() => ({ fetch: stubFetchMock }));

  await handleQueue(makeBatch([original]), {
    ...makeEnv(),
    FEATURES_CACHE: {
      head: vi.fn(async () => ({
        customMetadata: {
          "max-data-sakusei-nengappi": "20260628090000",
          "row-count": "12",
          "rs-predicted-at-max": "20260628090500",
          "rs-row-count": "12",
        },
      })),
    } as unknown as R2Bucket,
    FINISH_POSITION_RACE_CHAIN_CONTAINER: {
      get: raceGet,
      idFromName: vi.fn(() => ({ name: "race-chain-id" })),
    } as unknown as NonNullable<Env["FINISH_POSITION_RACE_CHAIN_CONTAINER"]>,
    RACE_CHAIN_CONTAINER_CATEGORIES: "jra",
    RACE_CHAIN_CONTAINER_ENABLED: "1",
  });

  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "race-chain-predict-jra",
      role: "race-chain",
      type: "container-stop",
      workKey: "focused-full:20260628:jra:02:01",
    }),
  );
  expect(sendMock).toHaveBeenCalledWith(
    { ...original.body, forceLegacyContainer: true },
    { delaySeconds: 30 },
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] detached race-chain requested legacy fallback category=jra runYmd=20260628 mode=full daysAhead=0 skipDedup=true busyRequeueCount=0 keibajo=02 race=01 delaySeconds=30",
  );
  warnSpy.mockRestore();
});

test("retries detached DAY_BASE_REQUIRED delivery when legacy enqueue fails", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: "DAY_BASE_REQUIRED: transient source failure",
      raceKey: "jra:20260628:02:01",
      status: "error",
    }),
  );
  sendMock.mockRejectedValueOnce(new Error("Queue unavailable"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const raceGet = vi.fn(() => ({ fetch: stubFetchMock }));

  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    {
      ...makeEnv(),
      FEATURES_CACHE: {
        head: vi.fn(async () => ({
          customMetadata: {
            "max-data-sakusei-nengappi": "20260628090000",
            "row-count": "12",
            "rs-predicted-at-max": "20260628090500",
            "rs-row-count": "12",
          },
        })),
      } as unknown as R2Bucket,
      FINISH_POSITION_RACE_CHAIN_CONTAINER: {
        get: raceGet,
        idFromName: vi.fn(() => ({ name: "race-chain-id" })),
      } as unknown as NonNullable<Env["FINISH_POSITION_RACE_CHAIN_CONTAINER"]>,
      RACE_CHAIN_CONTAINER_CATEGORIES: "jra",
      RACE_CHAIN_CONTAINER_ENABLED: "1",
    },
  );

  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] detached race-chain legacy fallback enqueue failed category=jra runYmd=20260628 mode=full daysAhead=0 skipDedup=true busyRequeueCount=0 keibajo=02 race=01:",
    "Error: Queue unavailable",
  );
  warnSpy.mockRestore();
});

test("treats missing status as Container recreation and stops before retry", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: null,
      raceKey: "jra:20260822:07:09",
      status: "missing",
    }),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "07",
        mode: "full",
        raceBango: "09",
        runYmd: "20260822",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: "07", raceBango: "09", status: "error" }),
  );
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra-2",
      type: "container-stop",
      workKey: "focused-full:20260822:jra:07:09",
    }),
  );
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full status missing; cleanup handed off before retry category=jra runYmd=20260822 mode=full daysAhead=0 skipDedup=true busyRequeueCount=0 keibajo=07 race=09; error=Focused-full status missing after Container recreation: jra:20260822:07:09",
  );
  warnSpy.mockRestore();
});

test("finishes cache pickup and terminal cleanup when detached status succeeds", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: null,
      raceKey: "jra:20260628:02:01",
      status: "success",
    }),
  );
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).toHaveBeenCalledTimes(2);
  const statusRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  const cacheRequest = (stubFetchMock.mock.calls[1] as unknown as [Request])[0];
  expect(statusRequest.url).toBe(
    "http://do/focused-full-status?category=jra&keibajoCode=02&raceBango=01&runDate=20260628",
  );
  expect(cacheRequest.url).toBe(
    "http://do/focused-full-cache?category=jra&runDate=20260628&keibajoCode=02&raceBango=01",
  );
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra",
      role: "legacy",
      workKey: "focused-full:20260628:jra:02:01",
    }),
  );
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("queues the focused-full stop before acknowledging a successful result", async () => {
  parseNdjsonStreamMock.mockResolvedValueOnce({
    type: "result",
    racesPredicted: 1,
    category: "jra",
    status: "success",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra-1",
      type: "container-stop",
      workKey: "focused-full:20260628:jra:02:01",
    }),
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("acks a successful focused-full result after scheduling cleanup-only stop retry", async () => {
  controlSendMock.mockRejectedValueOnce(new Error("control queue unavailable"));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );

  expect(sendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      name: "predict-jra-1",
      role: "legacy",
      type: "container-cleanup",
      workKey: "focused-full:20260628:jra:02:01",
    },
    { delaySeconds: 30 },
  );
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("stops the focused-full container after a failed focused-full fetch", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockRejectedValueOnce(new Error("container 503"));
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra-1",
      type: "container-stop",
      workKey: "focused-full:20260628:jra:02:01",
    }),
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  errorSpy.mockRestore();
});

test("releases the focused-full slot when the container reports busy", async () => {
  parseNdjsonStreamMock.mockResolvedValueOnce({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    status: "busy",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "35",
        mode: "full",
        raceBango: "01",
        runYmd: "20260629",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(releaseContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-nar",
    env: expect.any(Object),
    kind: "focused-full",
    workKey: "focused-full:20260629:nar:35:01",
  });
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(sendMock).not.toHaveBeenCalled();
});

test("commits and acknowledges without rescoring when terminal stop cannot be queued", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  controlSendMock.mockRejectedValueOnce(new Error("control queue unavailable"));
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "jra",
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(completeRescoreRaceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.any(Object),
    executionId: "predict-msg-1",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
    status: "success",
  });
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(sendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      name: "predict-jra",
      role: "legacy",
      type: "container-cleanup",
      workKey: "rescore:20260619:jra:05:11",
    },
    { delaySeconds: 30 },
  );
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "[container-cleanup] stop enqueue failed name=predict-jra role=legacy workKey=rescore:20260619:jra:05:11:",
    "Error: control queue unavailable",
  );
  errorSpy.mockRestore();
  consoleSpy.mockRestore();
});

test("does not rescore again after a committed viewer warm failure", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  warmPredictionCacheForRaceMock.mockRejectedValueOnce(new Error("viewer unavailable"));
  claimRescoreExecutionMock
    .mockResolvedValueOnce({ proceed: true })
    .mockResolvedValueOnce({ proceed: false, state: "success" });
  const body = {
    category: "jra",
    daysAhead: 0,
    keibajoCode: "05",
    mode: "rescore",
    raceBango: "11",
    runYmd: "20260619",
  } satisfies Partial<PredictQueueMessage>;

  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());
  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());

  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(completeRescoreRaceMock).toHaveBeenCalledTimes(1);
  expect(completeRescoreRaceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.any(Object),
    executionId: "predict-msg-1",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
    status: "success",
  });
  expect(controlSendMock).toHaveBeenCalledTimes(2);
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(2);
  expect(retryMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "[predict-queue] committed rescore viewer warm failed category=jra runYmd=20260619 mode=rescore daysAhead=0 skipDedup=false busyRequeueCount=0 keibajo=05 race=11:",
    "Error: viewer unavailable",
  );
  errorSpy.mockRestore();
  consoleSpy.mockRestore();
});

test("acks a completed rescore duplicate without starting the container", async () => {
  claimRescoreExecutionMock.mockResolvedValueOnce({ proceed: false, state: "success" });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "jra",
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(claimRescoreExecutionMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.any(Object),
    executionId: "predict-msg-1",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
    staleAfterMs: 1_860_000,
  });
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra",
      type: "container-stop",
      workKey: "rescore:20260619:jra:05:11",
    }),
  );
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});

test("retries a rescore whose semantic execution is already in progress", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  claimRescoreExecutionMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          category: "nar",
          daysAhead: 0,
          keibajoCode: "44",
          mode: "rescore",
          raceBango: "01",
          runYmd: "20260619",
        },
        4,
      ),
    ]),
    makeEnv(),
  );
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 90 });
  expect(warnSpy).toHaveBeenCalledWith(
    "Rescore deferred category=nar runYmd=20260619 keibajo=44 race=01 reason=execution-started attempts=4 delaySeconds=90",
  );
  warnSpy.mockRestore();
});

test("recovers terminal rescore cleanup after both stop queues fail without rescoring", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  controlSendMock.mockRejectedValue(new Error("control queue unavailable"));
  sendMock.mockRejectedValueOnce(new Error("predict queue unavailable"));
  claimRescoreExecutionMock
    .mockResolvedValueOnce({ proceed: true })
    .mockResolvedValueOnce({ proceed: false, state: "success" });
  const body = {
    category: "jra",
    daysAhead: 0,
    keibajoCode: "05",
    mode: "rescore",
    raceBango: "11",
    runYmd: "20260619",
  } satisfies Partial<PredictQueueMessage>;

  await expect(handleQueue(makeBatch([makeMessage(body)]), makeEnv())).rejects.toThrow(
    "predict queue unavailable",
  );
  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());

  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledTimes(1);
  expect(completeRescoreRaceMock).toHaveBeenCalledTimes(1);
  expect(claimContainerSlotMock).toHaveBeenCalledTimes(1);
  expect(sendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      name: "predict-jra",
      role: "legacy",
      type: "container-cleanup",
      workKey: "rescore:20260619:jra:05:11",
    },
    { delaySeconds: 30 },
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
  consoleSpy.mockRestore();
});

test("records semantic rescore success before acknowledging", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "ban-ei",
        daysAhead: 0,
        keibajoCode: "83",
        mode: "rescore",
        raceBango: "03",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(completeRescoreRaceMock).toHaveBeenCalledWith({
    category: "ban-ei",
    env: expect.any(Object),
    executionId: "predict-msg-1",
    keibajoCode: "83",
    raceBango: "03",
    runYmd: "20260619",
    status: "success",
  });
  expect(
    (completeRescoreRaceMock.mock.invocationCallOrder[0] ?? 0) <
      (controlSendMock.mock.invocationCallOrder[0] ?? 0),
  ).toBe(true);
  expect(
    (completeRescoreRaceMock.mock.invocationCallOrder[0] ?? 0) <
      (warmPredictionCacheForRaceMock.mock.invocationCallOrder[0] ?? 0),
  ).toBe(true);
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("consumes a day-base prewarm message and acknowledges a landed build", async () => {
  const message = {
    ack: ackMock,
    body: {
      category: "nar",
      daysAhead: 0,
      requestedAt: "2026-08-22T00:00:00.000Z",
      runYmd: "20260822",
      type: "day-base-prewarm",
    },
    retry: retryMock,
  } as unknown as Message<import("./types").DayBasePrewarmMessage>;
  await handleQueue(
    { messages: [message] } as unknown as MessageBatch<import("./types").PredictQueueBody>,
    makeEnv(),
  );
  expect(prewarmCategoryWithOutcomeMock).toHaveBeenCalledWith({
    category: "nar",
    daysAhead: 0,
    env: expect.any(Object),
    runYmd: "20260822",
  });
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});

test("acks old non-force day-base prewarm work without restarting its container", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  const message = {
    ack: ackMock,
    body: {
      category: "nar",
      daysAhead: 0,
      requestedAt: "2026-08-23T00:00:00.000Z",
      runYmd: "20260823",
      type: "day-base-prewarm",
    },
    retry: retryMock,
  } as unknown as Message<import("./types").DayBasePrewarmMessage>;
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await handleQueue(
    { messages: [message] } as unknown as MessageBatch<import("./types").PredictQueueBody>,
    makeEnv(),
  );
  expect(prewarmCategoryWithOutcomeMock).not.toHaveBeenCalled();
  expect(clearDayBaseRepairReservationMock).toHaveBeenCalledTimes(1);
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ workKey: "day-base:20260823:nar" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  warnSpy.mockRestore();
});

test("allows an explicitly forced historical day-base prewarm", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  const message = {
    ack: ackMock,
    body: {
      category: "nar",
      daysAhead: 0,
      force: true,
      requestedAt: "2026-08-23T00:00:00.000Z",
      runYmd: "20260823",
      type: "day-base-prewarm",
    },
    retry: retryMock,
  } as unknown as Message<import("./types").DayBasePrewarmMessage>;
  await handleQueue(
    { messages: [message] } as unknown as MessageBatch<import("./types").PredictQueueBody>,
    makeEnv(),
  );
  expect(prewarmCategoryWithOutcomeMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, runYmd: "20260823" }),
  );
  expect(clearDayBaseRepairReservationMock).not.toHaveBeenCalled();
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("retries failed prewarms but acknowledges pickup ownership", async () => {
  prewarmCategoryWithOutcomeMock.mockResolvedValue("failed");
  const base = {
    category: "jra" as const,
    daysAhead: 0,
    requestedAt: "2026-08-22T00:00:00.000Z",
    runYmd: "20260822",
    type: "day-base-prewarm" as const,
  };
  const makePrewarm = (generatePredictionsAfterHit?: boolean) =>
    ({
      ack: ackMock,
      body: { ...base, ...(generatePredictionsAfterHit ? { generatePredictionsAfterHit } : {}) },
      retry: retryMock,
    }) as unknown as Message<import("./types").DayBasePrewarmMessage>;
  await handleQueue(
    { messages: [makePrewarm()] } as unknown as MessageBatch<import("./types").PredictQueueBody>,
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  ackMock.mockClear();
  retryMock.mockClear();
  prewarmCategoryWithOutcomeMock.mockResolvedValue("pickup-scheduled");
  await handleQueue(
    { messages: [makePrewarm(true)] } as unknown as MessageBatch<
      import("./types").PredictQueueBody
    >,
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(prewarmCategoryWithOutcomeMock).toHaveBeenLastCalledWith(
    expect.objectContaining({ generatePredictionsAfterHit: true }),
  );
});

test("consumes cleanup-only messages without running prediction work", async () => {
  const message = {
    ack: ackMock,
    body: {
      attempt: 2,
      name: "predict-jra-2",
      role: "legacy",
      type: "container-cleanup",
      workKey: "rescore:20260823:jra:04:08",
    },
    retry: retryMock,
  } as unknown as Message<import("./types").ContainerCleanupMessage>;

  await handleQueue(
    { messages: [message] } as unknown as MessageBatch<import("./types").PredictQueueBody>,
    makeEnv(),
  );

  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra-2",
      role: "legacy",
      type: "container-stop",
      workKey: "rescore:20260823:jra:04:08",
    }),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(completeRescoreRaceMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});
