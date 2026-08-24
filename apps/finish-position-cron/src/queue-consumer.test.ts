// Run with bun. Tests for the queue consumer (DO-backed dedup).

import { beforeEach, expect, test, vi } from "vitest";
import type { RaceEntry } from "./cron-decision";
import type { ParseNdjsonStreamOptions, PredictResultLine } from "./ndjson-stream";
import type { PredictionKvPublishResult } from "./prediction-kv-writer";
import type { MarketSignalHookResult } from "./race-chain-market-signal-hook";
import type {
  Env,
  FocusedFullCompletionMessage,
  FocusedFullWatchTickMessage,
  PredictQueueBody,
  PredictQueueMessage,
} from "./types";

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
  cancelFocusedFullRaceRepairMock,
  claimContainerSlotMock,
  claimFocusedFullRaceMock,
  claimFocusedFullTerminalWatchMock,
  claimRescoreExecutionMock,
  claimRunMock,
  clearContainerSlotMock,
  completeFocusedFullRaceMock,
  completeFocusedFullTerminalWatchMock,
  markFocusedFullTerminalWatchStoppedMock,
  registerFocusedFullWatchOutboxMock,
  clearFocusedFullWatchOutboxMock,
  completeRescoreRaceMock,
  completeRunMock,
  releaseContainerSlotMock,
  reserveFocusedFullRaceRepairMock,
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
  const cancelFocusedFullRaceRepair = vi.fn(async () => undefined);
  const claimContainerSlot = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const claimFocusedFullRace = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const claimFocusedFullTerminalWatch = vi.fn(
    async (): Promise<ClaimResult> => ({ proceed: true }),
  );
  const claimRescoreExecution = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const claimRun = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const clearContainerSlot = vi.fn(async () => undefined);
  const releaseContainerSlot = vi.fn(async () => undefined);
  const touchContainerSlot = vi.fn(async () => undefined);
  const completeFocusedFullRace = vi.fn(async () => undefined);
  const completeFocusedFullTerminalWatch = vi.fn(async () => undefined);
  const completeRescoreRace = vi.fn(async () => undefined);
  const completeRun = vi.fn(async () => undefined);
  const reserveFocusedFullRaceRepair = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
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
      expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
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
    cancelFocusedFullRaceRepairMock: cancelFocusedFullRaceRepair,
    claimContainerSlotMock: claimContainerSlot,
    claimFocusedFullRaceMock: claimFocusedFullRace,
    claimFocusedFullTerminalWatchMock: claimFocusedFullTerminalWatch,
    claimRescoreExecutionMock: claimRescoreExecution,
    claimRunMock: claimRun,
    clearContainerSlotMock: clearContainerSlot,
    completeFocusedFullRaceMock: completeFocusedFullRace,
    completeFocusedFullTerminalWatchMock: completeFocusedFullTerminalWatch,
    markFocusedFullTerminalWatchStoppedMock: vi.fn(async () => undefined),
    registerFocusedFullWatchOutboxMock: vi.fn(async () => undefined),
    clearFocusedFullWatchOutboxMock: vi.fn(async () => undefined),
    completeRescoreRaceMock: completeRescoreRace,
    releaseContainerSlotMock: releaseContainerSlot,
    reserveFocusedFullRaceRepairMock: reserveFocusedFullRaceRepair,
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

const consumeContainerStopMock = vi.hoisted(() =>
  vi.fn(async (_env: unknown, _message: unknown, afterDestroyed?: () => Promise<void>) => {
    await afterDestroyed?.();
    return true;
  }),
);

const prepareMarketSignalFoundationBestEffortMock = vi.hoisted(() =>
  vi.fn(
    async (): Promise<MarketSignalHookResult> => ({
      reason: "disabled",
      status: "unavailable",
    }),
  ),
);

vi.mock("./container-control", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./container-control")>();
  return { ...actual, consumeContainerStop: consumeContainerStopMock };
});

vi.mock("./race-chain-market-signal-hook", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./race-chain-market-signal-hook")>();
  return {
    ...actual,
    prepareMarketSignalFoundationBestEffort: prepareMarketSignalFoundationBestEffortMock,
  };
});

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
  cancelFocusedFullRaceRepair: cancelFocusedFullRaceRepairMock,
  claimContainerSlot: claimContainerSlotMock,
  claimFocusedFullRace: claimFocusedFullRaceMock,
  claimFocusedFullTerminalWatch: claimFocusedFullTerminalWatchMock,
  claimRescoreExecution: claimRescoreExecutionMock,
  claimRun: claimRunMock,
  clearContainerSlot: clearContainerSlotMock,
  completeFocusedFullRace: completeFocusedFullRaceMock,
  completeFocusedFullTerminalWatch: completeFocusedFullTerminalWatchMock,
  markFocusedFullTerminalWatchStopped: markFocusedFullTerminalWatchStoppedMock,
  registerFocusedFullWatchOutbox: registerFocusedFullWatchOutboxMock,
  clearFocusedFullWatchOutbox: clearFocusedFullWatchOutboxMock,
  completeRescoreRace: completeRescoreRaceMock,
  completeRun: completeRunMock,
  releaseContainerSlot: releaseContainerSlotMock,
  reserveFocusedFullRaceRepair: reserveFocusedFullRaceRepairMock,
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

import {
  PER_RACE_SCOPE_INVALID_ERROR,
  PER_RACE_SCOPE_REQUIRED_ERROR,
} from "./per-race-scope-guard";
import { handleQueue } from "./queue-consumer";

const ackMock = vi.fn();
const retryMock = vi.fn();
const sendMock = vi.fn();
const watchSendMock = vi.fn();
const controlSendMock = vi.fn();
const runMock = vi.fn(async () => ({ success: true }));
const bindMock = vi.fn(() => ({ run: runMock }));
const prepareMock = vi.fn(() => ({ bind: bindMock }));
const idFromNameMock = vi.fn(() => ({ name: "test-id" }));
const stubFetchMock = vi.fn(
  async (_request?: Request) =>
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
  PC_KEIBA_VIEWER_INTERNAL_TOKEN: "secret-token",
  PREDICT_DAYS_AHEAD: "2",
  CONTAINER_CONTROL_QUEUE: {
    send: controlSendMock,
  } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
  FOCUSED_FULL_COMPLETION_QUEUE: {
    send: watchSendMock,
  } as unknown as NonNullable<Env["FOCUSED_FULL_COMPLETION_QUEUE"]>,
  PREDICT_QUEUE: { send: sendMock } as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
  REALTIME_DB: {} as unknown as D1Database,
  TRIGGER_TOKEN: "secret-token",
});

// Happy-path messages are per-race only: production rejects day-scoped bodies.
const DEFAULT_KEIBAJO_CODE = "05";
const DEFAULT_RACE_BANGO = "11";
const TEST_WEIGHT_SNAPSHOT_HASH =
  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

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
      ...(overrides.mode === "rescore"
        ? {
            raceStartAtJst: "2099-01-01T00:00:00+09:00",
            weightSnapshotCount: 3,
            weightSnapshotFetchedAt: "2026-06-19T14:30:00+09:00",
            weightSnapshotHash: TEST_WEIGHT_SNAPSHOT_HASH,
          }
        : {}),
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

const makeBatch = (messages: Message<PredictQueueBody>[]): MessageBatch<PredictQueueBody> =>
  ({ messages }) as unknown as MessageBatch<PredictQueueBody>;

const makeFocusedFullCompletionMessage = (
  overrides: Partial<FocusedFullCompletionMessage> = {},
): Message<FocusedFullCompletionMessage> =>
  ({
    ack: ackMock,
    attempts: 1,
    body: {
      body: makeMessage({ skipDedup: true }).body,
      doName: "predict-jra",
      outcome: "success",
      role: "legacy",
      type: "focused-full-completion",
      watchId: "watch-1",
      workKey: "focused-full:20260603:jra:05:11",
      ...overrides,
    } satisfies FocusedFullCompletionMessage,
    id: "completion-msg-1",
    retry: retryMock,
    timestamp: new Date("2026-07-12T01:00:01.000Z"),
  }) as unknown as Message<FocusedFullCompletionMessage>;

const makeFocusedFullCompletionBatch = (
  messages: Message<FocusedFullCompletionMessage>[],
): MessageBatch<FocusedFullCompletionMessage> =>
  ({ messages }) as unknown as MessageBatch<FocusedFullCompletionMessage>;

const makeFocusedFullWatchTickMessage = (): Message<FocusedFullWatchTickMessage> => ({
  ack: ackMock,
  attempts: 1,
  body: {
    body: makeMessage({ skipDedup: true }).body,
    deadlineAtMs: Date.now() + 60_000,
    doName: "predict-jra",
    role: "legacy",
    type: "focused-full-watch-tick",
    watchId: "watch-tick-1",
    workKey: "focused-full:20260603:jra:05:11",
  },
  id: "watch-tick-msg-1",
  retry: retryMock,
  timestamp: new Date("2026-07-12T01:00:01.000Z"),
});

beforeEach(() => {
  ackMock.mockClear();
  retryMock.mockClear();
  prewarmCategoryWithOutcomeMock.mockClear();
  prewarmCategoryWithOutcomeMock.mockResolvedValue("landed");
  sendMock.mockClear();
  sendMock.mockResolvedValue(undefined);
  watchSendMock.mockClear();
  watchSendMock.mockResolvedValue(undefined);
  consumeContainerStopMock.mockClear();
  prepareMarketSignalFoundationBestEffortMock.mockClear();
  prepareMarketSignalFoundationBestEffortMock.mockResolvedValue({
    reason: "disabled",
    status: "unavailable",
  });
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
  claimFocusedFullTerminalWatchMock.mockClear();
  claimFocusedFullTerminalWatchMock.mockResolvedValue({ proceed: true });
  claimRescoreExecutionMock.mockClear();
  claimRunMock.mockClear();
  clearContainerSlotMock.mockClear();
  releaseContainerSlotMock.mockClear();
  touchContainerSlotMock.mockClear();
  completeFocusedFullRaceMock.mockClear();
  completeFocusedFullTerminalWatchMock.mockClear();
  markFocusedFullTerminalWatchStoppedMock.mockClear();
  registerFocusedFullWatchOutboxMock.mockClear();
  clearFocusedFullWatchOutboxMock.mockClear();
  completeRescoreRaceMock.mockClear();
  completeRunMock.mockClear();
  cancelFocusedFullRaceRepairMock.mockClear();
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
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
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
  cancelFocusedFullRaceRepairMock.mockResolvedValue(undefined);
  reserveFocusedFullRaceRepairMock.mockClear();
  reserveFocusedFullRaceRepairMock.mockResolvedValue({ proceed: true });
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

test("does not gate ban-ei focused-full on optional running-style state", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "ban-ei",
        keibajoCode: "83",
        raceBango: "12",
        runYmd: "20260824",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );

  expect(getRunningStyleRaceReadinessMock).not.toHaveBeenCalled();
  expect(claimFocusedFullRaceMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
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
    force: true,
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

test("attaches the exact Worker artifact identity before focused-full Container dispatch", async () => {
  prepareMarketSignalFoundationBestEffortMock.mockResolvedValueOnce({
    attestation: {
      baseGenerationId: "base-generation",
      etag: "artifact-etag",
      key: "feat-racechain-market-signal/catalog-v1/jra/20260603/05/11/foundation.json",
      oddsSnapshotHash: "odds-snapshot-hash",
      version: "artifact-version",
    },
    cacheHit: false,
    status: "ready",
  });
  const env = { ...makeEnv(), WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED: "1" };

  await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), env);

  expect(prepareMarketSignalFoundationBestEffortMock).toHaveBeenCalledWith({
    category: "jra",
    env: {
      FEATURES_CACHE: env.FEATURES_CACHE,
      WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED: "1",
    },
    fetchImpl: fetch,
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260603",
  });
  const request: Request | undefined = stubFetchMock.mock.calls[0]?.[0];
  if (request === undefined) throw new Error("expected Container request");
  expect(Object.fromEntries(new URL(request.url).searchParams)).toMatchObject({
    marketSignalBaseGenerationId: "base-generation",
    marketSignalFoundationEtag: "artifact-etag",
    marketSignalFoundationKey:
      "feat-racechain-market-signal/catalog-v1/jra/20260603/05/11/foundation.json",
    marketSignalFoundationVersion: "artifact-version",
    marketSignalOddsSnapshotHash: "odds-snapshot-hash",
  });
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
      `Skipping invalid predict message category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=false busyRequeueCount=0: ${PER_RACE_SCOPE_REQUIRED_ERROR}`,
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

test("acks a malformed race target without retrying or touching runtime dependencies", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([makeMessage({ raceBango: "12 nar:43:01 nar:43:02", skipDedup: true })]),
    makeEnv(),
  );
  expectSkippedMissingPerRaceScope();
  expect(prepareMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    `Skipping invalid predict message category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=true busyRequeueCount=0 keibajo=05 race=12 nar:43:01 nar:43:02: ${PER_RACE_SCOPE_INVALID_ERROR}`,
  );
  warnSpy.mockRestore();
});

test("acks an out-of-range race number without retrying", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await handleQueue(makeBatch([makeMessage({ raceBango: "13" })]), makeEnv());
  expectSkippedMissingPerRaceScope();
  warnSpy.mockRestore();
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
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
    internalToken: "secret-token",
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

test("defers focused-full display repair when viewer cache bust is unavailable", async () => {
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
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
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  expect(sendMock).toHaveBeenCalledWith(
    {
      category: "nar",
      keibajoCode: "50",
      raceBango: "12",
      runYmd: "20260701",
      type: "prediction-cache-repair",
    },
    { delaySeconds: 30 },
  );
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

test("repairs Neon-complete missing cache when detached status is success", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
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

  expect(isPerRaceFeatureCachePresentMock).toHaveBeenCalledTimes(2);
  expect(reserveFocusedFullRaceRepairMock).toHaveBeenCalledTimes(1);
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, keibajoCode: "01", raceBango: "06" }),
    { delaySeconds: 30 },
  );
  expect(prepareMock).not.toHaveBeenCalledWith(
    expect.stringContaining("finish_position_predict_retry_errors"),
  );
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(warnSpy).toHaveBeenCalledWith(
    expect.stringMatching(
      /^\[predict-queue\] focused-full cache repair reason=cache-missing-after-success outcome=enqueued reservationId=\S+ category=jra .*keibajo=01 race=06$/,
    ),
  );
  warnSpy.mockRestore();
});

test("finishes Neon-complete status success when the one in-delivery cache retry succeeds", async () => {
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
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

  expect(isPerRaceFeatureCachePresentMock).toHaveBeenCalledTimes(2);
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(reserveFocusedFullRaceRepairMock).not.toHaveBeenCalled();
  expect(controlSendMock).toHaveBeenCalledTimes(1);
  expect(consumeContainerStopMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("acks status-success cache repair when the same semantic repair is already reserved", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  reserveFocusedFullRaceRepairMock.mockResolvedValueOnce({
    proceed: false,
    state: "enqueued",
  });
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

  expect(reserveFocusedFullRaceRepairMock).toHaveBeenCalledTimes(1);
  expect(sendMock).not.toHaveBeenCalled();
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(prepareMock).not.toHaveBeenCalledWith(
    expect.stringContaining("finish_position_predict_retry_errors"),
  );
  expect(warnSpy).toHaveBeenCalledWith(
    expect.stringMatching(
      /^\[predict-queue\] focused-full cache repair reason=cache-missing-after-success outcome=already-reserved reservationId=\S+ category=jra .*keibajo=01 race=06$/,
    ),
  );
  warnSpy.mockRestore();
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

  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(reserveFocusedFullRaceRepairMock).toHaveBeenCalledWith(
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
  expect(reserveFocusedFullRaceRepairMock.mock.invocationCallOrder[0]).toBeLessThan(
    sendMock.mock.invocationCallOrder[0] ?? 0,
  );
  expect(sendMock.mock.invocationCallOrder[0]).toBeLessThan(
    ackMock.mock.invocationCallOrder[0] ?? 0,
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalledWith(
    expect.stringContaining("finish_position_predict_retry_errors"),
  );
  expect(warnSpy).toHaveBeenCalledWith(
    expect.stringMatching(
      /^\[predict-queue\] focused-full cache repair reason=missing-status outcome=enqueued reservationId=\S+ category=jra .*keibajo=01 race=06$/,
    ),
  );
  warnSpy.mockRestore();
});

test("acks one missing-status delivery when its forced repair is already reserved", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  reserveFocusedFullRaceRepairMock.mockResolvedValueOnce({
    proceed: false,
    state: "enqueued",
  });
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

  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(reserveFocusedFullRaceRepairMock).toHaveBeenCalledTimes(1);
  expect(sendMock).not.toHaveBeenCalled();
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalledWith(
    expect.stringContaining("finish_position_predict_retry_errors"),
  );
  expect(warnSpy).toHaveBeenCalledWith(
    expect.stringMatching(
      /^\[predict-queue\] focused-full cache repair reason=missing-status outcome=already-reserved reservationId=\S+ category=jra .*keibajo=01 race=06$/,
    ),
  );
  warnSpy.mockRestore();
});

test("retries without ack when a missing-status repair no longer owns its lane", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  reserveFocusedFullRaceRepairMock.mockResolvedValueOnce({
    proceed: false,
    state: "lane-conflict",
  });
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

  expect(sendMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full missing-status repair failed category=jra runYmd=20260822 mode=full daysAhead=2 skipDedup=true busyRequeueCount=0 keibajo=01 race=06:",
    "Error: Focused-full cache repair lane unavailable: lane-conflict",
  );
  warnSpy.mockRestore();
});

test("acks without retry when focused-full repair budget is exhausted", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  reserveFocusedFullRaceRepairMock.mockResolvedValueOnce({
    proceed: false,
    state: "repair-budget-exhausted",
  });
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

  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    expect.stringContaining("focused-full repair budget exhausted"),
  );
  errorSpy.mockRestore();
});

test("stops and retries a Neon-complete cache miss whose detached status is error", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
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

  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      type: "container-stop",
      workKey: "focused-full:20260822:jra:01:06",
    }),
  );
  expect(reserveFocusedFullRaceRepairMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
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

  expect(cancelFocusedFullRaceRepairMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: "01", raceBango: "06", runYmd: "20260822" }),
  );
  expect(controlSendMock).not.toHaveBeenCalled();
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
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("keeps a Neon-complete cache miss queued when the status query rejects", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  stubFetchMock
    .mockResolvedValueOnce(Response.json({ found: false }))
    .mockRejectedValueOnce(new Error("status transport unavailable"));

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
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("does not warm the viewer display when focused-full KV publish is empty", async () => {
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    expectedGeneratedAt: null,
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
  expect(sendMock).toHaveBeenCalledWith(
    {
      category: "nar",
      keibajoCode: "50",
      raceBango: "12",
      runYmd: "20260701",
      type: "prediction-cache-repair",
    },
    { delaySeconds: 30 },
  );
});

test("does not warm the viewer display when focused-full KV publish returns error", async () => {
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    expectedGeneratedAt: null,
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
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ type: "prediction-cache-repair" }),
    { delaySeconds: 30 },
  );
});

test("republishes and warms a cache-only repair without invoking a prediction Container", async () => {
  await handleQueue(
    {
      messages: [
        {
          ack: ackMock,
          attempts: 1,
          body: {
            category: "ban-ei",
            keibajoCode: "83",
            raceBango: "12",
            runYmd: "20260824",
            type: "prediction-cache-repair",
          },
          id: "cache-repair-1",
          retry: retryMock,
          timestamp: new Date("2026-08-24T01:00:00.000Z"),
        },
      ],
    } as never,
    makeEnv(),
  );

  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledWith({
    bustCacheApi: true,
    category: "ban-ei",
    env: expect.anything(),
    keibajoCode: "83",
    raceBango: "12",
    runYmd: "20260824",
  });
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledWith({
    day: "24",
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
    internalToken: "secret-token",
    keibajoCode: "83",
    month: "08",
    raceNumber: "12",
    refresh: true,
    year: "2026",
  });
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});

test("retries a cache-only repair when prediction rows are still unavailable", async () => {
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    expectedGeneratedAt: null,
    status: "skipped-empty",
  });

  await handleQueue(
    {
      messages: [
        {
          ack: ackMock,
          attempts: 2,
          body: {
            category: "nar",
            keibajoCode: "55",
            raceBango: "10",
            runYmd: "20260823",
            type: "prediction-cache-repair",
          },
          id: "cache-repair-2",
          retry: retryMock,
        },
      ],
    } as never,
    makeEnv(),
  );

  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).not.toHaveBeenCalled();
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
});

test("acks an out-of-window cache-only repair without retrying forever", async () => {
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    expectedGeneratedAt: null,
    status: "skipped-outside-window",
  });

  await handleQueue(
    {
      messages: [
        {
          ack: ackMock,
          attempts: 3,
          body: {
            category: "nar",
            keibajoCode: "55",
            raceBango: "10",
            runYmd: "20260823",
            type: "prediction-cache-repair",
          },
          id: "cache-repair-3",
          retry: retryMock,
        },
      ],
    } as never,
    makeEnv(),
  );

  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
});

test("acks a cache-only repair when Viewer warming does not land", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  warmPredictionCacheForRaceMock.mockResolvedValue(false);

  await handleQueue(
    {
      messages: [
        {
          ack: ackMock,
          attempts: 4,
          body: {
            category: "jra",
            keibajoCode: "01",
            raceBango: "01",
            runYmd: "20260824",
            type: "prediction-cache-repair",
          },
          id: "cache-repair-4",
          retry: retryMock,
        },
      ],
    } as never,
    makeEnv(),
  );

  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] prediction cache repair exhausted category=jra runYmd=20260824 keibajo=01 race=01 attempts=4",
  );
  warnSpy.mockRestore();
});

test("retries a cache-only repair while its bounded warm budget remains", async () => {
  warmPredictionCacheForRaceMock.mockResolvedValue(false);

  await handleQueue(
    {
      messages: [
        {
          ack: ackMock,
          attempts: 1,
          body: {
            category: "jra",
            keibajoCode: "01",
            raceBango: "01",
            runYmd: "20260824",
            type: "prediction-cache-repair",
          },
          id: "cache-repair-5",
          retry: retryMock,
        },
      ],
    } as never,
    makeEnv(),
  );

  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).not.toHaveBeenCalled();
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

test("starts a forced focused-full when no completion exists after forceRequestedAt", async () => {
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
  expect(isFocusedFullPredictionCompleteMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.anything(),
    keibajoCode: "02",
    notBefore: "2026-07-12T01:00:00.000Z",
    raceBango: "01",
    runYmd: "20260712",
  });
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

test("preserves force through Queue redelivery when completion is not visible", async () => {
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
  expect(claimFocusedFullRaceMock).toHaveBeenCalledWith(expect.objectContaining({ force: true }));
  const predictRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(predictRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=0&mode=full&runDate=20260712&keibajoCode=02&raceBango=01&force=1",
  );
});

test("preserves force while a redelivered focused-full waits for its lane", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "queued" });

  await handleQueue(
    makeBatch([
      makeMessage(
        {
          category: "ban-ei",
          daysAhead: 0,
          force: true,
          forceRequestedAt: "2026-08-24T00:00:00.000Z",
          keibajoCode: "83",
          mode: "full",
          raceBango: "08",
          runYmd: "20260824",
          skipDedup: true,
        },
        6,
      ),
    ]),
    makeEnv(),
  );

  expect(claimFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, keibajoCode: "83", raceBango: "08" }),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 130 });
  expect(ackMock).not.toHaveBeenCalled();
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
    "http://do/predict?category=jra&daysAhead=0&mode=rescore&keibajoCode=05&raceBango=11&raceStartAtJst=2099-01-01T00%3A00%3A00%2B09%3A00&runDate=20260619&weightSnapshotCount=3&weightSnapshotFetchedAt=2026-06-19T14%3A30%3A00%2B09%3A00&weightSnapshotHash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  );
});

test("binds active and canceled runner identity into the Container rescore URL", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        activeHorseNumbers: [1, 3],
        daysAhead: 0,
        entrySnapshotFetchedAt: "2026-06-19T14:30:00+09:00",
        entrySnapshotHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        excludedHorseNumbers: [2],
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );

  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=0&mode=rescore&keibajoCode=05&raceBango=11&raceStartAtJst=2099-01-01T00%3A00%3A00%2B09%3A00&runDate=20260619&weightSnapshotCount=3&weightSnapshotFetchedAt=2026-06-19T14%3A30%3A00%2B09%3A00&weightSnapshotHash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&activeHorseNumbers=%5B1%2C3%5D&excludedHorseNumbers=%5B2%5D&entrySnapshotFetchedAt=2026-06-19T14%3A30%3A00%2B09%3A00&entrySnapshotHash=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
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

test("acks an old non-force day-base pickup only after its strict cleanup completes", async () => {
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
  await handleQueue({ messages: [pickup] } as never, makeEnv());
  expect(consumeDayBasePickupMock).toHaveBeenCalledWith({
    env: expect.any(Object),
    message: pickup.body,
  });
  expect(clearDayBaseRepairReservationMock).toHaveBeenCalledWith({
    category: "nar",
    env: expect.any(Object),
    runYmd: "20260823",
  });
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
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

test("does not ack an old day-base pickup when owner-safe Container cleanup fails", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  consumeDayBasePickupMock.mockRejectedValueOnce(new Error("cleanup queue unavailable"));
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
  await expect(handleQueue({ messages: [pickup] } as never, makeEnv())).rejects.toThrow(
    "cleanup queue unavailable",
  );
  expect(clearDayBaseRepairReservationMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
});

test("does not ack an old day-base pickup when repair reservation cleanup fails", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  clearDayBaseRepairReservationMock.mockRejectedValueOnce(new Error("D1 unavailable"));
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

  await expect(handleQueue({ messages: [pickup] } as never, makeEnv())).rejects.toThrow(
    "D1 unavailable",
  );

  expect(consumeDayBasePickupMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
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
      "http://do/predict?category=jra&daysAhead=0&mode=rescore&keibajoCode=05&raceBango=11&raceStartAtJst=2099-01-01T00%3A00%3A00%2B09%3A00&runDate=20260619&weightSnapshotCount=3&weightSnapshotFetchedAt=2026-06-19T14%3A30%3A00%2B09%3A00&weightSnapshotHash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ),
  );
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("retries a Container rescore before dispatch when its weight generation is incomplete", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "35",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260824",
        weightSnapshotHash: undefined,
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test.each([
  ["count", { weightSnapshotCount: undefined }],
  ["fetchedAt", { weightSnapshotFetchedAt: undefined }],
] as const)(
  "retries a Container rescore when its weight generation %s is missing",
  async (_field, generationOverride) => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
    await handleQueue(
      makeBatch([
        makeMessage({
          category: "nar",
          daysAhead: 0,
          keibajoCode: "35",
          mode: "rescore",
          raceBango: "01",
          runYmd: "20260824",
          ...generationOverride,
        }),
      ]),
      makeEnv(),
    );
    expect(stubFetchMock).not.toHaveBeenCalled();
    expect(retryMock).toHaveBeenCalledTimes(1);
    expect(ackMock).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  },
);

test("acks a legacy Container rescore without a race-start deadline before dispatch", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        mode: "rescore",
        raceStartAtJst: undefined,
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
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

test("drops a Worker rescore without publish or fallback when scoring crosses post time", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-08-24T05:59:59.000Z"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  rescoreJraRaceMock.mockImplementationOnce(async () => {
    vi.setSystemTime(new Date("2026-08-24T06:00:00.000Z"));
    return {
      modelVersion: "jra-cb-v9-sim-2013-clean",
      predictionCount: 3,
      racesPredicted: 1,
      status: "ok",
    };
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        raceStartAtJst: "2026-08-24T15:00:00+09:00",
        runYmd: "20260824",
      }),
    ]),
    { ...makeEnv(), JRA_WORKER_RESCORE_ENABLED: "1" },
  );
  expect(rescoreJraRaceMock).toHaveBeenCalledTimes(1);
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(completeRescoreRaceMock).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
  vi.useRealTimers();
});

test("blocks Worker KV publish when the deadline closes after the score-complete check", async () => {
  const beforePost = Date.parse("2026-08-24T14:59:59+09:00");
  const atPost = Date.parse("2026-08-24T15:00:00+09:00");
  const nowSpy = vi
    .spyOn(Date, "now")
    .mockReturnValueOnce(beforePost)
    .mockReturnValueOnce(beforePost)
    .mockReturnValueOnce(beforePost)
    .mockReturnValueOnce(beforePost)
    .mockReturnValueOnce(atPost);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        raceStartAtJst: "2026-08-24T15:00:00+09:00",
        runYmd: "20260824",
      }),
    ]),
    { ...makeEnv(), JRA_WORKER_RESCORE_ENABLED: "1" },
  );
  expect(rescoreJraRaceMock).toHaveBeenCalledTimes(1);
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(completeRescoreRaceMock).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
  nowSpy.mockRestore();
});

test("releases a Container slot when post time arrives before execution starts", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-08-24T05:59:59.000Z"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  claimContainerSlotMock.mockImplementationOnce(async () => {
    vi.setSystemTime(new Date("2026-08-24T06:00:00.000Z"));
    return { proceed: true };
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "35",
        mode: "rescore",
        raceBango: "01",
        raceStartAtJst: "2026-08-24T15:00:00+09:00",
        runYmd: "20260824",
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(releaseContainerSlotMock).toHaveBeenCalledTimes(1);
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
  vi.useRealTimers();
});

test("stops the Container without publish when scoring crosses post time", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-08-24T05:59:59.000Z"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockImplementationOnce(async () => {
    vi.setSystemTime(new Date("2026-08-24T06:00:00.000Z"));
    return {
      type: "result",
      racesPredicted: 1,
      category: "nar",
      status: "success",
    };
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "35",
        mode: "rescore",
        raceBango: "01",
        raceStartAtJst: "2026-08-24T15:00:00+09:00",
        runYmd: "20260824",
      }),
    ]),
    makeEnv(),
  );
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(completeRescoreRaceMock).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ type: "container-stop", workKey: "rescore:20260824:nar:35:01" }),
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
  vi.useRealTimers();
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

test("commits and acks an enabled Worker rescore when viewer warm fails", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
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
  expect(retryMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] viewer cache warm best-effort failed category=jra runYmd=20260619 keibajo=05 race=11: Error: viewer unavailable",
  );
  warnSpy.mockRestore();
  consoleSpy.mockRestore();
});

test("defers a cache-only repair after Worker rescore warm returns false", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  warmPredictionCacheForRaceMock.mockResolvedValue(false);
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
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ type: "prediction-cache-repair" }),
    { delaySeconds: 30 },
  );
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] viewer cache warm best-effort failed category=jra runYmd=20260619 keibajo=05 race=11: returned-false",
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] deferred prediction cache repair after warm miss category=jra runYmd=20260619 keibajo=05 race=11",
  );
  warnSpy.mockRestore();
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
    "http://do/predict?category=nar&daysAhead=0&mode=rescore&keibajoCode=44&raceBango=01&raceStartAtJst=2099-01-01T00%3A00%3A00%2B09%3A00&runDate=20260619&weightSnapshotCount=3&weightSnapshotFetchedAt=2026-06-19T14%3A30%3A00%2B09%3A00&weightSnapshotHash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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
    "http://do/predict?category=nar&daysAhead=0&mode=rescore&keibajoCode=54&raceBango=10&raceStartAtJst=2099-01-01T00%3A00%3A00%2B09%3A00&runDate=20260712&weightSnapshotCount=3&weightSnapshotFetchedAt=2026-06-19T14%3A30%3A00%2B09%3A00&weightSnapshotHash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&cardMaxRaceBango=10",
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
    "http://do/predict?category=nar&daysAhead=0&mode=rescore&keibajoCode=30&raceBango=10&raceStartAtJst=2099-01-01T00%3A00%3A00%2B09%3A00&runDate=20260712&weightSnapshotCount=3&weightSnapshotFetchedAt=2026-06-19T14%3A30%3A00%2B09%3A00&weightSnapshotHash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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

test("reconnects once with a fresh stub when the per-race rescore DO becomes inactive", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  stubFetchMock
    .mockRejectedValueOnce(
      new Error(
        "Connection closed: this Durable Object instance is no longer active. Reconnect or retry the request.",
      ),
    )
    .mockResolvedValueOnce(
      new Response(JSON.stringify({ type: "result", racesPredicted: 1, status: "success" }), {
        status: 200,
      }),
    );

  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "46",
        mode: "rescore",
        raceBango: "06",
        runYmd: "20260824",
      }),
    ]),
    makeEnv(),
  );

  expect(getMock).toHaveBeenCalledTimes(2);
  expect(stubFetchMock).toHaveBeenCalledTimes(2);
  expect(stubFetchMock.mock.calls[1]?.[0]?.url).toBe(stubFetchMock.mock.calls[0]?.[0]?.url);
  expect(warnSpy).toHaveBeenCalledWith(
    expect.stringContaining("container DO became inactive; reconnecting once"),
  );
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
  consoleSpy.mockRestore();
});

test("falls back to Queue retry when the fresh per-race rescore stub is also inactive", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  stubFetchMock.mockRejectedValue(
    new Error(
      "Connection closed: this Durable Object instance is no longer active. Reconnect or retry the request.",
    ),
  );

  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        keibajoCode: "46",
        mode: "rescore",
        raceBango: "06",
        runYmd: "20260824",
      }),
    ]),
    makeEnv(),
  );

  expect(getMock).toHaveBeenCalledTimes(2);
  expect(stubFetchMock).toHaveBeenCalledTimes(2);
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
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
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
    internalToken: "secret-token",
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
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
    internalToken: "secret-token",
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

test("acks a watched focused-full accepted response without Queue polling", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    category: "jra",
    racesPredicted: 0,
    status: "accepted",
    type: "result",
  });
  stubFetchMock.mockResolvedValueOnce(
    new Response("accepted", { headers: { "x-focused-full-watch-id": "watch-accepted-1" } }),
  );
  const completionSend = vi.fn(async () => undefined);
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
    {
      ...makeEnv(),
      FOCUSED_FULL_COMPLETION_QUEUE: { send: completionSend } as unknown as NonNullable<
        Env["FOCUSED_FULL_COMPLETION_QUEUE"]
      >,
      FOCUSED_FULL_WATCH_ENABLED: "1",
    },
  );
  const request = stubFetchMock.mock.calls[0]?.[0];
  expect(request).toBeInstanceOf(Request);
  expect(request?.headers.get("x-focused-full-watch-payload")).toMatch(
    /"workKey":"focused-full:20260628:jra:02:01"/u,
  );
  expect(request?.headers.get("x-focused-full-watch-payload")).toContain(
    '"watchId":"focused-full:20260628:jra:02:01:predict-msg-1"',
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  consoleSpy.mockRestore();
});

test("chains a running focused-full watch tick without Queue retry", async () => {
  stubFetchMock.mockResolvedValueOnce(
    Response.json({ error: null, raceKey: "jra:20260603:05:11", status: "running" }),
  );
  const tick = makeFocusedFullWatchTickMessage();

  await handleQueue(makeBatch([tick]), makeEnv());

  expect(touchContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-jra",
    env: expect.anything(),
    staleAfterMs: 1_200_000,
    workKey: "focused-full:20260603:jra:05:11",
  });
  expect(watchSendMock).toHaveBeenCalledWith(tick.body, { delaySeconds: 30 });
  expect(ackMock).toHaveBeenCalledOnce();
  expect(retryMock).not.toHaveBeenCalled();
});

test("transitions a terminal watch tick to a completion message without delay options", async () => {
  stubFetchMock.mockResolvedValueOnce(
    Response.json({ error: null, raceKey: "jra:20260603:05:11", status: "success" }),
  );

  await handleQueue(makeBatch([makeFocusedFullWatchTickMessage()]), makeEnv());

  expect(watchSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      outcome: "success",
      type: "focused-full-completion",
      watchId: "watch-tick-1",
    }),
  );
  expect(touchContainerSlotMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledOnce();
  expect(retryMock).not.toHaveBeenCalled();
});

test("leaves poll and successor-send failures to Cloudflare redelivery without explicit retry", async () => {
  stubFetchMock.mockRejectedValueOnce(new Error("status unavailable"));
  await expect(
    handleQueue(makeBatch([makeFocusedFullWatchTickMessage()]), makeEnv()),
  ).rejects.toThrow("status unavailable");
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();

  stubFetchMock.mockResolvedValueOnce(
    Response.json({ error: null, raceKey: "jra:20260603:05:11", status: "running" }),
  );
  watchSendMock.mockRejectedValueOnce(new Error("tick send unavailable"));
  await expect(
    handleQueue(makeBatch([makeFocusedFullWatchTickMessage()]), makeEnv()),
  ).rejects.toThrow("tick send unavailable");
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
});

test("leaves a running tick lease-touch failure unacked and does not create a successor", async () => {
  stubFetchMock.mockResolvedValueOnce(
    Response.json({ error: null, raceKey: "jra:20260603:05:11", status: "running" }),
  );
  touchContainerSlotMock.mockRejectedValueOnce(new Error("coordinator touch unavailable"));

  await expect(
    handleQueue(makeBatch([makeFocusedFullWatchTickMessage()]), makeEnv()),
  ).rejects.toThrow("coordinator touch unavailable");

  expect(watchSendMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
});

test("durably schedules a terminal watchdog before a persistent finalizer failure", async () => {
  completeFocusedFullRaceMock.mockRejectedValueOnce(new Error("coordinator unavailable"));

  await expect(
    handleQueue(makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage()]), makeEnv()),
  ).rejects.toThrow("coordinator unavailable");

  expect(watchSendMock).toHaveBeenCalledWith(expect.objectContaining({ watchId: "watch-1" }), {
    delaySeconds: 150,
  });
  expect(completeFocusedFullTerminalWatchMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
});

test("finalizes one watched focused-full success and acknowledges its terminal delivery", async () => {
  stubFetchMock.mockResolvedValueOnce(Response.json({ found: true }));
  await handleQueue(
    makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage()]),
    makeEnv(),
  );
  expect(claimFocusedFullTerminalWatchMock).toHaveBeenCalledWith({
    claimId: "completion-msg-1",
    env: expect.anything(),
    staleAfterMs: 960_000,
    watchId: "watch-1",
  });
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledWith({
    claimId: "completion-msg-1",
    env: expect.anything(),
    watchId: "watch-1",
  });
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(consumeContainerStopMock).toHaveBeenCalledTimes(1);
  expect(markFocusedFullTerminalWatchStoppedMock).toHaveBeenCalledWith({
    claimId: "completion-msg-1",
    env: expect.anything(),
    watchId: "watch-1",
  });
  expect(consumeContainerStopMock.mock.invocationCallOrder[0]).toBeLessThan(
    sendMock.mock.invocationCallOrder[0] ?? Number.POSITIVE_INFINITY,
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});

test("stops a watched success before repairing its missing cache", async () => {
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  stubFetchMock
    .mockResolvedValueOnce(Response.json({ found: false }))
    .mockResolvedValueOnce(Response.json({ found: false }));

  await handleQueue(
    makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage()]),
    makeEnv(),
  );

  expect(consumeContainerStopMock).toHaveBeenCalledTimes(1);
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, keibajoCode: "05", raceBango: "11" }),
    { delaySeconds: 30 },
  );
  expect(consumeContainerStopMock.mock.invocationCallOrder[0]).toBeLessThan(
    sendMock.mock.invocationCallOrder[0] ?? Number.POSITIVE_INFINITY,
  );
  expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("keeps the watchdog alive when synchronous stop or slot clear fails", async () => {
  stubFetchMock.mockResolvedValueOnce(Response.json({ found: true }));
  consumeContainerStopMock.mockRejectedValueOnce(new Error("stop unavailable"));

  await expect(
    handleQueue(makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage()]), makeEnv()),
  ).rejects.toThrow("stop unavailable");

  expect(watchSendMock).toHaveBeenCalledWith(expect.objectContaining({ watchId: "watch-1" }), {
    delaySeconds: 150,
  });
  expect(completeFocusedFullTerminalWatchMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
});

test("resumes after a stopped Container when terminal marking previously failed", async () => {
  stubFetchMock.mockImplementation(async () => Response.json({ found: true }));
  completeFocusedFullTerminalWatchMock.mockRejectedValueOnce(
    new Error("terminal mark unavailable"),
  );

  await expect(
    handleQueue(makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage()]), makeEnv()),
  ).rejects.toThrow("terminal mark unavailable");
  expect(consumeContainerStopMock).toHaveBeenCalledTimes(1);
  expect(markFocusedFullTerminalWatchStoppedMock).toHaveBeenCalledTimes(1);

  claimFocusedFullTerminalWatchMock.mockResolvedValueOnce({ proceed: true, state: "stopped" });
  await handleQueue(
    makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage()]),
    makeEnv(),
  );

  expect(consumeContainerStopMock).toHaveBeenCalledTimes(2);
  expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledTimes(2);
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("acknowledges completion when terminal stop ownership is already lost", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  stubFetchMock.mockResolvedValueOnce(Response.json({ found: true }));
  consumeContainerStopMock.mockResolvedValueOnce(false);

  await handleQueue(
    makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage()]),
    makeEnv(),
  );

  expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full terminal stop was not owned; treating completion as terminal doName=predict-jra workKey=focused-full:20260603:jra:05:11",
  );
  warnSpy.mockRestore();
});

test("acks a duplicate terminal watch that the coordinator already completed", async () => {
  claimFocusedFullTerminalWatchMock.mockResolvedValueOnce({ proceed: false, state: "terminal" });
  await handleQueue(
    makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage()]),
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
});

test("continues the watchdog chain for a concurrent terminal delivery without Queue retry", async () => {
  claimFocusedFullTerminalWatchMock.mockResolvedValueOnce({ proceed: false, state: "processing" });
  await handleQueue(
    makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage()]),
    makeEnv(),
  );
  expect(watchSendMock).toHaveBeenCalledWith(expect.objectContaining({ watchId: "watch-1" }), {
    delaySeconds: 150,
  });
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledOnce();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
});

test("turns one watched missing status into the existing atomic forced repair", async () => {
  await handleQueue(
    makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage({ outcome: "missing" })]),
    makeEnv(),
  );
  expect(reserveFocusedFullRaceRepairMock).toHaveBeenCalledTimes(1);
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, keibajoCode: "05", raceBango: "11" }),
    { delaySeconds: 30 },
  );
  expect(consumeContainerStopMock).toHaveBeenCalledTimes(1);
  expect(consumeContainerStopMock.mock.invocationCallOrder[0]).toBeLessThan(
    sendMock.mock.invocationCallOrder[0] ?? Number.POSITIVE_INFINITY,
  );
  expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});

test("resumes a stopped missing watch through cleanup-only before repair", async () => {
  claimFocusedFullTerminalWatchMock.mockResolvedValueOnce({ proceed: true, state: "stopped" });

  await handleQueue(
    makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage({ outcome: "missing" })]),
    makeEnv(),
  );

  expect(consumeContainerStopMock).toHaveBeenCalledTimes(1);
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, keibajoCode: "05", raceBango: "11" }),
    { delaySeconds: 30 },
  );
  expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test.each(["success", "timeout"] as const)(
  "resumes a stopped %s watch through cleanup-only before terminal work",
  async (outcome) => {
    claimFocusedFullTerminalWatchMock.mockResolvedValueOnce({ proceed: true, state: "stopped" });
    stubFetchMock.mockResolvedValue(Response.json({ found: true }));

    await handleQueue(
      makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage({ outcome })]),
      makeEnv(),
    );

    expect(consumeContainerStopMock).toHaveBeenCalledTimes(1);
    expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledTimes(1);
    expect(ackMock).toHaveBeenCalledTimes(1);
  },
);

test("turns one watched timeout into the existing atomic forced repair", async () => {
  await handleQueue(
    makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage({ outcome: "timeout" })]),
    makeEnv(),
  );
  expect(reserveFocusedFullRaceRepairMock).toHaveBeenCalledTimes(1);
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, keibajoCode: "05", raceBango: "11" }),
    { delaySeconds: 30 },
  );
  expect(consumeContainerStopMock).toHaveBeenCalledTimes(1);
  expect(consumeContainerStopMock.mock.invocationCallOrder[0]).toBeLessThan(
    sendMock.mock.invocationCallOrder[0] ?? Number.POSITIVE_INFINITY,
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("persists one watched explicit error and enqueues one forced recovery", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await handleQueue(
    makeFocusedFullCompletionBatch([
      makeFocusedFullCompletionMessage({
        error: "RuntimeError: detached failed",
        outcome: "error",
      }),
    ]),
    makeEnv(),
  );
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(bindMock).toHaveBeenCalledWith(
    "completion-msg-1",
    "20260603",
    "jra",
    "full",
    "05",
    "11",
    "Error",
    "RuntimeError: detached failed",
    expect.stringContaining("RuntimeError: detached failed"),
    null,
    null,
    1,
  );
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, forceRequestedAt: expect.any(String) }),
    { delaySeconds: 30 },
  );
  expect(consumeContainerStopMock).toHaveBeenCalledTimes(1);
  expect(consumeContainerStopMock.mock.invocationCallOrder[0]).toBeLessThan(
    sendMock.mock.invocationCallOrder[0] ?? Number.POSITIVE_INFINITY,
  );
  expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("leaves an explicit-error recovery enqueue failure to Cloudflare redelivery", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  sendMock.mockRejectedValueOnce(new Error("recovery queue unavailable"));

  await expect(
    handleQueue(
      makeFocusedFullCompletionBatch([
        makeFocusedFullCompletionMessage({ error: "detached failed", outcome: "error" }),
      ]),
      makeEnv(),
    ),
  ).rejects.toThrow("Focused-full terminal finalizer incomplete");

  expect(consumeContainerStopMock).toHaveBeenCalledTimes(1);
  expect(completeFocusedFullTerminalWatchMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    expect.stringContaining("terminal recovery enqueue failed"),
    "Error: recovery queue unavailable",
  );
  warnSpy.mockRestore();
  errorSpy.mockRestore();
});

test("safely completes an invalid watched body without prediction side effects", async () => {
  const terminal = makeFocusedFullCompletionMessage({
    body: makeMessage({ skipDedup: false }).body,
  });
  await handleQueue(makeFocusedFullCompletionBatch([terminal]), makeEnv());
  expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledTimes(1);
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
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
  expect(bindMock).toHaveBeenCalled();
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
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
    internalToken: "secret-token",
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

test("repairs container-complete focused full when its per-race R2 cache remains absent", async () => {
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

  expect(isPerRaceFeatureCachePresentMock).toHaveBeenCalledTimes(2);
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(reserveFocusedFullRaceRepairMock).toHaveBeenCalledTimes(1);
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, keibajoCode: "02", raceBango: "01" }),
    { delaySeconds: 30 },
  );
  expect(controlSendMock).not.toHaveBeenCalled();
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
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
    internalToken: "secret-token",
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

test("repairs a successful focused full result when its per-race R2 cache remains absent", async () => {
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

  expect(isPerRaceFeatureCachePresentMock).toHaveBeenCalledTimes(2);
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(reserveFocusedFullRaceRepairMock).toHaveBeenCalledTimes(1);
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, keibajoCode: "02", raceBango: "01" }),
    { delaySeconds: 30 },
  );
  expect(controlSendMock).not.toHaveBeenCalled();
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
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
    internalToken: "secret-token",
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
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
    internalToken: "secret-token",
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
    return {
      busted: true,
      expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
      status: "written",
    };
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
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(completeRescoreRaceMock).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(ackMock).not.toHaveBeenCalled();
  expect(handlerDone).toBe(false);
  releaseWarm();
  await running;
  expect(handlerDone).toBe(true);
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(controlSendMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("logs per-race KV publish status after a successful rescore", async () => {
  const logs: string[] = [];
  const consoleSpy = vi.spyOn(console, "log").mockImplementation((...args: unknown[]) => {
    logs.push(args.map(String).join(" "));
  });
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: true,
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
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

test("retries a successful score when KV publish returns skipped-empty", async () => {
  const logs: string[] = [];
  const consoleSpy = vi.spyOn(console, "log").mockImplementation((...args: unknown[]) => {
    logs.push(args.map(String).join(" "));
  });
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    expectedGeneratedAt: null,
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
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  expect(logs.some((line) => line.includes("status=skipped-empty"))).toBe(true);
  consoleSpy.mockRestore();
});

test("does not warm the viewer display when a rescore KV publish returns error", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    expectedGeneratedAt: null,
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
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  consoleSpy.mockRestore();
  warnSpy.mockRestore();
});

test("does not commit or ack when the prediction cache bust fails", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  publishFinishPositionPredictionCacheMock.mockResolvedValue({
    busted: false,
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
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
  expect(completeRescoreRaceMock).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  errorSpy.mockRestore();
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
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
    internalToken: "secret-token",
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
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
    internalToken: "secret-token",
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
  expect(prepareMock).toHaveBeenCalledTimes(2);
  expect(bindMock).toHaveBeenCalledWith("20260101", "jra", "full", "05", "11", 2);
  expect(runMock).toHaveBeenCalledTimes(2);
});

test("old-runYmd message with force:true bypasses the guard and reaches the Container DO", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  await handleQueue(makeBatch([makeMessage({ force: true, runYmd: "20260101" })]), makeEnv());
  expect(isOldDateRunYmdMock).not.toHaveBeenCalled();
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(prepareMock).toHaveBeenCalledTimes(3);
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
  expect(prepareMock).toHaveBeenCalledTimes(3);
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
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(touchContainerSlotMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full status query failed category=jra runYmd=20260628 mode=full daysAhead=0 skipDedup=true busyRequeueCount=0 keibajo=02 race=01:",
    "Error: Focused-full status returned 503",
  );
  warnSpy.mockRestore();
});

test("keeps claims and slots unchanged when focused-full status JSON is invalid", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  stubFetchMock.mockResolvedValueOnce(
    new Response("{", { headers: { "Content-Type": "application/json" } }),
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
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(touchContainerSlotMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
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

test("continues a never-dispatched focused-full after resumed status is missing", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: true, state: "resumed" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: null,
      raceKey: "jra:20260822:07:09",
      status: "missing",
    }),
  );
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
    expect.objectContaining({ keibajoCode: "07", raceBango: "09", status: "success" }),
  );
  expect(claimContainerSlotMock).toHaveBeenCalledWith(
    expect.objectContaining({
      allowSameOwner: true,
      doName: "predict-jra-2",
      workKey: "focused-full:20260822:jra:07:09",
    }),
  );
  const predictRequest = (stubFetchMock.mock.calls[1] as unknown as [Request])[0];
  expect(predictRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=0&mode=full&runDate=20260822&keibajoCode=07&raceBango=09",
  );
  expect(reserveFocusedFullRaceRepairMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalledWith(expect.objectContaining({ force: true }), {
    delaySeconds: 30,
  });
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
});

test("preserves forced execution after six deliveries and a resumed missing status", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: true, state: "resumed" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: null,
      raceKey: "jra:20260822:07:09",
      status: "missing",
    }),
  );

  await handleQueue(
    makeBatch([
      makeMessage(
        {
          daysAhead: 0,
          force: true,
          forceRequestedAt: "2026-08-22T00:00:00.000Z",
          keibajoCode: "07",
          mode: "full",
          raceBango: "09",
          runYmd: "20260822",
          skipDedup: true,
        },
        6,
      ),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );

  expect(isFocusedFullPredictionCompleteMock).toHaveBeenCalledWith(
    expect.objectContaining({ notBefore: "2026-08-22T00:00:00.000Z" }),
  );
  expect(claimFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, keibajoCode: "07", raceBango: "09" }),
  );
  const predictRequest = (stubFetchMock.mock.calls[1] as unknown as [Request])[0];
  expect(predictRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=0&mode=full&runDate=20260822&keibajoCode=07&raceBango=09&force=1",
  );
  expect(reserveFocusedFullRaceRepairMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("does not restart an accepted forced pipeline when resumed status is running", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: true, state: "resumed" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: null,
      raceKey: "jra:20260822:07:09",
      status: "running",
    }),
  );

  await handleQueue(
    makeBatch([
      makeMessage(
        {
          daysAhead: 0,
          force: true,
          forceRequestedAt: "2026-08-22T00:00:00.000Z",
          keibajoCode: "07",
          mode: "full",
          raceBango: "09",
          runYmd: "20260822",
          skipDedup: true,
        },
        7,
      ),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );

  expect(claimFocusedFullRaceMock).toHaveBeenCalledWith(expect.objectContaining({ force: true }));
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  const statusRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(statusRequest.url).toBe(
    "http://do/focused-full-status?category=jra&keibajoCode=07&raceBango=09&runDate=20260822",
  );
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).not.toHaveBeenCalled();
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
  expect(controlSendMock).toHaveBeenCalledTimes(1);
  expect(
    (stubFetchMock.mock.invocationCallOrder[1] ?? 0) <
      (controlSendMock.mock.invocationCallOrder[0] ?? 0),
  ).toBe(true);
  expect(
    (controlSendMock.mock.invocationCallOrder[0] ?? 0) < (ackMock.mock.invocationCallOrder[0] ?? 0),
  ).toBe(true);
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

test("keeps direct focused-full success and cleans up on redelivery when both stop queues fail", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  controlSendMock.mockRejectedValueOnce(new Error("control queue unavailable"));
  sendMock.mockRejectedValueOnce(new Error("cleanup queue unavailable"));
  isFocusedFullPredictionCompleteMock.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
  const body = {
    daysAhead: 0,
    keibajoCode: "02",
    mode: "full" as const,
    raceBango: "01",
    runYmd: "20260628",
    skipDedup: true,
  };

  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());
  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());

  expect(parseNdjsonStreamMock).toHaveBeenCalledTimes(1);
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(prepareMock).not.toHaveBeenCalledWith(
    expect.stringContaining("finish_position_predict_retry_errors"),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(controlSendMock).toHaveBeenCalledTimes(2);
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("keeps ack-guard success and cleans up on redelivery when both stop queues fail", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  controlSendMock.mockRejectedValueOnce(new Error("control queue unavailable"));
  sendMock.mockRejectedValueOnce(new Error("cleanup queue unavailable"));
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  const body = {
    daysAhead: 0,
    keibajoCode: "02",
    mode: "full" as const,
    raceBango: "01",
    runYmd: "20260628",
    skipDedup: true,
  };

  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());
  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());

  expect(parseNdjsonStreamMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(prepareMock).not.toHaveBeenCalledWith(
    expect.stringContaining("finish_position_predict_retry_errors"),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(controlSendMock).toHaveBeenCalledTimes(2);
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("keeps polled status success and cleans up on redelivery when both stop queues fail", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  controlSendMock.mockRejectedValueOnce(new Error("control queue unavailable"));
  sendMock.mockRejectedValueOnce(new Error("cleanup queue unavailable"));
  isFocusedFullPredictionCompleteMock.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: null,
      raceKey: "jra:20260628:02:01",
      status: "success",
    }),
  );
  const body = {
    daysAhead: 0,
    keibajoCode: "02",
    mode: "full" as const,
    raceBango: "01",
    runYmd: "20260628",
    skipDedup: true,
  };

  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());
  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());

  expect(parseNdjsonStreamMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(prepareMock).not.toHaveBeenCalledWith(
    expect.stringContaining("finish_position_predict_retry_errors"),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(controlSendMock).toHaveBeenCalledTimes(2);
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("defers viewer repair after focused-full success without downgrading prediction", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  publishFinishPositionPredictionCacheMock.mockRejectedValueOnce(new Error("KV unavailable"));

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

  expect(sendMock).toHaveBeenCalledWith(
    {
      category: "jra",
      keibajoCode: "02",
      raceBango: "01",
      runYmd: "20260628",
      type: "prediction-cache-repair",
    },
    { delaySeconds: 30 },
  );
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(controlSendMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("acks focused-full success when viewer warm throws without downgrading prediction", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  warmPredictionCacheForRaceMock.mockRejectedValueOnce(new Error("viewer unavailable"));

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

  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ type: "prediction-cache-repair" }),
    { delaySeconds: 30 },
  );
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(controlSendMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] viewer cache warm best-effort failed category=jra runYmd=20260628 keibajo=02 race=01: Error: viewer unavailable",
  );
  warnSpy.mockRestore();
});

test("retries only viewer cleanup when immediate and deferred cache repair both fail", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  publishFinishPositionPredictionCacheMock.mockRejectedValueOnce(new Error("KV unavailable"));
  sendMock.mockRejectedValueOnce(new Error("repair queue unavailable"));

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

  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(prepareMock).not.toHaveBeenCalledWith(
    expect.stringContaining("finish_position_predict_retry_errors"),
  );
  expect(controlSendMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).not.toHaveBeenCalled();
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
    weightSnapshotCount: 3,
    weightSnapshotFetchedAt: "2026-06-19T14:30:00+09:00",
    weightSnapshotHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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

test("does not retry scoring after a viewer warm failure", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  warmPredictionCacheForRaceMock.mockRejectedValueOnce(new Error("viewer unavailable"));
  claimRescoreExecutionMock
    .mockResolvedValueOnce({ proceed: true })
    .mockResolvedValueOnce({ proceed: true });
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

  expect(stubFetchMock).toHaveBeenCalledTimes(2);
  expect(completeRescoreRaceMock).toHaveBeenCalledTimes(2);
  expect(completeRescoreRaceMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.any(Object),
    executionId: "predict-msg-1",
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260619",
    status: "success",
    weightSnapshotCount: 3,
    weightSnapshotFetchedAt: "2026-06-19T14:30:00+09:00",
    weightSnapshotHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  });
  expect(controlSendMock).toHaveBeenCalledTimes(2);
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(2);
  expect(retryMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] viewer cache warm best-effort failed category=jra runYmd=20260619 keibajo=05 race=11: Error: viewer unavailable",
  );
  warnSpy.mockRestore();
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
    weightSnapshotCount: 3,
    weightSnapshotFetchedAt: "2026-06-19T14:30:00+09:00",
    weightSnapshotHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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

test("caps a near-post rescore retry at fifteen seconds", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-08-24T02:00:00.000Z"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  claimRescoreExecutionMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          category: "jra",
          daysAhead: 0,
          keibajoCode: "01",
          mode: "rescore",
          raceBango: "01",
          raceStartAtJst: "2026-08-24T11:03:00+09:00",
          runYmd: "20260824",
        },
        8,
      ),
    ]),
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 15 });
  expect(ackMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
  vi.useRealTimers();
});

test("acks and drops a rescore after its scheduled post time", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-08-24T02:05:00.000Z"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "jra",
        daysAhead: 0,
        keibajoCode: "01",
        mode: "rescore",
        raceBango: "01",
        raceStartAtJst: "2026-08-24T11:04:00+09:00",
        runYmd: "20260824",
      }),
    ]),
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(claimRescoreExecutionMock).not.toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
  vi.useRealTimers();
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
    weightSnapshotCount: 3,
    weightSnapshotFetchedAt: "2026-06-19T14:30:00+09:00",
    weightSnapshotHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  });
  expect(
    (completeRescoreRaceMock.mock.invocationCallOrder[0] ?? 0) <
      (controlSendMock.mock.invocationCallOrder[0] ?? 0),
  ).toBe(true);
  expect(
    (completeRescoreRaceMock.mock.invocationCallOrder[0] ?? 0) <
      (warmPredictionCacheForRaceMock.mock.invocationCallOrder[0] ?? 0),
  ).toBe(false);
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
