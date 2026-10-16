// Run with bun. Tests for the queue consumer (DO-backed dedup).

import { beforeEach, expect, test, vi } from "vitest";
import {
  buildFocusedFullCompletionCallbackUrl,
  handleFocusedFullCompletionCallback,
} from "./focused-full-callback";
import { PredictRunCoordinator } from "./predict-run-coordinator";
import { verifyRescoreWeightGeneration } from "./scoring/rescore-preflight";

vi.mock("./scoring/rescore-preflight", () => ({ verifyRescoreWeightGeneration: vi.fn() }));
import type { RaceEntry } from "./cron-decision";
import type { DayBaseRepairAcquireResult } from "./day-base-repair";
import type { ParseNdjsonStreamOptions, PredictResultLine } from "./ndjson-stream";
import type { PredictionKvPublishResult } from "./prediction-kv-writer";
import type {
  MarketSignalHookResult,
  PrepareMarketSignalFoundationInput,
} from "./race-chain-market-signal-hook";
import type {
  ContainerControlMessage,
  Env,
  FocusedFullCompletionMessage,
  FocusedFullWatchTickMessage,
  PredictQueueBody,
  PredictQueueMessage,
} from "./types";

interface ClaimResult {
  executionId?: string;
  proceed: boolean;
  state?: string;
}

interface BaneiShadowMockResult {
  score: Record<string, unknown> | null;
  status: "cache_miss" | "ok" | "race_not_found";
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
  observeFocusedFullAuthorityMock,
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
  compareBaneiShadowWithPersistedMock,
  rescoreJraRaceMock,
  shadowBaneiRaceMock,
  warmPredictionCacheForRaceMock,
  warmPredictionCacheForCategoryMock,
  publishFinishPositionPredictionCacheMock,
  publishFinishPositionPredictionCacheForCategoryMock,
  isFocusedFullPredictionCompleteMock,
  isPerRaceFeatureCachePresentMock,
  isPerRaceRescoreReadyMock,
  getRunningStyleRaceReadinessMock,
} = vi.hoisted(() => {
  const cancelFocusedFullRaceRepair = vi.fn<
    typeof import("./do-state").cancelFocusedFullRaceRepair
  >(async () => undefined);
  const claimContainerSlot = vi.fn<typeof import("./do-state").claimContainerSlot>(async () => ({
    proceed: true,
  }));
  const observeFocusedFullAuthority = vi.fn<
    typeof import("./do-state").observeFocusedFullAuthority
  >(async () => ({ proceed: true }));
  const claimFocusedFullRace = vi.fn<typeof import("./do-state").claimFocusedFullRace>(
    async () => ({ proceed: true }),
  );
  const claimFocusedFullTerminalWatch = vi.fn<
    typeof import("./do-state").claimFocusedFullTerminalWatch
  >(async () => ({ proceed: true }));
  const claimRescoreExecution = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const claimRun = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const clearContainerSlot = vi.fn<typeof import("./do-state").clearContainerSlot>(
    async () => undefined,
  );
  const releaseContainerSlot = vi.fn(async () => undefined);
  const touchContainerSlot = vi.fn(async () => undefined);
  const completeFocusedFullRace = vi.fn<typeof import("./do-state").completeFocusedFullRace>(
    async () => undefined,
  );
  const completeFocusedFullTerminalWatch = vi.fn<
    typeof import("./do-state").completeFocusedFullTerminalWatch
  >(async () => undefined);
  const completeRescoreRace = vi.fn(async () => undefined);
  const completeRun = vi.fn(async () => undefined);
  const reserveFocusedFullRaceRepair = vi.fn<
    typeof import("./do-state").reserveFocusedFullRaceRepair
  >(async (): Promise<ClaimResult> => ({ proceed: true }));
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
  const compareBaneiShadowWithPersisted = vi.fn(async () => ({
    matched: true,
    maxAbsoluteScoreDifference: 0,
    persistedCount: 2,
    reason: "match" as const,
  }));
  const shadowBaneiRace = vi.fn(
    async (): Promise<BaneiShadowMockResult> => ({
      score: {
        gradeCode: null,
        modelVersion: "banei-cb-v9-sim-2011",
        predictions: [],
        raceId: "nar:2026:0619:83:03",
        shadowOnly: true as const,
        variant: "sim" as const,
      },
      status: "ok" as const,
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
    observeFocusedFullAuthorityMock: observeFocusedFullAuthority,
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
    compareBaneiShadowWithPersistedMock: compareBaneiShadowWithPersisted,
    rescoreJraRaceMock: rescoreJraRace,
    shadowBaneiRaceMock: shadowBaneiRace,
    warmPredictionCacheForCategoryMock: warmPredictionCacheForCategory,
    warmPredictionCacheForRaceMock: warmPredictionCacheForRace,
  };
});

const {
  acquireDayBaseRepairMock,
  clearDayBaseRepairReservationMock,
  enqueueDayBaseRepairOnceMock,
  getFocusedFullDayBaseReadinessMock,
  getRaceScopedDayBaseReadinessMock,
} = vi.hoisted(() => ({
  acquireDayBaseRepairMock: vi.fn(
    async (params: { generationId: string }): Promise<DayBaseRepairAcquireResult> => ({
      generationId: params.generationId,
      status: "acquired",
    }),
  ),
  clearDayBaseRepairReservationMock: vi.fn(async () => undefined),
  enqueueDayBaseRepairOnceMock: vi.fn(async () => "enqueued"),
  getFocusedFullDayBaseReadinessMock: vi.fn(async () => ({ ready: true, reason: "ready" })),
  getRaceScopedDayBaseReadinessMock: vi.fn(async () => ({
    ready: false,
    reason: "race-entry-set-mismatch",
  })),
}));

const {
  fetchCatalogRaceSourceSnapshotsMock,
  getDayBaseRaceFoundationReadinessMock,
  materializeDayBasePerRaceCacheMock,
} = vi.hoisted(() => ({
  fetchCatalogRaceSourceSnapshotsMock: vi.fn(async () => new Map()),
  getDayBaseRaceFoundationReadinessMock: vi.fn(async () => ({ ready: true, reason: "ready" })),
  materializeDayBasePerRaceCacheMock: vi.fn(async () => ({
    featureHash: "feature-hash",
    manifestKey: "manifest-key",
    raceCount: 1,
    rowCount: 12,
    status: "materialized" as const,
  })),
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
  vi.fn(
    async (_env: Env, _message: ContainerControlMessage, afterDestroyed?: () => Promise<void>) => {
      await afterDestroyed?.();
      return true;
    },
  ),
);

const claimContainerSlotStopMock = vi.hoisted(() =>
  vi.fn<typeof import("./do-state").claimContainerSlotStop>(async () => ({
    allowed: false,
    state: "blocked",
  })),
);

const prepareMarketSignalFoundationBestEffortMock = vi.hoisted(() =>
  vi.fn(
    async (_input: PrepareMarketSignalFoundationInput): Promise<MarketSignalHookResult> => ({
      reason: "disabled",
      status: "unavailable",
    }),
  ),
);

vi.mock("./attested-race-cache-assembler", () => ({
  assembleAttestedRaceCaches: materializeDayBasePerRaceCacheMock,
}));

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
  acquireDayBaseRepair: acquireDayBaseRepairMock,
  clearDayBaseRepairReservation: clearDayBaseRepairReservationMock,
  enqueueDayBaseRepairOnce: enqueueDayBaseRepairOnceMock,
}));

vi.mock("./focused-full-day-base-readiness", () => ({
  getFocusedFullDayBaseReadiness: getFocusedFullDayBaseReadinessMock,
}));

vi.mock("./race-scoped-day-base-readiness", () => ({
  getRaceScopedDayBaseReadiness: getRaceScopedDayBaseReadinessMock,
}));

vi.mock("./day-base-race-materializer", () => ({
  getDayBaseRaceFoundationReadiness: getDayBaseRaceFoundationReadinessMock,
  materializeDayBasePerRaceCache: materializeDayBasePerRaceCacheMock,
}));

vi.mock("./race-source-snapshot", () => ({
  fetchCatalogRaceSourceSnapshots: fetchCatalogRaceSourceSnapshotsMock,
}));

vi.mock("./rescore-attestation", () => ({
  addRescoreAttestationToUrl: addRescoreAttestationToUrlMock,
  createRescoreAttestation: createRescoreAttestationMock,
}));

vi.mock("./do-state", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./do-state")>()),
  cancelFocusedFullRaceRepair: cancelFocusedFullRaceRepairMock,
  claimContainerSlot: claimContainerSlotMock,
  claimContainerSlotStop: claimContainerSlotStopMock,
  claimFocusedFullRace: claimFocusedFullRaceMock,
  observeFocusedFullAuthority: observeFocusedFullAuthorityMock,
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
  compareBaneiShadowWithPersisted: compareBaneiShadowWithPersistedMock,
  rescoreJraRace: rescoreJraRaceMock,
  shadowBaneiRace: shadowBaneiRaceMock,
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

const {
  getPredictionReadinessMock,
  hasCompleteCategoryPreWeightCoverageMock,
  prewarmCategoryWithOutcomeMock,
} = vi.hoisted(() => ({
  getPredictionReadinessMock: vi.fn(
    async (): Promise<{
      races: Array<{
        keibajoCode: string;
        postWeight: { complete: boolean; weightSnapshotAt: string | null };
        raceBango: string;
        source: string;
      }>;
    }> => ({ races: [] }),
  ),
  hasCompleteCategoryPreWeightCoverageMock: vi.fn(async () => false),
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

vi.mock("./prediction-readiness", () => ({
  getPredictionReadiness: getPredictionReadinessMock,
  hasCompleteCategoryPreWeightCoverage: hasCompleteCategoryPreWeightCoverageMock,
}));

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
const firstMock = vi.fn(async (): Promise<unknown> => null);
const bindMock = vi.fn(() => ({ first: firstMock, run: runMock }));
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
const getStateMock = vi.fn(async () => ({ status: "healthy" }));
const getMock = vi.fn(() => ({ fetch: stubFetchMock, getState: getStateMock }));

const makeEnv = (): Env => ({
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: { prepare: prepareMock } as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {
    get: getMock,
    idFromName: idFromNameMock,
  } as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PC_KEIBA_R2_CATALOG: { fetch: vi.fn() } as unknown as Fetcher,
  PC_KEIBA_VIEWER_INTERNAL_TOKEN: "secret-token",
  PREDICT_DAYS_AHEAD: "2",
  CONTAINER_CONTROL_QUEUE: {
    send: controlSendMock,
  } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
  FOCUSED_FULL_COMPLETION_QUEUE: {
    send: watchSendMock,
  } as unknown as NonNullable<Env["FOCUSED_FULL_COMPLETION_QUEUE"]>,
  PREDICT_QUEUE: { send: sendMock } as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {
    idFromName: (name: string) => name,
    get: () => ({ fetch: async () => Response.json({ allowed: true }) }),
  } as unknown as Env["PREDICT_RUN_COORDINATOR"],
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
      executionId: "execution-1",
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
    executionId: "execution-1",
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
  getPredictionReadinessMock.mockReset();
  getPredictionReadinessMock.mockResolvedValue({ races: [] });
  hasCompleteCategoryPreWeightCoverageMock.mockReset();
  hasCompleteCategoryPreWeightCoverageMock.mockResolvedValue(false);
  sendMock.mockClear();
  sendMock.mockResolvedValue(undefined);
  watchSendMock.mockClear();
  watchSendMock.mockResolvedValue(undefined);
  consumeContainerStopMock.mockClear();
  claimContainerSlotStopMock.mockClear();
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
  firstMock.mockReset();
  prepareMock.mockClear();
  idFromNameMock.mockClear();
  getMock.mockClear();
  getStateMock.mockClear();
  getStateMock.mockResolvedValue({ status: "healthy" });
  stubFetchMock.mockReset();
  claimContainerSlotMock.mockClear();
  observeFocusedFullAuthorityMock.mockReset().mockResolvedValue({ proceed: true });
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
  compareBaneiShadowWithPersistedMock.mockClear();
  shadowBaneiRaceMock.mockClear();
  rescoreJraRaceMock.mockClear();
  warmPredictionCacheForRaceMock.mockClear();
  warmPredictionCacheForCategoryMock.mockClear();
  publishFinishPositionPredictionCacheMock.mockClear();
  publishFinishPositionPredictionCacheForCategoryMock.mockClear();
  isFocusedFullPredictionCompleteMock.mockClear();
  isPerRaceFeatureCachePresentMock.mockClear();
  isPerRaceRescoreReadyMock.mockClear();
  getRunningStyleRaceReadinessMock.mockClear();
  acquireDayBaseRepairMock.mockReset().mockImplementation(async (params) => ({
    generationId: params.generationId,
    status: "acquired",
  }));
  clearDayBaseRepairReservationMock.mockClear();
  enqueueDayBaseRepairOnceMock.mockClear();
  getFocusedFullDayBaseReadinessMock.mockClear();
  getFocusedFullDayBaseReadinessMock.mockResolvedValue({ ready: true, reason: "ready" });
  getRaceScopedDayBaseReadinessMock.mockClear();
  getRaceScopedDayBaseReadinessMock.mockResolvedValue({
    ready: false,
    reason: "race-entry-set-mismatch",
  });
  getDayBaseRaceFoundationReadinessMock.mockClear();
  getDayBaseRaceFoundationReadinessMock.mockResolvedValue({ ready: true, reason: "ready" });
  materializeDayBasePerRaceCacheMock.mockClear();
  materializeDayBasePerRaceCacheMock.mockResolvedValue({
    featureHash: "feature-hash",
    manifestKey: "manifest-key",
    raceCount: 1,
    rowCount: 12,
    status: "materialized",
  });
  fetchCatalogRaceSourceSnapshotsMock.mockClear();
  fetchCatalogRaceSourceSnapshotsMock.mockResolvedValue(new Map());
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
  compareBaneiShadowWithPersistedMock.mockResolvedValue({
    matched: true,
    maxAbsoluteScoreDifference: 0,
    persistedCount: 2,
    reason: "match",
  });
  shadowBaneiRaceMock.mockResolvedValue({
    score: {
      gradeCode: null,
      modelVersion: "banei-cb-v9-sim-2011",
      predictions: [],
      raceId: "nar:2026:0619:83:03",
      shadowOnly: true,
      variant: "sim",
    },
    status: "ok",
  });
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
  claimContainerSlotMock.mockResolvedValue({ proceed: true, executionId: "execution-1" });
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

test("does not use the D1 running-style mirror as a second cache authority", async () => {
  getRunningStyleRaceReadinessMock.mockResolvedValueOnce([
    {
      race: { category: "jra", keibajoCode: "05", raceBango: "11" },
      reason: "prediction-count-7-of-14",
    },
  ]);
  const message = makeMessage({ skipDedup: true });

  await handleQueue(makeBatch([message]), makeEnv());

  expect(getRunningStyleRaceReadinessMock).not.toHaveBeenCalled();
  expect(getDayBaseRaceFoundationReadinessMock).toHaveBeenCalled();
  expect(claimFocusedFullRaceMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("allows an explicitly forced focused-full through stale RS sync state", async () => {
  getRunningStyleRaceReadinessMock.mockResolvedValueOnce([
    {
      race: { category: "jra", keibajoCode: "05", raceBango: "11" },
      reason: "sync-failed",
    },
  ]);

  await handleQueue(
    makeBatch([
      makeMessage({
        force: true,
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

  await handleQueue(makeBatch([makeMessage({ skipDedup: true }, 2)]), makeEnv());

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
  expect(getRaceScopedDayBaseReadinessMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.anything(),
    keibajoCode: "05",
    raceBango: "11",
    requireStableSourceHash: true,
    runYmd: "20260603",
  });
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full day-base deferred before claim category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=true busyRequeueCount=0 keibajo=05 race=11 reason=source-row-count-26-of-392 raceReason=race-entry-set-mismatch repair=enqueued attempts=2 delaySeconds=50",
  );
  warnSpy.mockRestore();
});

test("uses a fresh race-scoped foundation while another race makes the category stale", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "rs-row-count-455-of-454",
  });
  getRaceScopedDayBaseReadinessMock.mockResolvedValueOnce({ ready: true, reason: "ready" });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await handleQueue(makeBatch([makeMessage({ skipDedup: true }, 1)]), makeEnv());

  expect(enqueueDayBaseRepairOnceMock).not.toHaveBeenCalled();
  expect(claimFocusedFullRaceMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] using race-scoped foundation while category day-base is stale category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=true busyRequeueCount=0 keibajo=05 race=11 categoryReason=rs-row-count-455-of-454",
  );
  warnSpy.mockRestore();
});

test("uses a stable race source snapshot when another race changes the Catalog watermark", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "source-watermark-mismatch",
  });
  getRaceScopedDayBaseReadinessMock.mockResolvedValueOnce({ ready: true, reason: "ready" });

  await handleQueue(makeBatch([makeMessage({ skipDedup: true }, 1)]), makeEnv());

  expect(getRaceScopedDayBaseReadinessMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.anything(),
    keibajoCode: "05",
    raceBango: "11",
    requireStableSourceHash: true,
    runYmd: "20260603",
  });
  expect(enqueueDayBaseRepairOnceMock).not.toHaveBeenCalled();
  expect(claimFocusedFullRaceMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).toHaveBeenCalled();
});

test("repairs the category when the race-scoped fallback probe fails", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "rs-row-count-455-of-454",
  });
  getRaceScopedDayBaseReadinessMock.mockRejectedValueOnce(new Error("Catalog unavailable"));

  await handleQueue(makeBatch([makeMessage({ skipDedup: true }, 1)]), makeEnv());

  expect(enqueueDayBaseRepairOnceMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
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

test("does not clear a repair reservation from a focused race without its owner generation", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), makeEnv());

  expect(claimFocusedFullRaceMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).toHaveBeenCalled();
  expect(clearDayBaseRepairReservationMock).not.toHaveBeenCalled();
  expect(errorSpy).not.toHaveBeenCalled();
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
  const hotFetch = vi.fn(async () => Response.json({ latest: null }));
  const env = {
    ...makeEnv(),
    REALTIME_HOT: { fetch: hotFetch },
    WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED: "1",
  };

  await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), env);

  expect(prepareMarketSignalFoundationBestEffortMock).toHaveBeenCalledWith({
    category: "jra",
    env: {
      DAY_BASE_MANIFEST_REQUIRED: env.DAY_BASE_MANIFEST_REQUIRED,
      FEATURES_CACHE: env.FEATURES_CACHE,
      FINISH_POSITION_CRON_DB: env.FINISH_POSITION_CRON_DB,
      WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED: "1",
    },
    fetchImpl: expect.any(Function),
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260603",
  });
  const hookInput = prepareMarketSignalFoundationBestEffortMock.mock.calls[0]?.[0];
  if (hookInput === undefined) throw new Error("expected market-signal hook input");
  await hookInput.fetchImpl("https://sync-realtime-data-hot.internal/api/odds/test");
  expect(hotFetch).toHaveBeenCalledWith(
    "https://sync-realtime-data-hot.internal/api/odds/test",
    undefined,
  );
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

test("fails closed before claims when exact foundation remains absent after Worker warm", async () => {
  getDayBaseRaceFoundationReadinessMock
    .mockResolvedValueOnce({ ready: false, reason: "foundation-miss" })
    .mockResolvedValueOnce({ ready: false, reason: "foundation-miss" });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await handleQueue(makeBatch([makeMessage({ skipDedup: true }, 1)]), makeEnv());

  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(materializeDayBasePerRaceCacheMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.anything(),
    runYmd: "20260603",
  });
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full foundation deferred before claim category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=true busyRequeueCount=0 keibajo=05 race=11 reason=foundation-miss attempts=1 delaySeconds=30",
  );
  warnSpy.mockRestore();
});

test("retries without claims when Worker foundation materialization fails", async () => {
  getDayBaseRaceFoundationReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "foundation-miss",
  });
  materializeDayBasePerRaceCacheMock.mockRejectedValueOnce(new Error("R2 unavailable"));
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
    "[predict-queue] focused-full day-base readiness failed before claim category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=true busyRequeueCount=0 keibajo=05 race=11:",
    "Error: R2 unavailable",
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

test("focused full pins the published pointer in compatibility mode and retries pending", async () => {
  firstMock.mockResolvedValueOnce({
    generation_id: "owner-1",
    object_etag: "source-etag",
    object_key: "feat-daybase/catalog-v1/jra/20260628/generations/owner-1/features.parquet",
    state: "published",
    watermark_json: JSON.stringify({
      maxDataSakuseiNengappi: "20260628",
      rowCount: 12,
      rsPredictedAtMax: "none",
      rsRowCount: 0,
    }),
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
    { ...makeEnv(), DAY_BASE_MANIFEST_REQUIRED: "0" },
  );
  expect(stubFetchMock).toHaveBeenCalledTimes(2);
  const request = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(request.url).toBe(
    "http://do/predict?category=jra&daysAhead=0&mode=full&runDate=20260628&allowRaceScopedDayBase=1&dayBaseKey=feat-daybase%2Fcatalog-v1%2Fjra%2F20260628%2Fgenerations%2Fowner-1%2Ffeatures.parquet&dayBaseEtag=source-etag&keibajoCode=02&raceBango=01",
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  firstMock.mockResolvedValueOnce({ state: "pending" });
  stubFetchMock.mockClear();
  claimFocusedFullRaceMock.mockClear();
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
    { ...makeEnv(), DAY_BASE_MANIFEST_REQUIRED: "1" },
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(claimFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  firstMock.mockResolvedValueOnce(null);
  retryMock.mockClear();
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
    { ...makeEnv(), DAY_BASE_MANIFEST_REQUIRED: "0" },
  );
  expect(stubFetchMock).toHaveBeenCalledTimes(2);
  const legacyRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(legacyRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=0&mode=full&runDate=20260628&allowRaceScopedDayBase=1&keibajoCode=02&raceBango=01",
  );
  expect(retryMock).not.toHaveBeenCalled();
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
      "http://do/predict?category=jra&daysAhead=0&mode=full&runDate=20260628&allowRaceScopedDayBase=1&keibajoCode=02&raceBango=01",
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
    "http://do/predict?category=jra&daysAhead=2&mode=full&runDate=20260822&allowRaceScopedDayBase=1&keibajoCode=07&raceBango=11",
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

  expect(getDayBaseRaceFoundationReadinessMock).toHaveBeenCalledWith({
    category: "jra",
    env,
    raceNumber: "03",
    runYmd: "20260823",
    venueCode: "01",
  });
  expect(raceIdFromName).toHaveBeenCalledWith("race-chain-predict-jra");
  expect(raceGet).toHaveBeenCalledTimes(2);
  expect(claimContainerSlotMock).toHaveBeenCalledWith(
    expect.objectContaining({
      doName: "race-chain-predict-jra",
      replaceWorkKey: "warm-reuse:race-chain-predict-jra",
      workKey: "focused-full:20260823:jra:01:03",
    }),
  );
  expect(idFromNameMock).not.toHaveBeenCalled();
});

test("retries DAY_BASE_REQUIRED fail-closed and schedules a day-base repair", async () => {
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

  expect(sendMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(enqueueDayBaseRepairOnceMock).toHaveBeenCalledWith(
    expect.objectContaining({ category: "jra", force: true, runYmd: "20260603" }),
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] race-chain cache HIT lost; retrying fail-closed category=jra runYmd=20260603 mode=full daysAhead=2 skipDedup=true busyRequeueCount=0 keibajo=05 race=11 error=DayBaseRequiredError: DAY_BASE_REQUIRED: source unavailable delaySeconds=30",
  );
  warnSpy.mockRestore();
});

test("retries the original race-chain delivery without a legacy replacement", async () => {
  parseNdjsonStreamMock.mockResolvedValueOnce({
    category: "jra",
    error: "DAY_BASE_REQUIRED: transient source failure",
    racesPredicted: 0,
    status: "error",
    type: "result",
  });
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
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
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
    "http://do/predict?category=nar&daysAhead=0&mode=full&runDate=20260712&allowRaceScopedDayBase=1&keibajoCode=54&raceBango=10&cardMaxRaceBango=10",
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
  expect(getFocusedFullDayBaseReadinessMock).not.toHaveBeenCalled();
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

test("acks an old missing-status repair without resurrecting its Container", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  isOldDateRunYmdMock.mockReturnValue(true);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);
  stubFetchMock.mockResolvedValueOnce(Response.json({ found: false })).mockResolvedValueOnce(
    Response.json({
      error: null,
      raceKey: "jra:20260705:03:12",
      status: "missing",
    }),
  );

  await handleQueue(
    makeBatch([
      makeMessage({
        force: true,
        keibajoCode: "03",
        mode: "full",
        raceBango: "12",
        runYmd: "20260705",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );

  expect(reserveFocusedFullRaceRepairMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    expect.stringContaining(
      "Skipping old-dated predict message category=jra runYmd=20260705 mode=full",
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
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
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
            rescoreLifecycle: {
              executionId: "rescore-message-1",
              weightSnapshotCount: 3,
              weightSnapshotFetchedAt: "2026-08-24T00:55:00.000Z",
              weightSnapshotHash: "weight-hash",
            },
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
  expect(completeRescoreRaceMock).toHaveBeenNthCalledWith(1, {
    category: "ban-ei",
    env: expect.anything(),
    executionId: "rescore-message-1",
    keibajoCode: "83",
    raceBango: "12",
    runYmd: "20260824",
    status: "viewer_warm_complete",
    weightSnapshotCount: 3,
    weightSnapshotFetchedAt: "2026-08-24T00:55:00.000Z",
    weightSnapshotHash: "weight-hash",
  });
  expect(completeRescoreRaceMock).toHaveBeenNthCalledWith(
    2,
    expect.objectContaining({ status: "success" }),
  );
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
    "http://do/predict?category=jra&daysAhead=0&mode=full&runDate=20260712&allowRaceScopedDayBase=1&keibajoCode=02&raceBango=01&force=1",
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
    "http://do/predict?category=jra&daysAhead=0&mode=full&runDate=20260712&allowRaceScopedDayBase=1&keibajoCode=02&raceBango=01&force=1",
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

test("retries without starting a container when focused full completion guard fails", async () => {
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
    expect(stubFetchMock).not.toHaveBeenCalled();
    expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
    expect(ackMock).not.toHaveBeenCalled();
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
    "http://do/predict?category=nar&daysAhead=2&mode=full&runDate=20260629&allowRaceScopedDayBase=1&keibajoCode=35&raceBango=01",
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
      lastProgressAtMs: Date.now() - 1000,
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

test("deadline reached during preflight prevents Container startup", async () => {
  const nowSpy = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-09-13T00:00:00Z"));
  vi.mocked(verifyRescoreWeightGeneration).mockImplementationOnce(async () => {
    nowSpy.mockReturnValue(Date.parse("2026-09-13T00:00:02Z"));
  });
  try {
    await handleQueue(
      makeBatch([
        makeMessage({
          daysAhead: 0,
          keibajoCode: "05",
          mode: "rescore",
          raceBango: "11",
          runYmd: "20260913",
          raceStartAtJst: "2026-09-13T00:00:01Z",
        }),
      ]),
      makeEnv(),
    );
    expect(stubFetchMock).not.toHaveBeenCalled();
    expect(ackMock).toHaveBeenCalledOnce();
  } finally {
    nowSpy.mockRestore();
  }
});

test("weight preflight failure does not start a prediction Container", async () => {
  vi.mocked(verifyRescoreWeightGeneration).mockRejectedValueOnce(new Error("HTTP 502"));
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
  expect(verifyRescoreWeightGeneration).toHaveBeenCalled();
  expect(stubFetchMock).not.toHaveBeenCalled();
});

test("obsolete weight generation does not start a prediction Container", async () => {
  vi.mocked(verifyRescoreWeightGeneration).mockRejectedValueOnce(
    new Error("post-weight snapshot generation mismatch: race"),
  );
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

test("logs allowlisted operational progress but not generic progress when debug is off", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockImplementationOnce(
    async (
      _body: ReadableStream<Uint8Array>,
      options?: ParseNdjsonStreamOptions,
    ): Promise<PredictResultLine> => {
      options?.onProgress?.({ type: "progress", stage: "predict", elapsed_s: 12.3 });
      options?.onProgress?.({
        type: "progress",
        stage:
          "step=racechain-layer index=1/1 status=done category=jra script=cell.py target_race=05:11 elapsed_seconds=42.0",
        elapsed_s: 42,
      });
      return { type: "result", racesPredicted: 5, category: "jra", status: "success" };
    },
  );
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(consoleSpy).not.toHaveBeenCalledWith(
    "Predict progress category=jra runYmd=20260603 keibajo=05 race=11 stage=predict elapsed=12.3",
  );
  expect(consoleSpy).toHaveBeenCalledWith(
    "Predict progress category=jra runYmd=20260603 keibajo=05 race=11 stage=step=racechain-layer index=1/1 status=done category=jra script=cell.py target_race=05:11 elapsed_seconds=42.0 elapsed=42",
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

test("acks and cleans up a delayed day-base pickup after category coverage completes", async () => {
  hasCompleteCategoryPreWeightCoverageMock.mockResolvedValueOnce(true);
  const pickup = {
    ack: ackMock,
    body: {
      attempt: 5,
      category: "nar",
      generationId: "stale-generation",
      runYmd: "20260910",
      type: "day-base-pickup",
    },
    retry: retryMock,
  };
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await handleQueue({ messages: [pickup] } as never, makeEnv());

  expect(consumeDayBasePickupMock).not.toHaveBeenCalled();
  expect(clearDayBaseRepairReservationMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "nar",
      env: expect.any(Object),
      runYmd: "20260910",
    }),
  );
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ workKey: "day-base:20260910:nar:stale-generation" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(logSpy).toHaveBeenCalledWith(
    "[predict-queue] day-base work skipped after complete coverage type=day-base-pickup category=nar runYmd=20260910",
  );
  logSpy.mockRestore();
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

test("cleans an explicitly forced historical day-base pickup", async () => {
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
  expect(clearDayBaseRepairReservationMock).toHaveBeenCalledWith({
    category: "jra",
    env: expect.any(Object),
    runYmd: "20260823",
  });
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
    expect.objectContaining({
      method: "GET",
      url: "http://do/predict?category=jra&daysAhead=0&mode=rescore&keibajoCode=05&raceBango=11&raceStartAtJst=2099-01-01T00%3A00%3A00%2B09%3A00&runDate=20260619&weightSnapshotCount=3&weightSnapshotFetchedAt=2026-06-19T14%3A30%3A00%2B09%3A00&weightSnapshotHash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    }),
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
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "score_complete" }),
  );
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  warnSpy.mockRestore();
  consoleSpy.mockRestore();
});

test("resumes enabled Worker delivery without rerunning Worker or Container scoring", async () => {
  claimRescoreExecutionMock.mockResolvedValue({ proceed: false, state: "neon_complete" });
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
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledTimes(1);
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("resumes legacy Worker delivery whose generation predates weight identity fields", async () => {
  claimRescoreExecutionMock.mockResolvedValue({ proceed: false, state: "neon_complete" });
  await handleQueue(
    makeBatch([
      makeMessage({
        daysAhead: 0,
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
        weightSnapshotCount: undefined,
        weightSnapshotFetchedAt: undefined,
        weightSnapshotHash: undefined,
      }),
    ]),
    { ...makeEnv(), JRA_WORKER_RESCORE_ENABLED: "1" },
  );
  expect(sendMock).not.toHaveBeenCalled();
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
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
    expect.objectContaining({ status: "kv_complete" }),
  );
  expect(completeRescoreRaceMock).not.toHaveBeenCalledWith(
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
    expect.objectContaining({ status: "kv_complete" }),
  );
  expect(completeRescoreRaceMock).not.toHaveBeenCalledWith(
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

test("retries delivery without semantic downgrade when warm-repair enqueue fails", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  warmPredictionCacheForRaceMock.mockResolvedValue(false);
  sendMock.mockRejectedValueOnce(new Error("repair queue unavailable"));

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

  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "neon_complete" }),
  );
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  expect(completeRescoreRaceMock).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
  errorSpy.mockRestore();
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

test("acks an old NAR rescore generation after a newer post-weight generation is fully delivered", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    category: "nar",
    error:
      "RealtimeWeightFetchError: post-weight snapshot generation mismatch: race_key=nar:2026:0619:44:01",
    racesPredicted: 0,
    status: "error",
    type: "result",
  });
  getPredictionReadinessMock.mockResolvedValue({
    races: [
      {
        keibajoCode: "44",
        postWeight: { complete: true, weightSnapshotAt: "2026-06-19T14:35:00+09:00" },
        raceBango: "01",
        source: "nar",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValueOnce(true);
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
  expect(getPredictionReadinessMock).toHaveBeenCalledWith(
    expect.objectContaining({ category: "nar", runYmd: "20260619" }),
  );
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(isFocusedFullPredictionCompleteMock).toHaveBeenCalledWith(
    expect.objectContaining({
      notBefore: "2026-06-19T14:35:00+09:00",
      requireKv: true,
    }),
  );
  expect(consoleSpy).toHaveBeenCalledWith(
    expect.stringContaining("superseded rescore complete; acknowledging old generation"),
  );
  consoleSpy.mockRestore();
});

test("acks a phantom generation after the authoritative older snapshot is fully delivered", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    category: "nar",
    error: "post-weight snapshot generation mismatch",
    racesPredicted: 0,
    status: "error",
    type: "result",
  });
  getPredictionReadinessMock.mockResolvedValue({
    races: [
      {
        keibajoCode: "44",
        postWeight: { complete: true, weightSnapshotAt: "2026-06-19T14:11:00+09:00" },
        raceBango: "01",
        source: "nar",
      },
    ],
  });
  isFocusedFullPredictionCompleteMock.mockResolvedValueOnce(true);
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
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
  expect(isFocusedFullPredictionCompleteMock).toHaveBeenCalledWith(
    expect.objectContaining({ notBefore: "2026-06-19T14:11:00+09:00", requireKv: true }),
  );
  consoleSpy.mockRestore();
});

test("keeps retrying a generation mismatch until the newer post-weight delivery is complete", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    category: "nar",
    error: "post-weight snapshot generation mismatch",
    racesPredicted: 0,
    status: "error",
    type: "result",
  });
  getPredictionReadinessMock.mockResolvedValue({
    races: [
      {
        keibajoCode: "44",
        postWeight: { complete: false, weightSnapshotAt: "2026-06-19T14:35:00+09:00" },
        raceBango: "01",
        source: "nar",
      },
    ],
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
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

test("keeps retrying a mismatch against the same authoritative weight generation", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    category: "nar",
    error: "post-weight snapshot generation mismatch",
    racesPredicted: 0,
    status: "error",
    type: "result",
  });
  getPredictionReadinessMock.mockResolvedValue({
    races: [
      {
        keibajoCode: "44",
        postWeight: { complete: true, weightSnapshotAt: "2026-06-19T14:30:00+09:00" },
        raceBango: "01",
        source: "nar",
      },
    ],
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(isFocusedFullPredictionCompleteMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("keeps retrying a mismatch when the current race has no weight snapshot", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    category: "nar",
    error: "post-weight snapshot generation mismatch",
    racesPredicted: 0,
    status: "error",
    type: "result",
  });
  getPredictionReadinessMock.mockResolvedValue({
    races: [
      {
        keibajoCode: "44",
        postWeight: { complete: false, weightSnapshotAt: null },
        raceBango: "01",
        source: "nar",
      },
    ],
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(isFocusedFullPredictionCompleteMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("fails closed on a generation mismatch with an invalid requested timestamp", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    category: "nar",
    error: "post-weight snapshot generation mismatch",
    racesPredicted: 0,
    status: "error",
    type: "result",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
        weightSnapshotFetchedAt: "invalid",
      }),
    ]),
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  expect(getPredictionReadinessMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("fails closed when superseded-generation readiness cannot be checked", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    category: "jra",
    error: "post-weight snapshot generation mismatch",
    racesPredicted: 0,
    status: "error",
    type: "result",
  });
  getPredictionReadinessMock.mockRejectedValue(new Error("readiness unavailable"));
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "jra",
        keibajoCode: "05",
        mode: "rescore",
        raceBango: "11",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    expect.stringContaining("superseded rescore readiness unavailable"),
  );
  warnSpy.mockRestore();
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
  expect(shadowBaneiRaceMock).not.toHaveBeenCalled();
  expect(idFromNameMock).toHaveBeenCalledWith("predict-ban-ei");
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("shadows Ban-ei rescore, compares persisted Container output, and never replaces it", async () => {
  const logs: string[] = [];
  const consoleSpy = vi.spyOn(console, "log").mockImplementation((...args: unknown[]) => {
    logs.push(args.map(String).join(" "));
  });
  const env = { ...makeEnv(), BANEI_WORKER_RESCORE_SHADOW_ENABLED: "1" };
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
    env,
  );
  expect(shadowBaneiRaceMock).toHaveBeenCalledWith({
    attestation: expect.objectContaining({ featureCacheEtag: "feature-etag" }),
    env,
    fetchImpl: expect.any(Function),
    message: expect.objectContaining({ category: "ban-ei", raceBango: "07" }),
  });
  expect(compareBaneiShadowWithPersistedMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(logs.some((line) => line.includes('"event":"banei-worker-rescore-shadow-parity"'))).toBe(
    true,
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("keeps Container serving when Ban-ei shadow or parity observation fails", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  shadowBaneiRaceMock.mockRejectedValueOnce(new Error("shadow unavailable"));
  const env = { ...makeEnv(), BANEI_WORKER_RESCORE_SHADOW_ENABLED: "1" };
  const body = {
    category: "ban-ei",
    daysAhead: 0,
    keibajoCode: "83",
    mode: "rescore",
    raceBango: "07",
    runYmd: "20260619",
  } satisfies Partial<PredictQueueMessage>;
  await handleQueue(makeBatch([makeMessage(body)]), env);
  expect(compareBaneiShadowWithPersistedMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);

  shadowBaneiRaceMock.mockResolvedValueOnce({
    score: {
      gradeCode: null,
      modelVersion: "banei-cb-v9-sim-2011",
      predictions: [],
      raceId: "nar:2026:0619:83:07",
      shadowOnly: true,
      variant: "sim",
    },
    status: "ok",
  });
  compareBaneiShadowWithPersistedMock.mockRejectedValueOnce(new Error("Neon unavailable"));
  await handleQueue(makeBatch([makeMessage(body)]), env);
  expect(stubFetchMock).toHaveBeenCalledTimes(2);
  expect(ackMock).toHaveBeenCalledTimes(2);
  expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("Ban-ei Worker shadow unavailable"));
  expect(warnSpy).toHaveBeenCalledWith(
    expect.stringContaining("Ban-ei Worker shadow parity unavailable"),
  );
  warnSpy.mockRestore();
});

test("does not compare a Ban-ei shadow cache miss", async () => {
  shadowBaneiRaceMock.mockResolvedValueOnce({ score: null, status: "cache_miss" });
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
    { ...makeEnv(), BANEI_WORKER_RESCORE_SHADOW_ENABLED: "1" },
  );
  expect(compareBaneiShadowWithPersistedMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
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
    '"watchId":"focused-full:20260628:jra:02:01:predict-msg-1:execution-1"',
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
    executionId: "execution-1",
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

test("leaves status polling failure to Cloudflare redelivery without explicit retry", async () => {
  stubFetchMock.mockRejectedValueOnce(new Error("status unavailable"));
  await expect(
    handleQueue(makeBatch([makeFocusedFullWatchTickMessage()]), makeEnv()),
  ).rejects.toThrow("status unavailable");
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
});

test("acks a tick with a durably saved successor when fast Queue delivery fails", async () => {
  stubFetchMock.mockResolvedValueOnce(
    Response.json({ error: null, raceKey: "jra:20260603:05:11", status: "running" }),
  );
  watchSendMock.mockRejectedValueOnce(new Error("tick send unavailable"));
  await handleQueue(makeBatch([makeFocusedFullWatchTickMessage()]), makeEnv());
  expect(registerFocusedFullWatchOutboxMock).toHaveBeenCalledOnce();
  expect(registerFocusedFullWatchOutboxMock).toHaveBeenCalledWith({
    delaySeconds: 30,
    env: expect.anything(),
    message: expect.objectContaining({ type: "focused-full-watch-tick", watchId: "watch-tick-1" }),
    outboxId: expect.stringMatching(/^focused-full-watch-tick:watch-tick-1:/u),
  });
  expect(clearFocusedFullWatchOutboxMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledOnce();
  expect(retryMock).not.toHaveBeenCalled();
});

test("does not ack a tick if its successor cannot be durably registered", async () => {
  stubFetchMock.mockResolvedValueOnce(
    Response.json({ error: null, raceKey: "jra:20260603:05:11", status: "running" }),
  );
  registerFocusedFullWatchOutboxMock.mockRejectedValueOnce(new Error("outbox unavailable"));
  await expect(
    handleQueue(makeBatch([makeFocusedFullWatchTickMessage()]), makeEnv()),
  ).rejects.toThrow("outbox unavailable");
  expect(watchSendMock).not.toHaveBeenCalled();
  expect(clearFocusedFullWatchOutboxMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).not.toHaveBeenCalled();
});

test("acks a delivered tick when clearing its durable outbox fails", async () => {
  stubFetchMock.mockResolvedValueOnce(
    Response.json({ error: null, raceKey: "jra:20260603:05:11", status: "running" }),
  );
  clearFocusedFullWatchOutboxMock.mockRejectedValueOnce(new Error("outbox clear unavailable"));
  await handleQueue(makeBatch([makeFocusedFullWatchTickMessage()]), makeEnv());
  expect(registerFocusedFullWatchOutboxMock).toHaveBeenCalledOnce();
  expect(watchSendMock).toHaveBeenCalledOnce();
  expect(clearFocusedFullWatchOutboxMock).toHaveBeenCalledOnce();
  expect(ackMock).toHaveBeenCalledOnce();
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
    executionId: "execution-1",
    env: expect.anything(),
    staleAfterMs: 960_000,
    watchId: "watch-1",
  });
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledWith({
    claimId: "completion-msg-1",
    executionId: "execution-1",
    env: expect.anything(),
    watchId: "watch-1",
  });
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(consumeContainerStopMock).toHaveBeenCalledTimes(1);
  expect(markFocusedFullTerminalWatchStoppedMock).toHaveBeenCalledWith({
    claimId: "completion-msg-1",
    executionId: "execution-1",
    env: expect.anything(),
    watchId: "watch-1",
  });
  expect(consumeContainerStopMock.mock.invocationCallOrder[0]).toBeLessThan(
    sendMock.mock.invocationCallOrder[0] ?? Number.POSITIVE_INFINITY,
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});

test("stops a completed shard before Viewer work even when idle reuse is configured", async () => {
  const trace: string[] = [];
  const env: Env = makeEnv();
  stubFetchMock.mockImplementationOnce(async () => {
    trace.push("cache-pickup");
    return Response.json({ found: true });
  });
  isPerRaceFeatureCachePresentMock.mockImplementationOnce(async () => {
    trace.push("durable-cache-confirmed");
    return true;
  });
  completeFocusedFullRaceMock
    .mockImplementationOnce(async () => undefined)
    .mockImplementationOnce(async () => {
      trace.push("computation-completed");
    });
  consumeContainerStopMock.mockImplementationOnce(async (_env, _message, afterDestroyed) => {
    trace.push("owned-stop");
    await afterDestroyed?.();
    return true;
  });
  markFocusedFullTerminalWatchStoppedMock.mockImplementationOnce(async () => {
    trace.push("stop-recorded");
  });
  publishFinishPositionPredictionCacheMock.mockImplementationOnce(async () => {
    trace.push("publication");
    return { busted: true, expectedGeneratedAt: "2026-08-09T01:15:00.000Z", status: "written" };
  });
  warmPredictionCacheForRaceMock.mockImplementationOnce(async () => {
    trace.push("viewer-warm");
    return true;
  });
  completeFocusedFullTerminalWatchMock.mockImplementationOnce(async () => {
    trace.push("terminal");
  });

  await handleQueue(
    makeFocusedFullCompletionBatch([
      makeFocusedFullCompletionMessage({ doName: "race-chain-predict-jra", role: "race-chain" }),
    ]),
    {
      ...env,
      CONTAINER_REUSE_IDLE_SECONDS: "20",
      FINISH_POSITION_RACE_CHAIN_CONTAINER: env.FINISH_POSITION_PREDICT_CONTAINER,
    },
  );

  expect(trace).toStrictEqual([
    "cache-pickup",
    "durable-cache-confirmed",
    "computation-completed",
    "owned-stop",
    "stop-recorded",
    "publication",
    "viewer-warm",
    "terminal",
  ]);
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(consumeContainerStopMock).toHaveBeenCalledOnce();
  expect(ackMock).toHaveBeenCalledOnce();
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

  expect(consumeContainerStopMock).toHaveBeenCalledTimes(1);
  expect(clearContainerSlotMock).toHaveBeenCalledWith(
    expect.objectContaining({ executionId: "execution-1" }),
  );
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
  expect(markFocusedFullTerminalWatchStoppedMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
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

  expect(consumeContainerStopMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).toHaveBeenCalledWith(
    expect.objectContaining({ executionId: "execution-1" }),
  );
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

    expect(consumeContainerStopMock).not.toHaveBeenCalled();
    expect(clearContainerSlotMock).toHaveBeenCalledWith(
      expect.objectContaining({ executionId: "execution-1" }),
    );
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
  expect(controlSendMock).toHaveBeenCalledTimes(1);
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

test("retries KV delivery after a successful score without invoking the Container twice", async () => {
  const logs: string[] = [];
  const consoleSpy = vi.spyOn(console, "log").mockImplementation((...args: unknown[]) => {
    logs.push(args.map(String).join(" "));
  });
  claimRescoreExecutionMock
    .mockResolvedValueOnce({ proceed: true })
    .mockResolvedValueOnce({ proceed: false, state: "neon_complete" });
  publishFinishPositionPredictionCacheMock
    .mockResolvedValueOnce({
      busted: false,
      expectedGeneratedAt: null,
      status: "skipped-empty",
    })
    .mockResolvedValueOnce({
      busted: true,
      expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
      status: "written",
    });
  const body = {
    daysAhead: 0,
    keibajoCode: "04",
    mode: "rescore",
    raceBango: "01",
    runYmd: "20260809",
  } satisfies Partial<PredictQueueMessage>;
  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "neon_complete" }),
  );

  await handleQueue(makeBatch([makeMessage(body)]), makeEnv());

  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledTimes(1);
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
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

test("old-runYmd message with force:true is still fenced from the Container DO", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  await handleQueue(makeBatch([makeMessage({ force: true, runYmd: "20260101" })]), makeEnv());
  expect(isOldDateRunYmdMock).toHaveBeenCalledWith("20260101", expect.any(Date));
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(prepareMock).toHaveBeenCalledTimes(2);
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

test("keeps a canary rescore reconnect and terminal cleanup in its own namespace", async () => {
  const env = makeEnv();
  env.FINISH_POSITION_RESCORE_CONTAINER = makeEnv().FINISH_POSITION_PREDICT_CONTAINER;
  env.RESCORE_CONTAINER_ENABLED = "1";
  env.RESCORE_CONTAINER_RACES = "jra:20260619:05:11";
  const sourceLookup = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName.bind(
    env.FINISH_POSITION_PREDICT_CONTAINER,
  );
  const sourceGet = env.FINISH_POSITION_PREDICT_CONTAINER.get.bind(
    env.FINISH_POSITION_PREDICT_CONTAINER,
  );
  const legacyLookup = vi.fn((name: string): DurableObjectId => sourceLookup(name));
  const smallLookup = vi.fn((name: string): DurableObjectId => sourceLookup(name));
  const smallGet = vi.fn((id: DurableObjectId) => sourceGet(id));
  env.FINISH_POSITION_PREDICT_CONTAINER.idFromName = legacyLookup;
  env.FINISH_POSITION_RESCORE_CONTAINER.idFromName = smallLookup;
  env.FINISH_POSITION_RESCORE_CONTAINER.get = smallGet;
  stubFetchMock.mockRejectedValueOnce(
    new Error(
      "Connection closed: this Durable Object instance is no longer active. Reconnect or retry the request.",
    ),
  );
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          category: "jra",
          daysAhead: 0,
          keibajoCode: "05",
          mode: "rescore",
          raceBango: "11",
          runYmd: "20260619",
        },
        1,
      ),
    ]),
    env,
  );
  expect(smallLookup).toHaveBeenCalledWith("rescore-predict-jra");
  expect(smallGet).toHaveBeenCalledTimes(2);
  expect(legacyLookup).not.toHaveBeenCalled();
  expect(controlSendMock).toHaveBeenCalledWith({
    name: "rescore-predict-jra",
    role: "rescore",
    type: "container-stop",
    requestedAt: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T/),
    workKey: "rescore:20260619:jra:05:11",
  });
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  smallGet.mockRestore();
  smallLookup.mockRestore();
  legacyLookup.mockRestore();
});

test("cleans up a failed small rescore and dispatches its next Queue attempt to legacy", async () => {
  const env = makeEnv();
  env.FINISH_POSITION_RESCORE_CONTAINER = makeEnv().FINISH_POSITION_PREDICT_CONTAINER;
  env.RESCORE_CONTAINER_ENABLED = "1";
  env.RESCORE_CONTAINER_RACES = "jra:20260619:05:11";
  const sourceLookup = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName.bind(
    env.FINISH_POSITION_PREDICT_CONTAINER,
  );
  const legacyLookup = vi.fn((name: string): DurableObjectId => sourceLookup(name));
  const smallLookup = vi.fn((name: string): DurableObjectId => sourceLookup(name));
  env.FINISH_POSITION_PREDICT_CONTAINER.idFromName = legacyLookup;
  env.FINISH_POSITION_RESCORE_CONTAINER.idFromName = smallLookup;
  stubFetchMock.mockRejectedValueOnce(new Error("container 503"));
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          category: "jra",
          daysAhead: 0,
          keibajoCode: "05",
          mode: "rescore",
          raceBango: "11",
          runYmd: "20260619",
        },
        1,
      ),
    ]),
    env,
  );
  expect(legacyLookup).not.toHaveBeenCalled();
  expect(controlSendMock).toHaveBeenNthCalledWith(1, {
    name: "rescore-predict-jra",
    role: "rescore",
    type: "container-stop",
    requestedAt: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T/),
    workKey: "rescore:20260619:jra:05:11",
  });
  await handleQueue(
    makeBatch([
      makeMessage(
        {
          category: "jra",
          daysAhead: 0,
          keibajoCode: "05",
          mode: "rescore",
          raceBango: "11",
          runYmd: "20260619",
        },
        2,
      ),
    ]),
    env,
  );
  expect(legacyLookup).toHaveBeenCalledWith("predict-jra");
  expect(smallLookup).toHaveBeenCalledTimes(1);
  expect(controlSendMock).toHaveBeenNthCalledWith(2, {
    name: "predict-jra",
    role: "legacy",
    type: "container-stop",
    requestedAt: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T/),
    workKey: "rescore:20260619:jra:05:11",
  });
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  smallLookup.mockRestore();
  legacyLookup.mockRestore();
});

test("hands rescore cleanup off while the Ban-ei shadow is still pending", async () => {
  const shadow = Promise.withResolvers<Awaited<ReturnType<typeof shadowBaneiRaceMock>>>();
  shadowBaneiRaceMock.mockReturnValueOnce(shadow.promise);
  controlSendMock.mockImplementationOnce(async () => {
    expect(completeRescoreRaceMock).toHaveBeenCalledWith(
      expect.objectContaining({ status: "neon_complete" }),
    );
    expect(compareBaneiShadowWithPersistedMock).not.toHaveBeenCalled();
    expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
    shadow.resolve({ score: null, status: "cache_miss" });
  });
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
    { ...makeEnv(), BANEI_WORKER_RESCORE_SHADOW_ENABLED: "1" },
  );
  expect(controlSendMock).toHaveBeenCalledTimes(1);
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledTimes(1);
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("publishes durable rescore scores despite early cleanup failure and retries cleanup", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  controlSendMock.mockRejectedValueOnce(new Error("control unavailable"));
  sendMock.mockRejectedValueOnce(new Error("cleanup unavailable"));
  publishFinishPositionPredictionCacheMock.mockImplementationOnce(async () => {
    expect(controlSendMock).toHaveBeenCalledTimes(1);
    expect(sendMock).toHaveBeenCalledTimes(1);
    return { busted: true, expectedGeneratedAt: "2026-08-09T01:15:00.000Z", status: "written" };
  });
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
  expect(controlSendMock).toHaveBeenCalledTimes(2);
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  errorSpy.mockRestore();
});

test("queues the rescore container stop before Viewer warming and acknowledgement", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  publishFinishPositionPredictionCacheMock.mockImplementationOnce(async () => {
    expect(controlSendMock).toHaveBeenCalledTimes(1);
    expect(completeRescoreRaceMock).toHaveBeenCalledWith(
      expect.objectContaining({ status: "neon_complete" }),
    );
    return { busted: true, expectedGeneratedAt: "2026-08-09T01:15:00.000Z", status: "written" };
  });
  warmPredictionCacheForRaceMock.mockImplementationOnce(async () => {
    expect(controlSendMock).toHaveBeenCalledTimes(1);
    expect(completeRescoreRaceMock).toHaveBeenCalledWith(
      expect.objectContaining({ status: "neon_complete" }),
    );
    return true;
  });
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
  expect(getStateMock).toHaveBeenCalledTimes(1);
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

test("recovers immediately when focused-full status is absent and the Container is stopped", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  stubFetchMock.mockResolvedValueOnce(new Response("no instance", { status: 503 }));
  getStateMock.mockResolvedValueOnce({ status: "stopped" });
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

  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ keibajoCode: "02", raceBango: "01", status: "error" }),
  );
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra-1",
      type: "container-stop",
      workKey: "focused-full:20260628:jra:02:01",
    }),
  );
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(touchContainerSlotMock).not.toHaveBeenCalled();
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

test("rejects a focused-full status with an invalid progress heartbeat", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: null,
      lastProgressAtMs: "invalid",
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
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(touchContainerSlotMock).not.toHaveBeenCalled();
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
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] focused-full status error; cleanup handed off before retry category=jra runYmd=20260628 mode=full daysAhead=0 skipDedup=true busyRequeueCount=0 keibajo=02 race=01; error=RuntimeError: Catalog unavailable delaySeconds=70",
  );
  warnSpy.mockRestore();
});

test("retries detached DAY_BASE_REQUIRED fail-closed after scheduling repair", async () => {
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
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(enqueueDayBaseRepairOnceMock).toHaveBeenCalledWith(
    expect.objectContaining({ category: "jra", force: true, runYmd: "20260628" }),
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] detached race-chain cache HIT lost; retrying fail-closed category=jra runYmd=20260628 mode=full daysAhead=0 skipDedup=true busyRequeueCount=0 keibajo=02 race=01 error=DayBaseRequiredError: DAY_BASE_REQUIRED: layer subprocess terminated delaySeconds=70",
  );
  warnSpy.mockRestore();
});

test("retries detached DAY_BASE_REQUIRED without enqueueing a legacy fallback", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "started" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: "DAY_BASE_REQUIRED: transient source failure",
      raceKey: "jra:20260628:02:01",
      status: "error",
    }),
  );
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
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 70 });
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] detached race-chain cache HIT lost; retrying fail-closed category=jra runYmd=20260628 mode=full daysAhead=0 skipDedup=true busyRequeueCount=0 keibajo=02 race=01 error=DAY_BASE_REQUIRED: transient source failure delaySeconds=70",
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
    "http://do/predict?category=jra&daysAhead=0&mode=full&runDate=20260822&allowRaceScopedDayBase=1&keibajoCode=07&raceBango=09",
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
    "http://do/predict?category=jra&daysAhead=0&mode=full&runDate=20260822&allowRaceScopedDayBase=1&keibajoCode=07&raceBango=09&force=1",
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
      lastProgressAtMs: null,
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

test("restarts a resumed focused pipeline when its heartbeat is stale", async () => {
  claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: true, state: "resumed" });
  stubFetchMock.mockResolvedValueOnce(
    Response.json({
      error: null,
      lastProgressAtMs: Date.now() - 21 * 60 * 1000,
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
        8,
      ),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );

  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error", keibajoCode: "07", raceBango: "09" }),
  );
  expect(retryMock).toHaveBeenCalled();
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
    .mockResolvedValueOnce({ proceed: false, state: "kv_complete" });
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

test("resumes score-complete delivery without starting the prediction Container", async () => {
  claimRescoreExecutionMock.mockResolvedValueOnce({ proceed: false, state: "score_complete" });
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
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  for (const status of ["neon_complete", "kv_complete", "viewer_warm_complete", "success"]) {
    expect(completeRescoreRaceMock).toHaveBeenCalledWith(expect.objectContaining({ status }));
  }
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test.each(["neon_complete", "kv_complete"])(
  "resumes %s delivery without scoring again",
  async (state) => {
    claimRescoreExecutionMock.mockResolvedValueOnce({ proceed: false, state });
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
    expect(claimContainerSlotMock).not.toHaveBeenCalled();
    expect(completeRescoreRaceMock).not.toHaveBeenCalledWith(
      expect.objectContaining({ status: "neon_complete" }),
    );
    expect(completeRescoreRaceMock).toHaveBeenCalledWith(
      expect.objectContaining({ status: "success" }),
    );
    expect(ackMock).toHaveBeenCalledTimes(1);
    expect(retryMock).not.toHaveBeenCalled();
  },
);

test("defers viewer repair when resumed delivery cannot warm the cache", async () => {
  claimRescoreExecutionMock.mockResolvedValueOnce({ proceed: false, state: "kv_complete" });
  warmPredictionCacheForRaceMock.mockResolvedValueOnce(false);
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
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "kv_complete" }),
  );
  expect(completeRescoreRaceMock).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});

test("completes a warmed Worker delivery without acquiring a Container", async () => {
  claimRescoreExecutionMock.mockResolvedValueOnce({
    proceed: false,
    state: "viewer_warm_complete",
  });
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
    { ...makeEnv(), JRA_WORKER_RESCORE_ENABLED: "1" },
  );
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(rescoreJraRaceMock).not.toHaveBeenCalled();
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("finishes viewer-warm-complete delivery without republishing KV", async () => {
  claimRescoreExecutionMock.mockResolvedValueOnce({
    proceed: false,
    state: "viewer_warm_complete",
  });
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
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
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
    force: false,
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

test("forces a semantic execution takeover for a DLQ-redriven rescore", async () => {
  claimRescoreExecutionMock.mockResolvedValueOnce({ proceed: false, state: "success" });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        daysAhead: 0,
        dlqRedriveCount: 1,
        keibajoCode: "44",
        mode: "rescore",
        raceBango: "01",
        runYmd: "20260619",
      }),
    ]),
    makeEnv(),
  );
  expect(claimRescoreExecutionMock).toHaveBeenCalledWith(expect.objectContaining({ force: true }));
  expect(ackMock).toHaveBeenCalledTimes(1);
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

test("acks and drops a clock-only rescore after its scheduled post time", async () => {
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
        weightSnapshotCount: undefined,
        weightSnapshotFetchedAt: undefined,
        weightSnapshotHash: undefined,
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

test("runs an attested weight rescore after post time", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-08-24T02:05:00.000Z"));
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await handleQueue(
    makeBatch([
      makeMessage({
        allowPostTimeRescore: true,
        category: "nar",
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
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  logSpy.mockRestore();
  vi.useRealTimers();
});

test("publishes rescore results when early cleanup fails and retries cleanup afterwards", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  controlSendMock.mockRejectedValueOnce(new Error("control queue unavailable"));
  sendMock.mockRejectedValueOnce(new Error("predict queue unavailable"));
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
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledTimes(1);
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledTimes(1);
  expect(controlSendMock).toHaveBeenCalledTimes(2);
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
  logSpy.mockRestore();
});

test("recovers terminal rescore cleanup after both stop queues fail without rescoring", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  controlSendMock.mockRejectedValue(new Error("control queue unavailable"));
  sendMock
    .mockRejectedValueOnce(new Error("predict queue unavailable"))
    .mockRejectedValueOnce(new Error("predict queue unavailable"))
    .mockRejectedValueOnce(new Error("predict queue unavailable"));
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
  expect(completeRescoreRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "success" }),
  );
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
  const completionCalls = completeRescoreRaceMock.mock.calls as unknown as Array<
    [{ status: string }]
  >;
  const scoreCompleteCall = completionCalls.findIndex(
    ([params]) => params.status === "score_complete",
  );
  const successCall = completionCalls.findIndex(([params]) => params.status === "success");
  expect(
    (completeRescoreRaceMock.mock.invocationCallOrder[scoreCompleteCall] ?? 0) <
      (warmPredictionCacheForRaceMock.mock.invocationCallOrder[0] ?? 0),
  ).toBe(true);
  expect(
    (completeRescoreRaceMock.mock.invocationCallOrder[successCall] ?? 0) >
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

test("retries an old prewarm without starting a Container when a different D1 owner is active", async () => {
  acquireDayBaseRepairMock.mockResolvedValueOnce({
    generationId: "current-workflow-owner",
    status: "busy",
  });
  const message = {
    ack: ackMock,
    body: {
      category: "nar",
      daysAhead: 0,
      generationId: "legacy-prewarm-owner",
      requestedAt: "2026-09-10T00:00:00.000Z",
      runYmd: "20260910",
      type: "day-base-prewarm",
    },
    id: "legacy-queue-id",
    retry: retryMock,
  } as unknown as Message<import("./types").DayBasePrewarmMessage>;

  await handleQueue(
    { messages: [message] } as unknown as MessageBatch<import("./types").PredictQueueBody>,
    makeEnv(),
  );

  expect(acquireDayBaseRepairMock).toHaveBeenCalledWith(
    expect.objectContaining({ generationId: "legacy-prewarm-owner", force: false }),
  );
  expect(prewarmCategoryWithOutcomeMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
});

test("uses the stable Queue id as the owner for a legacy prewarm with no generation", async () => {
  const message = {
    ack: ackMock,
    body: {
      category: "nar",
      daysAhead: 0,
      requestedAt: "2026-09-10T00:00:00.000Z",
      runYmd: "20260910",
      type: "day-base-prewarm",
    },
    id: "legacy-queue-id",
    retry: retryMock,
  } as unknown as Message<import("./types").DayBasePrewarmMessage>;

  await handleQueue(
    { messages: [message] } as unknown as MessageBatch<import("./types").PredictQueueBody>,
    makeEnv(),
  );

  expect(prewarmCategoryWithOutcomeMock).toHaveBeenCalledWith(
    expect.objectContaining({ generationId: "legacy-queue-id" }),
  );
  expect(clearDayBaseRepairReservationMock).toHaveBeenCalledWith(
    expect.objectContaining({ generationId: "legacy-queue-id" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("acks a zero-day repair without starting a Container after category coverage is complete", async () => {
  hasCompleteCategoryPreWeightCoverageMock.mockResolvedValueOnce(true);
  const message = {
    ack: ackMock,
    body: {
      category: "nar",
      daysAhead: 0,
      requestedAt: "2026-09-10T09:00:00.000Z",
      runYmd: "20260910",
      type: "day-base-prewarm",
    },
    retry: retryMock,
  } as unknown as Message<import("./types").DayBasePrewarmMessage>;
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await handleQueue(
    { messages: [message] } as unknown as MessageBatch<import("./types").PredictQueueBody>,
    makeEnv(),
  );

  expect(clearDayBaseRepairReservationMock).toHaveBeenCalledWith({
    category: "nar",
    env: expect.any(Object),
    runYmd: "20260910",
  });
  expect(prewarmCategoryWithOutcomeMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  expect(logSpy).toHaveBeenCalledWith(
    "[predict-queue] day-base work skipped after complete coverage type=day-base-prewarm category=nar runYmd=20260910",
  );
  logSpy.mockRestore();
});

test("continues a zero-day repair when one category race is incomplete", async () => {
  hasCompleteCategoryPreWeightCoverageMock.mockResolvedValueOnce(false);
  const message = {
    ack: ackMock,
    body: {
      category: "nar",
      daysAhead: 0,
      requestedAt: "2026-09-10T09:00:00.000Z",
      runYmd: "20260910",
      type: "day-base-prewarm",
    },
    retry: retryMock,
  } as unknown as Message<import("./types").DayBasePrewarmMessage>;

  await handleQueue(
    { messages: [message] } as unknown as MessageBatch<import("./types").PredictQueueBody>,
    makeEnv(),
  );

  expect(prewarmCategoryWithOutcomeMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("continues prewarm fail-open when the optional complete-coverage check fails", async () => {
  hasCompleteCategoryPreWeightCoverageMock.mockRejectedValueOnce(
    new Error("readiness unavailable"),
  );
  const message = {
    ack: ackMock,
    body: {
      category: "nar",
      daysAhead: 0,
      requestedAt: "2026-09-10T09:00:00.000Z",
      runYmd: "20260910",
      type: "day-base-prewarm",
    },
    retry: retryMock,
  } as unknown as Message<import("./types").DayBasePrewarmMessage>;
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await handleQueue(
    { messages: [message] } as unknown as MessageBatch<import("./types").PredictQueueBody>,
    makeEnv(),
  );

  expect(prewarmCategoryWithOutcomeMock).toHaveBeenCalledTimes(1);
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-queue] day-base coverage check failed type=day-base-prewarm category=nar runYmd=20260910:",
    "Error: readiness unavailable",
  );
  warnSpy.mockRestore();
});

test("does not run complete-coverage suppression for a multi-day scheduled prewarm", async () => {
  const message = {
    ack: ackMock,
    body: {
      category: "nar",
      daysAhead: 2,
      requestedAt: "2026-09-10T00:30:00.000Z",
      runYmd: "20260910",
      type: "day-base-prewarm",
    },
    retry: retryMock,
  } as unknown as Message<import("./types").DayBasePrewarmMessage>;

  await handleQueue(
    { messages: [message] } as unknown as MessageBatch<import("./types").PredictQueueBody>,
    makeEnv(),
  );

  expect(hasCompleteCategoryPreWeightCoverageMock).not.toHaveBeenCalled();
  expect(prewarmCategoryWithOutcomeMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
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

test("cleans an explicitly forced historical day-base prewarm", async () => {
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
  expect(prewarmCategoryWithOutcomeMock).not.toHaveBeenCalled();
  expect(clearDayBaseRepairReservationMock).toHaveBeenCalledTimes(1);
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ workKey: "day-base:20260823:nar" }),
  );
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
  ackMock.mockClear();
  retryMock.mockClear();
  prewarmCategoryWithOutcomeMock.mockResolvedValue("busy");
  await handleQueue(
    { messages: [makePrewarm()] } as unknown as MessageBatch<import("./types").PredictQueueBody>,
    makeEnv(),
  );
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 30 });
  expect(ackMock).not.toHaveBeenCalled();
  ackMock.mockClear();
  retryMock.mockClear();
  prewarmCategoryWithOutcomeMock.mockResolvedValue("owned");
  await handleQueue(
    { messages: [makePrewarm(true)] } as unknown as MessageBatch<
      import("./types").PredictQueueBody
    >,
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});

test("acknowledges a superseded day-base generation without redelivery", async () => {
  prewarmCategoryWithOutcomeMock.mockResolvedValue("superseded");
  const message = {
    ack: ackMock,
    body: {
      category: "nar",
      daysAhead: 0,
      generationId: "retired-generation",
      requestedAt: "2026-08-26T00:00:00.000Z",
      runYmd: "20260826",
      type: "day-base-prewarm",
    },
    retry: retryMock,
  } as unknown as Message<import("./types").DayBasePrewarmMessage>;

  await handleQueue(
    { messages: [message] } as unknown as MessageBatch<import("./types").PredictQueueBody>,
    makeEnv(),
  );

  expect(prewarmCategoryWithOutcomeMock).toHaveBeenCalledWith(
    expect.objectContaining({ generationId: "retired-generation", runYmd: "20260826" }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
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

test("retries failed focused-full stop before publication without starting another computation", async () => {
  const trace: string[] = [];
  const message: Message<FocusedFullCompletionMessage> = makeFocusedFullCompletionMessage();
  consumeContainerStopMock.mockImplementationOnce(async () => {
    trace.push("stop-failed");
    throw new Error("stop unavailable");
  });
  await expect(handleQueue(makeFocusedFullCompletionBatch([message]), makeEnv())).rejects.toThrow(
    "stop unavailable",
  );
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
  expect(markFocusedFullTerminalWatchStoppedMock).not.toHaveBeenCalled();
  expect(completeFocusedFullTerminalWatchMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();

  consumeContainerStopMock.mockImplementationOnce(async (_env, _message, afterDestroyed) => {
    trace.push("stop-retry");
    await afterDestroyed?.();
    return true;
  });
  publishFinishPositionPredictionCacheMock.mockImplementationOnce(async () => {
    trace.push("publication");
    return { busted: true, expectedGeneratedAt: "2026-08-09T01:15:00.000Z", status: "written" };
  });
  await handleQueue(makeFocusedFullCompletionBatch([message]), makeEnv());

  expect(trace).toStrictEqual(["stop-failed", "stop-retry", "publication"]);
  expect(consumeContainerStopMock).toHaveBeenNthCalledWith(
    2,
    expect.anything(),
    {
      executionId: "execution-1",
      name: "predict-jra",
      requestedAt: "2026-07-12T01:00:01.000Z",
      role: "legacy",
      type: "container-stop",
      workKey: "focused-full:20260603:jra:05:11",
    },
    expect.any(Function),
  );
  expect(parseNdjsonStreamMock).not.toHaveBeenCalled();
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(reserveFocusedFullRaceRepairMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledOnce();
});

test("recovers publication failure after stop from durable cache and ignores terminal duplicates", async () => {
  const trace: string[] = [];
  const message: Message<FocusedFullCompletionMessage> = makeFocusedFullCompletionMessage();
  consumeContainerStopMock.mockImplementationOnce(async (_env, _message, afterDestroyed) => {
    trace.push("owned-stop");
    await afterDestroyed?.();
    return true;
  });
  publishFinishPositionPredictionCacheMock.mockImplementationOnce(async () => {
    trace.push("publication-failed");
    throw new Error("publication unavailable");
  });
  sendMock.mockRejectedValueOnce(new Error("repair queue unavailable"));

  await expect(handleQueue(makeFocusedFullCompletionBatch([message]), makeEnv())).rejects.toThrow(
    "Focused-full terminal finalizer incomplete",
  );
  expect(markFocusedFullTerminalWatchStoppedMock).toHaveBeenCalledOnce();
  expect(completeFocusedFullTerminalWatchMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  const pickupCount: number = stubFetchMock.mock.calls.length;

  claimFocusedFullTerminalWatchMock.mockResolvedValueOnce({ proceed: true, state: "stopped" });
  clearContainerSlotMock.mockImplementationOnce(async () => {
    trace.push("cleanup-only");
  });
  isPerRaceFeatureCachePresentMock.mockImplementationOnce(async () => {
    trace.push("durable-cache-confirmed");
    return true;
  });
  publishFinishPositionPredictionCacheMock.mockImplementationOnce(async () => {
    trace.push("publication-retry");
    return { busted: true, expectedGeneratedAt: "2026-08-09T01:15:00.000Z", status: "written" };
  });
  await handleQueue(makeFocusedFullCompletionBatch([message]), makeEnv());
  claimFocusedFullTerminalWatchMock.mockResolvedValueOnce({ proceed: false, state: "terminal" });
  await handleQueue(makeFocusedFullCompletionBatch([message]), makeEnv());

  expect(trace).toStrictEqual([
    "owned-stop",
    "publication-failed",
    "cleanup-only",
    "durable-cache-confirmed",
    "publication-retry",
  ]);
  expect(stubFetchMock).toHaveBeenCalledTimes(pickupCount);
  expect(consumeContainerStopMock).toHaveBeenCalledTimes(1);
  expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledTimes(2);
  expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledOnce();
  expect(parseNdjsonStreamMock).not.toHaveBeenCalled();
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(reserveFocusedFullRaceRepairMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(2);
});

test("does not publish a stopped terminal success whose durable cache disappeared", async () => {
  claimFocusedFullTerminalWatchMock.mockResolvedValueOnce({ proceed: true, state: "stopped" });
  isPerRaceFeatureCachePresentMock.mockResolvedValue(false);

  await handleQueue(
    makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage()]),
    makeEnv(),
  );

  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  expect(consumeContainerStopMock).not.toHaveBeenCalled();
  expect(clearContainerSlotMock).toHaveBeenCalledWith(
    expect.objectContaining({ executionId: "execution-1" }),
  );
  expect(reserveFocusedFullRaceRepairMock).toHaveBeenCalledOnce();
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, keibajoCode: "05", raceBango: "11" }),
    { delaySeconds: 30 },
  );
  expect(ackMock).toHaveBeenCalledOnce();
});

test.each(["direct", "already-complete", "completion-guard", "status-poll"])(
  "%s focused-full hands cleanup off after durable cache and before publication",
  async (path) => {
    const trace: string[] = [];
    if (path === "completion-guard") isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
    if (path === "already-complete") {
      parseNdjsonStreamMock.mockResolvedValueOnce({
        category: "jra",
        racesPredicted: 1,
        status: "already-complete",
        type: "result",
      });
    }
    if (path === "status-poll") {
      claimFocusedFullRaceMock.mockResolvedValueOnce({ proceed: false, state: "started" });
      stubFetchMock.mockResolvedValueOnce(
        Response.json({ error: null, raceKey: "jra:20260603:05:11", status: "success" }),
      );
    }
    isPerRaceFeatureCachePresentMock.mockImplementationOnce(async () => {
      trace.push("durable-cache-confirmed");
      return true;
    });
    completeFocusedFullRaceMock.mockImplementationOnce(async () => {
      trace.push("computation-completed");
    });
    controlSendMock.mockImplementationOnce(async () => {
      trace.push("cleanup-handoff");
    });
    publishFinishPositionPredictionCacheMock.mockImplementationOnce(async () => {
      trace.push("publication");
      return { busted: true, expectedGeneratedAt: "2026-08-09T01:15:00.000Z", status: "written" };
    });
    warmPredictionCacheForRaceMock.mockImplementationOnce(async () => {
      trace.push("viewer-warm");
      return true;
    });

    await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), makeEnv());

    expect(trace).toStrictEqual([
      "durable-cache-confirmed",
      "computation-completed",
      "cleanup-handoff",
      "publication",
      "viewer-warm",
    ]);
    expect(controlSendMock).toHaveBeenCalledWith({
      name: "predict-jra",
      requestedAt: expect.any(String),
      role: "legacy",
      type: "container-stop",
      workKey: "focused-full:20260603:jra:05:11",
    });
    expect(controlSendMock).toHaveBeenCalledOnce();
    expect(ackMock).toHaveBeenCalledOnce();
    expect(retryMock).not.toHaveBeenCalled();
  },
);

test.each(["stale", "stopped"])(
  "a %s terminal delivery respects a rejected owner/lease fence without stopping or clearing new work",
  async (state) => {
    const actual =
      await vi.importActual<typeof import("./container-control")>("./container-control");
    const trace: string[] = [];
    claimFocusedFullTerminalWatchMock.mockResolvedValueOnce({ proceed: true, state });
    if (state !== "stopped")
      consumeContainerStopMock.mockImplementationOnce(async (env, message, afterDestroyed) => {
        trace.push("check-owner-fence");
        return actual.consumeContainerStop(env, message, afterDestroyed);
      });
    if (state === "stopped")
      publishFinishPositionPredictionCacheMock.mockImplementationOnce(async () => {
        trace.push("publication");
        return { busted: true, expectedGeneratedAt: "2026-08-09T01:15:00.000Z", status: "written" };
      });

    await handleQueue(
      makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage()]),
      makeEnv(),
    );

    if (state === "stopped") expect(trace).toStrictEqual(["publication"]);
    else expect(trace).toStrictEqual(["check-owner-fence"]);
    if (state !== "stopped")
      expect(claimContainerSlotStopMock).toHaveBeenCalledWith({
        acceptableWorkKeys: undefined,
        allowUnowned: undefined,
        executionId: "execution-1",
        doName: "predict-jra",
        env: expect.anything(),
        force: undefined,
        requestedAt: "2026-07-12T01:00:01.000Z",
        workKey: "focused-full:20260603:jra:05:11",
      });
    if (state === "stopped") expect(stubFetchMock).not.toHaveBeenCalled();
    else {
      expect(stubFetchMock).toHaveBeenCalledOnce();
      expect(stubFetchMock.mock.calls[0]?.[0]?.url).toBe(
        "http://do/focused-full-cache?category=jra&runDate=20260603&keibajoCode=05&raceBango=11",
      );
    }
    expect(markFocusedFullTerminalWatchStoppedMock).not.toHaveBeenCalled();
    if (state === "stopped") {
      expect(consumeContainerStopMock).not.toHaveBeenCalled();
      expect(clearContainerSlotMock).toHaveBeenCalledWith(
        expect.objectContaining({ executionId: "execution-1" }),
      );
    } else expect(clearContainerSlotMock).not.toHaveBeenCalled();
    expect(claimContainerSlotMock).not.toHaveBeenCalled();
    expect(controlSendMock).not.toHaveBeenCalled();
    expect(parseNdjsonStreamMock).not.toHaveBeenCalled();
    expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledOnce();
    expect(ackMock).toHaveBeenCalledOnce();
  },
);

test.each(["success", "missing", "timeout", "error"])(
  "legacy %s completion retires its watch without stop, repair, or publication",
  async (outcome) => {
    const terminalOutcome =
      outcome === "success" || outcome === "missing" || outcome === "timeout" ? outcome : "error";
    await handleQueue(
      makeFocusedFullCompletionBatch([
        makeFocusedFullCompletionMessage({ executionId: undefined, outcome: terminalOutcome }),
      ]),
      makeEnv(),
    );
    expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledOnce();
    expect(consumeContainerStopMock).not.toHaveBeenCalled();
    expect(markFocusedFullTerminalWatchStoppedMock).not.toHaveBeenCalled();
    expect(reserveFocusedFullRaceRepairMock).not.toHaveBeenCalled();
    expect(sendMock).not.toHaveBeenCalled();
    expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
    expect(ackMock).toHaveBeenCalledOnce();
  },
);

test("legacy expired tick never polls or adopts the current physical execution", async () => {
  const tick = makeFocusedFullWatchTickMessage();
  tick.body.executionId = undefined;
  tick.body.deadlineAtMs = 0;
  await handleQueue(makeBatch([tick]), makeEnv());
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(touchContainerSlotMock).not.toHaveBeenCalled();
  expect(consumeContainerStopMock).not.toHaveBeenCalled();
  expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledOnce();
});

test("inline success uses admitted physical stop before Viewer publication", async () => {
  const trace: string[] = [];
  isPerRaceFeatureCachePresentMock.mockImplementation(async () => {
    trace.push("durable-cache");
    return true;
  });
  consumeContainerStopMock.mockImplementationOnce(async () => {
    trace.push("owned-stop");
    return true;
  });
  publishFinishPositionPredictionCacheMock.mockImplementationOnce(async () => {
    trace.push("publication");
    return { busted: true, expectedGeneratedAt: "2026-08-09T01:15:00.000Z", status: "written" };
  });
  await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), {
    ...makeEnv(),
    FOCUSED_FULL_WATCH_ENABLED: "1",
  });
  expect(trace).toStrictEqual(["durable-cache", "owned-stop", "publication"]);
  expect(consumeContainerStopMock).toHaveBeenCalledWith(
    expect.anything(),
    expect.objectContaining({ executionId: "execution-1" }),
  );
  expect(controlSendMock).not.toHaveBeenCalled();
});

interface PersistedCoordinatorHarness {
  coordinator: PredictRunCoordinator;
  env: Env;
  storage: Map<string, unknown>;
}

const makePersistedCoordinator = (
  storage = new Map<string, unknown>(),
): PersistedCoordinatorHarness => {
  const env = makeEnv();
  const instance: unknown = Reflect.construct(PredictRunCoordinator, [
    {
      blockConcurrencyWhile: async <T>(operation: () => Promise<T>): Promise<T> => operation(),
      storage: {
        get: async (key: string): Promise<unknown> => structuredClone(storage.get(key)),
        put: async (key: string, value: unknown): Promise<void> => {
          storage.set(key, structuredClone(value));
        },
        delete: async (key: string): Promise<boolean> => storage.delete(key),
        list: async ({ prefix }: { prefix: string }): Promise<Map<string, unknown>> =>
          new Map([...storage].filter(([key]) => key.startsWith(prefix))),
        setAlarm: async (): Promise<void> => undefined,
      },
    },
    env,
  ]);
  if (!(instance instanceof PredictRunCoordinator)) throw new Error("Invalid coordinator");
  Object.defineProperty(env, "PREDICT_RUN_COORDINATOR", {
    value: {
      idFromName: (name: string): string => name,
      get: (): { fetch: (request: Request) => Promise<Response> } => ({
        fetch: instance.fetch.bind(instance),
      }),
    },
  });
  return { coordinator: instance, env, storage };
};

test.each(["stopped", "active"])(
  "stale A %s success cannot mutate actual B race, lane, lease or publish",
  async (state) => {
    const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
    const harness = makePersistedCoordinator();
    await harness.coordinator.claimFocusedFullRace({
      category: "jra",
      runYmd: "20260603",
      keibajoCode: "05",
      raceBango: "11",
      doName: "predict-jra",
      staleAfterMs: 60_000,
    });
    const a = await harness.coordinator.claimContainerSlot({
      category: "jra",
      doName: "predict-jra",
      kind: "focused-full",
      workKey: "focused-full:20260603:jra:05:11",
      generationId: "generation-a",
    });
    await harness.coordinator.clearContainerSlot({
      doName: "predict-jra",
      workKey: "focused-full:20260603:jra:05:11",
      executionId: a.executionId,
    });
    const b = await harness.coordinator.claimContainerSlot({
      category: "jra",
      doName: "predict-jra",
      kind: "focused-full",
      workKey: "focused-full:20260603:jra:05:11",
      generationId: "generation-b",
    });
    const before = JSON.stringify([...harness.storage]);
    claimFocusedFullTerminalWatchMock.mockResolvedValueOnce({ proceed: true, state });
    completeFocusedFullRaceMock.mockImplementationOnce(actual.completeFocusedFullRace);
    await handleQueue(
      makeFocusedFullCompletionBatch([
        makeFocusedFullCompletionMessage({ executionId: a.executionId }),
      ]),
      harness.env,
    );
    expect(JSON.stringify([...harness.storage]) === before).toBe(true);
    expect(harness.storage.get("focused-full:20260603:jra:05:11")).toMatchObject({
      status: "started",
      executionId: b.executionId,
    });
    expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
    expect(warmPredictionCacheForRaceMock).not.toHaveBeenCalled();
    expect(prepareMock).not.toHaveBeenCalled();
    expect(consumeContainerStopMock).not.toHaveBeenCalled();
    expect(reserveFocusedFullRaceRepairMock).not.toHaveBeenCalled();
    expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledOnce();
    expect(ackMock).toHaveBeenCalledOnce();
  },
);

test("B admitted during A cache read is fenced at the actual race mutation", async () => {
  const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
  const harness = makePersistedCoordinator();
  await harness.coordinator.claimFocusedFullRace({
    category: "jra",
    runYmd: "20260603",
    keibajoCode: "05",
    raceBango: "11",
    doName: "predict-jra",
    staleAfterMs: 60_000,
  });
  const a = await harness.coordinator.claimContainerSlot({
    category: "jra",
    doName: "predict-jra",
    kind: "focused-full",
    workKey: "focused-full:20260603:jra:05:11",
    generationId: "generation-a",
  });
  const before: { state?: string } = {};
  isPerRaceFeatureCachePresentMock.mockImplementationOnce(async () => {
    await harness.coordinator.clearContainerSlot({
      doName: "predict-jra",
      workKey: "focused-full:20260603:jra:05:11",
      executionId: a.executionId,
    });
    await harness.coordinator.claimContainerSlot({
      category: "jra",
      doName: "predict-jra",
      kind: "focused-full",
      workKey: "focused-full:20260603:jra:05:11",
      generationId: "generation-b",
    });
    before.state = JSON.stringify([...harness.storage]);
    return true;
  });
  completeFocusedFullRaceMock
    .mockImplementationOnce(actual.completeFocusedFullRace)
    .mockImplementationOnce(actual.completeFocusedFullRace);
  await handleQueue(
    makeFocusedFullCompletionBatch([
      makeFocusedFullCompletionMessage({ executionId: a.executionId }),
    ]),
    harness.env,
  );
  expect(JSON.stringify([...harness.storage]) === before.state).toBe(true);
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalled();
  expect(consumeContainerStopMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledOnce();
});

test.each(["already-complete", "delivery-error"])(
  "admitted E survives %s cleanup through real coordinator decisions",
  async (result) => {
    const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
    const control =
      await vi.importActual<typeof import("./container-control")>("./container-control");
    const harness = makePersistedCoordinator();
    harness.env.FOCUSED_FULL_WATCH_ENABLED = "1";
    await harness.coordinator.claimFocusedFullRace({
      category: "jra",
      runYmd: "20260603",
      keibajoCode: "05",
      raceBango: "11",
      doName: "predict-jra",
      staleAfterMs: 60_000,
    });
    claimContainerSlotMock.mockImplementationOnce(actual.claimContainerSlot);
    completeFocusedFullRaceMock.mockImplementationOnce(actual.completeFocusedFullRace);
    claimContainerSlotStopMock.mockImplementationOnce(actual.claimContainerSlotStop);
    consumeContainerStopMock.mockImplementationOnce(control.consumeContainerStop);
    const physical: { stopped: boolean; executionId?: string } = { stopped: false };
    clearContainerSlotMock.mockImplementationOnce(actual.clearContainerSlot);
    stubFetchMock.mockImplementation(async (request?: Request) => {
      if (request === undefined) throw new Error("Missing request");
      if (new URL(request.url).pathname === "/predict") {
        if (result === "delivery-error") {
          physical.stopped = true;
          throw new Error("delivery response lost");
        }
        return new Response("already-complete");
      }
      if (new URL(request.url).pathname === "/focused-full-cache")
        return Response.json({ found: true });
      expect(request.headers.get("x-prediction-execution-id")).toBeTypeOf("string");
      if (request.headers.get("x-prediction-require-stopped") === "1" && !physical.stopped)
        return Response.json({ ok: false, state: "unresolved" }, { status: 425 });
      physical.executionId = request.headers.get("x-prediction-execution-id") ?? undefined;
      physical.stopped = true;
      return Response.json({ ok: true });
    });
    getStateMock.mockImplementation(async () => ({
      status: physical.stopped ? "stopped" : "healthy",
    }));
    parseNdjsonStreamMock.mockResolvedValueOnce({
      type: "result",
      category: "jra",
      status: "already-complete",
      racesPredicted: 1,
    });
    if (result === "delivery-error")
      parseNdjsonStreamMock.mockReset().mockResolvedValue({
        type: "result",
        category: "jra",
        status: "success",
        racesPredicted: 1,
      });
    isPerRaceFeatureCachePresentMock.mockResolvedValue(true);
    publishFinishPositionPredictionCacheMock.mockImplementationOnce(async () => {
      expect(physical.stopped).toBe(true);
      return { busted: true, expectedGeneratedAt: "2026-08-09T01:15:00.000Z", status: "written" };
    });
    await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), harness.env);
    expect(physical.stopped).toBe(true);
    expect(physical.executionId).toBeTypeOf("string");
    expect(harness.storage.get("container-stop-fences")).toBeUndefined();
    expect(harness.storage.get("container-slots")).toStrictEqual({ leases: [] });
    expect(controlSendMock).not.toHaveBeenCalled();
    if (result === "already-complete") expect(ackMock).toHaveBeenCalledOnce();
    else {
      expect(ackMock).not.toHaveBeenCalled();
      expect(retryMock).toHaveBeenCalledOnce();
    }
  },
);

test("persisted stopped A watch reentry retires A while real B race/lane remain untouched", async () => {
  const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
  const harness = makePersistedCoordinator();
  await harness.coordinator.claimFocusedFullRace({
    category: "jra",
    runYmd: "20260603",
    keibajoCode: "05",
    raceBango: "11",
    doName: "predict-jra",
    staleAfterMs: 60_000,
  });
  const a = await harness.coordinator.claimContainerSlot({
    category: "jra",
    doName: "predict-jra",
    kind: "focused-full",
    workKey: "focused-full:20260603:jra:05:11",
    generationId: "generation-a",
  });
  const message = makeFocusedFullCompletionMessage({ executionId: a.executionId });
  await harness.coordinator.claimFocusedFullTerminalWatch({
    executionId: a.executionId,
    watchId: message.body.watchId,
    claimId: message.id,
    staleAfterMs: 60_000,
  });
  await harness.coordinator.markFocusedFullTerminalWatchStopped({
    executionId: a.executionId,
    watchId: message.body.watchId,
    claimId: message.id,
  });
  // Physical stop and its watch marker persisted, but terminal completion did not.
  await harness.coordinator.clearContainerSlot({
    doName: "predict-jra",
    workKey: message.body.workKey,
    executionId: a.executionId,
  });
  await harness.coordinator.claimContainerSlot({
    category: "jra",
    doName: "predict-jra",
    kind: "focused-full",
    workKey: message.body.workKey,
    generationId: "generation-b",
  });
  const race = JSON.stringify(harness.storage.get(message.body.workKey));
  const lane = JSON.stringify(harness.storage.get("focused-full-lane:predict-jra"));
  const slots = JSON.stringify(harness.storage.get("container-slots"));
  claimFocusedFullTerminalWatchMock.mockImplementationOnce(actual.claimFocusedFullTerminalWatch);
  completeFocusedFullRaceMock.mockImplementationOnce(actual.completeFocusedFullRace);
  completeFocusedFullTerminalWatchMock.mockImplementationOnce(
    actual.completeFocusedFullTerminalWatch,
  );
  await handleQueue(makeFocusedFullCompletionBatch([message]), harness.env);
  expect(JSON.stringify(harness.storage.get(message.body.workKey)) === race).toBe(true);
  expect(JSON.stringify(harness.storage.get("focused-full-lane:predict-jra")) === lane).toBe(true);
  expect(JSON.stringify(harness.storage.get("container-slots")) === slots).toBe(true);
  expect(harness.storage.get(`focused-full-terminal-watch:${message.body.watchId}`)).toMatchObject({
    status: "terminal",
  });
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  expect(prepareMock).not.toHaveBeenCalled();
  expect(consumeContainerStopMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledOnce();
});

test("inline already-complete cannot ack or publish on an unconfirmed exact-E stop", async () => {
  const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
  const control =
    await vi.importActual<typeof import("./container-control")>("./container-control");
  const harness = makePersistedCoordinator();
  harness.env.FOCUSED_FULL_WATCH_ENABLED = "1";
  await harness.coordinator.claimFocusedFullRace({
    category: "jra",
    runYmd: "20260603",
    keibajoCode: "05",
    raceBango: "11",
    doName: "predict-jra",
    staleAfterMs: 60_000,
  });
  claimContainerSlotMock.mockImplementationOnce(actual.claimContainerSlot);
  completeFocusedFullRaceMock.mockImplementationOnce(actual.completeFocusedFullRace);
  claimContainerSlotStopMock.mockImplementationOnce(actual.claimContainerSlotStop);
  consumeContainerStopMock.mockImplementationOnce(control.consumeContainerStop);
  stubFetchMock.mockImplementation(async (request?: Request) => {
    if (request === undefined) throw new Error("Missing request");
    if (new URL(request.url).pathname === "/focused-full-cache")
      return Response.json({ found: true });
    return new URL(request.url).pathname === "/__admin/stop-container"
      ? Response.json({ ok: false, state: "unresolved" }, { status: 425 })
      : new Response("already-complete");
  });
  parseNdjsonStreamMock.mockResolvedValueOnce({
    type: "result",
    category: "jra",
    status: "already-complete",
    racesPredicted: 1,
  });
  isPerRaceFeatureCachePresentMock.mockResolvedValue(true);
  await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), harness.env);
  expect(harness.storage.get("container-slots")).toMatchObject({
    leases: [{ executionId: expect.any(String), kind: "focused-full" }],
  });
  expect(harness.storage.get("container-stop-fences")).toStrictEqual({});
  expect(ackMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  expect(controlSendMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledOnce();
});

test("callback-created watch for uncertain delivery retains E and cannot release a live lease on timeout", async () => {
  const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
  const control =
    await vi.importActual<typeof import("./container-control")>("./container-control");
  const harness = makePersistedCoordinator();
  await harness.coordinator.claimFocusedFullRace({
    category: "jra",
    runYmd: "20260603",
    keibajoCode: "05",
    raceBango: "11",
    doName: "predict-jra",
    staleAfterMs: 60_000,
  });
  const admission = await harness.coordinator.claimContainerSlot({
    category: "jra",
    doName: "predict-jra",
    kind: "focused-full",
    workKey: "focused-full:20260603:jra:05:11",
    generationId: "generation-a",
  });
  const body = { ...makeMessage({ skipDedup: true }).body, keibajoCode: "05", raceBango: "11" };
  const callback = await buildFocusedFullCompletionCallbackUrl(
    {
      TRIGGER_TOKEN: harness.env.TRIGGER_TOKEN,
      FOCUSED_FULL_CALLBACK_URL:
        "https://worker.test/api/internal/focused-full-completion-callback",
    },
    {
      body,
      doName: "predict-jra",
      role: "legacy",
      executionId: admission.executionId,
      watchId: "callback-watch",
      workKey: "focused-full:20260603:jra:05:11",
    },
  );
  if (callback === undefined) throw new Error("Missing callback");
  expect(
    (
      await handleFocusedFullCompletionCallback(new Request(callback), harness.env, {
        now: () => 1_000,
        send: async (_env, message) =>
          harness.coordinator.registerFocusedFullWatchOutbox({
            message,
            outboxId: "callback-outbox",
          }),
      })
    ).status,
  ).toBe(202);
  expect(harness.storage.get("focused-full-active-watches")).toMatchObject({
    "callback-watch": { message: { executionId: admission.executionId, deadlineAtMs: 1_861_000 } },
  });
  const before = JSON.stringify(harness.storage.get("container-slots"));
  completeFocusedFullRaceMock.mockImplementationOnce(actual.completeFocusedFullRace);
  claimContainerSlotStopMock.mockImplementationOnce(actual.claimContainerSlotStop);
  consumeContainerStopMock.mockImplementationOnce(control.consumeContainerStop);
  stubFetchMock.mockResolvedValueOnce(
    Response.json({ ok: false, state: "unresolved" }, { status: 425 }),
  );
  await expect(
    handleQueue(
      makeFocusedFullCompletionBatch([
        makeFocusedFullCompletionMessage({
          executionId: admission.executionId,
          watchId: "callback-watch",
          outcome: "timeout",
        }),
      ]),
      harness.env,
    ),
  ).rejects.toThrow("Container stop failed");
  expect(JSON.stringify(harness.storage.get("container-slots")) === before).toBe(true);
  expect(stubFetchMock.mock.calls[0]?.[0]?.headers.get("x-prediction-require-stopped")).toBe("1");
  expect(markFocusedFullTerminalWatchStoppedMock).not.toHaveBeenCalled();
  expect(completeFocusedFullTerminalWatchMock).not.toHaveBeenCalled();
  expect(reserveFocusedFullRaceRepairMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
});

test("lost response cleanup retains E but cannot force-stop a possibly running accepted job", async () => {
  const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
  const control =
    await vi.importActual<typeof import("./container-control")>("./container-control");
  const harness = makePersistedCoordinator();
  harness.env.FOCUSED_FULL_WATCH_ENABLED = "1";
  await harness.coordinator.claimFocusedFullRace({
    category: "jra",
    runYmd: "20260603",
    keibajoCode: "05",
    raceBango: "11",
    doName: "predict-jra",
    staleAfterMs: 60_000,
  });
  claimContainerSlotMock.mockImplementationOnce(actual.claimContainerSlot);
  completeFocusedFullRaceMock.mockImplementationOnce(actual.completeFocusedFullRace);
  claimContainerSlotStopMock.mockImplementationOnce(actual.claimContainerSlotStop);
  consumeContainerStopMock.mockImplementationOnce(control.consumeContainerStop);
  stubFetchMock
    .mockRejectedValueOnce(new Error("response lost after delivery"))
    .mockResolvedValueOnce(Response.json({ ok: false, state: "unresolved" }, { status: 425 }));
  await expect(
    handleQueue(makeBatch([makeMessage({ skipDedup: true })]), harness.env),
  ).rejects.toThrow("Container stop failed");
  expect(stubFetchMock.mock.calls[1]?.[0]?.headers.get("x-prediction-require-stopped")).toBe("1");
  expect(stubFetchMock.mock.calls[1]?.[0]?.headers.get("x-prediction-execution-id")).toBeTypeOf(
    "string",
  );
  expect(harness.storage.get("container-slots")).toMatchObject({
    leases: [{ executionId: expect.any(String) }],
  });
  expect(harness.storage.get("container-stop-fences")).toStrictEqual({});
  expect(markFocusedFullTerminalWatchStoppedMock).not.toHaveBeenCalled();
  expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
});

test("stopped backup retains E after actual repair send cancellation and retries without destroy", async () => {
  const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
  const harness = makePersistedCoordinator();
  const race = {
    category: "jra",
    runYmd: "20260603",
    keibajoCode: "05",
    raceBango: "11",
    doName: "predict-jra",
    staleAfterMs: 60_000,
  };
  await harness.coordinator.claimFocusedFullRace(race);
  const admission = await harness.coordinator.claimContainerSlot({
    category: "jra",
    doName: "predict-jra",
    kind: "focused-full",
    workKey: "focused-full:20260603:jra:05:11",
    generationId: "A",
  });
  await harness.coordinator.clearContainerSlot({
    doName: "predict-jra",
    workKey: "focused-full:20260603:jra:05:11",
    executionId: admission.executionId,
  });
  claimFocusedFullTerminalWatchMock.mockResolvedValue({ proceed: true, state: "stopped" });
  completeFocusedFullRaceMock.mockImplementation(actual.completeFocusedFullRace);
  reserveFocusedFullRaceRepairMock.mockImplementation(actual.reserveFocusedFullRaceRepair);
  cancelFocusedFullRaceRepairMock.mockImplementation(actual.cancelFocusedFullRaceRepair);
  sendMock.mockRejectedValueOnce(new Error("repair send failed"));
  const message = makeFocusedFullCompletionMessage({
    outcome: "missing",
    executionId: admission.executionId,
  });
  try {
    await expect(
      handleQueue(makeFocusedFullCompletionBatch([message]), harness.env),
    ).rejects.toThrow("Focused-full terminal finalizer incomplete");
    expect(completeFocusedFullTerminalWatchMock).not.toHaveBeenCalled();
    expect(ackMock).not.toHaveBeenCalled();
    const rebuilt = makePersistedCoordinator(harness.storage);
    expect(
      await rebuilt.coordinator.claimFocusedFullRace({ ...race, raceBango: "12", force: true }),
    ).toMatchObject({ proceed: true });
    const b = await rebuilt.coordinator.claimContainerSlot({
      category: "jra",
      doName: "predict-jra",
      kind: "focused-full",
      workKey: "focused-full:20260603:jra:05:12",
      generationId: "B",
    });
    await expect(
      handleQueue(makeFocusedFullCompletionBatch([message]), rebuilt.env),
    ).rejects.toThrow("Focused-full terminal finalizer incomplete");
    expect(ackMock).not.toHaveBeenCalled();
    expect(consumeContainerStopMock).not.toHaveBeenCalled();
    expect(rebuilt.storage.get("focused-full:20260603:jra:05:11")).toMatchObject({
      executionId: admission.executionId,
      focusedFullRepairCount: 1,
    });
    await rebuilt.coordinator.completeFocusedFullRace({
      ...race,
      raceBango: "12",
      executionId: b.executionId,
      status: "success",
    });
    await rebuilt.coordinator.clearContainerSlot({
      doName: "predict-jra",
      workKey: "focused-full:20260603:jra:05:12",
      executionId: b.executionId,
    });
    await handleQueue(makeFocusedFullCompletionBatch([message]), rebuilt.env);
    expect(sendMock).toHaveBeenCalledTimes(2);
    expect(consumeContainerStopMock).not.toHaveBeenCalled();
    expect(completeFocusedFullTerminalWatchMock).toHaveBeenCalledOnce();
    expect(ackMock).toHaveBeenCalledOnce();
  } finally {
    completeFocusedFullRaceMock.mockResolvedValue(undefined);
    reserveFocusedFullRaceRepairMock.mockResolvedValue({ proceed: true });
    cancelFocusedFullRaceRepairMock.mockResolvedValue(undefined);
  }
});

test("deferred focused publication retains immutable admitted E in its queue body", async () => {
  claimFocusedFullTerminalWatchMock.mockResolvedValue({ proceed: true, state: "stopped" });
  publishFinishPositionPredictionCacheMock
    .mockReset()
    .mockRejectedValueOnce(new Error("publication unavailable"));
  await handleQueue(
    makeFocusedFullCompletionBatch([makeFocusedFullCompletionMessage({ executionId: "A" })]),
    makeEnv(),
  );
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ type: "prediction-cache-repair", executionId: "A" }),
    expect.anything(),
  );
});

test.each([false, true])(
  "complete E redelivery with durable cache is read-only even if payload exists=%s",
  async (found) => {
    const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
    const h = makePersistedCoordinator();
    const body = makeMessage({ skipDedup: true }).body;
    const race = {
      ...body,
      keibajoCode: "05",
      raceBango: "11",
      doName: "predict-jra",
      staleAfterMs: 60_000,
    };
    await h.coordinator.claimFocusedFullRace(race);
    const a = await h.coordinator.claimContainerSlot({
      ...race,
      kind: "focused-full",
      workKey: "focused-full:20260603:jra:05:11",
      generationId: "A",
    });
    await h.coordinator.completeFocusedFullRace({
      ...race,
      executionId: a.executionId,
      status: "success",
    });
    await h.coordinator.clearContainerSlot({
      doName: "predict-jra",
      workKey: "focused-full:20260603:jra:05:11",
      executionId: a.executionId,
    });
    const before = JSON.stringify([...h.storage]);
    observeFocusedFullAuthorityMock.mockImplementation(actual.observeFocusedFullAuthority);
    isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
    isPerRaceFeatureCachePresentMock.mockResolvedValue(true);
    stubFetchMock.mockImplementation(async () => Response.json({ found }));
    await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), h.env);
    await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), h.env);
    expect(ackMock).toHaveBeenCalledTimes(2);
    expect(retryMock).not.toHaveBeenCalled();
    expect(stubFetchMock).not.toHaveBeenCalled();
    expect(consumeContainerStopMock).not.toHaveBeenCalled();
    expect(claimContainerSlotMock).not.toHaveBeenCalled();
    expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
    expect(JSON.stringify([...h.storage]) === before).toBe(true);
  },
);

test.each(["replacement", "repair", "commit"])(
  "read-only completion rechecks authority after durable reads: %s",
  async (change) => {
    const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
    const h = makePersistedCoordinator();
    const race = {
      category: "jra",
      runYmd: "20260603",
      keibajoCode: "05",
      raceBango: "11",
      doName: "predict-jra",
      staleAfterMs: 60_000,
    };
    await h.coordinator.claimFocusedFullRace(race);
    const a = await h.coordinator.claimContainerSlot({
      ...race,
      kind: "focused-full",
      workKey: "focused-full:20260603:jra:05:11",
      generationId: "A",
    });
    await h.coordinator.completeFocusedFullRace({
      ...race,
      executionId: a.executionId,
      status: "success",
    });
    await h.coordinator.clearContainerSlot({
      doName: "predict-jra",
      workKey: "focused-full:20260603:jra:05:11",
      executionId: a.executionId,
    });
    observeFocusedFullAuthorityMock.mockImplementation(actual.observeFocusedFullAuthority);
    isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
    isPerRaceFeatureCachePresentMock.mockImplementationOnce(async () => {
      if (change === "replacement")
        await h.coordinator.claimContainerSlot({
          ...race,
          kind: "focused-full",
          workKey: "focused-full:20260603:jra:05:11",
          generationId: "B",
        });
      if (change === "repair")
        await h.coordinator.reserveFocusedFullRaceRepair({
          ...race,
          executionId: a.executionId,
          reservationId: "R",
        });
      if (change === "commit")
        await h.coordinator.beginExternalCommit({
          workKey: "focused-full:20260603:jra:05:11",
          executionId: a.executionId,
          token: "held",
        });
      return true;
    });
    await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), h.env);
    expect(ackMock).not.toHaveBeenCalled();
    expect(retryMock).toHaveBeenCalledOnce();
    expect(stubFetchMock).not.toHaveBeenCalled();
    expect(consumeContainerStopMock).not.toHaveBeenCalled();
    expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
  },
);

test.each(["resume", "payload", "missing"])(
  "routine %s recovery carries exact committed E through actual coordinator helpers",
  async (path) => {
    const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
    const h = makePersistedCoordinator();
    const race = {
      category: "jra",
      runYmd: "20260603",
      keibajoCode: "05",
      raceBango: "11",
      doName: "predict-jra",
      staleAfterMs: 60_000,
    };
    await h.coordinator.claimFocusedFullRace(race);
    const a = await h.coordinator.claimContainerSlot({
      ...race,
      kind: "focused-full",
      workKey: "focused-full:20260603:jra:05:11",
      generationId: "A",
    });
    observeFocusedFullAuthorityMock.mockImplementation(actual.observeFocusedFullAuthority);
    claimFocusedFullRaceMock.mockImplementation(actual.claimFocusedFullRace);
    completeFocusedFullRaceMock.mockImplementation(actual.completeFocusedFullRace);
    reserveFocusedFullRaceRepairMock.mockImplementation(actual.reserveFocusedFullRaceRepair);
    Object.defineProperty(h.env, "FEATURES_CACHE", {
      value: { put: vi.fn(async () => undefined) },
    });
    isFocusedFullPredictionCompleteMock.mockResolvedValue(path !== "resume");
    isPerRaceFeatureCachePresentMock.mockResolvedValue(path !== "missing");
    stubFetchMock.mockImplementation(async (request) =>
      new URL(request?.url ?? "http://do").pathname === "/focused-full-status"
        ? Response.json({
            status: path === "missing" ? "missing" : "success",
            error: null,
            raceKey: "jra:20260603:05:11",
          })
        : Response.json({
            found: path !== "missing",
            parquetKey: "cache",
            parquetBase64: btoa("payload"),
          }),
    );
    await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), h.env);
    expect(ackMock).toHaveBeenCalledOnce();
    expect(retryMock).not.toHaveBeenCalled();
    expect(claimContainerSlotMock).not.toHaveBeenCalled();
    if (path === "missing") {
      expect(reserveFocusedFullRaceRepairMock).toHaveBeenCalledWith(
        expect.objectContaining({ executionId: a.executionId }),
      );
      expect(sendMock).toHaveBeenCalledOnce();
      expect(h.storage.get("focused-full:20260603:jra:05:11")).toMatchObject({
        executionId: a.executionId,
        status: "enqueued",
      });
    } else {
      expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
        expect.objectContaining({ executionId: a.executionId }),
      );
      expect(consumeContainerStopMock).toHaveBeenCalledWith(
        expect.anything(),
        expect.objectContaining({ executionId: a.executionId }),
      );
      expect(publishFinishPositionPredictionCacheMock).toHaveBeenCalledWith(
        expect.objectContaining({ executionId: a.executionId }),
      );
    }
  },
);

test.each(["cache-response", "status-response"])(
  "routine recovery refuses replacement E acquired during %s",
  async (heldRead) => {
    const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
    const h = makePersistedCoordinator();
    const race = {
      category: "jra",
      runYmd: "20260603",
      keibajoCode: "05",
      raceBango: "11",
      doName: "predict-jra",
      staleAfterMs: 60_000,
    };
    await h.coordinator.claimFocusedFullRace(race);
    const a = await h.coordinator.claimContainerSlot({
      ...race,
      kind: "focused-full",
      workKey: "focused-full:20260603:jra:05:11",
      generationId: "A",
    });
    observeFocusedFullAuthorityMock.mockImplementation(actual.observeFocusedFullAuthority);
    claimFocusedFullRaceMock.mockImplementation(actual.claimFocusedFullRace);
    completeFocusedFullRaceMock.mockImplementation(actual.completeFocusedFullRace);
    const put = vi.fn(async () => undefined);
    Object.defineProperty(h.env, "FEATURES_CACHE", { value: { put } });
    isFocusedFullPredictionCompleteMock.mockResolvedValue(heldRead === "cache-response");
    isPerRaceFeatureCachePresentMock.mockResolvedValue(true);
    stubFetchMock.mockImplementationOnce(async () => {
      await h.coordinator.clearContainerSlot({
        doName: "predict-jra",
        workKey: "focused-full:20260603:jra:05:11",
        executionId: a.executionId,
      });
      await h.coordinator.claimContainerSlot({
        ...race,
        kind: "focused-full",
        workKey: "focused-full:20260603:jra:05:11",
        generationId: "B",
      });
      return heldRead === "cache-response"
        ? Response.json({ found: true, parquetKey: "cache", parquetBase64: btoa("stale") })
        : Response.json({ status: "success", error: null, raceKey: "jra:20260603:05:11" });
    });
    stubFetchMock.mockImplementation(async () => Response.json({ found: false }));
    await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), h.env);
    expect(ackMock).not.toHaveBeenCalled();
    expect(retryMock).toHaveBeenCalledOnce();
    expect(put).not.toHaveBeenCalled();
    expect(consumeContainerStopMock).not.toHaveBeenCalled();
    expect(publishFinishPositionPredictionCacheMock).not.toHaveBeenCalled();
    expect(h.storage.get("focused-full:20260603:jra:05:11")).toMatchObject({ status: "started" });
  },
);

test("reserved replacement delivery progresses from old E to a fresh admission without identityless slot", async () => {
  const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
  const h = makePersistedCoordinator();
  h.env.FOCUSED_FULL_WATCH_ENABLED = "1";
  const race = {
    category: "jra",
    runYmd: "20260603",
    keibajoCode: "05",
    raceBango: "11",
    doName: "predict-jra",
    staleAfterMs: 60_000,
  };
  await h.coordinator.claimFocusedFullRace(race);
  const a = await h.coordinator.claimContainerSlot({
    ...race,
    kind: "focused-full",
    workKey: "focused-full:20260603:jra:05:11",
    generationId: "A",
  });
  await h.coordinator.completeFocusedFullRace({
    ...race,
    executionId: a.executionId,
    status: "success",
  });
  await h.coordinator.clearContainerSlot({
    doName: "predict-jra",
    workKey: "focused-full:20260603:jra:05:11",
    executionId: a.executionId,
  });
  await h.coordinator.reserveFocusedFullRaceRepair({
    ...race,
    executionId: a.executionId,
    reservationId: "R",
  });
  observeFocusedFullAuthorityMock.mockImplementation(actual.observeFocusedFullAuthority);
  claimFocusedFullRaceMock.mockImplementation(actual.claimFocusedFullRace);
  claimContainerSlotMock.mockImplementation(actual.claimContainerSlot);
  completeFocusedFullRaceMock.mockImplementation(actual.completeFocusedFullRace);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValue(true);
  stubFetchMock.mockImplementation(async () => Response.json({ found: false }));
  await handleQueue(makeBatch([makeMessage({ skipDedup: true, force: true })]), h.env);
  expect(ackMock).toHaveBeenCalledOnce();
  expect(retryMock).not.toHaveBeenCalled();
  expect(claimContainerSlotMock).toHaveBeenCalledWith(
    expect.objectContaining({ generationId: expect.any(String) }),
  );
  expect(h.storage.get("focused-full:20260603:jra:05:11")).toMatchObject({
    status: "success",
    focusedFullRepairCount: 1,
  });
  expect(h.storage.get("focused-full:20260603:jra:05:11")).not.toMatchObject({
    executionId: a.executionId,
  });
  expect(consumeContainerStopMock).toHaveBeenCalledOnce();
});

test("complete E with missing R2 picks up payload without creating a slot or stopping another owner", async () => {
  const actual = await vi.importActual<typeof import("./do-state")>("./do-state");
  const control =
    await vi.importActual<typeof import("./container-control")>("./container-control");
  const h = makePersistedCoordinator();
  const race = {
    category: "jra",
    runYmd: "20260603",
    keibajoCode: "05",
    raceBango: "11",
    doName: "predict-jra",
    staleAfterMs: 60_000,
  };
  await h.coordinator.claimFocusedFullRace(race);
  const a = await h.coordinator.claimContainerSlot({
    ...race,
    kind: "focused-full",
    workKey: "focused-full:20260603:jra:05:11",
    generationId: "A",
  });
  await h.coordinator.completeFocusedFullRace({
    ...race,
    executionId: a.executionId,
    status: "success",
  });
  await h.coordinator.clearContainerSlot({
    doName: "predict-jra",
    workKey: "focused-full:20260603:jra:05:11",
    executionId: a.executionId,
  });
  const b = await h.coordinator.claimContainerSlot({
    ...race,
    kind: "focused-full",
    workKey: "focused-full:20260603:jra:05:12",
    generationId: "B",
  });
  observeFocusedFullAuthorityMock.mockImplementation(actual.observeFocusedFullAuthority);
  completeFocusedFullRaceMock.mockImplementation(actual.completeFocusedFullRace);
  claimContainerSlotStopMock.mockImplementation(actual.claimContainerSlotStop);
  consumeContainerStopMock.mockImplementation(control.consumeContainerStop);
  const put = vi.fn(async () => undefined);
  Object.defineProperty(h.env, "FEATURES_CACHE", { value: { put } });
  isFocusedFullPredictionCompleteMock.mockResolvedValue(true);
  isPerRaceFeatureCachePresentMock.mockResolvedValueOnce(false).mockResolvedValue(true);
  stubFetchMock.mockImplementation(async () =>
    Response.json({ found: true, parquetKey: "cache", parquetBase64: btoa("payload") }),
  );
  await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), h.env);
  await handleQueue(makeBatch([makeMessage({ skipDedup: true })]), h.env);
  expect(put).toHaveBeenCalledOnce();
  expect(ackMock).toHaveBeenCalledOnce();
  expect(retryMock).toHaveBeenCalledOnce();
  expect(stubFetchMock).toHaveBeenCalledOnce();
  expect(stubFetchMock.mock.calls[0]?.[0]?.url).toMatch(/focused-full-cache/u);
  expect(claimContainerSlotMock).not.toHaveBeenCalled();
  expect(h.storage.get("focused-full:20260603:jra:05:12")).toMatchObject({
    executionId: b.executionId,
    status: "started",
  });
});

test("a failed viewer warm during cache repair releases the commit barrier and retries", async () => {
  const h = makePersistedCoordinator();
  h.storage.set("focused-full:20260603:jra:05:11", {
    executionId: "A",
    status: "success",
    timestamp: 1,
  });
  publishFinishPositionPredictionCacheMock.mockResolvedValueOnce({
    busted: true,
    expectedGeneratedAt: "2026-06-03T01:00:00.000Z",
    status: "written",
  });
  warmPredictionCacheForRaceMock.mockResolvedValueOnce(false);
  await handleQueue(
    {
      messages: [
        {
          ack: ackMock,
          attempts: 1,
          body: {
            category: "jra",
            executionId: "A",
            keibajoCode: "05",
            raceBango: "11",
            runYmd: "20260603",
            type: "prediction-cache-repair",
          },
          id: "cache-repair-warm-false",
          retry: retryMock,
          timestamp: new Date("2026-06-03T00:00:00.000Z"),
        },
      ],
    } as never,
    h.env,
  );
  expect(h.storage.has("external-commit:focused-full:20260603:jra:05:11")).toBe(false);
  expect(retryMock).toHaveBeenCalledOnce();
  expect(ackMock).not.toHaveBeenCalled();
});
