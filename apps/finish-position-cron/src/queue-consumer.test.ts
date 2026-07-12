// Run with bun. Tests for the queue consumer (DO-backed dedup).

import { beforeEach, expect, test, vi } from "vitest";
import type { ParseNdjsonStreamOptions, PredictResultLine } from "./ndjson-stream";
import type { Env, PredictQueueMessage } from "./types";

interface ClaimResult {
  proceed: boolean;
  state?: string;
}

interface RescoreResult {
  status: "ok" | "cache_miss" | "race_not_found";
  racesPredicted: number;
  predictionCount: number;
  etop2Fired: boolean;
}

const {
  claimFocusedFullRaceMock,
  claimRunMock,
  completeFocusedFullRaceMock,
  completeRunMock,
  isOldDateRunYmdMock,
  parseNdjsonStreamMock,
  rescoreJraRaceMock,
  warmPredictionCacheForRaceMock,
  warmPredictionCacheForCategoryMock,
  isFocusedFullPredictionCompleteMock,
} = vi.hoisted(() => {
  const claimFocusedFullRace = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const claimRun = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const completeFocusedFullRace = vi.fn(async () => undefined);
  const completeRun = vi.fn(async () => undefined);
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
      etop2Fired: false,
      predictionCount: 3,
      racesPredicted: 1,
      status: "ok",
    }),
  );
  const warmPredictionCacheForRace = vi.fn(async (): Promise<boolean> => true);
  const warmPredictionCacheForCategory = vi.fn(async (): Promise<number> => 0);
  const isFocusedFullPredictionComplete = vi.fn(async (): Promise<boolean> => false);
  return {
    claimFocusedFullRaceMock: claimFocusedFullRace,
    claimRunMock: claimRun,
    completeFocusedFullRaceMock: completeFocusedFullRace,
    completeRunMock: completeRun,
    isFocusedFullPredictionCompleteMock: isFocusedFullPredictionComplete,
    isOldDateRunYmdMock: isOldDateRunYmd,
    parseNdjsonStreamMock: parseNdjsonStream,
    rescoreJraRaceMock: rescoreJraRace,
    warmPredictionCacheForCategoryMock: warmPredictionCacheForCategory,
    warmPredictionCacheForRaceMock: warmPredictionCacheForRace,
  };
});

vi.mock("./do-state", () => ({
  claimFocusedFullRace: claimFocusedFullRaceMock,
  claimRun: claimRunMock,
  completeFocusedFullRace: completeFocusedFullRaceMock,
  completeRun: completeRunMock,
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

vi.mock("./prediction-cache-warm", () => ({
  warmPredictionCacheForCategory: warmPredictionCacheForCategoryMock,
  warmPredictionCacheForRace: warmPredictionCacheForRaceMock,
}));

vi.mock("./focused-full-completion", () => ({
  isFocusedFullPredictionComplete: isFocusedFullPredictionCompleteMock,
}));

import { handleQueue } from "./queue-consumer";

const ackMock = vi.fn();
const retryMock = vi.fn();
const sendMock = vi.fn();
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
  PREDICT_QUEUE: { send: sendMock } as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
  REALTIME_DB: {} as unknown as D1Database,
  TRIGGER_TOKEN: "secret-token",
});

const makeMessage = (overrides: Partial<PredictQueueMessage> = {}): Message<PredictQueueMessage> =>
  ({
    ack: ackMock,
    body: {
      category: "jra",
      daysAhead: 2,
      mode: "full",
      runDate: "2026-06-03",
      runDateIso: "2026-06-03",
      runYmd: "20260603",
      ...overrides,
    } satisfies PredictQueueMessage,
    retry: retryMock,
  }) as unknown as Message<PredictQueueMessage>;

const makeBatch = (messages: Message<PredictQueueMessage>[]): MessageBatch<PredictQueueMessage> =>
  ({ messages }) as unknown as MessageBatch<PredictQueueMessage>;

beforeEach(() => {
  ackMock.mockClear();
  retryMock.mockClear();
  sendMock.mockClear();
  sendMock.mockResolvedValue(undefined);
  runMock.mockClear();
  runMock.mockResolvedValue({ success: true });
  bindMock.mockClear();
  prepareMock.mockClear();
  idFromNameMock.mockClear();
  getMock.mockClear();
  stubFetchMock.mockClear();
  claimFocusedFullRaceMock.mockClear();
  claimRunMock.mockClear();
  completeFocusedFullRaceMock.mockClear();
  completeRunMock.mockClear();
  isOldDateRunYmdMock.mockClear();
  isOldDateRunYmdMock.mockReturnValue(false);
  parseNdjsonStreamMock.mockClear();
  rescoreJraRaceMock.mockClear();
  warmPredictionCacheForRaceMock.mockClear();
  warmPredictionCacheForCategoryMock.mockClear();
  isFocusedFullPredictionCompleteMock.mockClear();
  warmPredictionCacheForRaceMock.mockResolvedValue(true);
  warmPredictionCacheForCategoryMock.mockResolvedValue(0);
  isFocusedFullPredictionCompleteMock.mockResolvedValue(false);
  rescoreJraRaceMock.mockResolvedValue({
    etop2Fired: false,
    predictionCount: 3,
    racesPredicted: 1,
    status: "ok",
  });
  claimRunMock.mockResolvedValue({ proceed: true });
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: true });
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
    "http://do/predict?category=jra&daysAhead=2&mode=full&runDate=20260603",
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
    expect(stubFetchMock).toHaveBeenCalledTimes(1);
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
    year: "2026",
  });
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
    expect(stubFetchMock).toHaveBeenCalledTimes(1);
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
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=nar&daysAhead=2&mode=full&runDate=20260629&keibajoCode=35&raceBango=01",
  );
  expect(idFromNameMock).toHaveBeenCalledWith("predict-nar");
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(completeRunMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("retries focused skipDedup full messages without container fetch when focused claim is already started", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  claimFocusedFullRaceMock.mockResolvedValue({ proceed: false, state: "started" });
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
  expect(parseNdjsonStreamMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 150 });
  expect(ackMock).not.toHaveBeenCalled();
  consoleSpy.mockRestore();
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
  expect(idFromNameMock).toHaveBeenCalledTimes(2);
  expect(idFromNameMock).toHaveBeenNthCalledWith(1, "predict-nar");
  expect(idFromNameMock).toHaveBeenNthCalledWith(2, "predict-nar");
  const firstRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  const secondRequest = (stubFetchMock.mock.calls[1] as unknown as [Request])[0];
  expect(firstRequest.url).toContain("raceBango=01");
  expect(secondRequest.url).toContain("raceBango=02");
  expect(ackMock).toHaveBeenCalledTimes(2);
});

test("keeps category-level full messages on the category DO even when requestId is present", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        mode: "full",
        requestId: "request-123",
        runYmd: "20260629",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(idFromNameMock).toHaveBeenCalledWith("predict-nar");
  expect(completeRunMock).toHaveBeenCalledWith(expect.objectContaining({ status: "success" }));
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("omits keibajoCode and raceBango from URL when absent in message", async () => {
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).not.toContain("keibajoCode");
  expect(fetchRequest.url).not.toContain("raceBango");
});

test("calls stub.fetch with mode=rescore when message has mode rescore using YYYYMMDD", async () => {
  await handleQueue(
    makeBatch([makeMessage({ daysAhead: 0, mode: "rescore", runYmd: "20260619" })]),
    makeEnv(),
  );
  expect(idFromNameMock).toHaveBeenCalledWith("predict-jra");
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=0&mode=rescore&runDate=20260619",
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

test("logs container progress for category-level predict messages when debug is enabled", async () => {
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
    "Predict progress category=jra runYmd=20260603 keibajo=- race=- stage=predict elapsed=12.3",
  );
  expect(consoleSpy).toHaveBeenCalledWith(
    "Predict progress category=jra runYmd=20260603 keibajo=- race=- stage=- elapsed=-",
  );
  consoleSpy.mockRestore();
});

test("suppresses container progress logs for normal predict messages", async () => {
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
    "Predict progress category=jra runYmd=20260603 keibajo=- race=- stage=predict elapsed=12.3",
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
  expect(ackMock).not.toHaveBeenCalled();
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
  await Promise.resolve();
  expect(claimRunMock).toHaveBeenCalledTimes(1);
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

test("keeps a per-category rescore (no keibajo) on the container path", async () => {
  await handleQueue(
    makeBatch([makeMessage({ daysAhead: 0, mode: "rescore", runYmd: "20260619" })]),
    makeEnv(),
  );
  expect(rescoreJraRaceMock).not.toHaveBeenCalled();
  expect(idFromNameMock).toHaveBeenCalledWith("predict-jra");
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("skips claimRun and processes via container when category skipDedup is true", async () => {
  await handleQueue(makeBatch([makeMessage({ mode: "full", skipDedup: true })]), makeEnv());
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  expect(completeRunMock).toHaveBeenCalledWith(expect.objectContaining({ status: "success" }));
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("retries a skipDedup message when container fetch fails", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockRejectedValue(new Error("container down"));
  await handleQueue(makeBatch([makeMessage({ mode: "full", skipDedup: true })]), makeEnv());
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(completeRunMock).toHaveBeenCalledWith(expect.objectContaining({ status: "error" }));
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
      staleAfterMs: 900000,
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
    year: "2026",
  });
});

test("warms the category cache for category-level skipDedup full messages", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        mode: "full",
        runDateIso: "2026-06-28",
        runYmd: "20260628",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(completeRunMock).toHaveBeenCalledWith(expect.objectContaining({ status: "success" }));
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(warmPredictionCacheForCategoryMock).toHaveBeenCalledWith(
    expect.objectContaining({ category: "jra", runDate: "2026-06-28", runYmd: "20260628" }),
  );
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
  expect(retryMock).toHaveBeenCalledWith({ delaySeconds: 150 });
  expect(ackMock).not.toHaveBeenCalled();
  expect(completeRunMock).not.toHaveBeenCalled();
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  consoleSpy.mockRestore();
});

test("re-enqueues a fresh message at the base delay on the first busy encounter", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    status: "busy",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        keibajoCode: "35",
        mode: "full",
        raceBango: "01",
        runYmd: "20260629",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(sendMock).toHaveBeenCalledTimes(1);
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      busyRequeueCount: 1,
      category: "nar",
      keibajoCode: "35",
      raceBango: "01",
      runYmd: "20260629",
    }),
    { delaySeconds: 30 },
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
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

test("increments an existing busyRequeueCount and grows the re-enqueue delay when result status is busy again", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    status: "busy",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        busyRequeueCount: 5,
        category: "nar",
        keibajoCode: "35",
        mode: "full",
        raceBango: "01",
        runYmd: "20260629",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(sendMock).toHaveBeenCalledWith(expect.objectContaining({ busyRequeueCount: 6 }), {
    delaySeconds: 130,
  });
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error" }),
  );
  consoleSpy.mockRestore();
});

test("caps the re-enqueue delay at the maximum once busyRequeueCount is high", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    status: "busy",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        busyRequeueCount: 20,
        category: "nar",
        keibajoCode: "35",
        mode: "full",
        raceBango: "01",
        runYmd: "20260629",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(sendMock).toHaveBeenCalledWith(expect.objectContaining({ busyRequeueCount: 21 }), {
    delaySeconds: 300,
  });
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("still re-enqueues one busyRequeueCount below the exhaustion threshold", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    status: "busy",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        busyRequeueCount: 44,
        category: "nar",
        keibajoCode: "35",
        mode: "full",
        raceBango: "01",
        runYmd: "20260629",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(sendMock).toHaveBeenCalledWith(expect.objectContaining({ busyRequeueCount: 45 }), {
    delaySeconds: 300,
  });
  expect(retryMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("retries toward the DLQ instead of re-enqueueing once the busy requeue budget is exhausted", async () => {
  const consoleWarn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "nar",
    status: "busy",
  });
  await handleQueue(
    makeBatch([
      makeMessage({
        busyRequeueCount: 45,
        category: "nar",
        keibajoCode: "35",
        mode: "full",
        raceBango: "01",
        runYmd: "20260629",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(sendMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith();
  expect(ackMock).not.toHaveBeenCalled();
  consoleWarn.mockRestore();
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
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledWith({
    day: "28",
    keibajoCode: "02",
    month: "06",
    raceNumber: "01",
    year: "2026",
  });
  consoleSpy.mockRestore();
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
  expect(warmPredictionCacheForRaceMock).toHaveBeenCalledWith({
    day: "28",
    keibajoCode: "02",
    month: "06",
    raceNumber: "01",
    year: "2026",
  });
});

test("does not treat a category-level full message with status accepted as focused-full acceptance", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  parseNdjsonStreamMock.mockResolvedValue({
    type: "result",
    racesPredicted: 0,
    category: "jra",
    status: "accepted",
  });
  await handleQueue(makeBatch([makeMessage({ mode: "full", skipDedup: true })]), makeEnv());
  expect(completeRunMock).toHaveBeenCalledWith(
    expect.objectContaining({ status: "error", racesPredicted: 0 }),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(retryMock).toHaveBeenCalledWith();
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
  expect(retryMock).toHaveBeenCalledWith();
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
    year: "2026",
  });
  consoleSpy.mockRestore();
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
  errorSpy.mockRestore();
});

test("warms the viewer cache for the category after a skipDedup rescore succeeds", async () => {
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        mode: "rescore",
        runDateIso: "2026-06-19",
        runYmd: "20260619",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(warmPredictionCacheForCategoryMock).toHaveBeenCalledWith(
    expect.objectContaining({ category: "nar", runDate: "2026-06-19", runYmd: "20260619" }),
  );
});

test("does not warm the category cache for a non-skipDedup container run", async () => {
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(warmPredictionCacheForCategoryMock).not.toHaveBeenCalled();
});

test("does not warm the category cache when a skipDedup rescore fails", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  stubFetchMock.mockRejectedValue(new Error("container down"));
  await handleQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        mode: "full",
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
    year: "2026",
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
    year: "2026",
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
  expect(bindMock).toHaveBeenCalledWith("20260101", "jra", "full", null, null, 2);
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
    "Skipping old-dated predict message category=jra runYmd=20260101 mode=full daysAhead=2 skipDedup=false busyRequeueCount=0 thresholdDays=2",
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
