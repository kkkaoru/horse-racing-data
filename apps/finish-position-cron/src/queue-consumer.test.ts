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
  claimRunMock,
  completeRunMock,
  parseNdjsonStreamMock,
  rescoreJraRaceMock,
  warmPredictionCacheForRaceMock,
  warmPredictionCacheForCategoryMock,
} = vi.hoisted(() => {
  const claimRun = vi.fn(async (): Promise<ClaimResult> => ({ proceed: true }));
  const completeRun = vi.fn(async () => undefined);
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
  return {
    claimRunMock: claimRun,
    completeRunMock: completeRun,
    parseNdjsonStreamMock: parseNdjsonStream,
    rescoreJraRaceMock: rescoreJraRace,
    warmPredictionCacheForCategoryMock: warmPredictionCacheForCategory,
    warmPredictionCacheForRaceMock: warmPredictionCacheForRace,
  };
});

vi.mock("./do-state", () => ({
  claimRun: claimRunMock,
  completeRun: completeRunMock,
}));

vi.mock("./ndjson-stream", () => ({
  parseNdjsonStream: parseNdjsonStreamMock,
}));

vi.mock("./scoring/rescore-consumer", () => ({
  rescoreJraRace: rescoreJraRaceMock,
}));

vi.mock("./prediction-cache-warm", () => ({
  warmPredictionCacheForCategory: warmPredictionCacheForCategoryMock,
  warmPredictionCacheForRace: warmPredictionCacheForRaceMock,
}));

import { handleQueue } from "./queue-consumer";

const ackMock = vi.fn();
const retryMock = vi.fn();
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
  FINISH_POSITION_CRON_DB: {} as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {
    get: getMock,
    idFromName: idFromNameMock,
  } as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: {} as unknown as Env["PREDICT_QUEUE"],
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
  idFromNameMock.mockClear();
  getMock.mockClear();
  stubFetchMock.mockClear();
  claimRunMock.mockClear();
  completeRunMock.mockClear();
  parseNdjsonStreamMock.mockClear();
  rescoreJraRaceMock.mockClear();
  warmPredictionCacheForRaceMock.mockClear();
  warmPredictionCacheForCategoryMock.mockClear();
  warmPredictionCacheForRaceMock.mockResolvedValue(true);
  warmPredictionCacheForCategoryMock.mockResolvedValue(0);
  rescoreJraRaceMock.mockResolvedValue({
    etop2Fired: false,
    predictionCount: 3,
    racesPredicted: 1,
    status: "ok",
  });
  claimRunMock.mockResolvedValue({ proceed: true });
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

test("calls stub.fetch with keibajoCode and raceBango in URL for per-race full mode", async () => {
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
  expect(idFromNameMock).toHaveBeenCalledWith("predict-jra-20260628-02-01");
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(completeRunMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("adds requestId to the DO name for focused per-race full skipDedup messages", async () => {
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
  expect(idFromNameMock).toHaveBeenCalledWith("predict-nar-20260629-35-01-request-123");
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(completeRunMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
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

test("logs container progress for category-level predict messages", async () => {
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
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(consoleSpy).toHaveBeenCalledWith(
    "Predict progress category=jra runYmd=20260603 keibajo=- race=- stage=predict elapsed=12.3",
  );
  expect(consoleSpy).toHaveBeenCalledWith(
    "Predict progress category=jra runYmd=20260603 keibajo=- race=- stage=- elapsed=-",
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

test("routes a JRA per-race rescore to the Worker-native consumer (no container)", async () => {
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
  expect(rescoreJraRaceMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).not.toHaveBeenCalled();
  expect(claimRunMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  consoleSpy.mockRestore();
});

test("acks a JRA per-race rescore that returns cache_miss without retrying", async () => {
  const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  rescoreJraRaceMock.mockResolvedValue({
    etop2Fired: false,
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
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  consoleSpy.mockRestore();
});

test("retries a JRA per-race rescore when the consumer throws", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  rescoreJraRaceMock.mockRejectedValue(new Error("neon down"));
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
  expect(ackMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "Rescore JRA failed runYmd=20260619 keibajo=05 race=11:",
    "Error: neon down",
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

test("targets the per-race rescore at a race-scoped predict-nar DO with the exact query URL", async () => {
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
  expect(idFromNameMock).toHaveBeenCalledWith("predict-nar-20260619-44-01");
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=nar&daysAhead=0&mode=rescore&keibajoCode=44&raceBango=01&runDate=20260619",
  );
  consoleSpy.mockRestore();
});

test("keeps the per-race rescore DO name unchanged when requestId is present", async () => {
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
  expect(idFromNameMock).toHaveBeenCalledWith("predict-nar-20260619-44-01");
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

test("logs container progress with race scope for container per-race rescore messages", async () => {
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

test("routes a Ban-ei per-race rescore to a race-scoped container DO (not Worker-native)", async () => {
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
  expect(idFromNameMock).toHaveBeenCalledWith("predict-ban-ei-20260619-83-07");
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

test("does not warm the category cache for focused per-race skipDedup full messages", async () => {
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
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(warmPredictionCacheForCategoryMock).not.toHaveBeenCalled();
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
  expect(warmPredictionCacheForCategoryMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "Predict failed for category=jra runYmd=20260628 keibajo=02 race=01:",
    "Error: Container result status=error: RuntimeError: focused build failed",
  );
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
  expect(rescoreJraRaceMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).not.toHaveBeenCalled();
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

test("does not warm the race cache when a JRA per-race rescore throws", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  rescoreJraRaceMock.mockRejectedValue(new Error("neon down"));
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
