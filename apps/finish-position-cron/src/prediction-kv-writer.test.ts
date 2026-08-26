// Run with bun.
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import type { PredictionCacheBustOutcome } from "./prediction-cache-bust";
import type { Env } from "./types";

const { queryMock, neonMock, triggerPredictionCacheBustMock } = vi.hoisted(() => {
  const query = vi.fn(async (_sql: string, _params?: unknown[]): Promise<unknown> => []);
  const bust = vi.fn(
    async (): Promise<PredictionCacheBustOutcome> => ({ attempts: 1, status: "ok" }),
  );
  return {
    neonMock: vi.fn(() => ({ query })),
    queryMock: query,
    triggerPredictionCacheBustMock: bust,
  };
});

vi.mock("@neondatabase/serverless", () => ({
  neon: neonMock,
}));

vi.mock("./prediction-cache-bust", async () => {
  const actual =
    await vi.importActual<typeof import("./prediction-cache-bust")>("./prediction-cache-bust");
  return {
    ...actual,
    triggerPredictionCacheBust: triggerPredictionCacheBustMock,
  };
});

import {
  mapFinishPositionPredictionFeatures,
  publishFinishPositionPredictionCache,
  publishFinishPositionPredictionCacheForCategory,
  resolveExpectedPredictionGeneratedAt,
} from "./prediction-kv-writer";

const NOON_JST_MS = Date.parse("2026-08-09T12:00:00+09:00");

const putMock = vi.fn(
  async (_key: string, _value: string, _options?: { expirationTtl: number }): Promise<void> =>
    undefined,
);
const allMock = vi.fn(
  async (): Promise<{ results: Array<{ keibajo_code: string; race_bango: string }> }> => ({
    results: [],
  }),
);
const bindMock = vi.fn(() => ({ all: allMock }));
const prepareMock = vi.fn(() => ({ bind: bindMock }));

const makeEnv = (overrides: Partial<Env> = {}): Env =>
  ({
    DETAIL_SECTION_CACHE_KV: { put: putMock },
    NEON_DATABASE_URL: "postgres://example",
    REALTIME_DB: { prepare: prepareMock },
    ...overrides,
  }) as unknown as Env;

beforeEach(() => {
  queryMock.mockReset();
  neonMock.mockClear();
  triggerPredictionCacheBustMock.mockReset();
  putMock.mockReset();
  allMock.mockReset();
  bindMock.mockReset();
  prepareMock.mockReset();
  queryMock.mockResolvedValue([]);
  triggerPredictionCacheBustMock.mockResolvedValue({ attempts: 1, status: "ok" });
  putMock.mockResolvedValue(undefined);
  allMock.mockResolvedValue({ results: [] });
  prepareMock.mockReturnValue({ bind: bindMock });
  bindMock.mockReturnValue({ all: allMock });
});

afterEach(() => {
  vi.restoreAllMocks();
});

test("mapFinishPositionPredictionFeatures returns empty for no rows", () => {
  expect(mapFinishPositionPredictionFeatures([])).toStrictEqual([]);
});

test("resolveExpectedPredictionGeneratedAt returns the latest normalized timestamp", () => {
  expect(
    resolveExpectedPredictionGeneratedAt([
      {
        modelVersion: "v",
        predictedRank: 1,
        predictionGeneratedAt: "2026-08-22T01:15:00Z",
        predictedScore: 1,
        umaban: "1",
      },
      {
        modelVersion: "v",
        predictedRank: 2,
        predictionGeneratedAt: "2026-08-22T01:16:00.123Z",
        predictedScore: 0,
        umaban: "2",
      },
    ]),
  ).toBe("2026-08-22T01:16:00.123Z");
});

test("resolveExpectedPredictionGeneratedAt fails closed for empty, missing, or invalid timestamps", () => {
  expect(resolveExpectedPredictionGeneratedAt([])).toBeNull();
  expect(
    resolveExpectedPredictionGeneratedAt([
      {
        modelVersion: "v",
        predictedRank: 1,
        predictionGeneratedAt: null,
        predictedScore: 1,
        umaban: "1",
      },
    ]),
  ).toBeNull();
  expect(
    resolveExpectedPredictionGeneratedAt([
      {
        modelVersion: "v",
        predictedRank: 1,
        predictionGeneratedAt: "invalid",
        predictedScore: 1,
        umaban: "1",
      },
    ]),
  ).toBeNull();
});

test("mapFinishPositionPredictionFeatures computes finish norm, stddev, and high confidence", () => {
  expect(
    mapFinishPositionPredictionFeatures([
      {
        modelVersion: "jra-cb-v9-sim-2013-clean",
        predictedRank: 1,
        predictionGeneratedAt: "2026-08-22T01:15:00.000Z",
        predictedScore: 0,
        umaban: "3",
      },
      {
        modelVersion: "jra-cb-v9-sim-2013-clean",
        predictedRank: 2,
        predictionGeneratedAt: null,
        predictedScore: 3,
        umaban: "7",
      },
    ]),
  ).toStrictEqual([
    {
      confidenceTier: "high",
      horseNumber: "3",
      modelVersion: "jra-cb-v9-sim-2013-clean",
      predictedFinishNorm: 0,
      predictionGeneratedAt: "2026-08-22T01:15:00.000Z",
      predictedScoreStddev: 2.1213203435596424,
      showProbability: null,
      winProbability: null,
    },
    {
      confidenceTier: "high",
      horseNumber: "7",
      modelVersion: "jra-cb-v9-sim-2013-clean",
      predictedFinishNorm: 1,
      predictionGeneratedAt: null,
      predictedScoreStddev: 2.1213203435596424,
      showProbability: null,
      winProbability: null,
    },
  ]);
});

test("mapFinishPositionPredictionFeatures uses low confidence below the 1.3 stddev cut", () => {
  const mapped = mapFinishPositionPredictionFeatures([
    {
      modelVersion: "v",
      predictedRank: 1,
      predictionGeneratedAt: null,
      predictedScore: 1.0,
      umaban: "1",
    },
    {
      modelVersion: "v",
      predictedRank: 2,
      predictionGeneratedAt: null,
      predictedScore: 1.1,
      umaban: "2",
    },
  ]);
  expect(mapped[0]?.confidenceTier).toBe("low");
});

test("mapFinishPositionPredictionFeatures uses mid confidence between 1.3 and 1.5 stddev", () => {
  const mapped = mapFinishPositionPredictionFeatures([
    {
      modelVersion: "v",
      predictedRank: 1,
      predictionGeneratedAt: null,
      predictedScore: 0,
      umaban: "1",
    },
    {
      modelVersion: "v",
      predictedRank: 2,
      predictionGeneratedAt: null,
      predictedScore: 2,
      umaban: "2",
    },
  ]);
  expect(mapped[0]?.confidenceTier).toBe("mid");
});

test("mapFinishPositionPredictionFeatures leaves confidence null when fewer than two scores exist", () => {
  expect(
    mapFinishPositionPredictionFeatures([
      {
        modelVersion: "v",
        predictedRank: 1,
        predictionGeneratedAt: null,
        predictedScore: null,
        umaban: "1",
      },
      {
        modelVersion: "v",
        predictedRank: 2,
        predictionGeneratedAt: null,
        predictedScore: 1.4,
        umaban: "2",
      },
    ]),
  ).toStrictEqual([
    {
      confidenceTier: null,
      horseNumber: "1",
      modelVersion: "v",
      predictedFinishNorm: 0,
      predictionGeneratedAt: null,
      predictedScoreStddev: null,
      showProbability: null,
      winProbability: null,
    },
    {
      confidenceTier: null,
      horseNumber: "2",
      modelVersion: "v",
      predictedFinishNorm: 1,
      predictionGeneratedAt: null,
      predictedScoreStddev: null,
      showProbability: null,
      winProbability: null,
    },
  ]);
});

test("publishFinishPositionPredictionCache skips races outside the 3-day JST window", async () => {
  const result = await publishFinishPositionPredictionCache({
    bustCacheApi: false,
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    nowMs: NOON_JST_MS,
    raceBango: "11",
    runYmd: "20260801",
  });
  expect(result).toStrictEqual({
    busted: false,
    expectedGeneratedAt: null,
    status: "skipped-outside-window",
  });
  expect(queryMock).not.toHaveBeenCalled();
  expect(putMock).not.toHaveBeenCalled();
});

test("publishFinishPositionPredictionCache skips when KV binding is absent", async () => {
  const result = await publishFinishPositionPredictionCache({
    bustCacheApi: false,
    category: "jra",
    env: makeEnv({ DETAIL_SECTION_CACHE_KV: undefined }),
    keibajoCode: "05",
    nowMs: NOON_JST_MS,
    raceBango: "11",
    runYmd: "20260809",
  });
  expect(result).toStrictEqual({
    busted: false,
    expectedGeneratedAt: null,
    status: "skipped-no-kv",
  });
  expect(queryMock).not.toHaveBeenCalled();
});

test("publishFinishPositionPredictionCache skips when Neon returns no usable rows", async () => {
  queryMock.mockResolvedValue([
    { model_version: "v", predicted_rank: null, umaban: 1 },
    { model_version: "v", predicted_rank: Number.NaN, umaban: 2 },
    { model_version: "v", predicted_rank: "nope", umaban: 3 },
    {
      model_version: "v",
      predicted_rank: 1,
      predicted_score: true,
      umaban: Number.POSITIVE_INFINITY,
    },
    { model_version: "v", predicted_rank: 1, umaban: null },
  ]);
  const result = await publishFinishPositionPredictionCache({
    bustCacheApi: false,
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    nowMs: NOON_JST_MS,
    raceBango: "11",
    runYmd: "20260809",
  });
  expect(result).toStrictEqual({
    busted: false,
    expectedGeneratedAt: null,
    status: "skipped-empty",
  });
  expect(putMock).not.toHaveBeenCalled();
});

test("publishFinishPositionPredictionCache skips when Neon returns a non-array", async () => {
  queryMock.mockResolvedValue({ rows: [] });
  const result = await publishFinishPositionPredictionCache({
    bustCacheApi: false,
    category: "jra",
    env: makeEnv(),
    keibajoCode: "5",
    nowMs: NOON_JST_MS,
    raceBango: "1",
    runYmd: "20260809",
  });
  expect(result).toStrictEqual({
    busted: false,
    expectedGeneratedAt: null,
    status: "skipped-empty",
  });
});

test("publishFinishPositionPredictionCache fails closed when any row timestamp is invalid", async () => {
  queryMock.mockResolvedValue([
    {
      model_version: "v",
      predicted_rank: 1,
      predicted_score: 1,
      prediction_generated_at: "2026-08-09T01:15:00.000Z",
      umaban: 1,
    },
    {
      model_version: "v",
      predicted_rank: 2,
      predicted_score: 0,
      prediction_generated_at: "invalid",
      umaban: 2,
    },
  ]);
  const result = await publishFinishPositionPredictionCache({
    bustCacheApi: true,
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    nowMs: NOON_JST_MS,
    raceBango: "11",
    runYmd: "20260809",
  });
  expect(result).toStrictEqual({ busted: false, expectedGeneratedAt: null, status: "error" });
  expect(putMock).not.toHaveBeenCalled();
  expect(triggerPredictionCacheBustMock).not.toHaveBeenCalled();
});

test("publishFinishPositionPredictionCache writes today TTL without busting", async () => {
  queryMock.mockResolvedValue([
    {
      model_version: "jra-cb-v9-sim-2013-clean",
      predicted_rank: 1,
      predicted_score: "0",
      prediction_generated_at: "2026-08-22T01:15:00.000Z",
      umaban: 3,
    },
    {
      model_version: "jra-cb-v9-sim-2013-clean",
      predicted_rank: "2",
      predicted_score: 3,
      prediction_generated_at: new Date("2026-08-22T01:15:00.000Z"),
      umaban: "7",
    },
    42,
    {
      model_version: "jra-cb-v9-sim-2013-clean",
      predicted_rank: 3,
      predicted_score: 1,
      umaban: "",
    },
    { predicted_rank: 4, predicted_score: 1, umaban: 9 },
  ]);
  const result = await publishFinishPositionPredictionCache({
    bustCacheApi: false,
    category: "jra",
    env: makeEnv(),
    keibajoCode: "5",
    nowMs: NOON_JST_MS,
    raceBango: "11",
    runYmd: "20260809",
  });
  expect(result).toStrictEqual({
    busted: false,
    expectedGeneratedAt: "2026-08-22T01:15:00.000Z",
    status: "written",
  });
  expect(neonMock).toHaveBeenCalledWith("postgres://example");
  expect(queryMock).toHaveBeenCalledWith(expect.any(String), [
    "jra",
    "2026",
    "0809",
    "05",
    "11",
    "jra",
    "iter40-nar-settransformer-blend-v1",
  ]);
  const selectionSql = queryMock.mock.calls[0]?.[0] ?? "";
  expect(selectionSql).toMatch(/when \$6 = 'nar' and model_version = \$7 then 0/u);
  expect(selectionSql).not.toMatch(/ban-ei/u);
  expect(putMock).toHaveBeenCalledTimes(1);
  const writtenBody = putMock.mock.calls[0]?.[1] ?? "";
  expect(putMock).toHaveBeenCalledWith("pred:fp:v1:20260809:05:11", writtenBody, {
    expirationTtl: 129600,
  });
  const body = writtenBody;
  expect(JSON.parse(body)).toStrictEqual([
    {
      confidenceTier: "high",
      horseNumber: "3",
      modelVersion: "jra-cb-v9-sim-2013-clean",
      predictedFinishNorm: 0,
      predictionGeneratedAt: "2026-08-22T01:15:00.000Z",
      predictedScoreStddev: 2.1213203435596424,
      showProbability: null,
      winProbability: null,
    },
    {
      confidenceTier: "high",
      horseNumber: "7",
      modelVersion: "jra-cb-v9-sim-2013-clean",
      predictedFinishNorm: 1,
      predictionGeneratedAt: "2026-08-22T01:15:00.000Z",
      predictedScoreStddev: 2.1213203435596424,
      showProbability: null,
      winProbability: null,
    },
  ]);
  expect(triggerPredictionCacheBustMock).not.toHaveBeenCalled();
});

test("publishFinishPositionPredictionCache uses yesterday TTL and busts after overwrite", async () => {
  queryMock.mockResolvedValue([
    {
      model_version: "iter12-nar-xgb-hpo-v8-clean188",
      predicted_rank: 1,
      predicted_score: 1.2,
      prediction_generated_at: "2026-08-08T01:15:00.000Z",
      umaban: 1,
    },
    {
      model_version: "iter12-nar-xgb-hpo-v8-clean188",
      predicted_rank: 2,
      predicted_score: 0.4,
      prediction_generated_at: "2026-08-08T01:16:00.000Z",
      umaban: 2,
    },
  ]);
  const result = await publishFinishPositionPredictionCache({
    bustCacheApi: true,
    category: "nar",
    env: makeEnv(),
    keibajoCode: "44",
    nowMs: NOON_JST_MS,
    raceBango: "01",
    runYmd: "20260808",
  });
  expect(result).toStrictEqual({
    busted: true,
    expectedGeneratedAt: "2026-08-08T01:16:00.000Z",
    status: "written",
  });
  expect(putMock).toHaveBeenCalledWith("pred:fp:v1:20260808:44:01", putMock.mock.calls[0]?.[1], {
    expirationTtl: 86400,
  });
  expect(triggerPredictionCacheBustMock).toHaveBeenCalledWith(expect.anything(), {
    keibajoCode: "44",
    mmdd: "0808",
    raceBango: "01",
    source: "nar",
    year: "2026",
  });
});

test("publishFinishPositionPredictionCache uses tomorrow TTL and reports busted false when bust skips", async () => {
  queryMock.mockResolvedValue([
    {
      model_version: "banei-cb-v9-sim-2011",
      predicted_rank: 1,
      predicted_score: 1.1,
      prediction_generated_at: "2026-08-10T01:15:00.000Z",
      umaban: 4,
    },
    {
      model_version: "banei-cb-v9-sim-2011",
      predicted_rank: 2,
      predicted_score: 0.9,
      prediction_generated_at: "2026-08-10T01:15:00.000Z",
      umaban: 8,
    },
  ]);
  triggerPredictionCacheBustMock.mockResolvedValue({
    message: "PC_KEIBA_VIEWER_INTERNAL_TOKEN not configured",
    status: "skipped",
  });
  const result = await publishFinishPositionPredictionCache({
    bustCacheApi: true,
    category: "ban-ei",
    env: makeEnv(),
    keibajoCode: "83",
    nowMs: NOON_JST_MS,
    raceBango: "12",
    runYmd: "20260810",
  });
  expect(result).toStrictEqual({
    busted: false,
    expectedGeneratedAt: "2026-08-10T01:15:00.000Z",
    status: "written",
  });
  expect(queryMock).toHaveBeenCalledWith(expect.any(String), [
    "nar",
    "2026",
    "0810",
    "83",
    "12",
    "ban-ei",
    "iter40-nar-settransformer-blend-v1",
  ]);
  expect(putMock).toHaveBeenCalledWith("pred:fp:v1:20260810:83:12", putMock.mock.calls[0]?.[1], {
    expirationTtl: 86400,
  });
});

test("publishFinishPositionPredictionCache uses Date.now when nowMs is omitted", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(NOON_JST_MS);
  queryMock.mockResolvedValue([
    {
      model_version: "v",
      predicted_rank: 1,
      predicted_score: 0,
      prediction_generated_at: "2026-08-09T01:15:00.000Z",
      umaban: 1,
    },
    {
      model_version: "v",
      predicted_rank: 2,
      predicted_score: 3,
      prediction_generated_at: "2026-08-09T01:15:00.000Z",
      umaban: 2,
    },
  ]);
  const result = await publishFinishPositionPredictionCache({
    bustCacheApi: false,
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    raceBango: "11",
    runYmd: "20260809",
  });
  expect(result).toStrictEqual({
    busted: false,
    expectedGeneratedAt: "2026-08-09T01:15:00.000Z",
    status: "written",
  });
  expect(putMock).toHaveBeenCalledWith("pred:fp:v1:20260809:05:11", putMock.mock.calls[0]?.[1], {
    expirationTtl: 129600,
  });
  vi.useRealTimers();
});

test("publishFinishPositionPredictionCache returns error without throwing when neon fails", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  queryMock.mockRejectedValue(new Error("neon timeout"));
  const result = await publishFinishPositionPredictionCache({
    bustCacheApi: false,
    category: "jra",
    env: makeEnv(),
    keibajoCode: "05",
    nowMs: NOON_JST_MS,
    raceBango: "11",
    runYmd: "20260809",
  });
  expect(result).toStrictEqual({ busted: false, expectedGeneratedAt: null, status: "error" });
  expect(warnSpy).toHaveBeenCalledTimes(1);
});

test("publishFinishPositionPredictionCacheForCategory writes each listed race", async () => {
  allMock.mockResolvedValue({
    results: [
      { keibajo_code: "5", race_bango: "1" },
      { keibajo_code: "05", race_bango: "02" },
    ],
  });
  queryMock.mockResolvedValue([
    {
      model_version: "v",
      predicted_rank: 1,
      predicted_score: 1.6,
      prediction_generated_at: "2026-08-09T01:15:00.000Z",
      umaban: 1,
    },
    {
      model_version: "v",
      predicted_rank: 2,
      predicted_score: 0.2,
      prediction_generated_at: "2026-08-09T01:15:00.000Z",
      umaban: 2,
    },
  ]);
  const written = await publishFinishPositionPredictionCacheForCategory({
    bustCacheApi: false,
    category: "jra",
    env: makeEnv(),
    nowMs: NOON_JST_MS,
    runYmd: "20260809",
  });
  expect(written).toBe(2);
  expect(bindMock).toHaveBeenCalledWith("jra", "2026", "0809");
  expect(putMock).toHaveBeenCalledTimes(2);
  expect(putMock.mock.calls[0]?.[0]).toBe("pred:fp:v1:20260809:05:01");
  expect(putMock.mock.calls[1]?.[0]).toBe("pred:fp:v1:20260809:05:02");
});

test("publishFinishPositionPredictionCacheForCategory skips an overseas A8 venue", async () => {
  allMock.mockResolvedValue({
    results: [
      { keibajo_code: "05", race_bango: "11" },
      { keibajo_code: "A8", race_bango: "04" },
    ],
  });
  queryMock.mockResolvedValue([
    {
      model_version: "v",
      predicted_rank: 1,
      predicted_score: 1.6,
      prediction_generated_at: "2026-08-09T01:15:00.000Z",
      umaban: 1,
    },
  ]);
  const written = await publishFinishPositionPredictionCacheForCategory({
    bustCacheApi: false,
    category: "jra",
    env: makeEnv(),
    nowMs: NOON_JST_MS,
    runYmd: "20260809",
  });
  expect(written).toBe(1);
  expect(putMock).toHaveBeenCalledTimes(1);
  expect(putMock.mock.calls[0]?.[0]).toBe("pred:fp:v1:20260809:05:11");
});

test("publishFinishPositionPredictionCacheForCategory uses ban-ei include filter", async () => {
  allMock.mockResolvedValue({ results: [] });
  await publishFinishPositionPredictionCacheForCategory({
    bustCacheApi: false,
    category: "ban-ei",
    env: makeEnv(),
    nowMs: NOON_JST_MS,
    runYmd: "20260809",
  });
  expect(prepareMock).toHaveBeenCalledWith(expect.stringContaining("keibajo_code in (?)"));
  expect(bindMock).toHaveBeenCalledWith("nar", "2026", "0809", "83");
});

test("publishFinishPositionPredictionCacheForCategory uses nar exclude filter", async () => {
  allMock.mockResolvedValue({ results: [] });
  await publishFinishPositionPredictionCacheForCategory({
    bustCacheApi: false,
    category: "nar",
    env: makeEnv(),
    nowMs: NOON_JST_MS,
    runYmd: "20260809",
  });
  expect(prepareMock).toHaveBeenCalledWith(expect.stringContaining("keibajo_code not in (?)"));
  expect(bindMock).toHaveBeenCalledWith("nar", "2026", "0809", "83");
});

test("publishFinishPositionPredictionCacheForCategory returns 0 when D1 listing throws", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  prepareMock.mockImplementation(() => {
    throw new Error("d1 down");
  });
  const written = await publishFinishPositionPredictionCacheForCategory({
    bustCacheApi: false,
    category: "jra",
    env: makeEnv(),
    nowMs: NOON_JST_MS,
    runYmd: "20260809",
  });
  expect(written).toBe(0);
  expect(warnSpy).toHaveBeenCalledTimes(1);
});
