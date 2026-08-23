// Run with: bun run --filter finish-position-cron test
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { beforeEach, expect, test, vi } from "vitest";
import type { Env, PredictQueueMessage } from "../types";

const {
  queryMock,
  neonMock,
  loadSelectedModelMock,
  scoreShadowMock,
  selectShadowModelMock,
  fetchOddsMock,
  fetchWeightMock,
} = vi.hoisted(() => {
  const query = vi.fn(async () => []);
  const loadSelectedModel = vi.fn(async (_bucket, spec) => ({ spec }));
  const selectShadowModel = vi.fn(() => ({
    architecture: "catboost",
    featureCount: 250,
    modelVersion: "jra-cb-v9-sim-2013-clean",
    variant: "sim",
  }));
  const scoreShadow = vi.fn(() => ({
    gradeCode: null,
    modelVersion: "jra-cb-v9-sim-2013-clean",
    predictions: [
      { kettoTorokuBango: "2019100001", predictedRank: 1, predictedScore: 0.9, umaban: 1 },
      { kettoTorokuBango: "2019100002", predictedRank: 2, predictedScore: 0.7, umaban: 2 },
      { kettoTorokuBango: "2019100003", predictedRank: 3, predictedScore: 0.5, umaban: 3 },
    ],
    raceId: "jra:2026:0614:05:11",
    scoreStddev: 0.4,
    shadowOnly: true,
    stage1RescoreRequired: false,
    variant: "sim",
  }));
  const fetchOdds = vi.fn(async () => new Map());
  const fetchWeight = vi.fn(async () => new Map());
  return {
    fetchOddsMock: fetchOdds,
    fetchWeightMock: fetchWeight,
    loadSelectedModelMock: loadSelectedModel,
    neonMock: vi.fn(() => ({ query })),
    queryMock: query,
    scoreShadowMock: scoreShadow,
    selectShadowModelMock: selectShadowModel,
  };
});

vi.mock("@neondatabase/serverless", () => ({ neon: neonMock }));
vi.mock("./jra-shadow-scorer", async () => {
  const actual = await vi.importActual<typeof import("./jra-shadow-scorer")>("./jra-shadow-scorer");
  return {
    ...actual,
    loadSelectedJraShadowModel: loadSelectedModelMock,
    scoreJraRaceShadow: scoreShadowMock,
    selectJraShadowModel: selectShadowModelMock,
  };
});
vi.mock("./rescore-realtime", async () => {
  const actual = await vi.importActual<typeof import("./rescore-realtime")>("./rescore-realtime");
  return { ...actual, fetchOddsForRace: fetchOddsMock, fetchWeightForRace: fetchWeightMock };
});

import {
  buildTargetRaceId,
  buildUpsertParams,
  buildUpsertSql,
  classifyDistanceBand,
  classifyFieldSizeBand,
  classifySeasonBand,
  classifySurface,
  rescoreJraRace,
  splitRaceId,
} from "./rescore-consumer";

const SAMPLE_PARQUET_PATH = join(import.meta.dirname, "__fixtures__", "sample-cache.parquet");
const sampleBytes = new Uint8Array(readFileSync(SAMPLE_PARQUET_PATH));
const EMPTY_PARQUET_PATH = join(import.meta.dirname, "__fixtures__", "empty-cache.parquet");
const emptyBytes = new Uint8Array(readFileSync(EMPTY_PARQUET_PATH));
const PER_RACE_PARQUET_PATH = join(import.meta.dirname, "__fixtures__", "per-race-cache.parquet");
const threeRaceBytes = new Uint8Array(readFileSync(PER_RACE_PARQUET_PATH));

const PER_RACE_CACHE_KEY = "feat-cache/catalog-v1/jra/20260614/05/11/features.parquet";
const WHOLE_DAY_CACHE_KEY = "feat-cache/catalog-v1/jra/20260614/features.parquet";

const cacheObject = {
  arrayBuffer: async () => sampleBytes.buffer.slice(0),
};
const emptyCacheObject = {
  arrayBuffer: async () => emptyBytes.buffer.slice(0),
};

const makeEnv = (getImpl: (key: string) => Promise<unknown>): Env =>
  ({
    FEATURES_CACHE: { get: vi.fn(getImpl) } as unknown as R2Bucket,
    NEON_DATABASE_URL: "postgres://example",
  }) as unknown as Env;

const makeKeyedEnv = (objectsByKey: Map<string, unknown>): Env =>
  makeEnv(async (key: string) => objectsByKey.get(key) ?? null);

const makeMessage = (overrides: Partial<PredictQueueMessage> = {}): PredictQueueMessage => ({
  category: "jra",
  daysAhead: 0,
  keibajoCode: "05",
  mode: "rescore",
  raceBango: "11",
  runDate: "2026-06-14",
  runDateIso: "2026-06-14",
  runYmd: "20260614",
  ...overrides,
});

beforeEach(() => {
  queryMock.mockClear();
  neonMock.mockClear();
  loadSelectedModelMock.mockClear();
  scoreShadowMock.mockClear();
  selectShadowModelMock.mockClear();
  fetchOddsMock.mockClear();
  fetchWeightMock.mockClear();
  queryMock.mockResolvedValue([]);
  fetchOddsMock.mockResolvedValue(new Map());
  fetchWeightMock.mockResolvedValue(new Map());
});

test("buildTargetRaceId composes jra:nen:tsukihi:keibajo:bango from the message", () => {
  expect(buildTargetRaceId(makeMessage())).toBe("jra:2026:0614:05:11");
});

test("splitRaceId splits a colon-delimited race_id into its parts", () => {
  expect(splitRaceId("jra:2026:0614:05:11")).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0614",
    keibajoCode: "05",
    raceBango: "11",
    source: "jra",
  });
});

test("splitRaceId falls back to empty strings for a truncated race_id", () => {
  expect(splitRaceId("jra:2026")).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "",
    keibajoCode: "",
    raceBango: "",
    source: "jra",
  });
});

test("splitRaceId handles a fully empty race_id", () => {
  expect(splitRaceId("")).toStrictEqual({
    kaisaiNen: "",
    kaisaiTsukihi: "",
    keibajoCode: "",
    raceBango: "",
    source: "",
  });
});

test("classifyDistanceBand covers every Container-compatible boundary", () => {
  expect(classifyDistanceBand(null)).toBe(null);
  expect(classifyDistanceBand(1400)).toBe("sprint");
  expect(classifyDistanceBand(1800)).toBe("mile");
  expect(classifyDistanceBand(2200)).toBe("intermediate");
  expect(classifyDistanceBand(2800)).toBe("long");
  expect(classifyDistanceBand(3200)).toBe("extended");
});

test("classifyFieldSizeBand covers every Container-compatible boundary", () => {
  expect(classifyFieldSizeBand(undefined)).toBe(null);
  expect(classifyFieldSizeBand(8)).toBe("small");
  expect(classifyFieldSizeBand(14)).toBe("medium");
  expect(classifyFieldSizeBand(18)).toBe("large");
});

test("classifySeasonBand covers invalid and all seasonal month groups", () => {
  expect(classifySeasonBand("xx01")).toBe(null);
  expect(classifySeasonBand("0301")).toBe("spring");
  expect(classifySeasonBand("0601")).toBe("summer");
  expect(classifySeasonBand("0901")).toBe("autumn");
  expect(classifySeasonBand("1201")).toBe("winter");
});

test("classifySurface covers scalar coercion and every JRA surface", () => {
  expect(classifySurface({ code: 10 })).toBe(null);
  expect(classifySurface("not-a-number")).toBe(null);
  expect(classifySurface(BigInt(10))).toBe("turf");
  expect(classifySurface(23)).toBe("dirt");
  expect(classifySurface("51")).toBe("obstacle");
  expect(classifySurface(99)).toBe(null);
});

test("buildUpsertSql builds a parameterised multi-row UPSERT with $n placeholders", () => {
  const sql = buildUpsertSql(2);
  expect(sql).toBe(
    "insert into race_finish_position_model_predictions (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango, umaban, predicted_score, predicted_rank, predicted_top1_prob, predicted_top3_prob, predicted_finish_position, odds_score, tansho_odds, futan_juryo, weight_diff_from_avg, distance_band, field_size_band, season_band, class_code, surface)\n" +
      "    values\n" +
      "      ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22),\n" +
      "      ($23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44)\n" +
      "    on conflict (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)\n" +
      "    do update set\n" +
      "      umaban = excluded.umaban,\n" +
      "      predicted_score = excluded.predicted_score,\n" +
      "      predicted_rank = excluded.predicted_rank,\n" +
      "      predicted_top1_prob = excluded.predicted_top1_prob,\n" +
      "      predicted_top3_prob = excluded.predicted_top3_prob,\n" +
      "      predicted_finish_position = excluded.predicted_finish_position,\n" +
      "      odds_score = excluded.odds_score,\n" +
      "      tansho_odds = excluded.tansho_odds,\n" +
      "      futan_juryo = excluded.futan_juryo,\n" +
      "      weight_diff_from_avg = excluded.weight_diff_from_avg,\n" +
      "      distance_band = excluded.distance_band,\n" +
      "      field_size_band = excluded.field_size_band,\n" +
      "      season_band = excluded.season_band,\n" +
      "      class_code = excluded.class_code,\n" +
      "      surface = excluded.surface,\n" +
      "      prediction_generated_at = now()",
  );
});

test("buildUpsertParams includes current model, audit fields, and subgroup fields", () => {
  const params = buildUpsertParams(
    [
      {
        kettoTorokuBango: "2019100001",
        predictedRank: 1,
        predictedScore: 0.42,
        umaban: 1,
      },
    ],
    {
      entries: [
        {
          futan_juryo: 55,
          ketto_toroku_bango: "2019100001",
          kyori: 1600,
          kyoso_joken_code: "703",
          odds_score: 0.8,
          shusso_tosu: 12,
          tansho_odds: 2.5,
          track_code: "10",
          weight_diff_from_avg: 4,
        },
      ],
      modelVersion: "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
      parts: {
        kaisaiNen: "2026",
        kaisaiTsukihi: "0614",
        keibajoCode: "05",
        raceBango: "11",
        source: "jra",
      },
    },
  );
  expect(params).toStrictEqual([
    "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
    "jra",
    "2026",
    "0614",
    "05",
    "11",
    "2019100001",
    1,
    0.42,
    1,
    null,
    null,
    null,
    0.8,
    2.5,
    55,
    4,
    "mile",
    "medium",
    "summer",
    "703",
    "turf",
  ]);
});

test("rescoreJraRace returns cache_miss when neither the per-race nor whole-day cache exists", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const env = makeEnv(async () => null);
  const result = await rescoreJraRace({ env, fetchImpl: fetch, message: makeMessage() });
  expect(result.status).toBe("cache_miss");
  expect(queryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("rescoreJraRace returns race_not_found when the target race is absent from the whole-day cache", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const env = makeKeyedEnv(new Map([[WHOLE_DAY_CACHE_KEY, cacheObject]]));
  const result = await rescoreJraRace({
    env,
    fetchImpl: fetch,
    message: makeMessage({ keibajoCode: "09", raceBango: "12" }),
  });
  expect(result.status).toBe("race_not_found");
  expect(queryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("rescoreJraRace reads the per-race cache key directly without whole-day grouping", async () => {
  fetchOddsMock.mockResolvedValue(new Map([[1, { tanshoNinkijun: 1, tanshoOdds: 2.5 }]]));
  fetchWeightMock.mockResolvedValue(new Map([[1, 484]]));
  const perRaceObject = { arrayBuffer: async () => threeRaceBytes.buffer.slice(0) };
  const env = makeKeyedEnv(new Map([[PER_RACE_CACHE_KEY, perRaceObject]]));
  const result = await rescoreJraRace({ env, fetchImpl: fetch, message: makeMessage() });
  expect(result.status).toBe("ok");
  expect(result.racesPredicted).toBe(1);
  expect(result.predictionCount).toBe(3);
  expect(queryMock).toHaveBeenCalledTimes(1);
});

test("rescoreJraRace falls back to the whole-day cache when the per-race key is absent", async () => {
  fetchOddsMock.mockResolvedValue(new Map([[1, { tanshoNinkijun: 1, tanshoOdds: 2.5 }]]));
  fetchWeightMock.mockResolvedValue(new Map([[1, 484]]));
  const env = makeKeyedEnv(new Map([[WHOLE_DAY_CACHE_KEY, cacheObject]]));
  const result = await rescoreJraRace({ env, fetchImpl: fetch, message: makeMessage() });
  expect(result.status).toBe("ok");
  expect(result.predictionCount).toBe(3);
  expect(queryMock).toHaveBeenCalledTimes(1);
});

test("rescoreJraRace returns race_not_found when the per-race cache parquet is empty", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const env = makeKeyedEnv(new Map([[PER_RACE_CACHE_KEY, emptyCacheObject]]));
  const result = await rescoreJraRace({ env, fetchImpl: fetch, message: makeMessage() });
  expect(result.status).toBe("race_not_found");
  expect(queryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("rescoreJraRace passes a 66-element params list for the 3-horse target race", async () => {
  const env = makeKeyedEnv(new Map([[WHOLE_DAY_CACHE_KEY, cacheObject]]));
  await rescoreJraRace({ env, fetchImpl: fetch, message: makeMessage() });
  const call = queryMock.mock.calls[0] as unknown as [string, (string | number | null)[]];
  expect(call[1].length).toBe(66);
  expect(call[1][0]).toBe("jra-cb-v9-sim-2013-clean");
});

test("rescoreJraRace still scores when the realtime odds + weight maps are empty", async () => {
  fetchOddsMock.mockResolvedValue(new Map());
  fetchWeightMock.mockResolvedValue(new Map());
  const env = makeKeyedEnv(new Map([[WHOLE_DAY_CACHE_KEY, cacheObject]]));
  const result = await rescoreJraRace({ env, fetchImpl: fetch, message: makeMessage() });
  expect(result.status).toBe("ok");
  expect(result.predictionCount).toBe(3);
});

test("rescoreJraRace switches to the market-free model when stage-2 spread is degraded", async () => {
  scoreShadowMock
    .mockReturnValueOnce({
      gradeCode: null,
      modelVersion: "jra-cb-v9-sim-2013-clean",
      predictions: [
        { kettoTorokuBango: "2019100001", predictedRank: 1, predictedScore: 0.1, umaban: 1 },
        { kettoTorokuBango: "2019100002", predictedRank: 2, predictedScore: 0.1, umaban: 2 },
        { kettoTorokuBango: "2019100003", predictedRank: 3, predictedScore: 0.1, umaban: 3 },
      ],
      raceId: "jra:2026:0614:05:11",
      scoreStddev: 0,
      shadowOnly: true,
      stage1RescoreRequired: true,
      variant: "sim",
    })
    .mockReturnValueOnce({
      gradeCode: null,
      modelVersion: "jra-cb-stage1-marketfree235-2013",
      predictions: [
        { kettoTorokuBango: "2019100001", predictedRank: 1, predictedScore: 0.8, umaban: 1 },
        { kettoTorokuBango: "2019100002", predictedRank: 2, predictedScore: 0.6, umaban: 2 },
        { kettoTorokuBango: "2019100003", predictedRank: 3, predictedScore: 0.4, umaban: 3 },
      ],
      raceId: "jra:2026:0614:05:11",
      scoreStddev: 0.16,
      shadowOnly: true,
      stage1RescoreRequired: false,
      variant: "stage1_marketfree",
    });
  const env = makeKeyedEnv(new Map([[WHOLE_DAY_CACHE_KEY, cacheObject]]));
  const result = await rescoreJraRace({ env, fetchImpl: fetch, message: makeMessage() });
  expect(result.modelVersion).toBe("jra-cb-stage1-marketfree235-2013");
  expect(loadSelectedModelMock).toHaveBeenCalledTimes(2);
  expect(scoreShadowMock).toHaveBeenCalledTimes(2);
});

test("rescoreJraRace accepts an omitted optional race scope only for cache lookup fallback", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const env = makeEnv(async () => null);
  const result = await rescoreJraRace({
    env,
    fetchImpl: fetch,
    message: makeMessage({ keibajoCode: undefined, raceBango: undefined }),
  });
  expect(result.status).toBe("cache_miss");
  expect(queryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("rescoreJraRace refreshes odds before selecting and scoring the model", async () => {
  fetchOddsMock.mockResolvedValue(
    new Map([
      [1, { tanshoNinkijun: 1, tanshoOdds: 2.5 }],
      [2, { tanshoNinkijun: 2, tanshoOdds: 5 }],
      [3, { tanshoNinkijun: 3, tanshoOdds: 9 }],
    ]),
  );
  fetchWeightMock.mockResolvedValue(new Map());
  const env = makeKeyedEnv(new Map([[WHOLE_DAY_CACHE_KEY, cacheObject]]));
  await rescoreJraRace({ env, fetchImpl: fetch, message: makeMessage() });
  expect(selectShadowModelMock).toHaveBeenCalledTimes(1);
  expect(scoreShadowMock).toHaveBeenCalledTimes(1);
});
