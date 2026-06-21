// Run with: bun run --filter finish-position-cron test
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { beforeEach, expect, test, vi } from "vitest";
import { parseCatBoostJsonModel } from "catboost-json-tree";
import { parseXgboostJsonModel } from "xgboost-json-tree";
import cbSmall from "./__fixtures__/cb-small.json";
import golden from "./__fixtures__/jra-parity-golden.json";
import xgbSmall from "./__fixtures__/xgb-small.json";
import type { Env, PredictQueueMessage } from "../types";

const { queryMock, neonMock, loadJraModelsMock, fetchOddsMock, fetchWeightMock } = vi.hoisted(
  () => {
    const query = vi.fn(async () => []);
    const loadModels = vi.fn();
    const fetchOdds = vi.fn(async () => new Map());
    const fetchWeight = vi.fn(async () => new Map());
    return {
      fetchOddsMock: fetchOdds,
      fetchWeightMock: fetchWeight,
      loadJraModelsMock: loadModels,
      neonMock: vi.fn(() => ({ query })),
      queryMock: query,
    };
  },
);

vi.mock("@neondatabase/serverless", () => ({ neon: neonMock }));
vi.mock("./model-loader", async () => {
  const actual = await vi.importActual<typeof import("./model-loader")>("./model-loader");
  return { ...actual, loadJraModels: loadJraModelsMock };
});
vi.mock("./rescore-realtime", async () => {
  const actual = await vi.importActual<typeof import("./rescore-realtime")>("./rescore-realtime");
  return { ...actual, fetchOddsForRace: fetchOddsMock, fetchWeightForRace: fetchWeightMock };
});

import {
  buildTargetRaceId,
  buildUpsertParams,
  buildUpsertSql,
  raceClassFrom,
  rescoreJraRace,
  splitRaceId,
} from "./rescore-consumer";

const SAMPLE_PARQUET_PATH = join(import.meta.dirname, "__fixtures__", "sample-cache.parquet");
const sampleBytes = new Uint8Array(readFileSync(SAMPLE_PARQUET_PATH));
const EMPTY_PARQUET_PATH = join(import.meta.dirname, "__fixtures__", "empty-cache.parquet");
const emptyBytes = new Uint8Array(readFileSync(EMPTY_PARQUET_PATH));
const PER_RACE_PARQUET_PATH = join(import.meta.dirname, "__fixtures__", "per-race-cache.parquet");
const threeRaceBytes = new Uint8Array(readFileSync(PER_RACE_PARQUET_PATH));

const PER_RACE_CACHE_KEY = "feat-cache/jra/20260614/05/11/features.parquet";
const WHOLE_DAY_CACHE_KEY = "feat-cache/jra/20260614/features.parquet";

const cbModel = parseCatBoostJsonModel(cbSmall);
const xgbModel = parseXgboostJsonModel(xgbSmall);
const featureNames = golden.featureNames;

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
  loadJraModelsMock.mockClear();
  fetchOddsMock.mockClear();
  fetchWeightMock.mockClear();
  queryMock.mockResolvedValue([]);
  loadJraModelsMock.mockResolvedValue({
    catboostModel: cbModel,
    featureNames,
    xgboostModel: xgbModel,
  });
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

test("raceClassFrom reads the kyoso_joken_code of the first row", () => {
  expect(raceClassFrom([{ kyoso_joken_code: 703 }, { kyoso_joken_code: 701 }])).toBe("703");
});

test("raceClassFrom returns null when the first row has no class column", () => {
  expect(raceClassFrom([{ umaban: 1 }])).toBe(null);
});

test("raceClassFrom returns null when the rows array is empty", () => {
  expect(raceClassFrom([])).toBe(null);
});

test("buildUpsertSql builds a parameterised multi-row UPSERT with $n placeholders", () => {
  const sql = buildUpsertSql(2);
  expect(sql).toBe(
    "insert into race_finish_position_model_predictions (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango, umaban, predicted_score, predicted_rank, predicted_top1_prob, predicted_top3_prob, predicted_finish_position)\n" +
      "    values\n" +
      "      ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13),\n" +
      "      ($14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)\n" +
      "    on conflict (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)\n" +
      "    do update set\n" +
      "      umaban = excluded.umaban,\n" +
      "      predicted_score = excluded.predicted_score,\n" +
      "      predicted_rank = excluded.predicted_rank,\n" +
      "      predicted_top1_prob = excluded.predicted_top1_prob,\n" +
      "      predicted_top3_prob = excluded.predicted_top3_prob,\n" +
      "      predicted_finish_position = excluded.predicted_finish_position,\n" +
      "      prediction_generated_at = now()",
  );
});

test("buildUpsertParams flattens predictions into positional params with the E-top2 model_version", () => {
  const params = buildUpsertParams(
    [
      {
        kettoTorokuBango: "2019100001",
        predictedFinishPosition: null,
        predictedRank: 1,
        predictedScore: 0.42,
        predictedTop1Prob: null,
        predictedTop3Prob: null,
        umaban: 1,
      },
    ],
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0614",
      keibajoCode: "05",
      raceBango: "11",
      source: "jra",
    },
  );
  expect(params).toStrictEqual([
    "iter22-jra-etop2",
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

test("rescoreJraRace passes a 39-element params list for the 3-horse target race", async () => {
  const env = makeKeyedEnv(new Map([[WHOLE_DAY_CACHE_KEY, cacheObject]]));
  await rescoreJraRace({ env, fetchImpl: fetch, message: makeMessage() });
  const call = queryMock.mock.calls[0] as unknown as [string, (string | number | null)[]];
  expect(call[1].length).toBe(39);
  expect(call[1][0]).toBe("iter22-jra-etop2");
});

test("rescoreJraRace still scores when the realtime odds + weight maps are empty", async () => {
  fetchOddsMock.mockResolvedValue(new Map());
  fetchWeightMock.mockResolvedValue(new Map());
  const env = makeKeyedEnv(new Map([[WHOLE_DAY_CACHE_KEY, cacheObject]]));
  const result = await rescoreJraRace({ env, fetchImpl: fetch, message: makeMessage() });
  expect(result.status).toBe("ok");
  expect(result.predictionCount).toBe(3);
});

test("rescoreJraRace uses the odds-map size as runner_count so popularity is per-horse", async () => {
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
  const call = queryMock.mock.calls[0] as unknown as [string, (string | number | null)[]];
  const predictedScores = [call[1][8], call[1][21], call[1][34]];
  expect(predictedScores).toStrictEqual([
    0.8595943543380781, 0.6043951943871245, 0.43878901639259865,
  ]);
});
