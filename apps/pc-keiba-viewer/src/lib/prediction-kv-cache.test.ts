// Run with bun: `bun run --filter pc-keiba-viewer test`
import { expect, it } from "vitest";

import {
  PREDICTION_CACHE_BUST_INTERNAL_PATH,
  PREDICTION_KV_CACHE_URL_BASE,
  buildFinishPositionPredictionKvKey,
  buildFinishPredictionInputsCacheKey,
  buildPredictionCacheBustKeys,
  buildRaceYmd,
  buildRunningStylePredictionCacheKeyFromRace,
  buildRunningStylePredictionKvKey,
  createPredictionKvCacheRequest,
  getPredictionCacheApiTtlSeconds,
  getPredictionKvTtlSeconds,
  isPredictionCacheEligibleYmd,
  parseCachedRunningStyleRows,
  parseFinishPositionPredictionKvKey,
  parsePredictionFinishPositionFeatures,
  parsePredictionRunningStyleText,
  parseRunningStylePredictionKvKey,
  raceYmdFromPredictionRaceId,
  resolvePredictionCacheWindow,
  toJstYmd,
} from "./prediction-kv-cache";

const NOON_JST_MS = Date.parse("2026-08-09T12:00:00+09:00");

it("toJstYmd formats a UTC instant in Asia/Tokyo", () => {
  expect(toJstYmd(new Date("2026-08-08T16:00:00.000Z"))).toBe("20260809");
  expect(toJstYmd(new Date("2026-08-08T14:59:59.000Z"))).toBe("20260808");
});

it("buildRaceYmd zero-pads month and day", () => {
  expect(buildRaceYmd("2026", "8", "9")).toBe("20260809");
  expect(buildRaceYmd("2026", "08", "09")).toBe("20260809");
});

it("resolvePredictionCacheWindow classifies today in JST", () => {
  expect(resolvePredictionCacheWindow("20260809", NOON_JST_MS)).toBe("today");
});

it("resolvePredictionCacheWindow classifies yesterday in JST", () => {
  expect(resolvePredictionCacheWindow("20260808", NOON_JST_MS)).toBe("yesterday");
});

it("resolvePredictionCacheWindow classifies tomorrow in JST", () => {
  expect(resolvePredictionCacheWindow("20260810", NOON_JST_MS)).toBe("tomorrow");
});

it("resolvePredictionCacheWindow rejects dates outside the 3-day window", () => {
  expect(resolvePredictionCacheWindow("20260807", NOON_JST_MS)).toBe("outside");
  expect(resolvePredictionCacheWindow("20260811", NOON_JST_MS)).toBe("outside");
});

it("resolvePredictionCacheWindow rejects malformed ymd", () => {
  expect(resolvePredictionCacheWindow("2026-08-09", NOON_JST_MS)).toBe("outside");
  expect(resolvePredictionCacheWindow("abcd0809", NOON_JST_MS)).toBe("outside");
});

it("isPredictionCacheEligibleYmd is true only inside the 3-day window", () => {
  expect(isPredictionCacheEligibleYmd("20260808", NOON_JST_MS)).toBe(true);
  expect(isPredictionCacheEligibleYmd("20260809", NOON_JST_MS)).toBe(true);
  expect(isPredictionCacheEligibleYmd("20260810", NOON_JST_MS)).toBe(true);
  expect(isPredictionCacheEligibleYmd("20260811", NOON_JST_MS)).toBe(false);
});

it("getPredictionKvTtlSeconds uses a long today TTL so race-day writes survive the yesterday window", () => {
  expect(getPredictionKvTtlSeconds("today")).toBe(129600);
  expect(getPredictionKvTtlSeconds("tomorrow")).toBe(86400);
  expect(getPredictionKvTtlSeconds("yesterday")).toBe(86400);
  expect(getPredictionKvTtlSeconds("outside")).toBe(0);
});

it("getPredictionCacheApiTtlSeconds keeps today short for weight-rescore freshness", () => {
  expect(getPredictionCacheApiTtlSeconds("today")).toBe(30);
  expect(getPredictionCacheApiTtlSeconds("tomorrow")).toBe(300);
  expect(getPredictionCacheApiTtlSeconds("yesterday")).toBe(600);
  expect(getPredictionCacheApiTtlSeconds("outside")).toBe(0);
});

it("buildFinishPositionPredictionKvKey zero-pads venue and race number", () => {
  expect(
    buildFinishPositionPredictionKvKey({
      keibajoCode: "5",
      mmdd: "809",
      raceBango: "1",
      year: "2026",
    }),
  ).toBe("pred:fp:v1:20260809:05:01");
});

it("buildRunningStylePredictionKvKey includes source", () => {
  expect(
    buildRunningStylePredictionKvKey({
      keibajoCode: "83",
      mmdd: "0809",
      raceBango: "12",
      source: "nar",
      year: "2026",
    }),
  ).toBe("pred:rs:v1:nar:20260809:83:12");
});

it("buildFinishPredictionInputsCacheKey matches the pred:fp key", () => {
  expect(
    buildFinishPredictionInputsCacheKey({
      day: "9",
      keibajoCode: "05",
      month: "8",
      raceNumber: "11",
      year: "2026",
    }),
  ).toBe("pred:fp:v1:20260809:05:11");
});

it("buildRunningStylePredictionCacheKeyFromRace returns null for an unknown source", () => {
  expect(
    buildRunningStylePredictionCacheKeyFromRace({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0809",
      keibajoCode: "05",
      raceBango: "11",
      source: "overseas",
    }),
  ).toBeNull();
});

it("buildRunningStylePredictionCacheKeyFromRace builds a pred:rs key", () => {
  expect(
    buildRunningStylePredictionCacheKeyFromRace({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0809",
      keibajoCode: "05",
      raceBango: "11",
      source: "jra",
    }),
  ).toBe("pred:rs:v1:jra:20260809:05:11");
});

it("parseFinishPositionPredictionKvKey round-trips", () => {
  expect(parseFinishPositionPredictionKvKey("pred:fp:v1:20260809:05:11")).toStrictEqual({
    keibajoCode: "05",
    mmdd: "0809",
    raceBango: "11",
    year: "2026",
  });
});

it("parseFinishPositionPredictionKvKey rejects a malformed key", () => {
  expect(
    parseFinishPositionPredictionKvKey("pc-keiba-viewer:finish-prediction-inputs:v4"),
  ).toBeNull();
});

it("parseRunningStylePredictionKvKey round-trips", () => {
  expect(parseRunningStylePredictionKvKey("pred:rs:v1:jra:20260809:05:11")).toStrictEqual({
    keibajoCode: "05",
    mmdd: "0809",
    raceBango: "11",
    source: "jra",
    year: "2026",
  });
});

it("parseRunningStylePredictionKvKey rejects a non jra/nar source", () => {
  expect(parseRunningStylePredictionKvKey("pred:rs:v1:world:20260809:05:11")).toBeNull();
});

it("createPredictionKvCacheRequest encodes the key in the cache URL", () => {
  const request = createPredictionKvCacheRequest("pred:fp:v1:20260809:05:11");
  expect(request.url).toBe(
    "https://pc-keiba-viewer.local/prediction-kv/pred%3Afp%3Av1%3A20260809%3A05%3A11",
  );
  expect(PREDICTION_KV_CACHE_URL_BASE).toBe("https://pc-keiba-viewer.local/prediction-kv/");
});

it("buildPredictionCacheBustKeys returns fp then rs keys", () => {
  expect(
    buildPredictionCacheBustKeys({
      keibajoCode: "05",
      mmdd: "0809",
      raceBango: "11",
      source: "jra",
      year: "2026",
    }),
  ).toStrictEqual(["pred:fp:v1:20260809:05:11", "pred:rs:v1:jra:20260809:05:11"]);
});

it("PREDICTION_CACHE_BUST_INTERNAL_PATH points at the internal bust route", () => {
  expect(PREDICTION_CACHE_BUST_INTERNAL_PATH).toBe("/api/internal/prediction-cache-bust");
});

it("raceYmdFromPredictionRaceId concatenates year and mmdd", () => {
  expect(
    raceYmdFromPredictionRaceId({
      keibajoCode: "05",
      mmdd: "0809",
      raceBango: "11",
      year: "2026",
    }),
  ).toBe("20260809");
});
const SAMPLE_RS_ROW = {
  bamei: "サンプル",
  category: "マイル",
  horseNumber: 5,
  kaisaiNen: "2026",
  kettoTorokuBango: "1234567890",
  modelVersion: "prod-v3",
  p_nige: 0.1,
  p_oikomi: 0.3,
  p_sashi: 0.2,
  p_senkou: 0.4,
  predictedAt: "2026-07-22T03:00:00+09:00",
  predictedLabel: "senkou",
  raceKey: "jra:2026:0809:05:11",
};

it("parseCachedRunningStyleRows parses a full row array", () => {
  expect(parseCachedRunningStyleRows([SAMPLE_RS_ROW])).toStrictEqual([SAMPLE_RS_ROW]);
});

it("parseCachedRunningStyleRows returns null for a non-array payload", () => {
  expect(parseCachedRunningStyleRows({ rows: [SAMPLE_RS_ROW] })).toBeNull();
});

it("parseCachedRunningStyleRows returns null for an empty array", () => {
  expect(parseCachedRunningStyleRows([])).toBeNull();
});

it("parseCachedRunningStyleRows rejects a non-object row", () => {
  expect(parseCachedRunningStyleRows([42])).toBeNull();
});

it("parseCachedRunningStyleRows rejects a row with an invalid predictedLabel", () => {
  expect(parseCachedRunningStyleRows([{ ...SAMPLE_RS_ROW, predictedLabel: "bogus" }])).toBeNull();
});

it("parseCachedRunningStyleRows rejects a row with a non-numeric probability", () => {
  expect(
    parseCachedRunningStyleRows([
      {
        ...SAMPLE_RS_ROW,
        p_nige: "not-a-number",
      },
    ]),
  ).toBeNull();
});

it("parsePredictionRunningStyleText parses valid JSON rows", () => {
  expect(parsePredictionRunningStyleText(JSON.stringify([SAMPLE_RS_ROW]))).toStrictEqual([
    SAMPLE_RS_ROW,
  ]);
});

it("parsePredictionRunningStyleText returns null for malformed JSON", () => {
  expect(parsePredictionRunningStyleText("{oops")).toBeNull();
});

it("parsePredictionRunningStyleText returns null for JSON that is not an array", () => {
  expect(parsePredictionRunningStyleText('{"rows":[]}')).toBeNull();
});

it("parsePredictionFinishPositionFeatures parses a feature array", () => {
  const body = JSON.stringify([
    {
      confidenceTier: "high",
      horseNumber: "3",
      modelVersion: "jra-cb-v9-sim-2013-clean",
      predictedFinishNorm: 0.25,
      predictedScoreStddev: 1.4,
      showProbability: 0.12,
      winProbability: 0.05,
    },
  ]);
  expect(parsePredictionFinishPositionFeatures(body)).toStrictEqual([
    {
      confidenceTier: "high",
      horseNumber: "3",
      modelVersion: "jra-cb-v9-sim-2013-clean",
      predictedFinishNorm: 0.25,
      predictedScoreStddev: 1.4,
      showProbability: 0.12,
      winProbability: 0.05,
    },
  ]);
});

it("parsePredictionFinishPositionFeatures accepts a {features:[]} envelope", () => {
  const body = JSON.stringify({
    features: [
      {
        horseNumber: "3",
        modelVersion: "jra-cb-v9-sim-2013-clean",
        predictedFinishNorm: 0.25,
        showProbability: null,
        winProbability: null,
      },
    ],
  });
  expect(parsePredictionFinishPositionFeatures(body)).toStrictEqual([
    {
      confidenceTier: null,
      horseNumber: "3",
      modelVersion: "jra-cb-v9-sim-2013-clean",
      predictedFinishNorm: 0.25,
      predictedScoreStddev: null,
      showProbability: null,
      winProbability: null,
    },
  ]);
});

it("parsePredictionFinishPositionFeatures returns null for malformed JSON", () => {
  expect(parsePredictionFinishPositionFeatures("{oops")).toBeNull();
});

it("parsePredictionFinishPositionFeatures returns null when neither array nor features envelope", () => {
  expect(parsePredictionFinishPositionFeatures('{"k":"v"}')).toBeNull();
});

it("parsePredictionFinishPositionFeatures returns null when features is not an array", () => {
  expect(parsePredictionFinishPositionFeatures('{"features":"x"}')).toBeNull();
});

it("parsePredictionFinishPositionFeatures returns null for an empty array", () => {
  expect(parsePredictionFinishPositionFeatures("[]")).toBeNull();
});

it("parsePredictionFinishPositionFeatures rejects a non-object element", () => {
  expect(parsePredictionFinishPositionFeatures("[42]")).toBeNull();
});

it("parsePredictionFinishPositionFeatures rejects an element missing horseNumber", () => {
  expect(
    parsePredictionFinishPositionFeatures(
      JSON.stringify([{ modelVersion: "jra-cb-v9-sim-2013-clean" }]),
    ),
  ).toBeNull();
});

it("parsePredictionFinishPositionFeatures maps non-number and unknown fields to null", () => {
  const body = JSON.stringify([
    {
      confidenceTier: "mystery",
      horseNumber: "3",
      modelVersion: "jra-cb-v9-sim-2013-clean",
      predictedFinishNorm: "0.25",
      predictedScoreStddev: null,
      showProbability: "0.12",
      winProbability: null,
    },
  ]);
  expect(parsePredictionFinishPositionFeatures(body)).toStrictEqual([
    {
      confidenceTier: null,
      horseNumber: "3",
      modelVersion: "jra-cb-v9-sim-2013-clean",
      predictedFinishNorm: null,
      predictedScoreStddev: null,
      showProbability: null,
      winProbability: null,
    },
  ]);
});
