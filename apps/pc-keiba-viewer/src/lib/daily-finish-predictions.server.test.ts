// Run with bun (bunx vitest).
import { beforeEach, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  getActiveFinishPositionPredictions: vi.fn<() => Promise<unknown>>(),
  getRaceDetail: vi.fn<() => Promise<unknown>>(),
  getRaceRunners: vi.fn<() => Promise<unknown>>(),
  getRacesByDateWithoutJockeyNames: vi.fn<() => Promise<unknown>>(),
}));

vi.mock("../db/queries", () => mocks);

import { getDailyFinishPredictions } from "./daily-finish-predictions.server";

beforeEach(() => {
  vi.clearAllMocks();
});

it("returns ranked JRA predictions with canonical race ids and unavailable races", async () => {
  mocks.getRacesByDateWithoutJockeyNames.mockResolvedValue([
    {
      hassoJikoku: "1530",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0524",
      keibajoCode: "05",
      kyosomeiHondai: "WIN5対象",
      raceBango: "11",
      source: "jra",
    },
    {
      hassoJikoku: "1610",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0524",
      keibajoCode: "05",
      kyosomeiHondai: "最終競走",
      raceBango: "12",
      source: "jra",
    },
    {
      hassoJikoku: "2000",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0524",
      keibajoCode: "44",
      kyosomeiHondai: "NAR競走",
      raceBango: "10",
      source: "nar",
    },
  ]);
  mocks.getRaceDetail
    .mockResolvedValueOnce({
      hassoJikoku: "1530",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0524",
      keibajoCode: "05",
      kyori: "1600",
      kyosomeiHondai: "WIN5対象",
      raceBango: "11",
      source: "jra",
    })
    .mockResolvedValueOnce({
      hassoJikoku: "1610",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0524",
      keibajoCode: "05",
      kyori: "1800",
      kyosomeiHondai: "最終競走",
      raceBango: "12",
      source: "jra",
    });
  mocks.getRaceRunners
    .mockResolvedValueOnce([
      { bamei: "アルファ", horseNameFull: "アルファ号", umaban: "1" },
      { bamei: "ベータ", umaban: "02" },
    ])
    .mockResolvedValueOnce([
      { bamei: "ガンマ", umaban: "1" },
      { bamei: "デルタ", umaban: "2" },
    ]);
  mocks.getActiveFinishPositionPredictions
    .mockResolvedValueOnce([
      {
        confidenceTier: "high",
        horseNumber: "1",
        modelVersion: "jra-model",
        predictedFinishNorm: 0.7,
        predictedScoreStddev: 1.6,
        predictionGeneratedAt: "2026-05-24T01:00:00.000Z",
        showProbability: null,
        winProbability: null,
      },
      {
        confidenceTier: "high",
        horseNumber: "02",
        modelVersion: "jra-model",
        predictedFinishNorm: 0.1,
        predictedScoreStddev: 1.6,
        predictionGeneratedAt: "2026-05-24T01:00:00.000Z",
        showProbability: null,
        winProbability: null,
      },
    ])
    .mockResolvedValueOnce([]);

  const result = await getDailyFinishPredictions({
    day: "24",
    month: "05",
    source: "jra",
    year: "2026",
  });

  expect(result).toStrictEqual({
    availableRaceCount: 1,
    date: "2026-05-24",
    raceCount: 2,
    races: [
      {
        distance: "1600",
        keibajoCode: "05",
        modelVersion: "jra-model",
        prediction: [
          {
            confidenceTier: "high",
            horseName: "ベータ",
            horseNumber: "02",
            modelVersion: "jra-model",
            predictedFinishNorm: 0.1,
            predictedScoreStddev: 1.6,
            predictionGeneratedAt: "2026-05-24T01:00:00.000Z",
            rank: 1,
            showProbability: null,
            winProbability: null,
          },
          {
            confidenceTier: "high",
            horseName: "アルファ号",
            horseNumber: "01",
            modelVersion: "jra-model",
            predictedFinishNorm: 0.7,
            predictedScoreStddev: 1.6,
            predictionGeneratedAt: "2026-05-24T01:00:00.000Z",
            rank: 2,
            showProbability: null,
            winProbability: null,
          },
        ],
        predictionGeneratedAt: "2026-05-24T01:00:00.000Z",
        raceId: "jra:2026:0524:05:11",
        raceName: "WIN5対象",
        raceNumber: "11",
        startTime: "1530",
      },
    ],
    source: "jra",
    unavailableRaceIds: ["jra:2026:0524:05:12"],
  });
  expect(mocks.getRaceDetail).toHaveBeenCalledTimes(2);
  expect(mocks.getRaceRunners).toHaveBeenCalledTimes(2);
});

it("loads only the requested race when a race scope is supplied", async () => {
  mocks.getRacesByDateWithoutJockeyNames.mockResolvedValue([
    {
      hassoJikoku: "1530",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0524",
      keibajoCode: "05",
      kyosomeiHondai: "対象外",
      raceBango: "10",
      source: "jra",
    },
    {
      hassoJikoku: "1540",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0524",
      keibajoCode: "05",
      kyosomeiHondai: "対象競走",
      raceBango: "11",
      source: "jra",
    },
  ]);
  mocks.getRaceDetail.mockResolvedValue({
    hassoJikoku: "1540",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0524",
    keibajoCode: "05",
    kyori: "1600",
    kyosomeiHondai: "対象競走",
    raceBango: "11",
    source: "jra",
  });
  mocks.getRaceRunners.mockResolvedValue([
    { bamei: "アルファ", umaban: "1" },
    { bamei: "ベータ", umaban: "2" },
  ]);
  mocks.getActiveFinishPositionPredictions.mockResolvedValue([
    {
      horseNumber: "1",
      modelVersion: "jra-model",
      predictedFinishNorm: 0.1,
      showProbability: 0.8,
      winProbability: 0.5,
    },
  ]);

  const result = await getDailyFinishPredictions({
    day: "24",
    month: "05",
    race: { keibajoCode: "05", raceNumber: "11" },
    source: "jra",
    year: "2026",
  });

  expect(result.raceCount).toBe(1);
  expect(result.races.map((race) => race.raceId)).toStrictEqual(["jra:2026:0524:05:11"]);
  expect(mocks.getRaceDetail).toHaveBeenCalledTimes(1);
  expect(mocks.getRaceRunners).toHaveBeenCalledTimes(1);
});

it("marks malformed, missing, and entirely null prediction races unavailable", async () => {
  mocks.getRacesByDateWithoutJockeyNames.mockResolvedValue([
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0827",
      keibajoCode: "50",
      raceBango: "01",
      source: "nar",
    },
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0827",
      keibajoCode: "50",
      raceBango: "02",
      source: "nar",
    },
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0827",
      keibajoCode: "50",
      raceBango: "03",
      source: "nar",
    },
  ]);
  mocks.getRaceDetail
    .mockResolvedValueOnce(null)
    .mockResolvedValueOnce({
      hassoJikoku: "1120",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0827",
      keibajoCode: "50",
      kyori: "1400",
      kyosomeiHondai: null,
      raceBango: "02",
      source: "nar",
    })
    .mockResolvedValueOnce({
      hassoJikoku: "1150",
      kaisaiNen: "2026",
      kaisaiTsukihi: "0827",
      keibajoCode: "50",
      kyori: "1400",
      kyosomeiHondai: null,
      raceBango: "03",
      source: "nar",
    });
  mocks.getRaceRunners
    .mockResolvedValueOnce([{ bamei: "一頭", umaban: "1" }])
    .mockResolvedValueOnce([{ bamei: "一頭", umaban: "1" }])
    .mockResolvedValueOnce([
      { bamei: "一頭", umaban: "1" },
      { bamei: "二頭", umaban: "2" },
    ]);
  mocks.getActiveFinishPositionPredictions.mockResolvedValueOnce([
    {
      horseNumber: "bad",
      modelVersion: "nar-model",
      predictedFinishNorm: 0.1,
      showProbability: null,
      winProbability: null,
    },
    {
      horseNumber: "1",
      modelVersion: "nar-model",
      predictedFinishNorm: null,
      showProbability: null,
      winProbability: null,
    },
  ]);

  const result = await getDailyFinishPredictions({
    day: "27",
    month: "08",
    source: "nar",
    year: "2026",
  });

  expect(result).toStrictEqual({
    availableRaceCount: 0,
    date: "2026-08-27",
    raceCount: 3,
    races: [],
    source: "nar",
    unavailableRaceIds: ["nar:2026:0827:50:01", "nar:2026:0827:50:02", "nar:2026:0827:50:03"],
  });
  expect(mocks.getActiveFinishPositionPredictions).toHaveBeenCalledTimes(1);
});
