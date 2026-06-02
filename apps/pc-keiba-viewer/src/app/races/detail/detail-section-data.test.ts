// Run with bun (bunx vitest)

import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

import type { RaceDetail, Runner } from "../../../lib/race-types";
import type {
  RunningStyleBucketFilter,
  RunningStyleBucketMetrics,
} from "../../../lib/running-style-prediction-dimensions";

type GetRaceDetailFn = (
  source: string,
  year: string,
  month: string,
  day: string,
  keibajoCode: string,
  raceNumber: string,
) => Promise<RaceDetail | null>;

type GetRaceRunnersFn = (
  source: string,
  year: string,
  month: string,
  day: string,
  keibajoCode: string,
  raceNumber: string,
) => Promise<Runner[]>;

type GetRunningStyleBucketEvaluationFn = (args: {
  filter: RunningStyleBucketFilter;
}) => Promise<RunningStyleBucketMetrics | null>;

const { getRaceDetailMock, getRaceRunnersMock, getRunningStyleBucketEvaluationMock } = vi.hoisted(
  () => ({
    getRaceDetailMock: vi.fn<GetRaceDetailFn>(),
    getRaceRunnersMock: vi.fn<GetRaceRunnersFn>(),
    getRunningStyleBucketEvaluationMock: vi.fn<GetRunningStyleBucketEvaluationFn>(),
  }),
);

vi.mock("../../../db/queries", () => ({
  getActiveFinishPositionPredictions: vi.fn<() => Promise<unknown[]>>(),
  getActiveFinishPredictionEvaluation: vi.fn<() => Promise<unknown>>(),
  getBloodlineStats: vi.fn<() => Promise<unknown[]>>(),
  getFinishPositionSimilarityFeatures: vi.fn<() => Promise<unknown[]>>(),
  getFinishPositionStats: vi.fn<() => Promise<unknown[]>>(),
  getFrameStats: vi.fn<() => Promise<unknown[]>>(),
  getHorseRaceResults: vi.fn<() => Promise<unknown[]>>(),
  getPayoutStats: vi.fn<() => Promise<unknown[]>>(),
  getRaceAbilityTests: vi.fn<() => Promise<unknown[]>>(),
  getRaceDetail: getRaceDetailMock,
  getRacePaceModelPredictionFeatures: vi.fn<() => Promise<unknown[]>>(),
  getRacePaceSimilarityFeatures: vi.fn<() => Promise<unknown[]>>(),
  getRaceRunners: getRaceRunnersMock,
  getRaceTimeStats: vi.fn<() => Promise<unknown>>(),
  getRaceTrainings: vi.fn<() => Promise<unknown[]>>(),
  getRunningStyleBucketEvaluation: getRunningStyleBucketEvaluationMock,
  getSimilarRaceStats: vi.fn<() => Promise<unknown[]>>(),
  getTimeScoreRows: vi.fn<() => Promise<unknown[]>>(),
}));

vi.mock("../../../lib/premium-data-top-cache.server", () => ({
  getPremiumDataTopHorsesWithCache: vi.fn<() => Promise<unknown[]>>(),
}));

vi.mock("../../../lib/running-style-cache.server", () => ({
  getRaceRunningStylesWithCache: vi.fn<() => Promise<unknown[]>>(),
}));

vi.mock("../../../lib/top-races-cache.server", () => ({
  putTopRaceWindowsCache: vi.fn<() => Promise<void>>(),
  readTopRaceWindowsWithSwr: vi.fn<() => Promise<unknown>>(),
}));

const { getDetailSectionPayload, getRunningStyleBucketSectionData } =
  await import("./detail-section-data");

const JRA_RACE: RaceDetail = {
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: null,
  gradeCode: "G1",
  hassoJikoku: "1525",
  jockeyNames: [],
  juryoShubetsuCode: "11",
  kaisaiKai: "3",
  kaisaiNen: "2025",
  kaisaiNichime: "8",
  kaisaiTsukihi: "1228",
  keibajoCode: "06",
  kyori: "2500",
  kyosoJokenCode: "999",
  kyosoJokenMeisho: "オープン",
  kyosoKigoCode: null,
  kyosoShubetsuCode: "11",
  kyosomeiFukudai: null,
  kyosomeiHondai: "有馬記念",
  kyosomeiKakkonai: null,
  raceBango: "11",
  shussoTosu: "16",
  source: "jra",
  tenkoCode: null,
  torokuTosu: "16",
  trackCode: "10",
};

const NAR_RACE: RaceDetail = {
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: null,
  gradeCode: null,
  hassoJikoku: "1545",
  jockeyNames: [],
  juryoShubetsuCode: "11",
  kaisaiKai: "5",
  kaisaiNen: "2026",
  kaisaiNichime: "3",
  kaisaiTsukihi: "0530",
  keibajoCode: "55",
  kyori: "1800",
  kyosoJokenCode: "000",
  kyosoJokenMeisho: "A2 一般",
  kyosoKigoCode: null,
  kyosoShubetsuCode: "11",
  kyosomeiFukudai: null,
  kyosomeiHondai: "佐賀新聞杯",
  kyosomeiKakkonai: null,
  raceBango: "01",
  shussoTosu: "10",
  source: "nar",
  tenkoCode: null,
  torokuTosu: "10",
  trackCode: null,
};

const BAN_EI_RACE: RaceDetail = {
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: null,
  gradeCode: null,
  hassoJikoku: "1900",
  jockeyNames: [],
  juryoShubetsuCode: "11",
  kaisaiKai: "5",
  kaisaiNen: "2026",
  kaisaiNichime: "3",
  kaisaiTsukihi: "0530",
  keibajoCode: "83",
  kyori: "200",
  kyosoJokenCode: "000",
  kyosoJokenMeisho: "A2",
  kyosoKigoCode: null,
  kyosoShubetsuCode: "11",
  kyosomeiFukudai: null,
  kyosomeiHondai: "ばんえい記念",
  kyosomeiKakkonai: null,
  raceBango: "11",
  shussoTosu: "10",
  source: "nar",
  tenkoCode: null,
  torokuTosu: "10",
  trackCode: null,
};

const HAPPY_METRICS: RunningStyleBucketMetrics = {
  accuracy: 0.6,
  accuracyCI: { lower: 0.5, upper: 0.7 },
  confusionMatrix: [
    [10, 2, 0, 0],
    [2, 10, 1, 0],
    [0, 1, 10, 2],
    [0, 0, 1, 10],
  ],
  macroF1: 0.6,
  overallLogLoss: 0.5,
  perClass: {
    nige: { f1: 0.7, precision: 0.7, recall: 0.7, support: 12 },
    oikomi: { f1: 0.7, precision: 0.7, recall: 0.7, support: 11 },
    sashi: { f1: 0.6, precision: 0.6, recall: 0.6, support: 13 },
    senkou: { f1: 0.6, precision: 0.6, recall: 0.6, support: 13 },
  },
  perClassLogLoss: { nige: 0.4, oikomi: 0.5, sashi: 0.5, senkou: 0.6 },
  predictionCount: 49,
  qwk: 0.7,
  raceCount: 5,
  smallSampleWarning: false,
  top2Accuracy: 0.9,
  weightedF1: 0.6,
};

beforeEach(() => {
  getRaceDetailMock.mockReset();
  getRaceRunnersMock.mockReset();
  getRunningStyleBucketEvaluationMock.mockReset();
});

it("running-style payload returns empty values when getRaceDetail resolves null", async () => {
  getRaceDetailMock.mockResolvedValueOnce(null);
  const payload = await getDetailSectionPayload("running-style", {
    day: "30",
    keibajoCode: "06",
    month: "05",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2026",
  });
  expect(payload).toStrictEqual({
    bucketEvaluation: null,
    bucketFilter: null,
    bucketScope: null,
    dimensionFlags: {
      condition: false,
      distance: false,
      grade: false,
      keibajo: false,
      kyosoJoken: false,
      kyosoShubetsu: false,
      raceName: false,
      track: false,
    },
    type: "running-style",
  });
  expect(getRunningStyleBucketEvaluationMock).not.toHaveBeenCalled();
});

it("running-style payload skips bucket evaluation fetch for ban-ei race", async () => {
  getRaceDetailMock.mockResolvedValueOnce(BAN_EI_RACE);
  const payload = await getDetailSectionPayload("running-style", {
    day: "30",
    keibajoCode: "83",
    month: "05",
    query: {},
    raceNumber: "11",
    raceSource: "nar",
    year: "2026",
  });
  expect(payload).toStrictEqual({
    bucketEvaluation: null,
    bucketFilter: null,
    bucketScope: null,
    dimensionFlags: {
      condition: false,
      distance: false,
      grade: false,
      keibajo: false,
      kyosoJoken: false,
      kyosoShubetsu: false,
      raceName: false,
      track: false,
    },
    type: "running-style",
  });
  expect(getRunningStyleBucketEvaluationMock).not.toHaveBeenCalled();
});

it("running-style payload fetches bucket evaluation with default-on dimension flags for JRA G1", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRunningStyleBucketEvaluationMock.mockResolvedValueOnce(HAPPY_METRICS);
  const payload = await getDetailSectionPayload("running-style", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(payload).toStrictEqual({
    bucketEvaluation: HAPPY_METRICS,
    bucketFilter: {
      category: "jra",
      conditionKey: null,
      enabled: {
        condition: false,
        distance: true,
        grade: false,
        keibajo: true,
        kyosoJoken: true,
        kyosoShubetsu: true,
        raceName: false,
        track: true,
      },
      gradeCode: null,
      keibajoCode: "06",
      kyori: 2500,
      kyosoJokenCode: "999",
      kyosoShubetsuCode: "11",
      period: "all",
      raceName: null,
      source: "jra",
      trackCode: "10",
    },
    bucketScope: {
      flags: {
        condition: false,
        distance: true,
        grade: false,
        keibajo: true,
        kyosoJoken: true,
        kyosoShubetsu: true,
        raceName: false,
        track: true,
      },
      level: "exact",
    },
    dimensionFlags: {
      condition: false,
      distance: true,
      grade: false,
      keibajo: true,
      kyosoJoken: true,
      kyosoShubetsu: true,
      raceName: false,
      track: true,
    },
    type: "running-style",
  });
});

it("running-style payload propagates URL params to disable distance flag", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRunningStyleBucketEvaluationMock.mockResolvedValue(null);
  const payload = await getDetailSectionPayload("running-style", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: { runningStyleDistance: "0" },
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  if (
    payload === null ||
    typeof payload !== "object" ||
    !("type" in payload) ||
    payload.type !== "running-style" ||
    !("dimensionFlags" in payload)
  ) {
    throw new Error("payload must be a running-style payload");
  }
  expect(payload.dimensionFlags).toStrictEqual({
    condition: false,
    distance: false,
    grade: false,
    keibajo: true,
    kyosoJoken: true,
    kyosoShubetsu: true,
    raceName: false,
    track: true,
  });
});

it("running-style payload builds NAR filter with condition key fallback from kyoso_joken_meisho", async () => {
  getRaceDetailMock.mockResolvedValueOnce(NAR_RACE);
  getRunningStyleBucketEvaluationMock.mockResolvedValueOnce(HAPPY_METRICS);
  const payload = await getDetailSectionPayload("running-style", {
    day: "30",
    keibajoCode: "55",
    month: "05",
    query: {},
    raceNumber: "01",
    raceSource: "nar",
    year: "2026",
  });
  if (
    payload === null ||
    typeof payload !== "object" ||
    !("type" in payload) ||
    payload.type !== "running-style" ||
    !("bucketFilter" in payload)
  ) {
    throw new Error("payload must be a running-style payload");
  }
  expect(payload.bucketFilter).toStrictEqual({
    category: "nar",
    conditionKey: "A2 一般",
    enabled: {
      condition: true,
      distance: true,
      grade: false,
      keibajo: true,
      kyosoJoken: false,
      kyosoShubetsu: true,
      raceName: false,
      track: true,
    },
    gradeCode: null,
    keibajoCode: "55",
    kyori: 1800,
    kyosoJokenCode: null,
    kyosoShubetsuCode: "11",
    period: "all",
    raceName: null,
    source: "nar",
    trackCode: null,
  });
});

it("running-style payload returns null bucket evaluation when DB returns no metrics", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRunningStyleBucketEvaluationMock.mockResolvedValue(null);
  const payload = await getDetailSectionPayload("running-style", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  if (
    payload === null ||
    typeof payload !== "object" ||
    !("type" in payload) ||
    payload.type !== "running-style" ||
    !("bucketEvaluation" in payload)
  ) {
    throw new Error("payload must be a running-style payload");
  }
  expect(payload.bucketEvaluation).toBe(null);
});

it("running-style payload only fetches the bucket evaluation when race detail loaded", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRunningStyleBucketEvaluationMock.mockResolvedValueOnce(HAPPY_METRICS);
  await getDetailSectionPayload("running-style", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(getRunningStyleBucketEvaluationMock).toHaveBeenCalledTimes(1);
});

it("getRunningStyleBucketSectionData returns empty bucket data when getRaceDetail resolves null", async () => {
  getRaceDetailMock.mockResolvedValueOnce(null);
  const data = await getRunningStyleBucketSectionData({
    day: "30",
    keibajoCode: "06",
    month: "05",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2026",
  });
  expect(data).toStrictEqual({
    bucketEvaluation: null,
    bucketGradeCode: null,
    bucketRace: null,
    bucketScope: null,
    bucketSource: null,
    dimensionFlags: null,
  });
  expect(getRunningStyleBucketEvaluationMock).not.toHaveBeenCalled();
});

it("getRunningStyleBucketSectionData returns empty bucket data for ban-ei race", async () => {
  getRaceDetailMock.mockResolvedValueOnce(BAN_EI_RACE);
  const data = await getRunningStyleBucketSectionData({
    day: "30",
    keibajoCode: "83",
    month: "05",
    query: {},
    raceNumber: "11",
    raceSource: "nar",
    year: "2026",
  });
  expect(data).toStrictEqual({
    bucketEvaluation: null,
    bucketGradeCode: null,
    bucketRace: null,
    bucketScope: null,
    bucketSource: null,
    dimensionFlags: null,
  });
  expect(getRunningStyleBucketEvaluationMock).not.toHaveBeenCalled();
});

it("getRunningStyleBucketSectionData fetches bucket evaluation and exposes bucketRace fields for JRA G1", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRunningStyleBucketEvaluationMock.mockResolvedValueOnce(HAPPY_METRICS);
  const data = await getRunningStyleBucketSectionData({
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(data).toStrictEqual({
    bucketEvaluation: HAPPY_METRICS,
    bucketGradeCode: "G1",
    bucketRace: {
      gradeCode: "G1",
      keibajoCode: "06",
      kyori: 2500,
      kyosoJokenCode: "999",
      kyosoJokenMeisho: "オープン",
      kyosoShubetsuCode: "11",
      kyosomeiHondai: "有馬記念",
      source: "jra",
      trackCode: "10",
    },
    bucketScope: {
      flags: {
        condition: false,
        distance: true,
        grade: false,
        keibajo: true,
        kyosoJoken: true,
        kyosoShubetsu: true,
        raceName: false,
        track: true,
      },
      level: "exact",
    },
    bucketSource: "jra",
    dimensionFlags: {
      condition: false,
      distance: true,
      grade: false,
      keibajo: true,
      kyosoJoken: true,
      kyosoShubetsu: true,
      raceName: false,
      track: true,
    },
  });
});

it("getRunningStyleBucketSectionData propagates URL params to dimension flags", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRunningStyleBucketEvaluationMock.mockResolvedValue(null);
  const data = await getRunningStyleBucketSectionData({
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: { runningStyleDistance: "0" },
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(data.dimensionFlags).toStrictEqual({
    condition: false,
    distance: false,
    grade: false,
    keibajo: true,
    kyosoJoken: true,
    kyosoShubetsu: true,
    raceName: false,
    track: true,
  });
});

it("getRunningStyleBucketSectionData returns null bucketEvaluation when DB returns no metrics", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRunningStyleBucketEvaluationMock.mockResolvedValue(null);
  const data = await getRunningStyleBucketSectionData({
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(data.bucketEvaluation).toBe(null);
  expect(data.bucketScope).toBe(null);
});

it("getRunningStyleBucketSectionData reports the exact tier when the first call returns metrics", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRunningStyleBucketEvaluationMock.mockResolvedValueOnce(HAPPY_METRICS);
  const data = await getRunningStyleBucketSectionData({
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(getRunningStyleBucketEvaluationMock).toHaveBeenCalledTimes(1);
  expect(data.bucketScope).toStrictEqual({
    flags: {
      condition: false,
      distance: true,
      grade: false,
      keibajo: true,
      kyosoJoken: true,
      kyosoShubetsu: true,
      raceName: false,
      track: true,
    },
    level: "exact",
  });
});

it("getRunningStyleBucketSectionData falls back to the keibajo tier when the exact call returns null", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRunningStyleBucketEvaluationMock.mockResolvedValueOnce(null);
  getRunningStyleBucketEvaluationMock.mockResolvedValueOnce(HAPPY_METRICS);
  const data = await getRunningStyleBucketSectionData({
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(getRunningStyleBucketEvaluationMock).toHaveBeenCalledTimes(2);
  const keibajoCallArg = getRunningStyleBucketEvaluationMock.mock.calls[1]?.[0];
  if (keibajoCallArg === undefined) {
    throw new Error("expected a second bucket evaluation call");
  }
  expect(keibajoCallArg.filter.enabled).toStrictEqual({
    condition: false,
    distance: false,
    grade: false,
    keibajo: true,
    kyosoJoken: false,
    kyosoShubetsu: false,
    raceName: false,
    track: false,
  });
  expect(data.bucketEvaluation).toStrictEqual(HAPPY_METRICS);
  expect(data.bucketScope).toStrictEqual({
    flags: {
      condition: false,
      distance: false,
      grade: false,
      keibajo: true,
      kyosoJoken: false,
      kyosoShubetsu: false,
      raceName: false,
      track: false,
    },
    level: "keibajo",
  });
});

it("getRunningStyleBucketSectionData falls back to the category tier when exact and keibajo return null", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRunningStyleBucketEvaluationMock.mockResolvedValueOnce(null);
  getRunningStyleBucketEvaluationMock.mockResolvedValueOnce(null);
  getRunningStyleBucketEvaluationMock.mockResolvedValueOnce(HAPPY_METRICS);
  const data = await getRunningStyleBucketSectionData({
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(getRunningStyleBucketEvaluationMock).toHaveBeenCalledTimes(3);
  const categoryCallArg = getRunningStyleBucketEvaluationMock.mock.calls[2]?.[0];
  if (categoryCallArg === undefined) {
    throw new Error("expected a third bucket evaluation call");
  }
  expect(categoryCallArg.filter.enabled).toStrictEqual({
    condition: false,
    distance: false,
    grade: false,
    keibajo: false,
    kyosoJoken: false,
    kyosoShubetsu: false,
    raceName: false,
    track: false,
  });
  expect(data.bucketScope).toStrictEqual({
    flags: {
      condition: false,
      distance: false,
      grade: false,
      keibajo: false,
      kyosoJoken: false,
      kyosoShubetsu: false,
      raceName: false,
      track: false,
    },
    level: "category",
  });
});

it("buildRunningStyleBucketSectionPayload falls back to the keibajo tier and exposes the broadened filter", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRunningStyleBucketEvaluationMock.mockResolvedValueOnce(null);
  getRunningStyleBucketEvaluationMock.mockResolvedValueOnce(HAPPY_METRICS);
  const payload = await getDetailSectionPayload("running-style", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  if (
    payload === null ||
    typeof payload !== "object" ||
    !("type" in payload) ||
    payload.type !== "running-style" ||
    !("bucketScope" in payload) ||
    !("bucketFilter" in payload)
  ) {
    throw new Error("payload must be a running-style payload");
  }
  expect(getRunningStyleBucketEvaluationMock).toHaveBeenCalledTimes(2);
  expect(payload.bucketScope).toStrictEqual({
    flags: {
      condition: false,
      distance: false,
      grade: false,
      keibajo: true,
      kyosoJoken: false,
      kyosoShubetsu: false,
      raceName: false,
      track: false,
    },
    level: "keibajo",
  });
  expect(payload.bucketFilter?.enabled).toStrictEqual({
    condition: false,
    distance: false,
    grade: false,
    keibajo: true,
    kyosoJoken: false,
    kyosoShubetsu: false,
    raceName: false,
    track: false,
  });
});
