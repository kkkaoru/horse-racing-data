// Run with bun (bunx vitest)

import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

import type {
  FinishPositionBucketFilter,
  FinishPositionBucketMetrics,
} from "../../../lib/finish-prediction-dimensions";
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

type GetFinishPositionBucketEvaluationFn = (args: {
  filter: FinishPositionBucketFilter;
}) => Promise<FinishPositionBucketMetrics | null>;

const {
  getBloodlineStatsMock,
  getCarriedWeightClassStatsMock,
  getFinishPositionStatsMock,
  getFrameStatsMock,
  getHorseRaceResultsMock,
  getPayoutStatsMock,
  getRaceDetailMock,
  getRaceRunnersMock,
  getRaceTimeStatsMock,
  getRaceTrainingsMock,
  getSimilarRaceStatsMock,
  getTimeScoreRowsMock,
  getRunningStyleBucketEvaluationMock,
  getFinishPositionBucketEvaluationMock,
  fetchRaceTrainingsFromCatalogMock,
  getDatabaseTargetMock,
  getWeightClassStatsMock,
} = vi.hoisted(() => ({
  fetchRaceTrainingsFromCatalogMock: vi.fn<() => Promise<unknown[] | null>>(),
  getDatabaseTargetMock: vi.fn<() => "cloudflare" | "local" | "neon">(),
  getBloodlineStatsMock: vi.fn<() => Promise<unknown[]>>(),
  getCarriedWeightClassStatsMock: vi.fn<() => Promise<unknown[]>>(),
  getFinishPositionStatsMock: vi.fn<() => Promise<unknown[]>>(),
  getFrameStatsMock: vi.fn<() => Promise<unknown[]>>(),
  getHorseRaceResultsMock: vi.fn<() => Promise<unknown[]>>(),
  getPayoutStatsMock: vi.fn<() => Promise<unknown[]>>(),
  getRaceDetailMock: vi.fn<GetRaceDetailFn>(),
  getRaceRunnersMock: vi.fn<GetRaceRunnersFn>(),
  getRaceTimeStatsMock: vi.fn<() => Promise<unknown>>(),
  getRaceTrainingsMock: vi.fn<() => Promise<unknown[]>>(),
  getSimilarRaceStatsMock: vi.fn<() => Promise<unknown[]>>(),
  getTimeScoreRowsMock: vi.fn<() => Promise<unknown[]>>(),
  getRunningStyleBucketEvaluationMock: vi.fn<GetRunningStyleBucketEvaluationFn>(),
  getFinishPositionBucketEvaluationMock: vi.fn<GetFinishPositionBucketEvaluationFn>(),
  getWeightClassStatsMock: vi.fn<() => Promise<unknown[]>>(),
}));

vi.mock("../../../db/queries", () => ({
  getActiveFinishPositionPredictions: vi.fn<() => Promise<unknown[]>>(),
  getActiveFinishPredictionEvaluation: vi.fn<() => Promise<unknown>>(),
  getBloodlineStats: getBloodlineStatsMock,
  getFinishPositionBucketEvaluation: getFinishPositionBucketEvaluationMock,
  getFinishPositionSimilarityFeatures: vi.fn<() => Promise<unknown[]>>(),
  getFinishPositionStats: getFinishPositionStatsMock,
  getFrameStats: getFrameStatsMock,
  getWeightClassStats: getWeightClassStatsMock,
  getCarriedWeightClassStats: getCarriedWeightClassStatsMock,
  getHorseRaceResults: getHorseRaceResultsMock,
  getPayoutStats: getPayoutStatsMock,
  getRaceAbilityTests: vi.fn<() => Promise<unknown[]>>(),
  getRaceDetail: getRaceDetailMock,
  getRacePaceModelPredictionFeatures: vi.fn<() => Promise<unknown[]>>(),
  getRacePaceSimilarityFeatures: vi.fn<() => Promise<unknown[]>>(),
  getRaceRunners: getRaceRunnersMock,
  getRaceTimeStats: getRaceTimeStatsMock,
  getRaceTrainings: getRaceTrainingsMock,
  getRunningStyleBucketEvaluation: getRunningStyleBucketEvaluationMock,
  getSimilarRaceStats: getSimilarRaceStatsMock,
  getTimeScoreRows: getTimeScoreRowsMock,
}));

vi.mock("../../../db/client", () => ({
  getDatabaseTarget: getDatabaseTargetMock,
}));

vi.mock("../../../lib/race-time-stats-cache.server", () => ({
  getOrComputeRaceTimeStats: getRaceTimeStatsMock,
}));

vi.mock("../../../lib/race-training-catalog.server", () => ({
  fetchRaceTrainingsFromCatalog: fetchRaceTrainingsFromCatalogMock,
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

const {
  getDetailSectionPayload,
  getDetailStatsContext,
  getFinishPositionBucketSectionData,
  getRunningStyleBucketSectionData,
} = await import("./detail-section-data");

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

const OVERSEAS_RUNNER: Runner = {
  banushimei: "Owner",
  barei: "4",
  bamei: "Overseas Runner",
  bataiju: null,
  chokyoshimeiRyakusho: "Trainer",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  damSireName: null,
  futanJuryo: "570",
  kakuteiChakujun: "00",
  kettoTorokuBango: "0000000000",
  kishumeiRyakusho: "Jockey",
  kohan3f: null,
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
  sohaTime: null,
  tanshoNinkijun: "00",
  tanshoOdds: "0000",
  timeSa: null,
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
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
    nige: { accuracy: 0.86, f1: 0.7, precision: 0.7, recall: 0.7, support: 12 },
    oikomi: { accuracy: 0.86, f1: 0.7, precision: 0.7, recall: 0.7, support: 11 },
    sashi: { accuracy: 0.84, f1: 0.6, precision: 0.6, recall: 0.6, support: 13 },
    senkou: { accuracy: 0.84, f1: 0.6, precision: 0.6, recall: 0.6, support: 13 },
  },
  perClassLogLoss: { nige: 0.4, oikomi: 0.5, sashi: 0.5, senkou: 0.6 },
  predictionCount: 49,
  qwk: 0.7,
  raceCount: 5,
  smallSampleWarning: false,
  top2Accuracy: 0.9,
  corner1PairScore: { pairCount: 100, score: 0.72 },
  corner3PairScore: { pairCount: 100, score: 0.74 },
  corner4PairScore: { pairCount: 100, score: 0.75 },
  finishPairScore: { pairCount: 100, score: 0.7 },
  weightedF1: 0.6,
};

const FINISH_HAPPY_METRICS: FinishPositionBucketMetrics = {
  ndcgAt3Avg: 0.63,
  pairScoreAvg: 0.7,
  place1Accuracy: 0.52,
  place2Accuracy: 0.28,
  place3Accuracy: 0.2,
  predictionCount: 1500,
  raceCount: 120,
  smallSampleWarning: false,
  top1Accuracy: 0.525,
  top1AccuracyCI: { lower: 0.49, upper: 0.56 },
  top3BoxAccuracy: 0.12,
  top3ExactAccuracy: 0.03,
  top3PlaceRelationAvg: 0.57,
  top3WinnerCaptureRate: 0.71,
  top5WinnerCaptureRate: 0.86,
};

beforeEach(() => {
  fetchRaceTrainingsFromCatalogMock.mockReset();
  fetchRaceTrainingsFromCatalogMock.mockResolvedValue(null);
  getDatabaseTargetMock.mockReset();
  getDatabaseTargetMock.mockReturnValue("cloudflare");
  getBloodlineStatsMock.mockReset();
  getCarriedWeightClassStatsMock.mockReset();
  getFinishPositionStatsMock.mockReset();
  getFrameStatsMock.mockReset();
  getHorseRaceResultsMock.mockReset();
  getPayoutStatsMock.mockReset();
  getRaceDetailMock.mockReset();
  getRaceRunnersMock.mockReset();
  getRaceTimeStatsMock.mockReset();
  getRaceTrainingsMock.mockReset();
  getSimilarRaceStatsMock.mockReset();
  getTimeScoreRowsMock.mockReset();
  getRunningStyleBucketEvaluationMock.mockReset();
  getFinishPositionBucketEvaluationMock.mockReset();
  getWeightClassStatsMock.mockReset();
});

it("uses the stable title-relaxed default for ban-ei rate statistics", async () => {
  getRaceDetailMock.mockResolvedValueOnce(BAN_EI_RACE);
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);

  const context = await getDetailStatsContext({
    day: "30",
    keibajoCode: "83",
    month: "05",
    query: {},
    raceNumber: "11",
    raceSource: "nar",
    year: "2026",
  });

  expect(context?.statsSettings.includeRaceTitle).toBe(false);
});

it("uses cell classification flags for condition analysis past-race matching", async () => {
  getRaceDetailMock.mockResolvedValueOnce(NAR_RACE);
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);

  const context = await getDetailStatsContext({
    day: "30",
    keibajoCode: "55",
    month: "05",
    query: {},
    raceNumber: "01",
    raceSource: "nar",
    year: "2026",
  });

  expect(context?.conditionAnalysisSettings).toMatchObject({
    cellMatching: true,
    includeAge: true,
    includeClass: false,
    includeConditionKey: true,
    includeDistance: true,
    includeFrame: false,
    includeGrade: false,
    includeMonthWindow: false,
    includeRaceTitle: false,
    includeSex: false,
    includeSurface: false,
    includeTrackCode: true,
    includeTurn: false,
    includeVenue: true,
    includeWeight: false,
  });
});

it("turns off the analysis cell venue flag from analysisCellKeibajo=0", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);

  const context = await getDetailStatsContext({
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: { analysisCellKeibajo: "0" },
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(context?.conditionAnalysisSettings.cellMatching).toBe(true);
  expect(context?.conditionAnalysisSettings.includeVenue).toBe(false);
  expect(context?.conditionAnalysisSettings.includeClass).toBe(true);
  expect(context?.conditionAnalysisSettings.includeConditionKey).toBe(false);
});

it("bloodline payload filters thin overseas samples and discloses the venue fallback", async () => {
  getRaceDetailMock.mockResolvedValueOnce({ ...JRA_RACE, keibajoCode: "A8" });
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);
  getBloodlineStatsMock.mockResolvedValueOnce([
    {
      category: "sire",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 10,
      name: "Thin Sire",
      quinellaCount: 2,
      quinellaRate: 10,
      showCount: 3,
      showRate: 15,
      starts: 19,
      winCount: 1,
      winRate: 5,
    },
    {
      category: "sireSire",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 10,
      name: "Eligible Sire Sire",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
  ]);

  const payload = await getDetailSectionPayload("bloodline", {
    day: "28",
    keibajoCode: "A8",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    bloodlineVenueFallback: true,
    rows: [{ name: "Eligible Sire Sire", starts: 20 }],
    settings: { includeVenue: false },
    type: "bloodline",
  });
  expect(getBloodlineStatsMock).toHaveBeenCalledOnce();
});

it("similar payload uses broad JV stats for overseas races and suppresses samples below 20 starts", async () => {
  getRaceDetailMock.mockResolvedValueOnce({ ...JRA_RACE, keibajoCode: "A8" });
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);
  getSimilarRaceStatsMock.mockResolvedValueOnce([
    {
      category: "jockey",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 10,
      name: "Thin Jockey",
      quinellaCount: 2,
      quinellaRate: 10,
      showCount: 3,
      showRate: 15,
      starts: 19,
      winCount: 1,
      winRate: 5,
    },
    {
      category: "trainer",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 10,
      name: "Eligible Trainer",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
  ]);
  getBloodlineStatsMock.mockResolvedValue([]);

  const payload = await getDetailSectionPayload("similar", {
    day: "28",
    keibajoCode: "A8",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    rows: [{ name: "Eligible Trainer", starts: 20 }],
    settings: {
      includeAge: false,
      includeClass: false,
      includeDistance: false,
      includeRaceTitle: false,
      includeSurface: false,
      includeTurn: false,
      includeVenue: false,
    },
    similarStatsFallback: true,
    type: "similar",
  });
  expect(getSimilarRaceStatsMock).toHaveBeenCalledOnce();
  expect(getBloodlineStatsMock).toHaveBeenCalledOnce();
});

it("rejects a timed-out parallel domestic bloodline fallback", async () => {
  vi.useFakeTimers();
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);
  getSimilarRaceStatsMock.mockResolvedValueOnce([
    {
      category: "jockey",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 20,
      name: "Jockey",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
    {
      category: "trainer",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 20,
      name: "Trainer",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
  ]);
  getBloodlineStatsMock
    .mockResolvedValueOnce([])
    .mockImplementation(() => new Promise(() => undefined));

  try {
    const payloadPromise = getDetailSectionPayload("similar", {
      day: "28",
      keibajoCode: "06",
      month: "12",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2025",
    });
    payloadPromise.catch(() => undefined);
    await vi.advanceTimersByTimeAsync(6_000);
    await expect(payloadPromise).rejects.toThrow("bloodline statistics fallback timed out");

    expect(getBloodlineStatsMock.mock.calls.length).toBeGreaterThan(2);
  } finally {
    vi.useRealTimers();
  }
});

it("rejects a timed-out similar fallback", async () => {
  vi.useFakeTimers();
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);
  getSimilarRaceStatsMock
    .mockResolvedValueOnce([
      {
        category: "jockey",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "Uncomputed Jockey",
        quinellaCount: 0,
        quinellaRate: 0,
        showCount: 0,
        showRate: 0,
        starts: 0,
        winCount: 0,
        winRate: 0,
      },
      {
        category: "trainer",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 20,
        name: "Computed Trainer",
        quinellaCount: 3,
        quinellaRate: 15,
        showCount: 4,
        showRate: 20,
        starts: 20,
        winCount: 2,
        winRate: 10,
      },
    ])
    .mockImplementation(() => new Promise(() => undefined));
  getBloodlineStatsMock.mockResolvedValueOnce([
    {
      category: "sire",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 20,
      name: "Sire",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
  ]);

  try {
    const payloadPromise = getDetailSectionPayload("similar", {
      day: "28",
      keibajoCode: "06",
      month: "12",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2025",
    });
    payloadPromise.catch(() => undefined);
    await vi.advanceTimersByTimeAsync(6_000);
    await expect(payloadPromise).rejects.toThrow("similar statistics fallback timed out");
  } finally {
    vi.useRealTimers();
  }
});

it("returns the first canonical adequate candidate without waiting for later ones", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);
  const adequateRows = [
    {
      category: "sire",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 20,
      name: "Prefix Sire",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
  ];
  let resolveLater: ((rows: typeof adequateRows) => void) | undefined;
  const later = new Promise<typeof adequateRows>((resolve) => {
    resolveLater = resolve;
  });
  getSimilarRaceStatsMock.mockResolvedValueOnce([
    {
      category: "jockey",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 20,
      name: "Jockey",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
    {
      category: "trainer",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 20,
      name: "Trainer",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
  ]);
  getBloodlineStatsMock
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce(adequateRows)
    .mockReturnValueOnce(later);

  const payloadPromise = getDetailSectionPayload("bloodline", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  const payload = await payloadPromise;
  expect(payload).toMatchObject({
    rows: adequateRows,
    type: "bloodline",
  });
  expect(resolveLater).toBeTypeOf("function");
});

it("does not adopt a later faster candidate before the canonical prefix settles", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);
  const laterAdequateRows = [
    {
      category: "sire",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 20,
      name: "Later Sire",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
  ];
  const earlierAdequateRows = [
    {
      category: "sire",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 20,
      name: "Earlier Sire",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
  ];
  let resolveEarlier: ((rows: typeof earlierAdequateRows) => void) | undefined;
  const earlier = new Promise<typeof earlierAdequateRows>((resolve) => {
    resolveEarlier = resolve;
  });
  getSimilarRaceStatsMock.mockResolvedValueOnce([
    {
      category: "jockey",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 20,
      name: "Jockey",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
    {
      category: "trainer",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 20,
      name: "Trainer",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
  ]);
  getBloodlineStatsMock
    .mockResolvedValueOnce([])
    .mockReturnValueOnce(earlier)
    .mockResolvedValueOnce(laterAdequateRows);

  const payloadPromise = getDetailSectionPayload("bloodline", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  await Promise.resolve();
  expect(resolveEarlier).toBeTypeOf("function");
  resolveEarlier?.(earlierAdequateRows);
  const payload = await payloadPromise;
  expect(payload).toMatchObject({
    rows: earlierAdequateRows,
    type: "bloodline",
  });
});

it("keeps legitimate zero person rows when fallback candidates are exhausted", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);
  const zeroRows = [
    {
      category: "jockey",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 0,
      name: "Zero Jockey",
      quinellaCount: 0,
      quinellaRate: 0,
      showCount: 0,
      showRate: 0,
      starts: 0,
      winCount: 0,
      winRate: 0,
    },
  ];
  getSimilarRaceStatsMock.mockResolvedValue(zeroRows);
  getBloodlineStatsMock.mockResolvedValueOnce([]).mockResolvedValue([]);

  const payload = await getDetailSectionPayload("similar", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({ rows: zeroRows, similarStatsIncomplete: true });
});

it("rejects a timed-out time-score person fallback", async () => {
  vi.useFakeTimers();
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);
  getTimeScoreRowsMock.mockResolvedValueOnce([]);
  getRaceTimeStatsMock.mockResolvedValueOnce({ correlationRows: [] });
  getSimilarRaceStatsMock
    .mockResolvedValueOnce([
      {
        category: "jockey",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "Uncomputed Jockey",
        quinellaCount: 0,
        quinellaRate: 0,
        showCount: 0,
        showRate: 0,
        starts: 0,
        winCount: 0,
        winRate: 0,
      },
    ])
    .mockImplementation(() => new Promise(() => undefined));
  getBloodlineStatsMock.mockResolvedValueOnce([]).mockResolvedValue([]);

  try {
    const payloadPromise = getDetailSectionPayload("time-score", {
      day: "28",
      keibajoCode: "06",
      month: "12",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2025",
    });
    payloadPromise.catch(() => undefined);
    await vi.runAllTimersAsync();
    await expect(payloadPromise).rejects.toThrow("similar statistics fallback timed out");
  } finally {
    vi.useRealTimers();
  }
});

it("rejects a timed-out standalone bloodline fallback", async () => {
  vi.useFakeTimers();
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);
  getBloodlineStatsMock
    .mockResolvedValueOnce([])
    .mockImplementation(() => new Promise(() => undefined));

  try {
    const payloadPromise = getDetailSectionPayload("bloodline", {
      day: "28",
      keibajoCode: "06",
      month: "12",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2025",
    });
    payloadPromise.catch(() => undefined);
    await vi.advanceTimersByTimeAsync(6_000);
    await expect(payloadPromise).rejects.toThrow("bloodline statistics fallback timed out");
  } finally {
    vi.useRealTimers();
  }
});

it("rejects a timed-out time-score bloodline fallback", async () => {
  vi.useFakeTimers();
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);
  getTimeScoreRowsMock.mockResolvedValueOnce([]);
  getRaceTimeStatsMock.mockResolvedValueOnce({ correlationRows: [] });
  getSimilarRaceStatsMock.mockResolvedValueOnce([
    {
      category: "jockey",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 20,
      name: "Jockey",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
    {
      category: "trainer",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 20,
      name: "Trainer",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
  ]);
  getBloodlineStatsMock
    .mockResolvedValueOnce([])
    .mockImplementation(() => new Promise(() => undefined));

  try {
    const payloadPromise = getDetailSectionPayload("time-score", {
      day: "28",
      keibajoCode: "06",
      month: "12",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2025",
    });
    payloadPromise.catch(() => undefined);
    await vi.runAllTimersAsync();
    await expect(payloadPromise).rejects.toThrow("bloodline statistics fallback timed out");
  } finally {
    vi.useRealTimers();
  }
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

it("finish-position bucket returns empty values when getRaceDetail resolves null", async () => {
  getRaceDetailMock.mockResolvedValueOnce(null);
  const data = await getFinishPositionBucketSectionData({
    day: "30",
    keibajoCode: "06",
    month: "05",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(data).toStrictEqual({
    bucketEvaluation: null,
    bucketGradeCode: null,
    bucketModelVersion: null,
    bucketRace: null,
    bucketScope: null,
    bucketSource: null,
  });
  expect(getFinishPositionBucketEvaluationMock).not.toHaveBeenCalled();
});

it("finish-position bucket resolves the JRA v7-lineage model version on an exact-tier hit", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getFinishPositionBucketEvaluationMock.mockResolvedValueOnce(FINISH_HAPPY_METRICS);
  const data = await getFinishPositionBucketSectionData({
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(data.bucketModelVersion).toBe("jra-cb-v7-lineage-wf-21y");
  expect(data.bucketEvaluation).toStrictEqual(FINISH_HAPPY_METRICS);
  expect(data.bucketScope?.level).toBe("exact");
  expect(data.bucketSource).toBe("jra");
});

it("finish-position bucket resolves the ban-ei v7-lineage model version", async () => {
  getRaceDetailMock.mockResolvedValueOnce(BAN_EI_RACE);
  getFinishPositionBucketEvaluationMock.mockResolvedValueOnce(FINISH_HAPPY_METRICS);
  const data = await getFinishPositionBucketSectionData({
    day: "30",
    keibajoCode: "83",
    month: "05",
    query: {},
    raceNumber: "11",
    raceSource: "nar",
    year: "2026",
  });
  expect(data.bucketModelVersion).toBe("banei-cb-v7-lineage-wf-21y");
});

it("finish-position bucket resolves the NAR v7-lineage model version", async () => {
  getRaceDetailMock.mockResolvedValueOnce(NAR_RACE);
  getFinishPositionBucketEvaluationMock.mockResolvedValueOnce(FINISH_HAPPY_METRICS);
  const data = await getFinishPositionBucketSectionData({
    day: "30",
    keibajoCode: "55",
    month: "05",
    query: {},
    raceNumber: "01",
    raceSource: "nar",
    year: "2026",
  });
  expect(data.bucketModelVersion).toBe("nar-xgb-v7-lineage-wf-21y");
});

it("finish-position bucket falls back to the category tier when exact and keibajo tiers miss", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getFinishPositionBucketEvaluationMock
    .mockResolvedValueOnce(null)
    .mockResolvedValueOnce(null)
    .mockResolvedValueOnce(FINISH_HAPPY_METRICS);
  const data = await getFinishPositionBucketSectionData({
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(data.bucketScope?.level).toBe("category");
  expect(getFinishPositionBucketEvaluationMock).toHaveBeenCalledTimes(3);
});

it("finish-position bucket returns a null evaluation when every tier misses", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getFinishPositionBucketEvaluationMock.mockResolvedValue(null);
  const data = await getFinishPositionBucketSectionData({
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
  expect(data.bucketModelVersion).toBe("jra-cb-v7-lineage-wf-21y");
});

it("training payload decodes netkeiba oikiri.html as utf-8 and merges parsed reviews into trainings", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  getRaceTrainingsMock.mockResolvedValueOnce([
    {
      babamawari: null,
      bamei: "テストホース",
      chokyoJikoku: "0500",
      chokyoNengappi: "20251225",
      course: null,
      lapTime10f: null,
      lapTime1f: null,
      lapTime2f: null,
      lapTime3f: null,
      lapTime4f: null,
      lapTime5f: null,
      lapTime6f: null,
      lapTime7f: null,
      lapTime8f: null,
      lapTime9f: null,
      timeGokei10f: null,
      timeGokei2f: null,
      timeGokei3f: null,
      timeGokei4f: null,
      timeGokei5f: null,
      timeGokei6f: null,
      timeGokei7f: null,
      timeGokei8f: null,
      timeGokei9f: null,
      tracenKubun: null,
      trainingType: "通常",
      umaban: "5",
    },
  ]);
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(
      new Response(JSON.stringify({ stableComments: [], trainingReviews: [] }), {
        headers: { "content-type": "application/json" },
        status: 200,
      }),
    )
    .mockResolvedValueOnce(
      new Response(
        '<tr class="OikiriDataHead1"><td class="Umaban">5</td><td class="Training_Critic">気配上昇</td><td class="Rank_S">S</td></tr>',
        { headers: { "content-type": "text/html; charset=UTF-8" }, status: 200 },
      ),
    );
  const payload = await getDetailSectionPayload("training", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(payload).toStrictEqual({
    sourceLabel: "JRA 中央競馬",
    stableComments: [],
    trainings: [
      {
        babamawari: null,
        bamei: "テストホース",
        chokyoJikoku: "0500",
        chokyoNengappi: "20251225",
        course: null,
        lapTime10f: null,
        lapTime1f: null,
        lapTime2f: null,
        lapTime3f: null,
        lapTime4f: null,
        lapTime5f: null,
        lapTime6f: null,
        lapTime7f: null,
        lapTime8f: null,
        lapTime9f: null,
        premiumCommentText: null,
        premiumEvaluationGrade: "S",
        premiumEvaluationText: "気配上昇",
        timeGokei10f: null,
        timeGokei2f: null,
        timeGokei3f: null,
        timeGokei4f: null,
        timeGokei5f: null,
        timeGokei6f: null,
        timeGokei7f: null,
        timeGokei8f: null,
        timeGokei9f: null,
        tracenKubun: null,
        trainingRiderName: undefined,
        trainingType: "通常",
        umaban: "5",
      },
    ],
    type: "training",
  });
  expect(fetchMock.mock.calls[1]?.[0]).toBe(
    "https://race.netkeiba.com/race/oikiri.html?race_id=2025063811",
  );
  expect(fetchRaceTrainingsFromCatalogMock).toHaveBeenCalledWith({
    day: "28",
    keibajoCode: "06",
    month: "12",
    raceBango: "11",
    year: "2025",
  });
  expect(getRaceTrainingsMock).toHaveBeenCalledTimes(1);
  fetchMock.mockRestore();
});

it("training payload uses Catalog workout times and ignores D1 trainingWorkouts", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  fetchRaceTrainingsFromCatalogMock.mockResolvedValueOnce([
    {
      babamawari: "右",
      bamei: "現地調教馬",
      chokyoJikoku: "0600",
      chokyoNengappi: "20251227",
      course: "札幌ダート",
      currentJockeyName: "騎手",
      lapTime10f: null,
      lapTime1f: "123",
      lapTime2f: null,
      lapTime3f: null,
      lapTime4f: null,
      lapTime5f: null,
      lapTime6f: null,
      lapTime7f: null,
      lapTime8f: null,
      lapTime9f: null,
      timeGokei10f: null,
      premiumWorkoutIndex: 0,
      timeGokei2f: "247",
      timeGokei3f: "372",
      timeGokei4f: "498",
      timeGokei5f: null,
      timeGokei6f: null,
      timeGokei7f: null,
      timeGokei8f: null,
      timeGokei9f: null,
      tracenKubun: "札幌",
      trainerName: "調教師",
      trainingDataSource: "netkeiba",
      trainingRiderName: "助手",
      trainingType: "ダート",
      umaban: "5",
    },
  ]);
  const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
    new Response(
      JSON.stringify({
        stableComments: [],
        trainingReviews: [
          {
            commentText: "状態良好",
            evaluationGrade: "S",
            evaluationText: "抜群",
            horseNumber: "5",
            riderName: "助手",
            trainingDate: "20251227",
          },
        ],
        trainingWorkouts: [
          null,
          { horseNumber: "5", trainingDate: "20251227", workoutIndex: "invalid" },
          {
            course: "札幌ダート",
            courseDirection: "右",
            evaluationGrade: "A",
            evaluationText: "好気配",
            horseName: "現地調教馬",
            horseNumber: "5",
            lapTime1f: "999",
            riderName: "助手",
            timeGokei2f: "247",
            timeGokei3f: "372",
            timeGokei4f: "498",
            tracenKubun: "札幌",
            trainingDate: "20251227",
            trainingTime: "0600",
            trainingType: "ダート",
            workoutIndex: 0,
          },
        ],
      }),
      { headers: { "content-type": "application/json" }, status: 200 },
    ),
  );

  const payload = await getDetailSectionPayload("training", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload?.type).toBe("training");
  if (
    !payload ||
    payload.type !== "training" ||
    !("trainings" in payload) ||
    !Array.isArray(payload.trainings)
  ) {
    throw new Error("training payload expected");
  }
  expect(payload.trainings).toHaveLength(1);
  expect(payload.trainings[0]?.chokyoNengappi).toBe("20251227");
  expect(payload.trainings[0]?.timeGokei4f).toBe("498");
  expect(payload.trainings[0]?.lapTime1f).toBe("123");
  expect(payload.trainings[0]?.premiumEvaluationGrade).toBe("S");
  expect(payload.trainings[0]?.trainingDataSource).toBe("netkeiba");
  expect(getRaceTrainingsMock).not.toHaveBeenCalled();
  expect(fetchMock).toHaveBeenCalledTimes(1);
  fetchMock.mockRestore();
});

it("training payload returns empty trainingReviews when netkeiba oikiri.html returns non-ok", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  fetchRaceTrainingsFromCatalogMock.mockResolvedValueOnce([]);
  getRaceTrainingsMock.mockResolvedValueOnce([]);
  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(
      new Response(JSON.stringify({ stableComments: [], trainingReviews: [] }), {
        headers: { "content-type": "application/json" },
        status: 200,
      }),
    )
    .mockResolvedValueOnce(new Response("error", { status: 500 }));
  const payload = await getDetailSectionPayload("training", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(payload).toStrictEqual({
    sourceLabel: "JRA 中央競馬",
    stableComments: [],
    trainings: [],
    type: "training",
  });
  fetchMock.mockRestore();
});

it("training payload uses the PostgreSQL fallback directly for local development", async () => {
  getDatabaseTargetMock.mockReturnValue("local");
  getRaceDetailMock.mockResolvedValueOnce(null);
  getRaceTrainingsMock.mockResolvedValueOnce([]);

  const payload = await getDetailSectionPayload("training", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toStrictEqual({
    sourceLabel: "JRA 中央競馬",
    stableComments: [],
    trainings: [],
    type: "training",
  });
  expect(fetchRaceTrainingsFromCatalogMock).not.toHaveBeenCalled();
  expect(getRaceTrainingsMock).toHaveBeenCalledTimes(1);
});

it("skips 斤量 class stats for ばんえい condition payloads", async () => {
  getRaceDetailMock.mockResolvedValue(BAN_EI_RACE);
  getRaceRunnersMock.mockResolvedValue([]);
  getRaceTimeStatsMock.mockResolvedValue({
    averageKohan3f: null,
    averageRaceTime: null,
    correlationRows: [],
    fastestDetail: null,
    fastestKohan3f: null,
    fastestRaceTime: null,
    medianKohan3f: null,
    medianRaceTime: null,
    raceCount: 1,
    targetRaces: [],
  });
  getFinishPositionStatsMock.mockResolvedValue([{ count: 1 }]);
  getFrameStatsMock.mockResolvedValue([{ count: 1 }]);
  getPayoutStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([{ key: "le49" }]);

  const payload = await getDetailSectionPayload("condition", {
    day: "30",
    keibajoCode: "83",
    month: "05",
    query: { statsVenue: "1" },
    raceNumber: "11",
    raceSource: "nar",
    year: "2026",
  });

  expect(payload).toMatchObject({
    carriedWeightClassStats: [],
    type: "condition",
  });
  expect(getCarriedWeightClassStatsMock).not.toHaveBeenCalled();
  expect(getWeightClassStatsMock).toHaveBeenCalledTimes(1);
});

it("loads 斤量 class stats for non-ばんえい condition payloads", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([]);
  getRaceTimeStatsMock.mockResolvedValue({
    averageKohan3f: null,
    averageRaceTime: null,
    correlationRows: [],
    fastestDetail: null,
    fastestKohan3f: null,
    fastestRaceTime: null,
    medianKohan3f: null,
    medianRaceTime: null,
    raceCount: 1,
    targetRaces: [],
  });
  getFinishPositionStatsMock.mockResolvedValue([{ count: 1 }]);
  getFrameStatsMock.mockResolvedValue([{ count: 1 }]);
  getPayoutStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([{ key: "55.5-57" }]);

  const payload = await getDetailSectionPayload("condition", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: { statsVenue: "1" },
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    carriedWeightClassStats: [{ key: "55.5-57" }],
    type: "condition",
  });
  expect(getCarriedWeightClassStatsMock).toHaveBeenCalledTimes(1);
});

it("returns null heatmap payload when time-score data is missing", async () => {
  getRaceDetailMock.mockResolvedValue(null);
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(payload).toBeNull();
});

it("assembles heatmap payload from time-score, results, and condition sections", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([]);
  getTimeScoreRowsMock.mockResolvedValue([]);
  getRaceTimeStatsMock.mockResolvedValue({
    averageKohan3f: null,
    averageRaceTime: null,
    correlationRows: [],
    fastestDetail: null,
    fastestKohan3f: null,
    fastestRaceTime: null,
    medianKohan3f: null,
    medianRaceTime: null,
    raceCount: 1,
    targetRaces: [],
  });
  getSimilarRaceStatsMock.mockResolvedValue([]);
  getBloodlineStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([{ umaban: "01" }]);
  getFinishPositionStatsMock.mockResolvedValue([{ count: 1 }]);
  getFrameStatsMock.mockResolvedValue([{ count: 1, frameNumber: "1" }]);
  getPayoutStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([{ key: "480-499" }]);
  getCarriedWeightClassStatsMock.mockResolvedValue([{ key: "55.5-57" }]);

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: { statsVenue: "1" },
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    carriedWeightClassStats: [{ key: "55.5-57" }],
    frameStats: [{ count: 1, frameNumber: "1" }],
    horseResults: [{ umaban: "01" }],
    type: "win-rate-heatmap",
    weightClassStats: [{ key: "480-499" }],
  });
});
