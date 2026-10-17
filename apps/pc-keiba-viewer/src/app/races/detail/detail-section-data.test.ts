vi.mock("../../../lib/arc-official-card.server", () => ({
  loadOfficialCard: vi.fn<() => Promise<null>>().mockResolvedValue(null),
}));
// Run with bun (bunx vitest)

import { beforeEach, expect, it, vi } from "vitest";

import type { DetailSectionParams } from "./detail-section-data";

vi.mock("server-only", () => ({}));
const { heatmapRuntimeMock } = vi.hoisted(() => ({
  heatmapRuntimeMock: vi.fn<() => Promise<unknown>>(),
}));
vi.mock("../../../lib/cloudflare-context.server", () => ({
  safeGetCloudflareRuntime: heatmapRuntimeMock,
}));
import type {
  FinishPositionBucketFilter,
  FinishPositionBucketMetrics,
} from "../../../lib/finish-prediction-dimensions";
import type { RaceDetail, Runner, SimilarRaceStatsSettings } from "../../../lib/race-types";
import type {
  RunningStyleBucketFilter,
  RunningStyleBucketMetrics,
} from "../../../lib/running-style-prediction-dimensions";
import { isWinRateHeatmapSectionPayload } from "../../../lib/win-rate-heatmap-cache";
import {
  putWinRateHeatmapCache,
  getCachedWinRateHeatmapPayload,
} from "../../../lib/win-rate-heatmap-cache.server";

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

type GetBloodlineStatsFn = (
  race: RaceDetail,
  settings: SimilarRaceStatsSettings,
) => Promise<unknown[]>;

type GetRunningStyleBucketEvaluationFn = (args: {
  filter: RunningStyleBucketFilter;
}) => Promise<RunningStyleBucketMetrics | null>;

type GetFinishPositionBucketEvaluationFn = (args: {
  filter: FinishPositionBucketFilter;
}) => Promise<FinishPositionBucketMetrics | null>;

const {
  getActiveFinishPredictionEvaluationMock,
  getActiveFinishPositionPredictionsMock,
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
  fetchConditionHistoryStatsFromCatalogMock,
  fetchHorseRaceResultsFromCatalogMock,
  fetchRaceTrainingsFromCatalogMock,
  fetchHeatmapPartnershipRowsMock,
  fetchHeatmapLiveWeightsMock,
  fetchWinRateHeatmapStatsFromCatalogMock,
  getCachedDetailSectionResponseMock,
  getDatabaseTargetMock,
  getWeightClassStatsMock,
} = vi.hoisted(() => ({
  fetchConditionHistoryStatsFromCatalogMock: vi.fn<() => Promise<unknown>>(),
  fetchHorseRaceResultsFromCatalogMock: vi.fn<() => Promise<unknown[] | null>>(),
  fetchRaceTrainingsFromCatalogMock: vi.fn<() => Promise<unknown[] | null>>(),
  fetchHeatmapPartnershipRowsMock: vi.fn<() => Promise<unknown[] | null>>(),
  fetchHeatmapLiveWeightsMock: vi.fn<() => Promise<unknown[]>>(),
  fetchWinRateHeatmapStatsFromCatalogMock:
    vi.fn<(query: Record<string, unknown>) => Promise<unknown>>(),
  getCachedDetailSectionResponseMock: vi.fn<(key: string) => Promise<Response | null>>(),
  getActiveFinishPredictionEvaluationMock: vi.fn<() => Promise<unknown>>(),
  getActiveFinishPositionPredictionsMock: vi.fn<() => Promise<unknown[]>>(),
  getDatabaseTargetMock: vi.fn<() => "cloudflare" | "local" | "neon">(),
  getBloodlineStatsMock: vi.fn<GetBloodlineStatsFn>(),
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
  getActiveFinishPositionPredictions: getActiveFinishPositionPredictionsMock,
  getActiveFinishPredictionEvaluation: getActiveFinishPredictionEvaluationMock,
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

vi.mock("../../../lib/condition-history-catalog.server", () => ({
  fetchConditionHistoryStatsFromCatalog: fetchConditionHistoryStatsFromCatalogMock,
}));

vi.mock("../../../lib/horse-race-results-catalog.server", () => ({
  fetchHorseRaceResultsFromCatalog: fetchHorseRaceResultsFromCatalogMock,
}));

vi.mock("../../../lib/race-training-catalog.server", () => ({
  fetchRaceTrainingsFromCatalog: fetchRaceTrainingsFromCatalogMock,
}));

vi.mock("../../../lib/win-rate-heatmap-partnership.server", () => ({
  fetchHeatmapPartnershipRows: fetchHeatmapPartnershipRowsMock,
}));
vi.mock("../../../lib/win-rate-heatmap-live-weights.server", () => ({
  fetchHeatmapLiveWeights: fetchHeatmapLiveWeightsMock,
}));
vi.mock("../../../lib/win-rate-heatmap-catalog.server", () => ({
  fetchWinRateHeatmapStatsFromCatalog: fetchWinRateHeatmapStatsFromCatalogMock,
  groupCatalogBloodlineRows: (rows: unknown) => rows,
  groupCatalogSimilarRows: (rows: unknown) => rows,
}));

vi.mock("../../../lib/race-detail-section-cache.server", () => ({
  getCachedDetailSectionResponse: getCachedDetailSectionResponseMock,
}));

vi.mock("../../../lib/premium-data-top-cache.server", () => ({
  getPremiumDataTopHorsesWithCache: vi.fn<() => Promise<unknown[]>>(),
}));

vi.mock("../../../lib/running-style-cache.server", () => ({
  getRaceRunningStylesWithCache: vi.fn<() => Promise<unknown[]>>().mockResolvedValue([]),
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
  gradeCode: "A",
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

const EMPTY_CATALOG_CONDITION = {
  carriedWeightClassStats: [],
  finishPositionStats: [],
  frameStats: [],
  raceTimeStats: {
    averageKohan3f: null,
    averageRaceTime: null,
    correlationRows: [],
    fastestDetail: null,
    fastestKohan3f: null,
    fastestRaceTime: null,
    medianKohan3f: null,
    medianRaceTime: null,
    raceCount: 0,
    targetRaces: [],
  },
  weightClassStats: [],
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
  fetchConditionHistoryStatsFromCatalogMock.mockReset();
  // An incomplete Catalog answer (no race times): the condition section still
  // uses its database path, while heatmaps read these Catalog counts only.
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue(EMPTY_CATALOG_CONDITION);
  fetchHorseRaceResultsFromCatalogMock.mockReset();
  fetchHorseRaceResultsFromCatalogMock.mockResolvedValue(null);
  fetchRaceTrainingsFromCatalogMock.mockReset();
  fetchRaceTrainingsFromCatalogMock.mockResolvedValue(null);
  fetchHeatmapPartnershipRowsMock.mockReset();
  fetchHeatmapPartnershipRowsMock.mockResolvedValue([]);
  fetchHeatmapLiveWeightsMock.mockReset();
  fetchHeatmapLiveWeightsMock.mockResolvedValue([]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockReset();
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue(null);
  getCachedDetailSectionResponseMock.mockReset();
  getCachedDetailSectionResponseMock.mockResolvedValue(null);
  getActiveFinishPositionPredictionsMock.mockReset();
  getActiveFinishPositionPredictionsMock.mockResolvedValue([]);
  getDatabaseTargetMock.mockReset();
  getDatabaseTargetMock.mockReturnValue("cloudflare");
  getBloodlineStatsMock.mockReset();
  getBloodlineStatsMock.mockResolvedValue([]);
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

it("does not fetch or expose domestic evaluation metrics for an overseas race", async () => {
  getRaceDetailMock.mockResolvedValue({ ...JRA_RACE, keibajoCode: "A8" });
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getActiveFinishPositionPredictionsMock.mockResolvedValue([]);
  const payload = await getDetailSectionPayload("finish-prediction", {
    day: "24",
    keibajoCode: "A8",
    month: "08",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2026",
  });
  expect(payload).toMatchObject({
    type: "finish-prediction",
    evaluation: null,
    bucket: {
      bucketEvaluation: null,
      bucketGradeCode: null,
      bucketModelVersion: null,
      bucketRace: null,
      bucketScope: null,
      bucketSource: null,
    },
  });
  expect(getActiveFinishPredictionEvaluationMock).not.toHaveBeenCalled();
  expect(getFinishPositionBucketEvaluationMock).not.toHaveBeenCalled();
});

it("threads the expected generation into finish-position prediction loading", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getActiveFinishPositionPredictionsMock.mockRejectedValue(new Error("generation unavailable"));
  await expect(
    getDetailSectionPayload("finish-prediction", {
      day: "24",
      expectedPredictionGeneratedAt: "2026-08-24T03:40:15.000Z",
      keibajoCode: "06",
      month: "08",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2026",
    }),
  ).rejects.toThrow("generation unavailable");
  expect(getActiveFinishPositionPredictionsMock).toHaveBeenCalledWith(
    JRA_RACE,
    [OVERSEAS_RUNNER],
    "2026-08-24T03:40:15.000Z",
  );
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

it("keeps cell-matching grade off for JRA maidens with a space grade code", async () => {
  getRaceDetailMock.mockResolvedValueOnce({
    ...JRA_RACE,
    gradeCode: " ",
    kyosoJokenCode: "703",
    kyosoJokenMeisho: "未勝利",
    kyosoShubetsuCode: "11",
    kyosomeiHondai: "",
  });
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);

  const context = await getDetailStatsContext({
    day: "23",
    keibajoCode: "07",
    month: "08",
    query: {},
    raceNumber: "01",
    raceSource: "jra",
    year: "2026",
  });

  expect(context?.conditionAnalysisSettings).toMatchObject({
    cellMatching: true,
    includeGrade: false,
    includeClass: true,
    includeAge: true,
  });
});

it("keeps cell-matching grade off for JRA 特別 races", async () => {
  getRaceDetailMock.mockResolvedValueOnce({
    ...JRA_RACE,
    gradeCode: "E",
    kyosoJokenCode: "005",
    kyosoJokenMeisho: "1勝クラス",
    kyosomeiHondai: "豊川特別",
  });
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);

  const context = await getDetailStatsContext({
    day: "23",
    keibajoCode: "07",
    month: "08",
    query: {},
    raceNumber: "08",
    raceSource: "jra",
    year: "2026",
  });

  expect(context?.conditionAnalysisSettings.includeGrade).toBe(false);
});

it("enables cell-matching grade for JRA G3 condition analysis", async () => {
  getRaceDetailMock.mockResolvedValueOnce({
    ...JRA_RACE,
    gradeCode: "C",
    kyosomeiHondai: "札幌記念",
  });
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);

  const context = await getDetailStatsContext({
    day: "23",
    keibajoCode: "01",
    month: "08",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2026",
  });

  expect(context?.conditionAnalysisSettings).toMatchObject({
    cellMatching: true,
    includeClass: false,
    includeDistance: false,
    includeGrade: false,
    includeRaceTitle: true,
    includeTrackCode: false,
    includeVenue: true,
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
  expect(context?.conditionAnalysisSettings.includeClass).toBe(false);
  expect(context?.conditionAnalysisSettings.includeRaceTitle).toBe(true);
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

it("similar payload uses published-population scope for overseas races and suppresses samples below 20 starts", async () => {
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

it("retains partial published overseas statistics in time-score with provenance and incomplete status", async () => {
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
    raceCount: 0,
    targetRaces: [],
  });
  getRaceDetailMock.mockResolvedValueOnce({ ...JRA_RACE, keibajoCode: "A4" });
  getRaceRunnersMock.mockResolvedValueOnce([
    { ...OVERSEAS_RUNNER, umaban: "01" },
    { ...OVERSEAS_RUNNER, umaban: "02" },
    { ...OVERSEAS_RUNNER, umaban: "04" },
    { ...OVERSEAS_RUNNER, umaban: "07" },
  ]);
  getSimilarRaceStatsMock.mockResolvedValueOnce([
    {
      category: "jockey",
      currentHorseNumbers: "7",
      details: [],
      horseCount: 10,
      name: "Published Jockey",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 20,
      winCount: 2,
      winRate: 10,
      statsSource: "netkeiba",
      statsScope: "all-published-results",
    },
    {
      category: "trainer",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 10,
      name: "Thin Trainer",
      quinellaCount: 3,
      quinellaRate: 15,
      showCount: 4,
      showRate: 20,
      starts: 19,
      winCount: 2,
      winRate: 10,
    },
  ]);
  getBloodlineStatsMock.mockResolvedValue([]);

  const payload = await getDetailSectionPayload("time-score", {
    day: "19",
    keibajoCode: "A4",
    month: "09",
    query: {},
    raceNumber: "05",
    raceSource: "jra",
    year: "2026",
  });

  expect(payload).toMatchObject({
    similarRows: [
      {
        name: "Published Jockey",
        starts: 20,
        currentHorseNumbers: "7",
        statsSource: "netkeiba",
        statsScope: "all-published-results",
      },
    ],
    settings: { includeDistance: false, includeVenue: false },
    similarStatsIncomplete: true,
    similarStatsFallback: true,
    type: "time-score",
  });
  expect(getSimilarRaceStatsMock).toHaveBeenCalledOnce();
});

it("preserves explicitly requested overseas distance filtering", async () => {
  getRaceDetailMock.mockResolvedValueOnce({ ...JRA_RACE, keibajoCode: "A8" });
  getRaceRunnersMock.mockResolvedValueOnce([OVERSEAS_RUNNER]);

  const context = await getDetailStatsContext({
    day: "28",
    keibajoCode: "A8",
    month: "12",
    query: { similarStatsDistance: "1" },
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(context?.statsSettings.includeDistance).toBe(true);
});

it("keeps the first similar bloodline rows when the bloodline fallback times out", async () => {
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
    await expect(payloadPromise).resolves.toMatchObject({ type: "similar" });

    expect(getBloodlineStatsMock.mock.calls.length).toBeGreaterThan(2);
  } finally {
    vi.useRealTimers();
  }
});

it("keeps the first similar rows when the similar fallback times out", async () => {
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
    await expect(payloadPromise).resolves.toMatchObject({
      similarStatsIncomplete: true,
      type: "similar",
    });
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

it("keeps the first time-score person rows when the similar fallback times out", async () => {
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
    await expect(payloadPromise).resolves.toMatchObject({
      similarStatsIncomplete: true,
      type: "time-score",
    });
  } finally {
    vi.useRealTimers();
  }
});

it("keeps the first bloodline rows when the standalone bloodline fallback times out", async () => {
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
    await expect(payloadPromise).resolves.toMatchObject({
      bloodlineStatsIncomplete: true,
      type: "bloodline",
    });
  } finally {
    vi.useRealTimers();
  }
});

it("keeps the first time-score bloodline rows when the bloodline fallback times out", async () => {
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
    await expect(payloadPromise).resolves.toMatchObject({
      bloodlineStatsIncomplete: true,
      type: "time-score",
    });
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
        raceName: true,
        track: true,
      },
      gradeCode: null,
      keibajoCode: "06",
      kyori: 2500,
      kyosoJokenCode: "999",
      kyosoShubetsuCode: "11",
      period: "all",
      raceName: "有馬記念",
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
        raceName: true,
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
      raceName: true,
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
    raceName: true,
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
    bucketGradeCode: "A",
    bucketRace: {
      gradeCode: "A",
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
        raceName: true,
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
      raceName: true,
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
    raceName: true,
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
      raceName: true,
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

it("training payload decodes netkeiba oikiri.html as utf-8 and merges parsed reviews into Catalog trainings", async () => {
  getRaceDetailMock.mockResolvedValueOnce(JRA_RACE);
  fetchRaceTrainingsFromCatalogMock.mockResolvedValueOnce([
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
  expect(getRaceTrainingsMock).not.toHaveBeenCalled();
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
  getRaceTrainingsMock.mockRejectedValueOnce(new Error("Neon must not be contacted."));
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
  expect(getRaceTrainingsMock).not.toHaveBeenCalled();
  fetchMock.mockRestore();
});

it("training payload fails closed when Catalog is unavailable instead of reading Neon", async () => {
  getRaceDetailMock.mockResolvedValueOnce(null);
  fetchRaceTrainingsFromCatalogMock.mockResolvedValueOnce(null);
  getRaceTrainingsMock.mockRejectedValueOnce(new Error("Neon must not be contacted."));

  await expect(
    getDetailSectionPayload("training", {
      day: "28",
      keibajoCode: "06",
      month: "12",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2025",
    }),
  ).rejects.toThrow("R2 Catalog race trainings are unavailable.");
  expect(getRaceTrainingsMock).not.toHaveBeenCalled();
});

it("training payload propagates Catalog errors without reading Neon", async () => {
  getRaceDetailMock.mockResolvedValueOnce(null);
  fetchRaceTrainingsFromCatalogMock.mockRejectedValueOnce(new Error("Catalog request failed."));

  await expect(
    getDetailSectionPayload("training", {
      day: "28",
      keibajoCode: "06",
      month: "12",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2025",
    }),
  ).rejects.toThrow("Catalog request failed.");
  expect(getRaceTrainingsMock).not.toHaveBeenCalled();
});

it("training payload uses PostgreSQL directly for local development", async () => {
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

it("falls back to live results when cached heatmap source JSON is invalid", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getCachedDetailSectionResponseMock
    .mockResolvedValueOnce(new Response("not-json"))
    .mockResolvedValueOnce(null);
  getHorseRaceResultsMock.mockResolvedValue([{ umaban: "03" }]);
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
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getPayoutStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(payload).toMatchObject({
    horseResults: [{ umaban: "03" }],
    partnershipRows: [],
    liveHorseWeights: [],
    type: "win-rate-heatmap",
    producerDataRevision: 1,
  });
  expect(getHorseRaceResultsMock).toHaveBeenCalledTimes(1);
});

it("requests same-condition owner counts on both initial and relaxed heatmap queries", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  await getDetailSectionPayload("win-rate-heatmap", {
    day: "19",
    keibajoCode: "06",
    month: "09",
    query: {},
    raceNumber: "01",
    raceSource: "jra",
    year: "2026",
  });
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledTimes(2);
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenNthCalledWith(
    1,
    expect.objectContaining({ includeOwner: true, includeJockeyFrame: true }),
  );
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenNthCalledWith(
    2,
    expect.objectContaining({ includeOwner: true, includeJockeyFrame: true }),
  );
});

it("fills a named JRA heatmap's zero-win sire rows from title-free venue history", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, sireName: "クロフネ" }]);
  fetchWinRateHeatmapStatsFromCatalogMock
    .mockResolvedValueOnce({
      bloodlineRows: [
        {
          category: "sire",
          currentHorseNumbers: "1",
          details: [],
          horseCount: 0,
          name: "クロフネ",
          quinellaCount: 1,
          quinellaRate: 10,
          showCount: 1,
          showRate: 10,
          starts: 10,
          winCount: 0,
          winRate: 0,
        },
      ],
      similarRows: [{ category: "jockey", starts: 10, winCount: 1 }],
    })
    .mockResolvedValueOnce({
      bloodlineRows: [
        {
          category: "sire",
          currentHorseNumbers: "1",
          details: [],
          horseCount: 0,
          name: "クロフネ",
          quinellaCount: 9,
          quinellaRate: 9,
          showCount: 20,
          showRate: 20,
          starts: 100,
          winCount: 8,
          winRate: 8,
        },
      ],
      similarRows: [],
    });
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "27",
    keibajoCode: "06",
    month: "09",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2026",
  });
  expect(payload).toMatchObject({
    bloodlineRows: [{ category: "sire", starts: 100, winCount: 8 }],
    similarRows: [{ category: "jockey", starts: 10, winCount: 1 }],
  });
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledTimes(2);
  expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0]).toMatchObject({
    includeRaceTitle: true,
    includeVenue: true,
  });
  expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[1]?.[0]).toMatchObject({
    includeRaceTitle: false,
    includeVenue: true,
  });
});

it("preserves a genuine zero-win bloodline when the broader sample also has no wins", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, sireName: "父" }]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockImplementation((query) =>
    Promise.resolve({
      bloodlineRows: [
        {
          category: "sire",
          currentHorseNumbers: "1",
          details: [],
          horseCount: 0,
          name: "父",
          quinellaCount: 0,
          quinellaRate: 0,
          showCount: 0,
          showRate: 0,
          starts: query.includeRaceTitle === false ? 100 : 10,
          winCount: 0,
          winRate: 0,
        },
      ],
      similarRows: [{ category: "jockey", starts: 10, winCount: 0 }],
    }),
  );
  getCachedDetailSectionResponseMock.mockImplementation(async (key: string) =>
    key.includes("bloodline")
      ? Response.json({
          type: "bloodline",
          rows: [{ category: "sire", starts: 100, winCount: 0 }],
        })
      : null,
  );
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "27",
    keibajoCode: "06",
    month: "09",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2026",
  });
  expect(payload).toMatchObject({ bloodlineRows: [{ category: "sire", starts: 10, winCount: 0 }] });
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledTimes(2);
});

it("reuses the warmed bloodline section when heatmap cell filters find no rows", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, sireName: "父" }]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getCachedDetailSectionResponseMock.mockImplementation(async (key: string) =>
    key.includes("bloodline")
      ? Response.json({
          type: "bloodline",
          rows: [
            {
              category: "sire",
              currentHorseNumbers: "1",
              details: [],
              horseCount: 1,
              name: "父",
              quinellaCount: 1,
              quinellaRate: 10,
              showCount: 1,
              showRate: 10,
              starts: 10,
              winCount: 1,
              winRate: 10,
            },
          ],
        })
      : null,
  );
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "27",
    keibajoCode: "09",
    month: "09",
    query: {},
    raceNumber: "01",
    raceSource: "jra",
    year: "2026",
  });
  expect(payload).toMatchObject({ bloodlineRows: [{ category: "sire", name: "父", starts: 10 }] });
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledOnce();
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
});

it("reuses a broader cached sire sample when the narrow NAR sample has no wins", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue({ ...JRA_RACE, source: "nar" });
  getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, sireName: null }]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [
      {
        category: "sire",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 1,
        name: "父",
        quinellaCount: 0,
        quinellaRate: 0,
        showCount: 0,
        showRate: 0,
        starts: 16,
        winCount: 0,
        winRate: 0,
      },
    ],
    similarRows: [{ category: "jockey", starts: 20, winCount: 2 }],
  });
  getCachedDetailSectionResponseMock.mockImplementation(async (key: string) =>
    key.includes("bloodline")
      ? Response.json({
          type: "bloodline",
          rows: [
            {
              category: "sire",
              currentHorseNumbers: "1",
              details: [],
              horseCount: 1,
              name: "父",
              quinellaCount: 2,
              quinellaRate: 20,
              showCount: 3,
              showRate: 30,
              starts: 100,
              winCount: 8,
              winRate: 8,
            },
          ],
        })
      : null,
  );
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "27",
    keibajoCode: "55",
    month: "09",
    query: {},
    raceNumber: "02",
    raceSource: "nar",
    year: "2026",
  });
  expect(payload).toMatchObject({
    bloodlineRows: [{ category: "sire", starts: 100, winCount: 8 }],
    similarRows: [{ category: "jockey", starts: 20, winCount: 2 }],
  });
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledOnce();
});

it("rejects missing initial Catalog even when cached bloodline rows exist", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, sireName: "父" }]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue(null);
  getCachedDetailSectionResponseMock.mockImplementation(async (key: string) =>
    key.includes("bloodline")
      ? Response.json({
          type: "bloodline",
          rows: [
            {
              category: "sire",
              currentHorseNumbers: "1",
              details: [],
              horseCount: 1,
              name: "父",
              quinellaCount: 2,
              quinellaRate: 20,
              showCount: 3,
              showRate: 30,
              starts: 100,
              winCount: 8,
              winRate: 8,
            },
          ],
        })
      : null,
  );
  const work = getDetailSectionPayload("win-rate-heatmap", {
    day: "27",
    keibajoCode: "09",
    month: "09",
    query: {},
    raceNumber: "06",
    raceSource: "jra",
    year: "2026",
  });
  await expect(work).rejects.toThrow("Heatmap catalog is unavailable");
});

it("fills empty heatmap bloodline rows from the bloodline catalog for known-pedigree runners", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, sireName: "ディープインパクト" }]);
  fetchWinRateHeatmapStatsFromCatalogMock
    .mockResolvedValueOnce({
      bloodlineRows: [],
      similarRows: [
        {
          category: "jockey",
          currentHorseNumbers: "1",
          details: [],
          horseCount: 0,
          name: "武豊",
          quinellaCount: 3,
          quinellaRate: 30,
          showCount: 5,
          showRate: 50,
          starts: 10,
          winCount: 1,
          winRate: 10,
        },
      ],
    })
    .mockResolvedValueOnce({
      bloodlineRows: [
        {
          category: "sire",
          currentHorseNumbers: "1",
          details: [],
          horseCount: 1,
          name: "ディープインパクト",
          quinellaCount: 4,
          quinellaRate: 20,
          showCount: 6,
          showRate: 30,
          starts: 20,
          winCount: 2,
          winRate: 10,
        },
      ],
      similarRows: [],
    });
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "27",
    keibajoCode: "06",
    month: "09",
    query: {},
    raceNumber: "01",
    raceSource: "jra",
    year: "2026",
  });
  expect(payload).toMatchObject({
    bloodlineRows: [{ category: "sire", name: "ディープインパクト", starts: 20 }],
    similarRows: [{ category: "jockey", name: "武豊", starts: 10 }],
    type: "win-rate-heatmap",
    producerDataRevision: 1,
  });
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledTimes(2);
  expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[1]?.[0]).not.toHaveProperty(
    "includeOwner",
  );
  expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[1]?.[0]).not.toHaveProperty(
    "includeJockeyFrame",
  );
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
});

it("keeps names-only heatmap rows when the bloodline catalog has no stats", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, sireName: "父" }]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "27",
    keibajoCode: "09",
    month: "09",
    query: {},
    raceNumber: "01",
    raceSource: "jra",
    year: "2026",
  });
  expect(payload).toMatchObject({ bloodlineRows: [], runners: [{ sireName: "父" }] });
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledTimes(3);
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
});

it("recovers known JRA pedigrees when the ten-year catalog window returns empty", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, sireName: "父" }]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockImplementation((query) =>
    Promise.resolve({
      bloodlineRows:
        query.years === 9
          ? [
              {
                category: "sire",
                currentHorseNumbers: "1",
                details: [],
                horseCount: 1,
                name: "父",
                quinellaCount: 1,
                quinellaRate: 10,
                showCount: 1,
                showRate: 10,
                starts: 10,
                winCount: 1,
                winRate: 10,
              },
            ]
          : [],
      similarRows: [],
    }),
  );
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "27",
    keibajoCode: "09",
    month: "09",
    query: {},
    raceNumber: "01",
    raceSource: "jra",
    year: "2026",
  });
  expect(payload).toMatchObject({ bloodlineRows: [{ name: "父", starts: 10 }] });
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledTimes(3);
  expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[2]?.[0]).toMatchObject({
    includeAge: false,
    includeClass: false,
    includeDistance: false,
    includeVenue: true,
    years: 9,
  });
});

it("keeps names-only heatmap rows when the optional bloodline catalog fails", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, damSireName: "母父" }]);
  fetchWinRateHeatmapStatsFromCatalogMock
    .mockResolvedValueOnce({ bloodlineRows: [], similarRows: [] })
    .mockRejectedValueOnce(new Error("R2 SQL unavailable"));
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "27",
    keibajoCode: "09",
    month: "09",
    query: {},
    raceNumber: "01",
    raceSource: "jra",
    year: "2026",
  });
  expect(payload).toMatchObject({ bloodlineRows: [], runners: [{ damSireName: "母父" }] });
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledTimes(3);
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
});

it("rejects missing initial Catalog instead of publishing shorter-window bloodline alone", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, sireName: "父" }]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockImplementation((query) =>
    Promise.resolve(
      query.years === 9
        ? {
            bloodlineRows: [
              {
                category: "sire",
                currentHorseNumbers: "1",
                details: [],
                horseCount: 1,
                name: "父",
                quinellaCount: 1,
                quinellaRate: 10,
                showCount: 1,
                showRate: 10,
                starts: 10,
                winCount: 1,
                winRate: 10,
              },
            ],
            similarRows: [],
          }
        : null,
    ),
  );
  const work = getDetailSectionPayload("win-rate-heatmap", {
    day: "27",
    keibajoCode: "09",
    month: "09",
    query: {},
    raceNumber: "01",
    raceSource: "jra",
    year: "2026",
  });
  await expect(work).rejects.toThrow("Heatmap catalog is unavailable");
});

it("fills heatmap bloodline rows from the database when the catalog flags them unavailable", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    bloodlineUnavailable: true,
    similarRows: [
      {
        category: "jockey",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "武豊",
        quinellaCount: 3,
        quinellaRate: 30,
        showCount: 5,
        showRate: 50,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
    ],
  });
  getBloodlineStatsMock.mockResolvedValue([
    {
      category: "sire",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 1,
      name: "ディープインパクト",
      quinellaCount: 4,
      quinellaRate: 20,
      showCount: 6,
      showRate: 30,
      starts: 20,
      winCount: 2,
      winRate: 10,
    },
  ]);
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "23",
    keibajoCode: "06",
    month: "09",
    query: {},
    raceNumber: "05",
    raceSource: "jra",
    year: "2026",
  });
  expect(payload).toMatchObject({
    bloodlineRows: [{ category: "sire", name: "ディープインパクト", starts: 20 }],
    similarRows: [{ category: "jockey", name: "武豊", starts: 10 }],
    type: "win-rate-heatmap",
    producerDataRevision: 1,
  });
  expect(getBloodlineStatsMock).toHaveBeenCalledOnce();
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledOnce();
});

it("skips the relaxed heatmap catalog query after the catalog flags bloodline unavailable", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    bloodlineUnavailable: true,
    similarRows: [],
  });
  getBloodlineStatsMock.mockResolvedValue([]);
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "23",
    keibajoCode: "06",
    month: "09",
    query: {},
    raceNumber: "05",
    raceSource: "jra",
    year: "2026",
  });
  expect(payload).toMatchObject({ bloodlineRows: [], similarRows: [], type: "win-rate-heatmap" });
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledOnce();
  expect(getBloodlineStatsMock).toHaveBeenCalledOnce();
});

it("captures live weights in the warm payload without altering runner identities", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchHeatmapLiveWeightsMock.mockResolvedValue([{ horseNumber: "01", weight: 488 }]);
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });
  expect(payload).toMatchObject({
    liveHorseWeights: [{ horseNumber: "01", weight: 488 }],
    runners: [OVERSEAS_RUNNER],
  });
  expect(fetchHeatmapLiveWeightsMock).toHaveBeenCalledWith({
    day: "28",
    keibajoCode: "06",
    month: "12",
    raceNumber: "11",
    source: "jra",
    year: "2025",
  });
});

it("rejects partnership 502 instead of publishing empty partnership rows", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchHeatmapPartnershipRowsMock.mockRejectedValue(
    new Error("Partnership catalog request failed: 502"),
  );
  const work = getDetailSectionPayload("win-rate-heatmap", {
    day: "26",
    keibajoCode: "54",
    month: "09",
    query: {},
    raceNumber: "01",
    raceSource: "nar",
    year: "2026",
  });
  await expect(work).rejects.toThrow("Partnership catalog request failed: 502");
});

it("does not conceal a malformed partnership catalog response", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchHeatmapPartnershipRowsMock.mockRejectedValue(new Error("Partnership payload malformed"));
  await expect(
    getDetailSectionPayload("win-rate-heatmap", {
      day: "26",
      keibajoCode: "54",
      month: "09",
      query: {},
      raceNumber: "01",
      raceSource: "nar",
      year: "2026",
    }),
  ).rejects.toThrow("Partnership payload malformed");
});

it("refuses to publish a heatmap when the partnership catalog binding is unavailable", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchHeatmapPartnershipRowsMock.mockResolvedValue(null);
  await expect(
    getDetailSectionPayload("win-rate-heatmap", {
      day: "28",
      keibajoCode: "06",
      month: "12",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2025",
    }),
  ).rejects.toThrow("Partnership catalog binding is unavailable");
});

it("reuses cached results and condition payloads for heatmap assembly", async () => {
  // Local databases keep the full condition section for heatmaps.
  getDatabaseTargetMock.mockReturnValue("local");
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  fetchHeatmapPartnershipRowsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getCachedDetailSectionResponseMock
    .mockResolvedValueOnce(Response.json({ results: [{ umaban: "01" }], type: "results" }))
    .mockResolvedValueOnce(
      Response.json({
        carriedWeightClassStats: [{ key: "55.5-57" }],
        frameStats: [{ count: 1, frameNumber: "1" }],
        type: "condition",
        weightClassStats: [{ key: "480-499" }],
      }),
    );

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    carriedWeightClassStats: [{ key: "55.5-57" }],

    horseResults: [{ umaban: "01" }],
    partnershipRows: [],
    liveHorseWeights: [],
    type: "win-rate-heatmap",
    producerDataRevision: 1,
    weightClassStats: [{ key: "480-499" }],
  });
  expect(getHorseRaceResultsMock).not.toHaveBeenCalled();
  expect(getFrameStatsMock).not.toHaveBeenCalled();
});

it("keeps frameStats from condition payloads that omit weight class arrays", async () => {
  // Local databases keep the full condition section for heatmaps.
  getDatabaseTargetMock.mockReturnValue("local");
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getCachedDetailSectionResponseMock
    .mockResolvedValueOnce(Response.json({ results: [{ umaban: "01" }], type: "results" }))
    .mockResolvedValueOnce(
      Response.json({
        frameStats: [{ count: 8, frameNumber: "1", winRate: 12.5 }],
        type: "condition",
      }),
    );

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "22",
    keibajoCode: "55",
    month: "08",
    query: {},
    raceNumber: "10",
    raceSource: "nar",
    year: "2026",
  });

  expect(payload).toStrictEqual({
    bloodlineRows: [],
    carriedWeightClassStats: [],
    frameStats: [{ count: 8, frameNumber: "1", winRate: 12.5 }],
    horseResults: [{ umaban: "01" }],
    runners: [OVERSEAS_RUNNER],
    similarRows: [],
    partnershipRows: [],
    liveHorseWeights: [],
    type: "win-rate-heatmap",
    producerDataRevision: 1,
    weightChangeClassStats: [],
    weightClassStats: [],
  });
  expect(getFrameStatsMock).not.toHaveBeenCalled();
});

it("reuses cached condition payloads whose frameStats array is empty", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getCachedDetailSectionResponseMock
    .mockResolvedValueOnce(Response.json({ results: [{ umaban: "01" }], type: "results" }))
    .mockResolvedValueOnce(
      Response.json({
        frameStats: [],
        type: "condition",
      }),
    );

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "22",
    keibajoCode: "55",
    month: "08",
    query: {},
    raceNumber: "10",
    raceSource: "nar",
    year: "2026",
  });

  expect(payload).toStrictEqual({
    bloodlineRows: [],
    carriedWeightClassStats: [],
    frameStats: [],
    horseResults: [{ umaban: "01" }],
    runners: [OVERSEAS_RUNNER],
    similarRows: [],
    partnershipRows: [],
    liveHorseWeights: [],
    type: "win-rate-heatmap",
    producerDataRevision: 1,
    weightChangeClassStats: [],
    weightClassStats: [],
  });
  expect(getFrameStatsMock).not.toHaveBeenCalled();
});

it("rejects malformed condition payloads that omit frameStats", async () => {
  // Local databases keep the full condition section for heatmaps.
  getDatabaseTargetMock.mockReturnValue("local");
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getCachedDetailSectionResponseMock
    .mockResolvedValueOnce(Response.json({ results: [{ umaban: "01" }], type: "results" }))
    .mockResolvedValueOnce(
      Response.json({
        carriedWeightClassStats: [{ key: "55.5-57" }],
        type: "condition",
        weightClassStats: [{ key: "480-499" }],
      }),
    );

  const work = getDetailSectionPayload("win-rate-heatmap", {
    day: "22",
    keibajoCode: "55",
    month: "08",
    query: {},
    raceNumber: "10",
    raceSource: "nar",
    year: "2026",
  });

  await expect(work).rejects.toThrow("Heatmap condition is unavailable");
});

it("keeps frameStats when carriedWeightClassStats is not an array", async () => {
  // Local databases keep the full condition section for heatmaps.
  getDatabaseTargetMock.mockReturnValue("local");
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getCachedDetailSectionResponseMock
    .mockResolvedValueOnce(Response.json({ results: [{ umaban: "01" }], type: "results" }))
    .mockResolvedValueOnce(
      Response.json({
        carriedWeightClassStats: "missing",
        frameStats: [{ count: 1, frameNumber: "1" }],
        type: "condition",
      }),
    );

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "22",
    keibajoCode: "55",
    month: "08",
    query: {},
    raceNumber: "10",
    raceSource: "nar",
    year: "2026",
  });

  expect(payload).toStrictEqual({
    bloodlineRows: [],
    carriedWeightClassStats: [],
    frameStats: [{ count: 1, frameNumber: "1" }],
    horseResults: [{ umaban: "01" }],
    runners: [OVERSEAS_RUNNER],
    similarRows: [],
    partnershipRows: [],
    liveHorseWeights: [],
    type: "win-rate-heatmap",
    producerDataRevision: 1,
    weightChangeClassStats: [],
    weightClassStats: [],
  });
  expect(getFrameStatsMock).not.toHaveBeenCalled();
});

it("keeps frameStats when weightClassStats is not an array", async () => {
  // Local databases keep the full condition section for heatmaps.
  getDatabaseTargetMock.mockReturnValue("local");
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getCachedDetailSectionResponseMock
    .mockResolvedValueOnce(Response.json({ results: [{ umaban: "01" }], type: "results" }))
    .mockResolvedValueOnce(
      Response.json({
        frameStats: [{ count: 1, frameNumber: "1" }],
        type: "condition",
        weightClassStats: "missing",
      }),
    );

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "22",
    keibajoCode: "55",
    month: "08",
    query: {},
    raceNumber: "10",
    raceSource: "nar",
    year: "2026",
  });

  expect(payload).toStrictEqual({
    bloodlineRows: [],
    carriedWeightClassStats: [],
    frameStats: [{ count: 1, frameNumber: "1" }],
    horseResults: [{ umaban: "01" }],
    runners: [OVERSEAS_RUNNER],
    similarRows: [],
    partnershipRows: [],
    liveHorseWeights: [],
    type: "win-rate-heatmap",
    producerDataRevision: 1,
    weightChangeClassStats: [],
    weightClassStats: [],
  });
  expect(getFrameStatsMock).not.toHaveBeenCalled();
});

it("defaults omitted carriedWeightClassStats and keeps present weightClassStats", async () => {
  // Local databases keep the full condition section for heatmaps.
  getDatabaseTargetMock.mockReturnValue("local");
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getCachedDetailSectionResponseMock
    .mockResolvedValueOnce(Response.json({ results: [{ umaban: "01" }], type: "results" }))
    .mockResolvedValueOnce(
      Response.json({
        frameStats: [{ count: 2, frameNumber: "3" }],
        type: "condition",
        weightClassStats: [{ key: "480-499" }],
      }),
    );

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "22",
    keibajoCode: "55",
    month: "08",
    query: {},
    raceNumber: "10",
    raceSource: "nar",
    year: "2026",
  });

  expect(payload).toStrictEqual({
    bloodlineRows: [],
    carriedWeightClassStats: [],
    frameStats: [{ count: 2, frameNumber: "3" }],
    horseResults: [{ umaban: "01" }],
    runners: [OVERSEAS_RUNNER],
    similarRows: [],
    partnershipRows: [],
    liveHorseWeights: [],
    type: "win-rate-heatmap",
    producerDataRevision: 1,
    weightChangeClassStats: [],
    weightClassStats: [{ key: "480-499" }],
  });
});

it("defaults omitted weightClassStats and keeps present carriedWeightClassStats", async () => {
  // Local databases keep the full condition section for heatmaps.
  getDatabaseTargetMock.mockReturnValue("local");
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getCachedDetailSectionResponseMock
    .mockResolvedValueOnce(Response.json({ results: [{ umaban: "01" }], type: "results" }))
    .mockResolvedValueOnce(
      Response.json({
        carriedWeightClassStats: [{ key: "55.5-57" }],
        frameStats: [{ count: 2, frameNumber: "3" }],
        type: "condition",
      }),
    );

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "22",
    keibajoCode: "55",
    month: "08",
    query: {},
    raceNumber: "10",
    raceSource: "nar",
    year: "2026",
  });

  expect(payload).toStrictEqual({
    bloodlineRows: [],
    carriedWeightClassStats: [{ key: "55.5-57" }],
    frameStats: [{ count: 2, frameNumber: "3" }],
    horseResults: [{ umaban: "01" }],
    runners: [OVERSEAS_RUNNER],
    similarRows: [],
    partnershipRows: [],
    liveHorseWeights: [],
    type: "win-rate-heatmap",
    producerDataRevision: 1,
    weightChangeClassStats: [],
    weightClassStats: [],
  });
});

it("relaxes empty cell filters so heatmap rate values remain populated", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockImplementation((query) =>
    Promise.resolve(
      query.includeVenue
        ? { bloodlineRows: [], similarRows: [] }
        : {
            bloodlineRows: [
              {
                category: "sire",
                currentHorseNumbers: "1",
                details: [],
                horseCount: 1,
                name: "Sire",
                quinellaCount: 2,
                quinellaRate: 20,
                showCount: 3,
                showRate: 30,
                starts: 10,
                winCount: 1,
                winRate: 10,
              },
            ],
            similarRows: [
              {
                category: "jockey",
                currentHorseNumbers: "1",
                details: [],
                horseCount: 1,
                name: "Jockey",
                quinellaCount: 2,
                quinellaRate: 20,
                showCount: 3,
                showRate: 30,
                starts: 10,
                winCount: 1,
                winRate: 10,
              },
            ],
          },
    ),
  );

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "23",
    keibajoCode: "04",
    month: "08",
    query: {},
    raceNumber: "01",
    raceSource: "jra",
    year: "2026",
  });

  expect(payload).toMatchObject({
    bloodlineRows: [{ name: "Sire", starts: 10 }],
    similarRows: [{ name: "Jockey", starts: 10 }],
    partnershipRows: [],
    liveHorseWeights: [],
    type: "win-rate-heatmap",
    producerDataRevision: 1,
  });
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledTimes(2);
  expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[1]?.[0]).toMatchObject({
    includeAge: false,
    includeClass: false,
    includeConditionKey: false,
    includeDistance: false,
    includeGrade: false,
    includeRaceTitle: false,
    includeTrackCode: false,
    includeVenue: false,
  });
});

it("propagates Catalog failure so the route returns retryable unavailability, not empty success", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockRejectedValue(
    new Error("R2 Catalog heatmap stats failed: 502"),
  );
  await expect(
    getDetailSectionPayload("win-rate-heatmap", {
      day: "23",
      keibajoCode: "04",
      month: "08",
      query: {},
      raceNumber: "01",
      raceSource: "jra",
      year: "2026",
    }),
  ).rejects.toThrow("R2 Catalog heatmap stats failed: 502");
});

it("rejects source failures instead of publishing runners-only heatmaps", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);

  getCachedDetailSectionResponseMock.mockRejectedValue(
    new TypeError("Failed to parse URL from [object Request]"),
  );

  const work = getDetailSectionPayload("win-rate-heatmap", {
    day: "23",
    keibajoCode: "04",
    month: "08",
    query: {},
    raceNumber: "01",
    raceSource: "jra",
    year: "2026",
  });

  await expect(work).rejects.toThrow("Failed to parse URL from [object Request]");
});

it("returns null heatmap payload when race detail is missing", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
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
  expect(fetchWinRateHeatmapStatsFromCatalogMock).not.toHaveBeenCalled();
  expect(getSimilarRaceStatsMock).not.toHaveBeenCalled();
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
});

it("assembles heatmap payload from Catalog stats, results, and condition sections", async () => {
  // Local databases keep the full condition section for heatmaps.
  getDatabaseTargetMock.mockReturnValue("local");
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
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
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [
      {
        category: "sire",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "Sire",
        quinellaCount: 1,
        quinellaRate: 10,
        showCount: 1,
        showRate: 10,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
    ],
    similarRows: [
      {
        category: "jockey",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "Jockey",
        quinellaCount: 1,
        quinellaRate: 10,
        showCount: 1,
        showRate: 10,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
    ],
  });
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
    bloodlineRows: [{ details: [], name: "Sire" }],
    carriedWeightClassStats: [{ key: "55.5-57" }],
    frameStats: [{ count: 1, frameNumber: "1" }],
    horseResults: [{ umaban: "01" }],
    runners: [OVERSEAS_RUNNER],
    similarRows: [{ details: [], name: "Jockey" }],
    partnershipRows: [],
    liveHorseWeights: [],
    type: "win-rate-heatmap",
    producerDataRevision: 1,
    weightClassStats: [{ key: "480-499" }],
  });
  expect(getSimilarRaceStatsMock).not.toHaveBeenCalled();
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledTimes(1);
  expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0]).toStrictEqual({
    day: "28",
    includeAge: false,
    includeClass: false,
    includeConditionKey: false,
    includeDistance: false,
    includeGrade: false,
    includeJockeyFrame: true,
    includeOwner: true,
    includeRaceTitle: true,
    includeSurface: false,
    includeTrackCode: false,
    includeTurn: false,
    includeVenue: true,
    keibajoCode: "06",
    month: "12",
    raceNumber: "11",
    source: "jra",
    year: "2025",
    years: 10,
  });
});

it("drops Ban-ei heatmap age and condition-key Catalog filters so jockey and sire cells fill", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(BAN_EI_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
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
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [
      {
        category: "sire",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "BanEi Sire",
        quinellaCount: 1,
        quinellaRate: 10,
        showCount: 1,
        showRate: 10,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
    ],
    similarRows: [
      {
        category: "jockey",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "BanEi Jockey",
        quinellaCount: 1,
        quinellaRate: 10,
        showCount: 1,
        showRate: 10,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
    ],
  });
  getHorseRaceResultsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([{ count: 1, frameNumber: "1" }]);
  getPayoutStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "23",
    keibajoCode: "83",
    month: "08",
    query: {},
    raceNumber: "09",
    raceSource: "nar",
    year: "2026",
  });

  expect(payload).toMatchObject({
    bloodlineRows: [{ name: "BanEi Sire" }],
    similarRows: [{ name: "BanEi Jockey" }],
    partnershipRows: [],
    liveHorseWeights: [],
    type: "win-rate-heatmap",
    producerDataRevision: 1,
  });
  expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0]).toStrictEqual({
    day: "23",
    includeAge: false,
    includeClass: false,
    includeConditionKey: false,
    includeDistance: true,
    includeGrade: false,
    includeJockeyFrame: true,
    includeOwner: true,
    includeRaceTitle: false,
    includeSurface: false,
    includeTrackCode: false,
    includeTurn: false,
    includeVenue: true,
    keibajoCode: "83",
    month: "08",
    raceNumber: "09",
    source: "nar",
    year: "2026",
    years: 10,
  });
});

it("sends the 10-year Catalog window without foreign stats for a domestic default request", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
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
  getHorseRaceResultsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getPayoutStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });

  await getDetailSectionPayload("win-rate-heatmap", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: { statsYears: "all" },
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0]).toStrictEqual({
    day: "28",
    includeAge: false,
    includeClass: false,
    includeConditionKey: false,
    includeDistance: false,
    includeGrade: false,
    includeJockeyFrame: true,
    includeOwner: true,
    includeRaceTitle: true,
    includeSurface: false,
    includeTrackCode: false,
    includeTurn: false,
    includeVenue: true,
    keibajoCode: "06",
    month: "12",
    raceNumber: "11",
    source: "jra",
    year: "2025",
    years: 10,
  });
});

it("rejects unavailable Catalog even when horse and condition stats exist", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue(null);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
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
  getHorseRaceResultsMock.mockResolvedValue([{ umaban: "02" }]);
  getFinishPositionStatsMock.mockResolvedValue([{ count: 1 }]);
  getFrameStatsMock.mockResolvedValue([{ count: 1, frameNumber: "1" }]);
  getPayoutStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([{ key: "480-499" }]);
  getCarriedWeightClassStatsMock.mockResolvedValue([{ key: "55.5-57" }]);

  const work = getDetailSectionPayload("win-rate-heatmap", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  await expect(work).rejects.toThrow("Heatmap catalog is unavailable");
});

it("similar payload uses Catalog rows with includeOwner and skips Neon stats", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [
      {
        category: "sire",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "ディープインパクト",
        quinellaCount: 4,
        quinellaRate: 20,
        showCount: 6,
        showRate: 30,
        starts: 20,
        winCount: 2,
        winRate: 10,
      },
    ],
    similarRows: [
      {
        category: "jockey",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "ルメール",
        quinellaCount: 3,
        quinellaRate: 15,
        showCount: 4,
        showRate: 20,
        starts: 20,
        winCount: 2,
        winRate: 10,
      },
      {
        category: "owner",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "社台",
        quinellaCount: 2,
        quinellaRate: 10,
        showCount: 3,
        showRate: 15,
        starts: 20,
        winCount: 1,
        winRate: 5,
      },
    ],
  });

  const payload = await getDetailSectionPayload("similar", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    bloodlineRows: [{ name: "ディープインパクト" }],
    rows: [{ name: "ルメール" }, { name: "社台" }],
    type: "similar",
  });
  expect(getSimilarRaceStatsMock).not.toHaveBeenCalled();
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
  expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0]).toMatchObject({
    includeOwner: true,
    keibajoCode: "06",
    source: "jra",
  });
  expect(
    Object.prototype.hasOwnProperty.call(
      fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0] ?? {},
      "includeJockeyFrame",
    ),
  ).toBe(false);
});

it("similar payload falls back to Neon when Catalog binding is missing", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  getSimilarRaceStatsMock.mockResolvedValue([
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
  getBloodlineStatsMock.mockResolvedValue([]);

  const payload = await getDetailSectionPayload("similar", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({ type: "similar" });
  expect(getSimilarRaceStatsMock).toHaveBeenCalled();
  expect(getBloodlineStatsMock).toHaveBeenCalled();
});

it("similar payload keeps incomplete person stats after the bloodline relax fallback exhausts", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [
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
    ],
  });

  const payload = await getDetailSectionPayload("similar", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    similarStatsIncomplete: true,
    type: "similar",
  });
  expect(getSimilarRaceStatsMock).not.toHaveBeenCalled();
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
});

it("similar payload throws when Catalog returns HTTP 502", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockRejectedValue(
    new Error("R2 Catalog heatmap stats failed: 502"),
  );

  await expect(
    getDetailSectionPayload("similar", {
      day: "28",
      keibajoCode: "06",
      month: "12",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2025",
    }),
  ).rejects.toThrow("R2 Catalog heatmap stats failed: 502");
  expect(getSimilarRaceStatsMock).not.toHaveBeenCalled();
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
});

it("bloodline payload uses Catalog rows and skips Neon stats", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [
      {
        category: "sire",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "ディープインパクト",
        quinellaCount: 4,
        quinellaRate: 20,
        showCount: 6,
        showRate: 30,
        starts: 20,
        winCount: 2,
        winRate: 10,
      },
    ],
    similarRows: [],
  });

  const payload = await getDetailSectionPayload("bloodline", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    rows: [{ name: "ディープインパクト" }],
    type: "bloodline",
  });
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
  expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0]).toMatchObject({
    keibajoCode: "06",
    source: "jra",
  });
  expect(
    Object.prototype.hasOwnProperty.call(
      fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0] ?? {},
      "includeOwner",
    ),
  ).toBe(false);
  expect(
    Object.prototype.hasOwnProperty.call(
      fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0] ?? {},
      "includeJockeyFrame",
    ),
  ).toBe(false);
});

it("bloodline payload throws when Catalog returns HTTP 502", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockRejectedValue(
    new Error("R2 Catalog heatmap stats failed: 502"),
  );

  await expect(
    getDetailSectionPayload("bloodline", {
      day: "28",
      keibajoCode: "06",
      month: "12",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2025",
    }),
  ).rejects.toThrow("R2 Catalog heatmap stats failed: 502");
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
});

it("results payload uses Catalog rows and skips Neon history", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchHorseRaceResultsFromCatalogMock.mockResolvedValue([{ umaban: "07" }]);

  const payload = await getDetailSectionPayload("results", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    results: [{ umaban: "07" }],
    type: "results",
  });
  expect(getHorseRaceResultsMock).not.toHaveBeenCalled();
});

it("results payload falls back to Neon when Catalog binding is missing", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  getHorseRaceResultsMock.mockResolvedValue([{ umaban: "03" }]);

  const payload = await getDetailSectionPayload("results", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    results: [{ umaban: "03" }],
    type: "results",
  });
  expect(getHorseRaceResultsMock).toHaveBeenCalledOnce();
});

it("results payload throws when Catalog returns HTTP 502", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchHorseRaceResultsFromCatalogMock.mockRejectedValue(
    new Error("R2 Catalog horse race results failed: 502"),
  );

  await expect(
    getDetailSectionPayload("results", {
      day: "28",
      keibajoCode: "06",
      month: "12",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2025",
    }),
  ).rejects.toThrow("R2 Catalog horse race results failed: 502");
  expect(getHorseRaceResultsMock).not.toHaveBeenCalled();
});

it("condition payload uses Catalog stats, fills payouts from Neon, and skips other Neon history", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  getPayoutStatsMock.mockResolvedValue([{ betType: "単勝", count: 12 }]);
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
    carriedWeightClassStats: [{ key: "55.5-57" }],
    finishPositionStats: [{ count: 2, details: [], finishPosition: 1 }],
    frameStats: [{ count: 8, details: [], frameNumber: "1" }],
    raceTimeStats: {
      averageKohan3f: null,
      averageRaceTime: null,
      correlationRows: [{ horseNumber: "1", score: 0.7 }],
      fastestDetail: null,
      fastestKohan3f: null,
      fastestRaceTime: null,
      medianKohan3f: null,
      medianRaceTime: null,
      raceCount: 12,
      targetRaces: [
        {
          date: "20241027",
          horseName: "イクイノックス",
          horseNumber: "05",
          jockeyName: "ルメール",
          keibajoCode: "05",
          kohan3f: "351",
          ownerName: "シルク",
          popularity: "01",
          raceName: "天皇賞",
          raceNumber: "08",
          raceTime: "1450",
          trainerName: "堀",
        },
      ],
    },
    weightClassStats: [{ key: "480-499" }],
  });

  const payload = await getDetailSectionPayload("condition", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    carriedWeightClassStats: [{ key: "55.5-57" }],
    finishPositionStats: [{ finishPosition: 1 }],
    frameStats: [{ frameNumber: "1" }],
    payoutStats: [{ betType: "単勝", count: 12 }],
    type: "condition",
    weightClassStats: [{ key: "480-499" }],
  });
  expect(getFrameStatsMock).not.toHaveBeenCalled();
  expect(getPayoutStatsMock).toHaveBeenCalledOnce();
  expect(getFinishPositionStatsMock).not.toHaveBeenCalled();
  expect(getWeightClassStatsMock).not.toHaveBeenCalled();
  expect(getCarriedWeightClassStatsMock).not.toHaveBeenCalled();
  expect(getRaceTimeStatsMock).not.toHaveBeenCalled();
});

it("condition payload keeps Ban'ei carried weights empty when Catalog succeeds", async () => {
  getRaceDetailMock.mockResolvedValue(BAN_EI_RACE);
  getRaceRunnersMock.mockResolvedValue([]);
  getPayoutStatsMock.mockResolvedValue([]);
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
    targetRaces: [{ date: "20260501", horseName: "A", horseNumber: "1" }],
  });
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
    carriedWeightClassStats: [{ key: "le49" }],
    finishPositionStats: [{ count: 4, details: [], finishPosition: 1 }],
    frameStats: [{ count: 4, details: [], frameNumber: "1" }],
    raceTimeStats: {
      averageKohan3f: null,
      averageRaceTime: null,
      correlationRows: [],
      fastestDetail: null,
      fastestKohan3f: null,
      fastestRaceTime: null,
      medianKohan3f: null,
      medianRaceTime: null,
      raceCount: 4,
      targetRaces: [{ date: "20260501", horseName: "A", horseNumber: "1" }],
    },
    weightClassStats: [],
  });

  const payload = await getDetailSectionPayload("condition", {
    day: "30",
    keibajoCode: "83",
    month: "05",
    query: {},
    raceNumber: "11",
    raceSource: "nar",
    year: "2026",
  });

  expect(payload).toMatchObject({
    carriedWeightClassStats: [],
    type: "condition",
  });
  expect(getCarriedWeightClassStatsMock).not.toHaveBeenCalled();
});

it("condition payload falls back to Neon when Catalog history counts are empty", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  getPayoutStatsMock.mockResolvedValue([{ betType: "単勝", count: 8 }]);
  getFinishPositionStatsMock.mockResolvedValue([{ count: 40, finishPosition: 1 }]);
  getFrameStatsMock.mockResolvedValue([{ count: 40, frameNumber: "1" }]);
  getWeightClassStatsMock.mockResolvedValue([{ key: "480-499" }]);
  getCarriedWeightClassStatsMock.mockResolvedValue([{ key: "55.5-57" }]);
  getRaceTimeStatsMock.mockResolvedValue({
    averageKohan3f: null,
    averageRaceTime: null,
    correlationRows: [],
    fastestDetail: null,
    fastestKohan3f: null,
    fastestRaceTime: null,
    medianKohan3f: null,
    medianRaceTime: null,
    raceCount: 40,
    targetRaces: [{ date: "20241027", horseName: "A", horseNumber: "1" }],
  });
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
    carriedWeightClassStats: [],
    finishPositionStats: [],
    frameStats: [],
    raceTimeStats: {
      averageKohan3f: null,
      averageRaceTime: null,
      correlationRows: [],
      fastestDetail: null,
      fastestKohan3f: null,
      fastestRaceTime: null,
      medianKohan3f: null,
      medianRaceTime: null,
      raceCount: 0,
      targetRaces: [],
    },
    weightClassStats: [],
  });

  const payload = await getDetailSectionPayload("condition", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    finishPositionStats: [{ count: 40, finishPosition: 1 }],
    frameStats: [{ count: 40, frameNumber: "1" }],
    type: "condition",
  });
  expect(getFinishPositionStatsMock).toHaveBeenCalledOnce();
  expect(getFrameStatsMock).toHaveBeenCalledOnce();
});

it("condition payload falls back to Neon when Catalog returns HTTP 502", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchConditionHistoryStatsFromCatalogMock.mockRejectedValue(
    new Error("R2 Catalog condition history stats failed: 502"),
  );
  getRaceTimeStatsMock.mockResolvedValue({
    averageKohan3f: null,
    averageRaceTime: null,
    correlationRows: [],
    fastestDetail: null,
    fastestKohan3f: null,
    fastestRaceTime: null,
    medianKohan3f: null,
    medianRaceTime: null,
    raceCount: 4,
    targetRaces: [{ date: "20241027", horseName: "A", horseNumber: "1" }],
  });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([{ count: 3, frameNumber: "1" }]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);

  const payload = await getDetailSectionPayload("condition", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    frameStats: [{ frameNumber: "1" }],
    type: "condition",
  });
  expect(getFrameStatsMock.mock.calls.length > 0).toBe(true);
});

it("bloodline payload relaxes distance when Catalog coverage is incomplete", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockImplementation((query) =>
    Promise.resolve({
      bloodlineRows:
        query.includeDistance === false
          ? [
              {
                category: "sire",
                currentHorseNumbers: "1",
                details: [],
                horseCount: 1,
                name: "Sire",
                quinellaCount: 1,
                quinellaRate: 100,
                showCount: 1,
                showRate: 100,
                starts: 1,
                winCount: 1,
                winRate: 100,
              },
            ]
          : [],
      similarRows: [],
    }),
  );

  const payload = await getDetailSectionPayload("bloodline", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    rows: [{ currentHorseNumbers: "1", name: "Sire" }],
    settings: { includeDistance: false, includeRaceTitle: false },
    type: "bloodline",
  });
  expect(payload).not.toHaveProperty("bloodlineStatsIncomplete");
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
});

it("time-score payload uses Catalog similar/bloodline and Catalog raceTimeStats", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  getTimeScoreRowsMock.mockResolvedValue([
    { horseName: "A", horseNumber: "1", jockeyName: "J", score: 0.8, details: [] },
  ]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [
      {
        category: "sire",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "Sire",
        quinellaCount: 1,
        quinellaRate: 10,
        showCount: 1,
        showRate: 10,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
    ],
    similarRows: [
      {
        category: "jockey",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "Jockey",
        quinellaCount: 1,
        quinellaRate: 10,
        showCount: 1,
        showRate: 10,
        starts: 10,
        winCount: 1,
        winRate: 10,
      },
    ],
  });
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
    carriedWeightClassStats: [],
    finishPositionStats: [],
    frameStats: [],
    raceTimeStats: {
      averageKohan3f: null,
      averageRaceTime: null,
      correlationRows: [{ horseName: "A", horseNumber: "1", score: 0.6, details: [] }],
      fastestDetail: null,
      fastestKohan3f: null,
      fastestRaceTime: null,
      medianKohan3f: null,
      medianRaceTime: null,
      raceCount: 4,
      targetRaces: [],
    },
    weightClassStats: [],
  });

  const payload = await getDetailSectionPayload("time-score", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    bloodlineRows: [{ name: "Sire" }],
    correlationRows: [{ horseNumber: "1", score: 0.6 }],
    similarRows: [{ name: "Jockey" }],
    type: "time-score",
  });
  expect(getSimilarRaceStatsMock).not.toHaveBeenCalled();
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
  expect(getRaceTimeStatsMock).not.toHaveBeenCalled();
  expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0]).toMatchObject({
    includeOwner: true,
  });
  expect(
    Object.prototype.hasOwnProperty.call(
      fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0] ?? {},
      "includeJockeyFrame",
    ),
  ).toBe(false);
});

it("time-score payload falls back to Neon when Catalog heatmap stats return 502", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  getTimeScoreRowsMock.mockResolvedValue([]);
  getRaceTimeStatsMock.mockResolvedValue({
    correlationRows: [{ horseNumber: "1", score: 0.4 }],
  });
  fetchWinRateHeatmapStatsFromCatalogMock.mockRejectedValue(
    new Error("R2 Catalog heatmap stats failed: 502"),
  );
  getSimilarRaceStatsMock.mockResolvedValue([
    {
      category: "jockey",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 1,
      name: "Jockey",
      quinellaCount: 1,
      quinellaRate: 50,
      showCount: 1,
      showRate: 50,
      starts: 2,
      winCount: 1,
      winRate: 50,
    },
    {
      category: "trainer",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 1,
      name: "Trainer",
      quinellaCount: 1,
      quinellaRate: 50,
      showCount: 1,
      showRate: 50,
      starts: 2,
      winCount: 1,
      winRate: 50,
    },
  ]);
  getBloodlineStatsMock.mockResolvedValue([
    {
      category: "sire",
      currentHorseNumbers: "1",
      details: [],
      horseCount: 1,
      name: "Sire",
      quinellaCount: 1,
      quinellaRate: 100,
      showCount: 1,
      showRate: 100,
      starts: 1,
      winCount: 1,
      winRate: 100,
    },
  ]);

  const payload = await getDetailSectionPayload("time-score", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    correlationRows: [{ horseNumber: "1", score: 0.4 }],
    similarRows: [{ name: "Jockey" }, { name: "Trainer" }],
    type: "time-score",
  });
  expect(getSimilarRaceStatsMock).toHaveBeenCalledOnce();
  expect(getBloodlineStatsMock).toHaveBeenCalledOnce();
});

it("time-score payload uses Neon race time stats when Catalog correlationRows are empty", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  getTimeScoreRowsMock.mockResolvedValue([]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
    carriedWeightClassStats: [],
    finishPositionStats: [],
    frameStats: [],
    raceTimeStats: {
      averageKohan3f: null,
      averageRaceTime: null,
      correlationRows: [],
      fastestDetail: null,
      fastestKohan3f: null,
      fastestRaceTime: null,
      medianKohan3f: null,
      medianRaceTime: null,
      raceCount: 0,
      targetRaces: [],
    },
    weightClassStats: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({
    correlationRows: [{ horseNumber: "1", score: 0.4 }],
  });

  const payload = await getDetailSectionPayload("time-score", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({
    correlationRows: [{ horseNumber: "1", score: 0.4 }],
    type: "time-score",
  });
  expect(getRaceTimeStatsMock).toHaveBeenCalledOnce();
});

it("overall-score payload composes cached time-score with Catalog bloodline", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [
      {
        category: "sire",
        currentHorseNumbers: "1",
        details: [],
        horseCount: 0,
        name: "Sire",
        quinellaCount: 4,
        quinellaRate: 20,
        showCount: 6,
        showRate: 30,
        starts: 20,
        winCount: 2,
        winRate: 10,
      },
    ],
    similarRows: [],
  });
  getCachedDetailSectionResponseMock.mockResolvedValue(
    Response.json({
      correlationRows: [{ horseNumber: "1", horseName: "A", score: 0.7, details: [] }],
      rows: [{ horseNumber: "1", horseName: "A", jockeyName: "J", score: 0.8, details: [] }],
      type: "time-score",
    }),
  );

  const payload = await getDetailSectionPayload("overall-score", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({ type: "overall-score" });
  expect(getTimeScoreRowsMock).not.toHaveBeenCalled();
  expect(getRaceTimeStatsMock).not.toHaveBeenCalled();
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
});

it("overall-score payload falls back to Neon bloodline when Catalog binding is missing", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  getBloodlineStatsMock.mockResolvedValue([]);
  getTimeScoreRowsMock.mockResolvedValue([]);
  getRaceTimeStatsMock.mockResolvedValue({ correlationRows: [] });

  const payload = await getDetailSectionPayload("overall-score", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({ bloodlineStatsIncomplete: true, type: "overall-score" });
  expect(getBloodlineStatsMock).toHaveBeenCalled();
});

it("overall-score payload uses Catalog raceTimeStats when time-score cache is missing", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
    carriedWeightClassStats: [],
    finishPositionStats: [],
    frameStats: [],
    raceTimeStats: {
      averageKohan3f: null,
      averageRaceTime: null,
      correlationRows: [{ horseNumber: "1", horseName: "A", score: 0.55, details: [] }],
      fastestDetail: null,
      fastestKohan3f: null,
      fastestRaceTime: null,
      medianKohan3f: null,
      medianRaceTime: null,
      raceCount: 3,
      targetRaces: [],
    },
    weightClassStats: [],
  });
  getTimeScoreRowsMock.mockResolvedValue([
    { horseName: "A", horseNumber: "1", jockeyName: "J", score: 0.5, details: [] },
  ]);

  const payload = await getDetailSectionPayload("overall-score", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({ type: "overall-score" });
  expect(getTimeScoreRowsMock).toHaveBeenCalledOnce();
  expect(getRaceTimeStatsMock).not.toHaveBeenCalled();
});

it("overall-score payload uses Neon time rows when time-score cache is missing", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getTimeScoreRowsMock.mockResolvedValue([
    { horseName: "A", horseNumber: "1", jockeyName: "J", score: 0.5, details: [] },
  ]);
  getRaceTimeStatsMock.mockResolvedValue({ correlationRows: [] });

  const payload = await getDetailSectionPayload("overall-score", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({ bloodlineStatsIncomplete: true, type: "overall-score" });
  expect(getTimeScoreRowsMock).toHaveBeenCalledOnce();
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
});

it("overall-score payload ignores invalid cached time-score JSON and uses Neon time rows", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getCachedDetailSectionResponseMock.mockResolvedValue(Response.json({ type: "results" }));
  getTimeScoreRowsMock.mockResolvedValue([]);
  getRaceTimeStatsMock.mockResolvedValue({ correlationRows: [] });

  const payload = await getDetailSectionPayload("overall-score", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({ type: "overall-score" });
  expect(getTimeScoreRowsMock).toHaveBeenCalledOnce();
});

it("overall-score payload ignores unreadable cached time-score JSON and uses Neon time rows", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getCachedDetailSectionResponseMock.mockResolvedValue(new Response("not-json"));
  getTimeScoreRowsMock.mockResolvedValue([]);
  getRaceTimeStatsMock.mockResolvedValue({ correlationRows: [] });

  const payload = await getDetailSectionPayload("overall-score", {
    day: "28",
    keibajoCode: "06",
    month: "12",
    query: {},
    raceNumber: "11",
    raceSource: "jra",
    year: "2025",
  });

  expect(payload).toMatchObject({ type: "overall-score" });
  expect(getTimeScoreRowsMock).toHaveBeenCalledOnce();
});

it("overall-score payload throws when Catalog returns HTTP 502", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchWinRateHeatmapStatsFromCatalogMock.mockRejectedValue(
    new Error("R2 Catalog heatmap stats failed: 502"),
  );

  await expect(
    getDetailSectionPayload("overall-score", {
      day: "28",
      keibajoCode: "06",
      month: "12",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2025",
    }),
  ).rejects.toThrow("R2 Catalog heatmap stats failed: 502");
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
});

it("resolves time-score race-time stats without waiting for getTimeScoreRows", async () => {
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  const releaseRows: { resolve: (rows: unknown[]) => void } = { resolve: () => undefined };
  getSimilarRaceStatsMock.mockResolvedValue([]);
  getTimeScoreRowsMock.mockResolvedValue([]);
  getTimeScoreRowsMock.mockReturnValueOnce(
    new Promise<unknown[]>((resolve) => {
      releaseRows.resolve = resolve;
    }),
  );
  getRaceTimeStatsMock.mockImplementationOnce(async () => {
    releaseRows.resolve([]);
    return {
      averageKohan3f: null,
      averageRaceTime: null,
      correlationRows: [],
      fastestDetail: null,
      fastestKohan3f: null,
      fastestRaceTime: null,
      medianKohan3f: null,
      medianRaceTime: null,
      raceCount: 0,
      targetRaces: [],
    };
  });
  const payload = await getDetailSectionPayload("time-score", {
    day: "23",
    keibajoCode: "06",
    month: "09",
    query: {},
    raceNumber: "05",
    raceSource: "jra",
    year: "2026",
  });
  expect(payload).toMatchObject({ rows: [], type: "time-score" });
  expect(getRaceTimeStatsMock).toHaveBeenCalledOnce();
});

it.each([
  // Saved 05-11 sire aggregate: one start, no placings, unknown horse count.
  { horseCount: 0, starts: 1, showRate: 0, expected: 0.028333333333333335 },
  { horseCount: 0, starts: 4, showRate: 25, expected: 0.12583333333333332 },
  { horseCount: 1, starts: 4, showRate: 25, expected: 0.11083333333333333 },
  { horseCount: 0, starts: 0, showRate: 0, expected: 0 },
])(
  "distinguishes unknown bloodline counts from measured counts and no starts: $horseCount/$starts",
  async (sample) => {
    getRaceDetailMock.mockResolvedValue(JRA_RACE);
    getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
    // Positive-start aggregate shape from saved 08-11.json (Kizuna); the
    // measured-count case exercises the existing formula with an explicit count.
    fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
      bloodlineRows: [
        {
          category: "sire",
          currentHorseNumbers: "1",
          details: [],
          horseCount: sample.horseCount,
          name: "キズナ",
          starts: sample.starts,
          winCount: 0,
          winRate: 0,
          quinellaCount: 0,
          quinellaRate: 0,
          showCount: sample.showRate > 0 ? 1 : 0,
          showRate: sample.showRate,
        },
      ],
      similarRows: [],
    });
    getCachedDetailSectionResponseMock.mockResolvedValue(
      Response.json({
        correlationRows: [],
        rows: [],
        type: "time-score",
      }),
    );
    const payload = await getDetailSectionPayload("overall-score", {
      day: "04",
      keibajoCode: "08",
      month: "10",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2026",
    });
    expect(payload).toMatchObject({
      rows: [
        {
          storedOdds: null,
          storedPopularity: null,
          details: expect.arrayContaining([
            expect.objectContaining({
              label: "血統スコア",
              score: expect.closeTo(sample.expected, 12),
            }),
          ]),
        },
      ],
    });
  },
);

it.each([
  { race: JRA_RACE, odds: "0085", popularity: "03", expectedOdds: 8.5, expectedPopularity: 3 },
  {
    race: { ...JRA_RACE, keibajoCode: "C4" },
    odds: "0085",
    popularity: "03",
    expectedOdds: 8.5,
    expectedPopularity: 3,
  },
  {
    race: { ...JRA_RACE, keibajoCode: "C4" },
    odds: "0000",
    popularity: "00",
    expectedOdds: null,
    expectedPopularity: null,
  },
])(
  "decodes stored market values for ordinary and overseas overall scores: $odds/$popularity",
  async ({ race, odds, popularity, expectedOdds, expectedPopularity }) => {
    getRaceDetailMock.mockResolvedValue(race);
    getRaceRunnersMock.mockResolvedValue([
      { ...OVERSEAS_RUNNER, tanshoOdds: odds, tanshoNinkijun: popularity },
    ]);
    fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
      bloodlineRows: [],
      similarRows: [],
    });
    getCachedDetailSectionResponseMock.mockResolvedValue(
      Response.json({ correlationRows: [], rows: [], type: "time-score" }),
    );
    const payload = await getDetailSectionPayload("overall-score", {
      day: "04",
      keibajoCode: race.keibajoCode,
      month: "10",
      query: {},
      raceNumber: "11",
      raceSource: "jra",
      year: "2026",
    });
    expect(payload).toMatchObject({
      rows: [{ storedOdds: expectedOdds, storedPopularity: expectedPopularity }],
    });
  },
);

it("does not reuse any default section source for a filtered heatmap", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  getCachedDetailSectionResponseMock.mockImplementation(async () =>
    Response.json({ type: "bloodline", rows: [{ starts: 100, winCount: 50 }] }),
  );
  getHorseRaceResultsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    year: "2026",
    month: "08",
    day: "23",
    keibajoCode: "04",
    raceNumber: "01",
    raceSource: "jra",
    query: { statsVenue: "0", resultsSourceScope: "nar" },
  });
  expect(getCachedDetailSectionResponseMock).not.toHaveBeenCalled();
  expect(fetchHorseRaceResultsFromCatalogMock).toHaveBeenCalledWith(
    expect.objectContaining({ sourceScope: "nar" }),
  );
  expect(payload).toMatchObject({ type: "win-rate-heatmap", bloodlineRows: [], horseResults: [] });
});

it("waits for condition failures and results before rejecting the owned heatmap", async () => {
  // Local databases keep the full condition section for heatmaps.
  getDatabaseTargetMock.mockReturnValue("local");
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  const time = Promise.withResolvers<unknown>();
  const results = Promise.withResolvers<unknown[]>();
  const timeStarted = Promise.withResolvers<void>();
  getRaceTimeStatsMock.mockImplementation(() => {
    timeStarted.resolve();
    return time.promise;
  });
  getPayoutStatsMock.mockRejectedValue(new Error("condition failure"));
  getHorseRaceResultsMock.mockReturnValue(results.promise);
  const onSettled = vi.fn<(value: unknown) => void>();
  const work = getDetailSectionPayload("win-rate-heatmap", {
    year: "2026",
    month: "08",
    day: "23",
    keibajoCode: "04",
    raceNumber: "01",
    raceSource: "jra",
    query: {},
  }).catch(onSettled);
  await timeStarted.promise;
  await Promise.resolve();
  expect(onSettled).not.toHaveBeenCalled();
  time.resolve({});
  await Promise.resolve();
  expect(onSettled).not.toHaveBeenCalled();
  results.resolve([]);
  await work;
  expect(onSettled).toHaveBeenCalledWith(new Error("condition failure"));
});

it("waits for source work when a heatmap branch rejects", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    bloodlineRows: [],
    similarRows: [],
  });
  getRaceTimeStatsMock.mockResolvedValue({ raceCount: 0 });
  getPayoutStatsMock.mockResolvedValue([]);
  getFinishPositionStatsMock.mockResolvedValue([]);
  getFrameStatsMock.mockResolvedValue([]);
  getWeightClassStatsMock.mockResolvedValue([]);
  getCarriedWeightClassStatsMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  const results = Promise.withResolvers<unknown[]>();
  const started = Promise.withResolvers<void>();
  getHorseRaceResultsMock.mockImplementation(() => {
    started.resolve();
    return results.promise;
  });
  fetchHeatmapPartnershipRowsMock.mockRejectedValue(new Error("fatal partnership failure"));
  const rejected = vi.fn<(value: unknown) => void>();
  const work = getDetailSectionPayload("win-rate-heatmap", {
    year: "2026",
    month: "08",
    day: "23",
    keibajoCode: "04",
    raceNumber: "01",
    raceSource: "jra",
    query: {},
  }).catch(rejected);
  await started.promise;
  expect(rejected).not.toHaveBeenCalled();
  results.resolve([]);
  await work;
  expect(rejected).toHaveBeenCalledWith(new Error("fatal partnership failure"));
});

it.each(["results", "partnership", "catalog"])(
  "does not persist transient %s failure and retries Kyoto with observed 15/40/32 source rows",
  async (failedSource) => {
    const values = new Map<string, string>();
    const put = vi.fn<(key: string, value: string) => Promise<void>>(async (key, value) => {
      values.set(key, value);
    });
    heatmapRuntimeMock.mockResolvedValue({
      ctx: null,
      env: {
        DETAIL_SECTION_CACHE_KV: {
          get: vi.fn<(key: string) => Promise<string | null>>(
            async (key) => values.get(key) ?? null,
          ),
          put,
        },
      },
    });
    getRaceDetailMock.mockResolvedValue(JRA_RACE);
    getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
    // Projected observed Kyoto 2026-10-04 08/01 evidence; no invented rate values.
    const results = [
      {
        currentUmaban: "01",
        kakuteiChakujun: "02",
        keibajoCode: "01",
        bataiju: "520",
        futanJuryo: "550",
      },
      {
        currentUmaban: "01",
        kakuteiChakujun: "06",
        keibajoCode: "01",
        bataiju: "514",
        futanJuryo: "550",
      },
      {
        currentUmaban: "01",
        kakuteiChakujun: "08",
        keibajoCode: "05",
        bataiju: "520",
        futanJuryo: "550",
      },
      {
        currentUmaban: "02",
        kakuteiChakujun: "08",
        keibajoCode: "09",
        bataiju: "466",
        futanJuryo: "550",
      },
      {
        currentUmaban: "03",
        kakuteiChakujun: "05",
        keibajoCode: "09",
        bataiju: "500",
        futanJuryo: "550",
      },
      {
        currentUmaban: "03",
        kakuteiChakujun: "07",
        keibajoCode: "07",
        bataiju: "494",
        futanJuryo: "550",
      },
      {
        currentUmaban: "03",
        kakuteiChakujun: "03",
        keibajoCode: "10",
        bataiju: "504",
        futanJuryo: "550",
      },
      {
        currentUmaban: "03",
        kakuteiChakujun: "08",
        keibajoCode: "03",
        bataiju: "512",
        futanJuryo: "550",
      },
      {
        currentUmaban: "04",
        kakuteiChakujun: "02",
        keibajoCode: "09",
        bataiju: "532",
        futanJuryo: "540",
      },
      {
        currentUmaban: "05",
        kakuteiChakujun: "11",
        keibajoCode: "10",
        bataiju: "462",
        futanJuryo: "530",
      },
      {
        currentUmaban: "06",
        kakuteiChakujun: "03",
        keibajoCode: "09",
        bataiju: "442",
        futanJuryo: "550",
      },
      {
        currentUmaban: "07",
        kakuteiChakujun: "14",
        keibajoCode: "05",
        bataiju: "458",
        futanJuryo: "550",
      },
      {
        currentUmaban: "08",
        kakuteiChakujun: "04",
        keibajoCode: "09",
        bataiju: "486",
        futanJuryo: "550",
      },
      {
        currentUmaban: "08",
        kakuteiChakujun: "04",
        keibajoCode: "07",
        bataiju: "488",
        futanJuryo: "550",
      },
      {
        currentUmaban: "08",
        kakuteiChakujun: "08",
        keibajoCode: "09",
        bataiju: "482",
        futanJuryo: "550",
      },
    ];
    const partnershipRows = [
      {
        category: "horseJockey",
        currentHorseNumbers: "1",
        name: "マーズエクレール × 鮫島克駿",
        details: [],
        horseCount: 0,
        starts: 1,
        winCount: 0,
        quinellaCount: 1,
        showCount: 1,
        winRate: 0,
        quinellaRate: 100,
        showRate: 100,
      },
      {
        category: "horseJockey",
        currentHorseNumbers: "2",
        name: "ベンゲル × 池添謙一",
        details: [],
        horseCount: 0,
        starts: 1,
        winCount: 0,
        quinellaCount: 0,
        showCount: 0,
        winRate: 0,
        quinellaRate: 0,
        showRate: 0,
      },
      {
        category: "horseJockey",
        currentHorseNumbers: "3",
        name: "プリメラエストレラ × 北村友一",
        details: [],
        horseCount: 0,
        starts: 1,
        winCount: 0,
        quinellaCount: 0,
        showCount: 0,
        winRate: 0,
        quinellaRate: 0,
        showRate: 0,
      },
      {
        category: "horseJockey",
        currentHorseNumbers: "4",
        name: "マーノマエストロ × 田山旺佑",
        details: [],
        horseCount: 0,
        starts: 1,
        winCount: 0,
        quinellaCount: 1,
        showCount: 1,
        winRate: 0,
        quinellaRate: 100,
        showRate: 100,
      },
      {
        category: "horseJockey",
        currentHorseNumbers: "5",
        name: "ロードフェルム × 横山和生",
        details: [],
        horseCount: 0,
        starts: 0,
        winCount: 0,
        quinellaCount: 0,
        showCount: 0,
        winRate: 0,
        quinellaRate: 0,
        showRate: 0,
      },
      {
        category: "horseJockey",
        currentHorseNumbers: "6",
        name: "タガノフリアン × 角田大和",
        details: [],
        horseCount: 0,
        starts: 1,
        winCount: 0,
        quinellaCount: 0,
        showCount: 1,
        winRate: 0,
        quinellaRate: 0,
        showRate: 100,
      },
      {
        category: "horseJockey",
        currentHorseNumbers: "7",
        name: "ボードゥロレーヌ × 吉村誠之",
        details: [],
        horseCount: 0,
        starts: 0,
        winCount: 0,
        quinellaCount: 0,
        showCount: 0,
        winRate: 0,
        quinellaRate: 0,
        showRate: 0,
      },
      {
        category: "horseJockey",
        currentHorseNumbers: "8",
        name: "ジュンヴァグネル × 斎藤新",
        details: [],
        horseCount: 0,
        starts: 0,
        winCount: 0,
        quinellaCount: 0,
        showCount: 0,
        winRate: 0,
        quinellaRate: 0,
        showRate: 0,
      },
      {
        category: "jockeyVenue",
        currentHorseNumbers: "1",
        name: "鮫島克駿",
        details: [],
        horseCount: 0,
        starts: 343,
        winCount: 25,
        quinellaCount: 58,
        showCount: 93,
        winRate: 7.3,
        quinellaRate: 16.9,
        showRate: 27.1,
      },
      {
        category: "jockeyVenue",
        currentHorseNumbers: "2",
        name: "池添謙一",
        details: [],
        horseCount: 0,
        starts: 210,
        winCount: 15,
        quinellaCount: 35,
        showCount: 50,
        winRate: 7.1,
        quinellaRate: 16.7,
        showRate: 23.8,
      },
      {
        category: "jockeyVenue",
        currentHorseNumbers: "3",
        name: "北村友一",
        details: [],
        horseCount: 0,
        starts: 244,
        winCount: 20,
        quinellaCount: 46,
        showCount: 61,
        winRate: 8.2,
        quinellaRate: 18.9,
        showRate: 25,
      },
      {
        category: "jockeyVenue",
        currentHorseNumbers: "4",
        name: "田山旺佑",
        details: [],
        horseCount: 0,
        starts: 133,
        winCount: 12,
        quinellaCount: 24,
        showCount: 28,
        winRate: 9,
        quinellaRate: 18,
        showRate: 21.1,
      },
      {
        category: "jockeyVenue",
        currentHorseNumbers: "5",
        name: "横山和生",
        details: [],
        horseCount: 0,
        starts: 43,
        winCount: 5,
        quinellaCount: 12,
        showCount: 22,
        winRate: 11.6,
        quinellaRate: 27.9,
        showRate: 51.2,
      },
      {
        category: "jockeyVenue",
        currentHorseNumbers: "6",
        name: "角田大和",
        details: [],
        horseCount: 0,
        starts: 173,
        winCount: 9,
        quinellaCount: 18,
        showCount: 31,
        winRate: 5.2,
        quinellaRate: 10.4,
        showRate: 17.9,
      },
      {
        category: "jockeyVenue",
        currentHorseNumbers: "7",
        name: "吉村誠之",
        details: [],
        horseCount: 0,
        starts: 472,
        winCount: 27,
        quinellaCount: 58,
        showCount: 95,
        winRate: 5.7,
        quinellaRate: 12.3,
        showRate: 20.1,
      },
      {
        category: "jockeyVenue",
        currentHorseNumbers: "8",
        name: "斎藤新",
        details: [],
        horseCount: 0,
        starts: 100,
        winCount: 5,
        quinellaCount: 11,
        showCount: 17,
        winRate: 5,
        quinellaRate: 11,
        showRate: 17,
      },
      {
        category: "jockeyTrainerVenue",
        currentHorseNumbers: "1",
        name: "鮫島克駿 × 佐藤悠太",
        details: [],
        horseCount: 0,
        starts: 1,
        winCount: 0,
        quinellaCount: 0,
        showCount: 0,
        winRate: 0,
        quinellaRate: 0,
        showRate: 0,
      },
      {
        category: "jockeyTrainerVenue",
        currentHorseNumbers: "2",
        name: "池添謙一 × 飯田祐史",
        details: [],
        horseCount: 0,
        starts: 20,
        winCount: 3,
        quinellaCount: 5,
        showCount: 7,
        winRate: 15,
        quinellaRate: 25,
        showRate: 35,
      },
      {
        category: "jockeyTrainerVenue",
        currentHorseNumbers: "3",
        name: "北村友一 × 橋田宜長",
        details: [],
        horseCount: 0,
        starts: 0,
        winCount: 0,
        quinellaCount: 0,
        showCount: 0,
        winRate: 0,
        quinellaRate: 0,
        showRate: 0,
      },
      {
        category: "jockeyTrainerVenue",
        currentHorseNumbers: "4",
        name: "田山旺佑 × 寺島良",
        details: [],
        horseCount: 0,
        starts: 1,
        winCount: 0,
        quinellaCount: 0,
        showCount: 0,
        winRate: 0,
        quinellaRate: 0,
        showRate: 0,
      },
      {
        category: "jockeyTrainerVenue",
        currentHorseNumbers: "5",
        name: "横山和生 × 長谷川浩",
        details: [],
        horseCount: 0,
        starts: 0,
        winCount: 0,
        quinellaCount: 0,
        showCount: 0,
        winRate: 0,
        quinellaRate: 0,
        showRate: 0,
      },
      {
        category: "jockeyTrainerVenue",
        currentHorseNumbers: "6",
        name: "角田大和 × 角田晃一",
        details: [],
        horseCount: 0,
        starts: 47,
        winCount: 2,
        quinellaCount: 5,
        showCount: 8,
        winRate: 4.3,
        quinellaRate: 10.6,
        showRate: 17,
      },
      {
        category: "jockeyTrainerVenue",
        currentHorseNumbers: "7",
        name: "吉村誠之 × 新谷功一",
        details: [],
        horseCount: 0,
        starts: 6,
        winCount: 0,
        quinellaCount: 0,
        showCount: 1,
        winRate: 0,
        quinellaRate: 0,
        showRate: 16.7,
      },
      {
        category: "jockeyTrainerVenue",
        currentHorseNumbers: "8",
        name: "斎藤新 × 浜田多実",
        details: [],
        horseCount: 0,
        starts: 5,
        winCount: 1,
        quinellaCount: 1,
        showCount: 1,
        winRate: 20,
        quinellaRate: 20,
        showRate: 20,
      },
      {
        category: "ownerVenue",
        currentHorseNumbers: "1",
        name: "ＤＭＭドリームクラブ",
        details: [],
        horseCount: 0,
        starts: 65,
        winCount: 6,
        quinellaCount: 12,
        showCount: 18,
        winRate: 9.2,
        quinellaRate: 18.5,
        showRate: 27.7,
      },
      {
        category: "ownerVenue",
        currentHorseNumbers: "2",
        name: "カナヤマホールディングス",
        details: [],
        horseCount: 0,
        starts: 270,
        winCount: 30,
        quinellaCount: 56,
        showCount: 76,
        winRate: 11.1,
        quinellaRate: 20.7,
        showRate: 28.1,
      },
      {
        category: "ownerVenue",
        currentHorseNumbers: "3",
        name: "ノースヒルズ",
        details: [],
        horseCount: 0,
        starts: 736,
        winCount: 59,
        quinellaCount: 131,
        showCount: 202,
        winRate: 8,
        quinellaRate: 17.8,
        showRate: 27.4,
      },
      {
        category: "ownerVenue",
        currentHorseNumbers: "4",
        name: "吉田　照哉",
        details: [],
        horseCount: 0,
        starts: 544,
        winCount: 64,
        quinellaCount: 115,
        showCount: 161,
        winRate: 11.8,
        quinellaRate: 21.1,
        showRate: 29.6,
      },
      {
        category: "ownerVenue",
        currentHorseNumbers: "5",
        name: "ロードホースクラブ",
        details: [],
        horseCount: 0,
        starts: 421,
        winCount: 46,
        quinellaCount: 77,
        showCount: 124,
        winRate: 10.9,
        quinellaRate: 18.3,
        showRate: 29.5,
      },
      {
        category: "ownerVenue",
        currentHorseNumbers: "6",
        name: "八木　昌司",
        details: [],
        horseCount: 0,
        starts: 77,
        winCount: 7,
        quinellaCount: 14,
        showCount: 20,
        winRate: 9.1,
        quinellaRate: 18.2,
        showRate: 26,
      },
      {
        category: "ownerVenue",
        currentHorseNumbers: "7",
        name: "Ｇ１レーシング",
        details: [],
        horseCount: 0,
        starts: 301,
        winCount: 33,
        quinellaCount: 59,
        showCount: 89,
        winRate: 11,
        quinellaRate: 19.6,
        showRate: 29.6,
      },
      {
        category: "ownerVenue",
        currentHorseNumbers: "8",
        name: "河合　純二",
        details: [],
        horseCount: 0,
        starts: 38,
        winCount: 2,
        quinellaCount: 9,
        showCount: 14,
        winRate: 5.3,
        quinellaRate: 23.7,
        showRate: 36.8,
      },
      {
        category: "jockeyTrainerOwner",
        currentHorseNumbers: "1",
        name: "鮫島克駿 × 佐藤悠太 × ＤＭＭドリームクラブ",
        details: [],
        horseCount: 0,
        starts: 2,
        winCount: 0,
        quinellaCount: 1,
        showCount: 1,
        winRate: 0,
        quinellaRate: 50,
        showRate: 50,
      },
      {
        category: "jockeyTrainerOwner",
        currentHorseNumbers: "2",
        name: "池添謙一 × 飯田祐史 × カナヤマホールディングス",
        details: [],
        horseCount: 0,
        starts: 3,
        winCount: 0,
        quinellaCount: 0,
        showCount: 1,
        winRate: 0,
        quinellaRate: 0,
        showRate: 33.3,
      },
      {
        category: "jockeyTrainerOwner",
        currentHorseNumbers: "3",
        name: "北村友一 × 橋田宜長 × ノースヒルズ",
        details: [],
        horseCount: 0,
        starts: 0,
        winCount: 0,
        quinellaCount: 0,
        showCount: 0,
        winRate: 0,
        quinellaRate: 0,
        showRate: 0,
      },
      {
        category: "jockeyTrainerOwner",
        currentHorseNumbers: "4",
        name: "田山旺佑 × 寺島良 × 吉田　照哉",
        details: [],
        horseCount: 0,
        starts: 1,
        winCount: 0,
        quinellaCount: 1,
        showCount: 1,
        winRate: 0,
        quinellaRate: 100,
        showRate: 100,
      },
      {
        category: "jockeyTrainerOwner",
        currentHorseNumbers: "5",
        name: "横山和生 × 長谷川浩 × ロードホースクラブ",
        details: [],
        horseCount: 0,
        starts: 1,
        winCount: 0,
        quinellaCount: 0,
        showCount: 0,
        winRate: 0,
        quinellaRate: 0,
        showRate: 0,
      },
      {
        category: "jockeyTrainerOwner",
        currentHorseNumbers: "6",
        name: "角田大和 × 角田晃一 × 八木　昌司",
        details: [],
        horseCount: 0,
        starts: 0,
        winCount: 0,
        quinellaCount: 0,
        showCount: 0,
        winRate: 0,
        quinellaRate: 0,
        showRate: 0,
      },
      {
        category: "jockeyTrainerOwner",
        currentHorseNumbers: "7",
        name: "吉村誠之 × 新谷功一 × Ｇ１レーシング",
        details: [],
        horseCount: 0,
        starts: 0,
        winCount: 0,
        quinellaCount: 0,
        showCount: 0,
        winRate: 0,
        quinellaRate: 0,
        showRate: 0,
      },
      {
        category: "jockeyTrainerOwner",
        currentHorseNumbers: "8",
        name: "斎藤新 × 浜田多実 × 河合　純二",
        details: [],
        horseCount: 0,
        starts: 3,
        winCount: 1,
        quinellaCount: 1,
        showCount: 1,
        winRate: 33.3,
        quinellaRate: 33.3,
        showRate: 33.3,
      },
    ];
    const similarRows = [
      {
        category: "jockey",
        currentHorseNumbers: "1",
        name: "鮫島克駿",
        details: [],
        horseCount: 0,
        starts: 7315,
        winCount: 593,
        quinellaCount: 1239,
        showCount: 1885,
        winRate: 8.1,
        quinellaRate: 16.9,
        showRate: 25.8,
      },
      {
        category: "jockeyFrame",
        currentHorseNumbers: "1",
        name: "鮫島克駿",
        details: [],
        horseCount: 0,
        starts: 712,
        winCount: 43,
        quinellaCount: 114,
        showCount: 178,
        winRate: 6,
        quinellaRate: 16,
        showRate: 25,
      },
      {
        category: "owner",
        currentHorseNumbers: "1",
        name: "ＤＭＭドリームクラブ",
        details: [],
        horseCount: 0,
        starts: 1498,
        winCount: 175,
        quinellaCount: 324,
        showCount: 455,
        winRate: 11.7,
        quinellaRate: 21.6,
        showRate: 30.4,
      },
      {
        category: "trainer",
        currentHorseNumbers: "1",
        name: "佐藤悠太",
        details: [],
        horseCount: 0,
        starts: 450,
        winCount: 44,
        quinellaCount: 97,
        showCount: 134,
        winRate: 9.8,
        quinellaRate: 21.6,
        showRate: 29.8,
      },
      {
        category: "jockey",
        currentHorseNumbers: "2",
        name: "池添謙一",
        details: [],
        horseCount: 0,
        starts: 4898,
        winCount: 470,
        quinellaCount: 896,
        showCount: 1320,
        winRate: 9.6,
        quinellaRate: 18.3,
        showRate: 26.9,
      },
      {
        category: "jockeyFrame",
        currentHorseNumbers: "2",
        name: "池添謙一",
        details: [],
        horseCount: 0,
        starts: 510,
        winCount: 55,
        quinellaCount: 101,
        showCount: 136,
        winRate: 10.8,
        quinellaRate: 19.8,
        showRate: 26.7,
      },
      {
        category: "owner",
        currentHorseNumbers: "2",
        name: "カナヤマホールディングス",
        details: [],
        horseCount: 0,
        starts: 3196,
        winCount: 497,
        quinellaCount: 846,
        showCount: 1174,
        winRate: 15.6,
        quinellaRate: 26.5,
        showRate: 36.7,
      },
      {
        category: "trainer",
        currentHorseNumbers: "2",
        name: "飯田祐史",
        details: [],
        horseCount: 0,
        starts: 2344,
        winCount: 145,
        quinellaCount: 320,
        showCount: 470,
        winRate: 6.2,
        quinellaRate: 13.7,
        showRate: 20.1,
      },
      {
        category: "jockey",
        currentHorseNumbers: "3",
        name: "北村友一",
        details: [],
        horseCount: 0,
        starts: 5278,
        winCount: 568,
        quinellaCount: 1095,
        showCount: 1599,
        winRate: 10.8,
        quinellaRate: 20.7,
        showRate: 30.3,
      },
      {
        category: "jockeyFrame",
        currentHorseNumbers: "3",
        name: "北村友一",
        details: [],
        horseCount: 0,
        starts: 613,
        winCount: 64,
        quinellaCount: 130,
        showCount: 200,
        winRate: 10.4,
        quinellaRate: 21.2,
        showRate: 32.6,
      },
      {
        category: "owner",
        currentHorseNumbers: "3",
        name: "ノースヒルズ",
        details: [],
        horseCount: 0,
        starts: 4991,
        winCount: 452,
        quinellaCount: 900,
        showCount: 1386,
        winRate: 9.1,
        quinellaRate: 18,
        showRate: 27.8,
      },
      {
        category: "trainer",
        currentHorseNumbers: "3",
        name: "橋田宜長",
        details: [],
        horseCount: 0,
        starts: 126,
        winCount: 8,
        quinellaCount: 18,
        showCount: 30,
        winRate: 6.3,
        quinellaRate: 14.3,
        showRate: 23.8,
      },
      {
        category: "jockey",
        currentHorseNumbers: "4",
        name: "田山旺佑",
        details: [],
        horseCount: 0,
        starts: 991,
        winCount: 69,
        quinellaCount: 135,
        showCount: 200,
        winRate: 7,
        quinellaRate: 13.6,
        showRate: 20.2,
      },
      {
        category: "jockeyFrame",
        currentHorseNumbers: "4",
        name: "田山旺佑",
        details: [],
        horseCount: 0,
        starts: 132,
        winCount: 5,
        quinellaCount: 11,
        showCount: 24,
        winRate: 3.8,
        quinellaRate: 8.3,
        showRate: 18.2,
      },
      {
        category: "owner",
        currentHorseNumbers: "4",
        name: "吉田照哉",
        details: [],
        horseCount: 0,
        starts: 8003,
        winCount: 894,
        quinellaCount: 1709,
        showCount: 2466,
        winRate: 11.2,
        quinellaRate: 21.4,
        showRate: 30.8,
      },
      {
        category: "trainer",
        currentHorseNumbers: "4",
        name: "寺島良",
        details: [],
        horseCount: 0,
        starts: 3289,
        winCount: 290,
        quinellaCount: 573,
        showCount: 840,
        winRate: 8.8,
        quinellaRate: 17.4,
        showRate: 25.5,
      },
      {
        category: "jockey",
        currentHorseNumbers: "5",
        name: "横山和生",
        details: [],
        horseCount: 0,
        starts: 4887,
        winCount: 471,
        quinellaCount: 874,
        showCount: 1257,
        winRate: 9.6,
        quinellaRate: 17.9,
        showRate: 25.7,
      },
      {
        category: "jockeyFrame",
        currentHorseNumbers: "5",
        name: "横山和生",
        details: [],
        horseCount: 0,
        starts: 651,
        winCount: 59,
        quinellaCount: 115,
        showCount: 163,
        winRate: 9.1,
        quinellaRate: 17.7,
        showRate: 25,
      },
      {
        category: "owner",
        currentHorseNumbers: "5",
        name: "ロードホースクラブ",
        details: [],
        horseCount: 0,
        starts: 3579,
        winCount: 381,
        quinellaCount: 698,
        showCount: 995,
        winRate: 10.6,
        quinellaRate: 19.5,
        showRate: 27.8,
      },
      {
        category: "trainer",
        currentHorseNumbers: "5",
        name: "長谷川浩",
        details: [],
        horseCount: 0,
        starts: 1867,
        winCount: 158,
        quinellaCount: 308,
        showCount: 474,
        winRate: 8.5,
        quinellaRate: 16.5,
        showRate: 25.4,
      },
      {
        category: "jockey",
        currentHorseNumbers: "6",
        name: "角田大和",
        details: [],
        horseCount: 0,
        starts: 2719,
        winCount: 141,
        quinellaCount: 320,
        showCount: 510,
        winRate: 5.2,
        quinellaRate: 11.8,
        showRate: 18.8,
      },
      {
        category: "jockeyFrame",
        currentHorseNumbers: "6",
        name: "角田大和",
        details: [],
        horseCount: 0,
        starts: 358,
        winCount: 14,
        quinellaCount: 39,
        showCount: 63,
        winRate: 3.9,
        quinellaRate: 10.9,
        showRate: 17.6,
      },
      {
        category: "owner",
        currentHorseNumbers: "6",
        name: "八木昌司",
        details: [],
        horseCount: 0,
        starts: 241,
        winCount: 29,
        quinellaCount: 61,
        showCount: 87,
        winRate: 12,
        quinellaRate: 25.3,
        showRate: 36.1,
      },
      {
        category: "trainer",
        currentHorseNumbers: "6",
        name: "角田晃一",
        details: [],
        horseCount: 0,
        starts: 2761,
        winCount: 230,
        quinellaCount: 477,
        showCount: 717,
        winRate: 8.3,
        quinellaRate: 17.3,
        showRate: 26,
      },
      {
        category: "jockey",
        currentHorseNumbers: "7",
        name: "吉村誠之",
        details: [],
        horseCount: 0,
        starts: 2215,
        winCount: 161,
        quinellaCount: 320,
        showCount: 502,
        winRate: 7.3,
        quinellaRate: 14.4,
        showRate: 22.7,
      },
      {
        category: "jockeyFrame",
        currentHorseNumbers: "7",
        name: "吉村誠之",
        details: [],
        horseCount: 0,
        starts: 313,
        winCount: 26,
        quinellaCount: 46,
        showCount: 80,
        winRate: 8.3,
        quinellaRate: 14.7,
        showRate: 25.6,
      },
      {
        category: "owner",
        currentHorseNumbers: "7",
        name: "Ｇ１レーシング",
        details: [],
        horseCount: 0,
        starts: 5873,
        winCount: 574,
        quinellaCount: 1106,
        showCount: 1628,
        winRate: 9.8,
        quinellaRate: 18.8,
        showRate: 27.7,
      },
      {
        category: "trainer",
        currentHorseNumbers: "7",
        name: "新谷功一",
        details: [],
        horseCount: 0,
        starts: 1860,
        winCount: 162,
        quinellaCount: 318,
        showCount: 464,
        winRate: 8.7,
        quinellaRate: 17.1,
        showRate: 24.9,
      },
      {
        category: "jockey",
        currentHorseNumbers: "8",
        name: "斎藤新",
        details: [],
        horseCount: 0,
        starts: 4592,
        winCount: 309,
        quinellaCount: 604,
        showCount: 898,
        winRate: 6.7,
        quinellaRate: 13.2,
        showRate: 19.6,
      },
      {
        category: "jockeyFrame",
        currentHorseNumbers: "8",
        name: "斎藤新",
        details: [],
        horseCount: 0,
        starts: 701,
        winCount: 50,
        quinellaCount: 84,
        showCount: 144,
        winRate: 7.1,
        quinellaRate: 12,
        showRate: 20.5,
      },
      {
        category: "owner",
        currentHorseNumbers: "8",
        name: "河合純二",
        details: [],
        horseCount: 0,
        starts: 1561,
        winCount: 157,
        quinellaCount: 323,
        showCount: 461,
        winRate: 10.1,
        quinellaRate: 20.7,
        showRate: 29.5,
      },
      {
        category: "trainer",
        currentHorseNumbers: "8",
        name: "浜田多実",
        details: [],
        horseCount: 0,
        starts: 2633,
        winCount: 191,
        quinellaCount: 367,
        showCount: 552,
        winRate: 7.3,
        quinellaRate: 13.9,
        showRate: 21,
      },
    ];
    fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
      bloodlineRows: [],
      similarRows,
    });
    fetchHeatmapPartnershipRowsMock.mockResolvedValue(partnershipRows);
    getCachedDetailSectionResponseMock.mockImplementation(async (key: string) =>
      Response.json(
        key.includes(":results:")
          ? { type: "results", results }
          : { type: "condition", frameStats: [] },
      ),
    );
    if (failedSource === "results")
      getCachedDetailSectionResponseMock.mockRejectedValueOnce(new Error("results unavailable"));
    if (failedSource === "partnership")
      fetchHeatmapPartnershipRowsMock.mockRejectedValueOnce(
        new Error("Partnership catalog request failed: 502"),
      );
    if (failedSource === "catalog")
      fetchWinRateHeatmapStatsFromCatalogMock.mockRejectedValueOnce(
        new Error("catalog unavailable"),
      );
    const warm = async () => {
      const payload = await getDetailSectionPayload("win-rate-heatmap", {
        year: "2026",
        month: "10",
        day: "04",
        keibajoCode: "08",
        raceNumber: "01",
        raceSource: "jra",
        query: {},
      });
      if (!isWinRateHeatmapSectionPayload(payload)) throw new Error("Invalid heatmap");
      await putWinRateHeatmapCache({ cacheKey: "kyoto-retry", payload });
      return payload;
    };
    await expect(warm()).rejects.toThrow(/unavailable|502/u);
    expect(put).not.toHaveBeenCalled();
    const payload = await warm();
    expect(payload.horseResults).toHaveLength(15);
    expect(payload.partnershipRows).toHaveLength(40);
    expect(payload.similarRows).toHaveLength(32);
    const cached = await getCachedWinRateHeatmapPayload("kyoto-retry", payload.runners);
    expect(cached?.producerDataRevision).toBe(1);
    expect(cached?.horseRateStats).toHaveLength(8);
    expect(cached?.partnershipRows).toHaveLength(40);
    expect(cached?.similarRows).toHaveLength(32);
  },
);

it("does not relax overseas empty frames into domestic bins", async () => {
  getRaceDetailMock.mockResolvedValue({ ...JRA_RACE, keibajoCode: "A8" });
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
    frameStats: [],
    weightClassStats: [],
    carriedWeightClassStats: [],
    finishPositionStats: [],
    raceTimeStats: { targetRaces: [{}] },
    mappingRevision: "gate-v1-test",
  });
  const payload = await getDetailSectionPayload("condition", {
    day: "04",
    month: "10",
    year: "2026",
    keibajoCode: "A8",
    raceNumber: "05",
    raceSource: "jra",
    query: {},
  });
  expect(payload).toMatchObject({ frameStats: [], mappingRevision: "gate-v1-test" });
  expect(getFrameStatsMock).not.toHaveBeenCalled();
});
it("does not fall back to domestic frame SQL when overseas catalog fails", async () => {
  getRaceDetailMock.mockResolvedValue({ ...JRA_RACE, keibajoCode: "A8" });
  fetchConditionHistoryStatsFromCatalogMock.mockRejectedValue(new Error("offline"));
  await expect(
    getDetailSectionPayload("condition", {
      day: "04",
      month: "10",
      year: "2026",
      keibajoCode: "A8",
      raceNumber: "05",
      raceSource: "jra",
      query: {},
    }),
  ).rejects.toThrow("offline");
  expect(getFrameStatsMock).not.toHaveBeenCalled();
});
it("does not fall back when the overseas catalog binding is absent", async () => {
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue(null);
  getRaceDetailMock.mockResolvedValue({ ...JRA_RACE, keibajoCode: "A8" });
  await expect(
    getDetailSectionPayload("condition", {
      day: "04",
      month: "10",
      year: "2026",
      keibajoCode: "A8",
      raceNumber: "05",
      raceSource: "jra",
      query: {},
    }),
  ).rejects.toThrow("Overseas condition catalog is unavailable");
  expect(getFrameStatsMock).not.toHaveBeenCalled();
});

import officialEvidence from "../../../lib/__fixtures__/arc-official-card.json";
import { loadOfficialCard } from "../../../lib/arc-official-card.server";
it("feeds all sixteen official histories through the real pace section loader", async () => {
  vi.mocked(loadOfficialCard)
    .mockResolvedValueOnce(officialEvidence.card)
    .mockResolvedValueOnce(officialEvidence.card);
  getRaceDetailMock.mockResolvedValue({
    ...JRA_RACE,
    kaisaiNen: "2026",
    kaisaiTsukihi: "1004",
    keibajoCode: "A8",
    raceBango: "05",
    kyori: "2400",
    trackCode: "17",
  });
  getRaceRunnersMock.mockResolvedValue(officialEvidence.runners);
  getHorseRaceResultsMock.mockResolvedValue(
    officialEvidence.results.map((r) =>
      r.currentUmaban === "04" || r.currentUmaban === "05"
        ? r
        : Object.assign({}, r, {
            corner1: null,
            corner2: null,
            corner3: null,
            corner4: null,
            sohaTime: null,
          }),
    ),
  );
  const payload = await getDetailSectionPayload("pace-prediction", {
    raceSource: "jra",
    year: "2026",
    month: "10",
    day: "04",
    keibajoCode: "A8",
    raceNumber: "05",
    query: {},
  });
  expect(payload).toMatchObject({ type: "pace-prediction", supported: true });
  if (!payload || !("rows" in payload) || !Array.isArray(payload.rows))
    throw new Error("Pace payload missing");
  expect(payload.rows).toHaveLength(16);
  expect(
    payload.rows.filter(
      (row) =>
        typeof row === "object" &&
        row !== null &&
        "corner1" in row &&
        row.corner1 !== null &&
        "corner2" in row &&
        row.corner2 !== null &&
        "corner3" in row &&
        row.corner3 !== null &&
        "corner4" in row &&
        row.corner4 !== null,
    ),
  ).toHaveLength(16);
});

it.each([false, true])(
  "loads fresh Arc counts without unrelated work or stale section cache (%s)",
  async (stale) => {
    getHorseRaceResultsMock.mockResolvedValue([]);
    fetchHorseRaceResultsFromCatalogMock.mockResolvedValue([]);
    getCachedDetailSectionResponseMock.mockResolvedValue(
      stale
        ? Response.json({ type: "condition", frameStats: [], mappingRevision: "gate-v1-old" })
        : null,
    );
    getPayoutStatsMock.mockRejectedValue(new Error("Unrelated payout computation must not run"));
    getRaceTimeStatsMock.mockRejectedValue(new Error("Unrelated time computation must not run"));
    fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
      frameStats: [],
      weightClassStats: [],
      carriedWeightClassStats: [],
      finishPositionStats: [],
      raceTimeStats: { targetRaces: [] },
      mappingRevision: "gate-v1-test",
    });
    getRaceDetailMock.mockResolvedValue({ ...JRA_RACE, keibajoCode: "A8" });
    getRaceRunnersMock.mockResolvedValue([]);
    fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
      arcQualifiedStats: {
        rows: [],
        coverage: { complete: false, rawRows: 0, invalidHorseRows: 0 },
      },

      mappingRevision: "gate-v1-test",
      bloodlineRows: [],
      similarRows: [],
    });
    await getDetailSectionPayload("win-rate-heatmap", {
      year: "2026",
      month: "10",
      day: "04",
      keibajoCode: "A8",
      raceNumber: "05",
      raceSource: "jra",
      query: {},
    });
    expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0]).toMatchObject({
      includeForeignSourceStats: true,
      includeSurface: true,
      years: 10,
      includeVenue: true,
      includeRaceTitle: true,
    });
    expect(getPayoutStatsMock).not.toHaveBeenCalled();
    expect(getRaceTimeStatsMock).not.toHaveBeenCalled();
    expect(getCachedDetailSectionResponseMock).not.toHaveBeenCalled();
  },
);

it("rejects missing fresh Arc condition counts instead of reusing stale counts", async () => {
  getHorseRaceResultsMock.mockResolvedValue([]);
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue(null);
  getRaceDetailMock.mockResolvedValue({ ...JRA_RACE, keibajoCode: "A8" });
  getRaceRunnersMock.mockResolvedValue([]);
  getCachedDetailSectionResponseMock.mockResolvedValue(
    Response.json({ type: "condition", frameStats: [], mappingRevision: "gate-v1-old" }),
  );
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    arcQualifiedStats: { rows: [], coverage: { complete: false, rawRows: 0, invalidHorseRows: 0 } },
    mappingRevision: "gate-v1-test",
    bloodlineRows: [],
    similarRows: [],
  });
  await expect(
    getDetailSectionPayload("win-rate-heatmap", {
      year: "2026",
      month: "10",
      day: "04",
      keibajoCode: "A8",
      raceNumber: "05",
      raceSource: "jra",
      query: {},
    }),
  ).rejects.toThrow("Heatmap condition is unavailable");
  expect(getPayoutStatsMock).not.toHaveBeenCalled();
  expect(getCachedDetailSectionResponseMock).not.toHaveBeenCalled();
});

it("preserves actual Arc same-title zero wins without another query or broader bloodline cache", async () => {
  getHorseRaceResultsMock.mockResolvedValue([
    {
      currentUmaban: "01",
      kaisaiNen: "2025",
      kaisaiTsukihi: "1005",
      keibajoCode: "A8",
      kyosomeiHondai: "凱旋門賞",
      kettoTorokuBango: "2020100001",
      raceBango: "05",
      kakuteiChakujun: "04",
    },
  ]);
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
    frameStats: [],
    weightClassStats: [],
    carriedWeightClassStats: [],
    finishPositionStats: [],
    raceTimeStats: { targetRaces: [{}] },
    mappingRevision: "gate-v1-test",
  });
  getRaceDetailMock.mockResolvedValue({
    ...JRA_RACE,
    kaisaiNen: "2026",
    keibajoCode: "A8",
    raceBango: "05",
    kaisaiTsukihi: "1004",
    kyori: "2400",
    kyosomeiHondai: "凱旋門賞",
  });
  getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, sireName: "Sire" }]);
  getCachedDetailSectionResponseMock.mockImplementation(async (key: string) =>
    key.includes("bloodline")
      ? Response.json({
          type: "bloodline",
          rows: [{ category: "sire", name: "Sire", starts: 100, winCount: 20, winRate: 20 }],
        })
      : null,
  );
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
    arcQualifiedStats: { rows: [], coverage: { complete: false, rawRows: 0, invalidHorseRows: 0 } },

    mappingRevision: "gate-v1-test",
    bloodlineRows: [{ category: "sire", name: "Sire", starts: 10, winCount: 0, winRate: 0 }],
    similarRows: [{ category: "jockey", starts: 10, winCount: 0, winRate: 0 }],
  });
  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    year: "2026",
    month: "10",
    day: "04",
    keibajoCode: "A8",
    raceNumber: "05",
    raceSource: "jra",
    query: {},
  });
  expect(payload).toMatchObject({
    arcCohort: "arc-20261004-jra-A8-05-v2",
    horseResults: [
      expect.objectContaining({ source: "jra", kyosomeiHondai: "凱旋門賞", kaisaiNen: "2025" }),
    ],
    bloodlineRows: [{ category: "sire", name: "Sire", starts: 10, winCount: 0, winRate: 0 }],
    similarRows: [{ category: "jockey", starts: 10, winCount: 0, winRate: 0 }],
  });
  expect(getHorseRaceResultsMock).toHaveBeenCalledWith(
    "jra",
    "2026",
    "10",
    "04",
    "A8",
    "05",
    "jra",
  );
  expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledOnce();
  expect(getBloodlineStatsMock).not.toHaveBeenCalled();
  expect(
    getCachedDetailSectionResponseMock.mock.calls.filter(([key]) => key.includes("bloodline")),
  ).toStrictEqual([]);
  expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0]).toMatchObject({
    years: 10,
    includeVenue: true,
    includeRaceTitle: true,
  });
});

it.each([{}, { statsYears: "3", analysisCellKeibajo: "0", statsRaceTitle: "1" }])(
  "keeps actual Arc empty rows unknown with the requested cohort %j",
  async (query) => {
    getHorseRaceResultsMock.mockResolvedValue([]);
    fetchHorseRaceResultsFromCatalogMock.mockResolvedValue([]);
    fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
      frameStats: [],
      weightClassStats: [],
      carriedWeightClassStats: [],
      finishPositionStats: [],
      raceTimeStats: { targetRaces: [{}] },
      mappingRevision: "gate-v1-test",
    });
    getRaceDetailMock.mockResolvedValue({
      ...JRA_RACE,
      kaisaiNen: "2026",
      keibajoCode: "A8",
      raceBango: "05",
      kaisaiTsukihi: "1004",
      kyori: "2400",
      kyosomeiHondai: "凱旋門賞",
    });
    getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, sireName: "Sire" }]);
    getCachedDetailSectionResponseMock.mockImplementation(async (key: string) =>
      key.includes("bloodline")
        ? Response.json({
            type: "bloodline",
            rows: [{ category: "sire", name: "Sire", starts: 100, winCount: 20, winRate: 20 }],
          })
        : null,
    );
    fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
      arcQualifiedStats: {
        rows: [],
        coverage: { complete: false, rawRows: 0, invalidHorseRows: 0 },
      },

      mappingRevision: "gate-v1-test",
      bloodlineRows: [],
      similarRows: [],
    });
    const payload = await getDetailSectionPayload("win-rate-heatmap", {
      year: "2026",
      month: "10",
      day: "04",
      keibajoCode: "A8",
      raceNumber: "05",
      raceSource: "jra",
      query,
    });
    expect(payload).toMatchObject({ bloodlineRows: [], similarRows: [] });
    expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledOnce();
    expect(getBloodlineStatsMock).not.toHaveBeenCalled();
    expect(
      getCachedDetailSectionResponseMock.mock.calls.filter(([key]) => key.includes("bloodline")),
    ).toStrictEqual([]);
    expect(fetchWinRateHeatmapStatsFromCatalogMock.mock.calls[0]?.[0]).toMatchObject({
      years: query.statsYears === "3" ? 3 : 10,
      includeVenue: query.analysisCellKeibajo !== "0",
      includeRaceTitle: true,
    });
  },
);

it.each(["unavailable", "error", "null"])(
  "fails closed for actual Arc catalog %s without a bloodline fallback",
  async (failure) => {
    getHorseRaceResultsMock.mockResolvedValue([]);
    fetchHorseRaceResultsFromCatalogMock.mockResolvedValue([]);
    fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
      frameStats: [],
      weightClassStats: [],
      carriedWeightClassStats: [],
      finishPositionStats: [],
      raceTimeStats: { targetRaces: [{}] },
      mappingRevision: "gate-v1-test",
    });
    getRaceDetailMock.mockResolvedValue({
      ...JRA_RACE,
      kaisaiNen: "2026",
      keibajoCode: "A8",
      raceBango: "05",
      kaisaiTsukihi: "1004",
      kyori: "2400",
      kyosomeiHondai: "凱旋門賞",
    });
    getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, sireName: "Sire" }]);
    getCachedDetailSectionResponseMock.mockImplementation(async (key: string) =>
      key.includes("bloodline")
        ? Response.json({
            type: "bloodline",
            rows: [{ category: "sire", name: "Sire", starts: 100, winCount: 20, winRate: 20 }],
          })
        : null,
    );
    if (failure === "error")
      fetchWinRateHeatmapStatsFromCatalogMock.mockRejectedValue(new Error("Arc query failed"));
    else
      fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue(
        failure === "null"
          ? null
          : {
              mappingRevision: "gate-v1-test",
              bloodlineRows: [],
              similarRows: [],
              bloodlineUnavailable: true,
            },
      );
    await expect(
      getDetailSectionPayload("win-rate-heatmap", {
        year: "2026",
        month: "10",
        day: "04",
        keibajoCode: "A8",
        raceNumber: "05",
        raceSource: "jra",
        query: {},
      }),
    ).rejects.toThrow(
      {
        unavailable: "Arc heatmap bloodline stats are unavailable",
        error: "Arc query failed",
        null: "Heatmap catalog is unavailable",
      }[failure],
    );
    expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledOnce();
    expect(getBloodlineStatsMock).not.toHaveBeenCalled();
    expect(
      getCachedDetailSectionResponseMock.mock.calls.filter(([key]) => key.includes("bloodline")),
    ).toStrictEqual([]);
  },
);

it.each<Partial<DetailSectionParams>>([
  { year: "2025" },
  { month: "09" },
  { day: "03" },
  { keibajoCode: "A7" },
  { raceNumber: "04" },
  { raceSource: "nar" },
])(
  "keeps existing bloodline cache fallback outside the exact Arc identity %j",
  async (override) => {
    const params: DetailSectionParams = {
      year: "2026",
      month: "10",
      day: "04",
      keibajoCode: "A8",
      raceNumber: "05",
      raceSource: "jra",
      query: {},
      ...override,
    };
    getHorseRaceResultsMock.mockResolvedValue([]);
    fetchHorseRaceResultsFromCatalogMock.mockResolvedValue([]);
    fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
      frameStats: [],
      weightClassStats: [],
      carriedWeightClassStats: [],
      finishPositionStats: [],
      raceTimeStats: { targetRaces: [{}] },
      mappingRevision: "gate-v1-test",
    });
    getRaceDetailMock.mockResolvedValue({
      ...JRA_RACE,
      kaisaiNen: "2026",
      keibajoCode: "A8",
      raceBango: "05",
      kaisaiTsukihi: "1004",
      kyori: "2400",
      kyosomeiHondai: "凱旋門賞",
      source: params.raceSource,
    });
    getRaceRunnersMock.mockResolvedValue([{ ...OVERSEAS_RUNNER, sireName: "Sire" }]);
    getCachedDetailSectionResponseMock.mockImplementation(async (key: string) =>
      key.includes("bloodline")
        ? Response.json({
            type: "bloodline",
            rows: [{ category: "sire", name: "Sire", starts: 100, winCount: 20, winRate: 20 }],
          })
        : null,
    );

    fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({
      arcQualifiedStats: {
        rows: [],
        coverage: { complete: false, rawRows: 0, invalidHorseRows: 0 },
      },

      mappingRevision: "gate-v1-test",
      bloodlineRows: [],
      similarRows: [{ category: "jockey", starts: 10, winCount: 1 }],
    });
    const payload = await getDetailSectionPayload("win-rate-heatmap", params);
    expect(payload).toMatchObject({
      bloodlineRows: [{ category: "sire", name: "Sire", starts: 100, winCount: 20, winRate: 20 }],
    });
    expect(fetchWinRateHeatmapStatsFromCatalogMock).toHaveBeenCalledOnce();
    expect(
      getCachedDetailSectionResponseMock.mock.calls.filter(([key]) => key.includes("bloodline")),
    ).toHaveLength(1);
  },
);

it.each(["all", "jra", "nar"])(
  "loads qualified Arc history despite partial nonnull Catalog (%s)",
  async (sourceScope) => {
    getRaceDetailMock.mockResolvedValue({
      ...JRA_RACE,
      kaisaiNen: "2026",
      kaisaiTsukihi: "1004",
      keibajoCode: "A8",
      raceBango: "05",
      kyori: "2400",
      trackCode: "17",
    });
    getRaceRunnersMock.mockResolvedValue(officialEvidence.runners);
    fetchHorseRaceResultsFromCatalogMock.mockResolvedValue(
      officialEvidence.results.filter(
        (row) => row.currentUmaban === "04" || row.currentUmaban === "05",
      ),
    );
    getHorseRaceResultsMock.mockResolvedValue(officialEvidence.results);
    vi.mocked(loadOfficialCard).mockResolvedValue(officialEvidence.card);
    const payload = await getDetailSectionPayload("results", {
      raceSource: "jra",
      year: "2026",
      month: "10",
      day: "04",
      keibajoCode: "A8",
      raceNumber: "05",
      query: { resultsSourceScope: sourceScope },
    });
    if (!payload || !("results" in payload) || !payload.results) throw new Error("Missing results");
    expect(getHorseRaceResultsMock).toHaveBeenCalledWith(
      "jra",
      "2026",
      "10",
      "04",
      "A8",
      "05",
      sourceScope,
    );
    expect(fetchHorseRaceResultsFromCatalogMock).not.toHaveBeenCalled();
    expect(new Set(payload.results.map((row) => row.currentUmaban)).size).toBe(16);
    expect(new Set(payload.results.map((row) => row.kettoTorokuBango)).size).toBe(16);
    expect(payload.results).toHaveLength(173);
    expect(
      new Set(
        payload.results.map((row) =>
          [
            row.currentUmaban,
            row.kaisaiNen,
            row.kaisaiTsukihi,
            row.keibajoCode,
            row.raceBango,
            row.kettoTorokuBango,
          ].join("|"),
        ),
      ).size,
    ).toBe(173);
    expect(payload.results.every((row) => row.kaisaiNen + row.kaisaiTsukihi < "20261004")).toBe(
      true,
    );
    expect(
      payload.results.filter((row) => row.currentUmaban === "04" || row.currentUmaban === "05"),
    ).toMatchObject(
      officialEvidence.results.filter(
        (row) => row.currentUmaban === "04" || row.currentUmaban === "05",
      ),
    );
  },
);

it("keeps non-Arc overseas results on the Catalog path", async () => {
  getRaceRunnersMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue({ ...JRA_RACE, keibajoCode: "A8" });
  fetchHorseRaceResultsFromCatalogMock.mockResolvedValue([{ umaban: "07" }]);
  const payload = await getDetailSectionPayload("results", {
    raceSource: "jra",
    year: "2026",
    month: "10",
    day: "04",
    keibajoCode: "A8",
    raceNumber: "06",
    query: {},
  });
  expect(payload).toMatchObject({ results: [{ umaban: "07" }] });
  expect(getHorseRaceResultsMock).not.toHaveBeenCalled();
});

it("reads heatmap condition counts from the Catalog only in production", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({ bloodlineRows: [], similarRows: [] });
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
    ...EMPTY_CATALOG_CONDITION,
    carriedWeightClassStats: [{ key: "55.5-57" }],
    frameStats: [{ count: 3, details: [], frameNumber: "2" }],
    mappingRevision: "gate-v1-abc",
    weightChangeClassStats: [{ key: "minus2" }],
    weightClassStats: [{ key: "480-499" }],
  });

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "17",
    keibajoCode: "08",
    month: "10",
    query: {},
    raceNumber: "05",
    raceSource: "jra",
    year: "2026",
  });

  expect(payload).toMatchObject({
    carriedWeightClassStats: [{ key: "55.5-57" }],
    frameStats: [{ count: 3, details: [], frameNumber: "2" }],
    mappingRevision: "gate-v1-abc",
    weightChangeClassStats: [{ key: "minus2" }],
    weightClassStats: [{ key: "480-499" }],
  });
  expect(fetchConditionHistoryStatsFromCatalogMock).toHaveBeenCalledTimes(1);
  expect(getPayoutStatsMock).not.toHaveBeenCalled();
  expect(getRaceTimeStatsMock).not.toHaveBeenCalled();
  expect(getFrameStatsMock).not.toHaveBeenCalled();
  expect(getWeightClassStatsMock).not.toHaveBeenCalled();
});

it("relaxes the Catalog condition query when the heatmap frame counts are empty", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({ bloodlineRows: [], similarRows: [] });
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchConditionHistoryStatsFromCatalogMock
    .mockResolvedValueOnce(EMPTY_CATALOG_CONDITION)
    .mockResolvedValueOnce({
      ...EMPTY_CATALOG_CONDITION,
      frameStats: [{ count: 9, details: [], frameNumber: "1" }],
    });

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "17",
    keibajoCode: "08",
    month: "10",
    query: {},
    raceNumber: "05",
    raceSource: "jra",
    year: "2026",
  });

  expect(payload).toMatchObject({ frameStats: [{ count: 9, details: [], frameNumber: "1" }] });
  expect(fetchConditionHistoryStatsFromCatalogMock).toHaveBeenCalledTimes(2);
  expect(fetchConditionHistoryStatsFromCatalogMock).toHaveBeenNthCalledWith(
    2,
    expect.objectContaining({ includeVenue: false }),
  );
  expect(getFrameStatsMock).not.toHaveBeenCalled();
});

it("keeps the empty heatmap frame counts when the relaxed Catalog query also fails", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({ bloodlineRows: [], similarRows: [] });
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchConditionHistoryStatsFromCatalogMock
    .mockResolvedValueOnce(EMPTY_CATALOG_CONDITION)
    .mockRejectedValueOnce(new Error("catalog timeout"));

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "17",
    keibajoCode: "08",
    month: "10",
    query: {},
    raceNumber: "05",
    raceSource: "jra",
    year: "2026",
  });

  expect(payload).toMatchObject({ frameStats: [] });
  expect(getFrameStatsMock).not.toHaveBeenCalled();
});

it("fails the production heatmap instead of loading Neon when the condition Catalog is unavailable", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({ bloodlineRows: [], similarRows: [] });
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(JRA_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue(null);

  await expect(
    getDetailSectionPayload("win-rate-heatmap", {
      day: "17",
      keibajoCode: "08",
      month: "10",
      query: {},
      raceNumber: "05",
      raceSource: "jra",
      year: "2026",
    }),
  ).rejects.toThrow("Heatmap condition catalog is unavailable");
  expect(getFrameStatsMock).not.toHaveBeenCalled();
  expect(getPayoutStatsMock).not.toHaveBeenCalled();
});

it("fills ban-ei heatmap weight classes from the database while the Catalog has none", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({ bloodlineRows: [], similarRows: [] });
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(BAN_EI_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  getWeightClassStatsMock.mockResolvedValue([{ key: "900-949" }]);
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
    ...EMPTY_CATALOG_CONDITION,
    carriedWeightClassStats: [{ key: "ignored" }],
    frameStats: [{ count: 2, details: [], frameNumber: "1" }],
  });

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "17",
    keibajoCode: "83",
    month: "10",
    query: {},
    raceNumber: "05",
    raceSource: "nar",
    year: "2026",
  });

  expect(payload).toMatchObject({
    carriedWeightClassStats: [],
    weightClassStats: [{ key: "900-949" }],
  });
  expect(getWeightClassStatsMock).toHaveBeenCalledTimes(1);
});

it("keeps Catalog ban-ei heatmap weight classes when they are present", async () => {
  fetchWinRateHeatmapStatsFromCatalogMock.mockResolvedValue({ bloodlineRows: [], similarRows: [] });
  getHorseRaceResultsMock.mockResolvedValue([]);
  getRaceDetailMock.mockResolvedValue(BAN_EI_RACE);
  getRaceRunnersMock.mockResolvedValue([OVERSEAS_RUNNER]);
  fetchConditionHistoryStatsFromCatalogMock.mockResolvedValue({
    ...EMPTY_CATALOG_CONDITION,
    frameStats: [{ count: 2, details: [], frameNumber: "1" }],
    weightClassStats: [{ key: "catalog-900" }],
  });

  const payload = await getDetailSectionPayload("win-rate-heatmap", {
    day: "17",
    keibajoCode: "83",
    month: "10",
    query: {},
    raceNumber: "05",
    raceSource: "nar",
    year: "2026",
  });

  expect(payload).toMatchObject({ weightClassStats: [{ key: "catalog-900" }] });
  expect(getWeightClassStatsMock).not.toHaveBeenCalled();
});
