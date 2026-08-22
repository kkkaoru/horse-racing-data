import "server-only";
import { getDatabaseTarget } from "../../../db/client";
import {
  getBloodlineStats,
  getActiveFinishPositionPredictions,
  getActiveFinishPredictionEvaluation,
  getFinishPositionBucketEvaluation,
  getFinishPositionSimilarityFeatures,
  getFinishPositionStats,
  getFrameStats,
  getHorseRaceResults,
  getPayoutStats,
  getRaceAbilityTests,
  getRaceDetail,
  getRacePaceModelPredictionFeatures,
  getRacePaceSimilarityFeatures,
  getRaceRunners,
  getRaceTrainings,
  getRunningStyleBucketEvaluation,
  getSimilarRaceStats,
  getTimeScoreRows,
  getCarriedWeightClassStats,
  getWeightClassStats,
} from "../../../db/queries";
import { SOURCE_LABELS, type RaceSource } from "../../../lib/codes";
import { fetchConditionHistoryStatsFromCatalog } from "../../../lib/condition-history-catalog.server";
import type { FinishPredictionBuildInputs } from "../../../lib/finish-position-prediction";
import {
  type FinishPredictionEvaluationMetrics,
  FINISH_POSITION_PREDICTION_EVALUATIONS,
  getFinishPredictionEvaluation,
  getFinishPredictionEvaluationCategory,
} from "../../../lib/finish-position-prediction-evaluation";
import {
  buildFinishPositionBucketFilter,
  buildFinishPositionBucketTiers,
  type FinishPositionBucketMetrics,
  type FinishPositionBucketScope,
  type FinishPredictionDimensionFlags,
  getFinishPredictionDimensionFlags,
  resolveFinishPositionBucketModelVersion,
} from "../../../lib/finish-prediction-dimensions";
import {
  cleanText,
  formatDistance,
  formatKeibajo,
  formatRaceNumber,
  getTrackSurfaceLabel,
  getTrackTurnLabel,
} from "../../../lib/format";
import { fetchHorseRaceResultsFromCatalog } from "../../../lib/horse-race-results-catalog.server";
import { buildNetkeibaRaceId, parseNetkeibaTrainingReviews } from "../../../lib/netkeiba-training";
import {
  ANALYSIS_CELL_PARAM_NAMES,
  buildCellMatchingStatsSettings,
  withDisabledCellDimensions,
} from "../../../lib/past-race-cell-matching";
import { getPremiumDataTopHorsesWithCache } from "../../../lib/premium-data-top-cache.server";
import {
  getAgeLabel,
  getConditionLabel,
  getGradeLabel,
  getRaceSymbolLabel,
  getRaceTags,
  getWeightLabel,
} from "../../../lib/race-classification";
import { buildDetailSectionCacheKey } from "../../../lib/race-detail-section-cache";
import { getCachedDetailSectionResponse } from "../../../lib/race-detail-section-cache.server";
import {
  applyRunningStyleSortToRacePaceRows,
  buildRacePacePredictionRowsFromResults,
  isCornerPacePredictionSupported,
} from "../../../lib/race-pace-prediction";
import { getOrComputeRaceTimeStats } from "../../../lib/race-time-stats-cache.server";
import { fetchRaceTrainingsFromCatalog } from "../../../lib/race-training-catalog.server";
import type {
  BloodlineStatsRow,
  FinishPositionStatsRow,
  FrameStatsRow,
  HorseRaceResult,
  OverallScoreDetail,
  OverallScoreRow,
  PayoutStatsRow,
  RaceDetail,
  RaceTimeStats,
  Runner,
  SameDayVenueJockeyWinFeature,
  SimilarRaceStatsRow,
  StableComment,
  SimilarRaceStatsSettings,
  TimeScoreRow,
  Training,
  PremiumDataTopHorse,
  WeightClassStatsRow,
} from "../../../lib/race-types";
import { getRunnerDisplayNames } from "../../../lib/runner-display";
import { isOverseasKeibajoCode } from "../../../lib/runner-format";
import { formatRunnerNumber, isBanEiKeibajoCode } from "../../../lib/runner-format";
import { getRaceRunningStylesWithCache } from "../../../lib/running-style-cache.server";
import {
  buildRunningStyleBucketFilter,
  getRunningStyleDimensionFlags,
  type RaceRowForRunningStyleBucketFilter,
  type RunningStyleBucketFilter,
  type RunningStyleBucketMetrics,
  type RunningStyleBucketScope,
  type RunningStyleDimensionFlags,
} from "../../../lib/running-style-prediction-dimensions";
import {
  fetchWinRateHeatmapStatsFromCatalog,
  groupCatalogBloodlineRows,
  groupCatalogSimilarRows,
  type WinRateHeatmapCatalogQuery,
  type WinRateHeatmapCatalogStats,
} from "../../../lib/win-rate-heatmap-catalog.server";
import type { FinishPositionBucketRace } from "./finish-position-bucket-section";
import { mergePremiumTrainingReviews, type PremiumTrainingReview } from "./premium-training-merge";

export type DetailSection =
  | "ability"
  | "bloodline"
  | "condition"
  | "finish-prediction"
  | "overall-score"
  | "pace-prediction"
  | "premium-data-top"
  | "results"
  | "running-style"
  | "similar"
  | "time-score"
  | "training"
  | "win-rate-heatmap";

export interface RunningStyleBucketSectionPayload {
  bucketEvaluation: RunningStyleBucketMetrics | null;
  bucketFilter: RunningStyleBucketFilter | null;
  bucketScope: RunningStyleBucketScope | null;
  dimensionFlags: RunningStyleDimensionFlags;
  type: "running-style";
}

export interface RunningStyleBucketSectionData {
  bucketEvaluation: RunningStyleBucketMetrics | null;
  bucketScope: RunningStyleBucketScope | null;
  bucketRace: RaceRowForRunningStyleBucketFilter | null;
  bucketSource: "jra" | "nar" | null;
  bucketGradeCode: string | null;
  dimensionFlags: RunningStyleDimensionFlags | null;
}

export interface FinishPositionBucketSectionData {
  bucketEvaluation: FinishPositionBucketMetrics | null;
  bucketScope: FinishPositionBucketScope | null;
  bucketRace: FinishPositionBucketRace | null;
  bucketSource: "jra" | "nar" | null;
  bucketGradeCode: string | null;
  bucketModelVersion: string | null;
}

interface FinishPositionBucketTier {
  level: FinishPositionBucketScope["level"];
  flags: FinishPredictionDimensionFlags;
}

interface FinishPositionBucketResolution {
  bucketEvaluation: FinishPositionBucketMetrics | null;
  bucketScope: FinishPositionBucketScope | null;
}

interface ResolveFinishPositionBucketInput {
  race: FinishPositionBucketRace;
  query: Record<string, string | string[] | undefined>;
  modelVersion: string;
  tiers: readonly FinishPositionBucketTier[];
}

export interface DetailSectionParams {
  day: string;
  keibajoCode: string;
  month: string;
  query: Record<string, string | string[] | undefined>;
  raceNumber: string;
  raceSource: RaceSource;
  year: string;
}

interface RunningStyleBucketTier {
  level: RunningStyleBucketScope["level"];
  flags: RunningStyleDimensionFlags;
}

interface RunningStyleBucketResolution {
  bucketEvaluation: RunningStyleBucketMetrics | null;
  bucketFilter: RunningStyleBucketFilter | null;
  bucketScope: RunningStyleBucketScope | null;
}

interface ResolveRunningStyleBucketInput {
  race: RaceRowForRunningStyleBucketFilter;
  query: Record<string, string | string[] | undefined>;
  tiers: readonly RunningStyleBucketTier[];
}

interface CachedTimeScorePayload {
  correlationRows: RaceTimeStats["correlationRows"];
  rows: TimeScoreRow[];
}

const LISTED_OR_HIGHER_GRADE_CODES = new Set(["A", "B", "C", "D", "F", "G", "H", "L", "S"]);

const RUNNING_STYLE_KEIBAJO_ONLY_FLAGS: RunningStyleDimensionFlags = {
  condition: false,
  distance: false,
  grade: false,
  keibajo: true,
  kyosoJoken: false,
  kyosoShubetsu: false,
  raceName: false,
  track: false,
};

const RUNNING_STYLE_CATEGORY_ONLY_FLAGS: RunningStyleDimensionFlags = {
  condition: false,
  distance: false,
  grade: false,
  keibajo: false,
  kyosoJoken: false,
  kyosoShubetsu: false,
  raceName: false,
  track: false,
};

const CONDITION_ANALYSIS_RELAX_KEYS = [
  "includeRaceTitle",
  "includeRaceSubtitle",
  "includeAge",
  "includeClass",
  "includeSex",
  "includeWeight",
  "includeSurface",
  "includeTurn",
  "includeDistance",
  "includeRunnerCount",
  "includeFrame",
  "includeRaceNumber",
  "includeMonthWindow",
] as const;

const RATE_STATS_FALLBACK_TIMEOUT_MS = 6_000;
const OVERSEAS_BLOODLINE_MINIMUM_STARTS = 20;
const OVERSEAS_SIMILAR_STATS_MINIMUM_STARTS = 20;
const HEATMAP_STATS_YEARS_FALLBACK = 10;

type ConditionAnalysisStats = [
  RaceTimeStats,
  PayoutStatsRow[],
  FinishPositionStatsRow[],
  FrameStatsRow[],
];

type ConditionAnalysisCandidateMatch<T extends SimilarRaceStatsSettings> = {
  settings: T;
  stats: ConditionAnalysisStats;
};

const OVERALL_SCORE_WEIGHTS = {
  bloodline: 0.2,
  correlation: 0.2,
  jockey: 0.1,
  owner: 0.1,
  time: 0.3,
  trainer: 0.1,
};

const isSameDayVenueJockeyWinsPayload = (
  value: unknown,
): value is { jockeyWins: SameDayVenueJockeyWinFeature[] } => {
  if (typeof value !== "object" || value === null || !("jockeyWins" in value)) {
    return false;
  }
  const jockeyWins = value.jockeyWins;
  return (
    Array.isArray(jockeyWins) &&
    jockeyWins.every((row) => {
      if (typeof row !== "object" || row === null) {
        return false;
      }
      return (
        "jockeyName" in row &&
        "latestRaceNumber" in row &&
        "winCount" in row &&
        typeof row.jockeyName === "string" &&
        typeof row.latestRaceNumber === "string" &&
        typeof row.winCount === "number"
      );
    })
  );
};

const getRealtimeApiBaseUrl = (): string =>
  process.env.NEXT_PUBLIC_REALTIME_DATA_API_BASE_URL ?? "https://sync-realtime-data.kkk4oru.com";

const isPremiumTrainingReview = (value: unknown): value is PremiumTrainingReview => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return (
    "horseNumber" in value &&
    "trainingDate" in value &&
    typeof value.horseNumber === "string" &&
    typeof value.trainingDate === "string"
  );
};

const isStableComment = (value: unknown): value is StableComment => {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return (
    "commentText" in value &&
    "fetchedAt" in value &&
    "horseNumber" in value &&
    typeof value.commentText === "string" &&
    (!("evaluationGrade" in value) ||
      value.evaluationGrade === null ||
      typeof value.evaluationGrade === "number") &&
    typeof value.fetchedAt === "string" &&
    typeof value.horseNumber === "string"
  );
};

const fetchPremiumRacePayload = async (
  race: RaceDetail,
): Promise<{
  stableComments: StableComment[];
  trainingReviews: PremiumTrainingReview[];
}> => {
  if (race.source !== "jra") {
    return { stableComments: [], trainingReviews: [] };
  }
  const url = `${getRealtimeApiBaseUrl().replace(/\/$/u, "")}/api/jra/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(0, 2)}/${race.kaisaiTsukihi.slice(2, 4)}/${race.keibajoCode}/${race.raceBango}/premium`;
  try {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      return { stableComments: [], trainingReviews: await fetchNetkeibaTrainingReviews(race) };
    }
    const data: unknown = await response.json();
    if (typeof data !== "object" || data === null) {
      return { stableComments: [], trainingReviews: await fetchNetkeibaTrainingReviews(race) };
    }
    const trainingReviews = "trainingReviews" in data ? data.trainingReviews : [];
    const stableComments = "stableComments" in data ? data.stableComments : [];
    const payload = {
      stableComments: Array.isArray(stableComments) ? stableComments.filter(isStableComment) : [],
      trainingReviews: Array.isArray(trainingReviews)
        ? trainingReviews.filter(isPremiumTrainingReview)
        : [],
    };
    if (payload.trainingReviews.length > 0) {
      return payload;
    }
    return {
      ...payload,
      trainingReviews: await fetchNetkeibaTrainingReviews(race),
    };
  } catch {
    return { stableComments: [], trainingReviews: await fetchNetkeibaTrainingReviews(race) };
  }
};

const getRaceTrainingsWithCatalogFallback = async (
  source: RaceSource,
  year: string,
  month: string,
  day: string,
  keibajoCode: string,
  raceNumber: string,
): Promise<Training[]> => {
  if (source === "jra" && getDatabaseTarget() === "cloudflare") {
    const catalogRows = await fetchRaceTrainingsFromCatalog({
      day,
      keibajoCode,
      month,
      raceBango: raceNumber,
      year,
    });
    if (catalogRows && catalogRows.length > 0) {
      return catalogRows;
    }
  }
  return getRaceTrainings(source, year, month, day, keibajoCode, raceNumber);
};

const fetchNetkeibaTrainingReviews = async (race: RaceDetail): Promise<PremiumTrainingReview[]> => {
  const raceId = buildNetkeibaRaceId({
    kaisaiKai: race.kaisaiKai,
    kaisaiNen: race.kaisaiNen,
    kaisaiNichime: race.kaisaiNichime,
    keibajoCode: race.keibajoCode,
    raceBango: race.raceBango,
    source: race.source,
  });
  if (!raceId) {
    return [];
  }
  try {
    const response = await fetch(`https://race.netkeiba.com/race/oikiri.html?race_id=${raceId}`, {
      cache: "no-store",
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
      },
    });
    if (!response.ok) {
      return [];
    }
    const html = await response.text();
    return parseNetkeibaTrainingReviews(html);
  } catch {
    return [];
  }
};

const fetchSameDayVenueJockeyWins = async (
  race: RaceDetail,
): Promise<SameDayVenueJockeyWinFeature[]> => {
  if (race.source !== "nar") {
    return [];
  }
  const url = `${getRealtimeApiBaseUrl().replace(/\/$/u, "")}/api/nar/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(0, 2)}/${race.kaisaiTsukihi.slice(2, 4)}/${race.keibajoCode}/${race.raceBango}/jockey-wins`;
  try {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      return [];
    }
    const data: unknown = await response.json();
    return isSameDayVenueJockeyWinsPayload(data) ? data.jockeyWins : [];
  } catch {
    return [];
  }
};

const STORED_ODDS_EMPTY = "0000";
const STORED_POPULARITY_EMPTY = "00";
const STORED_ODDS_DECIMAL_DIVISOR = 10;

const clampScore = (value: number): number => Math.max(0, Math.min(1, value));

const roundScore = (value: number): number => Math.round(value * 100) / 100;

const parseStoredNumber = (value: string | null | undefined, emptyValue: string): number | null => {
  const cleaned = (value ?? "").trim();
  if (!cleaned || cleaned === emptyValue) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseStoredOdds = (value: string | null | undefined): number | null => {
  const parsed = parseStoredNumber(value, STORED_ODDS_EMPTY);
  return parsed === null ? null : parsed / STORED_ODDS_DECIMAL_DIVISOR;
};

const parseStoredPopularity = (value: string | null | undefined): number | null =>
  parseStoredNumber(value, STORED_POPULARITY_EMPTY);

const enrichPremiumDataTopHorses = (
  dataTopHorses: PremiumDataTopHorse[],
  runners: Runner[],
): PremiumDataTopHorse[] => {
  const runnerByHorse = new Map(
    runners.map((runner) => [formatRunnerNumber(runner.umaban ?? ""), runner]),
  );
  return dataTopHorses.map((horse) => {
    const runner = runnerByHorse.get(formatRunnerNumber(horse.horseNumber));
    return {
      ...horse,
      jockeyName: runner ? getRunnerDisplayNames(runner).jockey || null : null,
      storedOdds: parseStoredOdds(runner?.tanshoOdds),
      storedPopularity: parseStoredPopularity(runner?.tanshoNinkijun),
    };
  });
};

const splitHorseNumbers = (value: string): string[] =>
  value
    .split(",")
    .map((item) => item.trim().replace(/^0+/u, "") || item.trim())
    .filter(Boolean);

const getBloodlineScoreByHorse = (rows: BloodlineStatsRow[]): Map<string, number> => {
  const scoreTotals = new Map<string, { score: number; weight: number }>();
  const categoryWeights: Record<BloodlineStatsRow["category"], number> = {
    damDamSire: 0,
    damSire: 0.35,
    damSireSire: 0,
    sire: 0.45,
    sireDamSire: 0,
    sireSire: 0.2,
    sireSireSire: 0,
  };

  for (const row of rows) {
    const categoryWeight = categoryWeights[row.category];
    const startsScore = clampScore(row.starts / 30);
    const horseCountScore = clampScore(row.horseCount / 5);
    const score =
      clampScore(row.showRate / 100) * 0.35 +
      clampScore(row.quinellaRate / 100) * 0.25 +
      clampScore(row.winRate / 100) * 0.25 +
      startsScore * 0.1 +
      horseCountScore * 0.05;

    for (const horseNumber of splitHorseNumbers(row.currentHorseNumbers)) {
      const current = scoreTotals.get(horseNumber) ?? { score: 0, weight: 0 };
      scoreTotals.set(horseNumber, {
        score: current.score + score * categoryWeight,
        weight: current.weight + categoryWeight,
      });
    }
  }

  const scores = new Map<string, number>();
  for (const [horseNumber, total] of scoreTotals) {
    scores.set(horseNumber, total.weight > 0 ? total.score / total.weight : 0.5);
  }
  return scores;
};

const findCorrelationDetailScore = (
  row: RaceTimeStats["correlationRows"][number] | undefined,
  key: "jockeyShow" | "ownerShow" | "trainerShow",
): number => row?.details.find((detail) => detail.key === key)?.score ?? 0.5;

const buildOverallScoreRows = ({
  bloodlineRows,
  correlationRows,
  runners,
  timeRows,
}: {
  bloodlineRows: BloodlineStatsRow[];
  correlationRows: RaceTimeStats["correlationRows"];
  runners: Runner[];
  timeRows: TimeScoreRow[];
}): OverallScoreRow[] => {
  const bloodlineScores = getBloodlineScoreByHorse(bloodlineRows);
  const correlationByHorse = new Map(correlationRows.map((row) => [row.horseNumber, row]));
  const timeByHorse = new Map(timeRows.map((row) => [row.horseNumber, row]));

  return runners
    .map((runner): OverallScoreRow => {
      const rawHorseNumber = runner.umaban ?? "";
      const horseNumber = rawHorseNumber.replace(/^0+/u, "") || rawHorseNumber;
      const timeScore = timeByHorse.get(horseNumber)?.score ?? 0.5;
      const correlationRow = correlationByHorse.get(horseNumber);
      const correlationScore = correlationRow?.score ?? 0.5;
      const bloodlineScore = bloodlineScores.get(horseNumber) ?? 0.5;
      const jockeyScore = findCorrelationDetailScore(correlationRow, "jockeyShow");
      const trainerScore = findCorrelationDetailScore(correlationRow, "trainerShow");
      const ownerScore = findCorrelationDetailScore(correlationRow, "ownerShow");
      const details: OverallScoreDetail[] = [
        {
          label: "タイムスコア",
          reason: "過去競走成績と同条件1〜3着馬のタイム傾向を距離・日付・年齢で重み付けして評価",
          score: timeScore,
          weight: OVERALL_SCORE_WEIGHTS.time,
        },
        {
          label: "1〜3着相関スコア",
          reason: "同条件レースで1〜3着に入った馬の傾向との近さを評価",
          score: correlationScore,
          weight: OVERALL_SCORE_WEIGHTS.correlation,
        },
        {
          label: "血統スコア",
          reason: "父・母父・父父の同条件成績を出走馬ごとに合成して評価",
          score: bloodlineScore,
          weight: OVERALL_SCORE_WEIGHTS.bloodline,
        },
        {
          label: "騎手スコア",
          reason: "今回騎乗予定騎手の同条件傾向との相性を評価",
          score: jockeyScore,
          weight: OVERALL_SCORE_WEIGHTS.jockey,
        },
        {
          label: "調教師スコア",
          reason: "今回出走馬の調教師の同条件傾向との相性を評価",
          score: trainerScore,
          weight: OVERALL_SCORE_WEIGHTS.trainer,
        },
        {
          label: "馬主スコア",
          reason: "今回出走馬の馬主の同条件傾向との相性を評価",
          score: ownerScore,
          weight: OVERALL_SCORE_WEIGHTS.owner,
        },
      ];
      const displayNames = getRunnerDisplayNames(runner);
      return {
        details,
        horseName: displayNames.horse || "-",
        horseNumber,
        jockeyName: displayNames.jockey || "-",
        score: roundScore(
          details.reduce((total, detail) => total + detail.score * detail.weight, 0),
        ),
        storedOdds: parseStoredOdds(runner.tanshoOdds),
        storedPopularity: parseStoredPopularity(runner.tanshoNinkijun),
      };
    })
    .toSorted(
      (left, right) =>
        right.score - left.score || Number(left.horseNumber) - Number(right.horseNumber),
    );
};

const getFirstSearchParam = (value: string | string[] | undefined): string | undefined =>
  Array.isArray(value) ? value[0] : value;

const normalizeHorseNumber = (value: string | null | undefined): string =>
  cleanText(value, "").replace(/^0+/u, "") || (value ? "0" : "");

const getFlag = (value: string | string[] | undefined): boolean =>
  getFirstSearchParam(value) !== "0";

const getOptionalFlag = (value: string | string[] | undefined): boolean =>
  getFirstSearchParam(value) === "1";

const getDefaultFlag = (value: string | string[] | undefined, defaultValue: boolean): boolean => {
  const firstValue = getFirstSearchParam(value);
  if (firstValue === undefined) {
    return defaultValue;
  }
  return firstValue !== "0";
};

const getScopedStatsParamName = (prefix: string, name: string): string =>
  prefix ? `${prefix}${name.charAt(0).toUpperCase()}${name.slice(1)}` : name;

const getStatsQueryParam = (
  query: Record<string, string | string[] | undefined>,
  prefix: string,
  name: string,
): string | string[] | undefined => {
  const scopedValue = query[getScopedStatsParamName(prefix, name)];
  return scopedValue === undefined ? query[name] : scopedValue;
};

const hasExplicitStatsState = (
  query: Record<string, string | string[] | undefined>,
  prefix: string,
): boolean =>
  Object.keys(query).some((name) => {
    if (getFirstSearchParam(query[name]) === undefined) {
      return false;
    }
    return (
      name.startsWith(`${prefix}Stats`) ||
      name.startsWith("stats") ||
      (prefix === "analysis" && name.startsWith("analysisCell")) ||
      (prefix === "analysis" && name === "similarStatsVenue")
    );
  });

const getStatsSourceScope = (
  query: Record<string, string | string[] | undefined>,
  prefix: string,
): RaceSource | "all" => {
  const value = getFirstSearchParam(getStatsQueryParam(query, prefix, "statsSourceScope"));
  if (value === "jra" || value === "nar") {
    return value;
  }
  if (value === "all") {
    return "all";
  }
  return getOptionalFlag(getStatsQueryParam(query, prefix, "statsNarOnly")) ? "nar" : "all";
};

const getResultsSourceScope = (
  query: Record<string, string | string[] | undefined>,
): RaceSource | "all" => {
  const value = getFirstSearchParam(query.resultsSourceScope);
  if (value === "jra" || value === "nar") {
    return value;
  }
  return "all";
};

const getStatsYears = (
  value: string | string[] | undefined,
  defaultYears: number | null,
): number | null => {
  const firstValue = getFirstSearchParam(value);
  if (firstValue === "all" || (firstValue === undefined && defaultYears === null)) {
    return null;
  }

  const parsed = Number(firstValue ?? String(defaultYears));
  return Number.isInteger(parsed) && parsed >= 1 && parsed <= 10 ? parsed : defaultYears;
};

const cleanConditionText = (value: string | null | undefined): string =>
  cleanText(value, "").replace(/\s+/g, " ").replace(/　+/g, " ").trim();

const RACE_NAME_TOKEN_PATTERN = /[\p{L}\p{N}ー・－-]+(?:杯|賞|記念|ステークス|カップ)/gu;

const getStatsRaceNameToken = (race: RaceDetail): string | null => {
  const subtitle = `${cleanConditionText(race.kyosomeiFukudai)} ${cleanConditionText(
    race.kyosomeiKakkonai,
  )}`;
  const combined = `${cleanConditionText(race.kyosomeiHondai)} ${subtitle}`;
  if (combined.includes("ジョッキーズカップ")) {
    return "ジョッキーズカップ";
  }

  const subtitleMatch = [...subtitle.matchAll(RACE_NAME_TOKEN_PATTERN)].at(-1)?.[0] ?? "";
  if (subtitleMatch) {
    return subtitleMatch;
  }

  return (
    [...cleanConditionText(race.kyosomeiHondai).matchAll(RACE_NAME_TOKEN_PATTERN)].at(-1)?.[0] ??
    null
  );
};

const getLocalConditionLabel = (value: string | null | undefined): string => {
  const cleaned = cleanConditionText(value);
  const normalized = cleaned
    .replace(/[Ａ-Ｚａ-ｚ０-９]/g, (char) => String.fromCharCode(char.charCodeAt(0) - 0xfee0))
    .replace(/[－ー―‐]/g, "-");
  if (/OP/.test(normalized)) {
    const opMatch = cleaned.match(/[ＯO]Ｐ|OP/i);
    return opMatch?.[0] ?? "OP";
  }
  const localClass = normalized.split(" ")[0] ?? "";
  return /^[A-Z][0-9]+(?:-[0-9]+)?$/.test(localClass) ? localClass : "";
};

const getClassConditionLabel = (race: RaceDetail): string | null => {
  if (race.source === "nar" && cleanText(race.kyosoJokenCode, "") === "000") {
    return getLocalConditionLabel(race.kyosoJokenMeisho) || null;
  }
  const label = getConditionLabel(race.kyosoJokenCode);
  return label === "-" ? null : label;
};

const isListedOrHigher = (race: RaceDetail): boolean =>
  LISTED_OR_HIGHER_GRADE_CODES.has(cleanText(race.gradeCode, ""));

const getStatsClassConditionLabel = (race: RaceDetail): string | null => {
  if (race.source === "jra" && isListedOrHigher(race)) {
    const label = getGradeLabel(race.gradeCode);
    return label === "-" ? null : label;
  }
  return getClassConditionLabel(race);
};

const getRaceNameFilterLabels = (
  race: RaceDetail,
): { subtitle: string | null; title: string | null } => {
  const tags = getRaceTags(race).join(" ");
  const grade = cleanText(race.gradeCode, "");
  const condition = cleanConditionText(race.kyosoJokenMeisho);
  const title = cleanText(race.kyosomeiHondai, "");
  const subtitle = cleanText(race.kyosomeiFukudai, "") || cleanText(race.kyosomeiKakkonai, "");
  const statsRaceNameToken = getStatsRaceNameToken(race);
  const hasNamedClass =
    grade.length > 0 || /G[1-3]|Jpn[1-3]|リステッド|OP|ＯＰ|オープン/.test(`${tags} ${condition}`);
  const hasSpecialRaceName =
    title.includes("ファイナルレース") ||
    subtitle.includes("一発逆転") ||
    Boolean(statsRaceNameToken);

  if (!hasNamedClass && !hasSpecialRaceName) {
    return { subtitle: null, title: null };
  }

  if (statsRaceNameToken) {
    return {
      subtitle: null,
      title: statsRaceNameToken,
    };
  }

  return {
    subtitle: subtitle || null,
    title: title || null,
  };
};

const hasConditionAnalysisRows = (stats: ConditionAnalysisStats): boolean => {
  const [timeStats, payoutRows, finishRows, frameRows] = stats;
  return (
    timeStats.raceCount > 0 ||
    payoutRows.some((row) => row.count > 0) ||
    finishRows.some((row) => row.count > 0) ||
    frameRows.some((row) => row.count > 0)
  );
};

const hasCompleteConditionAnalysisRows = (stats: ConditionAnalysisStats): boolean => {
  const [timeStats, , finishRows, frameRows] = stats;
  return (
    timeStats.raceCount > 0 &&
    finishRows.some((row) => row.count > 0) &&
    frameRows.some((row) => row.count > 0)
  );
};

const relaxAllConditionAnalysisSettings = <T extends SimilarRaceStatsSettings>(settings: T): T => {
  if (settings.cellMatching) {
    return { ...settings, ...withDisabledCellDimensions(settings, false) };
  }
  const relaxedSettings = { ...settings };
  for (const key of CONDITION_ANALYSIS_RELAX_KEYS) {
    relaxedSettings[key] = false;
  }
  relaxedSettings.includeVenue = false;
  return relaxedSettings;
};

const getConditionAnalysisSettingCandidates = <T extends SimilarRaceStatsSettings>(
  settings: T,
): T[] => {
  if (settings.cellMatching) {
    return [
      settings,
      { ...settings, ...withDisabledCellDimensions(settings, true) },
      { ...settings, ...withDisabledCellDimensions(settings, false) },
    ];
  }
  const candidates = [settings];
  const relaxedSettings = { ...settings };

  for (const key of CONDITION_ANALYSIS_RELAX_KEYS) {
    if (!relaxedSettings[key]) {
      continue;
    }
    relaxedSettings[key] = false;
    candidates.push({ ...relaxedSettings });
  }

  return candidates;
};

const hasRateRows = (rows: readonly (BloodlineStatsRow | SimilarRaceStatsRow)[]): boolean =>
  rows.some((row) => row.starts > 0);

const getEligibleSimilarStatsRows = (
  race: RaceDetail,
  rows: SimilarRaceStatsRow[],
): SimilarRaceStatsRow[] =>
  isOverseasKeibajoCode(race.keibajoCode)
    ? rows.filter((row) => row.starts >= OVERSEAS_SIMILAR_STATS_MINIMUM_STARTS)
    : rows;

const getSimilarStatsFallbackPayload = (
  race: RaceDetail,
  settings: SimilarRaceStatsSettings,
): { similarStatsFallback: true } | Record<string, never> =>
  isOverseasKeibajoCode(race.keibajoCode) &&
  !settings.includeVenue &&
  CONDITION_ANALYSIS_RELAX_KEYS.every((key) => !settings[key])
    ? { similarStatsFallback: true }
    : {};

const getEligibleBloodlineRows = (
  race: RaceDetail,
  rows: BloodlineStatsRow[],
): BloodlineStatsRow[] =>
  isOverseasKeibajoCode(race.keibajoCode)
    ? rows.filter((row) => row.starts >= OVERSEAS_BLOODLINE_MINIMUM_STARTS)
    : rows;

const getBloodlineVenueFallbackPayload = (
  race: RaceDetail,
  settings: SimilarRaceStatsSettings,
): { bloodlineVenueFallback: true } | Record<string, never> =>
  isOverseasKeibajoCode(race.keibajoCode) && !settings.includeVenue
    ? { bloodlineVenueFallback: true }
    : {};

const hasBloodlineScoreCoverage = (
  rows: readonly BloodlineStatsRow[],
  runners: readonly Runner[],
): boolean => {
  if (runners.length === 0) {
    return hasRateRows(rows);
  }

  const startsByHorse = new Map<string, number>();
  for (const row of rows) {
    for (const horseNumber of splitHorseNumbers(row.currentHorseNumbers)) {
      startsByHorse.set(horseNumber, (startsByHorse.get(horseNumber) ?? 0) + row.starts);
    }
  }

  const coveredCount = runners.filter((runner) => {
    const rawHorseNumber = runner.umaban ?? "";
    const horseNumber = rawHorseNumber.replace(/^0+/u, "") || rawHorseNumber;
    return (startsByHorse.get(horseNumber) ?? 0) >= 1;
  }).length;
  const requiredCount = Math.ceil((runners.length * 2) / 3);
  return coveredCount >= requiredCount;
};

const getBloodlineIncompletePayload = (
  rows: readonly BloodlineStatsRow[],
  runners: readonly Runner[],
): { bloodlineStatsIncomplete: true } | Record<string, never> =>
  hasBloodlineScoreCoverage(rows, runners) ? {} : { bloodlineStatsIncomplete: true };

const hasSimilarJockeyTrainerCoverage = (
  rows: readonly SimilarRaceStatsRow[],
  runners: readonly Runner[],
): boolean => {
  if (runners.length === 0) {
    return hasRateRows(rows);
  }

  const startsByCategoryHorse = new Map<SimilarRaceStatsRow["category"], Map<string, number>>([
    ["jockey", new Map()],
    ["trainer", new Map()],
  ]);
  for (const row of rows) {
    if (row.category !== "jockey" && row.category !== "trainer") {
      continue;
    }
    const startsByHorse = startsByCategoryHorse.get(row.category);
    if (!startsByHorse) {
      continue;
    }
    for (const horseNumber of splitHorseNumbers(row.currentHorseNumbers)) {
      startsByHorse.set(horseNumber, (startsByHorse.get(horseNumber) ?? 0) + row.starts);
    }
  }

  const requiredCount = Math.ceil((runners.length * 2) / 3);
  return (["jockey", "trainer"] as const).every((category) => {
    const startsByHorse = startsByCategoryHorse.get(category);
    if (!startsByHorse) {
      return false;
    }
    const coveredCount = runners.filter((runner) => {
      const rawHorseNumber = runner.umaban ?? "";
      const horseNumber = rawHorseNumber.replace(/^0+/u, "") || rawHorseNumber;
      return (startsByHorse.get(horseNumber) ?? 0) >= 2;
    }).length;
    return coveredCount >= requiredCount;
  });
};

const findConditionAnalysisCandidate = async <T extends SimilarRaceStatsSettings>(
  candidates: readonly T[],
  getStats: (settings: T) => Promise<ConditionAnalysisStats>,
  index = 0,
  partialMatch: ConditionAnalysisCandidateMatch<T> | null = null,
): Promise<ConditionAnalysisCandidateMatch<T> | null> => {
  const settings = candidates[index];

  if (!settings) {
    return partialMatch;
  }

  const stats = await getStats(settings);

  if (hasCompleteConditionAnalysisRows(stats)) {
    return { settings, stats };
  }

  return findConditionAnalysisCandidate(
    candidates,
    getStats,
    index + 1,
    partialMatch ?? (hasConditionAnalysisRows(stats) ? { settings, stats } : null),
  );
};

type RateStatsCandidateResult<T, R> =
  | { status: "exhausted" }
  | { status: "matched"; settings: T; stats: R }
  | { status: "timedOut" };

const findRateStatsCandidate = async <
  T extends SimilarRaceStatsSettings,
  R extends readonly (BloodlineStatsRow | SimilarRaceStatsRow)[],
>(
  candidates: readonly T[],
  getStats: (settings: T) => Promise<R>,
  hasEnoughStats: (stats: R) => boolean = hasRateRows,
): Promise<RateStatsCandidateResult<T, R>> => {
  if (candidates.length === 0) {
    return { status: "exhausted" };
  }

  const pending = candidates.map(async (settings) => ({
    settings,
    stats: await getStats(settings),
  }));
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<"timedOut">((resolve) => {
    timeout = setTimeout(() => resolve("timedOut"), RATE_STATS_FALLBACK_TIMEOUT_MS);
  });
  const waitForCanonicalPrefix = async (index: number): Promise<RateStatsCandidateResult<T, R>> => {
    const next = pending[index];
    if (!next) {
      return { status: "exhausted" };
    }
    const raced = await Promise.race([next, deadline]);
    if (raced === "timedOut") {
      return { status: "timedOut" };
    }
    if (hasEnoughStats(raced.stats)) {
      return { status: "matched", ...raced };
    }
    return waitForCanonicalPrefix(index + 1);
  };
  try {
    return await waitForCanonicalPrefix(0);
  } finally {
    clearTimeout(timeout);
  }
};

export const getDetailStatsContext = async ({
  day,
  keibajoCode,
  month,
  query,
  raceNumber,
  raceSource,
  year,
}: DetailSectionParams) => {
  const [race, runners] = await Promise.all([
    getRaceDetail(raceSource, year, month, day, keibajoCode, raceNumber),
    getRaceRunners(raceSource, year, month, day, keibajoCode, raceNumber),
  ]);

  if (!race) {
    return null;
  }

  const banEiRace = race.source === "nar" && isBanEiKeibajoCode(race.keibajoCode);
  const statsClassConditionLabel = getStatsClassConditionLabel(race);
  const raceNameFilterLabels = getRaceNameFilterLabels(race);
  const raceSymbolLabel = getRaceSymbolLabel(race.kyosoKigoCode);
  const defaultStatsYears = 10;
  const defaultBloodlineStatsYears = 10;
  const defaultStatsIncludeAge = !getAgeLabel(race.kyosoShubetsuCode).includes("4歳以上");
  const defaultSimilarStatsIncludeSex = raceSymbolLabel !== "牝馬限定";
  const buildStatsSettings = (
    prefix: string,
    defaultYearsForPrefix: number | null,
    defaultIncludeSex: boolean,
  ): SimilarRaceStatsSettings => ({
    cellMatching: false,
    classConditionName: statsClassConditionLabel,
    includeAge: getDefaultFlag(
      getStatsQueryParam(query, prefix, "statsAge") ??
        getStatsQueryParam(query, prefix, "statsClass"),
      defaultStatsIncludeAge,
    ),
    includeBloodlineAncestors: true,
    includeClass: getDefaultFlag(
      getStatsQueryParam(query, prefix, "statsClass"),
      Boolean(statsClassConditionLabel),
    ),
    includeConditionKey: false,
    includeDistance: banEiRace
      ? false
      : getFlag(getStatsQueryParam(query, prefix, "statsDistance")),
    includeFrame: getOptionalFlag(getStatsQueryParam(query, prefix, "statsFrame")),
    includeGrade: false,
    includeMonthWindow: getOptionalFlag(
      getStatsQueryParam(query, prefix, "statsRaceMonth") ??
        getStatsQueryParam(query, prefix, "statsMonthWindow"),
    ),
    includeNarOnly: getStatsSourceScope(query, prefix) === "nar",
    includeRaceNumber: getOptionalFlag(getStatsQueryParam(query, prefix, "statsRaceNumber")),
    includeRaceSubtitle: getDefaultFlag(
      getStatsQueryParam(query, prefix, "statsRaceSubtitle") ??
        getStatsQueryParam(query, prefix, "statsRaceName"),
      Boolean(raceNameFilterLabels.subtitle),
    ),
    includeRaceTitle: getDefaultFlag(
      getStatsQueryParam(query, prefix, "statsRaceTitle") ??
        getStatsQueryParam(query, prefix, "statsRaceName"),
      Boolean(raceNameFilterLabels.title),
    ),
    includeRunnerCount: false,
    includeSex: getDefaultFlag(getStatsQueryParam(query, prefix, "statsSex"), defaultIncludeSex),
    includeTrackCode: false,
    includeSurface: banEiRace
      ? false
      : getFlag(
          getStatsQueryParam(query, prefix, "statsSurface") ??
            getStatsQueryParam(query, prefix, "statsTrack"),
        ),
    includeTurn: banEiRace
      ? false
      : getFlag(
          getStatsQueryParam(query, prefix, "statsTurn") ??
            getStatsQueryParam(query, prefix, "statsTrack"),
        ),
    includeVenue: banEiRace
      ? false
      : getDefaultFlag(
          getStatsQueryParam(query, prefix, "statsVenue") ??
            (prefix === "analysis"
              ? getStatsQueryParam(query, "similar", "statsVenue")
              : undefined),
          true,
        ),
    includeWeight: getFlag(getStatsQueryParam(query, prefix, "statsWeight")),
    runnerCount: null,
    sourceScope: getStatsSourceScope(query, prefix),
    years: getStatsYears(getStatsQueryParam(query, prefix, "statsYears"), defaultYearsForPrefix),
  });

  const baseStatsSettings = buildStatsSettings(
    "similar",
    defaultStatsYears,
    defaultSimilarStatsIncludeSex,
  );
  const statsSettings: SimilarRaceStatsSettings =
    isOverseasKeibajoCode(race.keibajoCode) && !hasExplicitStatsState(query, "similar")
      ? relaxAllConditionAnalysisSettings(baseStatsSettings)
      : banEiRace && !hasExplicitStatsState(query, "similar")
        ? // Ban-ei titles are commonly one-off sponsor or dedication labels; class is the
          // repeatable comparison boundary, while exact-title filtering collapses history.
          { ...baseStatsSettings, includeRaceTitle: false }
        : baseStatsSettings;
  const baseBloodlineStatsSettings = buildStatsSettings(
    "bloodline",
    defaultBloodlineStatsYears,
    true,
  );
  const hasExplicitBloodlineVenue =
    getStatsQueryParam(query, "bloodline", "statsVenue") !== undefined;
  const baseResolvedBloodlineStatsSettings: SimilarRaceStatsSettings = {
    ...baseBloodlineStatsSettings,
    includeBloodlineAncestors: !getOptionalFlag(
      getStatsQueryParam(query, "bloodline", "statsOffspringOnly"),
    ),
    includeRunnerCount: false,
    includeVenue:
      isOverseasKeibajoCode(race.keibajoCode) && !hasExplicitBloodlineVenue
        ? false
        : baseBloodlineStatsSettings.includeVenue,
    runnerCount: null,
  };
  const bloodlineStatsSettings =
    isOverseasKeibajoCode(race.keibajoCode) && !hasExplicitStatsState(query, "bloodline")
      ? {
          ...relaxAllConditionAnalysisSettings(baseResolvedBloodlineStatsSettings),
          includeDistance: baseResolvedBloodlineStatsSettings.includeDistance,
        }
      : baseResolvedBloodlineStatsSettings;
  const statsConditionLabels = {
    age: getAgeLabel(race.kyosoShubetsuCode),
    class: statsClassConditionLabel,
    distance: banEiRace ? null : formatDistance(race.kyori),
    frame: "枠番号",
    grade: null,
    monthWindow: "開催月±1か月",
    raceNumber: formatRaceNumber(race.raceBango),
    raceSubtitle: raceNameFilterLabels.subtitle,
    raceTitle: raceNameFilterLabels.title,
    runnerCount: null,
    sex: raceSymbolLabel.startsWith("競走記号") ? null : raceSymbolLabel,
    surface: banEiRace ? null : getTrackSurfaceLabel(race.trackCode),
    track: null,
    turn: banEiRace ? null : getTrackTurnLabel(race.trackCode),
    venue: banEiRace ? null : formatKeibajo(keibajoCode),
    weight: getWeightLabel(race.juryoShubetsuCode),
  };
  const analysisCellFlags = getFinishPredictionDimensionFlags({
    gradeCode: race.gradeCode ?? null,
    isBanEi: banEiRace,
    paramNames: ANALYSIS_CELL_PARAM_NAMES,
    query,
    source: race.source,
  });
  const conditionAnalysisSettings: SimilarRaceStatsSettings = {
    ...buildCellMatchingStatsSettings({
      classConditionName: statsClassConditionLabel,
      flags: analysisCellFlags,
      sourceScope: getStatsSourceScope(query, "analysis"),
      years: getStatsYears(getStatsQueryParam(query, "analysis", "statsYears"), null),
    }),
  };
  const cellGradeLabel = getGradeLabel(race.gradeCode, race.source);
  const cellTrackLabel =
    banEiRace || race.trackCode === null || race.trackCode === ""
      ? null
      : `${getTrackSurfaceLabel(race.trackCode)}${getTrackTurnLabel(race.trackCode)}`;
  const cellRaceNameLabel = (race.kyosomeiHondai ?? "").trim();
  const conditionAnalysisLabels = {
    ...statsConditionLabels,
    class:
      race.source === "jra"
        ? getConditionLabel(race.kyosoJokenCode)
        : (race.kyosoJokenMeisho ?? "").trim() || statsClassConditionLabel,
    grade: cellGradeLabel === "-" ? null : cellGradeLabel,
    raceTitle: cellRaceNameLabel === "" ? null : cellRaceNameLabel,
    runnerCount: null,
    track: cellTrackLabel,
    venue: formatKeibajo(keibajoCode),
  };

  return {
    bloodlineStatsSettings,
    conditionAnalysisLabels,
    conditionAnalysisSettings,
    race,
    runners,
    statsClassConditionLabel,
    statsConditionLabels,
    statsSettings,
  };
};

const EMPTY_RUNNING_STYLE_FLAGS: RunningStyleDimensionFlags = {
  condition: false,
  distance: false,
  grade: false,
  keibajo: false,
  kyosoJoken: false,
  kyosoShubetsu: false,
  raceName: false,
  track: false,
};

const buildEmptyRunningStyleBucketPayload = (): RunningStyleBucketSectionPayload => ({
  bucketEvaluation: null,
  bucketFilter: null,
  bucketScope: null,
  dimensionFlags: EMPTY_RUNNING_STYLE_FLAGS,
  type: "running-style",
});

const EMPTY_RUNNING_STYLE_BUCKET_RESOLUTION: RunningStyleBucketResolution = {
  bucketEvaluation: null,
  bucketFilter: null,
  bucketScope: null,
};

const buildRunningStyleBucketTiers = (
  flags: RunningStyleDimensionFlags,
): readonly RunningStyleBucketTier[] => [
  { flags, level: "exact" },
  { flags: RUNNING_STYLE_KEIBAJO_ONLY_FLAGS, level: "keibajo" },
  { flags: RUNNING_STYLE_CATEGORY_ONLY_FLAGS, level: "category" },
];

// Resolve the bucket evaluation across progressively-relaxed dimension tiers,
// stopping at the first tier that returns metrics. Recursion keeps the early-exit
// behaviour without a for loop or extra nesting.
const resolveRunningStyleBucketTier = async (
  input: ResolveRunningStyleBucketInput,
  index: number,
): Promise<RunningStyleBucketResolution> => {
  const tier = input.tiers[index];
  if (tier === undefined) {
    return EMPTY_RUNNING_STYLE_BUCKET_RESOLUTION;
  }
  const bucketFilter = buildRunningStyleBucketFilter({
    flags: tier.flags,
    query: input.query,
    race: input.race,
  });
  const bucketEvaluation = await getRunningStyleBucketEvaluation({ filter: bucketFilter });
  if (bucketEvaluation === null) {
    return resolveRunningStyleBucketTier(input, index + 1);
  }
  return {
    bucketEvaluation,
    bucketFilter,
    bucketScope: { flags: tier.flags, level: tier.level },
  };
};

const parseKyoriOrZero = (value: string | null | undefined): number => {
  if (value === null || value === undefined) {
    return 0;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

const buildBucketRaceFromRaceDetail = (race: RaceDetail): RaceRowForRunningStyleBucketFilter => ({
  gradeCode: race.gradeCode ?? null,
  keibajoCode: race.keibajoCode,
  kyori: parseKyoriOrZero(race.kyori),
  kyosoJokenCode: race.kyosoJokenCode ?? null,
  kyosoJokenMeisho: race.kyosoJokenMeisho ?? null,
  kyosoShubetsuCode: race.kyosoShubetsuCode ?? "",
  kyosomeiHondai: race.kyosomeiHondai ?? null,
  source: race.source,
  trackCode: race.trackCode ?? null,
});

const buildEmptyRunningStyleBucketSectionData = (): RunningStyleBucketSectionData => ({
  bucketEvaluation: null,
  bucketGradeCode: null,
  bucketRace: null,
  bucketScope: null,
  bucketSource: null,
  dimensionFlags: null,
});

export const getRunningStyleBucketSectionData = async (
  params: DetailSectionParams,
): Promise<RunningStyleBucketSectionData> => {
  const { day, keibajoCode, month, query, raceNumber, raceSource, year } = params;
  const race = await getRaceDetail(raceSource, year, month, day, keibajoCode, raceNumber);
  if (!race) {
    return buildEmptyRunningStyleBucketSectionData();
  }
  const isBanEi = race.source === "nar" && isBanEiKeibajoCode(race.keibajoCode);
  if (isBanEi) {
    return buildEmptyRunningStyleBucketSectionData();
  }
  const flags = getRunningStyleDimensionFlags({
    gradeCode: race.gradeCode ?? null,
    isBanEi,
    query,
    source: race.source,
  });
  const bucketRace = buildBucketRaceFromRaceDetail(race);
  const resolution = await resolveRunningStyleBucketTier(
    { query, race: bucketRace, tiers: buildRunningStyleBucketTiers(flags) },
    0,
  );
  return {
    bucketEvaluation: resolution.bucketEvaluation,
    bucketGradeCode: race.gradeCode ?? null,
    bucketRace,
    bucketScope: resolution.bucketScope,
    bucketSource: race.source,
    dimensionFlags: flags,
  };
};

const EMPTY_FINISH_POSITION_BUCKET_RESOLUTION: FinishPositionBucketResolution = {
  bucketEvaluation: null,
  bucketScope: null,
};

// Resolve the finish-position bucket evaluation across progressively-relaxed
// dimension tiers, stopping at the first tier that returns metrics. Recursion
// keeps the early-exit behaviour without a for loop or extra nesting; the
// undefined tier guard enforces the three-tier max depth.
const resolveFinishPositionBucketTier = async (
  input: ResolveFinishPositionBucketInput,
  index: number,
): Promise<FinishPositionBucketResolution> => {
  const tier = input.tiers[index];
  if (tier === undefined) {
    return EMPTY_FINISH_POSITION_BUCKET_RESOLUTION;
  }
  const bucketFilter = buildFinishPositionBucketFilter({
    flags: tier.flags,
    modelVersion: input.modelVersion,
    query: input.query,
    race: {
      conditionKey: input.race.kyosoJokenMeisho,
      gradeCode: input.race.gradeCode,
      keibajoCode: input.race.keibajoCode,
      kyori: String(input.race.kyori),
      kyosoJokenCode: input.race.kyosoJokenCode,
      kyosoJokenMeisho: input.race.kyosoJokenMeisho,
      kyosoShubetsuCode: input.race.kyosoShubetsuCode,
      kyosomeiHondai: input.race.kyosomeiHondai,
      raceName: input.race.kyosomeiHondai,
      source: input.race.source,
      trackCode: input.race.trackCode,
    },
  });
  const bucketEvaluation = await getFinishPositionBucketEvaluation({ filter: bucketFilter });
  if (bucketEvaluation === null) {
    return resolveFinishPositionBucketTier(input, index + 1);
  }
  return {
    bucketEvaluation,
    bucketScope: { flags: tier.flags, level: tier.level },
  };
};

const buildFinishPositionBucketRaceFromRaceDetail = (
  race: RaceDetail,
): FinishPositionBucketRace => ({
  gradeCode: race.gradeCode ?? null,
  keibajoCode: race.keibajoCode,
  kyori: parseKyoriOrZero(race.kyori),
  kyosoJokenCode: race.kyosoJokenCode ?? null,
  kyosoJokenMeisho: race.kyosoJokenMeisho ?? null,
  kyosoShubetsuCode: race.kyosoShubetsuCode ?? "",
  kyosomeiHondai: race.kyosomeiHondai ?? null,
  source: race.source,
  trackCode: race.trackCode ?? null,
});

const buildEmptyFinishPositionBucketSectionData = (): FinishPositionBucketSectionData => ({
  bucketEvaluation: null,
  bucketGradeCode: null,
  bucketModelVersion: null,
  bucketRace: null,
  bucketScope: null,
  bucketSource: null,
});

export const getFinishPositionBucketSectionData = async (
  params: DetailSectionParams,
): Promise<FinishPositionBucketSectionData> => {
  const { day, keibajoCode, month, query, raceNumber, raceSource, year } = params;
  const race = await getRaceDetail(raceSource, year, month, day, keibajoCode, raceNumber);
  if (!race) {
    return buildEmptyFinishPositionBucketSectionData();
  }
  const isBanEi = race.source === "nar" && isBanEiKeibajoCode(race.keibajoCode);
  const category = getFinishPredictionEvaluationCategory({
    keibajoCode: race.keibajoCode,
    source: race.source,
  });
  const modelVersion = resolveFinishPositionBucketModelVersion(category);
  if (modelVersion === null) {
    return buildEmptyFinishPositionBucketSectionData();
  }
  const flags = getFinishPredictionDimensionFlags({
    gradeCode: race.gradeCode ?? null,
    isBanEi,
    query,
    source: race.source,
  });
  const bucketRace = buildFinishPositionBucketRaceFromRaceDetail(race);
  const resolution = await resolveFinishPositionBucketTier(
    {
      modelVersion,
      query,
      race: bucketRace,
      tiers: buildFinishPositionBucketTiers(flags),
    },
    0,
  );
  return {
    bucketEvaluation: resolution.bucketEvaluation,
    bucketGradeCode: race.gradeCode ?? null,
    bucketModelVersion: modelVersion,
    bucketRace,
    bucketScope: resolution.bucketScope,
    bucketSource: race.source,
  };
};

const buildRunningStyleBucketSectionPayload = async (
  params: DetailSectionParams,
): Promise<RunningStyleBucketSectionPayload> => {
  const { day, keibajoCode, month, query, raceNumber, raceSource, year } = params;
  const race = await getRaceDetail(raceSource, year, month, day, keibajoCode, raceNumber);
  if (!race) {
    return buildEmptyRunningStyleBucketPayload();
  }
  const isBanEi = race.source === "nar" && isBanEiKeibajoCode(race.keibajoCode);
  if (isBanEi) {
    return buildEmptyRunningStyleBucketPayload();
  }
  const flags = getRunningStyleDimensionFlags({
    gradeCode: race.gradeCode ?? null,
    isBanEi,
    query,
    source: race.source,
  });
  const bucketRace = buildBucketRaceFromRaceDetail(race);
  const resolution = await resolveRunningStyleBucketTier(
    { query, race: bucketRace, tiers: buildRunningStyleBucketTiers(flags) },
    0,
  );
  return {
    bucketEvaluation: resolution.bucketEvaluation,
    bucketFilter: resolution.bucketFilter,
    bucketScope: resolution.bucketScope,
    dimensionFlags: flags,
    type: "running-style",
  };
};

const heatmapStatsYears = (years: number | null): number =>
  years === null ? HEATMAP_STATS_YEARS_FALLBACK : years;

const buildWinRateHeatmapCatalogQuery = (
  params: DetailSectionParams,
  settings: SimilarRaceStatsSettings,
  source: RaceSource,
  includeOwner: boolean,
): WinRateHeatmapCatalogQuery => {
  const query: WinRateHeatmapCatalogQuery = {
    day: params.day,
    includeAge: settings.includeAge,
    includeClass: settings.includeClass,
    includeConditionKey: settings.includeConditionKey,
    includeDistance: settings.includeDistance,
    includeGrade: settings.includeGrade,
    includeRaceTitle: settings.includeRaceTitle,
    includeSurface: settings.includeSurface,
    includeTrackCode: settings.includeTrackCode,
    includeTurn: settings.includeTurn,
    includeVenue: settings.includeVenue,
    keibajoCode: params.keibajoCode,
    month: params.month,
    raceNumber: params.raceNumber,
    source,
    year: params.year,
    years: heatmapStatsYears(settings.years),
  };
  return includeOwner ? { ...query, includeOwner: true } : query;
};

const loadCatalogGroupedRateStats = async (
  params: DetailSectionParams,
  settings: SimilarRaceStatsSettings,
  source: RaceSource,
  includeOwner: boolean,
): Promise<WinRateHeatmapCatalogStats | null> => {
  const catalogStats = await fetchWinRateHeatmapStatsFromCatalog(
    buildWinRateHeatmapCatalogQuery(params, settings, source, includeOwner),
  );
  if (catalogStats === null) return null;
  return {
    bloodlineRows: groupCatalogBloodlineRows(catalogStats.bloodlineRows),
    similarRows: groupCatalogSimilarRows(catalogStats.similarRows),
  };
};

const loadConditionHistoryCatalogStats = async (
  params: DetailSectionParams,
  settings: SimilarRaceStatsSettings,
  source: RaceSource,
) => {
  try {
    return await fetchConditionHistoryStatsFromCatalog({
      day: params.day,
      includeAge: settings.includeAge,
      includeClass: settings.includeClass,
      includeConditionKey: settings.includeConditionKey,
      includeDistance: settings.includeDistance,
      includeGrade: settings.includeGrade,
      includeRaceTitle: settings.includeRaceTitle,
      includeSurface: settings.includeSurface,
      includeTrackCode: settings.includeTrackCode,
      includeTurn: settings.includeTurn,
      includeVenue: settings.includeVenue,
      keibajoCode: params.keibajoCode,
      month: params.month,
      raceNumber: params.raceNumber,
      source,
      year: params.year,
      years: heatmapStatsYears(settings.years),
    });
  } catch {
    return null;
  }
};

const isCachedTimeScorePayload = (value: unknown): value is CachedTimeScorePayload =>
  typeof value === "object" &&
  value !== null &&
  "type" in value &&
  value.type === "time-score" &&
  "correlationRows" in value &&
  Array.isArray(value.correlationRows) &&
  "rows" in value &&
  Array.isArray(value.rows);

const loadCachedTimeScorePayload = async (
  params: DetailSectionParams,
): Promise<CachedTimeScorePayload | null> => {
  const cacheKey = buildDetailSectionCacheKey({
    day: params.day,
    keibajoCode: params.keibajoCode,
    month: params.month,
    raceNumber: params.raceNumber,
    section: "time-score",
    year: params.year,
  });
  const cached = await getCachedDetailSectionResponse(cacheKey);
  if (cached === null) return null;
  try {
    const payload: unknown = await cached.json();
    return isCachedTimeScorePayload(payload) ? payload : null;
  } catch {
    return null;
  }
};

const resolveRaceTimeStats = async (
  race: RaceDetail,
  settings: SimilarRaceStatsSettings,
  catalogRaceTimeStats: RaceTimeStats | null,
): Promise<RaceTimeStats> =>
  catalogRaceTimeStats !== null && catalogRaceTimeStats.correlationRows.length > 0
    ? catalogRaceTimeStats
    : getOrComputeRaceTimeStats({ race, settings });

const loadDetailSectionPayload = async (section: DetailSection, params: DetailSectionParams) => {
  const { day, keibajoCode, month, query, raceNumber, raceSource, year } = params;

  if (section === "premium-data-top") {
    const [race, runners] = await Promise.all([
      getRaceDetail(raceSource, year, month, day, keibajoCode, raceNumber),
      getRaceRunners(raceSource, year, month, day, keibajoCode, raceNumber),
    ]);
    if (!race || (race.source === "nar" && isBanEiKeibajoCode(race.keibajoCode))) {
      return { dataTopHorses: [], type: section };
    }
    const dataTopHorses = await getPremiumDataTopHorsesWithCache({
      kaisaiNen: race.kaisaiNen,
      kaisaiTsukihi: race.kaisaiTsukihi,
      keibajoCode: race.keibajoCode,
      raceBango: race.raceBango,
      source: race.source,
    });
    return {
      dataTopHorses: enrichPremiumDataTopHorses(dataTopHorses, runners),
      type: section,
    };
  }

  if (section === "running-style") {
    return buildRunningStyleBucketSectionPayload({
      day,
      keibajoCode,
      month,
      query,
      raceNumber,
      raceSource,
      year,
    });
  }

  if (section === "training") {
    const [race, trainings] = await Promise.all([
      getRaceDetail(raceSource, year, month, day, keibajoCode, raceNumber),
      getRaceTrainingsWithCatalogFallback(raceSource, year, month, day, keibajoCode, raceNumber),
    ]);
    const premiumPayload = race
      ? await fetchPremiumRacePayload(race)
      : { stableComments: [], trainingReviews: [] };
    return {
      sourceLabel: SOURCE_LABELS[raceSource],
      stableComments: premiumPayload.stableComments,
      trainings: mergePremiumTrainingReviews(trainings, premiumPayload.trainingReviews),
      type: section,
    };
  }

  const context = await getDetailStatsContext(params);
  if (!context) {
    return null;
  }

  const { race, runners } = context;

  if (section === "results") {
    const resultsSourceScope = getResultsSourceScope(query);
    const catalogResults = await fetchHorseRaceResultsFromCatalog({
      day,
      keibajoCode,
      month,
      raceBango: raceNumber,
      source: race.source,
      sourceScope: resultsSourceScope,
      year,
    });
    const results =
      catalogResults === null
        ? await getHorseRaceResults(
            raceSource,
            year,
            month,
            day,
            keibajoCode,
            raceNumber,
            resultsSourceScope,
          )
        : catalogResults;
    return {
      classConditionName: context.statsClassConditionLabel,
      currentDistance: race.kyori,
      currentKeibajoCode: race.keibajoCode,
      currentRaceDate: `${year}${month}${day}`,
      currentTrackCode: race.trackCode,
      defaultIncludeClass: context.statsSettings.includeClass,
      results,
      runners,
      source: raceSource,
      sourceScope: resultsSourceScope,
      type: section,
    };
  }

  if (section === "ability") {
    const abilityTests =
      raceSource === "nar"
        ? await getRaceAbilityTests(raceSource, year, month, day, keibajoCode, raceNumber)
        : [];
    return {
      abilityTests,
      type: section,
    };
  }

  if (section === "condition") {
    const catalogCondition = await loadConditionHistoryCatalogStats(
      params,
      context.conditionAnalysisSettings,
      race.source,
    );
    if (catalogCondition !== null) {
      const [raceTimeStats, payoutStats] = await Promise.all([
        catalogCondition.raceTimeStats.targetRaces.length > 0
          ? Promise.resolve(catalogCondition.raceTimeStats)
          : getOrComputeRaceTimeStats({
              race,
              settings: context.conditionAnalysisSettings,
            }),
        getPayoutStats(race, context.conditionAnalysisSettings),
      ]);
      return {
        carriedWeightClassStats: isBanEiKeibajoCode(race.keibajoCode)
          ? []
          : catalogCondition.carriedWeightClassStats,
        conditionLabels: context.conditionAnalysisLabels,
        finishPositionStats: catalogCondition.finishPositionStats,
        frameStats: catalogCondition.frameStats,
        payoutStats,
        raceTimeStats,
        runners,
        settings: context.conditionAnalysisSettings,
        source: race.source,
        type: section,
        weightClassStats: catalogCondition.weightClassStats,
      };
    }
    let resolvedSettings = context.conditionAnalysisSettings;
    const getConditionAnalysisStats = async (settings: typeof resolvedSettings) =>
      Promise.all([
        getOrComputeRaceTimeStats({ race, settings }),
        getPayoutStats(race, settings),
        getFinishPositionStats(race, settings),
        getFrameStats(race, settings),
      ]) satisfies Promise<ConditionAnalysisStats>;
    let stats = await getConditionAnalysisStats(resolvedSettings);
    if (!hasExplicitStatsState(query, "analysis") && !hasCompleteConditionAnalysisRows(stats)) {
      const candidates = getConditionAnalysisSettingCandidates(resolvedSettings).slice(1);
      const matched = await findConditionAnalysisCandidate(candidates, getConditionAnalysisStats);
      if (matched) {
        resolvedSettings = matched.settings;
        stats = matched.stats;
      }
    }
    const [raceTimeStats, payoutStats, finishPositionStats, frameStats] = stats;
    const [weightClassStats, carriedWeightClassStats]: [
      WeightClassStatsRow[],
      WeightClassStatsRow[],
    ] = await Promise.all([
      getWeightClassStats(race, resolvedSettings),
      isBanEiKeibajoCode(race.keibajoCode)
        ? Promise.resolve([])
        : getCarriedWeightClassStats(race, resolvedSettings),
    ]);
    return {
      carriedWeightClassStats,
      conditionLabels: context.conditionAnalysisLabels,
      finishPositionStats,
      frameStats,
      payoutStats,
      raceTimeStats,
      runners,
      settings: resolvedSettings,
      source: race.source,
      type: section,
      weightClassStats,
    };
  }

  if (section === "time-score") {
    const [catalogStats, catalogCondition, rows] = await Promise.all([
      loadCatalogGroupedRateStats(params, context.statsSettings, race.source, true),
      loadConditionHistoryCatalogStats(params, context.conditionAnalysisSettings, race.source),
      getTimeScoreRows(race, context.conditionAnalysisSettings),
    ]);
    const raceTimeStats = await resolveRaceTimeStats(
      race,
      context.conditionAnalysisSettings,
      catalogCondition === null ? null : catalogCondition.raceTimeStats,
    );
    const jockeyNameByHorse = new Map(
      context.runners.map((runner) => [
        normalizeHorseNumber(runner.umaban),
        getRunnerDisplayNames(runner).jockey || "-",
      ]),
    );
    const mappedRows = rows.map((row) =>
      Object.assign(row, {
        jockeyName:
          jockeyNameByHorse.get(normalizeHorseNumber(row.horseNumber)) || row.jockeyName || "-",
      }),
    );
    if (catalogStats !== null) {
      const similarRows = getEligibleSimilarStatsRows(race, catalogStats.similarRows);
      const bloodlineRows = getEligibleBloodlineRows(race, catalogStats.bloodlineRows);
      return {
        bloodlineRows,
        ...getBloodlineIncompletePayload(bloodlineRows, runners),
        bloodlineSettings: context.bloodlineStatsSettings,
        ...getBloodlineVenueFallbackPayload(race, context.bloodlineStatsSettings),
        conditionLabels: context.statsConditionLabels,
        correlationRows: raceTimeStats.correlationRows,
        rows: mappedRows,
        runners,
        settings: context.statsSettings,
        similarRows,
        ...(hasSimilarJockeyTrainerCoverage(similarRows, runners)
          ? {}
          : { similarStatsIncomplete: true }),
        ...getSimilarStatsFallbackPayload(race, context.statsSettings),
        source: race.source,
        type: section,
      };
    }
    let resolvedSimilarSettings = context.statsSettings;
    let similarStatsIncomplete = false;
    let similarRows = getEligibleSimilarStatsRows(
      race,
      await getSimilarRaceStats(race, resolvedSimilarSettings),
    );
    if (
      !hasExplicitStatsState(query, "similar") &&
      !hasSimilarJockeyTrainerCoverage(similarRows, runners)
    ) {
      const candidates = getConditionAnalysisSettingCandidates(resolvedSimilarSettings).slice(1);
      const matched = await findRateStatsCandidate(
        candidates,
        async (candidate) =>
          getEligibleSimilarStatsRows(race, await getSimilarRaceStats(race, candidate)),
        (stats) => hasSimilarJockeyTrainerCoverage(stats, runners),
      );
      if (matched.status === "matched") {
        resolvedSimilarSettings = matched.settings;
        similarRows = matched.stats;
      } else {
        similarStatsIncomplete = true;
      }
    }
    let resolvedBloodlineSettings = context.bloodlineStatsSettings;
    let bloodlineRows = getEligibleBloodlineRows(
      race,
      await getBloodlineStats(race, resolvedBloodlineSettings),
    );
    if (
      !isOverseasKeibajoCode(race.keibajoCode) &&
      !hasExplicitStatsState(query, "bloodline") &&
      !hasBloodlineScoreCoverage(bloodlineRows, runners)
    ) {
      const candidates = getConditionAnalysisSettingCandidates(resolvedBloodlineSettings).slice(1);
      const matched = await findRateStatsCandidate(
        candidates,
        async (candidate) =>
          getEligibleBloodlineRows(race, await getBloodlineStats(race, candidate)),
        (stats) => hasBloodlineScoreCoverage(stats, runners),
      );
      if (matched.status === "matched") {
        resolvedBloodlineSettings = matched.settings;
        bloodlineRows = matched.stats;
      }
    }
    return {
      bloodlineRows,
      ...getBloodlineIncompletePayload(bloodlineRows, runners),
      bloodlineSettings: resolvedBloodlineSettings,
      ...getBloodlineVenueFallbackPayload(race, resolvedBloodlineSettings),
      conditionLabels: context.statsConditionLabels,
      correlationRows: raceTimeStats.correlationRows,
      rows: mappedRows,
      runners,
      settings: resolvedSimilarSettings,
      similarRows,
      ...(similarStatsIncomplete ? { similarStatsIncomplete: true } : {}),
      ...getSimilarStatsFallbackPayload(race, resolvedSimilarSettings),
      source: race.source,
      type: section,
    };
  }

  if (section === "finish-prediction") {
    const results = await getHorseRaceResults(
      race.source,
      year,
      month,
      day,
      keibajoCode,
      raceNumber,
      getResultsSourceScope(params.query),
    );
    const evaluationCategory = getFinishPredictionEvaluationCategory({
      keibajoCode: race.keibajoCode,
      source: race.source,
    });
    const [
      similarityFeatures,
      modelPredictionFeatures,
      sameDayVenueJockeyWins,
      dbEvaluation,
      bucketSectionData,
    ] = await Promise.all([
      getFinishPositionSimilarityFeatures(race, runners),
      getActiveFinishPositionPredictions(race, runners),
      fetchSameDayVenueJockeyWins(race),
      getActiveFinishPredictionEvaluation(evaluationCategory),
      getFinishPositionBucketSectionData(params),
    ]);
    const staticEvaluation: FinishPredictionEvaluationMetrics =
      FINISH_POSITION_PREDICTION_EVALUATIONS[evaluationCategory];
    const evaluationFromDb: FinishPredictionEvaluationMetrics | null =
      dbEvaluation === null
        ? null
        : {
            category: evaluationCategory,
            categoryLabel: staticEvaluation.categoryLabel,
            fromDate: dbEvaluation.evaluationWindowFrom,
            pairScore:
              dbEvaluation.pairScore === null
                ? staticEvaluation.pairScore
                : dbEvaluation.pairScore * 100,
            place1Accuracy:
              dbEvaluation.place1Accuracy === null
                ? staticEvaluation.place1Accuracy
                : dbEvaluation.place1Accuracy * 100,
            place2Accuracy:
              dbEvaluation.place2Accuracy === null
                ? staticEvaluation.place2Accuracy
                : dbEvaluation.place2Accuracy * 100,
            place3Accuracy:
              dbEvaluation.place3Accuracy === null
                ? staticEvaluation.place3Accuracy
                : dbEvaluation.place3Accuracy * 100,
            raceCount: dbEvaluation.raceCount,
            target: staticEvaluation.target,
            toDate: dbEvaluation.evaluationWindowTo,
            top1Accuracy:
              dbEvaluation.top1Accuracy === null
                ? staticEvaluation.top1Accuracy
                : dbEvaluation.top1Accuracy * 100,
            top3BoxAccuracy:
              dbEvaluation.top3BoxAccuracy === null
                ? staticEvaluation.top3BoxAccuracy
                : dbEvaluation.top3BoxAccuracy * 100,
            top3ExactOrderAccuracy:
              dbEvaluation.top3ExactAccuracy === null
                ? staticEvaluation.top3ExactOrderAccuracy
                : dbEvaluation.top3ExactAccuracy * 100,
            top3PlaceRelation:
              dbEvaluation.top3PlaceRelation === null
                ? staticEvaluation.top3PlaceRelation
                : dbEvaluation.top3PlaceRelation * 100,
            top3WinnerCapture:
              dbEvaluation.top3WinnerCapture === null
                ? staticEvaluation.top3WinnerCapture
                : dbEvaluation.top3WinnerCapture * 100,
            top5WinnerCapture:
              dbEvaluation.top5WinnerCapture === null
                ? staticEvaluation.top5WinnerCapture
                : dbEvaluation.top5WinnerCapture * 100,
          };
    const inputs: FinishPredictionBuildInputs = {
      currentDistance: race.kyori,
      currentGradeCode: race.gradeCode,
      currentKeibajoCode: race.keibajoCode,
      currentKyosoJokenCode: race.kyosoJokenCode,
      currentKyosoJokenMeisho: race.kyosoJokenMeisho,
      currentRaceDate: `${race.kaisaiNen}${race.kaisaiTsukihi}`,
      currentSource: race.source,
      currentTrackCode: race.trackCode,
      modelPredictionFeatures,
      results,
      runners,
      sameDayVenueJockeyWins,
      similarityFeatures,
    };
    return {
      bucket: bucketSectionData,
      evaluation:
        evaluationFromDb ??
        getFinishPredictionEvaluation({
          keibajoCode: race.keibajoCode,
          source: race.source,
        }),
      inputs,
      type: section,
    };
  }

  if (section === "overall-score") {
    const [catalogStats, cachedTimeScore] = await Promise.all([
      loadCatalogGroupedRateStats(params, context.bloodlineStatsSettings, race.source, false),
      loadCachedTimeScorePayload(params),
    ]);
    const resolvedBloodlineSettings = context.bloodlineStatsSettings;
    const bloodlineRows =
      catalogStats === null
        ? getEligibleBloodlineRows(race, await getBloodlineStats(race, resolvedBloodlineSettings))
        : getEligibleBloodlineRows(race, catalogStats.bloodlineRows);
    if (cachedTimeScore !== null) {
      return {
        ...getBloodlineIncompletePayload(bloodlineRows, runners),
        ...getBloodlineVenueFallbackPayload(race, resolvedBloodlineSettings),
        rows: buildOverallScoreRows({
          bloodlineRows,
          correlationRows: cachedTimeScore.correlationRows,
          runners,
          timeRows: cachedTimeScore.rows,
        }),
        type: section,
      };
    }
    const catalogCondition = await loadConditionHistoryCatalogStats(
      params,
      context.conditionAnalysisSettings,
      race.source,
    );
    const [timeRows, raceTimeStats] = await Promise.all([
      getTimeScoreRows(race, context.conditionAnalysisSettings),
      resolveRaceTimeStats(
        race,
        context.conditionAnalysisSettings,
        catalogCondition === null ? null : catalogCondition.raceTimeStats,
      ),
    ]);
    return {
      ...getBloodlineIncompletePayload(bloodlineRows, runners),
      ...getBloodlineVenueFallbackPayload(race, resolvedBloodlineSettings),
      rows: buildOverallScoreRows({
        bloodlineRows,
        correlationRows: raceTimeStats.correlationRows,
        runners,
        timeRows,
      }),
      type: section,
    };
  }

  if (section === "pace-prediction") {
    if (
      !isCornerPacePredictionSupported({
        distance: race.kyori,
        keibajoCode: race.keibajoCode,
        source: race.source,
      })
    ) {
      return {
        rows: [],
        supported: false,
        type: section,
      };
    }
    const results = await getHorseRaceResults(
      race.source,
      year,
      month,
      day,
      keibajoCode,
      raceNumber,
      getResultsSourceScope(params.query),
    );
    const [similarityFeatures, modelPredictionFeatures, runningStyleRows] = await Promise.all([
      getRacePaceSimilarityFeatures(race, runners),
      getRacePaceModelPredictionFeatures(race, runners),
      getRaceRunningStylesWithCache({
        kaisaiNen: race.kaisaiNen,
        kaisaiTsukihi: race.kaisaiTsukihi,
        keibajoCode: race.keibajoCode,
        raceBango: race.raceBango,
        source: race.source,
      }).catch(() => []),
    ]);
    const paceRows = buildRacePacePredictionRowsFromResults({
      currentConditionCode: race.kyosoJokenCode,
      currentConditionName: race.kyosoJokenMeisho,
      currentDistance: race.kyori,
      currentGradeCode: race.gradeCode,
      currentRaceAgeCode: race.kyosoShubetsuCode,
      currentRaceDate: `${race.kaisaiNen}${race.kaisaiTsukihi}`,
      currentSource: race.source,
      currentTrackCode: race.trackCode,
      modelPredictionFeatures,
      results,
      runners,
      similarityFeatures,
    });
    const probabilities = runningStyleRows.map((row) => ({
      pNige: row.p_nige,
      pOikomi: row.p_oikomi,
      pSashi: row.p_sashi,
      pSenkou: row.p_senkou,
      predictedLabel: row.predictedLabel,
      umaban: row.horseNumber,
    }));
    return {
      rows: applyRunningStyleSortToRacePaceRows(paceRows, probabilities),
      supported: true,
      type: section,
    };
  }

  if (section === "bloodline") {
    const catalogStats = await loadCatalogGroupedRateStats(
      params,
      context.bloodlineStatsSettings,
      race.source,
      false,
    );
    if (catalogStats !== null) {
      const rows = getEligibleBloodlineRows(race, catalogStats.bloodlineRows);
      return {
        ...getBloodlineIncompletePayload(rows, runners),
        ...getBloodlineVenueFallbackPayload(race, context.bloodlineStatsSettings),
        conditionLabels: context.statsConditionLabels,
        rows,
        runners,
        settings: context.bloodlineStatsSettings,
        source: race.source,
        type: section,
      };
    }
    let resolvedSettings = context.bloodlineStatsSettings;
    let rows = getEligibleBloodlineRows(race, await getBloodlineStats(race, resolvedSettings));
    if (
      !isOverseasKeibajoCode(race.keibajoCode) &&
      !hasExplicitStatsState(query, "bloodline") &&
      !hasBloodlineScoreCoverage(rows, runners)
    ) {
      const candidates = getConditionAnalysisSettingCandidates(resolvedSettings).slice(1);
      const matched = await findRateStatsCandidate(
        candidates,
        async (candidate) =>
          getEligibleBloodlineRows(race, await getBloodlineStats(race, candidate)),
        (stats) => hasBloodlineScoreCoverage(stats, runners),
      );
      if (matched.status === "matched") {
        resolvedSettings = matched.settings;
        rows = matched.stats;
      }
    }
    return {
      ...getBloodlineIncompletePayload(rows, runners),
      ...getBloodlineVenueFallbackPayload(race, resolvedSettings),
      conditionLabels: context.statsConditionLabels,
      rows,
      runners,
      settings: resolvedSettings,
      source: race.source,
      type: section,
    };
  }

  const catalogSimilarStats = await loadCatalogGroupedRateStats(
    params,
    context.statsSettings,
    race.source,
    true,
  );
  if (catalogSimilarStats !== null) {
    const similarRows = getEligibleSimilarStatsRows(race, catalogSimilarStats.similarRows);
    const bloodlineRows = getEligibleBloodlineRows(race, catalogSimilarStats.bloodlineRows);
    return {
      bloodlineRows,
      ...getBloodlineIncompletePayload(bloodlineRows, runners),
      bloodlineSettings: context.bloodlineStatsSettings,
      ...getBloodlineVenueFallbackPayload(race, context.bloodlineStatsSettings),
      conditionLabels: context.statsConditionLabels,
      rows: similarRows,
      runners,
      settings: context.statsSettings,
      ...(hasSimilarJockeyTrainerCoverage(similarRows, runners)
        ? {}
        : { similarStatsIncomplete: true }),
      ...getSimilarStatsFallbackPayload(race, context.statsSettings),
      source: race.source,
      type: "similar" satisfies DetailSection,
    };
  }
  let resolvedSettings = context.statsSettings;
  let similarStatsIncomplete = false;
  let rows = getEligibleSimilarStatsRows(race, await getSimilarRaceStats(race, resolvedSettings));
  if (!hasExplicitStatsState(query, "similar") && !hasSimilarJockeyTrainerCoverage(rows, runners)) {
    const candidates = getConditionAnalysisSettingCandidates(resolvedSettings).slice(1);
    const matched = await findRateStatsCandidate(
      candidates,
      async (candidate) =>
        getEligibleSimilarStatsRows(race, await getSimilarRaceStats(race, candidate)),
      (stats) => hasSimilarJockeyTrainerCoverage(stats, runners),
    );
    if (matched.status === "matched") {
      resolvedSettings = matched.settings;
      rows = matched.stats;
    } else {
      similarStatsIncomplete = true;
    }
  }
  let resolvedBloodlineSettings = context.bloodlineStatsSettings;
  let bloodlineRows = getEligibleBloodlineRows(
    race,
    await getBloodlineStats(race, resolvedBloodlineSettings),
  );
  if (
    !isOverseasKeibajoCode(race.keibajoCode) &&
    !hasExplicitStatsState(query, "bloodline") &&
    !hasBloodlineScoreCoverage(bloodlineRows, runners)
  ) {
    const candidates = getConditionAnalysisSettingCandidates(resolvedBloodlineSettings).slice(1);
    const matched = await findRateStatsCandidate(
      candidates,
      async (candidate) => getEligibleBloodlineRows(race, await getBloodlineStats(race, candidate)),
      (stats) => hasBloodlineScoreCoverage(stats, runners),
    );
    if (matched.status === "matched") {
      resolvedBloodlineSettings = matched.settings;
      bloodlineRows = matched.stats;
    }
  }

  return {
    bloodlineRows,
    ...getBloodlineIncompletePayload(bloodlineRows, runners),
    bloodlineSettings: resolvedBloodlineSettings,
    ...getBloodlineVenueFallbackPayload(race, resolvedBloodlineSettings),
    conditionLabels: context.statsConditionLabels,
    rows,
    runners,
    settings: resolvedSettings,
    ...(similarStatsIncomplete ? { similarStatsIncomplete: true } : {}),
    ...getSimilarStatsFallbackPayload(race, resolvedSettings),
    source: race.source,
    type: "similar" satisfies DetailSection,
  };
};

interface HeatmapResultsSource {
  results: HorseRaceResult[];
  type: "results";
}

interface HeatmapConditionSource {
  carriedWeightClassStats?: unknown;
  frameStats: FrameStatsRow[];
  type: "condition";
  weightClassStats?: unknown;
}

const isRecordPayload = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isHeatmapResultsSource = (value: unknown): value is HeatmapResultsSource =>
  isRecordPayload(value) && value.type === "results" && Array.isArray(value.results);

const isWeightClassStatsList = (value: unknown): value is WeightClassStatsRow[] =>
  Array.isArray(value);

const isHeatmapConditionSource = (value: unknown): value is HeatmapConditionSource =>
  isRecordPayload(value) && value.type === "condition" && Array.isArray(value.frameStats);

const readWeightClassStats = (value: unknown): WeightClassStatsRow[] =>
  isWeightClassStatsList(value) ? value : [];

const loadHeatmapSectionSource = async (
  section: "condition" | "results",
  params: DetailSectionParams,
): Promise<unknown> => {
  const cacheKey = buildDetailSectionCacheKey({
    day: params.day,
    keibajoCode: params.keibajoCode,
    month: params.month,
    raceNumber: params.raceNumber,
    section,
    year: params.year,
  });
  const cached = await getCachedDetailSectionResponse(cacheKey);
  if (cached === null) {
    return loadDetailSectionPayload(section, params);
  }
  try {
    return await cached.json();
  } catch {
    return loadDetailSectionPayload(section, params);
  }
};

export const getDetailSectionPayload = async (
  section: DetailSection,
  params: DetailSectionParams,
) => {
  if (section !== "win-rate-heatmap") {
    return loadDetailSectionPayload(section, params);
  }
  const context = await getDetailStatsContext(params);
  if (context === null) {
    return null;
  }
  const [catalogStats, resultsPayload, conditionPayload] = await Promise.all([
    fetchWinRateHeatmapStatsFromCatalog({
      ...buildWinRateHeatmapCatalogQuery(
        params,
        context.conditionAnalysisSettings,
        context.race.source,
        false,
      ),
      includeJockeyFrame: true,
    }),
    loadHeatmapSectionSource("results", params),
    loadHeatmapSectionSource("condition", params),
  ]);
  const conditionSource = isHeatmapConditionSource(conditionPayload) ? conditionPayload : null;
  return {
    bloodlineRows: catalogStats === null ? [] : catalogStats.bloodlineRows,
    carriedWeightClassStats:
      conditionSource === null ? [] : readWeightClassStats(conditionSource.carriedWeightClassStats),
    frameStats: conditionSource === null ? [] : conditionSource.frameStats,
    horseResults: isHeatmapResultsSource(resultsPayload) ? resultsPayload.results : [],
    runners: context.runners,
    similarRows: catalogStats === null ? [] : catalogStats.similarRows,
    type: "win-rate-heatmap",
    weightClassStats:
      conditionSource === null ? [] : readWeightClassStats(conditionSource.weightClassStats),
  };
};
