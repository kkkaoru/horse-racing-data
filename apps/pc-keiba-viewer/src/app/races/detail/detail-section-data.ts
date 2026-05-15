import "server-only";
import {
  getBloodlineStats,
  getActiveFinishPositionPredictions,
  getActiveFinishPredictionEvaluation,
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
  getRaceTimeStats,
  getRaceTrainings,
  getSimilarRaceStats,
  getTimeScoreRows,
} from "../../../db/queries";
import { SOURCE_LABELS, type RaceSource } from "../../../lib/codes";
import { buildFinishPredictionRowsFromResults } from "../../../lib/finish-position-prediction";
import {
  type FinishPredictionEvaluationMetrics,
  FINISH_POSITION_PREDICTION_EVALUATIONS,
  getFinishPredictionEvaluation,
  getFinishPredictionEvaluationCategory,
} from "../../../lib/finish-position-prediction-evaluation";
import {
  cleanText,
  formatDistance,
  formatKeibajo,
  formatRaceNumber,
  getTrackSurfaceLabel,
  getTrackTurnLabel,
} from "../../../lib/format";
import {
  getAgeLabel,
  getConditionLabel,
  getGradeLabel,
  getRaceSymbolLabel,
  getRaceTags,
  getWeightLabel,
} from "../../../lib/race-classification";
import {
  buildRacePacePredictionRowsFromResults,
  isCornerPacePredictionSupported,
} from "../../../lib/race-pace-prediction";
import type {
  BloodlineStatsRow,
  FinishPositionStatsRow,
  FrameStatsRow,
  OverallScoreDetail,
  OverallScoreRow,
  PayoutStatsRow,
  RaceDetail,
  RaceTimeStats,
  Runner,
  SameDayVenueJockeyWinFeature,
  SimilarRaceStatsRow,
  SimilarRaceStatsSettings,
  TimeScoreRow,
} from "../../../lib/race-types";
import { isBanEiKeibajoCode } from "../../../lib/runner-format";

export type DetailSection =
  | "ability"
  | "bloodline"
  | "condition"
  | "finish-prediction"
  | "overall-score"
  | "pace-prediction"
  | "results"
  | "similar"
  | "time-score"
  | "training";

export interface DetailSectionParams {
  day: string;
  keibajoCode: string;
  month: string;
  query: Record<string, string | string[] | undefined>;
  raceNumber: string;
  raceSource: RaceSource;
  year: string;
}

const LISTED_OR_HIGHER_GRADE_CODES = new Set(["A", "B", "C", "D", "F", "G", "H", "L", "S"]);

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

const RATE_STATS_CANDIDATE_BATCH_SIZE = 3;

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

const splitHorseNumbers = (value: string): string[] =>
  value
    .split(",")
    .map((item) => item.trim().replace(/^0+/u, "") || item.trim())
    .filter(Boolean);

const getBloodlineScoreByHorse = (rows: BloodlineStatsRow[]): Map<string, number> => {
  const scoreTotals = new Map<string, { score: number; weight: number }>();
  const categoryWeights: Record<BloodlineStatsRow["category"], number> = {
    damSire: 0.35,
    sire: 0.45,
    sireSire: 0.2,
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
      return {
        details,
        horseName: runner.bamei?.trim() || "-",
        horseNumber,
        jockeyName: runner.kishumeiRyakusho?.trim() || "-",
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

const isJraG1ToG3 = (race: RaceDetail): boolean =>
  race.source === "jra" && ["A", "B", "C"].includes(cleanText(race.gradeCode, ""));

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

const getConditionAnalysisSettingCandidates = <T extends SimilarRaceStatsSettings>(
  settings: T,
): T[] => {
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

const findRateStatsCandidate = async <
  T extends SimilarRaceStatsSettings,
  R extends readonly (BloodlineStatsRow | SimilarRaceStatsRow)[],
>(
  candidates: readonly T[],
  getStats: (settings: T) => Promise<R>,
  hasEnoughStats: (stats: R) => boolean = hasRateRows,
  index = 0,
): Promise<{ settings: T; stats: R } | null> => {
  const candidateBatch = candidates.slice(index, index + RATE_STATS_CANDIDATE_BATCH_SIZE);

  if (candidateBatch.length === 0) {
    return null;
  }

  const batchStats = await Promise.all(
    candidateBatch.map(async (settings) => ({
      settings,
      stats: await getStats(settings),
    })),
  );

  const matched = batchStats.find(({ stats }) => hasEnoughStats(stats));
  if (matched) {
    return matched;
  }

  return findRateStatsCandidate(
    candidates,
    getStats,
    hasEnoughStats,
    index + RATE_STATS_CANDIDATE_BATCH_SIZE,
  );
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
  const defaultStatsYears =
    raceSource === "nar" ? null : isJraG1ToG3(race) ? null : isListedOrHigher(race) ? 10 : 5;
  const defaultBloodlineStatsYears = 10;
  const defaultStatsIncludeAge = !getAgeLabel(race.kyosoShubetsuCode).includes("4歳以上");
  const defaultSimilarStatsIncludeSex = raceSymbolLabel !== "牝馬限定";
  const parsedRaceRunnerCount = Number(cleanText(race.shussoTosu, "").replace(/[^0-9]/g, ""));
  const currentRunnerCount =
    runners.length > 0
      ? runners.length
      : Number.isFinite(parsedRaceRunnerCount) && parsedRaceRunnerCount > 0
        ? parsedRaceRunnerCount
        : null;
  const buildStatsSettings = (
    prefix: string,
    defaultYearsForPrefix: number | null,
    defaultIncludeSex: boolean,
  ): SimilarRaceStatsSettings => ({
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
    includeDistance: banEiRace
      ? false
      : getFlag(getStatsQueryParam(query, prefix, "statsDistance")),
    includeFrame: getOptionalFlag(getStatsQueryParam(query, prefix, "statsFrame")),
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

  const statsSettings = buildStatsSettings(
    "similar",
    defaultStatsYears,
    defaultSimilarStatsIncludeSex,
  );
  const bloodlineStatsSettings: SimilarRaceStatsSettings = {
    ...buildStatsSettings("bloodline", defaultBloodlineStatsYears, true),
    includeBloodlineAncestors: !getOptionalFlag(
      getStatsQueryParam(query, "bloodline", "statsOffspringOnly"),
    ),
    includeRunnerCount: false,
    runnerCount: null,
  };
  const statsConditionLabels = {
    age: getAgeLabel(race.kyosoShubetsuCode),
    class: statsClassConditionLabel,
    distance: banEiRace ? null : formatDistance(race.kyori),
    frame: "枠番号",
    monthWindow: "開催月±1か月",
    raceNumber: formatRaceNumber(race.raceBango),
    raceSubtitle: raceNameFilterLabels.subtitle,
    raceTitle: raceNameFilterLabels.title,
    runnerCount: null,
    sex: raceSymbolLabel.startsWith("競走記号") ? null : raceSymbolLabel,
    surface: banEiRace ? null : getTrackSurfaceLabel(race.trackCode),
    turn: banEiRace ? null : getTrackTurnLabel(race.trackCode),
    venue: banEiRace ? null : formatKeibajo(keibajoCode),
    weight: getWeightLabel(race.juryoShubetsuCode),
  };
  const conditionAnalysisSettings: SimilarRaceStatsSettings = {
    ...buildStatsSettings("analysis", null, defaultSimilarStatsIncludeSex),
    includeRunnerCount: getDefaultFlag(
      getStatsQueryParam(query, "analysis", "statsRunnerCount"),
      currentRunnerCount !== null,
    ),
    runnerCount: currentRunnerCount,
  };
  const conditionAnalysisLabels = {
    ...statsConditionLabels,
    runnerCount: currentRunnerCount === null ? null : `${currentRunnerCount}頭`,
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

export const getDetailSectionPayload = async (
  section: DetailSection,
  params: DetailSectionParams,
) => {
  const { day, keibajoCode, month, query, raceNumber, raceSource, year } = params;

  if (section === "training") {
    const trainings = await getRaceTrainings(raceSource, year, month, day, keibajoCode, raceNumber);
    return {
      sourceLabel: SOURCE_LABELS[raceSource],
      trainings,
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
    const results = await getHorseRaceResults(
      raceSource,
      year,
      month,
      day,
      keibajoCode,
      raceNumber,
      resultsSourceScope,
    );
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
    let resolvedSettings = context.conditionAnalysisSettings;
    const getConditionAnalysisStats = async (settings: typeof resolvedSettings) =>
      Promise.all([
        getRaceTimeStats(race, settings),
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
    return {
      conditionLabels: context.conditionAnalysisLabels,
      finishPositionStats,
      frameStats,
      payoutStats,
      raceTimeStats,
      runners,
      settings: resolvedSettings,
      source: race.source,
      type: section,
    };
  }

  if (section === "time-score") {
    const rows: TimeScoreRow[] = await getTimeScoreRows(race, context.conditionAnalysisSettings);
    return {
      rows,
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
    const [similarityFeatures, modelPredictionFeatures, sameDayVenueJockeyWins, dbEvaluation] =
      await Promise.all([
        getFinishPositionSimilarityFeatures(race, runners),
        getActiveFinishPositionPredictions(race, runners),
        fetchSameDayVenueJockeyWins(race),
        getActiveFinishPredictionEvaluation(evaluationCategory),
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
    return {
      evaluation:
        evaluationFromDb ??
        getFinishPredictionEvaluation({
          keibajoCode: race.keibajoCode,
          source: race.source,
        }),
      rows: buildFinishPredictionRowsFromResults({
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
      }),
      type: section,
    };
  }

  if (section === "overall-score") {
    const [timeRows, raceTimeStats, bloodlineRows] = await Promise.all([
      getTimeScoreRows(race, context.conditionAnalysisSettings),
      getRaceTimeStats(race, context.conditionAnalysisSettings),
      getBloodlineStats(race, context.bloodlineStatsSettings),
    ]);
    return {
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
    const [similarityFeatures, modelPredictionFeatures] = await Promise.all([
      getRacePaceSimilarityFeatures(race, runners),
      getRacePaceModelPredictionFeatures(race, runners),
    ]);
    return {
      rows: buildRacePacePredictionRowsFromResults({
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
      }),
      supported: true,
      type: section,
    };
  }

  if (section === "bloodline") {
    let resolvedSettings = context.bloodlineStatsSettings;
    let rows = await getBloodlineStats(race, resolvedSettings);
    if (!hasExplicitStatsState(query, "bloodline") && !hasBloodlineScoreCoverage(rows, runners)) {
      const candidates = getConditionAnalysisSettingCandidates(resolvedSettings).slice(1);
      const matched = await findRateStatsCandidate(
        candidates,
        (candidate) => getBloodlineStats(race, candidate),
        (stats) => hasBloodlineScoreCoverage(stats, runners),
      );
      if (matched) {
        resolvedSettings = matched.settings;
        rows = matched.stats;
      }
    }
    return {
      conditionLabels: context.statsConditionLabels,
      rows,
      runners,
      settings: resolvedSettings,
      source: race.source,
      type: section,
    };
  }

  let resolvedSettings = context.statsSettings;
  let rows = await getSimilarRaceStats(race, resolvedSettings);
  if (!hasExplicitStatsState(query, "similar") && !hasSimilarJockeyTrainerCoverage(rows, runners)) {
    const candidates = getConditionAnalysisSettingCandidates(resolvedSettings).slice(1);
    const matched = await findRateStatsCandidate(
      candidates,
      (candidate) => getSimilarRaceStats(race, candidate),
      (stats) => hasSimilarJockeyTrainerCoverage(stats, runners),
    );
    if (matched) {
      resolvedSettings = matched.settings;
      rows = matched.stats;
    }
  }

  return {
    conditionLabels: context.statsConditionLabels,
    rows,
    settings: resolvedSettings,
    source: race.source,
    type: "similar" satisfies DetailSection,
  };
};
