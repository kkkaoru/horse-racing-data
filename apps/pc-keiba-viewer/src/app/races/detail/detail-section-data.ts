import "server-only";
import {
  getBloodlineStats,
  getFinishPositionStats,
  getFrameStats,
  getHorseRaceResults,
  getPayoutStats,
  getRaceAbilityTests,
  getRaceDetail,
  getRaceRunners,
  getRaceTimeStats,
  getRaceTrainings,
  getSimilarRaceStats,
} from "../../../db/queries";
import { SOURCE_LABELS, type RaceSource } from "../../../lib/codes";
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
import type {
  BloodlineStatsRow,
  FinishPositionStatsRow,
  FrameStatsRow,
  PayoutStatsRow,
  RaceDetail,
  RaceTimeStats,
  SimilarRaceStatsRow,
  SimilarRaceStatsSettings,
} from "../../../lib/race-types";
import { isBanEiKeibajoCode } from "../../../lib/runner-format";

export type DetailSection =
  | "ability"
  | "bloodline"
  | "condition"
  | "results"
  | "similar"
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

const CONDITION_ANALYSIS_OVERRIDE_PARAMS = [
  "statsAge",
  "statsClass",
  "statsDistance",
  "statsFrame",
  "statsMonthWindow",
  "statsRaceMonth",
  "statsRaceName",
  "statsRaceNumber",
  "statsRaceSubtitle",
  "statsRaceTitle",
  "statsRunnerCount",
  "statsSex",
  "statsSurface",
  "statsTrack",
  "statsTurn",
  "statsVenue",
  "statsWeight",
];

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

const hasSearchParam = (
  query: Record<string, string | string[] | undefined>,
  names: string[],
): boolean => names.some((name) => getFirstSearchParam(query[name]) !== undefined);

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
  const hasNamedClass =
    grade.length > 0 || /G[1-3]|Jpn[1-3]|リステッド|OP|ＯＰ|オープン/.test(`${tags} ${condition}`);
  const hasSpecialRaceName = title.includes("ファイナルレース") || subtitle.includes("一発逆転");

  if (!hasNamedClass && !hasSpecialRaceName) {
    return { subtitle: null, title: null };
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
  const [timeStats, payoutRows, finishRows, frameRows] = stats;
  return (
    timeStats.raceCount > 0 &&
    payoutRows.some((row) => row.count > 0) &&
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
  index = 0,
): Promise<{ settings: T; stats: R } | null> => {
  const settings = candidates[index];

  if (!settings) {
    return null;
  }

  const stats = await getStats(settings);

  if (hasRateRows(stats)) {
    return { settings, stats };
  }

  return findRateStatsCandidate(candidates, getStats, index + 1);
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
  const defaultStatsIncludeAge = !getAgeLabel(race.kyosoShubetsuCode).includes("4歳以上");
  const defaultSimilarStatsIncludeSex = raceSymbolLabel !== "牝馬限定";
  const parsedRaceRunnerCount = Number(cleanText(race.shussoTosu, "").replace(/[^0-9]/g, ""));
  const currentRunnerCount =
    runners.length > 0
      ? runners.length
      : Number.isFinite(parsedRaceRunnerCount) && parsedRaceRunnerCount > 0
        ? parsedRaceRunnerCount
        : null;

  const statsSettings: SimilarRaceStatsSettings = {
    classConditionName: statsClassConditionLabel,
    includeAge: getDefaultFlag(query.statsAge ?? query.statsClass, defaultStatsIncludeAge),
    includeClass: getDefaultFlag(query.statsClass, Boolean(statsClassConditionLabel)),
    includeDistance: banEiRace ? false : getFlag(query.statsDistance),
    includeFrame: getOptionalFlag(query.statsFrame),
    includeMonthWindow: getOptionalFlag(query.statsRaceMonth ?? query.statsMonthWindow),
    includeRaceNumber: getOptionalFlag(query.statsRaceNumber),
    includeRaceSubtitle: getDefaultFlag(
      query.statsRaceSubtitle ?? query.statsRaceName,
      Boolean(raceNameFilterLabels.subtitle),
    ),
    includeRaceTitle: getDefaultFlag(
      query.statsRaceTitle ?? query.statsRaceName,
      Boolean(raceNameFilterLabels.title),
    ),
    includeRunnerCount: false,
    includeSex: getDefaultFlag(query.statsSex, defaultSimilarStatsIncludeSex),
    includeSurface: banEiRace ? false : getFlag(query.statsSurface ?? query.statsTrack),
    includeTurn: banEiRace ? false : getFlag(query.statsTurn ?? query.statsTrack),
    includeVenue: banEiRace ? false : getDefaultFlag(query.statsVenue, true),
    includeWeight: getFlag(query.statsWeight),
    runnerCount: null,
    years: getStatsYears(query.statsYears, defaultStatsYears),
  };
  const bloodlineStatsSettings: SimilarRaceStatsSettings = {
    ...statsSettings,
    includeRunnerCount: false,
    includeSex: getFlag(query.statsSex),
    runnerCount: null,
    years: getStatsYears(query.statsYears, null),
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
    ...statsSettings,
    includeRunnerCount: getDefaultFlag(query.statsRunnerCount, currentRunnerCount !== null),
    runnerCount: currentRunnerCount,
    years: getStatsYears(query.statsYears, null),
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
    const results = await getHorseRaceResults(
      raceSource,
      year,
      month,
      day,
      keibajoCode,
      raceNumber,
    );
    return {
      classConditionName: context.statsClassConditionLabel,
      currentDistance: race.kyori,
      currentKeibajoCode: race.keibajoCode,
      currentRaceDate: `${year}${month}${day}`,
      defaultIncludeClass: context.statsSettings.includeClass,
      results,
      runners,
      source: raceSource,
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
    if (
      !hasSearchParam(query, CONDITION_ANALYSIS_OVERRIDE_PARAMS) &&
      !hasCompleteConditionAnalysisRows(stats)
    ) {
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
      settings: resolvedSettings,
      type: section,
    };
  }

  if (section === "bloodline") {
    let resolvedSettings = context.bloodlineStatsSettings;
    let rows = await getBloodlineStats(race, resolvedSettings);
    if (!hasSearchParam(query, CONDITION_ANALYSIS_OVERRIDE_PARAMS) && !hasRateRows(rows)) {
      const candidates = getConditionAnalysisSettingCandidates(resolvedSettings).slice(1);
      const matched = await findRateStatsCandidate(candidates, (candidate) =>
        getBloodlineStats(race, candidate),
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
      type: section,
    };
  }

  let resolvedSettings = context.statsSettings;
  let rows = await getSimilarRaceStats(race, resolvedSettings);
  if (!hasSearchParam(query, CONDITION_ANALYSIS_OVERRIDE_PARAMS) && !hasRateRows(rows)) {
    const candidates = getConditionAnalysisSettingCandidates(resolvedSettings).slice(1);
    const matched = await findRateStatsCandidate(candidates, (candidate) =>
      getSimilarRaceStats(race, candidate),
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
    type: "similar" satisfies DetailSection,
  };
};
