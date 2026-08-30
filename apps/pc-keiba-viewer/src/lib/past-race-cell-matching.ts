// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)
import type { RaceSource } from "./codes";
import type { FinishPredictionDimensionFlags } from "./finish-prediction-dimensions";
import type { SimilarRaceStatsSettings } from "./race-types";

export const ANALYSIS_CELL_PARAM_NAMES = {
  keibajo: "analysisCellKeibajo",
  distance: "analysisCellDistance",
  kyosoShubetsu: "analysisCellShubetsu",
  kyosoJoken: "analysisCellJoken",
  condition: "analysisCellCondition",
  track: "analysisCellTrack",
  grade: "analysisCellGrade",
  raceName: "analysisCellRaceName",
} satisfies Record<keyof FinishPredictionDimensionFlags, string>;

export interface BuildCellMatchingStatsSettingsInput {
  classConditionName: string | null;
  flags: FinishPredictionDimensionFlags;
  sourceScope: RaceSource | "all";
  years: number | null;
}

export const buildCellMatchingStatsSettings = (
  input: BuildCellMatchingStatsSettingsInput,
): SimilarRaceStatsSettings => ({
  cellMatching: true,
  classConditionName: input.classConditionName,
  includeAge: input.flags.kyosoShubetsu,
  includeBloodlineAncestors: true,
  includeClass: input.flags.kyosoJoken,
  includeConditionKey: input.flags.condition,
  includeDistance: input.flags.distance,
  includeFrame: false,
  includeGrade: input.flags.grade,
  includeMonthWindow: false,
  includeNarOnly: input.sourceScope === "nar",
  includeRaceNumber: false,
  includeRaceSubtitle: false,
  includeRaceTitle: input.flags.raceName,
  includeRunnerCount: false,
  includeSex: false,
  includeSurface: false,
  includeTrackCode: input.flags.track,
  includeTurn: false,
  includeVenue: input.flags.keibajo,
  includeWeight: false,
  runnerCount: null,
  sourceScope: input.sourceScope,
  years: input.years,
});

export const withDisabledCellDimensions = (
  settings: SimilarRaceStatsSettings,
  keepVenue: boolean,
): SimilarRaceStatsSettings => ({
  ...settings,
  includeAge: false,
  includeClass: false,
  includeConditionKey: false,
  includeDistance: false,
  includeGrade: false,
  includeRaceTitle: false,
  includeTrackCode: false,
  includeVenue: keepVenue,
});

export const toConditionAnalysisFallbackCell = (
  settings: SimilarRaceStatsSettings,
  keepVenue: boolean,
): SimilarRaceStatsSettings => ({
  ...withDisabledCellDimensions(settings, keepVenue),
  includeDistance: settings.includeDistance,
});
