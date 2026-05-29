// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)
import type { RaceListItem } from "./race-types";
import { isBanEiKeibajoCode } from "./runner-format";

export interface FinishPredictionDimensionFlags {
  keibajo: boolean;
  distance: boolean;
  kyosoShubetsu: boolean;
  kyosoJoken: boolean;
  condition: boolean;
  track: boolean;
  grade: boolean;
  raceName: boolean;
}

export interface FinishPredictionBucketFilter {
  category: string;
  source: "jra" | "nar";
  keibajoCode: string;
  kyori: number;
  kyosoShubetsuCode: string;
  kyosoJokenCode: string | null;
  conditionKey: string | null;
  trackCode: string | null;
  gradeCode: string | null;
  raceName: string | null;
  enabled: FinishPredictionDimensionFlags;
}

export interface RunningStyleLocalPredictionRaceKey {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  kettoTorokuBango: string;
}

export interface RunningStyleLocalPrediction {
  predictedLabel: string;
  pNige: number;
  pSenkou: number;
  pSashi: number;
  pOikomi: number;
  featureVersion: string;
}

export interface RunningStyleLocalLoaderInterface {
  get(raceKey: RunningStyleLocalPredictionRaceKey): RunningStyleLocalPrediction | null;
}

export interface FinishPositionLocalPrediction {
  predictedScore: number;
  predictedRank: number;
  predictedTop1Prob: number;
  predictedTop3Prob: number;
  predictedFinishPosition: number;
  modelVersion: string;
  runningStyleFeatureVersion: string;
  finishPositionVersion: string;
}

export interface FinishPositionLocalLoaderInterface {
  get(raceKey: RunningStyleLocalPredictionRaceKey): FinishPositionLocalPrediction | null;
}

export type RaceRowForBucketFilter = Pick<
  RaceListItem,
  | "keibajoCode"
  | "kyori"
  | "kyosoShubetsuCode"
  | "kyosoJokenCode"
  | "kyosoJokenMeisho"
  | "trackCode"
  | "gradeCode"
  | "kyosomeiHondai"
> & {
  source: "jra" | "nar";
  conditionKey: string | null;
  raceName: string | null;
};

export interface GetFinishPredictionDimensionFlagsInput {
  query: Record<string, string | string[] | undefined>;
  source: "jra" | "nar";
  gradeCode: string | null;
  isBanEi: boolean;
}

export const FINISH_PREDICTION_PARAM_NAMES = {
  keibajo: "finishPredictionKeibajo",
  distance: "finishPredictionDistance",
  kyosoShubetsu: "finishPredictionShubetsu",
  kyosoJoken: "finishPredictionJoken",
  condition: "finishPredictionCondition",
  track: "finishPredictionTrack",
  grade: "finishPredictionGrade",
  raceName: "finishPredictionRaceName",
} satisfies Record<keyof FinishPredictionDimensionFlags, string>;

const RACE_NAME_GRADE_CODES = new Set<string>(["A", "F"]);
const CATEGORY_BANEI = "ban-ei";
const CATEGORY_JRA = "jra";
const CATEGORY_NAR = "nar";

const readFlag = (query: Record<string, string | string[] | undefined>, name: string): boolean => {
  const raw = query[name];
  const value = Array.isArray(raw) ? raw[0] : raw;
  return value !== "0";
};

const isGradeEligible = (gradeCode: string | null): boolean =>
  gradeCode !== null && gradeCode !== "";

const isRaceNameEligible = (gradeCode: string | null): boolean =>
  gradeCode !== null && RACE_NAME_GRADE_CODES.has(gradeCode);

export const getFinishPredictionDimensionFlags = (
  input: GetFinishPredictionDimensionFlagsInput,
): FinishPredictionDimensionFlags => {
  const { query, source, gradeCode, isBanEi } = input;
  const keibajo = readFlag(query, FINISH_PREDICTION_PARAM_NAMES.keibajo);
  const distance = readFlag(query, FINISH_PREDICTION_PARAM_NAMES.distance);
  const kyosoShubetsu = readFlag(query, FINISH_PREDICTION_PARAM_NAMES.kyosoShubetsu);
  const kyosoJokenRaw = readFlag(query, FINISH_PREDICTION_PARAM_NAMES.kyosoJoken);
  const conditionRaw = readFlag(query, FINISH_PREDICTION_PARAM_NAMES.condition);
  const trackRaw = readFlag(query, FINISH_PREDICTION_PARAM_NAMES.track);
  const gradeRaw = readFlag(query, FINISH_PREDICTION_PARAM_NAMES.grade);
  const raceNameRaw = readFlag(query, FINISH_PREDICTION_PARAM_NAMES.raceName);
  const kyosoJoken = source === "nar" ? false : kyosoJokenRaw;
  const condition = source === "jra" ? false : conditionRaw;
  const track = isBanEi ? false : trackRaw;
  const gradeAllowed = source !== "jra" && isGradeEligible(gradeCode);
  const grade = gradeAllowed ? gradeRaw : false;
  const raceName = isRaceNameEligible(gradeCode) ? raceNameRaw : false;
  return {
    keibajo,
    distance,
    kyosoShubetsu,
    kyosoJoken,
    condition,
    track,
    grade,
    raceName,
  };
};

const resolveCategory = (row: RaceRowForBucketFilter): string => {
  if (isBanEiKeibajoCode(row.keibajoCode)) {
    return CATEGORY_BANEI;
  }
  return row.source === "nar" ? CATEGORY_NAR : CATEGORY_JRA;
};

const parseKyori = (value: string | null): number => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

export const buildBucketFilter = (
  race: RaceRowForBucketFilter,
  flags: FinishPredictionDimensionFlags,
): FinishPredictionBucketFilter => {
  const category = resolveCategory(race);
  const kyori = parseKyori(race.kyori);
  return {
    category,
    source: race.source,
    keibajoCode: race.keibajoCode,
    kyori,
    kyosoShubetsuCode: race.kyosoShubetsuCode ?? "",
    kyosoJokenCode: flags.kyosoJoken ? race.kyosoJokenCode : null,
    conditionKey: flags.condition ? race.conditionKey : null,
    trackCode: flags.track ? race.trackCode : null,
    gradeCode: flags.grade ? race.gradeCode : null,
    raceName: flags.raceName ? race.raceName : null,
    enabled: flags,
  };
};
