// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)
import {
  FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS,
  type V7LineageCategory,
} from "../scripts/finish-position-features/v7-lineage-model-versions";
import type { RaceListItem } from "./race-types";
import { isBanEiKeibajoCode } from "./runner-format";

export type FinishPositionBucketEvaluationPeriod = "all" | "oos-only";

export type FinishPositionBucketScopeLevel = "exact" | "keibajo" | "category";

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

export interface FinishPositionBucketFilter extends FinishPredictionBucketFilter {
  modelVersion: string;
  period: FinishPositionBucketEvaluationPeriod;
}

export interface FinishPositionBucketScope {
  level: FinishPositionBucketScopeLevel;
  flags: FinishPredictionDimensionFlags;
}

export interface FinishPositionConfidenceInterval {
  lower: number;
  upper: number;
}

export interface FinishPositionBucketMetrics {
  raceCount: number;
  predictionCount: number;
  top1Accuracy: number;
  place1Accuracy: number;
  place2Accuracy: number;
  place3Accuracy: number;
  top3BoxAccuracy: number;
  top3ExactAccuracy: number;
  top3WinnerCaptureRate: number;
  top5WinnerCaptureRate: number;
  top3PlaceRelationAvg: number;
  pairScoreAvg: number;
  ndcgAt3Avg: number;
  top1AccuracyCI: FinishPositionConfidenceInterval;
  smallSampleWarning: boolean;
}

export interface BuildFinishPositionBucketFilterInput {
  race: RaceRowForBucketFilter;
  flags: FinishPredictionDimensionFlags;
  query: Record<string, string | string[] | undefined>;
  modelVersion: string;
}

export interface DeriveFinishPositionWilsonScoreCIInput {
  successes: number;
  trials: number;
}

interface FinishPositionBucketTier {
  level: FinishPositionBucketScopeLevel;
  flags: FinishPredictionDimensionFlags;
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

export const FINISH_POSITION_BUCKET_PERIOD_PARAM_NAME = "fp_period";

// Single source of truth: re-export the per-category v7-lineage model versions
// from the pipeline DRY constant so the viewer queries the exact model_version
// that Stage 4 (evaluate-bucket-21y-v7lineage) persisted into
// model_prediction_bucket_evaluations.
export const FINISH_POSITION_BUCKET_MODEL_VERSIONS = FINISH_POSITION_V7_LINEAGE_MODEL_VERSIONS;

const RACE_NAME_GRADE_CODES = new Set<string>(["A", "F"]);
const CATEGORY_BANEI = "ban-ei";
const CATEGORY_JRA = "jra";
const CATEGORY_NAR = "nar";
const FINISH_POSITION_BUCKET_PERIOD_OOS_ONLY = "oos-only";
const FINISH_POSITION_BUCKET_PERIOD_ALL = "all";
const FINISH_POSITION_WILSON_Z_95 = 1.96;

const FINISH_POSITION_BUCKET_KEIBAJO_ONLY_FLAGS: FinishPredictionDimensionFlags = {
  condition: false,
  distance: false,
  grade: false,
  keibajo: true,
  kyosoJoken: false,
  kyosoShubetsu: false,
  raceName: false,
  track: false,
};

const FINISH_POSITION_BUCKET_CATEGORY_ONLY_FLAGS: FinishPredictionDimensionFlags = {
  condition: false,
  distance: false,
  grade: false,
  keibajo: false,
  kyosoJoken: false,
  kyosoShubetsu: false,
  raceName: false,
  track: false,
};

const FINISH_POSITION_BUCKET_CATEGORY_TO_MODEL: Record<string, V7LineageCategory> = {
  "ban-ei": "banei",
  jra: "jra",
  nar: "nar",
};

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

export const resolveFinishPositionBucketModelVersion = (category: string): string | null => {
  const modelCategory = FINISH_POSITION_BUCKET_CATEGORY_TO_MODEL[category];
  if (modelCategory === undefined) {
    return null;
  }
  return FINISH_POSITION_BUCKET_MODEL_VERSIONS[modelCategory];
};

const resolveFinishPositionPeriod = (
  query: Record<string, string | string[] | undefined>,
): FinishPositionBucketEvaluationPeriod => {
  const raw = query[FINISH_POSITION_BUCKET_PERIOD_PARAM_NAME];
  const value = Array.isArray(raw) ? raw[0] : raw;
  return value === FINISH_POSITION_BUCKET_PERIOD_OOS_ONLY
    ? FINISH_POSITION_BUCKET_PERIOD_OOS_ONLY
    : FINISH_POSITION_BUCKET_PERIOD_ALL;
};

export const buildFinishPositionBucketFilter = (
  input: BuildFinishPositionBucketFilterInput,
): FinishPositionBucketFilter => ({
  ...buildBucketFilter(input.race, input.flags),
  modelVersion: input.modelVersion,
  period: resolveFinishPositionPeriod(input.query),
});

export const buildFinishPositionBucketTiers = (
  flags: FinishPredictionDimensionFlags,
): readonly FinishPositionBucketTier[] => [
  { flags, level: "exact" },
  { flags: FINISH_POSITION_BUCKET_KEIBAJO_ONLY_FLAGS, level: "keibajo" },
  { flags: FINISH_POSITION_BUCKET_CATEGORY_ONLY_FLAGS, level: "category" },
];

export const deriveFinishPositionWilsonScoreCI = (
  input: DeriveFinishPositionWilsonScoreCIInput,
): FinishPositionConfidenceInterval => {
  const { successes, trials } = input;
  if (trials === 0) {
    return { lower: 0, upper: 0 };
  }
  const z = FINISH_POSITION_WILSON_Z_95;
  const proportion = successes / trials;
  const zSquared = z * z;
  const denominator = 1 + zSquared / trials;
  const center = (proportion + zSquared / (2 * trials)) / denominator;
  const marginNumerator = Math.sqrt(
    (proportion * (1 - proportion)) / trials + zSquared / (4 * trials * trials),
  );
  const margin = (z * marginNumerator) / denominator;
  const lower = Math.max(0, center - margin);
  const upper = Math.min(1, center + margin);
  return { lower, upper };
};
