import type { RaceSource } from "./codes";
import { cleanText } from "./format";
import type {
  HorseRaceResult,
  RacePaceModelPredictionFeature,
  RacePacePredictionDetail,
  RacePacePredictionRow,
  RacePaceSimilarityFeature,
  Runner,
} from "./race-types";
import { isBanEiKeibajoCode } from "./runner-format";

interface BuildRacePacePredictionRowsParams {
  currentConditionCode?: string | null;
  currentConditionName?: string | null;
  currentDistance: string | null | undefined;
  currentGradeCode?: string | null;
  currentRaceAgeCode?: string | null;
  currentRaceDate: string;
  currentRunnerCount?: number | null;
  currentSource?: RaceSource;
  currentTrackCode?: string | null;
  model?: RacePacePredictionModel;
  modelPredictionFeatures?: RacePaceModelPredictionFeature[];
  results: HorseRaceResult[];
  runners: Runner[];
  similarityFeatures?: RacePaceSimilarityFeature[];
}

export const RACE_PACE_PREDICTION_RESULTS_EVENT = "pc-keiba:race-pace-prediction-results";

const JRA_RECENCY_DECAY_DAYS = 60;
const NAR_RECENCY_DECAY_DAYS = 30;
const GRADE_CODES = new Set(["A", "B", "C", "D", "E", "F", "G", "H", "L"]);

type RacePacePredictionConfig = {
  decayDays: number;
  distanceBandMeters: number;
  distanceScale: number;
  horseWeight: number;
  jockeyWeight: number;
  lowSampleHorseNumberPriorWeight: number;
  lowSamplePriorMaxWeight: number;
  minDistanceWeight: number;
  modelPredictionWeight: number;
  popularityPriorFloorWeight: number;
  similarityWeight: number;
  trainerWeight: number;
};

export type RacePacePredictionModel = {
  jraDecayMultiplier: number;
  jraHorseWeightMultiplier: number;
  jraJockeyWeightMultiplier: number;
  jraLowSampleHorseNumberPriorWeight: number;
  jraLowSamplePriorMultiplier: number;
  jraModelPredictionWeight: number;
  jraPopularityPriorFloorWeight: number;
  jraSimilarityWeight: number;
  jraTrainerWeightMultiplier: number;
  narDecayMultiplier: number;
  narHorseWeightMultiplier: number;
  narJockeyWeightMultiplier: number;
  narLowSampleHorseNumberPriorWeight: number;
  narLowSamplePriorMultiplier: number;
  narModelPredictionWeight: number;
  narPopularityPriorFloorWeight: number;
  narSimilarityWeight: number;
  narTrainerWeightMultiplier: number;
};

export const DEFAULT_RACE_PACE_PREDICTION_MODEL: RacePacePredictionModel = {
  jraDecayMultiplier: 1,
  jraHorseWeightMultiplier: 1,
  jraJockeyWeightMultiplier: 1,
  jraLowSampleHorseNumberPriorWeight: 0,
  jraLowSamplePriorMultiplier: 1,
  jraModelPredictionWeight: 0.04,
  jraPopularityPriorFloorWeight: 0.15,
  jraSimilarityWeight: 0.08,
  jraTrainerWeightMultiplier: 1,
  narDecayMultiplier: 1,
  narHorseWeightMultiplier: 1,
  narJockeyWeightMultiplier: 1,
  narLowSampleHorseNumberPriorWeight: 0,
  narLowSamplePriorMultiplier: 1,
  narModelPredictionWeight: 0,
  narPopularityPriorFloorWeight: 0,
  narSimilarityWeight: 0.1,
  narTrainerWeightMultiplier: 1,
};

export const isCornerPacePredictionSupported = ({
  distance,
  keibajoCode,
  source,
}: {
  distance: string | null | undefined;
  keibajoCode: string | null | undefined;
  source: RaceSource;
}): boolean => {
  if (source === "nar" && isBanEiKeibajoCode(keibajoCode)) {
    return false;
  }
  return !(cleanText(keibajoCode, "") === "04" && cleanText(distance, "") === "1000");
};

const clampScore = (value: number): number => Math.max(0, Math.min(1, value));

const roundScore = (value: number): number => Math.round(value * 100) / 100;

const parseStoredNumber = (value: string | null | undefined, emptyValue: string): number | null => {
  const cleaned = cleanText(value, "");
  if (!cleaned || cleaned === emptyValue) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseCorner = (value: string | null | undefined): number | null => {
  const parsed = parseStoredNumber(value, "00");
  return parsed === null || parsed <= 0 ? null : parsed;
};

const parseRunnerCount = (value: string | null | undefined): number | null => {
  const parsed = parseStoredNumber(value, "00");
  return parsed === null || parsed <= 1 ? null : parsed;
};

const isGradedRace = (gradeCode: string | null | undefined): boolean =>
  GRADE_CODES.has(cleanText(gradeCode, ""));

const isYoungOrMaidenRace = ({
  conditionName,
  raceAgeCode,
}: {
  conditionName: string | null | undefined;
  raceAgeCode: string | null | undefined;
}): boolean => {
  const condition = cleanText(conditionName, "");
  const ageCode = cleanText(raceAgeCode, "");
  return (
    condition.includes("新馬") ||
    condition.includes("未勝利") ||
    condition.includes("２歳") ||
    condition.includes("2歳") ||
    ageCode === "11" ||
    ageCode === "12"
  );
};

const isNarClassRace = ({
  conditionCode,
  conditionName,
}: {
  conditionCode: string | null | undefined;
  conditionName: string | null | undefined;
}): boolean => {
  const code = cleanText(conditionCode, "");
  const condition = cleanText(conditionName, "");
  return code === "000" || /^[ABC]\d?/u.test(condition);
};

const getBasePredictionConfig = ({
  conditionCode,
  conditionName,
  gradeCode,
  model,
  raceAgeCode,
  source,
}: {
  conditionCode: string | null | undefined;
  conditionName: string | null | undefined;
  gradeCode: string | null | undefined;
  model: RacePacePredictionModel;
  raceAgeCode: string | null | undefined;
  source: RaceSource | undefined;
}): RacePacePredictionConfig => {
  const graded = isGradedRace(gradeCode);
  const youngOrMaiden = isYoungOrMaidenRace({ conditionName, raceAgeCode });
  if (source === "nar") {
    const classRace = isNarClassRace({ conditionCode, conditionName });
    return {
      decayDays: (classRace ? 24 : NAR_RECENCY_DECAY_DAYS) * model.narDecayMultiplier,
      distanceBandMeters: 400,
      distanceScale: 0.5,
      horseWeight: (graded ? 0.74 : 0.68) * model.narHorseWeightMultiplier,
      jockeyWeight: (graded ? 0.14 : 0.16) * model.narJockeyWeightMultiplier,
      lowSampleHorseNumberPriorWeight: model.narLowSampleHorseNumberPriorWeight,
      lowSamplePriorMaxWeight: (graded ? 0.22 : 0.34) * model.narLowSamplePriorMultiplier,
      minDistanceWeight: 0.25,
      modelPredictionWeight: model.narModelPredictionWeight,
      popularityPriorFloorWeight: model.narPopularityPriorFloorWeight,
      similarityWeight: model.narSimilarityWeight,
      trainerWeight: (graded ? 0.12 : 0.16) * model.narTrainerWeightMultiplier,
    };
  }
  return {
    decayDays:
      (youngOrMaiden ? 35 : graded ? 75 : JRA_RECENCY_DECAY_DAYS) * model.jraDecayMultiplier,
    distanceBandMeters: 400,
    distanceScale: 0.5,
    horseWeight: (graded ? 0.74 : youngOrMaiden ? 0.62 : 0.7) * model.jraHorseWeightMultiplier,
    jockeyWeight: (graded ? 0.14 : youngOrMaiden ? 0.2 : 0.15) * model.jraJockeyWeightMultiplier,
    lowSampleHorseNumberPriorWeight: model.jraLowSampleHorseNumberPriorWeight,
    lowSamplePriorMaxWeight:
      (graded ? 0.22 : youngOrMaiden ? 0.38 : 0.32) * model.jraLowSamplePriorMultiplier,
    minDistanceWeight: 0.25,
    modelPredictionWeight: model.jraModelPredictionWeight,
    popularityPriorFloorWeight: model.jraPopularityPriorFloorWeight,
    similarityWeight: model.jraSimilarityWeight,
    trainerWeight: (graded ? 0.12 : youngOrMaiden ? 0.18 : 0.15) * model.jraTrainerWeightMultiplier,
  };
};

const getHorseHistoryAdjustedConfig = (
  baseConfig: RacePacePredictionConfig,
  horseResultsCount: number,
): RacePacePredictionConfig => {
  if (horseResultsCount <= 1) {
    return {
      ...baseConfig,
      horseWeight: Math.max(0.5, baseConfig.horseWeight - 0.1),
      jockeyWeight: baseConfig.jockeyWeight + 0.05,
      lowSamplePriorMaxWeight: Math.min(0.48, baseConfig.lowSamplePriorMaxWeight + 0.04),
      trainerWeight: baseConfig.trainerWeight + 0.05,
    };
  }
  if (horseResultsCount >= 6) {
    return {
      ...baseConfig,
      horseWeight: Math.min(0.78, baseConfig.horseWeight + 0.06),
      jockeyWeight: Math.max(0.1, baseConfig.jockeyWeight - 0.03),
      lowSamplePriorMaxWeight: Math.max(0.08, baseConfig.lowSamplePriorMaxWeight - 0.06),
      trainerWeight: Math.max(0.08, baseConfig.trainerWeight - 0.03),
    };
  }
  return baseConfig;
};

const normalizeHorseNumber = (value: string | null | undefined): string => {
  const cleaned = cleanText(value, "");
  return cleaned.replace(/^0+/u, "") || cleaned;
};

const getRaceDateValue = (row: Pick<HorseRaceResult, "kaisaiNen" | "kaisaiTsukihi">): number => {
  const parsed = Number(`${row.kaisaiNen}${row.kaisaiTsukihi}`);
  return Number.isFinite(parsed) ? parsed : 0;
};

const getRaceDateWeight = (
  currentRaceDate: string,
  config: RacePacePredictionConfig,
  row: Pick<HorseRaceResult, "kaisaiNen" | "kaisaiTsukihi">,
): number => {
  const currentDate = new Date(
    `${currentRaceDate.slice(0, 4)}-${currentRaceDate.slice(4, 6)}-${currentRaceDate.slice(6, 8)}T00:00:00+09:00`,
  );
  const pastRaceDate = `${row.kaisaiNen}${row.kaisaiTsukihi}`;
  const pastDate = new Date(
    `${pastRaceDate.slice(0, 4)}-${pastRaceDate.slice(4, 6)}-${pastRaceDate.slice(6, 8)}T00:00:00+09:00`,
  );
  const elapsedDays = Math.max(0, (currentDate.getTime() - pastDate.getTime()) / 86_400_000);
  return 1 / (1 + elapsedDays / Math.max(1, config.decayDays));
};

const getDistanceWeight = (
  currentDistance: string | null | undefined,
  resultDistance: string | null | undefined,
  config: RacePacePredictionConfig,
): number => {
  const current = parseStoredNumber(currentDistance, "");
  const past = parseStoredNumber(resultDistance, "");
  if (current === null || past === null) {
    return 0.75;
  }
  return Math.max(
    config.minDistanceWeight,
    1 -
      Math.abs(current - past) /
        Math.max(current * config.distanceScale, config.distanceBandMeters),
  );
};

const getTrackWeight = (
  currentTrackCode: string | null | undefined,
  resultTrackCode: string | null | undefined,
): number => {
  const current = cleanText(currentTrackCode, "");
  const past = cleanText(resultTrackCode, "");
  if (!current || !past) {
    return 0.85;
  }
  if (current === past) {
    return 1.2;
  }
  return current.slice(0, 1) === past.slice(0, 1) ? 1 : 0.55;
};

const getWeightedCornerAverage = (
  results: HorseRaceResult[],
  cornerKey: "corner1" | "corner2" | "corner3" | "corner4",
  currentRaceDate: string,
  currentDistance: string | null | undefined,
  currentRunnerCount: number | null,
  currentTrackCode: string | null | undefined,
  config: RacePacePredictionConfig,
): number | null => {
  let total = 0;
  let weightTotal = 0;
  for (const result of results) {
    const corner = parseCorner(result[cornerKey]);
    if (corner === null) {
      continue;
    }
    const weight =
      getRaceDateWeight(currentRaceDate, config, result) *
      getDistanceWeight(currentDistance, result.kyori, config) *
      getTrackWeight(currentTrackCode, result.trackCode);
    const pastRunnerCount = parseRunnerCount(result.shussoTosu);
    const normalizedCorner =
      pastRunnerCount !== null && currentRunnerCount !== null
        ? ((corner - 1) / (pastRunnerCount - 1)) * (currentRunnerCount - 1) + 1
        : corner;
    total += normalizedCorner * weight;
    weightTotal += weight;
  }
  return weightTotal > 0 ? total / weightTotal : null;
};

const formatPredictedCorners = (corners: Array<number | null>): string => {
  const formatted = corners.map((corner) =>
    corner === null ? "-" : `${Math.max(1, Math.round(corner))}`,
  );
  return formatted.some((corner) => corner !== "-") ? formatted.join("-") : "-";
};

const averageNullable = (values: Array<number | null>): number | null => {
  const numbers = values.filter((value): value is number => value !== null);
  return numbers.length > 0
    ? numbers.reduce((total, value) => total + value, 0) / numbers.length
    : null;
};

const getRunnerNumberPrior = (runner: Runner, currentRunnerCount: number | null): number | null => {
  const horseNumber = parseStoredNumber(runner.umaban, "");
  if (horseNumber === null || currentRunnerCount === null || currentRunnerCount <= 1) {
    return null;
  }
  return Math.max(1, Math.min(currentRunnerCount, horseNumber));
};

const getPopularityPrior = (runner: Runner, currentRunnerCount: number | null): number | null => {
  const popularity = parseStoredNumber(runner.tanshoNinkijun, "00");
  if (popularity === null || currentRunnerCount === null || currentRunnerCount <= 1) {
    return null;
  }
  return Math.max(1, Math.min(currentRunnerCount, popularity));
};

const blendLowSamplePrior = ({
  currentRunnerCount,
  horseNumberPriorWeight,
  lowSamplePriorMaxWeight,
  popularityPriorFloorWeight,
  horseResultsCount,
  predictedCorner,
  runner,
}: {
  currentRunnerCount: number | null;
  horseNumberPriorWeight: number;
  horseResultsCount: number;
  lowSamplePriorMaxWeight: number;
  popularityPriorFloorWeight: number;
  predictedCorner: number | null;
  runner: Runner;
}): number | null => {
  if (predictedCorner === null) {
    return null;
  }
  const popularityPrior = getPopularityPrior(runner, currentRunnerCount);
  const horseNumberPrior = getRunnerNumberPrior(runner, currentRunnerCount);
  const weightedPriors = [
    { value: popularityPrior, weight: 1 },
    { value: horseNumberPrior, weight: horseNumberPriorWeight },
  ].filter(
    (item): item is { value: number; weight: number } => item.value !== null && item.weight > 0,
  );
  const priorWeightTotal = weightedPriors.reduce((total, item) => total + item.weight, 0);
  if (priorWeightTotal <= 0) {
    return predictedCorner;
  }
  const priorAverage =
    weightedPriors.reduce((total, item) => total + item.value * item.weight, 0) / priorWeightTotal;
  const priorWeight = Math.max(
    clampScore(popularityPriorFloorWeight),
    clampScore(lowSamplePriorMaxWeight) * Math.max(0, 1 - horseResultsCount / 4),
  );
  return predictedCorner * (1 - priorWeight) + priorAverage * priorWeight;
};

export const buildRacePacePredictionRowsFromResults = ({
  currentConditionCode,
  currentConditionName,
  currentDistance,
  currentGradeCode,
  currentRaceAgeCode,
  currentRaceDate,
  currentRunnerCount,
  currentSource,
  currentTrackCode,
  model = DEFAULT_RACE_PACE_PREDICTION_MODEL,
  modelPredictionFeatures = [],
  results,
  runners,
  similarityFeatures = [],
}: BuildRacePacePredictionRowsParams): RacePacePredictionRow[] => {
  const resolvedCurrentRunnerCount =
    typeof currentRunnerCount === "number" && Number.isFinite(currentRunnerCount)
      ? currentRunnerCount
      : runners.length > 1
        ? runners.length
        : null;
  const baseConfig = getBasePredictionConfig({
    conditionCode: currentConditionCode,
    conditionName: currentConditionName,
    gradeCode: currentGradeCode,
    model,
    raceAgeCode: currentRaceAgeCode,
    source: currentSource,
  });
  const resultsByHorse = new Map<string, HorseRaceResult[]>();
  const modelPredictionByHorse = new Map<string, RacePaceModelPredictionFeature>();
  const similarityByHorse = new Map<string, RacePaceSimilarityFeature>();
  for (const feature of modelPredictionFeatures) {
    modelPredictionByHorse.set(normalizeHorseNumber(feature.horseNumber), feature);
  }
  for (const feature of similarityFeatures) {
    similarityByHorse.set(normalizeHorseNumber(feature.horseNumber), feature);
  }
  for (const result of results) {
    const horseNumber = normalizeHorseNumber(result.currentUmaban);
    if (!horseNumber) {
      continue;
    }
    const current = resultsByHorse.get(horseNumber) ?? [];
    current.push(result);
    resultsByHorse.set(horseNumber, current);
  }

  return runners
    .map((runner): RacePacePredictionRow => {
      const horseNumber = normalizeHorseNumber(runner.umaban);
      const horseResults = (resultsByHorse.get(horseNumber) ?? []).toSorted(
        (left, right) => getRaceDateValue(right) - getRaceDateValue(left),
      );
      const currentJockey = cleanText(runner.kishumeiRyakusho);
      const currentTrainer = cleanText(runner.chokyoshimeiRyakusho);
      const jockeyResults = horseResults.filter(
        (result) => cleanText(result.kishumeiRyakusho) === currentJockey,
      );
      const trainerResults = horseResults.filter(
        (result) => cleanText(result.chokyoshimeiRyakusho) === currentTrainer,
      );
      const config = getHorseHistoryAdjustedConfig(baseConfig, horseResults.length);
      const horseCornerAverages = [
        getWeightedCornerAverage(
          horseResults,
          "corner1",
          currentRaceDate,
          currentDistance,
          resolvedCurrentRunnerCount,
          currentTrackCode,
          config,
        ),
        getWeightedCornerAverage(
          horseResults,
          "corner2",
          currentRaceDate,
          currentDistance,
          resolvedCurrentRunnerCount,
          currentTrackCode,
          config,
        ),
        getWeightedCornerAverage(
          horseResults,
          "corner3",
          currentRaceDate,
          currentDistance,
          resolvedCurrentRunnerCount,
          currentTrackCode,
          config,
        ),
        getWeightedCornerAverage(
          horseResults,
          "corner4",
          currentRaceDate,
          currentDistance,
          resolvedCurrentRunnerCount,
          currentTrackCode,
          config,
        ),
      ];
      const jockeyCornerAverages = [
        getWeightedCornerAverage(
          jockeyResults,
          "corner1",
          currentRaceDate,
          currentDistance,
          resolvedCurrentRunnerCount,
          currentTrackCode,
          config,
        ),
        getWeightedCornerAverage(
          jockeyResults,
          "corner2",
          currentRaceDate,
          currentDistance,
          resolvedCurrentRunnerCount,
          currentTrackCode,
          config,
        ),
        getWeightedCornerAverage(
          jockeyResults,
          "corner3",
          currentRaceDate,
          currentDistance,
          resolvedCurrentRunnerCount,
          currentTrackCode,
          config,
        ),
        getWeightedCornerAverage(
          jockeyResults,
          "corner4",
          currentRaceDate,
          currentDistance,
          resolvedCurrentRunnerCount,
          currentTrackCode,
          config,
        ),
      ];
      const trainerCornerAverages = [
        getWeightedCornerAverage(
          trainerResults,
          "corner1",
          currentRaceDate,
          currentDistance,
          resolvedCurrentRunnerCount,
          currentTrackCode,
          config,
        ),
        getWeightedCornerAverage(
          trainerResults,
          "corner2",
          currentRaceDate,
          currentDistance,
          resolvedCurrentRunnerCount,
          currentTrackCode,
          config,
        ),
        getWeightedCornerAverage(
          trainerResults,
          "corner3",
          currentRaceDate,
          currentDistance,
          resolvedCurrentRunnerCount,
          currentTrackCode,
          config,
        ),
        getWeightedCornerAverage(
          trainerResults,
          "corner4",
          currentRaceDate,
          currentDistance,
          resolvedCurrentRunnerCount,
          currentTrackCode,
          config,
        ),
      ];
      const similarityFeature = similarityByHorse.get(horseNumber);
      const similarityCornerAverages = similarityFeature
        ? [
            similarityFeature.corner1,
            similarityFeature.corner2,
            similarityFeature.corner3,
            similarityFeature.corner4,
          ]
        : [null, null, null, null];
      const modelPredictionFeature = modelPredictionByHorse.get(horseNumber);
      const modelPredictionCorners = modelPredictionFeature
        ? [
            modelPredictionFeature.corner1,
            modelPredictionFeature.corner2,
            modelPredictionFeature.corner3,
            modelPredictionFeature.corner4,
          ]
        : [null, null, null, null];
      const predictedCorners = horseCornerAverages.map((horseAverage, index) => {
        const weighted = [
          { value: horseAverage, weight: config.horseWeight },
          { value: jockeyCornerAverages[index], weight: config.jockeyWeight },
          { value: trainerCornerAverages[index], weight: config.trainerWeight },
          { value: similarityCornerAverages[index], weight: config.similarityWeight },
          { value: modelPredictionCorners[index], weight: config.modelPredictionWeight },
        ].filter((item): item is { value: number; weight: number } => item.value !== null);
        const weightTotal = weighted.reduce((total, item) => total + item.weight, 0);
        const predictedCorner =
          weightTotal > 0
            ? weighted.reduce((total, item) => total + item.value * item.weight, 0) / weightTotal
            : null;
        return blendLowSamplePrior({
          currentRunnerCount: resolvedCurrentRunnerCount,
          horseResultsCount: horseResults.length,
          horseNumberPriorWeight: config.lowSampleHorseNumberPriorWeight,
          lowSamplePriorMaxWeight: config.lowSamplePriorMaxWeight,
          popularityPriorFloorWeight: config.popularityPriorFloorWeight,
          predictedCorner,
          runner,
        });
      });
      const details: RacePacePredictionDetail[] = [
        {
          label: "馬自身の通過傾向",
          reason:
            "競走成績のコーナー通過順を頭数で補正し、日付の新しさと今回距離への近さで重み付け",
          value: averageNullable(horseCornerAverages),
          weight: config.horseWeight,
        },
        {
          label: "騎手との組み合わせ",
          reason: "今回騎手で走った過去成績のコーナー通過順を補助的に反映",
          value: averageNullable(jockeyCornerAverages),
          weight: config.jockeyWeight,
        },
        {
          label: "調教師の傾向",
          reason: "今回調教師で走った過去成績のコーナー通過順を補助的に反映",
          value: averageNullable(trainerCornerAverages),
          weight: config.trainerWeight,
        },
        {
          label: "似た出走条件の近傍馬",
          reason:
            "距離、頭数、馬番、人気、オッズ、トラック条件が近い過去馬のコーナー通過順を補助的に反映",
          value: averageNullable(similarityCornerAverages),
          weight: config.similarityWeight,
        },
        {
          label: "LightGBMモデル予測",
          reason:
            "過去データから学習したモデルのコーナー通過順予測を補助的に反映。未登録のレースでは使用しない",
          value: averageNullable(modelPredictionCorners),
          weight: config.modelPredictionWeight,
        },
      ];
      const availableCornerCount = predictedCorners.filter((corner) => corner !== null).length;
      return {
        confidence: roundScore(
          clampScore(availableCornerCount / 4) * 0.5 +
            clampScore(horseResults.length / 6) * 0.35 +
            clampScore((jockeyResults.length + trainerResults.length) / 4) * 0.15,
        ),
        corner1: predictedCorners[0] ?? null,
        corner2: predictedCorners[1] ?? null,
        corner3: predictedCorners[2] ?? null,
        corner4: predictedCorners[3] ?? null,
        details,
        horseName: cleanText(runner.bamei, "-"),
        horseNumber,
        predictedCorners: formatPredictedCorners(predictedCorners),
      };
    })
    .toSorted(
      (left, right) =>
        (left.corner1 ?? Number.POSITIVE_INFINITY) - (right.corner1 ?? Number.POSITIVE_INFINITY) ||
        Number(left.horseNumber) - Number(right.horseNumber),
    );
};
