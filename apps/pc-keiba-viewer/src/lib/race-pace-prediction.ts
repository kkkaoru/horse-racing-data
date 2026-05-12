import { cleanText } from "./format";
import { isBanEiKeibajoCode } from "./runner-format";
import type { RaceSource } from "./codes";
import type {
  HorseRaceResult,
  RacePacePredictionDetail,
  RacePacePredictionRow,
  Runner,
} from "./race-types";

interface BuildRacePacePredictionRowsParams {
  currentDistance: string | null | undefined;
  currentRaceDate: string;
  results: HorseRaceResult[];
  runners: Runner[];
}

export const RACE_PACE_PREDICTION_RESULTS_EVENT = "pc-keiba:race-pace-prediction-results";

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
  return 1 / (1 + elapsedDays / 365);
};

const getDistanceWeight = (
  currentDistance: string | null | undefined,
  resultDistance: string | null | undefined,
): number => {
  const current = parseStoredNumber(currentDistance, "");
  const past = parseStoredNumber(resultDistance, "");
  if (current === null || past === null) {
    return 0.75;
  }
  return Math.max(0.25, 1 - Math.abs(current - past) / Math.max(current * 0.5, 400));
};

const getWeightedCornerAverage = (
  results: HorseRaceResult[],
  cornerKey: "corner1" | "corner2" | "corner3" | "corner4",
  currentRaceDate: string,
  currentDistance: string | null | undefined,
): number | null => {
  let total = 0;
  let weightTotal = 0;
  for (const result of results) {
    const corner = parseCorner(result[cornerKey]);
    if (corner === null) {
      continue;
    }
    const weight =
      getRaceDateWeight(currentRaceDate, result) * getDistanceWeight(currentDistance, result.kyori);
    total += corner * weight;
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

export const buildRacePacePredictionRowsFromResults = ({
  currentDistance,
  currentRaceDate,
  results,
  runners,
}: BuildRacePacePredictionRowsParams): RacePacePredictionRow[] => {
  const resultsByHorse = new Map<string, HorseRaceResult[]>();
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
      const horseCornerAverages = [
        getWeightedCornerAverage(horseResults, "corner1", currentRaceDate, currentDistance),
        getWeightedCornerAverage(horseResults, "corner2", currentRaceDate, currentDistance),
        getWeightedCornerAverage(horseResults, "corner3", currentRaceDate, currentDistance),
        getWeightedCornerAverage(horseResults, "corner4", currentRaceDate, currentDistance),
      ];
      const jockeyCornerAverages = [
        getWeightedCornerAverage(jockeyResults, "corner1", currentRaceDate, currentDistance),
        getWeightedCornerAverage(jockeyResults, "corner2", currentRaceDate, currentDistance),
        getWeightedCornerAverage(jockeyResults, "corner3", currentRaceDate, currentDistance),
        getWeightedCornerAverage(jockeyResults, "corner4", currentRaceDate, currentDistance),
      ];
      const trainerCornerAverages = [
        getWeightedCornerAverage(trainerResults, "corner1", currentRaceDate, currentDistance),
        getWeightedCornerAverage(trainerResults, "corner2", currentRaceDate, currentDistance),
        getWeightedCornerAverage(trainerResults, "corner3", currentRaceDate, currentDistance),
        getWeightedCornerAverage(trainerResults, "corner4", currentRaceDate, currentDistance),
      ];
      const predictedCorners = horseCornerAverages.map((horseAverage, index) => {
        const weighted = [
          { value: horseAverage, weight: 0.7 },
          { value: jockeyCornerAverages[index], weight: 0.15 },
          { value: trainerCornerAverages[index], weight: 0.15 },
        ].filter((item): item is { value: number; weight: number } => item.value !== null);
        const weightTotal = weighted.reduce((total, item) => total + item.weight, 0);
        return weightTotal > 0
          ? weighted.reduce((total, item) => total + item.value * item.weight, 0) / weightTotal
          : null;
      });
      const details: RacePacePredictionDetail[] = [
        {
          label: "馬自身の通過傾向",
          reason: "競走成績のコーナー通過順を日付の新しさと今回距離への近さで重み付け",
          value: averageNullable(horseCornerAverages),
          weight: 0.7,
        },
        {
          label: "騎手との組み合わせ",
          reason: "今回騎手で走った過去成績のコーナー通過順を補助的に反映",
          value: averageNullable(jockeyCornerAverages),
          weight: 0.15,
        },
        {
          label: "調教師の傾向",
          reason: "今回調教師で走った過去成績のコーナー通過順を補助的に反映",
          value: averageNullable(trainerCornerAverages),
          weight: 0.15,
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
