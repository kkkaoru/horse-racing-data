// Run with bun. Builds the bounded per-day finish prediction payload exposed by MCP.

import "server-only";
import {
  getActiveFinishPositionPredictions,
  getRaceDetail,
  getRaceRunners,
  getRacesByDateWithoutJockeyNames,
} from "../db/queries";
import type { RaceSource } from "./codes";
import type { FinishPositionModelPredictionFeature, RaceListItem, Runner } from "./race-types";
import { getRunnerDisplayNames } from "./runner-display";

export interface DailyFinishPredictionRequest {
  day: string;
  month: string;
  race?: {
    keibajoCode: string;
    raceNumber: string;
  };
  source: RaceSource;
  year: string;
}

export interface DailyFinishPredictionItem {
  confidenceTier: string | null;
  horseName: string;
  horseNumber: string;
  modelVersion: string | null;
  predictedFinishNorm: number | null;
  predictedScoreStddev: number | null;
  predictionGeneratedAt: string | null;
  rank: number;
  showProbability: number | null;
  winProbability: number | null;
}

export interface DailyFinishPredictionRace {
  distance: string | null;
  keibajoCode: string;
  modelVersion: string | null;
  prediction: DailyFinishPredictionItem[];
  predictionGeneratedAt: string | null;
  raceId: string;
  raceName: string | null;
  raceNumber: string;
  startTime: string | null;
}

export interface DailyFinishPredictionsPayload {
  availableRaceCount: number;
  date: string;
  raceCount: number;
  races: DailyFinishPredictionRace[];
  source: RaceSource;
  unavailableRaceIds: string[];
}

interface DailyRaceBuildInput {
  day: string;
  month: string;
  race: RaceListItem;
  year: string;
}

interface LoadedRacePrediction {
  race: DailyFinishPredictionRace | null;
  raceId: string;
}

interface PredictionCandidate {
  confidenceTier: string | null;
  horseName: string;
  horseNumber: string;
  modelVersion: string | null;
  predictedFinishNorm: number | null;
  predictedScoreStddev: number | null;
  predictionGeneratedAt: string | null;
  showProbability: number | null;
  winProbability: number | null;
}

const DAILY_RACE_BATCH_SIZE: number = 6;

const normalizeHorseNumber = (value: string | null): string | null => {
  if (value === null || !/^\d+$/u.test(value)) return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? String(parsed) : null;
};

const formatHorseNumber = (value: string): string => value.padStart(2, "0");

const buildRaceId = (race: RaceListItem): string =>
  `${race.source}:${race.kaisaiNen}:${race.kaisaiTsukihi}:${race.keibajoCode}:${race.raceBango}`;

const buildRunnerNames = (runners: readonly Runner[]): ReadonlyMap<string, string> =>
  new Map(
    runners.flatMap((runner) => {
      const horseNumber = normalizeHorseNumber(runner.umaban);
      return horseNumber === null
        ? []
        : [[horseNumber, getRunnerDisplayNames(runner).horse] satisfies [string, string]];
    }),
  );

const comparePrediction = (left: PredictionCandidate, right: PredictionCandidate): number => {
  if (left.predictedFinishNorm === null) {
    return right.predictedFinishNorm === null
      ? left.horseNumber.localeCompare(right.horseNumber, "en")
      : 1;
  }
  if (right.predictedFinishNorm === null) return -1;
  const scoreDifference = left.predictedFinishNorm - right.predictedFinishNorm;
  return scoreDifference === 0
    ? left.horseNumber.localeCompare(right.horseNumber, "en")
    : scoreDifference;
};

const buildPredictionCandidate = (
  feature: FinishPositionModelPredictionFeature,
  names: ReadonlyMap<string, string>,
): PredictionCandidate | null => {
  const horseNumber = normalizeHorseNumber(feature.horseNumber);
  if (horseNumber === null) return null;
  return {
    confidenceTier: feature.confidenceTier ?? null,
    horseName: names.get(horseNumber) ?? "",
    horseNumber: formatHorseNumber(horseNumber),
    modelVersion: feature.modelVersion,
    predictedFinishNorm: feature.predictedFinishNorm,
    predictedScoreStddev: feature.predictedScoreStddev ?? null,
    predictionGeneratedAt: feature.predictionGeneratedAt ?? null,
    showProbability: feature.showProbability,
    winProbability: feature.winProbability,
  };
};

const buildPrediction = (
  features: readonly FinishPositionModelPredictionFeature[],
  runners: readonly Runner[],
): DailyFinishPredictionItem[] => {
  const names = buildRunnerNames(runners);
  return features
    .flatMap((feature) => {
      const candidate = buildPredictionCandidate(feature, names);
      return candidate === null ? [] : [candidate];
    })
    .toSorted(comparePrediction)
    .map((candidate, index) => Object.assign(candidate, { rank: index + 1 }));
};

const uniqueValue = (values: readonly (string | null)[]): string | null => {
  const unique = [...new Set(values.filter((value): value is string => value !== null))];
  return unique.length === 1 ? (unique[0] ?? null) : null;
};

const loadRacePrediction = async (input: DailyRaceBuildInput): Promise<LoadedRacePrediction> => {
  const raceId = buildRaceId(input.race);
  const [detail, runners] = await Promise.all([
    getRaceDetail(
      input.race.source,
      input.year,
      input.month,
      input.day,
      input.race.keibajoCode,
      input.race.raceBango,
    ),
    getRaceRunners(
      input.race.source,
      input.year,
      input.month,
      input.day,
      input.race.keibajoCode,
      input.race.raceBango,
    ),
  ]);
  if (detail === null || runners.length <= 1) return { race: null, raceId };
  const features = await getActiveFinishPositionPredictions(detail, runners);
  const prediction = buildPrediction(features, runners);
  if (prediction.every((item) => item.predictedFinishNorm === null)) {
    return { race: null, raceId };
  }
  return {
    race: {
      distance: detail.kyori,
      keibajoCode: detail.keibajoCode,
      modelVersion: uniqueValue(prediction.map((item) => item.modelVersion)),
      prediction,
      predictionGeneratedAt: uniqueValue(prediction.map((item) => item.predictionGeneratedAt)),
      raceId,
      raceName: detail.kyosomeiHondai,
      raceNumber: detail.raceBango,
      startTime: detail.hassoJikoku,
    },
    raceId,
  };
};

const loadRacePredictionBatch = async (
  inputs: readonly DailyRaceBuildInput[],
): Promise<LoadedRacePrediction[]> => {
  if (inputs.length === 0) return [];
  const current = await Promise.all(inputs.slice(0, DAILY_RACE_BATCH_SIZE).map(loadRacePrediction));
  const remaining = await loadRacePredictionBatch(inputs.slice(DAILY_RACE_BATCH_SIZE));
  return [...current, ...remaining];
};

export const getDailyFinishPredictions = async (
  request: DailyFinishPredictionRequest,
): Promise<DailyFinishPredictionsPayload> => {
  const allRaces = await getRacesByDateWithoutJockeyNames(request.year, request.month, request.day);
  const sourceRaces = allRaces.filter((race) => race.source === request.source);
  const scopedRace = request.race;
  const selectedRaces =
    scopedRace === undefined
      ? sourceRaces
      : sourceRaces.filter(
          (race) =>
            race.keibajoCode === scopedRace.keibajoCode && race.raceBango === scopedRace.raceNumber,
        );
  const loaded = await loadRacePredictionBatch(
    selectedRaces.map((race) => ({
      day: request.day,
      month: request.month,
      race,
      year: request.year,
    })),
  );
  const races = loaded.flatMap((entry) => (entry.race === null ? [] : [entry.race]));
  return {
    availableRaceCount: races.length,
    date: `${request.year}-${request.month}-${request.day}`,
    raceCount: selectedRaces.length,
    races,
    source: request.source,
    unavailableRaceIds: loaded.flatMap((entry) => (entry.race === null ? [entry.raceId] : [])),
  };
};
