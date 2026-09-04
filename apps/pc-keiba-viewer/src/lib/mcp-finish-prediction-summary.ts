// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

import type { RaceSource } from "./codes";
import {
  getPredictionProbabilityAvailability,
  parsePredictionProbability,
  type PredictionProbabilityAvailability,
} from "./prediction-probability";

export type FinishPredictionSummaryErrorCode =
  | "INVALID_ARGUMENT"
  | "INVALID_RACE_NUMBER"
  | "INVALID_SOURCE"
  | "INVALID_VENUE_CODE"
  | "PREDICTION_NOT_AVAILABLE"
  | "PREDICTION_PAYLOAD_MALFORMED"
  | "RACE_NOT_FOUND"
  | "RESPONSE_TOO_LARGE"
  | "TIMEOUT"
  | "UPSTREAM_API_ERROR";

export interface FinishPredictionSummaryError {
  code: FinishPredictionSummaryErrorCode;
  message: string;
}

export interface FinishPredictionSummaryRoute {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

export interface FinishPredictionSummaryRace {
  distance?: number | string;
  keibajoCode: string;
  raceDate: string;
  raceName?: string;
  raceNumber: string;
  source: RaceSource;
  trackCode?: string;
}

export interface FinishPredictionSummaryItem {
  confidenceTier: string | null;
  horseName: string;
  horseNumber: string;
  jockeyName?: string;
  modelVersion: string | null;
  predictedFinishNorm: number | null;
  predictedScoreStddev: number | null;
  predictionGeneratedAt: string | null;
  rank: number;
  showProbability: number | null;
  winProbability: number | null;
}

export interface FinishPredictionSummaryEvaluation {
  ndcgAt3Avg?: number;
  pairScoreAvg?: number;
  predictionCount?: number;
  raceCount?: number;
  smallSampleWarning?: boolean;
  top1Accuracy?: number;
  top3BoxAccuracy?: number;
  top3ExactAccuracy?: number;
  top3WinnerCaptureRate?: number;
  top5WinnerCaptureRate?: number;
}

export interface FinishPredictionSummaryJockeyWin {
  jockeyName: string;
  latestRaceNumber?: string;
  winCount: number;
}

export interface FinishPredictionSummary {
  evaluation?: FinishPredictionSummaryEvaluation;
  prediction: FinishPredictionSummaryItem[];
  probabilityAvailability: PredictionProbabilityAvailability;
  race: FinishPredictionSummaryRace;
  sameDayVenueJockeyWins?: FinishPredictionSummaryJockeyWin[];
}

interface RunnerIdentity {
  horseName: string;
  jockeyName?: string;
}

interface PredictionCandidate {
  confidenceTier: string | null;
  horseName: string;
  horseNumber: string;
  jockeyName?: string;
  modelVersion: string | null;
  predictedFinishNorm: number | null;
  predictedScoreStddev: number | null;
  predictionGeneratedAt: string | null;
  showProbability: number | null;
  winProbability: number | null;
}

export type FinishPredictionSummaryBuildResult =
  | { error: FinishPredictionSummaryError; status: "error" }
  | { status: "ok"; summary: FinishPredictionSummary };

const MALFORMED_MESSAGE: string =
  "The finish prediction API returned a payload that cannot be summarized safely.";
const NOT_AVAILABLE_MESSAGE: string = "Finish prediction has not been generated for this race.";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const readString = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;

const readNumber = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isFinite(value) ? value : undefined;

const readNullableString = (value: unknown): string | null => readString(value) ?? null;

const readNullableNumber = (value: unknown): number | null => readNumber(value) ?? null;

const normalizeHorseNumber = (value: unknown): string | null => {
  const raw = readString(value);
  if (raw === undefined || !/^\d+$/u.test(raw)) {
    return null;
  }
  const parsed = Number(raw);
  return Number.isSafeInteger(parsed) && parsed > 0 ? String(parsed) : null;
};

const formatHorseNumber = (value: string): string => value.padStart(2, "0");

const preferredName = (
  record: Record<string, unknown>,
  fullNameKey: string,
  shortNameKey: string,
): string | undefined => readString(record[fullNameKey]) ?? readString(record[shortNameKey]);

const buildRunnerIndex = (value: unknown): Map<string, RunnerIdentity> | null => {
  if (!Array.isArray(value)) {
    return null;
  }
  const rows = value.flatMap((entry) => {
    if (!isRecord(entry)) {
      return [];
    }
    const horseNumber = normalizeHorseNumber(entry.umaban);
    const horseName = preferredName(entry, "horseNameFull", "bamei");
    if (horseNumber === null || horseName === undefined) {
      return [];
    }
    return [
      [
        horseNumber,
        {
          horseName,
          jockeyName: preferredName(entry, "jockeyNameFull", "kishumeiRyakusho"),
        },
      ] satisfies [string, RunnerIdentity],
    ];
  });
  return new Map(rows);
};

const buildPredictionCandidate = (
  value: unknown,
  runners: ReadonlyMap<string, RunnerIdentity>,
): PredictionCandidate | null => {
  if (!isRecord(value)) {
    return null;
  }
  const normalizedHorseNumber = normalizeHorseNumber(value.horseNumber);
  const runner = normalizedHorseNumber === null ? undefined : runners.get(normalizedHorseNumber);
  if (normalizedHorseNumber === null || runner === undefined) {
    return null;
  }
  const predictedFinishNorm = readNullableNumber(value.predictedFinishNorm);
  if (value.predictedFinishNorm !== null && predictedFinishNorm === null) {
    return null;
  }
  return {
    confidenceTier: readNullableString(value.confidenceTier),
    horseName: runner.horseName,
    horseNumber: formatHorseNumber(normalizedHorseNumber),
    jockeyName: runner.jockeyName,
    modelVersion: readNullableString(value.modelVersion),
    predictedFinishNorm,
    predictedScoreStddev: readNullableNumber(value.predictedScoreStddev),
    predictionGeneratedAt: readNullableString(value.predictionGeneratedAt),
    showProbability: parsePredictionProbability(value.showProbability),
    winProbability: parsePredictionProbability(value.winProbability),
  };
};

const comparePredictionCandidates = (
  left: PredictionCandidate,
  right: PredictionCandidate,
): number => {
  if (left.predictedFinishNorm === null) {
    return right.predictedFinishNorm === null
      ? left.horseNumber.localeCompare(right.horseNumber, "en")
      : 1;
  }
  if (right.predictedFinishNorm === null) {
    return -1;
  }
  const scoreDifference = left.predictedFinishNorm - right.predictedFinishNorm;
  return scoreDifference === 0
    ? left.horseNumber.localeCompare(right.horseNumber, "en")
    : scoreDifference;
};

const buildPrediction = (
  value: unknown,
  runners: ReadonlyMap<string, RunnerIdentity>,
): FinishPredictionSummaryItem[] | null => {
  if (!Array.isArray(value)) {
    return null;
  }
  const candidates = value.flatMap((feature) => {
    const candidate = buildPredictionCandidate(feature, runners);
    return candidate === null ? [] : [candidate];
  });
  if (candidates.length !== value.length) {
    return null;
  }
  return candidates
    .toSorted(comparePredictionCandidates)
    .map((candidate, index) => Object.assign(candidate, { rank: index + 1 }));
};

const buildBucketEvaluation = (
  value: Record<string, unknown>,
): FinishPredictionSummaryEvaluation => ({
  ndcgAt3Avg: readNumber(value.ndcgAt3Avg),
  pairScoreAvg: readNumber(value.pairScoreAvg),
  predictionCount: readNumber(value.predictionCount),
  raceCount: readNumber(value.raceCount),
  smallSampleWarning:
    typeof value.smallSampleWarning === "boolean" ? value.smallSampleWarning : undefined,
  top1Accuracy: readNumber(value.top1Accuracy),
  top3BoxAccuracy: readNumber(value.top3BoxAccuracy),
  top3ExactAccuracy: readNumber(value.top3ExactAccuracy),
  top3WinnerCaptureRate: readNumber(value.top3WinnerCaptureRate),
  top5WinnerCaptureRate: readNumber(value.top5WinnerCaptureRate),
});

const buildLegacyEvaluation = (
  value: Record<string, unknown>,
): FinishPredictionSummaryEvaluation => ({
  pairScoreAvg: readNumber(value.pairScore),
  raceCount: readNumber(value.raceCount),
  top1Accuracy: readNumber(value.top1Accuracy),
  top3BoxAccuracy: readNumber(value.top3BoxAccuracy),
  top3ExactAccuracy: readNumber(value.top3ExactOrderAccuracy),
  top3WinnerCaptureRate: readNumber(value.top3WinnerCapture),
  top5WinnerCaptureRate: readNumber(value.top5WinnerCapture),
});

const hasEvaluationValue = (value: FinishPredictionSummaryEvaluation): boolean =>
  Object.values(value).some((entry) => entry !== undefined);

const buildEvaluation = (
  payload: Record<string, unknown>,
): FinishPredictionSummaryEvaluation | undefined => {
  const bucket = payload.bucket;
  const bucketEvaluation = isRecord(bucket) ? bucket.bucketEvaluation : undefined;
  const preferred = isRecord(bucketEvaluation)
    ? buildBucketEvaluation(bucketEvaluation)
    : isRecord(payload.evaluation)
      ? buildLegacyEvaluation(payload.evaluation)
      : undefined;
  return preferred !== undefined && hasEvaluationValue(preferred) ? preferred : undefined;
};

const buildJockeyWins = (value: unknown): FinishPredictionSummaryJockeyWin[] | undefined => {
  if (!Array.isArray(value)) {
    return undefined;
  }
  return value.flatMap((entry) => {
    if (!isRecord(entry)) {
      return [];
    }
    const jockeyName = readString(entry.jockeyName);
    const winCount = readNumber(entry.winCount);
    if (jockeyName === undefined || winCount === undefined) {
      return [];
    }
    return [
      {
        jockeyName,
        latestRaceNumber: readString(entry.latestRaceNumber),
        winCount,
      },
    ];
  });
};

const buildRace = (
  payload: Record<string, unknown>,
  inputs: Record<string, unknown>,
  route: FinishPredictionSummaryRoute,
): FinishPredictionSummaryRace | null => {
  const expectedRaceDate = `${route.year}${route.month}${route.day}`;
  if (
    inputs.currentRaceDate !== expectedRaceDate ||
    inputs.currentSource !== route.source ||
    inputs.currentKeibajoCode !== route.keibajoCode
  ) {
    return null;
  }
  const bucket = payload.bucket;
  const bucketRace =
    isRecord(bucket) && isRecord(bucket.bucketRace) ? bucket.bucketRace : undefined;
  const distance =
    typeof inputs.currentDistance === "string" || typeof inputs.currentDistance === "number"
      ? inputs.currentDistance
      : undefined;
  return {
    distance,
    keibajoCode: route.keibajoCode,
    raceDate: `${route.year}-${route.month}-${route.day}`,
    raceName:
      bucketRace === undefined
        ? undefined
        : (readString(bucketRace.kyosomeiHondai) ?? readString(bucketRace.raceName)),
    raceNumber: route.raceNumber,
    source: route.source,
    trackCode: readString(inputs.currentTrackCode),
  };
};

export const createFinishPredictionSummaryError = (
  code: FinishPredictionSummaryErrorCode,
  message: string,
): FinishPredictionSummaryError => ({ code, message });

export const buildFinishPredictionSummary = (
  payload: unknown,
  route: FinishPredictionSummaryRoute,
): FinishPredictionSummaryBuildResult => {
  if (!isRecord(payload) || payload.type !== "finish-prediction" || !isRecord(payload.inputs)) {
    return {
      error: createFinishPredictionSummaryError("PREDICTION_PAYLOAD_MALFORMED", MALFORMED_MESSAGE),
      status: "error",
    };
  }
  const runners = buildRunnerIndex(payload.inputs.runners);
  if (runners === null) {
    return {
      error: createFinishPredictionSummaryError("PREDICTION_PAYLOAD_MALFORMED", MALFORMED_MESSAGE),
      status: "error",
    };
  }
  const prediction = buildPrediction(payload.inputs.modelPredictionFeatures, runners);
  if (prediction === null) {
    return {
      error: createFinishPredictionSummaryError("PREDICTION_PAYLOAD_MALFORMED", MALFORMED_MESSAGE),
      status: "error",
    };
  }
  if (prediction.every((item) => item.predictedFinishNorm === null)) {
    return {
      error: createFinishPredictionSummaryError("PREDICTION_NOT_AVAILABLE", NOT_AVAILABLE_MESSAGE),
      status: "error",
    };
  }
  const race = buildRace(payload, payload.inputs, route);
  if (race === null) {
    return {
      error: createFinishPredictionSummaryError("PREDICTION_PAYLOAD_MALFORMED", MALFORMED_MESSAGE),
      status: "error",
    };
  }
  const evaluation = buildEvaluation(payload);
  const sameDayVenueJockeyWins = buildJockeyWins(payload.inputs.sameDayVenueJockeyWins);
  return {
    status: "ok",
    summary: {
      evaluation,
      prediction,
      probabilityAvailability: getPredictionProbabilityAvailability(prediction),
      race,
      sameDayVenueJockeyWins,
    },
  };
};
