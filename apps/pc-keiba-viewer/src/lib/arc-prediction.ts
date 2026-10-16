// Runs with Bun for tests and in the browser for the dedicated Arc display.
import type { Runner } from "./race-types";

export interface ArcRaceScope {
  source: string;
  year: string;
  month: string;
  day: string;
  keibajoCode: string;
  raceNumber: string;
}

export interface ArcPredictionRow {
  horseId: string;
  umaban: number;
  name: string;
  baseScore: number;
  winScore: number;
  predictedRank: number;
}

export interface ArcPredictionCard {
  modelVersion: string;
  modelSha256: string;
  featureSnapshotAt: string;
  trainingMaxDate: string;
  predictions: ArcPredictionRow[];
}

export type ArcRunner = Pick<Runner, "umaban" | "bamei" | "kettoTorokuBango">;

const ARC_DATE: string = "2026-10-04";
const ARC_FIELD_SIZE: number = 16;
const SHA256_PATTERN: RegExp = /^[a-f0-9]{64}$/u;
const DATE_PATTERN: RegExp = /^\d{4}-\d{2}-\d{2}$/u;

export const isArcPredictionRace = (scope: ArcRaceScope): boolean =>
  scope.source === "jra" &&
  scope.year === "2026" &&
  scope.month.padStart(2, "0") === "10" &&
  scope.day.padStart(2, "0") === "04" &&
  scope.keibajoCode === "A8" &&
  scope.raceNumber.padStart(2, "0") === "05";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const decodeRow = (value: unknown): ArcPredictionRow => {
  if (
    !isRecord(value) ||
    typeof value.horseId !== "string" ||
    !value.horseId.trim() ||
    typeof value.name !== "string" ||
    !value.name.trim() ||
    typeof value.umaban !== "number" ||
    !Number.isInteger(value.umaban) ||
    value.umaban < 1 ||
    value.umaban > ARC_FIELD_SIZE ||
    typeof value.predictedRank !== "number" ||
    !Number.isInteger(value.predictedRank) ||
    value.predictedRank < 1 ||
    value.predictedRank > ARC_FIELD_SIZE ||
    typeof value.baseScore !== "number" ||
    !Number.isFinite(value.baseScore) ||
    typeof value.winScore !== "number" ||
    !Number.isFinite(value.winScore)
  )
    throw new Error("Invalid Arc prediction row");
  return {
    horseId: value.horseId,
    umaban: value.umaban,
    name: value.name,
    baseScore: value.baseScore,
    winScore: value.winScore,
    predictedRank: value.predictedRank,
  };
};

const compareRank = (left: ArcPredictionRow, right: ArcPredictionRow): number =>
  left.predictedRank - right.predictedRank;

export const readArcPrediction = (
  value: unknown,
  runners: readonly ArcRunner[],
): ArcPredictionCard => {
  if (
    !isRecord(value) ||
    value.raceDate !== ARC_DATE ||
    value.featureCutoffExclusive !== ARC_DATE ||
    value.liveFeatures !== false ||
    value.evaluationWasTuned !== true ||
    value.policy !== "top2-win-rerank" ||
    typeof value.modelVersion !== "string" ||
    !value.modelVersion.startsWith("arc-") ||
    typeof value.modelSha256 !== "string" ||
    !SHA256_PATTERN.test(value.modelSha256) ||
    typeof value.featureSnapshotAt !== "string" ||
    !Number.isFinite(Date.parse(value.featureSnapshotAt)) ||
    typeof value.trainingMaxDate !== "string" ||
    !DATE_PATTERN.test(value.trainingMaxDate) ||
    !Number.isFinite(Date.parse(value.trainingMaxDate)) ||
    value.trainingMaxDate >= ARC_DATE ||
    !Array.isArray(value.predictions) ||
    value.predictions.length !== ARC_FIELD_SIZE ||
    runners.length !== ARC_FIELD_SIZE
  )
    throw new Error("Invalid Arc prediction card");
  const predictions: ArcPredictionRow[] = value.predictions.map(decodeRow);
  const rosterMatches: boolean = predictions.every((prediction) =>
    runners.some(
      (runner) =>
        Number(runner.umaban) === prediction.umaban &&
        runner.bamei?.trim() === prediction.name &&
        (runner.kettoTorokuBango === "0000000000" ||
          runner.kettoTorokuBango === prediction.horseId),
    ),
  );
  if (
    !rosterMatches ||
    new Set(predictions.map((row) => row.umaban)).size !== ARC_FIELD_SIZE ||
    new Set(predictions.map((row) => row.horseId)).size !== ARC_FIELD_SIZE ||
    new Set(predictions.map((row) => row.predictedRank)).size !== ARC_FIELD_SIZE
  )
    throw new Error("Arc prediction roster does not match the current declarations");
  return {
    modelVersion: value.modelVersion,
    modelSha256: value.modelSha256,
    featureSnapshotAt: value.featureSnapshotAt,
    trainingMaxDate: value.trainingMaxDate,
    predictions: predictions.toSorted(compareRank),
  };
};
