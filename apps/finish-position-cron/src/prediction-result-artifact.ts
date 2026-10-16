// Run with bun. Read one explicitly committed, immutable prediction generation.
// References must come from the coordinator's fenced manifest, never a Queue
// body's arbitrary key or an R2 listing. This reader does not select a model.

import type { FinishPositionPredictionRow } from "./prediction-kv-writer";
import type { PredictCategory } from "./types";

export interface PredictionArtifactRunner {
  horseId: string;
  horseNumber: string;
}

export interface PredictionArtifactReference {
  key: string;
  sha256: string;
  generationId: string;
  inputFingerprint: string;
  modelVersion: string;
  predictionGeneratedAt: string;
  runners: ReadonlyArray<PredictionArtifactRunner>;
}

interface PredictionArtifactBucket {
  get(key: string): Promise<Pick<R2ObjectBody, "size" | "arrayBuffer"> | null>;
}

export interface ReadPredictionArtifactParams {
  bucket: PredictionArtifactBucket;
  category: PredictCategory;
  keibajoCode: string;
  raceBango: string;
  runYmd: string;
  reference: PredictionArtifactReference;
}

interface ArtifactRow extends FinishPositionPredictionRow {
  horseId: string;
}

const SCHEMA_VERSION = 1;
const MAX_ARTIFACT_BYTES = 65_536;
const MAX_RUNNERS = 99;
const HASH_PATTERN = /^[0-9a-f]{64}$/u;
const RUN_YMD_PATTERN = /^[0-9]{8}$/u;
const VENUE_PATTERN = /^[0-9A-Z]{2}$/u;
const RACE_PATTERN = /^[0-9]{2}$/u;
const HORSE_NUMBER_PATTERN = /^[1-9][0-9]?$/u;
const GENERATION_PATTERN = /^[a-zA-Z0-9_-]{1,160}$/u;
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isText = (value: unknown): value is string =>
  typeof value === "string" && value.trim().length > 0;

const isProbability = (value: unknown): value is number | null =>
  value === null ||
  (typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 1);

const runnerToken = (runner: PredictionArtifactRunner): string =>
  JSON.stringify([runner.horseNumber, runner.horseId]);

const validateReference = (params: ReadPredictionArtifactParams): void => {
  const ref: PredictionArtifactReference = params.reference;
  const expectedKey: string = `prediction-results/v1/${params.category}/${params.runYmd}/${params.keibajoCode}/${params.raceBango}/${ref.generationId}/${ref.sha256}.json`;
  if (
    !RUN_YMD_PATTERN.test(params.runYmd) ||
    !VENUE_PATTERN.test(params.keibajoCode) ||
    !RACE_PATTERN.test(params.raceBango) ||
    !HASH_PATTERN.test(ref.sha256) ||
    !HASH_PATTERN.test(ref.inputFingerprint) ||
    !GENERATION_PATTERN.test(ref.generationId) ||
    !isText(ref.modelVersion) ||
    !Number.isFinite(Date.parse(ref.predictionGeneratedAt)) ||
    ref.key !== expectedKey
  ) {
    throw new Error("Invalid prediction artifact reference");
  }
  if (
    ref.runners.length === 0 ||
    ref.runners.length > MAX_RUNNERS ||
    ref.runners.some(
      (runner) => !HORSE_NUMBER_PATTERN.test(runner.horseNumber) || !isText(runner.horseId),
    ) ||
    new Set(ref.runners.map((runner) => runner.horseNumber)).size !== ref.runners.length ||
    new Set(ref.runners.map((runner) => runner.horseId)).size !== ref.runners.length
  ) {
    throw new Error("Invalid prediction artifact runner set");
  }
};

const parseRow = (value: unknown, ref: PredictionArtifactReference): ArtifactRow => {
  if (
    !isRecord(value) ||
    !isText(value.horseId) ||
    typeof value.horseNumber !== "string" ||
    !HORSE_NUMBER_PATTERN.test(value.horseNumber) ||
    typeof value.predictedRank !== "number" ||
    !Number.isSafeInteger(value.predictedRank) ||
    value.predictedRank < 1 ||
    value.predictedRank > ref.runners.length ||
    typeof value.predictedScore !== "number" ||
    !Number.isFinite(value.predictedScore) ||
    !isProbability(value.predictedTop1Prob) ||
    !isProbability(value.predictedTop3Prob)
  ) {
    throw new Error("Invalid prediction artifact row");
  }
  return {
    horseId: value.horseId,
    umaban: value.horseNumber,
    predictedRank: value.predictedRank,
    predictedScore: value.predictedScore,
    predictedTop1Prob: value.predictedTop1Prob,
    predictedTop3Prob: value.predictedTop3Prob,
    modelVersion: ref.modelVersion,
    predictionGeneratedAt: ref.predictionGeneratedAt,
  };
};

const byHorseNumber = (left: ArtifactRow, right: ArtifactRow): number =>
  Number(left.umaban) - Number(right.umaban);

const parseArtifact = (
  value: unknown,
  params: ReadPredictionArtifactParams,
): FinishPositionPredictionRow[] => {
  const ref: PredictionArtifactReference = params.reference;
  if (
    !isRecord(value) ||
    value.schemaVersion !== SCHEMA_VERSION ||
    value.category !== params.category ||
    value.runYmd !== params.runYmd ||
    value.keibajoCode !== params.keibajoCode ||
    value.raceBango !== params.raceBango ||
    value.generationId !== ref.generationId ||
    value.inputFingerprint !== ref.inputFingerprint ||
    value.modelVersion !== ref.modelVersion ||
    value.predictionGeneratedAt !== ref.predictionGeneratedAt ||
    !Array.isArray(value.rows) ||
    value.rows.length !== ref.runners.length
  ) {
    throw new Error("Prediction artifact generation or scope mismatch");
  }
  const rows: ArtifactRow[] = value.rows.map((row: unknown) => parseRow(row, ref));
  const expected: Set<string> = new Set(ref.runners.map(runnerToken));
  const actual: Set<string> = new Set(
    rows.map((row) => runnerToken({ horseId: row.horseId, horseNumber: row.umaban })),
  );
  if (
    actual.size !== expected.size ||
    [...actual].some((token) => !expected.has(token)) ||
    new Set(rows.map((row) => row.predictedRank)).size !== rows.length
  ) {
    throw new Error("Prediction artifact roster or rank mismatch");
  }
  return rows.toSorted(byHorseNumber).map((row) => ({
    umaban: row.umaban,
    modelVersion: row.modelVersion,
    predictionGeneratedAt: row.predictionGeneratedAt,
    predictedRank: row.predictedRank,
    predictedScore: row.predictedScore,
    predictedTop1Prob: row.predictedTop1Prob,
    predictedTop3Prob: row.predictedTop3Prob,
  }));
};

const hexByte = (value: number): string => value.toString(16).padStart(2, "0");

export const readPredictionArtifact = async (
  params: ReadPredictionArtifactParams,
): Promise<FinishPositionPredictionRow[]> => {
  validateReference(params);
  const object: Pick<R2ObjectBody, "size" | "arrayBuffer"> | null = await params.bucket.get(
    params.reference.key,
  );
  if (object === null || object.size > MAX_ARTIFACT_BYTES) {
    throw new Error("Missing or oversized prediction artifact");
  }
  const bytes: ArrayBuffer = await object.arrayBuffer();
  if (bytes.byteLength > MAX_ARTIFACT_BYTES) throw new Error("Oversized prediction artifact body");
  const digest: ArrayBuffer = await crypto.subtle.digest("SHA-256", bytes);
  if (Array.from(new Uint8Array(digest), hexByte).join("") !== params.reference.sha256) {
    throw new Error("Prediction artifact checksum mismatch");
  }
  const value: unknown = JSON.parse(decoder.decode(bytes));
  return parseArtifact(value, params);
};
