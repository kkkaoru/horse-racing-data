// This file runs with Bun. Publish one complete overseas generation, never model activation.
import { buildProductionRequest, type ProductionRequestInput } from "./production-request";

export interface PredictionPublicationInput {
  readonly request: ProductionRequestInput;
  readonly predictions: unknown;
  readonly generatedAt: string;
}

export interface PredictionStatement {
  readonly text: string;
  readonly values: readonly string[];
}

export interface PredictionPublicationPort {
  readonly withTransaction: (statements: readonly PredictionStatement[]) => Promise<void>;
}

interface PredictionRow {
  readonly umaban: number;
  readonly horse: string;
  readonly score: number;
  readonly rank: number;
  readonly top1: number;
  readonly top3: number;
  readonly finish: number;
}

const MODEL_VERSION: string = "overseas-lgbm-fp-v3";
const UPSERT: string = `insert into race_finish_position_model_predictions (
 model_version,source,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,
 ketto_toroku_bango,umaban,predicted_score,predicted_rank,predicted_top1_prob,
 predicted_top3_prob,predicted_finish_position,prediction_generated_at
) values ($1,'overseas',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
on conflict (model_version,source,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,ketto_toroku_bango)
do update set umaban=excluded.umaban,predicted_score=excluded.predicted_score,
 predicted_rank=excluded.predicted_rank,predicted_top1_prob=excluded.predicted_top1_prob,
 predicted_top3_prob=excluded.predicted_top3_prob,predicted_finish_position=excluded.predicted_finish_position,
 prediction_generated_at=excluded.prediction_generated_at`;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const finiteNumber = (row: Record<string, unknown>, key: string): number => {
  const value: unknown = row[key];
  if (typeof value !== "number" || !Number.isFinite(value))
    throw new Error("Prediction contains an invalid numeric value");
  return value;
};

const parsePrediction = (value: unknown): PredictionRow => {
  if (!isRecord(value) || typeof value.ketto_toroku_bango !== "string")
    throw new Error("Invalid prediction row");
  return {
    umaban: finiteNumber(value, "umaban"),
    horse: value.ketto_toroku_bango,
    score: finiteNumber(value, "predicted_score"),
    rank: finiteNumber(value, "predicted_rank"),
    top1: finiteNumber(value, "predicted_top1_prob"),
    top3: finiteNumber(value, "predicted_top3_prob"),
    finish: finiteNumber(value, "predicted_finish_position"),
  };
};

const expectedHorse = (
  runner: Readonly<Record<string, string | null>>,
): string | null | undefined =>
  runner.ketto_toroku_bango === "0000000000"
    ? `UMABAN_${runner.umaban}`
    : runner.ketto_toroku_bango;

const validatePredictions = (input: PredictionPublicationInput): readonly PredictionRow[] => {
  buildProductionRequest(input.request);
  if (Number.isNaN(Date.parse(input.generatedAt)))
    throw new Error("Invalid prediction generation time");
  if (
    !Array.isArray(input.predictions) ||
    input.predictions.length !== input.request.runners.length
  )
    throw new Error("Prediction generation is incomplete");
  const rows: readonly PredictionRow[] = input.predictions.map(parsePrediction);
  if (
    rows.some(
      (row) =>
        !Number.isInteger(row.umaban) ||
        !Number.isInteger(row.rank) ||
        row.rank < 1 ||
        row.rank > rows.length ||
        row.top1 < 0 ||
        row.top1 > 1 ||
        row.top3 < 0 ||
        row.top3 > 1 ||
        row.finish < 0,
    )
  )
    throw new Error("Prediction rank or probability is out of range");
  if (
    new Set(rows.map((row) => row.umaban)).size !== rows.length ||
    new Set(rows.map((row) => row.rank)).size !== rows.length
  )
    throw new Error("Prediction generation has duplicate runners or ranks");
  if (
    rows.some(
      (row) =>
        !input.request.runners.some(
          (runner) => Number(runner.umaban) === row.umaban && expectedHorse(runner) === row.horse,
        ),
    )
  )
    throw new Error("Prediction identity does not match the registered field");
  return rows;
};

export const buildPredictionStatements = (
  input: PredictionPublicationInput,
): readonly PredictionStatement[] => {
  const rows: readonly PredictionRow[] = validatePredictions(input);
  const race: Readonly<Record<string, string | null>> = input.request.race;
  return rows.map(
    (row): PredictionStatement => ({
      text: UPSERT,
      values: [
        MODEL_VERSION,
        String(race.kaisai_nen),
        String(race.kaisai_tsukihi),
        String(race.keibajo_code),
        String(race.race_bango),
        row.horse,
        String(row.umaban),
        String(row.score),
        String(row.rank),
        String(row.top1),
        String(row.top3),
        String(row.finish),
        input.generatedAt,
      ],
    }),
  );
};

export const publishPredictions = async (
  input: PredictionPublicationInput,
  port: PredictionPublicationPort,
): Promise<void> => {
  const statements: readonly PredictionStatement[] = buildPredictionStatements(input);
  await port.withTransaction(statements);
};
