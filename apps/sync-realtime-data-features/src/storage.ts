// Run with bun. D1 storage helpers for the new features worker DB.

import type {
  FinishPositionInferenceStateRow,
  FinishPositionPredictionsRow,
  RunningStyleInferenceStateRow,
  RunningStyleRow,
} from "./types";

interface RaceRunningStyleD1Row {
  race_key: string;
  horse_number: number;
  ketto_toroku_bango: string;
  bamei: string | null;
  category: string;
  kaisai_nen: string;
  model_version: string;
  p_nige: number;
  p_senkou: number;
  p_sashi: number;
  p_oikomi: number;
  predicted_label: string;
  predicted_at: string;
}

interface RunningStyleInferenceStateD1Row {
  race_key: string;
  source: "jra" | "nar";
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  status: string;
  features_r2_key: string | null;
  model_version: string | null;
  expected_horse_count: number | null;
  written_horse_count: number | null;
  attempted_at: string | null;
  completed_at: string | null;
  error_message: string | null;
}

interface FinishPositionInferenceStateD1Row {
  race_key: string;
  source: "jra" | "nar";
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  status: string;
  predictions_r2_key: string | null;
  model_version: string | null;
  attempted_at: string | null;
  completed_at: string | null;
  error_message: string | null;
}

interface RaceFinishPositionPredictionsD1Row {
  race_key: string;
  source: "jra" | "nar";
  predictions_json: string;
  predicted_at: string;
  predictor_version: string;
}

const UPSERT_RUNNING_STYLE_SQL = `insert into race_running_styles (
  race_key, horse_number, ketto_toroku_bango, bamei, category, kaisai_nen,
  model_version, p_nige, p_senkou, p_sashi, p_oikomi, predicted_label, predicted_at
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
on conflict(race_key, horse_number) do update set
  ketto_toroku_bango = excluded.ketto_toroku_bango,
  bamei = excluded.bamei,
  category = excluded.category,
  kaisai_nen = excluded.kaisai_nen,
  model_version = excluded.model_version,
  p_nige = excluded.p_nige,
  p_senkou = excluded.p_senkou,
  p_sashi = excluded.p_sashi,
  p_oikomi = excluded.p_oikomi,
  predicted_label = excluded.predicted_label,
  predicted_at = excluded.predicted_at`;

// Defensive `?? null` on parquet-derived fields (kettoTorokuBango / bamei):
// D1 prepared statements reject `undefined` with D1_TYPE_ERROR, and the
// TypeScript types do not capture the runtime `undefined` that hyparquet
// can leak for optional parquet columns. The remaining fields are sourced
// from the job context or constants and cannot be undefined.
export const upsertRunningStyle = async (db: D1Database, row: RunningStyleRow): Promise<void> => {
  await db
    .prepare(UPSERT_RUNNING_STYLE_SQL)
    .bind(
      row.raceKey,
      row.horseNumber,
      row.kettoTorokuBango ?? null,
      row.bamei ?? null,
      row.category,
      row.kaisaiNen,
      row.modelVersion,
      row.pNige,
      row.pSenkou,
      row.pSashi,
      row.pOikomi,
      row.predictedLabel,
      row.predictedAt,
    )
    .run();
};

export const listRaceRunningStyles = async (
  db: D1Database,
  raceKey: string,
): Promise<RunningStyleRow[]> => {
  const result = await db
    .prepare(
      `select race_key, horse_number, ketto_toroku_bango, bamei, category, kaisai_nen,
       model_version, p_nige, p_senkou, p_sashi, p_oikomi, predicted_label, predicted_at
       from race_running_styles where race_key = ? order by horse_number asc`,
    )
    .bind(raceKey)
    .all<RaceRunningStyleD1Row>();
  return result.results.map((row) => ({
    raceKey: row.race_key,
    horseNumber: row.horse_number,
    kettoTorokuBango: row.ketto_toroku_bango,
    bamei: row.bamei,
    category: row.category,
    kaisaiNen: row.kaisai_nen,
    modelVersion: row.model_version,
    pNige: row.p_nige,
    pSenkou: row.p_senkou,
    pSashi: row.p_sashi,
    pOikomi: row.p_oikomi,
    predictedLabel: row.predicted_label,
    predictedAt: row.predicted_at,
  }));
};

const UPSERT_FINISH_POSITION_PREDICTIONS_SQL = `insert into race_finish_position_predictions (
  race_key, source, predictions_json, predicted_at, predictor_version
) values (?, ?, ?, ?, ?)
on conflict(race_key) do update set
  source = excluded.source,
  predictions_json = excluded.predictions_json,
  predicted_at = excluded.predicted_at,
  predictor_version = excluded.predictor_version`;

export const upsertFinishPositionPredictions = async (
  db: D1Database,
  row: FinishPositionPredictionsRow,
): Promise<void> => {
  await db
    .prepare(UPSERT_FINISH_POSITION_PREDICTIONS_SQL)
    .bind(row.raceKey, row.source, row.predictionsJson, row.predictedAt, row.predictorVersion)
    .run();
};

export const getFinishPositionPredictions = async (
  db: D1Database,
  raceKey: string,
): Promise<FinishPositionPredictionsRow | null> => {
  const row = await db
    .prepare(
      `select race_key, source, predictions_json, predicted_at, predictor_version
       from race_finish_position_predictions where race_key = ?`,
    )
    .bind(raceKey)
    .first<RaceFinishPositionPredictionsD1Row>();
  if (!row) {
    return null;
  }
  return {
    raceKey: row.race_key,
    source: row.source,
    predictionsJson: row.predictions_json,
    predictedAt: row.predicted_at,
    predictorVersion: row.predictor_version,
  };
};

const UPSERT_RUNNING_STYLE_STATE_SQL = `insert into running_style_inference_state (
  race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, status,
  features_r2_key, model_version, expected_horse_count, written_horse_count,
  attempted_at, completed_at, error_message
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
on conflict(race_key) do update set
  status = excluded.status,
  features_r2_key = excluded.features_r2_key,
  model_version = excluded.model_version,
  expected_horse_count = excluded.expected_horse_count,
  written_horse_count = excluded.written_horse_count,
  attempted_at = excluded.attempted_at,
  completed_at = excluded.completed_at,
  error_message = excluded.error_message`;

export const upsertRunningStyleInferenceState = async (
  db: D1Database,
  row: RunningStyleInferenceStateRow,
): Promise<void> => {
  await db
    .prepare(UPSERT_RUNNING_STYLE_STATE_SQL)
    .bind(
      row.raceKey,
      row.source,
      row.kaisaiNen,
      row.kaisaiTsukihi,
      row.keibajoCode,
      row.raceBango,
      row.status,
      row.featuresR2Key,
      row.modelVersion,
      row.expectedHorseCount,
      row.writtenHorseCount,
      row.attemptedAt,
      row.completedAt,
      row.errorMessage,
    )
    .run();
};

export const getRunningStyleInferenceState = async (
  db: D1Database,
  raceKey: string,
): Promise<RunningStyleInferenceStateRow | null> => {
  const row = await db
    .prepare(
      `select race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
        status, features_r2_key, model_version, expected_horse_count, written_horse_count,
        attempted_at, completed_at, error_message
       from running_style_inference_state where race_key = ?`,
    )
    .bind(raceKey)
    .first<RunningStyleInferenceStateD1Row>();
  if (!row) {
    return null;
  }
  return {
    raceKey: row.race_key,
    source: row.source,
    kaisaiNen: row.kaisai_nen,
    kaisaiTsukihi: row.kaisai_tsukihi,
    keibajoCode: row.keibajo_code,
    raceBango: row.race_bango,
    status: row.status,
    featuresR2Key: row.features_r2_key,
    modelVersion: row.model_version,
    expectedHorseCount: row.expected_horse_count,
    writtenHorseCount: row.written_horse_count,
    attemptedAt: row.attempted_at,
    completedAt: row.completed_at,
    errorMessage: row.error_message,
  };
};

const UPSERT_FINISH_POSITION_STATE_SQL = `insert into finish_position_inference_state (
  race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, status,
  predictions_r2_key, model_version, attempted_at, completed_at, error_message
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
on conflict(race_key) do update set
  status = excluded.status,
  predictions_r2_key = excluded.predictions_r2_key,
  model_version = excluded.model_version,
  attempted_at = excluded.attempted_at,
  completed_at = excluded.completed_at,
  error_message = excluded.error_message`;

export const upsertFinishPositionInferenceState = async (
  db: D1Database,
  row: FinishPositionInferenceStateRow,
): Promise<void> => {
  await db
    .prepare(UPSERT_FINISH_POSITION_STATE_SQL)
    .bind(
      row.raceKey,
      row.source,
      row.kaisaiNen,
      row.kaisaiTsukihi,
      row.keibajoCode,
      row.raceBango,
      row.status,
      row.predictionsR2Key,
      row.modelVersion,
      row.attemptedAt,
      row.completedAt,
      row.errorMessage,
    )
    .run();
};

export const getFinishPositionInferenceState = async (
  db: D1Database,
  raceKey: string,
): Promise<FinishPositionInferenceStateRow | null> => {
  const row = await db
    .prepare(
      `select race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
        status, predictions_r2_key, model_version, attempted_at, completed_at, error_message
       from finish_position_inference_state where race_key = ?`,
    )
    .bind(raceKey)
    .first<FinishPositionInferenceStateD1Row>();
  if (!row) {
    return null;
  }
  return {
    raceKey: row.race_key,
    source: row.source,
    kaisaiNen: row.kaisai_nen,
    kaisaiTsukihi: row.kaisai_tsukihi,
    keibajoCode: row.keibajo_code,
    raceBango: row.race_bango,
    status: row.status,
    predictionsR2Key: row.predictions_r2_key,
    modelVersion: row.model_version,
    attemptedAt: row.attempted_at,
    completedAt: row.completed_at,
    errorMessage: row.error_message,
  };
};
