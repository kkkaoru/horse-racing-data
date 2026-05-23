// Run with bun. D1 helpers for finish-position feature inference state only.
// Predictions are generated dynamically at request time and are not stored in D1.

export type FinishPositionInferenceStatus = "pending" | "processing" | "completed" | "failed";

export interface FinishPositionInferenceRace {
  raceKey: string;
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

export interface FinishPositionInferenceState {
  raceKey: string;
  status: FinishPositionInferenceStatus;
  featuresR2Key: string | null;
  modelVersion: string | null;
  attemptedAt: string | null;
  completedAt: string | null;
}

const INSERT_OR_REPLACE_SQL = `insert or replace into finish_position_inference_state (
  race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
  status, predictions_r2_key, model_version, attempted_at, completed_at, error_message
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

export const markFinishPositionFeaturesCached = async (
  db: D1Database,
  race: FinishPositionInferenceRace,
  {
    attemptedAt,
    completedAt,
    featuresR2Key,
    modelVersion,
  }: {
    attemptedAt: string;
    completedAt: string;
    featuresR2Key: string;
    modelVersion: string;
  },
): Promise<void> => {
  await db
    .prepare(INSERT_OR_REPLACE_SQL)
    .bind(
      race.raceKey,
      race.source,
      race.kaisaiNen,
      race.kaisaiTsukihi,
      race.keibajoCode,
      race.raceBango,
      "completed",
      featuresR2Key,
      modelVersion,
      attemptedAt,
      completedAt,
      null,
    )
    .run();
};

export const getFinishPositionInferenceState = async (
  db: D1Database,
  raceKey: string,
): Promise<FinishPositionInferenceState | null> => {
  const row = await db
    .prepare(
      `select race_key, status, predictions_r2_key, model_version, attempted_at, completed_at
         from finish_position_inference_state
        where race_key = ?`,
    )
    .bind(raceKey)
    .first<{
      attempted_at: string | null;
      completed_at: string | null;
      model_version: string | null;
      predictions_r2_key: string | null;
      race_key: string;
      status: FinishPositionInferenceStatus;
    }>();
  if (!row) {
    return null;
  }
  return {
    attemptedAt: row.attempted_at,
    completedAt: row.completed_at,
    featuresR2Key: row.predictions_r2_key,
    modelVersion: row.model_version,
    raceKey: row.race_key,
    status: row.status,
  };
};
