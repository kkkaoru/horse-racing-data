import type { Win5PredictionPayload, Win5Schedule } from "../../pc-keiba-viewer/src/lib/win5/types";

export interface Win5ScheduleRow {
  kaisai_nen: string;
  kaisai_tsukihi: string;
  sale_deadline: string | null;
  source: string;
  legs_json: string;
  fetched_at: string;
}

export interface Win5PredictionRow {
  kaisai_nen: string;
  kaisai_tsukihi: string;
  model_version: string;
  recommended_budget_yen: number;
  default_budget_yen: number;
  prediction_json: string;
  predicted_at: string;
}

export const serializeWin5Schedule = (schedule: Win5Schedule): string => JSON.stringify(schedule);

export const parseWin5ScheduleRow = (row: Win5ScheduleRow): Win5Schedule => {
  const parsed = JSON.parse(row.legs_json) as Pick<Win5Schedule, "legs">;
  return {
    fetchedAt: row.fetched_at,
    kaisaiNen: row.kaisai_nen,
    kaisaiTsukihi: row.kaisai_tsukihi,
    legs: parsed.legs,
    saleDeadline: row.sale_deadline,
    source: row.source === "jvd_wf" ? "jvd_wf" : "jra_web",
  };
};

export const parseWin5PredictionRow = (row: Win5PredictionRow): Win5PredictionPayload =>
  JSON.parse(row.prediction_json) as Win5PredictionPayload;

export const upsertWin5Schedule = async (db: D1Database, schedule: Win5Schedule): Promise<void> => {
  await db
    .prepare(
      `
        insert into win5_schedules (
          kaisai_nen,
          kaisai_tsukihi,
          sale_deadline,
          source,
          legs_json,
          fetched_at
        ) values (?, ?, ?, ?, ?, ?)
        on conflict (kaisai_nen, kaisai_tsukihi) do update set
          sale_deadline = excluded.sale_deadline,
          source = excluded.source,
          legs_json = excluded.legs_json,
          fetched_at = excluded.fetched_at
      `,
    )
    .bind(
      schedule.kaisaiNen,
      schedule.kaisaiTsukihi,
      schedule.saleDeadline ?? null,
      schedule.source,
      serializeWin5Schedule(schedule),
      schedule.fetchedAt,
    )
    .run();
};

export const upsertWin5Prediction = async (
  db: D1Database,
  payload: Win5PredictionPayload,
): Promise<void> => {
  await db
    .prepare(
      `
        insert into win5_predictions (
          kaisai_nen,
          kaisai_tsukihi,
          model_version,
          recommended_budget_yen,
          default_budget_yen,
          prediction_json,
          predicted_at
        ) values (?, ?, ?, ?, ?, ?, ?)
        on conflict (kaisai_nen, kaisai_tsukihi, model_version) do update set
          recommended_budget_yen = excluded.recommended_budget_yen,
          default_budget_yen = excluded.default_budget_yen,
          prediction_json = excluded.prediction_json,
          predicted_at = excluded.predicted_at
      `,
    )
    .bind(
      payload.kaisaiNen,
      payload.kaisaiTsukihi,
      payload.modelVersion,
      payload.recommendedBudgetYen,
      payload.defaultBudgetYen,
      JSON.stringify(payload),
      payload.predictedAt,
    )
    .run();
};

export const getWin5Schedule = async (
  db: D1Database,
  kaisaiNen: string,
  kaisaiTsukihi: string,
): Promise<Win5Schedule | null> => {
  const row = await db
    .prepare(
      `
        select
          kaisai_nen,
          kaisai_tsukihi,
          sale_deadline,
          source,
          legs_json,
          fetched_at
        from win5_schedules
        where kaisai_nen = ? and kaisai_tsukihi = ?
      `,
    )
    .bind(kaisaiNen, kaisaiTsukihi)
    .first<Win5ScheduleRow>();
  return row ? parseWin5ScheduleRow(row) : null;
};

export const getWin5Prediction = async (
  db: D1Database,
  kaisaiNen: string,
  kaisaiTsukihi: string,
  modelVersion: string,
): Promise<Win5PredictionPayload | null> => {
  const row = await db
    .prepare(
      `
        select
          kaisai_nen,
          kaisai_tsukihi,
          model_version,
          recommended_budget_yen,
          default_budget_yen,
          prediction_json,
          predicted_at
        from win5_predictions
        where kaisai_nen = ? and kaisai_tsukihi = ? and model_version = ?
      `,
    )
    .bind(kaisaiNen, kaisaiTsukihi, modelVersion)
    .first<Win5PredictionRow>();
  return row ? parseWin5PredictionRow(row) : null;
};

export const listWin5SchedulesByYear = async (
  db: D1Database,
  kaisaiNen: string,
): Promise<Win5Schedule[]> => {
  const result = await db
    .prepare(
      `
        select
          kaisai_nen,
          kaisai_tsukihi,
          sale_deadline,
          source,
          legs_json,
          fetched_at
        from win5_schedules
        where kaisai_nen = ?
        order by kaisai_tsukihi asc
      `,
    )
    .bind(kaisaiNen)
    .all<Win5ScheduleRow>();
  return (result.results ?? []).map(parseWin5ScheduleRow);
};

export const markWin5InferenceState = async (
  db: D1Database,
  params: {
    kaisaiNen: string;
    kaisaiTsukihi: string;
    status: "pending" | "processing" | "completed" | "failed";
    lastError?: string | null;
    incrementAttempt?: boolean;
    updatedAt: string;
  },
): Promise<void> => {
  await db
    .prepare(
      `
        insert into win5_inference_state (
          kaisai_nen,
          kaisai_tsukihi,
          status,
          attempt_count,
          last_error,
          updated_at
        ) values (?, ?, ?, ?, ?, ?)
        on conflict (kaisai_nen, kaisai_tsukihi) do update set
          status = excluded.status,
          attempt_count = case
            when excluded.status = 'processing' then win5_inference_state.attempt_count + 1
            else win5_inference_state.attempt_count
          end,
          last_error = excluded.last_error,
          updated_at = excluded.updated_at
      `,
    )
    .bind(
      params.kaisaiNen,
      params.kaisaiTsukihi,
      params.status,
      params.incrementAttempt ? 1 : 0,
      params.lastError ?? null,
      params.updatedAt,
    )
    .run();
};
