// Run with bun. Batch upsert helpers for the D1 `race_running_styles`
// table populated by the v1.5 Worker inference pipeline. Centralises the
// SQL so both the Cron consumer and the on-demand admin route see the
// same column order.

import {
  type D1QueryCacheRaceDayContext,
  withD1QueryCache,
} from "./d1-query-cache";
import type { RunningStyleClassLabel } from "./running-style-lightgbm-tree";

export interface RaceRunningStyleRow {
  raceKey: string;
  horseNumber: number;
  kettoTorokuBango: string;
  bamei: string | null;
  category: string;
  kaisaiNen: string;
  modelVersion: string;
  pNige: number;
  pSenkou: number;
  pSashi: number;
  pOikomi: number;
  predictedLabel: RunningStyleClassLabel;
  predictedAt: string;
}

export interface RaceRunningStyleCount {
  raceKey: string;
  count: number;
}

export type RunningStyleInferenceStatus = "pending" | "processing" | "completed" | "failed";

export interface RunningStyleInferenceState {
  raceKey: string;
  status: RunningStyleInferenceStatus;
  attemptedAt: string | null;
}

export interface RunningStyleInferenceStateDetail extends RunningStyleInferenceState {
  completedAt: string | null;
  expectedHorseCount: number | null;
  featuresR2Key: string | null;
  modelVersion: string | null;
  writtenHorseCount: number | null;
}

export interface RunningStyleInferenceRace {
  raceKey: string;
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

export interface RunningStylePendingRace extends RunningStyleInferenceRace {
  expectedHorseCount: number;
}

const D1_BATCH_SIZE = 50;

const INSERT_SQL = `insert or replace into race_running_styles (
  race_key, horse_number, ketto_toroku_bango, bamei, category, kaisai_nen,
  model_version, p_nige, p_senkou, p_sashi, p_oikomi, predicted_label, predicted_at
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

const bindValues = (row: RaceRunningStyleRow): unknown[] => [
  row.raceKey,
  row.horseNumber,
  row.kettoTorokuBango,
  row.bamei,
  row.category,
  row.kaisaiNen,
  row.modelVersion,
  row.pNige,
  row.pSenkou,
  row.pSashi,
  row.pOikomi,
  row.predictedLabel,
  row.predictedAt,
];

const chunkArray = <T>(items: ReadonlyArray<T>, size: number): ReadonlyArray<ReadonlyArray<T>> => {
  const chunks: T[][] = [];
  items.forEach((item, index) => {
    if (index % size === 0) chunks.push([]);
    chunks[chunks.length - 1]?.push(item);
  });
  return chunks;
};

const buildPlaceholders = (count: number): string =>
  Array.from({ length: count }, () => "?").join(",");

const parseRaceDayFromRaceKey = (raceKey: string): D1QueryCacheRaceDayContext | undefined => {
  const parts = raceKey.split(":");
  const datePart = parts[1];
  if (datePart === undefined || datePart.length < 8) {
    return undefined;
  }
  return {
    kaisaiNen: datePart.slice(0, 4),
    kaisaiTsukihi: datePart.slice(4, 8),
  };
};

export const upsertRaceRunningStyles = async (
  db: D1Database,
  rows: ReadonlyArray<RaceRunningStyleRow>,
): Promise<number> => {
  if (rows.length === 0) return 0;
  const statements = rows.map((row) => db.prepare(INSERT_SQL).bind(...bindValues(row)));
  const batches = chunkArray(statements, D1_BATCH_SIZE);
  const tasks = batches.map((batch) => db.batch([...batch]));
  await Promise.all(tasks);
  return rows.length;
};

export const listRaceRunningStyleCounts = async (
  db: D1Database,
  raceKeys: ReadonlyArray<string>,
  ctx?: ExecutionContext,
): Promise<Map<string, number>> => {
  const uniqueRaceKeys = Array.from(new Set(raceKeys.filter((raceKey) => raceKey.length > 0)));
  if (uniqueRaceKeys.length === 0) {
    return new Map();
  }
  return withD1QueryCache(
    "running-style-races",
    ["listRaceRunningStyleCounts", uniqueRaceKeys],
    async () => {
      const counts = new Map<string, number>();
      for (const chunk of chunkArray(uniqueRaceKeys, D1_BATCH_SIZE)) {
        if (chunk.length === 0) continue;
        const result = await db
          .prepare(
            `select race_key, count(*) as count
               from race_running_styles
              where race_key in (${buildPlaceholders(chunk.length)})
              group by race_key`,
          )
          .bind(...chunk)
          .all<{ race_key: string; count: number }>();
        result.results.forEach((row) => counts.set(row.race_key, Number(row.count)));
      }
      return Object.fromEntries(counts);
    },
    { ctx, raceDay: parseRaceDayFromRaceKey(uniqueRaceKeys[0] ?? "") },
  ).then((record) => new Map(Object.entries(record)));
};

const queryRaceRunningStylesForRace = async (
  db: D1Database,
  raceKey: string,
): Promise<RaceRunningStyleRow[]> => {
  const result = await db
    .prepare(
      `select race_key, horse_number, ketto_toroku_bango, bamei, category, kaisai_nen,
              model_version, p_nige, p_senkou, p_sashi, p_oikomi, predicted_label, predicted_at
         from race_running_styles
        where race_key = ?
        order by horse_number`,
    )
    .bind(raceKey)
    .all<{
      bamei: string | null;
      category: string;
      horse_number: number;
      kaisai_nen: string;
      ketto_toroku_bango: string;
      model_version: string;
      p_nige: number;
      p_oikomi: number;
      p_sashi: number;
      p_senkou: number;
      predicted_at: string;
      predicted_label: RaceRunningStyleRow["predictedLabel"];
      race_key: string;
    }>();
  return result.results.map((row) => ({
    bamei: row.bamei,
    category: row.category,
    horseNumber: Number(row.horse_number),
    kaisaiNen: row.kaisai_nen,
    kettoTorokuBango: row.ketto_toroku_bango,
    modelVersion: row.model_version,
    pNige: Number(row.p_nige),
    pOikomi: Number(row.p_oikomi),
    pSashi: Number(row.p_sashi),
    pSenkou: Number(row.p_senkou),
    predictedAt: row.predicted_at,
    predictedLabel: row.predicted_label,
    raceKey: row.race_key,
  }));
};

export const listRaceRunningStylesForRace = async (
  db: D1Database,
  raceKey: string,
  ctx?: ExecutionContext,
): Promise<RaceRunningStyleRow[]> =>
  withD1QueryCache(
    "running-style-race",
    ["getRaceRunningStylesFromD1", raceKey],
    () => queryRaceRunningStylesForRace(db, raceKey),
    { ctx, raceDay: parseRaceDayFromRaceKey(raceKey) },
  );

export const listRunningStyleInferenceStates = async (
  db: D1Database,
  raceKeys: ReadonlyArray<string>,
): Promise<Map<string, RunningStyleInferenceState>> => {
  const states = new Map<string, RunningStyleInferenceState>();
  for (const chunk of chunkArray(raceKeys, D1_BATCH_SIZE)) {
    if (chunk.length === 0) continue;
    const result = await db
      .prepare(
        `select race_key, status, attempted_at
           from running_style_inference_state
          where race_key in (${buildPlaceholders(chunk.length)})`,
      )
      .bind(...chunk)
      .all<{
        race_key: string;
        status: RunningStyleInferenceStatus;
        attempted_at: string | null;
      }>();
    result.results.forEach((row) =>
      states.set(row.race_key, {
        attemptedAt: row.attempted_at,
        raceKey: row.race_key,
        status: row.status,
      }),
    );
  }
  return states;
};

export const getRunningStyleInferenceState = async (
  db: D1Database,
  raceKey: string,
): Promise<RunningStyleInferenceStateDetail | null> => {
  const row = await db
    .prepare(
      `select race_key, status, attempted_at, features_r2_key, model_version,
              expected_horse_count, written_horse_count, completed_at
         from running_style_inference_state
        where race_key = ?`,
    )
    .bind(raceKey)
    .first<{
      attempted_at: string | null;
      completed_at: string | null;
      expected_horse_count: number | null;
      features_r2_key: string | null;
      model_version: string | null;
      race_key: string;
      status: RunningStyleInferenceStatus;
      written_horse_count: number | null;
    }>();
  if (row === null) return null;
  return {
    attemptedAt: row.attempted_at,
    completedAt: row.completed_at,
    expectedHorseCount: row.expected_horse_count === null ? null : Number(row.expected_horse_count),
    featuresR2Key: row.features_r2_key,
    modelVersion: row.model_version,
    raceKey: row.race_key,
    status: row.status,
    writtenHorseCount: row.written_horse_count === null ? null : Number(row.written_horse_count),
  };
};

export const upsertRunningStylePendingStates = async (
  db: D1Database,
  rows: ReadonlyArray<RunningStylePendingRace>,
  nowIso: string,
): Promise<void> => {
  if (rows.length === 0) return;
  const statements = rows.map((row) =>
    db
      .prepare(
        `insert into running_style_inference_state (
          race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
          status, expected_horse_count, attempted_at, completed_at, error_message
        ) values (?, ?, ?, ?, ?, ?, 'pending', ?, ?, null, null)
        on conflict(race_key) do update set
          status = 'pending',
          source = excluded.source,
          kaisai_nen = excluded.kaisai_nen,
          kaisai_tsukihi = excluded.kaisai_tsukihi,
          keibajo_code = excluded.keibajo_code,
          race_bango = excluded.race_bango,
          features_r2_key = null,
          model_version = null,
          expected_horse_count = excluded.expected_horse_count,
          written_horse_count = null,
          attempted_at = excluded.attempted_at,
          completed_at = null,
          error_message = null`,
      )
      .bind(
        row.raceKey,
        row.source,
        row.kaisaiNen,
        row.kaisaiTsukihi,
        row.keibajoCode,
        row.raceBango,
        row.expectedHorseCount,
        nowIso,
      ),
  );
  for (const chunk of chunkArray(statements, D1_BATCH_SIZE)) {
    await db.batch([...chunk]);
  }
};

export const markRunningStyleInferenceProcessing = async (
  db: D1Database,
  row: RunningStyleInferenceRace,
  nowIso: string,
): Promise<void> => {
  await db
    .prepare(
      `insert into running_style_inference_state (
        race_key, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
        status, attempted_at, error_message
      ) values (?, ?, ?, ?, ?, ?, 'processing', ?, null)
      on conflict(race_key) do update set
        status = 'processing',
        attempted_at = excluded.attempted_at,
        error_message = null`,
    )
    .bind(
      row.raceKey,
      row.source,
      row.kaisaiNen,
      row.kaisaiTsukihi,
      row.keibajoCode,
      row.raceBango,
      nowIso,
    )
    .run();
};

export const markRunningStyleInferenceCompleted = async (
  db: D1Database,
  params: {
    raceKey: string;
    featuresR2Key: string;
    modelVersion: string;
    expectedHorseCount: number;
    writtenHorseCount: number;
    completedAt: string;
  },
): Promise<void> => {
  await db
    .prepare(
      `update running_style_inference_state
          set status = 'completed',
              features_r2_key = ?,
              model_version = ?,
              expected_horse_count = ?,
              written_horse_count = ?,
              completed_at = ?,
              error_message = null
        where race_key = ?`,
    )
    .bind(
      params.featuresR2Key,
      params.modelVersion,
      params.expectedHorseCount,
      params.writtenHorseCount,
      params.completedAt,
      params.raceKey,
    )
    .run();
};

export const markRunningStyleInferenceFailed = async (
  db: D1Database,
  raceKey: string,
  error: unknown,
): Promise<void> => {
  const message = error instanceof Error ? error.message : String(error);
  await db
    .prepare(
      `update running_style_inference_state
          set status = 'failed',
              error_message = ?
        where race_key = ?`,
    )
    .bind(message, raceKey)
    .run();
};
