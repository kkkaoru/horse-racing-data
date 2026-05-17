// Run with bun. Batch upsert helpers for the D1 `race_running_styles`
// table populated by the v1.5 Worker inference pipeline. Centralises the
// SQL so both the Cron consumer and the on-demand admin route see the
// same column order.

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
