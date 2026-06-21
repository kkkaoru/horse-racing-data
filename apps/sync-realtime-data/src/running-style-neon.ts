// Run with bun. Neon upsert helpers for race_running_style_model_predictions.
// Called after D1 write succeeds so the viewer can read predictions from Neon.
// Failures are non-fatal — D1 remains the source of truth and the cron will
// retry the Neon write on the next tick via the backfill path.

import type { Pool } from "pg";

import type { RaceRunningStyleRow } from "./running-style-d1";
import type { RunningStyleClassLabel } from "./running-style-lightgbm-tree";

const NEON_BATCH_SIZE = 50;

const LABEL_CLASS_INDEX: Readonly<Record<RunningStyleClassLabel, number>> = {
  nige: 0,
  oikomi: 3,
  sashi: 2,
  senkou: 1,
};

interface RaceKeyParts {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

const parseRaceKey = (raceKey: string): RaceKeyParts | null => {
  const parts = raceKey.split(":");
  if (parts.length !== 4) return null;
  const [source, datePart, keibajoCode, raceBango] = parts as [string, string, string, string];
  if (datePart.length !== 8) return null;
  return {
    kaisaiNen: datePart.slice(0, 4),
    kaisaiTsukihi: datePart.slice(4, 8),
    keibajoCode,
    raceBango,
    source,
  };
};

const buildPlaceholders = (rowCount: number, colCount: number): string =>
  Array.from(
    { length: rowCount },
    (_, rowIndex) =>
      `(${Array.from({ length: colCount }, (__, colIndex) => `$${rowIndex * colCount + colIndex + 1}`).join(", ")})`,
  ).join(", ");

const upsertNeonBatch = async (
  pool: Pool,
  rows: ReadonlyArray<RaceRunningStyleRow>,
): Promise<void> => {
  const COL_COUNT = 14;
  const values = rows.flatMap((row) => {
    const parsed = parseRaceKey(row.raceKey)!;
    const predictedClass = LABEL_CLASS_INDEX[row.predictedLabel]!;
    return [
      row.modelVersion,
      parsed.source,
      parsed.kaisaiNen,
      parsed.kaisaiTsukihi,
      parsed.keibajoCode,
      parsed.raceBango,
      row.kettoTorokuBango,
      row.horseNumber,
      row.pNige,
      row.pSenkou,
      row.pSashi,
      row.pOikomi,
      row.predictedLabel,
      predictedClass,
    ];
  });
  const actualRows = values.length / COL_COUNT;
  const actualPlaceholders = buildPlaceholders(actualRows, COL_COUNT);
  await pool.query(
    `insert into race_running_style_model_predictions
       (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
        ketto_toroku_bango, umaban, p_nige, p_senkou, p_sashi, p_oikomi, predicted_label, predicted_class)
     values ${actualPlaceholders}
     on conflict (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
     do update set
       umaban = excluded.umaban,
       p_nige = excluded.p_nige,
       p_senkou = excluded.p_senkou,
       p_sashi = excluded.p_sashi,
       p_oikomi = excluded.p_oikomi,
       predicted_label = excluded.predicted_label,
       predicted_class = excluded.predicted_class,
       prediction_generated_at = now()`,
    values,
  );
};

export const upsertRunningStylePredictionsToNeon = async (
  pool: Pool,
  rows: ReadonlyArray<RaceRunningStyleRow>,
): Promise<number> => {
  if (rows.length === 0) return 0;
  const validRows = rows.filter((row) => {
    const parsed = parseRaceKey(row.raceKey);
    return parsed !== null && LABEL_CLASS_INDEX[row.predictedLabel] !== undefined;
  });
  for (let start = 0; start < validRows.length; start += NEON_BATCH_SIZE) {
    await upsertNeonBatch(pool, validRows.slice(start, start + NEON_BATCH_SIZE));
  }
  return validRows.length;
};
