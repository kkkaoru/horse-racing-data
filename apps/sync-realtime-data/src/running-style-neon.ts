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
  const COL_COUNT = 16;
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
      row.cellModelKey ?? null,
      row.cellVariantId ?? null,
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
        ketto_toroku_bango, umaban, cell_model_key, cell_variant_id,
        p_nige, p_senkou, p_sashi, p_oikomi, predicted_label, predicted_class)
     values ${actualPlaceholders}
     on conflict (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
     do update set
       umaban = excluded.umaban,
       cell_model_key = excluded.cell_model_key,
       cell_variant_id = excluded.cell_variant_id,
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

export const listRaceRunningStylePredictionCountsByDate = async (
  pool: Pool,
  date: string,
): Promise<Map<string, Map<string, number>>> => {
  const kaisaiNen = date.slice(0, 4);
  const kaisaiTsukihi = date.slice(4, 8);
  const result = await pool.query<{
    count: string;
    kaisai_nen: string;
    kaisai_tsukihi: string;
    keibajo_code: string;
    model_version: string;
    race_bango: string;
    source: string;
  }>(
    `select model_version, source, kaisai_nen, kaisai_tsukihi,
            lpad(keibajo_code::text, 2, '0') as keibajo_code,
            lpad(race_bango::text, 2, '0') as race_bango,
            count(*)::text as count
       from race_running_style_model_predictions
      where source in ('jra', 'nar')
        and kaisai_nen = $1
        and kaisai_tsukihi = $2
      group by model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`,
    [kaisaiNen, kaisaiTsukihi],
  );
  const counts = new Map<string, Map<string, number>>();
  result.rows.forEach((row) => {
    const raceKey = `${row.source}:${row.kaisai_nen}${row.kaisai_tsukihi}:${row.keibajo_code}:${row.race_bango}`;
    const modelCounts = counts.get(raceKey) ?? new Map<string, number>();
    modelCounts.set(row.model_version, Number(row.count));
    counts.set(raceKey, modelCounts);
  });
  return counts;
};
