// Run with bun. Builds a per-WIN5-day ML score lookup from
// race_finish_position_model_predictions.

import "server-only";

import type { Pool } from "pg";

import type {
  Win5ModelScoreLookup,
  Win5ModelScoreLookupParams,
} from "./leg-inputs";

interface BuildLookupParams {
  pool: Pool;
  modelVersion: string;
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
}

interface ModelScoreRow {
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  predicted_score: string;
}

interface BuildRaceIdParams {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

const buildRaceId = (params: BuildRaceIdParams): string =>
  `${params.source}:${params.kaisaiNen}:${params.kaisaiTsukihi}:${params.keibajoCode}:${params.raceBango}`;

const buildLookupKey = (params: { raceId: string; kettoTorokuBango: string }): string =>
  `${params.raceId}|${params.kettoTorokuBango}`;

const buildScoreMap = (params: BuildLookupParams, rows: readonly ModelScoreRow[]): Map<string, number> => {
  const map = new Map<string, number>();
  rows.forEach((row) => {
    const raceId = buildRaceId({
      source: params.source,
      kaisaiNen: params.kaisaiNen,
      kaisaiTsukihi: params.kaisaiTsukihi,
      keibajoCode: row.keibajo_code,
      raceBango: row.race_bango,
    });
    const numeric = Number(row.predicted_score);
    if (Number.isFinite(numeric)) {
      map.set(buildLookupKey({ raceId, kettoTorokuBango: row.ketto_toroku_bango }), numeric);
    }
  });
  return map;
};

export const buildModelScoreLookupFromPool = async (
  params: BuildLookupParams,
): Promise<Win5ModelScoreLookup> => {
  const result = await params.pool.query<ModelScoreRow>(
    `
      select keibajo_code, race_bango, ketto_toroku_bango, predicted_score
      from race_finish_position_model_predictions
      where model_version = $1
        and source = $2
        and kaisai_nen = $3
        and kaisai_tsukihi = $4
    `,
    [params.modelVersion, params.source, params.kaisaiNen, params.kaisaiTsukihi],
  );
  const map = buildScoreMap(params, result.rows);
  return {
    get: (lookupParams: Win5ModelScoreLookupParams): number | null =>
      map.get(buildLookupKey(lookupParams)) ?? null,
  };
};

export { buildRaceId };
