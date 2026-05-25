// Run with bun. Builds a per-WIN5-day ML score lookup from
// race_finish_position_model_predictions. Mirrors the per-race rs-overlay
// fallback used by getFinishPositionLambdarankPredictions (see commit
// c0424cb): when a `<modelVersion>-rs-overlay-<YYYYMMDD>` variant exists for
// the WIN5 day, the lookup transparently picks it up; otherwise it falls
// back to the base model_version.

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

const buildOverlayModelVersion = (params: {
  baseModelVersion: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
}): string =>
  `${params.baseModelVersion}-rs-overlay-${params.kaisaiNen}${params.kaisaiTsukihi}`;

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
  const overlayModelVersion = buildOverlayModelVersion({
    baseModelVersion: params.modelVersion,
    kaisaiNen: params.kaisaiNen,
    kaisaiTsukihi: params.kaisaiTsukihi,
  });
  // Prefer per-day overlay rows when present; coalesce to base model otherwise.
  // The CTE filters by date+source so both candidate sets stay bounded to ~600
  // rows on a WIN5 day, then picks whichever model_version supplies rows for
  // each (keibajo, race, horse) triple.
  const result = await params.pool.query<ModelScoreRow>(
    `
      with day_rows as (
        select
          model_version,
          keibajo_code,
          race_bango,
          ketto_toroku_bango,
          predicted_score,
          case when model_version = $5 then 0 else 1 end as priority
        from race_finish_position_model_predictions
        where source = $2
          and kaisai_nen = $3
          and kaisai_tsukihi = $4
          and model_version in ($1, $5)
      )
      select distinct on (keibajo_code, race_bango, ketto_toroku_bango)
        keibajo_code, race_bango, ketto_toroku_bango, predicted_score
      from day_rows
      order by keibajo_code, race_bango, ketto_toroku_bango, priority
    `,
    [
      params.modelVersion,
      params.source,
      params.kaisaiNen,
      params.kaisaiTsukihi,
      overlayModelVersion,
    ],
  );
  const map = buildScoreMap(params, result.rows);
  return {
    get: (lookupParams: Win5ModelScoreLookupParams): number | null =>
      map.get(buildLookupKey(lookupParams)) ?? null,
  };
};

export { buildRaceId };
