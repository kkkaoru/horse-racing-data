// Run with bun. Resolves per-date race lists for running-style jobs.

import type { Pool } from "pg";

import { getFinishPositionPool } from "./finish-position-lite-pool";
import type { RegisteredRaceRow } from "./running-style-cron";
import type { Env } from "./types";

export type RunningStyleRaceListSource = "d1" | "features";

export interface RunningStyleRaceListResult {
  races: RegisteredRaceRow[];
  source: RunningStyleRaceListSource;
}

interface FeatureRaceRow {
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  source: "jra" | "nar";
}

const listRegisteredRacesByDateFromD1 = async (
  db: D1Database,
  date: string,
): Promise<RegisteredRaceRow[]> => {
  const result = await db
    .prepare(
      `select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
         from realtime_race_sources
        where source in ('jra', 'nar')
          and kaisai_nen = ?
          and kaisai_tsukihi = ?
        order by race_start_at_jst, source, keibajo_code, race_bango`,
    )
    .bind(date.slice(0, 4), date.slice(4, 8))
    .all<RegisteredRaceRow>();
  return result.results;
};

const listRegisteredRacesByDateFromFeatures = async (
  pool: Pool,
  date: string,
): Promise<RegisteredRaceRow[]> => {
  const featureResult = await pool.query<FeatureRaceRow>(
    `
      select distinct
        source,
        kaisai_nen,
        kaisai_tsukihi,
        lpad(keibajo_code::text, 2, '0') as keibajo_code,
        lpad(race_bango::text, 2, '0') as race_bango
      from race_entry_corner_features
      where source in ('jra', 'nar')
        and race_date = $1
      order by source, keibajo_code, race_bango
    `,
    [date],
  );
  return featureResult.rows;
};

export const listRunningStyleRacesByDate = async (
  env: Env,
  date: string,
): Promise<RunningStyleRaceListResult> => {
  const d1Races = await listRegisteredRacesByDateFromD1(env.REALTIME_DB, date);
  if (d1Races.length > 0) {
    return { races: d1Races, source: "d1" };
  }
  const pool = getFinishPositionPool(env);
  const featureRaces = await listRegisteredRacesByDateFromFeatures(pool, date);
  return { races: featureRaces, source: "features" };
};
