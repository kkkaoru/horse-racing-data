// Run with bun. Per-race feature build that reads ONLY from Hyperdrive (Postgres).
// The old legacy D1 `daily_race_entries` read is forbidden by Phase 0 rule 3.

import type { Pool } from "pg";

import { buildDailyFeatureSelectSql } from "./build-sql";
import { normaliseDailyRaceEntryRow } from "./normalise";
import { getFeaturesPool } from "./postgres-pool";
import type { DailyRaceEntryRow, Env, RaceJobKey } from "../types";

interface BuildRaceFeaturesContext {
  pool?: Pool;
}

export const fetchAllRaceFeatures = async (
  pool: Pool,
  options: { fromDate: string; toDate?: string; sourceScope?: "all" | "ban-ei" | "jra" | "nar" },
): Promise<DailyRaceEntryRow[]> => {
  const sql = buildDailyFeatureSelectSql(options);
  const result = await pool.query<Record<string, unknown>>(sql);
  return result.rows.map(normaliseDailyRaceEntryRow);
};

const filterRowsByRace = (rows: DailyRaceEntryRow[], job: RaceJobKey): DailyRaceEntryRow[] =>
  rows.filter(
    (row) =>
      row.source === job.source &&
      row.kaisai_nen === job.kaisaiNen &&
      row.kaisai_tsukihi === job.kaisaiTsukihi &&
      row.keibajo_code === job.keibajoCode.padStart(2, "0") &&
      row.race_bango === job.raceBango.padStart(2, "0"),
  );

const resolveScope = (source: "jra" | "nar"): "jra" | "nar" => source;

export const buildRaceFeatures = async (
  job: RaceJobKey,
  env: Env,
  context: BuildRaceFeaturesContext = {},
): Promise<DailyRaceEntryRow[]> => {
  const date = `${job.kaisaiNen}${job.kaisaiTsukihi}`;
  const pool = context.pool ?? getFeaturesPool(env);
  const rows = await fetchAllRaceFeatures(pool, {
    fromDate: date,
    toDate: date,
    sourceScope: resolveScope(job.source),
  });
  return filterRowsByRace(rows, job);
};
