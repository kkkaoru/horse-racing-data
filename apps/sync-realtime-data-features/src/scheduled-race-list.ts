// Run with bun. Hyperdrive-direct read of today's race_key list for the
// scheduled handler. Replaces the legacy `REALTIME_OLD.fetch` path that
// hit the old D1-backed worker endpoint
// `/api/internal/list-race-keys-by-date-from-hyperdrive`.
// daily_race_entries SELECT is forbidden by Phase 0 rule 3.

import type { Pool } from "pg";

import { getFeaturesPool } from "./features/postgres-pool";
import type { Env, RaceJobKey } from "./types";

export type TodayRaceKeySource = "jra" | "nar";

export interface TodayRaceKey {
  raceKey: string;
  source: TodayRaceKeySource;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

export interface ListTodayRaceKeysContext {
  pool?: Pool;
}

interface SourcedRaceKeyRow {
  [key: string]: unknown;
  source: TodayRaceKeySource;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
}

const KEIBAJO_CODE_PAD_WIDTH = 2;
const RACE_BANGO_PAD_WIDTH = 2;

const SELECT_RACE_KEYS_SQL = `
  select 'jra' as source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
  from jvd_ra
  where kaisai_nen = $1 and kaisai_tsukihi = $2
  union all
  select 'nar' as source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
  from nvd_ra
  where kaisai_nen = $1 and kaisai_tsukihi = $2
  order by source, keibajo_code, race_bango
`;

const isTodayRaceKeySource = (value: unknown): value is TodayRaceKeySource =>
  value === "jra" || value === "nar";

const normaliseCode = (value: string, width: number): string => value.padStart(width, "0");

const buildRaceKey = (row: SourcedRaceKeyRow): string =>
  `${row.source}:${row.kaisai_nen}:${row.kaisai_tsukihi}:${normaliseCode(row.keibajo_code, KEIBAJO_CODE_PAD_WIDTH)}:${normaliseCode(row.race_bango, RACE_BANGO_PAD_WIDTH)}`;

const toTodayRaceKey = (row: SourcedRaceKeyRow): TodayRaceKey => ({
  kaisaiNen: row.kaisai_nen,
  kaisaiTsukihi: row.kaisai_tsukihi,
  keibajoCode: normaliseCode(row.keibajo_code, KEIBAJO_CODE_PAD_WIDTH),
  raceBango: normaliseCode(row.race_bango, RACE_BANGO_PAD_WIDTH),
  raceKey: buildRaceKey(row),
  source: row.source,
});

const isCompleteRow = (row: Record<string, unknown>): row is SourcedRaceKeyRow =>
  isTodayRaceKeySource(row.source) &&
  typeof row.kaisai_nen === "string" &&
  typeof row.kaisai_tsukihi === "string" &&
  typeof row.keibajo_code === "string" &&
  typeof row.race_bango === "string";

const splitTodayYyyymmdd = (yyyymmdd: string): { kaisaiNen: string; kaisaiTsukihi: string } => ({
  kaisaiNen: yyyymmdd.slice(0, 4),
  kaisaiTsukihi: yyyymmdd.slice(4, 8),
});

export const listTodayRaceKeysFromHyperdrive = async (
  env: Env,
  yyyymmdd: string,
  context: ListTodayRaceKeysContext = {},
): Promise<TodayRaceKey[]> => {
  const pool = context.pool ?? getFeaturesPool(env);
  const { kaisaiNen, kaisaiTsukihi } = splitTodayYyyymmdd(yyyymmdd);
  const result = await pool.query<Record<string, unknown>>(SELECT_RACE_KEYS_SQL, [
    kaisaiNen,
    kaisaiTsukihi,
  ]);
  return result.rows.filter(isCompleteRow).map(toTodayRaceKey);
};

export const toRaceJobKeyFromTodayRaceKey = (entry: TodayRaceKey): RaceJobKey => ({
  kaisaiNen: entry.kaisaiNen,
  kaisaiTsukihi: entry.kaisaiTsukihi,
  keibajoCode: entry.keibajoCode,
  raceBango: entry.raceBango,
  raceKey: entry.raceKey,
  source: entry.source,
});
