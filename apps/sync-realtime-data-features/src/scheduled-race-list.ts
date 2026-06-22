// Run with bun. Hyperdrive-direct read of today's race_key list for the
// scheduled handler. Replaces the legacy `REALTIME_OLD.fetch` path that
// hit the old D1-backed worker endpoint
// `/api/internal/list-race-keys-by-date-from-hyperdrive`.
// daily_race_entries SELECT is forbidden by Phase 0 rule 3.

import type { Pool } from "pg";

import { getFeaturesPool } from "./features/postgres-pool";
import { getTodayRaceKeysFromKv, putTodayRaceKeysToKv } from "./gates/today-race-keys-kv-cache";
import { computeTomorrowJst } from "./time";
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

const partitionByCachedSource = (
  rows: TodayRaceKey[],
): { jra: TodayRaceKey[]; nar: TodayRaceKey[] } => ({
  jra: rows.filter((row) => row.source === "jra"),
  nar: rows.filter((row) => row.source === "nar"),
});

interface ListTodayRaceKeysWithKvCacheArgs {
  env: Env;
  yyyymmdd: string;
  context: ListTodayRaceKeysContext;
}

interface ListTomorrowRaceKeysWithKvCacheArgs {
  env: Env;
  now: Date;
  context: ListTodayRaceKeysContext;
}

const fetchAndCacheTodayRaceKeys = async (
  args: ListTodayRaceKeysWithKvCacheArgs,
): Promise<TodayRaceKey[]> => {
  const fresh = await listTodayRaceKeysFromHyperdrive(args.env, args.yyyymmdd, args.context);
  const partitioned = partitionByCachedSource(fresh);
  await Promise.all([
    putTodayRaceKeysToKv(args.env, "jra", args.yyyymmdd, partitioned.jra),
    putTodayRaceKeysToKv(args.env, "nar", args.yyyymmdd, partitioned.nar),
  ]);
  return fresh;
};

// KV-cached variant of listTodayRaceKeysFromHyperdrive. Reads the per-source
// `race-keys:v1:{source}:{yyyymmdd}` entries first; only when at least one
// is missing does it fall through to Hyperdrive. The single UNION query is
// then partitioned and written back into KV so the next tick can short-circuit.
// On Hyperdrive failure we log + return an empty list so the */10 cron tick
// no-ops cleanly and the next tick retries; the put-side is gated on a
// successful query so the cache is never poisoned with empty values.
export const listTodayRaceKeysWithKvCache = async (
  args: ListTodayRaceKeysWithKvCacheArgs,
): Promise<TodayRaceKey[]> => {
  const [jraCached, narCached] = await Promise.all([
    getTodayRaceKeysFromKv(args.env, "jra", args.yyyymmdd),
    getTodayRaceKeysFromKv(args.env, "nar", args.yyyymmdd),
  ]);
  if (jraCached && narCached) {
    return [...jraCached, ...narCached];
  }
  try {
    return await fetchAndCacheTodayRaceKeys(args);
  } catch (error) {
    console.error("[features] listTodayRaceKeysWithKvCache hyperdrive failure", error);
    return [];
  }
};

export const listTomorrowRaceKeysWithKvCache = async (
  args: ListTomorrowRaceKeysWithKvCacheArgs,
): Promise<TodayRaceKey[]> => {
  const tomorrowJst = computeTomorrowJst(args.now);
  return listTodayRaceKeysWithKvCache({
    context: args.context,
    env: args.env,
    yyyymmdd: tomorrowJst,
  });
};

export const toRaceJobKeyFromTodayRaceKey = (entry: TodayRaceKey): RaceJobKey => ({
  kaisaiNen: entry.kaisaiNen,
  kaisaiTsukihi: entry.kaisaiTsukihi,
  keibajoCode: entry.keibajoCode,
  raceBango: entry.raceBango,
  raceKey: entry.raceKey,
  source: entry.source,
});

// Tomorrow's race lister — same SQL as today, but with kaisaiNen / kaisaiTsukihi
// computed from `now` shifted by +1 JST day. Used for Phase F auto-seed so the
// scheduled worker can preheat next-day Parquet without any Mac CLI.
export const listTomorrowRaceKeysFromHyperdrive = async (
  env: Env,
  now: Date,
  context: ListTodayRaceKeysContext = {},
): Promise<TodayRaceKey[]> => {
  const tomorrowJst = computeTomorrowJst(now);
  return listTodayRaceKeysFromHyperdrive(env, tomorrowJst, context);
};
