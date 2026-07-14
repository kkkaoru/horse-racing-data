// Run with bun. Catalog read of race keys for scheduled prediction and feature jobs.

import { fetchCatalogRows, isRecord } from "./catalog-client";
import {
  getTodayRaceKeysFromKv,
  putTodayRaceKeysToKv,
  type TodayRaceKeysKvEnv,
} from "./gates/today-race-keys-kv-cache";
import { computeTomorrowJst } from "./time";
import type { CatalogServiceBinding, Env, RaceJobKey } from "./types";

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
  catalog?: CatalogServiceBinding;
}

export type ScheduledRaceListEnv = Pick<Env, "PC_KEIBA_R2_CATALOG"> & TodayRaceKeysKvEnv;

interface CatalogRaceKeyRow {
  [key: string]: unknown;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  raceKey: string;
  source: TodayRaceKeySource;
}

const RACE_KEYS_URL = "https://pc-keiba-r2-catalog/v1/race-keys";

const isTodayRaceKeySource = (value: unknown): value is TodayRaceKeySource =>
  value === "jra" || value === "nar";

const toTodayRaceKey = (row: CatalogRaceKeyRow): TodayRaceKey => ({
  kaisaiNen: row.kaisaiNen,
  kaisaiTsukihi: row.kaisaiTsukihi,
  keibajoCode: row.keibajoCode,
  raceBango: row.raceBango,
  raceKey: row.raceKey,
  source: row.source,
});

const isCompleteRow = (row: unknown): row is CatalogRaceKeyRow =>
  isRecord(row) &&
  isTodayRaceKeySource(row.source) &&
  typeof row.kaisaiNen === "string" &&
  typeof row.kaisaiTsukihi === "string" &&
  typeof row.keibajoCode === "string" &&
  typeof row.raceBango === "string" &&
  typeof row.raceKey === "string";

const buildRaceKeysUrl = (yyyymmdd: string): URL => {
  const url = new URL(RACE_KEYS_URL);
  url.searchParams.set("date", yyyymmdd);
  return url;
};

export const listTodayRaceKeysFromCatalog = async (
  env: Pick<Env, "PC_KEIBA_R2_CATALOG">,
  yyyymmdd: string,
  context: ListTodayRaceKeysContext = {},
): Promise<TodayRaceKey[]> => {
  const catalog = context.catalog ?? env.PC_KEIBA_R2_CATALOG;
  const rows = await fetchCatalogRows(catalog, buildRaceKeysUrl(yyyymmdd));
  return rows.filter(isCompleteRow).map(toTodayRaceKey);
};

const partitionByCachedSource = (
  rows: TodayRaceKey[],
): { jra: TodayRaceKey[]; nar: TodayRaceKey[] } => ({
  jra: rows.filter((row) => row.source === "jra"),
  nar: rows.filter((row) => row.source === "nar"),
});

interface ListTodayRaceKeysWithKvCacheArgs {
  env: ScheduledRaceListEnv;
  yyyymmdd: string;
  context: ListTodayRaceKeysContext;
}

interface ListTomorrowRaceKeysWithKvCacheArgs {
  env: ScheduledRaceListEnv;
  now: Date;
  context: ListTodayRaceKeysContext;
}

const fetchAndCacheTodayRaceKeys = async (
  args: ListTodayRaceKeysWithKvCacheArgs,
): Promise<TodayRaceKey[]> => {
  const fresh = await listTodayRaceKeysFromCatalog(args.env, args.yyyymmdd, args.context);
  const partitioned = partitionByCachedSource(fresh);
  await Promise.all([
    putTodayRaceKeysToKv(args.env, "jra", args.yyyymmdd, partitioned.jra),
    putTodayRaceKeysToKv(args.env, "nar", args.yyyymmdd, partitioned.nar),
  ]);
  return fresh;
};

// Reads per-source KV entries first. A miss performs one catalog request and
// populates both entries. Failed catalog requests are not cached and retry on
// the next scheduled tick.
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
    console.error("[features] listTodayRaceKeysWithKvCache catalog failure", error);
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

export const listTomorrowRaceKeysFromCatalog = async (
  env: Pick<Env, "PC_KEIBA_R2_CATALOG">,
  now: Date,
  context: ListTodayRaceKeysContext = {},
): Promise<TodayRaceKey[]> => {
  const tomorrowJst = computeTomorrowJst(now);
  return listTodayRaceKeysFromCatalog(env, tomorrowJst, context);
};
