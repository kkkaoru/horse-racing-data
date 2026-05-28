import type { Env, OddsSource, RaceListEntry } from "../types";

const RACE_LIST_KV_KEY_PREFIX = "odds:race-list:v1";
const DEFAULT_RACE_LIST_KV_TTL_SECONDS = 21_600;

const buildRaceListKey = (source: OddsSource, yyyymmdd: string): string =>
  `${RACE_LIST_KV_KEY_PREFIX}:${source}:${yyyymmdd}`;

const resolveTtlSeconds = (env: Env): number => {
  const raw = env.ODDS_RACE_LIST_KV_TTL_SECONDS;
  if (!raw) {
    return DEFAULT_RACE_LIST_KV_TTL_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_RACE_LIST_KV_TTL_SECONDS;
};

export const getRaceListFromKv = async (
  env: Env,
  source: OddsSource,
  yyyymmdd: string,
): Promise<RaceListEntry[] | null> => {
  const json = await env.ODDS_HOT_KV.get(buildRaceListKey(source, yyyymmdd));
  return json ? (JSON.parse(json) as RaceListEntry[]) : null;
};

export const putRaceListToKv = async (
  env: Env,
  source: OddsSource,
  yyyymmdd: string,
  list: RaceListEntry[],
): Promise<void> => {
  await env.ODDS_HOT_KV.put(buildRaceListKey(source, yyyymmdd), JSON.stringify(list), {
    expirationTtl: resolveTtlSeconds(env),
  });
};

export const invalidateRaceListInKv = async (
  env: Env,
  source: OddsSource,
  yyyymmdd: string,
): Promise<void> => {
  await env.ODDS_HOT_KV.delete(buildRaceListKey(source, yyyymmdd));
};

export const patchLastFetchInKv = async (
  env: Env,
  source: OddsSource,
  yyyymmdd: string,
  raceKey: string,
  fetchedAt: string,
): Promise<boolean> => {
  const list = await getRaceListFromKv(env, source, yyyymmdd);
  if (!list) {
    return false;
  }
  const target = list.find((entry) => entry.raceKey === raceKey);
  if (!target) {
    return false;
  }
  target.lastOddsFetchAt = fetchedAt;
  await putRaceListToKv(env, source, yyyymmdd, list);
  return true;
};
