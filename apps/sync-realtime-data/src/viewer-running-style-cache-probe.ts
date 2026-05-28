// Run with bun. Probe whether viewer-compatible running-style caches exist.

import { readD1QueryCache } from "./d1-query-cache";
import {
  buildRunningStyleCacheRequest,
  getRunningStyleCacheTtlSeconds,
} from "./running-style-cache";
import type { RunningStyleInferenceRace } from "./running-style-d1";
import type { Env } from "./types";

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

export const isViewerRunningStyleRaceCacheReady = async (
  env: Env,
  race: RunningStyleInferenceRace,
): Promise<boolean> => {
  const raceKey = race.raceKey;
  const raceDay = {
    kaisaiNen: race.kaisaiNen,
    kaisaiTsukihi: race.kaisaiTsukihi,
  };
  const hashCached = await readD1QueryCache<unknown[]>(
    "running-style-race",
    ["getRaceRunningStylesFromD1", raceKey],
    { raceDay },
  );
  if (hashCached !== null && hashCached.length > 0) {
    return true;
  }
  const cache = getDefaultCache();
  if (cache === null) {
    return false;
  }
  const ttlSeconds = getRunningStyleCacheTtlSeconds(race);
  if (ttlSeconds <= 0) {
    return false;
  }
  const cacheRequest = buildRunningStyleCacheRequest(env, race);
  const cachedResponse = await cache.match(cacheRequest);
  return cachedResponse?.ok === true;
};
