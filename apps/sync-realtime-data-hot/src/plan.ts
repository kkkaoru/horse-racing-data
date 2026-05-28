import {
  acquireEnqueueLock,
  calculateEnqueueLockTtlSeconds,
  isEnqueueLocked,
} from "./gates/enqueue-lock-kv";
import { shouldRunOddsCron } from "./gates/polling-window-gate";
import { getRaceListFromKv, putRaceListToKv } from "./gates/race-list-kv-cache";
import { listOddsFetchStateForDate } from "./storage";
import type { Env, OddsSource, RaceListEntry } from "./types";

export interface PlanOddsFetchesResult {
  queued: number;
  skipped: number;
}

const ODDS_SOURCES: OddsSource[] = ["jra", "nar"];

const loadRaceList = async (
  env: Env,
  source: OddsSource,
  yyyymmdd: string,
): Promise<RaceListEntry[]> => {
  const cached = await getRaceListFromKv(env, source, yyyymmdd);
  if (cached) {
    return cached;
  }
  const fresh = await listOddsFetchStateForDate(
    env.REALTIME_HOT_DB,
    source,
    yyyymmdd.slice(0, 4),
    yyyymmdd.slice(4),
  );
  await putRaceListToKv(env, source, yyyymmdd, fresh);
  return fresh;
};

const planRacesForSource = async (
  env: Env,
  source: OddsSource,
  yyyymmdd: string,
  now: Date,
): Promise<PlanOddsFetchesResult> => {
  const list = await loadRaceList(env, source, yyyymmdd);
  let queued = 0;
  let skipped = 0;
  for (const entry of list) {
    const ttl = calculateEnqueueLockTtlSeconds(new Date(entry.raceStartAtJst), now);
    if (ttl > 0 && (await isEnqueueLocked(env, entry.raceKey))) {
      skipped += 1;
      continue;
    }
    await env.REALTIME_HOT_JOBS.send({ raceKey: entry.raceKey, type: "fetch-odds" });
    await acquireEnqueueLock(env, entry.raceKey, ttl);
    queued += 1;
  }
  return { queued, skipped };
};

export const planOddsFetches = async (
  env: Env,
  now: Date,
  yyyymmdd: string,
): Promise<PlanOddsFetchesResult> => {
  if (!shouldRunOddsCron(now)) {
    return { queued: 0, skipped: 0 };
  }
  const results = await Promise.all(
    ODDS_SOURCES.map((source) => planRacesForSource(env, source, yyyymmdd, now)),
  );
  return results.reduce<PlanOddsFetchesResult>(
    (accumulator, current) => ({
      queued: accumulator.queued + current.queued,
      skipped: accumulator.skipped + current.skipped,
    }),
    { queued: 0, skipped: 0 },
  );
};
