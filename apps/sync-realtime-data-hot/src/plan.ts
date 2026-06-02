import {
  acquireEnqueueLock,
  calculateEnqueueLockTtlSeconds,
  isEnqueueLocked,
} from "./gates/enqueue-lock-kv";
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

interface TryEnqueueRaceArgs {
  env: Env;
  entry: RaceListEntry;
  now: Date;
}

interface RaceEnqueueOutcome {
  enqueued: boolean;
}

const tryEnqueueRace = async ({
  env,
  entry,
  now,
}: TryEnqueueRaceArgs): Promise<RaceEnqueueOutcome> => {
  const ttl = calculateEnqueueLockTtlSeconds(new Date(entry.raceStartAtJst), now);
  // ttl === 0 represents "past race / out of window" — skip enqueue entirely.
  if (ttl === 0) {
    return { enqueued: false };
  }
  if (await isEnqueueLocked(env, entry.raceKey)) {
    return { enqueued: false };
  }
  await env.REALTIME_HOT_JOBS.send({ raceKey: entry.raceKey, type: "fetch-odds" });
  await acquireEnqueueLock(env, entry.raceKey, ttl);
  return { enqueued: true };
};

const planRacesForSource = async (
  env: Env,
  source: OddsSource,
  yyyymmdd: string,
  now: Date,
): Promise<PlanOddsFetchesResult> => {
  const list = await loadRaceList(env, source, yyyymmdd);
  const outcomes = await Promise.all(list.map((entry) => tryEnqueueRace({ entry, env, now })));
  return outcomes.reduce<PlanOddsFetchesResult>(
    (accumulator, current) =>
      current.enqueued
        ? { queued: accumulator.queued + 1, skipped: accumulator.skipped }
        : { queued: accumulator.queued, skipped: accumulator.skipped + 1 },
    { queued: 0, skipped: 0 },
  );
};

export const planOddsFetches = async (
  env: Env,
  now: Date,
  yyyymmdd: string,
): Promise<PlanOddsFetchesResult> => {
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
