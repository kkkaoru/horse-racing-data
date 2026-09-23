// Run with bun (vitest) / Cloudflare Workers runtime.
// De-duplicates race-trend cache warm enqueues across the */5 scheduler runs.
//
// The scheduler re-enqueued every due variant every 5 minutes until the race's
// trend generation turned valid, so a lagging consumer accumulated duplicate
// messages. A short-lived KV marker per (race, generation, variant) makes each
// run enqueue a message only once per marker window.

import type { RaceTrendCacheWarmMessage } from "./race-trend-cache";

export interface RaceTrendEnqueueMarkerKv {
  get(key: string): Promise<string | null>;
  put(key: string, value: string, options: { expirationTtl: number }): Promise<void>;
}

export interface RaceTrendEnqueueCandidate {
  delaySeconds: number;
  message: RaceTrendCacheWarmMessage;
}

export interface RaceTrendEnqueueMarkerParams {
  candidates: readonly RaceTrendEnqueueCandidate[];
  kv: RaceTrendEnqueueMarkerKv | undefined;
}

const MARKER_PREFIX = "race-trend-warm-enqueued:v1";
// Three scheduler periods: a message still queued after this is re-enqueued
// once more, which bounds duplicates instead of growing them every 5 minutes.
export const RACE_TREND_ENQUEUE_MARKER_TTL_SECONDS = 15 * 60;

export const buildRaceTrendEnqueueMarkerKey = (message: RaceTrendCacheWarmMessage): string =>
  [
    MARKER_PREFIX,
    message.source,
    `${message.year}${message.month}${message.day}`,
    message.keibajoCode,
    message.raceNumber,
    message.cacheGeneration,
    JSON.stringify(message.options),
  ].join(":");

export const filterUnmarkedRaceTrendCandidates = async ({
  candidates,
  kv,
}: RaceTrendEnqueueMarkerParams): Promise<RaceTrendEnqueueCandidate[]> => {
  if (kv === undefined) return [...candidates];
  const marked = await Promise.all(
    candidates.map(
      async (candidate) =>
        (await kv.get(buildRaceTrendEnqueueMarkerKey(candidate.message))) !== null,
    ),
  );
  return candidates.filter((_candidate, index) => !marked[index]);
};

export const markRaceTrendCandidatesEnqueued = async ({
  candidates,
  kv,
}: RaceTrendEnqueueMarkerParams): Promise<void> => {
  if (kv === undefined) return;
  await Promise.all(
    candidates.map((candidate) =>
      kv.put(buildRaceTrendEnqueueMarkerKey(candidate.message), "1", {
        expirationTtl: RACE_TREND_ENQUEUE_MARKER_TTL_SECONDS,
      }),
    ),
  );
};
