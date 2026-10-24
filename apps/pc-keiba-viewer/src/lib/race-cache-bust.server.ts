// Run with bun. Server-only wrapper around `race-cache-bust.ts`: rotates the
// per-race cache generation triggered by sync-realtime-data after
// `fetchAndStoreResults` lands, and resolves generation-qualified cache keys
// for readers and writers.
//
// The generation is the bust time in ms, written without reading the old
// value: a failed read can never roll it back onto keys that still hold
// pre-bust payloads, and every bust yields a value no reader has seen.

import "server-only";
import { cache } from "react";

import { safeGetCloudflareRuntime } from "./cloudflare-context.server";
import {
  buildRaceCacheBustKeys,
  buildRaceCacheGenerationKey,
  type RaceCacheBustKeySet,
  type RaceCacheBustRequest,
  type RaceCacheGenerationInput,
} from "./race-cache-bust";
import { INITIAL_CACHE_GENERATION, withCacheGeneration } from "./race-detail-section-cache";

const GENERATION_TTL_SECONDS = 60 * 60 * 24 * 30;
const LEGACY_STALE_KEY_PREFIX = "stale";

export interface BustRaceCacheResult {
  busted: number;
  generation: number;
}

interface TrainingBustParams {
  keys: RaceCacheBustKeySet;
  kv: PcKeibaKvNamespace;
}

const bumpGeneration = async (
  kv: PcKeibaKvNamespace,
  generationKey: string,
): Promise<BustRaceCacheResult> => {
  const generation = Date.now();
  const busted = await kv
    .put(generationKey, String(generation), { expirationTtl: GENERATION_TTL_SECONDS })
    .then(
      () => 1,
      () => 0,
    );
  return { busted, generation };
};

const deleteEvery = async (kv: PcKeibaKvNamespace, keys: string[]): Promise<number> => {
  const outcomes = await Promise.all(
    keys.map((key) =>
      kv
        .delete(key)
        .then(() => true)
        .catch(() => false),
    ),
  );
  return outcomes.filter(Boolean).length;
};

// Generation "0" readers still fall back to the legacy keys, so those go too.
const collectTrainingDeleteKeys = (baseKey: string, generation: string): string[] =>
  generation === INITIAL_CACHE_GENERATION
    ? [withCacheGeneration(baseKey, generation), baseKey, `${LEGACY_STALE_KEY_PREFIX}:${baseKey}`]
    : [withCacheGeneration(baseKey, generation)];

// Training busts touch only the training sections, so they delete those keys
// of the current generation instead of rotating every section of the race.
const bustTrainingSections = async ({
  keys,
  kv,
}: TrainingBustParams): Promise<BustRaceCacheResult> => {
  const generation = await kv.get(keys.generationKey).then(
    (value) => value ?? INITIAL_CACHE_GENERATION,
    () => null,
  );
  // Without the current generation the targeted keys are unknown; rotate it.
  if (generation === null) {
    return bumpGeneration(kv, keys.generationKey);
  }
  const busted = await deleteEvery(
    kv,
    keys.trainingSectionKeys.flatMap((baseKey) => collectTrainingDeleteKeys(baseKey, generation)),
  );
  return { busted, generation: Number(generation) };
};

export const bustRaceCachesForRace = async (
  request: RaceCacheBustRequest,
): Promise<BustRaceCacheResult> => {
  const { env } = await safeGetCloudflareRuntime();
  const kv = env?.DETAIL_SECTION_CACHE_KV;
  if (!kv) {
    return { busted: 0, generation: 0 };
  }
  const keys = buildRaceCacheBustKeys(request);
  return request.scope === "training"
    ? bustTrainingSections({ keys, kv })
    : bumpGeneration(kv, keys.generationKey);
};

// Memoized per server-component render; route handlers (no React cache scope)
// read once per resolve call.
const readRaceCacheGeneration = cache(async (generationKey: string): Promise<string> => {
  const { env } = await safeGetCloudflareRuntime();
  return (await env?.DETAIL_SECTION_CACHE_KV?.get(generationKey)) ?? INITIAL_CACHE_GENERATION;
});

// Null when the generation cannot be read: callers then bypass the cache
// entirely, because any guessed generation could serve a pre-bust payload.
export const resolveRaceCacheKey = async (
  baseKey: string,
  race: RaceCacheGenerationInput,
): Promise<string | null> =>
  readRaceCacheGeneration(buildRaceCacheGenerationKey(race)).then(
    (generation) => withCacheGeneration(baseKey, generation),
    () => null,
  );
