// Run with bun. Generation-bound markers make scheduled cache warming idempotent.

import {
  buildRaceCacheGenerationKey,
  buildRaceCacheWarmMarkerKey,
  type RaceCacheBustRequest,
  type RaceCacheWarmKind,
} from "./race-cache-bust";

const CACHE_WARM_MARKER_TTL_SECONDS = 60 * 60 * 24 * 30;
const INITIAL_GENERATION = "0";

export interface RaceCacheWarmGenerationState {
  generation: string;
  markerKey: string;
  valid: boolean;
}

interface CacheWarmKv {
  get(key: string): Promise<string | null>;
  put(key: string, value: string, options?: { expirationTtl?: number }): Promise<void>;
}

interface ReadRaceCacheWarmGenerationParams {
  kind: RaceCacheWarmKind;
  kv: CacheWarmKv | undefined;
  race: RaceCacheBustRequest;
}

interface MarkRaceCacheWarmGenerationParams extends ReadRaceCacheWarmGenerationParams {
  generation: string;
}

export const readRaceCacheWarmGeneration = async ({
  kind,
  kv,
  race,
}: ReadRaceCacheWarmGenerationParams): Promise<RaceCacheWarmGenerationState | null> => {
  if (!kv) {
    return null;
  }
  const generationKey = buildRaceCacheGenerationKey(race);
  const markerKey = buildRaceCacheWarmMarkerKey(kind, race);
  try {
    const [generationValue, markerValue] = await Promise.all([
      kv.get(generationKey),
      kv.get(markerKey),
    ]);
    const generation = generationValue ?? INITIAL_GENERATION;
    return { generation, markerKey, valid: markerValue === generation };
  } catch {
    return null;
  }
};

export const markRaceCacheWarmGeneration = async ({
  generation,
  kind,
  kv,
  race,
}: MarkRaceCacheWarmGenerationParams): Promise<boolean> => {
  if (!kv) {
    return false;
  }
  const generationKey = buildRaceCacheGenerationKey(race);
  try {
    const currentGeneration = (await kv.get(generationKey)) ?? INITIAL_GENERATION;
    if (currentGeneration !== generation) {
      return false;
    }
    await kv.put(buildRaceCacheWarmMarkerKey(kind, race), generation, {
      expirationTtl: CACHE_WARM_MARKER_TTL_SECONDS,
    });
    return true;
  } catch {
    return false;
  }
};
