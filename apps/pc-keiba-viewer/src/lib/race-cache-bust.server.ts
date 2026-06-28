// Run with bun. Server-only wrapper around `race-cache-bust.ts` that
// performs the per-race KV invalidation + generation bump triggered by
// sync-realtime-data after `fetchAndStoreResults` lands.
//
// The generation key is bumped (atomic-ish via read-then-write — the
// invariant is "after a bust the value is strictly greater than what any
// in-flight reader saw", monotone is sufficient even under tiny races) so
// downstream readers that fold the value into their Cache API request URL
// see a brand-new cache key and bypass the per-edge Cache API tier.

import "server-only";
import { safeGetCloudflareRuntime } from "./cloudflare-context.server";
import {
  buildRaceCacheBustKeys,
  type RaceCacheBustKeySet,
  type RaceCacheBustRequest,
} from "./race-cache-bust";

const GENERATION_TTL_SECONDS = 60 * 60 * 24 * 30;
const INITIAL_GENERATION = 1;

export interface BustRaceCacheResult {
  busted: number;
  generation: number;
}

const parseGeneration = (raw: string | null | undefined): number => {
  if (raw === null || raw === undefined) return 0;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
};

const bumpGeneration = async (kv: PcKeibaKvNamespace, generationKey: string): Promise<number> => {
  const current = await kv.get(generationKey).catch(() => null);
  const next = parseGeneration(current) + INITIAL_GENERATION;
  await kv
    .put(generationKey, String(next), { expirationTtl: GENERATION_TTL_SECONDS })
    .catch(() => undefined);
  return next;
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

const collectAllKeys = (keys: RaceCacheBustKeySet): string[] => [
  ...keys.mainKeys,
  ...keys.staleKeys,
];

export const bustRaceCachesForRace = async (
  request: RaceCacheBustRequest,
): Promise<BustRaceCacheResult> => {
  const { env } = await safeGetCloudflareRuntime();
  const kv = env?.DETAIL_SECTION_CACHE_KV;
  if (!kv) {
    return { busted: 0, generation: 0 };
  }
  const keys = buildRaceCacheBustKeys(request);
  const [busted, generation] = await Promise.all([
    deleteEvery(kv, collectAllKeys(keys)),
    bumpGeneration(kv, keys.generationKey),
  ]);
  return { busted, generation };
};
