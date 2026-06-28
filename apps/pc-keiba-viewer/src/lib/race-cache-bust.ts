// Run with bun. Pure key/path builders for the per-race cache-bust signal
// triggered by `sync-realtime-data` after a successful `fetchAndStoreResults`
// write. Kept lib-side (no server-only deps) so the helpers can be unit
// tested without instantiating Cloudflare bindings.
//
// 2026-06-28: introduced after a fetch-results outage left the detail-
// section KV serving 6+ hours stale because no upstream signal forced an
// invalidation across the main + stale KV tiers + Cache API.

import type { RaceSource } from "./codes";
import {
  DETAIL_SECTION_CACHEABLE_SECTIONS,
  buildDetailSectionCacheKey,
  type DetailSectionCacheableSection,
} from "./race-detail-section-cache";

// Generation counter key shape. Lives in `DETAIL_SECTION_CACHE_KV` next to
// the actual cache entries so cache-warm / read paths can fold it into the
// Cache-API request URL and defeat the per-edge Cache API tier without
// purging it explicitly (Cloudflare Cache API does not expose a global
// purge from inside a Worker).
const RACE_CACHE_GEN_PREFIX = "race-cache:gen";
const STALE_KEY_PREFIX = "stale";

// PC_KEIBA_INTERNAL_TOKEN-protected internal endpoint path.
export const RACE_CACHE_BUST_INTERNAL_PATH = "/api/internal/race-cache-bust";

export interface RaceCacheBustRequest {
  keibajoCode: string;
  mmdd: string;
  raceBango: string;
  source: RaceSource;
  year: string;
}

export interface RaceCacheBustKeySet {
  generationKey: string;
  mainKeys: string[];
  staleKeys: string[];
}

const YYYY_PATTERN = /^\d{4}$/u;
const MMDD_PATTERN = /^\d{4}$/u;
const KEIBAJO_PATTERN = /^\d{2}$/u;
const RACE_BANGO_PATTERN = /^\d{2}$/u;

const isRaceSource = (value: unknown): value is RaceSource => value === "jra" || value === "nar";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

export const parseRaceCacheBustRequest = (value: unknown): RaceCacheBustRequest | null => {
  if (!isRecord(value)) return null;
  if (!isRaceSource(value.source)) return null;
  if (typeof value.year !== "string" || !YYYY_PATTERN.test(value.year)) return null;
  if (typeof value.mmdd !== "string" || !MMDD_PATTERN.test(value.mmdd)) return null;
  if (typeof value.keibajoCode !== "string" || !KEIBAJO_PATTERN.test(value.keibajoCode)) {
    return null;
  }
  if (typeof value.raceBango !== "string" || !RACE_BANGO_PATTERN.test(value.raceBango)) {
    return null;
  }
  return {
    keibajoCode: value.keibajoCode,
    mmdd: value.mmdd,
    raceBango: value.raceBango,
    source: value.source,
    year: value.year,
  };
};

const buildSectionMainKey = (
  request: RaceCacheBustRequest,
  section: DetailSectionCacheableSection,
): string =>
  buildDetailSectionCacheKey({
    day: request.mmdd.slice(2, 4),
    keibajoCode: request.keibajoCode,
    month: request.mmdd.slice(0, 2),
    raceNumber: request.raceBango,
    section,
    year: request.year,
  });

export const buildRaceCacheGenerationKey = (request: RaceCacheBustRequest): string =>
  [
    RACE_CACHE_GEN_PREFIX,
    request.source,
    request.year,
    request.mmdd,
    request.keibajoCode,
    request.raceBango,
  ].join(":");

export const buildRaceCacheBustKeys = (request: RaceCacheBustRequest): RaceCacheBustKeySet => {
  const mainKeys = DETAIL_SECTION_CACHEABLE_SECTIONS.map((section) =>
    buildSectionMainKey(request, section),
  );
  const staleKeys = mainKeys.map((key) => `${STALE_KEY_PREFIX}:${key}`);
  return {
    generationKey: buildRaceCacheGenerationKey(request),
    mainKeys,
    staleKeys,
  };
};

// Race-key helper used by sync-realtime-data when it only has the raceKey
// shape (e.g. `nar:20260628:50:07`). The regex enforces alternative
// `(jra|nar)` and non-optional fixed-width capture groups so each `match[N]!`
// is statically guaranteed to be defined.
const RACE_KEY_PATTERN = /^(jra|nar):(\d{4})(\d{4}):(\d{2}):(\d{2})$/u;

export const parseRaceKey = (raceKey: string): RaceCacheBustRequest | null => {
  const match = RACE_KEY_PATTERN.exec(raceKey);
  if (!match) return null;
  const sourceMatch = match[1]!;
  const source: RaceSource = sourceMatch === "jra" ? "jra" : "nar";
  return {
    keibajoCode: match[4]!,
    mmdd: match[3]!,
    raceBango: match[5]!,
    source,
    year: match[2]!,
  };
};
