// Run with bun. Pure key/path builders for the per-race cache-bust signal
// triggered by `sync-realtime-data` after a successful `fetchAndStoreResults`
// write. Kept lib-side (no server-only deps) so the helpers can be unit
// tested without instantiating Cloudflare bindings.
//
// 2026-06-28: introduced after a fetch-results outage left the detail-
// section KV serving 6+ hours stale because no upstream signal forced an
// invalidation across the main + stale KV tiers + Cache API.
// 2026-10-24: busts rotate a per-race generation instead of deleting keys.

import type { RaceSource } from "./codes";
import {
  buildDetailSectionCacheKey,
  type DetailSectionCacheableSection,
} from "./race-detail-section-cache";

// Generation key shape. Lives in `DETAIL_SECTION_CACHE_KV` next to the cache
// entries; readers append its value to their section / SSR keys, so a bust is
// one put here instead of deleting every key. It has no source segment
// because the section keys have none either (a route is one race).
const RACE_CACHE_GEN_PREFIX = "race-cache:gen:v2";
const CACHE_WARM_MARKER_PREFIX = "race-cache:warm:v1";
const RACE_DETAIL_SSR_CACHE_VERSION = "v4";
const TRAINING_SCOPE_SECTIONS: readonly DetailSectionCacheableSection[] = [
  "training",
  "premium-data-top",
];

// PC_KEIBA_INTERNAL_TOKEN-protected internal endpoint path.
export const RACE_CACHE_BUST_INTERNAL_PATH = "/api/internal/race-cache-bust";

export interface RaceCacheGenerationInput {
  keibajoCode: string;
  mmdd: string;
  raceBango: string;
  year: string;
}

export interface RaceCacheBustRequest extends RaceCacheGenerationInput {
  scope?: "training";
  source: RaceSource;
}

export interface RaceDetailSsrCacheKeyParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

export interface RaceCacheBustKeySet {
  generationKey: string;
  // Generation-less section keys a training-scope bust deletes.
  trainingSectionKeys: string[];
}

export type RaceCacheWarmKind = "race-detail-ssr" | "race-trend";

const YYYY_PATTERN = /^\d{4}$/u;
const MMDD_PATTERN = /^\d{4}$/u;
const KEIBAJO_PATTERN = /^[0-9A-Z]{2}$/u;
const RACE_BANGO_PATTERN = /^\d{2}$/u;

const isRaceSource = (value: unknown): value is RaceSource => value === "jra" || value === "nar";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

export const parseRaceCacheBustRequest = (value: unknown): RaceCacheBustRequest | null => {
  if (!isRecord(value)) return null;
  if (!isRaceSource(value.source)) return null;
  if (value.scope !== undefined && value.scope !== "training") return null;
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
    ...(value.scope === "training" ? { scope: value.scope } : {}),
    source: value.source,
    year: value.year,
  };
};

const buildSectionMainKey = (
  request: RaceCacheGenerationInput,
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

export const buildRaceCacheGenerationKey = (request: RaceCacheGenerationInput): string =>
  [RACE_CACHE_GEN_PREFIX, request.year, request.mmdd, request.keibajoCode, request.raceBango].join(
    ":",
  );

export const buildRaceDetailSsrCacheKey = (params: RaceDetailSsrCacheKeyParams): string =>
  [
    "race-detail-ssr",
    RACE_DETAIL_SSR_CACHE_VERSION,
    params.source,
    params.year,
    params.month,
    params.day,
    params.keibajoCode,
    params.raceNumber,
  ].join(":");

export const buildRaceCacheWarmMarkerKey = (
  kind: RaceCacheWarmKind,
  request: RaceCacheGenerationInput,
): string => `${CACHE_WARM_MARKER_PREFIX}:${kind}:${buildRaceCacheGenerationKey(request)}`;

export const buildRaceCacheBustKeys = (request: RaceCacheBustRequest): RaceCacheBustKeySet => ({
  generationKey: buildRaceCacheGenerationKey(request),
  trainingSectionKeys: TRAINING_SCOPE_SECTIONS.map((section) =>
    buildSectionMainKey(request, section),
  ),
});

// Race-key helper used by sync-realtime-data when it only has the raceKey
// shape (e.g. `nar:20260628:50:07`). The regex enforces alternative
// `(jra|nar)` and non-optional fixed-width capture groups so each `match[N]!`
// is statically guaranteed to be defined.
const RACE_KEY_PATTERN = /^(jra|nar):(\d{4})(\d{4}):([0-9A-Z]{2}):(\d{2})$/u;

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
