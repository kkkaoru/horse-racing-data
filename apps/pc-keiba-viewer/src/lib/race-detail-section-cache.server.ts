import "server-only";
import { safeGetCloudflareRuntime } from "./cloudflare-context.server";
import {
  DETAIL_SECTION_CACHE_AFTER_START_SECONDS,
  buildDetailSectionCacheKey,
  expandDetailSectionCacheReadKeys,
  type DetailSectionCacheableSection,
  type DetailSectionCacheWarmMessage,
} from "./race-detail-section-cache";
import {
  STALE_DETAIL_SECTION_MAX_AGE_MS,
  getJstMidnightMsForToday,
  parseStaleDetailSectionEnvelope,
  serializeStaleDetailSectionEnvelope,
} from "./race-detail-section-stale";
import type { RaceDetail } from "./race-types";

const CACHE_CONTROL_HEADER = "public, max-age=%d";
// v2 invalidates Cache API entries created before race cache busts could reliably
// replace pre-race training data. KV keys remain stable, so existing valid payloads
// can repopulate the new Cache API namespace without a full cache rebuild.
const CACHE_URL_BASE = "https://pc-keiba-viewer.local/detail-section-cache/v2/";
const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";

// Stale snapshots persist much longer than the fresh tier (which expires
// race start + 6h). Past races are immutable, and for upcoming races we
// only need stale to last long enough that the next visitor can serve it
// while the background refresh runs.
// NOTE: a separate read-time max-stale cap (4h + same-JST-day window) is
// enforced in `getStaleDetailSectionBody` so a long-lived KV entry cannot
// keep serving a payload that is hours past the most recent D1 write.
const STALE_CACHE_KEY_PREFIX = "stale";
const STALE_TTL_SECONDS = 30 * 24 * 60 * 60;

// 2026-07-04: the 21:00-JST-day-before pre-warm computes the "training"
// section before netkeiba's premium training reviews / stable comments have
// been scraped, so the payload it caches has zero premium content. Nothing
// busts that cache once the premium fetch eventually lands, so the empty
// section used to persist for the full `race start + 6h` TTL. Detecting
// emptiness at write time and using a much shorter TTL lets the section
// self-heal: the entry expires quickly, the next request recomputes it, and
// once the premium fetch has landed the recompute is cached with the normal
// long TTL again.

type CacheSource = "cache-api" | "kv";

const getCacheRequest = (cacheKey: string): Request =>
  new Request(`${CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const getConfiguredAfterStartSeconds = (env: CloudflareEnv | null): number => {
  const parsed = Number(env?.PC_KEIBA_DETAIL_SECTION_CACHE_AFTER_START_SECONDS);
  return Number.isFinite(parsed) && parsed >= 60
    ? Math.floor(parsed)
    : DETAIL_SECTION_CACHE_AFTER_START_SECONDS;
};

const getRaceStartTimeMs = (race: RaceDetail): number | null => {
  const normalizedTime = race.hassoJikoku?.trim().padStart(4, "0");
  if (!normalizedTime || !/^\d{4}$/u.test(normalizedTime)) {
    return null;
  }
  const startTime = Date.parse(
    `${race.kaisaiNen}-${race.kaisaiTsukihi.slice(0, 2)}-${race.kaisaiTsukihi.slice(
      2,
      4,
    )}T${normalizedTime.slice(0, 2)}:${normalizedTime.slice(2, 4)}:00+09:00`,
  );
  return Number.isFinite(startTime) ? startTime : null;
};

const getRaceDayFallbackBaseTimeMs = (race: RaceDetail): number =>
  Date.parse(
    `${race.kaisaiNen}-${race.kaisaiTsukihi.slice(0, 2)}-${race.kaisaiTsukihi.slice(
      2,
      4,
    )}T23:59:59+09:00`,
  );

export const getDetailSectionCacheTtlSeconds = (
  race: RaceDetail,
  env: CloudflareEnv | null,
  nowMs = Date.now(),
): number => {
  const afterStartSeconds = getConfiguredAfterStartSeconds(env);
  const raceStartTime = getRaceStartTimeMs(race);
  const expiresAt =
    (raceStartTime ?? getRaceDayFallbackBaseTimeMs(race)) + afterStartSeconds * 1000;
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

const buildCachedResponse = (body: string, source: CacheSource): Response =>
  new Response(body, {
    headers: {
      "Cache-Control": "public, max-age=60",
      "Content-Type": DEFAULT_CONTENT_TYPE,
      "X-Detail-Section-Cache": `HIT-${source}`,
    },
  });

const readCachedDetailSectionForKey = async (
  cacheKey: string,
  populateCurrentCacheApi: boolean,
): Promise<Response | null> => {
  const defaultCache = getDefaultCache();
  const cacheRequest = getCacheRequest(cacheKey);
  const cachedResponse = await defaultCache?.match(cacheRequest);
  if (cachedResponse?.ok) {
    const cachedBody = await cachedResponse.text();
    if (!isEmptyDataSectionBody(cachedBody)) {
      return buildCachedResponse(cachedBody, "cache-api");
    }
  }

  const { env, ctx } = await safeGetCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!kvBody || isEmptyDataSectionBody(kvBody)) {
    return null;
  }

  const putCache = async () => {
    await defaultCache?.put(
      cacheRequest,
      new Response(kvBody, {
        headers: {
          "Cache-Control": "public, max-age=60",
          "Content-Type": DEFAULT_CONTENT_TYPE,
        },
      }),
    );
  };
  if (populateCurrentCacheApi) {
    ctx?.waitUntil(putCache());
  }
  return buildCachedResponse(kvBody, "kv");
};

export const getCachedDetailSectionResponse = async (
  cacheKey: string,
): Promise<Response | null> => {
  const readKeys = expandDetailSectionCacheReadKeys(cacheKey);
  const firstKey = readKeys[0];
  if (firstKey === undefined) {
    return null;
  }
  const currentHit = await readCachedDetailSectionForKey(firstKey, true);
  if (currentHit !== null) {
    return currentHit;
  }
  const fallbackKey = readKeys[1];
  return fallbackKey === undefined ? null : readCachedDetailSectionForKey(fallbackKey, false);
};

const getStaleCacheKey = (cacheKey: string): string => `${STALE_CACHE_KEY_PREFIX}:${cacheKey}`;

// Read-time freshness gate.  Cached envelope is rejected when:
//   1. Body is not a `{ payload, writtenAt }` envelope (legacy raw entry =
//      treated as expired — caller will recompute).
//   2. writtenAt is more than 4h old (max-stale cap).
//   3. writtenAt is older than the most recent JST midnight (so yesterday's
//      payload never wins for today's races).
const isEnvelopeStillFresh = (writtenAt: number, nowMs: number): boolean => {
  if (nowMs - writtenAt >= STALE_DETAIL_SECTION_MAX_AGE_MS) {
    return false;
  }
  return writtenAt >= getJstMidnightMsForToday(nowMs);
};

const readStaleDetailSectionBodyForKey = async (
  cacheKey: string,
  nowMs: number,
): Promise<string | null> => {
  const { env } = await safeGetCloudflareRuntime();
  const raw = await env?.DETAIL_SECTION_CACHE_KV?.get(getStaleCacheKey(cacheKey)).catch(() => null);
  if (raw === null || raw === undefined) {
    return null;
  }
  const envelope = parseStaleDetailSectionEnvelope(raw);
  if (envelope === null) {
    return null;
  }
  if (!isEnvelopeStillFresh(envelope.writtenAt, nowMs)) {
    return null;
  }
  return isEmptyDataSectionBody(envelope.payload) ? null : envelope.payload;
};

export const getStaleDetailSectionBody = async (
  cacheKey: string,
  nowMs = Date.now(),
): Promise<string | null> => {
  const readKeys = expandDetailSectionCacheReadKeys(cacheKey);
  const firstKey = readKeys[0];
  if (firstKey === undefined) {
    return null;
  }
  const currentHit = await readStaleDetailSectionBodyForKey(firstKey, nowMs);
  if (currentHit !== null) {
    return currentHit;
  }
  const fallbackKey = readKeys[1];
  return fallbackKey === undefined ? null : readStaleDetailSectionBodyForKey(fallbackKey, nowMs);
};

export const buildStaleDetailSectionResponse = (body: string): Response =>
  new Response(body, {
    headers: {
      "Cache-Control": "public, max-age=60",
      "Content-Type": DEFAULT_CONTENT_TYPE,
      "X-Detail-Section-Cache": "STALE-kv",
    },
  });

const hasTrainingContent = (training: unknown): boolean =>
  typeof training === "object" &&
  training !== null &&
  (("chokyoNengappi" in training &&
    typeof training.chokyoNengappi === "string" &&
    training.chokyoNengappi.length === 8) ||
    ("premiumCommentText" in training && Boolean(training.premiumCommentText)) ||
    ("premiumEvaluationGrade" in training && Boolean(training.premiumEvaluationGrade)) ||
    ("premiumEvaluationText" in training && Boolean(training.premiumEvaluationText)));

// Mirrors the shape written by the "training" `getDetailSectionPayload`
// branch: `{ type: "training", stableComments: [...], trainings: [...] }`,
// where each `training` row only carries `premiumCommentText` /
// `premiumEvaluationGrade` / `premiumEvaluationText` once a premium review
// was merged onto it.
const isEmptyPremiumTrainingSectionBody = (body: string): boolean => {
  try {
    const parsed: unknown = JSON.parse(body);
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      !("type" in parsed) ||
      parsed.type !== "training" ||
      !("stableComments" in parsed) ||
      !Array.isArray(parsed.stableComments) ||
      !("trainings" in parsed) ||
      !Array.isArray(parsed.trainings)
    ) {
      return false;
    }
    return parsed.stableComments.length === 0 && !parsed.trainings.some(hasTrainingContent);
  } catch {
    return false;
  }
};

// Defense-in-depth mirror of the "premium-data-top" emptiness check the
// `[section]` route already applies before calling `putDetailSectionCache`
// (so a route-level regression there would still self-heal via this cache
// layer instead of caching an empty result for the full long TTL).
const isEmptyPremiumDataTopSectionBody = (body: string): boolean => {
  try {
    const parsed: unknown = JSON.parse(body);
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      !("type" in parsed) ||
      parsed.type !== "premium-data-top" ||
      !("dataTopHorses" in parsed) ||
      !Array.isArray(parsed.dataTopHorses)
    ) {
      return false;
    }
    return parsed.dataTopHorses.length === 0;
  } catch {
    return false;
  }
};

const isEmptyDataSectionBody = (body: string): boolean =>
  isEmptyPremiumTrainingSectionBody(body) || isEmptyPremiumDataTopSectionBody(body);

export const putDetailSectionCache = async ({
  body,
  cacheKey,
  race,
}: {
  body: string;
  cacheKey: string;
  race: RaceDetail;
}): Promise<void> => {
  const { env } = await safeGetCloudflareRuntime();
  if (isEmptyDataSectionBody(body)) return;
  const ttlSeconds = getDetailSectionCacheTtlSeconds(race, env);
  const cacheControl = CACHE_CONTROL_HEADER.replace("%d", String(ttlSeconds));
  // The 30-day stale snapshot is written even when fresh TTL is already
  // 0 (the race finished more than 6h ago) so future visits still get an
  // instant render via the SWR path.  Stored as `{ payload, writtenAt }`
  // so the read path can enforce a max-stale cap + JST-midnight boundary
  // without depending on KV's `expirationTtl` for freshness.
  const staleEnvelope = serializeStaleDetailSectionEnvelope(body, Date.now());
  const stalePut = env?.DETAIL_SECTION_CACHE_KV?.put(getStaleCacheKey(cacheKey), staleEnvelope, {
    expirationTtl: STALE_TTL_SECONDS,
  }).catch(() => undefined);
  if (ttlSeconds <= 0) {
    await stalePut;
    return;
  }
  await Promise.all([
    getDefaultCache()
      ?.put(
        getCacheRequest(cacheKey),
        new Response(body, {
          headers: {
            "Cache-Control": cacheControl,
            "Content-Type": DEFAULT_CONTENT_TYPE,
          },
        }),
      )
      .catch(() => undefined),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, { expirationTtl: ttlSeconds }).catch(
      () => undefined,
    ),
    stalePut,
  ]);
};

export const buildDetailSectionCacheKeyForMessage = (
  message: Omit<DetailSectionCacheWarmMessage, "section" | "source"> & {
    section: DetailSectionCacheableSection;
  },
): string => buildDetailSectionCacheKey(message);
