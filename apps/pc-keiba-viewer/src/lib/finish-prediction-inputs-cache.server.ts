import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import type { FinishPredictionBuildInputs } from "./finish-position-prediction";
import type { FinishPredictionEvaluationMetrics } from "./finish-position-prediction-evaluation";
import type { RaceDetail } from "./race-types";
import { DETAIL_SECTION_CACHE_AFTER_START_SECONDS } from "./race-detail-section-cache";

const CACHE_NAMESPACE = "pc-keiba-viewer:finish-prediction-inputs:v2";
const CACHE_URL_BASE = "https://pc-keiba-viewer.local/finish-prediction-inputs-cache/";
const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";

export interface FinishPredictionStaticPayload {
  evaluation: FinishPredictionEvaluationMetrics;
  inputs: FinishPredictionBuildInputs;
}

const getCloudflareRuntime = async (): Promise<{
  ctx: PcKeibaExecutionContext | null;
  env: CloudflareEnv | null;
}> => {
  try {
    const context = await getCloudflareContext<Record<string, unknown>, PcKeibaExecutionContext>({
      async: true,
    });
    return { ctx: context.ctx, env: context.env };
  } catch {
    return { ctx: null, env: null };
  }
};

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

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

export const getFinishPredictionInputsCacheTtlSeconds = (
  race: RaceDetail,
  env: CloudflareEnv | null,
  nowMs = Date.now(),
): number => {
  const parsed = Number(env?.PC_KEIBA_DETAIL_SECTION_CACHE_AFTER_START_SECONDS);
  const afterStartSeconds =
    Number.isFinite(parsed) && parsed >= 60
      ? Math.floor(parsed)
      : DETAIL_SECTION_CACHE_AFTER_START_SECONDS;
  const raceStartTime = getRaceStartTimeMs(race);
  const expiresAt =
    (raceStartTime ?? getRaceDayFallbackBaseTimeMs(race)) + afterStartSeconds * 1000;
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

export const buildFinishPredictionInputsCacheKey = ({
  day,
  keibajoCode,
  month,
  raceNumber,
  year,
}: {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  year: string;
}): string =>
  [CACHE_NAMESPACE, year, month, day, keibajoCode, raceNumber, "inputs"].join(":");

const getCacheRequest = (cacheKey: string): Request =>
  new Request(`${CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

export const getCachedFinishPredictionInputs = async (
  cacheKey: string,
): Promise<FinishPredictionStaticPayload | null> => {
  const defaultCache = getDefaultCache();
  const cacheRequest = getCacheRequest(cacheKey);
  const cachedResponse = await defaultCache?.match(cacheRequest);
  if (cachedResponse?.ok) {
    try {
      return (await cachedResponse.json()) as FinishPredictionStaticPayload;
    } catch {
      await defaultCache?.delete(cacheRequest);
    }
  }

  const { env, ctx } = await getCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!kvBody) {
    return null;
  }

  try {
    const parsed = JSON.parse(kvBody) as FinishPredictionStaticPayload;
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
    ctx?.waitUntil(putCache());
    return parsed;
  } catch {
    return null;
  }
};

export const putFinishPredictionInputsCache = async ({
  body,
  cacheKey,
  race,
}: {
  body: string;
  cacheKey: string;
  race: RaceDetail;
}): Promise<void> => {
  const { env, ctx } = await getCloudflareRuntime();
  const ttlSeconds = getFinishPredictionInputsCacheTtlSeconds(race, env);
  if (ttlSeconds <= 0) {
    return;
  }
  const defaultCache = getDefaultCache();
  const cacheRequest = getCacheRequest(cacheKey);
  const putCaches = Promise.all([
    defaultCache?.put(
      cacheRequest,
      new Response(body, {
        headers: {
          "Cache-Control": `public, max-age=${Math.min(ttlSeconds, 60)}`,
          "Content-Type": DEFAULT_CONTENT_TYPE,
        },
      }),
    ),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, { expirationTtl: ttlSeconds }),
  ]);
  if (ctx !== null) {
    ctx.waitUntil(putCaches);
    return;
  }
  await putCaches;
};
