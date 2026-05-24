// API endpoint for the paddock-edit page's per-horse recent race results.
// SSR used to await getHorseRaceResults inline (~360-row Postgres scan +
// large JSON serialisation in the page response, which crashed mobile
// browsers). We now serve it through this lazy endpoint and cache the
// result in KV/Cache API so subsequent fetches in any colo are cheap.
// Execute with bun: opennextjs-cloudflare build && wrangler dev

import { getCloudflareContext } from "@opennextjs/cloudflare";
import { NextResponse } from "next/server";

import { getHorseRaceResults, getRaceSourceByRoute } from "../../../../../../../../../db/queries";
import type { RaceSource } from "../../../../../../../../../lib/codes";

export const dynamic = "force-dynamic";

interface RouteContext {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

const isRaceSource = (value: string | null): value is RaceSource =>
  value === "jra" || value === "nar";

const isValidRouteParams = (
  year: string,
  month: string,
  day: string,
  keibajoCode: string,
  raceNumber: string,
): boolean =>
  /^\d{4}$/.test(year) &&
  /^\d{2}$/.test(month) &&
  /^\d{2}$/.test(day) &&
  /^[0-9A-Z]{2}$/.test(keibajoCode) &&
  /^\d{2}$/.test(raceNumber);

const CACHE_NAMESPACE = "pc-keiba-viewer:recent-results:v1";
const CACHE_URL_BASE = "https://pc-keiba-viewer.local/recent-results-cache/";
const DEFAULT_CACHE_TTL_SECONDS = 6 * 60 * 60;
const MIN_KV_TTL_SECONDS = 60;

const buildCacheKey = (params: {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  sourceScope: RaceSource | "all";
  year: string;
}): string =>
  [
    CACHE_NAMESPACE,
    params.source,
    params.year,
    params.month,
    params.day,
    params.keibajoCode,
    params.raceNumber,
    params.sourceScope,
  ].join(":");

const getCacheRequest = (cacheKey: string): Request =>
  new Request(`${CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const tryGetCloudflareRuntime = async (): Promise<{
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

const isSourceScope = (value: string | null): value is RaceSource | "all" =>
  value === "jra" || value === "nar" || value === "all";

const getSourceScope = (searchParams: URLSearchParams): RaceSource | "all" => {
  const raw = searchParams.get("sourceScope");
  return isSourceScope(raw) ? raw : "all";
};

const readCachedRecentResults = async (cacheKey: string): Promise<string | null> => {
  const cache = getDefaultCache();
  const cachedResponse = await cache?.match(getCacheRequest(cacheKey));
  if (cachedResponse?.ok) {
    return cachedResponse.text();
  }
  const { env, ctx } = await tryGetCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!kvBody) {
    return null;
  }
  if (cache) {
    ctx?.waitUntil(
      cache.put(
        getCacheRequest(cacheKey),
        new Response(kvBody, {
          headers: {
            "Cache-Control": "public, max-age=60",
            "Content-Type": "application/json; charset=utf-8",
          },
        }),
      ),
    );
  }
  return kvBody;
};

const persistRecentResults = async (cacheKey: string, body: string): Promise<void> => {
  const cache = getDefaultCache();
  const { env } = await tryGetCloudflareRuntime();
  await Promise.all([
    cache?.put(
      getCacheRequest(cacheKey),
      new Response(body, {
        headers: {
          "Cache-Control": `public, max-age=${DEFAULT_CACHE_TTL_SECONDS}`,
          "Content-Type": "application/json; charset=utf-8",
        },
      }),
    ),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, {
      expirationTtl: Math.max(MIN_KV_TTL_SECONDS, DEFAULT_CACHE_TTL_SECONDS),
    }),
  ]);
};

export async function GET(request: Request, context: RouteContext) {
  const { day, keibajoCode, month, raceNumber, year } = await context.params;
  if (!isValidRouteParams(year, month, day, keibajoCode, raceNumber)) {
    return NextResponse.json({ error: "invalid_params" }, { status: 400 });
  }
  const searchParams = new URL(request.url).searchParams;
  const sourceParam = searchParams.get("source");
  const source = isRaceSource(sourceParam)
    ? sourceParam
    : await getRaceSourceByRoute(year, month, day, keibajoCode, raceNumber);
  if (!source) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }
  const sourceScope = getSourceScope(searchParams);
  const cacheKey = buildCacheKey({
    day,
    keibajoCode,
    month,
    raceNumber,
    source,
    sourceScope,
    year,
  });
  const cached = await readCachedRecentResults(cacheKey);
  if (cached) {
    return new Response(cached, {
      headers: {
        "Cache-Control": `public, max-age=${DEFAULT_CACHE_TTL_SECONDS}`,
        "Content-Type": "application/json; charset=utf-8",
        "X-Recent-Results-Cache": "HIT",
      },
    });
  }
  const results = await getHorseRaceResults(
    source,
    year,
    month,
    day,
    keibajoCode,
    raceNumber,
    sourceScope,
  );
  const body = JSON.stringify({ results });
  await persistRecentResults(cacheKey, body);
  return new Response(body, {
    headers: {
      "Cache-Control": `public, max-age=${DEFAULT_CACHE_TTL_SECONDS}`,
      "Content-Type": "application/json; charset=utf-8",
      "X-Recent-Results-Cache": "MISS-STORED",
    },
  });
}
