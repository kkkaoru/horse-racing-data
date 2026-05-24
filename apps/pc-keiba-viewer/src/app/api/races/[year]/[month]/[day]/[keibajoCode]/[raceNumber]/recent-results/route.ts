// API endpoint for the paddock-edit page's per-horse recent race results.
// SSR used to await getHorseRaceResults inline (~360-row Postgres scan +
// large JSON serialisation in the page response, which crashed mobile
// browsers). We now serve it through this lazy endpoint and cache the
// result in KV/Cache API via `recent-results-cache.server` so subsequent
// fetches in any colo are cheap.
// Execute with bun: opennextjs-cloudflare build && wrangler dev

import { NextResponse } from "next/server";

import { getHorseRaceResults, getRaceSourceByRoute } from "../../../../../../../../../db/queries";
import type { RaceSource } from "../../../../../../../../../lib/codes";
import {
  buildRecentResultsCacheKey,
  getCachedRecentResultsBody,
  putRecentResultsCache,
  RECENT_RESULTS_CACHE_TTL_SECONDS,
  type RecentResultsSourceScope,
} from "../../../../../../../../../lib/recent-results-cache.server";

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

const isSourceScope = (value: string | null): value is RecentResultsSourceScope =>
  value === "jra" || value === "nar" || value === "all";

const getSourceScope = (searchParams: URLSearchParams): RecentResultsSourceScope => {
  const raw = searchParams.get("sourceScope");
  return isSourceScope(raw) ? raw : "all";
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
  const cacheKey = buildRecentResultsCacheKey({
    day,
    keibajoCode,
    month,
    raceNumber,
    source,
    sourceScope,
    year,
  });
  const cached = await getCachedRecentResultsBody(cacheKey);
  if (cached) {
    return new Response(cached, {
      headers: {
        "Cache-Control": `public, max-age=${RECENT_RESULTS_CACHE_TTL_SECONDS}`,
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
  await putRecentResultsCache(cacheKey, body);
  return new Response(body, {
    headers: {
      "Cache-Control": `public, max-age=${RECENT_RESULTS_CACHE_TTL_SECONDS}`,
      "Content-Type": "application/json; charset=utf-8",
      "X-Recent-Results-Cache": "MISS-STORED",
    },
  });
}
