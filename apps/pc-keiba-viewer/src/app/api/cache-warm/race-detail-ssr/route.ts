// Warm the KV-backed race-detail SSR snapshot for a date's races. The
// scheduled handler hits this with no date to target today (default) or
// `?date=YYYY-MM-DD` to target a specific day. Each race resolves the
// (race, runners, courseInfo, sameVenueRaces) fan-out once and pushes it
// to both Cache API and DETAIL_SECTION_CACHE_KV so subsequent SSR hits
// in any colo skip Hyperdrive.
// Execute with bun: opennextjs-cloudflare build && wrangler dev

import { NextResponse } from "next/server";

// Single-race warm (?keibajo=&race=) resolves via getRaceSourceByRoute +
// getRaceDetail, the same path as page SSR. Date-wide warm still lists
// through getRacesByDate so it shares that query-cache key with the day page.
import {
  getHorseRaceResults,
  getRaceCourseInfo,
  getRaceDetail,
  getRaceRunners,
  getRaceSourceByRoute,
  getRacesByDate,
  getSameVenueRacesByDate,
} from "../../../../db/queries";
import { safeGetCloudflareEnv } from "../../../../lib/cloudflare-context.server";
import type { RaceSource } from "../../../../lib/codes";
import {
  markRaceCacheWarmGeneration,
  readRaceCacheWarmGeneration,
} from "../../../../lib/race-cache-warm-generation";
import { getJstDateParts, parseIsoDateParts } from "../../../../lib/race-detail-section-cache";
import {
  buildRaceDetailSsrCacheKey,
  putRaceDetailSsrSnapshot,
} from "../../../../lib/race-detail-ssr-cache.server";
import {
  buildRecentResultsCacheKey,
  putRecentResultsCache,
} from "../../../../lib/recent-results-cache.server";

export const dynamic = "force-dynamic";

interface WarmRaceParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceBango: string;
  source: RaceSource;
  year: string;
}

interface TargetDateParts {
  day: string;
  month: string;
  year: string;
}

interface ResolveSingleRaceParamsInput {
  keibajoCode: string;
  raceBango: string;
  target: TargetDateParts;
}

interface ResolveListedRaceParamsInput {
  keibajoCode: string | null;
  raceBango: string | null;
  target: TargetDateParts;
}

const WARM_CONCURRENCY = 6;
const RACE_CODE_WIDTH: number = 2;

const isRaceSource = (value: string): value is RaceSource => value === "jra" || value === "nar";

const padRaceCode = (value: string): string => value.padStart(RACE_CODE_WIDTH, "0");

const matchesPaddedRaceCode = (actual: string, expected: string): boolean =>
  padRaceCode(actual) === padRaceCode(expected);

const resolveSingleRaceParams = async (
  input: ResolveSingleRaceParamsInput,
): Promise<WarmRaceParams[]> => {
  const keibajoCode = padRaceCode(input.keibajoCode);
  const raceBango = padRaceCode(input.raceBango);
  const source = await getRaceSourceByRoute(
    input.target.year,
    input.target.month,
    input.target.day,
    keibajoCode,
    raceBango,
  );
  return source === null
    ? []
    : [
        {
          day: input.target.day,
          keibajoCode,
          month: input.target.month,
          raceBango,
          source,
          year: input.target.year,
        },
      ];
};

const resolveListedRaceParams = async (
  input: ResolveListedRaceParamsInput,
): Promise<WarmRaceParams[]> => {
  const races = await getRacesByDate(input.target.year, input.target.month, input.target.day);
  return races
    .filter((race): race is typeof race & { source: RaceSource } => isRaceSource(race.source))
    .filter(
      (race) =>
        input.keibajoCode === null || matchesPaddedRaceCode(race.keibajoCode, input.keibajoCode),
    )
    .filter(
      (race) => input.raceBango === null || matchesPaddedRaceCode(race.raceBango, input.raceBango),
    )
    .map((race) => ({
      day: input.target.day,
      keibajoCode: padRaceCode(race.keibajoCode),
      month: input.target.month,
      raceBango: padRaceCode(race.raceBango),
      source: race.source,
      year: input.target.year,
    }));
};

const warmRecentResults = async (params: WarmRaceParams): Promise<void> => {
  const { day, keibajoCode, month, raceBango, source, year } = params;
  const cacheKey = buildRecentResultsCacheKey({
    day,
    keibajoCode,
    month,
    raceNumber: raceBango,
    source,
    sourceScope: "all",
    year,
  });
  const results = await getHorseRaceResults(
    source,
    year,
    month,
    day,
    keibajoCode,
    raceBango,
    "all",
  ).catch(() => []);
  if (results.length === 0) {
    return;
  }
  await putRecentResultsCache(cacheKey, JSON.stringify({ results })).catch(() => undefined);
};

const warmRaceDetailSsr = async (params: WarmRaceParams): Promise<"warmed" | "missing"> => {
  const { day, keibajoCode, month, raceBango, source, year } = params;
  const race = await getRaceDetail(source, year, month, day, keibajoCode, raceBango);
  if (!race) {
    return "missing";
  }
  const [courseInfo, runners, sameVenueRaces] = await Promise.all([
    getRaceCourseInfo(keibajoCode, race.kyori, race.trackCode),
    getRaceRunners(source, year, month, day, keibajoCode, raceBango),
    getSameVenueRacesByDate(source, year, month, day, keibajoCode),
  ]);
  await Promise.all([
    putRaceDetailSsrSnapshot({
      cacheKey: buildRaceDetailSsrCacheKey({
        day,
        keibajoCode,
        month,
        raceNumber: raceBango,
        source,
        year,
      }),
      params: { day, keibajoCode, month, raceNumber: raceBango, source, year },
      snapshot: { courseInfo, race, runners, sameVenueRaces },
    }),
    warmRecentResults(params),
  ]);
  return "warmed";
};

const warmRaceDetailSsrIfInvalidated = async (
  params: WarmRaceParams,
  kv: PcKeibaKvNamespace | undefined,
): Promise<"warmed" | "missing" | "valid"> => {
  const race = {
    keibajoCode: params.keibajoCode,
    mmdd: `${params.month}${params.day}`,
    raceBango: params.raceBango,
    source: params.source,
    year: params.year,
  };
  const state = await readRaceCacheWarmGeneration({ kind: "race-detail-ssr", kv, race });
  if (state?.valid) {
    return "valid";
  }
  const outcome = await warmRaceDetailSsr(params);
  if (outcome === "missing") {
    return outcome;
  }
  await markRaceCacheWarmGeneration({
    generation: state?.generation ?? "0",
    kind: "race-detail-ssr",
    kv,
    race,
  });
  return outcome;
};

const chunkArray = <T>(items: readonly T[], size: number): T[][] =>
  items.reduce<T[][]>((accumulator, item, index) => {
    if (index % size === 0) {
      accumulator.push([]);
    }
    accumulator[accumulator.length - 1]?.push(item);
    return accumulator;
  }, []);

const processInPool = async <T, R>(
  items: readonly T[],
  size: number,
  worker: (item: T) => Promise<R>,
): Promise<R[]> => {
  // Process the queue in batches of `size` so we never have more than that
  // many in-flight Hyperdrive queries while still keeping the loop linear.
  const batches = chunkArray(items, size);
  const allResults: R[] = [];
  for (const batch of batches) {
    // eslint-disable-next-line no-await-in-loop
    const batchResults = await Promise.all(batch.map((item) => worker(item)));
    allResults.push(...batchResults);
  }
  return allResults;
};

const getTargetDateParts = (searchParams: URLSearchParams): TargetDateParts =>
  parseIsoDateParts(searchParams.get("date")) ?? getJstDateParts(new Date());

export async function POST(request: Request) {
  const searchParams = new URL(request.url).searchParams;
  const allowed =
    request.headers.get("X-PC-Keiba-Cache-Warm") === "scheduled" ||
    searchParams.get("debug") === "1";
  if (!allowed) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const target = getTargetDateParts(searchParams);
  const keibajoCode = searchParams.get("keibajo");
  const raceBango = searchParams.get("race");
  const raceParams: WarmRaceParams[] =
    keibajoCode !== null && raceBango !== null
      ? await resolveSingleRaceParams({ keibajoCode, raceBango, target })
      : await resolveListedRaceParams({ keibajoCode, raceBango, target });
  const env = await safeGetCloudflareEnv();
  const outcomes = await processInPool(raceParams, WARM_CONCURRENCY, async (params) => {
    try {
      return await warmRaceDetailSsrIfInvalidated(params, env?.DETAIL_SECTION_CACHE_KV);
    } catch {
      return "missing" as const;
    }
  });
  return NextResponse.json({
    date: `${target.year}-${target.month}-${target.day}`,
    raceCount: raceParams.length,
    warmed: outcomes.filter((outcome) => outcome === "warmed").length,
  });
}
