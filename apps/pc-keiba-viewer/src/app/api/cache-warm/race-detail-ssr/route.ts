// Warm the KV-backed race-detail SSR snapshot for a date's races. The
// scheduled handler hits this with no date to target today (default) or
// `?date=YYYY-MM-DD` to target a specific day. Each race resolves the
// (race, runners, courseInfo, sameVenueRaces) fan-out once and pushes it
// to both Cache API and DETAIL_SECTION_CACHE_KV so subsequent SSR hits
// in any colo skip Hyperdrive.
// Execute with bun: opennextjs-cloudflare build && wrangler dev

import { NextResponse } from "next/server";

import {
  getRaceCourseInfo,
  getRaceDetail,
  getRaceRunners,
  getRacesByDateWithoutJockeyNames,
  getSameVenueRacesByDate,
} from "../../../../db/queries";
import type { RaceSource } from "../../../../lib/codes";
import {
  getJstDateParts,
  parseIsoDateParts,
} from "../../../../lib/race-detail-section-cache";
import {
  buildRaceDetailSsrCacheKey,
  putRaceDetailSsrSnapshot,
} from "../../../../lib/race-detail-ssr-cache.server";

export const dynamic = "force-dynamic";

const WARM_CONCURRENCY = 6;

interface WarmRaceParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceBango: string;
  source: RaceSource;
  year: string;
}

const isRaceSource = (value: string): value is RaceSource => value === "jra" || value === "nar";

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
  await putRaceDetailSsrSnapshot({
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
  });
  return "warmed";
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

const getTargetDateParts = (
  searchParams: URLSearchParams,
): { day: string; month: string; year: string } =>
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
  const races = await getRacesByDateWithoutJockeyNames(target.year, target.month, target.day);
  const raceParams: WarmRaceParams[] = races
    .filter((race): race is typeof race & { source: RaceSource } => isRaceSource(race.source))
    .map((race) => ({
      day: target.day,
      keibajoCode: race.keibajoCode,
      month: target.month,
      raceBango: race.raceBango,
      source: race.source,
      year: target.year,
    }));
  const outcomes = await processInPool(raceParams, WARM_CONCURRENCY, async (params) => {
    try {
      return await warmRaceDetailSsr(params);
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
