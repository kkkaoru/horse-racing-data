// Run with bun (Next.js route).
// Internal endpoint hit by sync-realtime-data when a race finishes. Busts the
// day-level trend caches (race-trend + d1-daily + d1-snapshot) plus every
// detail-section cache (main + stale tier) for every race on the day so the
// very next fetch from any open trend page returns fresh data without waiting
// for the hourly daily-feature-build cron.
import { NextResponse } from "next/server";

import { getRacesByDateWithoutJockeyNames } from "../../../../db/queries";
import {
  safeGetCloudflareEnv,
  safeGetCloudflareExecutionContext,
} from "../../../../lib/cloudflare-context.server";
import type { RaceSource } from "../../../../lib/codes";
import { bustRaceCachesForRace } from "../../../../lib/race-cache-bust.server";
import { readRaceCacheWarmGeneration } from "../../../../lib/race-cache-warm-generation";
import {
  buildDefaultRaceTrendCacheOptions,
  type RaceTrendCacheWarmMessage,
} from "../../../../lib/race-trend-cache";
import {
  bustRaceTrendCachesForDay,
  type BustRaceTrendCachesParams,
} from "../../../../lib/race-trend-cache.server";

export const dynamic = "force-dynamic";

const YYYYMMDD_PATTERN = /^\d{8}$/u;
const KEIBAJO_CODE_PATTERN = /^[0-9A-Z]{2}$/u;
const RACE_BANGO_PATTERN = /^\d{2}$/u;
const AUTH_HEADER = "x-pc-keiba-internal-token";

const isRaceSource = (value: unknown): value is RaceSource => value === "jra" || value === "nar";

interface LegacyBustRequestBody {
  source: RaceSource;
  targetYmd: string;
}

interface ScopedBustRequestBody extends LegacyBustRequestBody {
  keibajoCode: string;
  raceBango: string;
}

type BustRequestBody = LegacyBustRequestBody | ScopedBustRequestBody;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const parseBody = (raw: unknown): BustRequestBody | null => {
  if (!isRecord(raw)) return null;
  if (!isRaceSource(raw.source)) return null;
  if (typeof raw.targetYmd !== "string" || !YYYYMMDD_PATTERN.test(raw.targetYmd)) {
    return null;
  }
  const hasKeibajoCode = raw.keibajoCode !== undefined;
  const hasRaceBango = raw.raceBango !== undefined;
  if (!hasKeibajoCode && !hasRaceBango) {
    return { source: raw.source, targetYmd: raw.targetYmd };
  }
  if (
    typeof raw.keibajoCode !== "string" ||
    !KEIBAJO_CODE_PATTERN.test(raw.keibajoCode) ||
    typeof raw.raceBango !== "string" ||
    !RACE_BANGO_PATTERN.test(raw.raceBango)
  ) {
    return null;
  }
  return {
    keibajoCode: raw.keibajoCode,
    raceBango: raw.raceBango,
    source: raw.source,
    targetYmd: raw.targetYmd,
  };
};

const isAuthorized = (request: Request): boolean => {
  const expected = process.env.PC_KEIBA_INTERNAL_TOKEN;
  if (!expected) return false;
  return request.headers.get(AUTH_HEADER) === expected;
};

interface YmdParts {
  day: string;
  month: string;
  year: string;
}

const splitYmd = (ymd: string): YmdParts => ({
  day: ymd.slice(6, 8),
  month: ymd.slice(4, 6),
  year: ymd.slice(0, 4),
});

interface DayRaceRef {
  keibajoCode: string;
  raceBango: string;
}

const isScopedBustRequest = (body: BustRequestBody): body is ScopedBustRequestBody =>
  "keibajoCode" in body;

const listDayRaces = async (body: BustRequestBody): Promise<DayRaceRef[]> => {
  const parts = splitYmd(body.targetYmd);
  const races = await getRacesByDateWithoutJockeyNames(parts.year, parts.month, parts.day).catch(
    () => [],
  );
  return races
    .filter((race) => race.source === body.source)
    .filter(
      (race) =>
        !isScopedBustRequest(body) ||
        (race.keibajoCode === body.keibajoCode &&
          race.raceBango.padStart(2, "0") >= body.raceBango),
    )
    .map((race) => ({ keibajoCode: race.keibajoCode, raceBango: race.raceBango }));
};

interface BustDetailSectionsForDayParams {
  races: ReadonlyArray<DayRaceRef>;
  source: RaceSource;
  targetYmd: string;
}

const bustDetailSectionsForDay = async ({
  races,
  source,
  targetYmd,
}: BustDetailSectionsForDayParams): Promise<number> => {
  const outcomes = await Promise.all(
    races.map((race) =>
      bustRaceCachesForRace({
        keibajoCode: race.keibajoCode,
        mmdd: targetYmd.slice(4, 8),
        raceBango: race.raceBango,
        source,
        year: targetYmd.slice(0, 4),
      }).catch(() => ({ busted: 0, generation: 0 })),
    ),
  );
  return outcomes.reduce((sum, outcome) => sum + outcome.busted, 0);
};

interface EnqueueAffectedTrendWarmsParams {
  races: ReadonlyArray<DayRaceRef>;
  source: RaceSource;
  targetYmd: string;
}

const enqueueAffectedTrendWarms = async ({
  races,
  source,
  targetYmd,
}: EnqueueAffectedTrendWarmsParams): Promise<void> => {
  const env = await safeGetCloudflareEnv();
  const queue = env?.DETAIL_SECTION_CACHE_QUEUE;
  if (!queue) {
    return;
  }
  const parts = splitYmd(targetYmd);
  await Promise.all(
    races.map(async (race) => {
      const generationState = await readRaceCacheWarmGeneration({
        kind: "race-trend",
        kv: env.DETAIL_SECTION_CACHE_KV,
        race: {
          keibajoCode: race.keibajoCode,
          mmdd: targetYmd.slice(4, 8),
          raceBango: race.raceBango,
          source,
          year: parts.year,
        },
      });
      const message: RaceTrendCacheWarmMessage = {
        cacheGeneration: generationState?.generation ?? "0",
        day: parts.day,
        kind: "race-trend",
        keibajoCode: race.keibajoCode,
        month: parts.month,
        options: buildDefaultRaceTrendCacheOptions(source, targetYmd),
        raceNumber: race.raceBango,
        source,
        year: parts.year,
      };
      await queue.send(message);
    }),
  );
};

interface TrendCacheBustResult {
  keys: string[];
  notified: number;
  sectionBusted: number;
}

const runTrendCacheBust = async (body: BustRequestBody): Promise<TrendCacheBustResult> => {
  const races = await listDayRaces(body);
  const params: BustRaceTrendCachesParams = {
    races,
    source: body.source,
    targetYmd: body.targetYmd,
  };
  const result = await bustRaceTrendCachesForDay(params);
  const sectionBusted = await bustDetailSectionsForDay({
    races,
    source: body.source,
    targetYmd: body.targetYmd,
  });
  await enqueueAffectedTrendWarms({ races, source: body.source, targetYmd: body.targetYmd });
  // The subsequent generation-bound warm rebuild calls
  // notifyRaceTrendRoomIfChanged with the actual payload hash. Do not wake
  // every DO room before fresh bytes exist.
  return { keys: result.keys, notified: 0, sectionBusted };
};

export async function POST(request: Request): Promise<Response> {
  if (!isAuthorized(request)) {
    return NextResponse.json({ error: "forbidden" }, { status: 403 });
  }
  const rawBody = (await request.json().catch(() => null)) as unknown;
  const body = parseBody(rawBody);
  if (!body) {
    return NextResponse.json({ error: "invalid body" }, { status: 400 });
  }
  const ctx = await safeGetCloudflareExecutionContext();
  if (ctx) {
    ctx.waitUntil(
      runTrendCacheBust(body).catch((error: unknown) => {
        console.error("Trend cache bust background task failed", error);
      }),
    );
    return NextResponse.json({ accepted: true, ok: true }, { status: 202 });
  }
  const result = await runTrendCacheBust(body);
  return NextResponse.json({
    keys: result.keys,
    notified: result.notified,
    ok: true,
    sectionBusted: result.sectionBusted,
  });
}
