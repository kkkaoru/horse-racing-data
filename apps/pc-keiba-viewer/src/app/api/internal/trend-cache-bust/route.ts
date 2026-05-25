// Run with bun (Next.js route).
// Internal endpoint hit by sync-realtime-data when a race finishes. Busts the
// day-level trend caches (race-trend + d1-daily + d1-snapshot) so the very next
// fetch from any open trend page returns fresh data without waiting for the
// hourly daily-feature-build cron.
import { NextResponse } from "next/server";

import { getRacesByDateWithoutJockeyNames } from "../../../../db/queries";
import type { RaceSource } from "../../../../lib/codes";
import {
  bustRaceTrendCachesForDay,
  type BustRaceTrendCachesParams,
} from "../../../../lib/race-trend-cache.server";
import { notifyRaceTrendRoom } from "../../../../lib/race-trend-room.server";

export const dynamic = "force-dynamic";

const YYYYMMDD_PATTERN = /^\d{8}$/u;
const AUTH_HEADER = "x-pc-keiba-internal-token";

const isRaceSource = (value: unknown): value is RaceSource =>
  value === "jra" || value === "nar";

interface BustRequestBody {
  source: RaceSource;
  targetYmd: string;
}

const parseBody = (raw: unknown): BustRequestBody | null => {
  if (typeof raw !== "object" || raw === null) return null;
  const candidate = raw as Record<string, unknown>;
  if (!isRaceSource(candidate.source)) return null;
  if (typeof candidate.targetYmd !== "string" || !YYYYMMDD_PATTERN.test(candidate.targetYmd)) {
    return null;
  }
  return { source: candidate.source, targetYmd: candidate.targetYmd };
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

const notifyAllRoomsForDay = async (
  source: RaceSource,
  ymd: string,
): Promise<number> => {
  const parts = splitYmd(ymd);
  const races = await getRacesByDateWithoutJockeyNames(parts.year, parts.month, parts.day).catch(
    () => [],
  );
  const targetRaces = races.filter((race) => race.source === source);
  const outcomes = await Promise.all(
    targetRaces.map((race) =>
      notifyRaceTrendRoom(
        {
          day: parts.day,
          keibajoCode: race.keibajoCode,
          month: parts.month,
          raceNumber: race.raceBango,
          source,
          year: parts.year,
        },
        { cacheKey: `race-trend-day:${source}:${ymd}` },
      ).catch(() => false),
    ),
  );
  return outcomes.filter(Boolean).length;
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
  const params: BustRaceTrendCachesParams = {
    source: body.source,
    targetYmd: body.targetYmd,
  };
  const result = await bustRaceTrendCachesForDay(params);
  const notified = await notifyAllRoomsForDay(body.source, body.targetYmd);
  return NextResponse.json({ keys: result.keys, notified, ok: true });
}
