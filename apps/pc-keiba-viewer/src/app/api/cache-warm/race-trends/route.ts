import { getCloudflareContext } from "@opennextjs/cloudflare";
import { NextResponse } from "next/server";

import { getRacesByDate } from "../../../../db/queries";
import { getJstDateParts, parseIsoDateParts } from "../../../../lib/race-detail-section-cache";
import {
  RACE_TREND_CACHE_PRE_START_SECONDS,
  buildDefaultRaceTrendCacheOptions,
  getRaceStartTimeMs,
  type RaceTrendCacheWarmMessage,
} from "../../../../lib/race-trend-cache";

export const dynamic = "force-dynamic";

const WARM_LOOKAHEAD_SECONDS = 5 * 60;
const WARM_AFTER_START_SECONDS = 60;

const getCloudflareEnv = async (): Promise<CloudflareEnv | null> => {
  try {
    return (await getCloudflareContext({ async: true })).env;
  } catch {
    return null;
  }
};

const parseNowMs = (searchParams: URLSearchParams): number => {
  const value = searchParams.get("now");
  if (!value) {
    return Date.now();
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : Date.now();
};

const getTargetDateParts = (searchParams: URLSearchParams, nowMs: number) =>
  parseIsoDateParts(searchParams.get("date")) ?? getJstDateParts(new Date(nowMs));

const getWarmDelaySeconds = (
  race: {
    hassoJikoku?: string | null;
    kaisaiNen: string;
    kaisaiTsukihi: string;
  },
  nowMs: number,
): number | null => {
  const raceStartTime = getRaceStartTimeMs(race);
  if (raceStartTime === null) {
    return null;
  }

  const warmStart = raceStartTime - RACE_TREND_CACHE_PRE_START_SECONDS * 1000;
  const warmEnd = raceStartTime + WARM_AFTER_START_SECONDS * 1000;
  if (nowMs >= warmStart && nowMs <= warmEnd) {
    return 0;
  }
  if (warmStart > nowMs && warmStart <= nowMs + WARM_LOOKAHEAD_SECONDS * 1000) {
    return Math.max(0, Math.ceil((warmStart - nowMs) / 1000));
  }
  return null;
};

export async function POST(request: Request) {
  const searchParams = new URL(request.url).searchParams;
  const allowed =
    request.headers.get("X-PC-Keiba-Cache-Warm") === "scheduled" ||
    searchParams.get("debug") === "1";
  if (!allowed) {
    return NextResponse.json({ error: "not_found" }, { status: 404 });
  }

  const nowMs = parseNowMs(searchParams);
  const target = getTargetDateParts(searchParams, nowMs);
  const races = await getRacesByDate(target.year, target.month, target.day);
  const messages = races
    .map((race): { delaySeconds: number; message: RaceTrendCacheWarmMessage } | null => {
      const delaySeconds = getWarmDelaySeconds(race, nowMs);
      if (delaySeconds === null) {
        return null;
      }
      return {
        delaySeconds,
        message: {
          day: target.day,
          kind: "race-trend",
          keibajoCode: race.keibajoCode,
          month: target.month,
          options: buildDefaultRaceTrendCacheOptions(
            race.source,
            `${race.kaisaiNen}${race.kaisaiTsukihi}`,
          ),
          raceNumber: race.raceBango,
          source: race.source,
          year: target.year,
        },
      };
    })
    .filter(
      (entry): entry is { delaySeconds: number; message: RaceTrendCacheWarmMessage } =>
        entry !== null,
    );

  const env = await getCloudflareEnv();
  const queue = env?.DETAIL_SECTION_CACHE_QUEUE;
  if (!queue) {
    return NextResponse.json(
      {
        date: `${target.year}-${target.month}-${target.day}`,
        dueRaceCount: messages.length,
        enqueued: 0,
        error: "DETAIL_SECTION_CACHE_QUEUE binding is unavailable",
        raceCount: races.length,
      },
      { status: 503 },
    );
  }

  await Promise.all(
    messages.map(({ delaySeconds, message }) =>
      queue.send(message, delaySeconds > 0 ? { delaySeconds } : undefined),
    ),
  );

  return NextResponse.json({
    date: `${target.year}-${target.month}-${target.day}`,
    dueRaceCount: messages.length,
    enqueued: messages.length,
    raceCount: races.length,
  });
}
