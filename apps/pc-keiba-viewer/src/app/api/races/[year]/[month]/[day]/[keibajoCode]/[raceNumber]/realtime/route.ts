// Run with bun. Viewer-side realtime API route. In production it calls the
// `sync-realtime-data-hot` Worker directly via a service binding so the
// legacy `sync-realtime-data` Worker (and its saturated D1) is no longer in
// the request path. In local dev the keiba.go.jp scrape path is preserved
// behind `PC_KEIBA_DEV_REALTIME_SCRAPER=1`.
import { getCloudflareContext } from "@opennextjs/cloudflare";
import type {
  RealtimeHorseOddsTrend,
  RealtimeOddsData,
  RealtimeOddsHistoryPoint,
  RealtimeOddsTrend,
  RealtimeOddsTrendPoint,
  RealtimeOddsType,
  RealtimeRacePayload,
} from "horse-racing-realtime/types";
import { NextResponse } from "next/server";

import type { RaceSource } from "../../../../../../../../../lib/codes";
import {
  buildDevRealtimePayload,
  isDevScraperEnabled,
} from "../../../../../../../../../lib/dev-realtime-scraper.server";

interface RouteContext {
  params: Promise<{
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    year: string;
  }>;
}

// Mirrors the OddsCachePayload returned by sync-realtime-data-hot/odds-cache.
// `history` and `historyByType` are ALREADY grouped (HorseOddsTrend / OddsTrend
// shapes) on the worker side, so the viewer can route them straight into the
// grouped `horseTrends` / `trendsByType` fields without re-aggregating.
export interface HotOddsPayload {
  fetchedAt: string | null;
  history: RealtimeHorseOddsTrend[];
  historyByType: Partial<Record<RealtimeOddsType, RealtimeOddsTrend[]>>;
  latest: Partial<Record<RealtimeOddsType, RealtimeOddsData[]>>;
}

interface BuildRaceKeyParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

const HOT_WORKER_ORIGIN = "https://sync-realtime-data-hot.kkk4oru.com";
const NO_STORE_HEADERS = { "cache-control": "no-store" };

const isRaceSource = (value: string | null): value is RaceSource =>
  value === "jra" || value === "nar";

const padRaceNumber = (raceNumber: string): string => raceNumber.padStart(2, "0");

export const buildRaceKey = (params: BuildRaceKeyParams): string =>
  `${params.source}:${params.year}:${params.month}${params.day}:${params.keibajoCode}:${padRaceNumber(params.raceNumber)}`;

const isHotOddsPayload = (value: unknown): value is HotOddsPayload => {
  if (!value || typeof value !== "object") {
    return false;
  }
  if (
    !("fetchedAt" in value) ||
    !("latest" in value) ||
    !("history" in value) ||
    !("historyByType" in value)
  ) {
    return false;
  }
  const fetchedAt: unknown = Reflect.get(value, "fetchedAt");
  const latest: unknown = Reflect.get(value, "latest");
  const history: unknown = Reflect.get(value, "history");
  const historyByType: unknown = Reflect.get(value, "historyByType");
  if (fetchedAt !== null && typeof fetchedAt !== "string") {
    return false;
  }
  if (!Array.isArray(history)) {
    return false;
  }
  if (!historyByType || typeof historyByType !== "object") {
    return false;
  }
  return Boolean(latest) && typeof latest === "object";
};

const REALTIME_ODDS_TYPES: RealtimeOddsType[] = [
  "3renpuku",
  "3rentan",
  "fukusho",
  "tansho",
  "umaren",
  "umatan",
  "wakuren",
  "wide",
];

const flattenHistory = (history: RealtimeHorseOddsTrend[]): RealtimeOddsHistoryPoint[] =>
  history.flatMap((trend) => trend.points);

const flattenTrendsForType = (trends: RealtimeOddsTrend[]): RealtimeOddsTrendPoint[] =>
  trends.flatMap((trend) => trend.points);

const flattenHistoryByType = (
  historyByType: Partial<Record<RealtimeOddsType, RealtimeOddsTrend[]>>,
): Partial<Record<RealtimeOddsType, RealtimeOddsTrendPoint[]>> => {
  const result: Partial<Record<RealtimeOddsType, RealtimeOddsTrendPoint[]>> = {};
  REALTIME_ODDS_TYPES.forEach((oddsType) => {
    const trends = historyByType[oddsType];
    if (trends) {
      result[oddsType] = flattenTrendsForType(trends);
    }
  });
  return result;
};

export const fetchOddsFromHot = async (
  hot: CloudflareEnv["REALTIME_HOT"],
  raceKey: string,
): Promise<HotOddsPayload | null> => {
  if (!hot) {
    return null;
  }
  try {
    const response = await hot.fetch(`${HOT_WORKER_ORIGIN}/api/odds/${raceKey}`);
    if (!response.ok) {
      return null;
    }
    const json: unknown = await response.json();
    return isHotOddsPayload(json) ? json : null;
  } catch {
    return null;
  }
};

export const buildRealtimePayloadFromHot = (
  raceKey: string,
  odds: HotOddsPayload | null,
): RealtimeRacePayload => ({
  horseWeights: null,
  odds:
    odds && odds.fetchedAt
      ? {
          fetchedAt: odds.fetchedAt,
          history: flattenHistory(odds.history),
          historyByType: flattenHistoryByType(odds.historyByType),
          horseTrends: odds.history,
          latest: odds.latest,
          trendsByType: odds.historyByType,
        }
      : null,
  raceEntries: null,
  raceKey,
  raceResults: null,
  source: null,
  trackCondition: null,
});

export const dynamic = "force-dynamic";

export async function GET(request: Request, context: RouteContext) {
  const sourceParam = new URL(request.url).searchParams.get("source");
  if (!isRaceSource(sourceParam)) {
    return NextResponse.json({ error: "invalid source" }, { status: 400 });
  }

  const { day, keibajoCode, month, raceNumber, year } = await context.params;

  if (isDevScraperEnabled()) {
    const payload = await buildDevRealtimePayload({
      day,
      keibajoCode,
      month,
      raceNumber,
      source: sourceParam,
      year,
    });
    return NextResponse.json(payload, { headers: NO_STORE_HEADERS });
  }

  const raceKey = buildRaceKey({
    day,
    keibajoCode,
    month,
    raceNumber,
    source: sourceParam,
    year,
  });
  const { env } = await getCloudflareContext({ async: true });
  const odds = await fetchOddsFromHot(env.REALTIME_HOT, raceKey);
  const payload = buildRealtimePayloadFromHot(raceKey, odds);
  return NextResponse.json(payload, { headers: NO_STORE_HEADERS });
}
