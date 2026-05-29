// Run with bun. Shared mapping helpers between the production realtime API
// route and the dev-only NAR scraper. Both call the `sync-realtime-data-hot`
// Worker (one via service binding, the other via direct CF Access fetch in
// `next dev`) and project the same `HotOddsPayload` shape into the viewer's
// `RealtimeRacePayload`.
import "server-only";
import type {
  RealtimeHorseOddsTrend,
  RealtimeOddsData,
  RealtimeOddsHistoryPoint,
  RealtimeOddsTrend,
  RealtimeOddsTrendPoint,
  RealtimeOddsType,
  RealtimeRacePayload,
} from "horse-racing-realtime/types";

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

export const HOT_WORKER_ORIGIN = "https://sync-realtime-data-hot.kkk4oru.com";

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

export const isHotOddsPayload = (value: unknown): value is HotOddsPayload => {
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
