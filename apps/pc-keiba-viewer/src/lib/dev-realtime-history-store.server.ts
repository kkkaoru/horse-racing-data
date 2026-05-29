// Run with bun. Process-local in-memory odds history store for the dev-only
// NAR realtime scraper. The viewer's `next dev` poll appends one snapshot per
// fetch and reads back the accumulated time series so the trend chart has
// something to draw before D1 / sync-realtime-data-hot are wired up locally.
import "server-only";
import type {
  RealtimeHorseOddsTrend,
  RealtimeOddsHistoryPoint,
  RealtimeOddsTrend,
  RealtimeOddsTrendPoint,
} from "horse-racing-realtime/types";
import type { OddsData, OddsType } from "sync-realtime-data-hot/types";

const MAX_POINTS_PER_RACE = 60;
const SUPPORTED_TYPES: OddsType[] = ["tansho", "fukusho"];
const TANSHO_TYPE: OddsType = "tansho";

export interface DevOddsSnapshot {
  byType: Partial<Record<OddsType, OddsData[]>>;
  fetchedAt: string;
}

interface RaceHistory {
  snapshots: DevOddsSnapshot[];
}

const store = new Map<string, RaceHistory>();

const resolveOdds = (item: OddsData): number | null => {
  if (typeof item.odds === "number") {
    return item.odds;
  }
  if (typeof item.averageOdds === "number") {
    return item.averageOdds;
  }
  return null;
};

const resolveRank = (item: OddsData): number | null =>
  typeof item.rank === "number" ? item.rank : null;

export const appendSnapshot = (raceKey: string, snapshot: DevOddsSnapshot): void => {
  const existing = store.get(raceKey) ?? { snapshots: [] };
  const tail = existing.snapshots.at(-1);
  if (tail && tail.fetchedAt === snapshot.fetchedAt) {
    store.set(raceKey, existing);
    return;
  }
  const next = [...existing.snapshots, snapshot];
  const trimmed = next.length > MAX_POINTS_PER_RACE ? next.slice(-MAX_POINTS_PER_RACE) : next;
  store.set(raceKey, { snapshots: trimmed });
};

export const readHistory = (raceKey: string): readonly DevOddsSnapshot[] =>
  store.get(raceKey)?.snapshots ?? [];

export const resetHistoryStore = (): void => {
  store.clear();
};

export const buildTanshoHistoryPoints = (
  snapshots: readonly DevOddsSnapshot[],
): RealtimeOddsHistoryPoint[] =>
  snapshots.flatMap((snapshot) =>
    (snapshot.byType[TANSHO_TYPE] ?? []).map((item) => ({
      fetchedAt: snapshot.fetchedAt,
      horseNumber: item.combination,
      odds: resolveOdds(item),
      popularity: resolveRank(item),
    })),
  );

export const buildHorseTrends = (history: RealtimeOddsHistoryPoint[]): RealtimeHorseOddsTrend[] => {
  const byHorse = new Map<string, RealtimeOddsHistoryPoint[]>();
  history.forEach((point) => {
    byHorse.set(point.horseNumber, [...(byHorse.get(point.horseNumber) ?? []), point]);
  });
  return Array.from(byHorse.entries()).map(([horseNumber, points]) => ({ horseNumber, points }));
};

export const buildHistoryByType = (
  snapshots: readonly DevOddsSnapshot[],
): Partial<Record<OddsType, RealtimeOddsTrendPoint[]>> => {
  const result: Partial<Record<OddsType, RealtimeOddsTrendPoint[]>> = {};
  SUPPORTED_TYPES.forEach((oddsType) => {
    const points = snapshots.flatMap((snapshot) =>
      (snapshot.byType[oddsType] ?? []).map((item) => ({
        combination: item.combination,
        fetchedAt: snapshot.fetchedAt,
        odds: resolveOdds(item),
        rank: resolveRank(item),
      })),
    );
    if (points.length > 0) {
      result[oddsType] = points;
    }
  });
  return result;
};

const groupTrendByCombination = (points: RealtimeOddsTrendPoint[]): RealtimeOddsTrend[] => {
  const byCombination = new Map<string, RealtimeOddsTrendPoint[]>();
  points.forEach((point) => {
    byCombination.set(point.combination, [...(byCombination.get(point.combination) ?? []), point]);
  });
  return Array.from(byCombination.entries()).map(([combination, items]) => ({
    combination,
    points: items,
  }));
};

export const buildTrendsByType = (
  historyByType: Partial<Record<OddsType, RealtimeOddsTrendPoint[]>>,
): Partial<Record<OddsType, RealtimeOddsTrend[]>> => {
  const result: Partial<Record<OddsType, RealtimeOddsTrend[]>> = {};
  SUPPORTED_TYPES.forEach((oddsType) => {
    const points = historyByType[oddsType];
    if (points) {
      result[oddsType] = groupTrendByCombination(points);
    }
  });
  return result;
};
