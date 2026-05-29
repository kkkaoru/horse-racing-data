// Run with bun. Stub aggregator for race-trend endpoint.
// Real aggregation will read past-N-race Parquet files; this skeleton
// exposes the function shape so downstream callers can compile.

import type { DailyRaceEntryRow } from "../types";

export interface RaceTrendQueryParams {
  source: "jra" | "nar";
  keibajoCode: string;
  raceBango: string;
  from: string;
  to: string;
}

export interface RaceTrendAggregate {
  raceCount: number;
  starterCount: number;
  byJockey: Record<string, number>;
  byWaku: Record<string, number>;
}

export const buildRaceTrendCacheKey = (params: RaceTrendQueryParams): string =>
  `${params.source}-${params.keibajoCode}-${params.raceBango}-${params.from}-${params.to}`;

export const aggregateRaceTrend = (rows: DailyRaceEntryRow[]): RaceTrendAggregate => {
  const byJockey: Record<string, number> = {};
  const byWaku: Record<string, number> = {};
  const raceKeys = new Set<string>();
  for (const row of rows) {
    raceKeys.add(`${row.source}:${row.race_date}:${row.keibajo_code}:${row.race_bango}`);
    if (row.kishumei_ryakusho) {
      byJockey[row.kishumei_ryakusho] = (byJockey[row.kishumei_ryakusho] ?? 0) + 1;
    }
    if (row.wakuban) {
      byWaku[row.wakuban] = (byWaku[row.wakuban] ?? 0) + 1;
    }
  }
  return {
    byJockey,
    byWaku,
    raceCount: raceKeys.size,
    starterCount: rows.length,
  };
};
