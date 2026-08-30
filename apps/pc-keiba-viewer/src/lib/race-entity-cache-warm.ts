// Run with bun. Defines per-race MCP entity-history cache warm queue messages.
import type { RaceSource } from "./codes";

export interface RaceEntityCacheWarmMessage {
  day: string;
  keibajoCode: string;
  kind: "race-entity-results";
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

export const buildRaceEntityCacheWarmPath = (message: RaceEntityCacheWarmMessage): string => {
  const url = new URL(
    "/v1/internal/race-entity-recent-results/warm",
    "https://pc-keiba-r2-catalog.internal",
  );
  url.searchParams.set("date", `${message.year}${message.month}${message.day}`);
  url.searchParams.set("keibajoCode", message.keibajoCode);
  url.searchParams.set("raceBango", message.raceNumber);
  url.searchParams.set("source", message.source);
  return `${url.pathname}${url.search}`;
};
