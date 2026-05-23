// Run with bun. Resolves expected active horse counts for running-style generation.

import {
  evaluateRunningStyleCacheCoverage,
  listActiveRunningStyleHorseNumbers,
  type RunningStyleEntrySnapshot,
} from "./running-style-entry-coverage";
import { getLatestRaceEntries } from "./storage";

export const resolveRunningStyleExpectedHorseCount = (
  featureCount: number,
  entries: { horses: ReadonlyArray<RunningStyleEntrySnapshot> } | null,
): number => {
  if (entries !== null && entries.horses.length > 0) {
    const activeHorseCount = evaluateRunningStyleCacheCoverage(entries.horses, []).activeHorseCount;
    if (activeHorseCount > 0) {
      return activeHorseCount;
    }
  }
  return featureCount;
};

export const listRunningStyleExpectedHorseCounts = async (
  db: D1Database,
  raceKeys: ReadonlyArray<string>,
  featureCounts: ReadonlyMap<string, number>,
): Promise<Map<string, number>> => {
  const counts = new Map<string, number>();
  await Promise.all(
    raceKeys.map(async (raceKey) => {
      const featureCount = featureCounts.get(raceKey) ?? 0;
      const entries = await getLatestRaceEntries(db, raceKey);
      counts.set(raceKey, resolveRunningStyleExpectedHorseCount(featureCount, entries));
    }),
  );
  return counts;
};

export const filterRunningStyleFeatureRowsByActiveEntries = <T extends { umaban: number }>(
  rows: ReadonlyArray<T>,
  entries: { horses: ReadonlyArray<RunningStyleEntrySnapshot> } | null,
): T[] => {
  if (entries === null || entries.horses.length === 0) {
    return [...rows];
  }
  const activeNumbers = new Set(listActiveRunningStyleHorseNumbers(entries.horses));
  if (activeNumbers.size === 0) {
    return [...rows];
  }
  return rows.filter((row) => activeNumbers.has(row.umaban));
};
