// Run with bun. Resolves expected active horse counts for running-style generation.

import {
  evaluateRunningStyleCacheCoverage,
  isRunningStyleScratchStatus,
  normalizeRunningStyleHorseNumber,
  type RunningStyleEntrySnapshot,
} from "./running-style-entry-coverage";
import {
  buildRealtimeRaceKeyFromRunningStyle,
  parseRunningStyleRaceKey,
} from "./running-style-features";
import { getLatestRaceEntries } from "./storage";

export const resolveRunningStyleExpectedHorseCount = (
  featureCount: number,
  entries: { horses: ReadonlyArray<RunningStyleEntrySnapshot> } | null,
  featureHorseNumbers?: ReadonlySet<number>,
): number => {
  // Catalog coverage is produced from the same card as the feature rows. A
  // partial realtime snapshot must not silently remove entrants merely
  // because they are absent. Explicit scratch rows may still be newer than
  // Catalog, so subtract them only when that horse number is present in the
  // Catalog generation signature.
  if (featureCount > 0) {
    if (entries === null || featureHorseNumbers === undefined) return featureCount;
    const scratchedFeatureNumbers = new Set<number>();
    entries.horses.forEach((entry) => {
      if (!isRunningStyleScratchStatus(entry.status)) return;
      const horseNumber = normalizeRunningStyleHorseNumber(entry.horseNumber);
      if (horseNumber !== null && featureHorseNumbers.has(horseNumber)) {
        scratchedFeatureNumbers.add(horseNumber);
      }
    });
    return Math.max(0, featureCount - scratchedFeatureNumbers.size);
  }
  if (entries !== null && entries.horses.length > 0) {
    const activeHorseCount = evaluateRunningStyleCacheCoverage(entries.horses, []).activeHorseCount;
    if (activeHorseCount > 0) return activeHorseCount;
  }
  return featureCount;
};

const toRealtimeKeyForLookup = (runningStyleRaceKey: string): string | null => {
  const parsed = parseRunningStyleRaceKey(runningStyleRaceKey);
  return parsed === null ? null : buildRealtimeRaceKeyFromRunningStyle(parsed);
};

const horseNumbersFromEntrySignature = (signature: string | undefined): Set<number> | undefined => {
  if (signature === undefined) return undefined;
  return new Set(
    signature.split("|").flatMap((identity) => {
      const horseNumber = normalizeRunningStyleHorseNumber(identity.split(":", 1)[0] ?? "");
      return horseNumber === null ? [] : [horseNumber];
    }),
  );
};

export const listRunningStyleExpectedHorseCounts = async (
  db: D1Database,
  raceKeys: ReadonlyArray<string>,
  featureCounts: ReadonlyMap<string, number>,
  featureEntrySignatures: ReadonlyMap<string, string> = new Map(),
): Promise<Map<string, number>> => {
  const counts = new Map<string, number>();
  await Promise.all(
    raceKeys.map(async (raceKey) => {
      const featureCount = featureCounts.get(raceKey) ?? 0;
      const realtimeKey = toRealtimeKeyForLookup(raceKey);
      const entries = realtimeKey === null ? null : await getLatestRaceEntries(db, realtimeKey);
      counts.set(
        raceKey,
        resolveRunningStyleExpectedHorseCount(
          featureCount,
          entries,
          horseNumbersFromEntrySignature(featureEntrySignatures.get(raceKey)),
        ),
      );
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
  const scratchedNumbers = new Set<number>();
  entries.horses.forEach((entry) => {
    if (!isRunningStyleScratchStatus(entry.status)) return;
    const horseNumber = normalizeRunningStyleHorseNumber(entry.horseNumber);
    if (horseNumber !== null) scratchedNumbers.add(horseNumber);
  });
  if (scratchedNumbers.size === 0) return [...rows];
  return rows.filter((row) => !scratchedNumbers.has(row.umaban));
};
