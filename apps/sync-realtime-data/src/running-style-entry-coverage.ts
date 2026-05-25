// Run with bun. Determines when running-style caches may be published.

import type { RaceRunningStyleRow } from "./running-style-d1";

export const RUNNING_STYLE_SCRATCH_STATUS_LABELS = [
  "出場停止",
  "出走取消",
  "取消",
  "競走除外",
  "除外",
] as const;

export interface RunningStyleEntrySnapshot {
  horseNumber: string;
  status: string | null;
}

export interface RunningStyleCacheCoverage {
  activeHorseCount: number;
  cacheable: boolean;
  cacheableRows: RaceRunningStyleRow[];
}

const SCRATCH_STATUS_SET = new Set<string>(RUNNING_STYLE_SCRATCH_STATUS_LABELS);

export const isRunningStyleScratchStatus = (status: string | null | undefined): boolean =>
  typeof status === "string" && status.length > 0 && SCRATCH_STATUS_SET.has(status);

export const normalizeRunningStyleHorseNumber = (value: string | number): number | null => {
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) ? parsed : null;
};

export const listActiveRunningStyleHorseNumbers = (
  entries: ReadonlyArray<RunningStyleEntrySnapshot>,
): number[] => {
  const numbers = new Set<number>();
  entries.forEach((entry) => {
    if (isRunningStyleScratchStatus(entry.status)) {
      return;
    }
    const horseNumber = normalizeRunningStyleHorseNumber(entry.horseNumber);
    if (horseNumber !== null) {
      numbers.add(horseNumber);
    }
  });
  return [...numbers].toSorted((left, right) => left - right);
};

export const evaluateRunningStyleCacheCoverage = (
  entries: ReadonlyArray<RunningStyleEntrySnapshot> | null,
  rows: ReadonlyArray<RaceRunningStyleRow>,
): RunningStyleCacheCoverage => {
  if (entries === null || entries.length === 0) {
    return {
      activeHorseCount: rows.length,
      cacheable: rows.length > 0,
      cacheableRows: [...rows],
    };
  }
  const activeHorseNumbers = listActiveRunningStyleHorseNumbers(entries);
  if (activeHorseNumbers.length === 0) {
    return {
      activeHorseCount: 0,
      cacheable: false,
      cacheableRows: [],
    };
  }
  const activeNumberSet = new Set(activeHorseNumbers);
  const cacheableRows = rows.filter((row) => activeNumberSet.has(row.horseNumber));
  const coveredNumbers = new Set(cacheableRows.map((row) => row.horseNumber));
  const cacheable =
    cacheableRows.length === activeHorseNumbers.length &&
    activeHorseNumbers.every((horseNumber) => coveredNumbers.has(horseNumber));
  return {
    activeHorseCount: activeHorseNumbers.length,
    cacheable,
    cacheableRows,
  };
};
