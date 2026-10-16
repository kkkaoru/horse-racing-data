// This file runs with Bun. Comparison only: never deduplicate or modify either source snapshot.
import { historyRowFingerprint } from "./history-preparation";
import type { HistorySourceRow } from "./sources/history-source-page";

export const verifyHistoryRowsIncluded = (
  original: readonly HistorySourceRow[],
  current: readonly HistorySourceRow[],
): void => {
  const remaining: Map<string, number> = new Map();
  current.forEach((row) => {
    const key: string = historyRowFingerprint(row);
    remaining.set(key, (remaining.get(key) ?? 0) + 1);
  });
  original.forEach((row) => {
    const key: string = historyRowFingerprint(row);
    const count: number = remaining.get(key) ?? 0;
    if (count === 0)
      throw new Error("A source row or duplicate occurrence is absent from the fresh snapshot.");
    remaining.set(key, count - 1);
  });
};
