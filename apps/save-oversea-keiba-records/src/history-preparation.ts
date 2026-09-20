// This file runs with Bun. Archival evidence is never silently promoted to canonical data.
import {
  verifyHistoryArchive,
  type HistoryArchivePlan,
  type HistoryArchiveState,
} from "./history-archive";
import {
  buildHistoryStatements,
  type HistoryPublicationInput,
} from "./storage/history-publication";
import type { HistorySourceRow } from "./sources/history-source-page";
import type {
  SecondaryHorseResult,
  SecondaryPersonResult,
} from "./sources/secondary-result-parser";

export interface PreparedHistoryArchive {
  readonly input: HistoryPublicationInput;
  readonly archivedRows: number;
  readonly publishedCount: number | null;
  readonly sourceComplete: boolean;
  readonly sourcePartialRows: readonly HistorySourceRow[];
  readonly duplicateRows: number;
}

const assertScope = (row: HistorySourceRow, plan: HistoryArchivePlan): void => {
  const matches: boolean =
    "personKind" in row
      ? row.personKind === plan.kind && row.sourcePersonId === plan.sourceId
      : plan.kind === "horse" && row.sourceHorseId === plan.sourceId;
  if (!matches) throw new Error("History archive contains a different source entity.");
};

const isSourcePartial = (row: HistorySourceRow): boolean =>
  "personKind" in row &&
  (row.finishPositionText.trim() === "" ||
    (row.sourceHorseId === null && (row.horseName === null || row.horseName.trim() === "")));

const naturalKey = (row: HistorySourceRow): string =>
  "personKind" in row
    ? JSON.stringify([
        row.personKind,
        row.sourcePersonId,
        row.sourceRaceId,
        row.sourceHorseId,
        row.sourceHorseId === null ? row.horseName : null,
      ])
    : JSON.stringify([row.sourceHorseId, row.sourceRaceId]);

const compareFields = (left: [string, unknown], right: [string, unknown]): number =>
  left[0].localeCompare(right[0]);
export const historyRowFingerprint = (row: HistorySourceRow): string =>
  JSON.stringify(Object.entries(row).sort(compareFields));

export const historyRowsToInput = (rows: readonly HistorySourceRow[]): HistoryPublicationInput => ({
  horses: rows.filter(
    (row: HistorySourceRow): row is SecondaryHorseResult => !("personKind" in row),
  ),
  people: rows.filter((row: HistorySourceRow): row is SecondaryPersonResult => "personKind" in row),
});

export const prepareHistoryArchive = (
  state: HistoryArchiveState,
  plan: HistoryArchivePlan,
): PreparedHistoryArchive => {
  verifyHistoryArchive(state, plan);
  state.rows.forEach((row: HistorySourceRow): void => assertScope(row, plan));
  const partial: readonly HistorySourceRow[] = state.rows.filter(isSourcePartial);
  const eligible: readonly HistorySourceRow[] = state.rows.filter(
    (row: HistorySourceRow): boolean => !isSourcePartial(row),
  );
  const distinct: Map<string, HistorySourceRow> = new Map();
  eligible.forEach((row: HistorySourceRow): void => {
    const key: string = naturalKey(row);
    const existing: HistorySourceRow | undefined = distinct.get(key);
    if (existing !== undefined && historyRowFingerprint(existing) !== historyRowFingerprint(row)) {
      throw new Error("History archive contains conflicting records for one natural key.");
    }
    distinct.set(key, row);
  });
  const input: HistoryPublicationInput = historyRowsToInput([...distinct.values()]);
  // Every other malformed record fails preparation. Only observed missing
  // runner/finish fields remain explicitly source-partial in the durable archive.
  buildHistoryStatements(input);
  return {
    input,
    archivedRows: state.rows.length,
    publishedCount: state.publishedCount,
    sourceComplete: state.checkpoint.pendingUrl === null && eligible.length === distinct.size,
    sourcePartialRows: partial,
    duplicateRows: eligible.length - distinct.size,
  };
};
