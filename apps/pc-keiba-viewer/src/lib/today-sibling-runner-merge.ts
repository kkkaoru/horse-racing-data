// Run with bun. Pure helper that merges entry-master metadata (wakuban and
// trainer name from jvd_se / nvd_se) into the today-sibling RaceTrendStarterRow
// payload. The DO and legacy snapshot paths intentionally omit chokyoshiName
// (race_entry_snapshots has no trainer column) and derive wakuban from horse
// count, so a trend-section detail row for a sibling race renders "-" for
// both fields whenever the upstream data is absent. This helper supplies the
// missing values from the viewer-side Hyperdrive pool without touching the
// realtime D1 / DO paths.
import type { RaceTrendStarterRow } from "horse-racing-realtime/race-trend-daily-track-types";

export interface TodaySiblingRunnerEntry {
  raceBango: string;
  umaban: string;
  wakuban: string | null;
  chokyoshiName: string | null;
}

const buildEntryKey = (raceBango: string, umaban: string): string => `${raceBango}:${umaban}`;

export const buildTodaySiblingRunnerLookup = (
  entries: ReadonlyArray<TodaySiblingRunnerEntry>,
): Map<string, TodaySiblingRunnerEntry> => {
  const map = new Map<string, TodaySiblingRunnerEntry>();
  for (const entry of entries) {
    map.set(buildEntryKey(entry.raceBango, entry.umaban), entry);
  }
  return map;
};

const pickWakuban = (rowWakuban: string | null, entryWakuban: string | null): string | null => {
  if (rowWakuban !== null && rowWakuban !== "") return rowWakuban;
  return entryWakuban;
};

const pickChokyoshiName = (
  rowValue: string | null | undefined,
  entryValue: string | null,
): string | null => {
  if (typeof rowValue === "string" && rowValue !== "") return rowValue;
  return entryValue;
};

const mergeRowWithEntry = (
  row: RaceTrendStarterRow,
  entry: TodaySiblingRunnerEntry,
): RaceTrendStarterRow => ({
  ...row,
  wakuban: pickWakuban(row.wakuban, entry.wakuban),
  chokyoshiName: pickChokyoshiName(row.chokyoshiName, entry.chokyoshiName),
});

// Merge entry-master metadata into each today-sibling row by (raceBango,
// umaban). Rows without a matching entry pass through unchanged so a partial
// fetch (e.g. one sibling race missing from jvd_se yet) cannot blank out the
// existing DO-supplied wakuban.
export const mergeTodaySiblingRunnerData = (
  rows: ReadonlyArray<RaceTrendStarterRow>,
  entries: ReadonlyArray<TodaySiblingRunnerEntry>,
): RaceTrendStarterRow[] => {
  if (entries.length === 0) return rows.slice();
  const lookup = buildTodaySiblingRunnerLookup(entries);
  return rows.map((row) => {
    const umaban = row.umaban;
    if (umaban === null || umaban === "") return row;
    const entry = lookup.get(buildEntryKey(row.raceBango, umaban));
    return entry ? mergeRowWithEntry(row, entry) : row;
  });
};
