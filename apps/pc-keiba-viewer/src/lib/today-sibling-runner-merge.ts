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

// race_entry_snapshots stores horse_number unpadded ("1"..."16") while jvd_se /
// nvd_se stores umaban zero-padded ("01"..."16"). Without normalisation the
// merge key collides for umaban >= 10 only, leaving 1-9 horses with null
// trainer / wakuban. Normalising both sides to the parsed-integer form
// removes the padding asymmetry without altering the externally visible
// umaban string on either record. Whitespace inputs are tolerated because
// snapshot values occasionally carry stray padding from upstream feeds.
const normalizeUmabanForKey = (umaban: string): string => {
  const trimmed = umaban.trim();
  if (trimmed === "") return "";
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isFinite(parsed) ? String(parsed) : trimmed;
};

const buildEntryKey = (raceBango: string, umaban: string): string =>
  `${raceBango}:${normalizeUmabanForKey(umaban)}`;

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

// Tansho-odds enrichment mirrors the wakuban / trainer merge above but for
// the (race-trend DO) → viewer path. The DO state never carries
// tanshoOdds / tanshoPopularity because its self-pull SQL lives in
// REALTIME_DB while live odds land in REALTIME_HOT_DB (separate D1 binding).
// The viewer holds both bindings, so it enriches the do-hit rows in-place.
// Numeric inputs match the storage format produced by `toStarterRow` in the
// legacy D1 path: tanshoOddsTenth is the odds value * 10 padded to width 4,
// tanshoPopularity is the rank padded to width 2 — so the renderer can treat
// DO-hit and legacy rows identically.
export interface TanshoOddsEnrichmentEntry {
  raceKey: string;
  umaban: string;
  tanshoOddsTenth: number | null;
  tanshoPopularity: number | null;
}

const TANSHO_ODDS_WIDTH = 4;
const TANSHO_POPULARITY_WIDTH = 2;

const padNumericString = (value: number | null, width: number): string | null =>
  value === null ? null : String(value).padStart(width, "0");

const buildOddsKey = (raceKey: string, umaban: string): string =>
  `${raceKey}:${normalizeUmabanForKey(umaban)}`;

const buildStarterRaceKey = (row: RaceTrendStarterRow): string =>
  [row.source, row.kaisaiNen, row.kaisaiTsukihi, row.keibajoCode, row.raceBango].join(":");

const pickTanshoString = (
  current: string | null,
  incoming: number | null,
  width: number,
): string | null => {
  if (current !== null && current !== "") return current;
  return padNumericString(incoming, width);
};

export const mergeTanshoOddsEnrichment = (
  rows: ReadonlyArray<RaceTrendStarterRow>,
  entries: ReadonlyArray<TanshoOddsEnrichmentEntry>,
): RaceTrendStarterRow[] => {
  if (entries.length === 0) return rows.slice();
  const lookup = new Map<string, TanshoOddsEnrichmentEntry>();
  for (const entry of entries) {
    lookup.set(buildOddsKey(entry.raceKey, entry.umaban), entry);
  }
  return rows.map((row) => {
    const umaban = row.umaban;
    if (umaban === null || umaban === "") return row;
    const key = buildOddsKey(buildStarterRaceKey(row), umaban);
    const odds = lookup.get(key);
    if (!odds) return row;
    return {
      ...row,
      tanshoOdds: pickTanshoString(row.tanshoOdds, odds.tanshoOddsTenth, TANSHO_ODDS_WIDTH),
      tanshoPopularity: pickTanshoString(
        row.tanshoPopularity,
        odds.tanshoPopularity,
        TANSHO_POPULARITY_WIDTH,
      ),
    };
  });
};
