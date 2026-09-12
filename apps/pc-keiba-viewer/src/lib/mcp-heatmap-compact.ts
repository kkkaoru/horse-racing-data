// Run with bun run test / bun run tsc.
import type { WinRateHeatmapCell, WinRateHeatmapRow } from "./win-rate-heatmap";

export interface CompactHeatmapOptions {
  horseNumbers: readonly string[] | null;
  offset: number;
  limit: number | null;
}

export type CompactHeatmapCell = Pick<
  WinRateHeatmapCell,
  "name" | "starts" | "winRate" | "quinellaRate" | "showRate"
>;

export interface CompactHeatmapRow {
  horseNumber: string;
  horseName: string;
  heatmap: Record<string, CompactHeatmapCell>;
}

export interface CompactHeatmapPage {
  rows: CompactHeatmapRow[];
  total: number;
  offset: number;
  nextOffset: number | null;
}

export const MAX_COMPACT_HEATMAP_LIMIT: number = 99;
const FIRST_OFFSET: number = 0;
const DEFAULT_COMPACT_HEATMAP_LIMIT: number = 1;
const HORSE_NUMBER_PATTERN: RegExp = /^(?:0?[1-9]|[1-9]\d)$/;

const isHorseNumber = (value: unknown): value is string =>
  typeof value === "string" && HORSE_NUMBER_PATTERN.test(value);

export const parseCompactHeatmapOptions = (
  args: Record<string, unknown>,
): CompactHeatmapOptions | string => {
  const horseNumbers: unknown = args.horseNumbers;
  if (
    horseNumbers !== undefined &&
    (!Array.isArray(horseNumbers) ||
      horseNumbers.length === 0 ||
      !horseNumbers.every(isHorseNumber))
  ) {
    return "horseNumbers must be a non-empty array of horse number strings from 1 to 99";
  }
  const offset: unknown = args.offset === undefined ? FIRST_OFFSET : args.offset;
  if (typeof offset !== "number" || !Number.isSafeInteger(offset) || offset < FIRST_OFFSET) {
    return "offset must be a non-negative safe integer";
  }
  const limit: unknown = args.limit;
  if (
    limit !== undefined &&
    (typeof limit !== "number" ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > MAX_COMPACT_HEATMAP_LIMIT)
  ) {
    return "limit must be an integer from 1 to 99";
  }
  return {
    horseNumbers: Array.isArray(horseNumbers)
      ? [...new Set(horseNumbers.map((number: string) => String(Number(number))))]
      : null,
    offset,
    limit: typeof limit === "number" ? limit : DEFAULT_COMPACT_HEATMAP_LIMIT,
  };
};

const compactCell = ([key, cell]: [string, WinRateHeatmapCell]): [string, CompactHeatmapCell] => [
  key,
  {
    name: cell.name,
    starts: cell.starts,
    winRate: cell.winRate,
    quinellaRate: cell.quinellaRate,
    showRate: cell.showRate,
  },
];

const compactRow = (row: WinRateHeatmapRow): CompactHeatmapRow => ({
  horseNumber: row.horseNumber,
  horseName: row.horseName,
  heatmap: Object.fromEntries(Object.entries(row.cells).map(compactCell)),
});

export const buildCompactHeatmap = (
  rows: readonly WinRateHeatmapRow[],
  options: CompactHeatmapOptions,
): CompactHeatmapPage | string => {
  const horseNumbers: readonly string[] | null = options.horseNumbers;
  if (horseNumbers?.some((number) => !rows.some((row) => row.horseNumber === number))) {
    return "horseNumbers contains a horse not present in this race";
  }
  // Select after building all rows so pooled statistics retain the same inputs as the UI.
  const selected: readonly WinRateHeatmapRow[] =
    horseNumbers === null ? rows : rows.filter((row) => horseNumbers.includes(row.horseNumber));
  const end: number = options.limit === null ? selected.length : options.offset + options.limit;
  return {
    rows: selected.slice(options.offset, end).map(compactRow),
    total: selected.length,
    offset: options.offset,
    nextOffset: end < selected.length ? end : null,
  };
};
