// Run with bun. Pure helpers that turn the D1 `race_result_snapshots` finish
// rows into a horse-number -> 着順 lookup for the runners table. Kept free of
// any I/O so they stay unit-testable; the D1 read lives in
// ../db/race-finish-d1.server.ts.

import { formatRunnerNumber } from "./runner-format";

interface FinishPositionEntry {
  finishPosition: string;
  horseNumber: string;
}

const ALL_ZERO_PATTERN = /^0+$/;
const DECIMAL_RADIX = 10;
const INVALID_HORSE_NUMBER = "-";

// Normalize a raw D1 finish_position string for the 着順 column. The snapshot
// writer stores the scraped TEXT verbatim, so empty / all-zero placeholders
// (NAR persists only top-3 finishers -> non-placed horses arrive as "0") and
// non-numeric statuses (e.g. "中止") must collapse to null so the table falls
// through to the PG value instead of rendering a meaningless rank.
export const normalizeD1FinishPosition = (value: string | null | undefined): string | null => {
  if (value === null || value === undefined) return null;
  const trimmed = value.trim();
  if (trimmed === "" || ALL_ZERO_PATTERN.test(trimmed)) return null;
  const parsed = Number.parseInt(trimmed, DECIMAL_RADIX);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return trimmed;
};

// Build the horse-number -> normalized-finish map. Keys match the runners
// table's `formatRunnerNumber(runner.umaban)` lookup so a stored "01" / " 1 "
// resolves to the same "1" key. Entries with an unparseable horse number or a
// null-normalized finish are dropped, leaving only real placed finishes.
export const buildD1FinishMap = (
  entries: ReadonlyArray<FinishPositionEntry>,
): Map<string, string> =>
  entries.reduce((map, entry) => {
    const horseNumber = formatRunnerNumber(entry.horseNumber);
    const finishPosition = normalizeD1FinishPosition(entry.finishPosition);
    if (horseNumber === INVALID_HORSE_NUMBER || finishPosition === null) return map;
    map.set(horseNumber, finishPosition);
    return map;
  }, new Map<string, string>());
