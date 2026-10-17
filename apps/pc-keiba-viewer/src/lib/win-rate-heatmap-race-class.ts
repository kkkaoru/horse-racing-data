// Run with bun (bunx vitest).
import type { RaceListItem } from "./race-types";
import {
  winRateHeatmapBackground,
  winRateHeatmapForeground,
  type WinRateHeatmapDisplayModel,
  type WinRateHeatmapDisplaySwatch,
  type WinRateHeatmapMetricKey,
} from "./win-rate-heatmap";

export type WinRateHeatmapRaceClass = "debut" | "maiden" | "other";

export type WinRateHeatmapRaceClassSource = Pick<
  RaceListItem,
  "kyosoJokenCode" | "kyosoJokenMeisho" | "kyosomeiHondai"
>;

const JRA_CONDITION_RACE_CLASSES: ReadonlyMap<string, WinRateHeatmapRaceClass> = new Map([
  ["701", "debut"],
  ["703", "maiden"],
]);
const BLANK_SWATCH_HUE = 0;
const WHITESPACE_PATTERN = /[\s　]/gu;

// Columns whose history is the runner's own career. A 新馬 runner has none,
// and a 未勝利 runner has never won, so its win rate is always zero.
const HORSE_CAREER_COLUMN_KEYS: ReadonlySet<WinRateHeatmapMetricKey> = new Set([
  "horse",
  "horseJockey",
]);

// JRA marks the class with a condition code. NAR leaves it at 000 and writes
// the class into the condition name (２歳　新馬) or the title (２歳新馬).
export const getWinRateHeatmapRaceClass = (
  race: WinRateHeatmapRaceClassSource,
): WinRateHeatmapRaceClass => {
  const coded = JRA_CONDITION_RACE_CLASSES.get(race.kyosoJokenCode?.trim() ?? "");
  if (coded !== undefined) return coded;
  const text = `${race.kyosoJokenMeisho ?? ""}${race.kyosomeiHondai ?? ""}`.replaceAll(
    WHITESPACE_PATTERN,
    "",
  );
  if (text.includes("新馬")) return "debut";
  if (text.includes("未勝利")) return "maiden";
  return "other";
};

const blankSwatch = (
  swatch: WinRateHeatmapDisplaySwatch,
  display: WinRateHeatmapDisplayModel,
): WinRateHeatmapDisplaySwatch => {
  const scale = display.colorScales[swatch.metricKey];
  const fill = {
    hue: BLANK_SWATCH_HUE,
    maxRate: scale.maxRate,
    minRate: scale.minRate,
    rate: null,
  };
  return {
    ...swatch,
    background: winRateHeatmapBackground(fill),
    foreground: winRateHeatmapForeground(fill),
    graphStartsLabel: null,
    isZeroGraphStarts: false,
    isZeroValue: false,
    startsLabel: null,
    valueLabel: "",
  };
};

// 新馬: the horse columns are removed in every view. 未勝利: they stay, but
// show nothing in the 勝率 view; 連対率, 複勝率 and the combined view keep values.
export const applyWinRateHeatmapRaceClass = (
  display: WinRateHeatmapDisplayModel,
  raceClass: WinRateHeatmapRaceClass,
): WinRateHeatmapDisplayModel => {
  if (raceClass === "debut") {
    return {
      ...display,
      rows: display.rows.map((row) => ({
        ...row,
        swatches: row.swatches.filter((swatch) => !HORSE_CAREER_COLUMN_KEYS.has(swatch.columnKey)),
      })),
      visibleColumns: display.visibleColumns.filter(
        (column) => !HORSE_CAREER_COLUMN_KEYS.has(column.key),
      ),
    };
  }
  if (raceClass === "maiden" && display.viewMode === "winRate") {
    return {
      ...display,
      rows: display.rows.map((row) => ({
        ...row,
        swatches: row.swatches.map((swatch) =>
          HORSE_CAREER_COLUMN_KEYS.has(swatch.columnKey) ? blankSwatch(swatch, display) : swatch,
        ),
      })),
    };
  }
  return display;
};
