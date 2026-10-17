// Run with bun (bunx vitest).
import { expect, it } from "vitest";

import type { WinRateHeatmapDisplayModel, WinRateHeatmapDisplaySwatch } from "./win-rate-heatmap";
import {
  applyWinRateHeatmapRaceClass,
  getWinRateHeatmapRaceClass,
} from "./win-rate-heatmap-race-class";

const swatch = (
  columnKey: WinRateHeatmapDisplaySwatch["columnKey"],
  metricKey: WinRateHeatmapDisplaySwatch["metricKey"],
): WinRateHeatmapDisplaySwatch => ({
  background: "hsl(1, 1%, 1%)",
  columnKey,
  columnLabel: columnKey,
  foreground: "#000",
  graphStartsLabel: "(4)",
  isZeroGraphStarts: true,
  isZeroValue: true,
  metricKey,
  metricLabel: metricKey,
  name: "Name",
  startsLabel: "(4)",
  valueLabel: "0.0",
});

const display = (viewMode: WinRateHeatmapDisplayModel["viewMode"]): WinRateHeatmapDisplayModel => ({
  colorScales: {
    quinellaRate: { maxRate: 40, minRate: 0, ticks: [] },
    showRate: { maxRate: 40, minRate: 0, ticks: [] },
    winRate: { maxRate: 40, minRate: 0, ticks: [] },
  },
  empty: false,
  entityColSpan: 1,
  rows: [
    {
      frameNumber: "1",
      horseName: "Horse",
      horseNumber: "1",
      swatches: [
        swatch("horse", viewMode === "all" ? "winRate" : viewMode),
        swatch("horseJockey", viewMode === "all" ? "winRate" : viewMode),
        swatch("jockey", viewMode === "all" ? "winRate" : viewMode),
      ],
    },
  ],
  showCarriedWeight: false,
  showWeight: false,
  viewMode,
  visibleColumns: [
    { key: "horse", label: "馬" },
    { key: "horseJockey", label: "馬×騎手" },
    { key: "jockey", label: "騎手" },
  ],
  visibleRateMetrics: [],
});

it("reads JRA debut and maiden condition codes", () => {
  expect(
    getWinRateHeatmapRaceClass({
      kyosoJokenCode: "701",
      kyosoJokenMeisho: null,
      kyosomeiHondai: null,
    }),
  ).toBe("debut");
  expect(
    getWinRateHeatmapRaceClass({
      kyosoJokenCode: " 703 ",
      kyosoJokenMeisho: null,
      kyosomeiHondai: null,
    }),
  ).toBe("maiden");
});

it("reads NAR classes from the condition name or the race title", () => {
  expect(
    getWinRateHeatmapRaceClass({
      kyosoJokenCode: "000",
      kyosoJokenMeisho: "２歳　　",
      kyosomeiHondai: "ホープフルデビュー　２歳新馬　　",
    }),
  ).toBe("debut");
  expect(
    getWinRateHeatmapRaceClass({
      kyosoJokenCode: "000",
      kyosoJokenMeisho: "２歳　新馬　　ア",
      kyosomeiHondai: "ドリームチャレンジ",
    }),
  ).toBe("debut");
  expect(
    getWinRateHeatmapRaceClass({
      kyosoJokenCode: "000",
      kyosoJokenMeisho: "４歳上Ｃ２－10　未勝利",
      kyosomeiHondai: null,
    }),
  ).toBe("maiden");
  expect(
    getWinRateHeatmapRaceClass({
      kyosoJokenCode: null,
      kyosoJokenMeisho: null,
      kyosomeiHondai: "Ｃ級未勝利",
    }),
  ).toBe("maiden");
});

it("treats every other race as an ordinary class", () => {
  expect(
    getWinRateHeatmapRaceClass({
      kyosoJokenCode: "005",
      kyosoJokenMeisho: "1勝クラス",
      kyosomeiHondai: "",
    }),
  ).toBe("other");
});

it("removes the horse columns from every debut view", () => {
  const result = applyWinRateHeatmapRaceClass(display("quinellaRate"), "debut");
  expect(result.visibleColumns).toStrictEqual([{ key: "jockey", label: "騎手" }]);
  expect(result.rows[0]?.swatches.map((item) => item.columnKey)).toStrictEqual(["jockey"]);
});

it("blanks the horse columns only in the maiden win-rate view", () => {
  const result = applyWinRateHeatmapRaceClass(display("winRate"), "maiden");
  expect(result.visibleColumns.map((column) => column.key)).toStrictEqual([
    "horse",
    "horseJockey",
    "jockey",
  ]);
  expect(
    result.rows[0]?.swatches.map((item) => [
      item.columnKey,
      item.valueLabel,
      item.startsLabel,
      item.graphStartsLabel,
      item.isZeroValue,
      item.isZeroGraphStarts,
      item.background,
    ]),
  ).toStrictEqual([
    ["horse", "", null, null, false, false, "hsl(0, 0%, 96%)"],
    ["horseJockey", "", null, null, false, false, "hsl(0, 0%, 96%)"],
    ["jockey", "0.0", "(4)", "(4)", true, true, "hsl(1, 1%, 1%)"],
  ]);
});

it("keeps maiden values in the place, show and combined views", () => {
  const quinella = display("quinellaRate");
  const show = display("showRate");
  const all = display("all");
  expect(applyWinRateHeatmapRaceClass(quinella, "maiden")).toBe(quinella);
  expect(applyWinRateHeatmapRaceClass(show, "maiden")).toBe(show);
  expect(applyWinRateHeatmapRaceClass(all, "maiden")).toBe(all);
});

it("leaves ordinary races untouched", () => {
  const input = display("winRate");
  expect(applyWinRateHeatmapRaceClass(input, "other")).toBe(input);
});
