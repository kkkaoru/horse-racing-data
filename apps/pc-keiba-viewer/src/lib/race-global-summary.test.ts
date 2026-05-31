// Run with bun. Tests for race-global-summary helper. NOT DRY by design.

import { expect, it } from "vitest";

import { buildRaceGlobalSummaryItems, type RaceGlobalSummaryItem } from "./race-global-summary";

it("orders venue, race number, track surface, distance, condition for a typical JRA race", () => {
  const items = buildRaceGlobalSummaryItems({
    conditionLabel: "4歳以上 / 2勝クラス / 牝馬限定",
    keibajoCode: "08",
    kyori: "2000",
    raceNumber: "08",
    trackCode: "12",
  });
  expect(items).toStrictEqual([
    { className: null, key: "venue", text: "京都" },
    { className: null, key: "raceNumber", text: "8R" },
    { className: null, key: "trackSurface", text: "芝" },
    { className: null, key: "distance", text: "2000m" },
    {
      className: "race-global-summary-condition",
      key: "condition",
      text: "4歳以上 / 2勝クラス / 牝馬限定",
    },
  ]);
});

it("places track and distance directly after race number and before condition", () => {
  const items = buildRaceGlobalSummaryItems({
    conditionLabel: "3歳以上 / 1勝クラス",
    keibajoCode: "05",
    kyori: "1600",
    raceNumber: "11",
    trackCode: "23",
  });
  const keys = items.map((item: RaceGlobalSummaryItem) => item.key);
  expect(keys).toStrictEqual(["venue", "raceNumber", "trackSurface", "distance", "condition"]);
});

it("omits the condition item when the condition label is empty", () => {
  const items = buildRaceGlobalSummaryItems({
    conditionLabel: "",
    keibajoCode: "06",
    kyori: "1200",
    raceNumber: "01",
    trackCode: "10",
  });
  expect(items).toStrictEqual([
    { className: null, key: "venue", text: "中山" },
    { className: null, key: "raceNumber", text: "1R" },
    { className: null, key: "trackSurface", text: "芝" },
    { className: null, key: "distance", text: "1200m" },
  ]);
});

it("renders hyphen placeholders when track and distance are missing", () => {
  const items = buildRaceGlobalSummaryItems({
    conditionLabel: "未勝利",
    keibajoCode: "09",
    kyori: null,
    raceNumber: "03",
    trackCode: null,
  });
  expect(items).toStrictEqual([
    { className: null, key: "venue", text: "阪神" },
    { className: null, key: "raceNumber", text: "3R" },
    { className: null, key: "trackSurface", text: "-" },
    { className: null, key: "distance", text: "-" },
    { className: "race-global-summary-condition", key: "condition", text: "未勝利" },
  ]);
});
