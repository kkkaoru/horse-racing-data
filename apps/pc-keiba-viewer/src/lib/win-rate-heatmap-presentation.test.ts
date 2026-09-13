// Run with bun (bunx vitest).
import { expect, it } from "vitest";

import {
  buildHeatmapPresentation,
  heatmapDisplayKey,
  isHeatmapPresentation,
  selectHeatmapDisplay,
} from "./win-rate-heatmap-presentation";

it("warms every view, count and bloodline-pooling variant", () => {
  const presentation = buildHeatmapPresentation({
    bloodlineRows: [],
    frameStats: [],
    horseResults: [],
    keibajoCode: "06",
    liveWeightKgByHorse: new Map(),
    runners: [],
    similarRows: [],
  });
  expect(isHeatmapPresentation(presentation)).toBe(true);
  expect(presentation.version).toBe(1);
  expect(presentation.rows).toStrictEqual([]);
  expect(presentation.combinedBloodlineRows).toStrictEqual([]);
  expect(Object.keys(presentation.displays)).toStrictEqual([
    "winRate:0:0",
    "winRate:0:1",
    "winRate:1:0",
    "winRate:1:1",
    "quinellaRate:0:0",
    "quinellaRate:0:1",
    "quinellaRate:1:0",
    "quinellaRate:1:1",
    "showRate:0:0",
    "showRate:0:1",
    "showRate:1:0",
    "showRate:1:1",
    "all:0:0",
    "all:0:1",
    "all:1:0",
    "all:1:1",
  ]);
  expect(
    selectHeatmapDisplay(presentation, {
      showStarts: true,
      splitBloodlineLines: true,
      viewMode: "all",
    }) === presentation.displays["all:1:1"],
  ).toBe(true);
  expect(
    selectHeatmapDisplay(presentation, {
      showStarts: false,
      splitBloodlineLines: false,
      viewMode: "winRate",
    })?.viewMode,
  ).toBe("winRate");
});

it.each([
  null,
  [],
  {},
  { version: 2 },
  { version: 1 },
  { version: 1, rows: [] },
  { version: 1, rows: [], combinedBloodlineRows: [] },
  { version: 1, rows: [], combinedBloodlineRows: [], displays: [] },
  { version: 1, rows: [], combinedBloodlineRows: [], displays: { broken: null } },
  { version: 1, rows: [], combinedBloodlineRows: [], displays: { broken: {} } },
])("rejects malformed presentation envelopes: %j", (value) => {
  expect(isHeatmapPresentation(value)).toBe(false);
});

it("does not synthesize an unwarmed display variant", () => {
  expect(
    selectHeatmapDisplay(
      { version: 1, rows: [], combinedBloodlineRows: [], displays: {} },
      {
        showStarts: false,
        splitBloodlineLines: true,
        viewMode: "winRate",
      },
    ),
  ).toBeNull();
  expect(
    heatmapDisplayKey({ showStarts: false, splitBloodlineLines: true, viewMode: "winRate" }),
  ).toBe("winRate:0:1");
});
