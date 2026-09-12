// Run with bun run test.
import { expect, it } from "vitest";

import { buildCompactHeatmap, parseCompactHeatmapOptions } from "./mcp-heatmap-compact";
import type { WinRateHeatmapRow } from "./win-rate-heatmap";
import { buildWinRateHeatmapRows } from "./win-rate-heatmap";

const emptyRows: WinRateHeatmapRow[] = buildWinRateHeatmapRows({
  bloodlineRows: [],
  frameStats: [],
  horseResults: [],
  keibajoCode: "09",
  liveWeightKgByHorse: new Map(),
  runners: [],
  similarRows: [],
});

it("defaults to all horses and no row limit", () => {
  expect(parseCompactHeatmapOptions({})).toStrictEqual({
    horseNumbers: null,
    offset: 0,
    limit: null,
  });
  expect(
    buildCompactHeatmap(emptyRows, { horseNumbers: null, offset: 0, limit: null }),
  ).toStrictEqual({
    rows: [],
    total: 0,
    offset: 0,
    nextOffset: null,
  });
});

it("normalizes duplicate and zero-padded horse numbers", () => {
  expect(
    parseCompactHeatmapOptions({ horseNumbers: ["02", "2", "1"], offset: 1, limit: 1 }),
  ).toStrictEqual({
    horseNumbers: ["2", "1"],
    offset: 1,
    limit: 1,
  });
});

it.each([null, [], "1", [1], ["0"], ["00"], ["100"], [" 1"], ["x"]])(
  "rejects invalid horse selections: %j",
  (horseNumbers) => {
    expect(parseCompactHeatmapOptions({ horseNumbers })).toBe(
      "horseNumbers must be a non-empty array of horse number strings from 1 to 99",
    );
  },
);

it.each([-1, 0.5, "1", null, Number.NaN, Number.POSITIVE_INFINITY])(
  "rejects invalid offsets: %j",
  (offset) => {
    expect(parseCompactHeatmapOptions({ offset })).toBe(
      "offset must be a non-negative safe integer",
    );
  },
);

it.each([0, -1, 0.5, "1", null, Number.POSITIVE_INFINITY, 100])(
  "rejects invalid limits: %j",
  (limit) => {
    expect(parseCompactHeatmapOptions({ limit })).toBe("limit must be an integer from 1 to 99");
  },
);
