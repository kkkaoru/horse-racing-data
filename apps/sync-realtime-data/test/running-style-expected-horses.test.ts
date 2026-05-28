import { describe, expect, test } from "vitest";

import {
  filterRunningStyleFeatureRowsByActiveEntries,
  resolveRunningStyleExpectedHorseCount,
} from "../src/running-style-expected-horses";

describe("running-style expected horses", () => {
  test("prefers active entry count over feature count", () => {
    expect(
      resolveRunningStyleExpectedHorseCount(14, {
        horses: [
          { horseNumber: "1", status: null },
          { horseNumber: "2", status: "出走取消" },
          { horseNumber: "3", status: null },
        ],
      }),
    ).toBe(2);
  });

  test("falls back to feature count when entry snapshot is missing", () => {
    expect(resolveRunningStyleExpectedHorseCount(12, null)).toBe(12);
  });

  test("falls back to feature count when entries exist but no horse is active", () => {
    expect(
      resolveRunningStyleExpectedHorseCount(12, {
        horses: [
          { horseNumber: "1", status: "出走取消" },
          { horseNumber: "2", status: "取消" },
        ],
      }),
    ).toBe(12);
  });

  test("filters feature rows to active horses only", () => {
    expect(
      filterRunningStyleFeatureRowsByActiveEntries([{ umaban: 1 }, { umaban: 2 }, { umaban: 3 }], {
        horses: [
          { horseNumber: "1", status: null },
          { horseNumber: "2", status: "出走取消" },
          { horseNumber: "3", status: null },
        ],
      }),
    ).toEqual([{ umaban: 1 }, { umaban: 3 }]);
  });
});
