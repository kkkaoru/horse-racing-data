// Run with bun: bunx vitest run src/lib/runner-finish-position.test.ts
import { expect, test } from "vitest";

import { pickFinishPosition } from "./runner-finish-position";

test("pickFinishPosition returns trend value when entry is 00 and trend is 3", () => {
  expect(pickFinishPosition({ entryValue: "00", trendValue: 3 })).toStrictEqual("3");
});

test("pickFinishPosition returns entry value when trend is null and entry is 05", () => {
  expect(pickFinishPosition({ entryValue: "05", trendValue: null })).toStrictEqual("5");
});

test("pickFinishPosition returns null when both entry and trend are unavailable", () => {
  expect(pickFinishPosition({ entryValue: null, trendValue: null })).toStrictEqual(null);
});

test("pickFinishPosition prefers entry value when trend is zero", () => {
  expect(pickFinishPosition({ entryValue: "07", trendValue: 0 })).toStrictEqual("7");
});
