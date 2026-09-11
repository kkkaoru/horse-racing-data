// Run with bun (exercised via `bunx vitest run`).
import { expect, it } from "vitest";

import { buildWeightFutanDiffRows } from "./weight-futan-diff";

it("computes Ban-ei hex horse-weight minus carried-weight in kilograms", () => {
  expect(
    buildWeightFutanDiffRows({
      keibajoCode: "83",
      runners: [{ bataiju: "3E8", futanJuryo: "262", umaban: "01" }],
    }),
  ).toStrictEqual([{ d: 390, f: 610, u: "1", w: 1000 }]);
});

it("decodes non-Ban-ei futan from 0.1kg units", () => {
  expect(
    buildWeightFutanDiffRows({
      keibajoCode: "05",
      runners: [{ bataiju: "480", futanJuryo: "550", umaban: "02" }],
    }),
  ).toStrictEqual([{ d: 425, f: 55, u: "2", w: 480 }]);
});

it("drops blank umaban and keeps nulls when a weight is missing", () => {
  expect(
    buildWeightFutanDiffRows({
      keibajoCode: "83",
      runners: [
        { bataiju: "3E8", futanJuryo: "262", umaban: "00" },
        { bataiju: "000", futanJuryo: "262", umaban: "03" },
        { bataiju: "3E8", futanJuryo: "FFF", umaban: "04" },
      ],
    }),
  ).toStrictEqual([
    { d: null, f: 610, u: "3", w: null },
    { d: null, f: null, u: "4", w: 1000 },
  ]);
});

it("treats non-Ban-ei 999 horse weight as unmeasured", () => {
  expect(
    buildWeightFutanDiffRows({
      keibajoCode: "05",
      runners: [{ bataiju: "999", futanJuryo: "550", umaban: "01" }],
    }),
  ).toStrictEqual([{ d: null, f: 55, u: "1", w: null }]);
});
