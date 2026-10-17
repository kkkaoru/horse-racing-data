// bun で実行する (bunx vitest 経由)
import { expect, it } from "vitest";

import {
  HORSE_WEIGHT_CHANGE_CLASSES,
  getHorseWeightChangeClass,
  indexLiveHorseWeightChangeKg,
  parseHorseWeightChangeKg,
  resolveCurrentHorseWeightChangeKg,
} from "./horse-weight-change-class";

it("lists the nine weight-change classes from the largest gain to the largest loss", () => {
  expect(HORSE_WEIGHT_CHANGE_CLASSES.map((changeClass) => changeClass.label)).toStrictEqual([
    "+20kg以上",
    "+20kg未満+10kg以上",
    "+10kg未満+5kg以上",
    "+5kg未満+2kg以上",
    "+2kg未満-2kg以上",
    "-2kg未満-5kg以上",
    "-5kg未満-10kg以上",
    "-10kg未満-20kg以上",
    "-20kg未満",
  ]);
});

it("places each boundary in the class whose lower bound it meets", () => {
  expect(getHorseWeightChangeClass(20).key).toBe("plus20");
  expect(getHorseWeightChangeClass(19).key).toBe("plus10");
  expect(getHorseWeightChangeClass(10).key).toBe("plus10");
  expect(getHorseWeightChangeClass(9).key).toBe("plus5");
  expect(getHorseWeightChangeClass(5).key).toBe("plus5");
  expect(getHorseWeightChangeClass(4).key).toBe("plus2");
  expect(getHorseWeightChangeClass(2).key).toBe("plus2");
  expect(getHorseWeightChangeClass(1).key).toBe("even");
  expect(getHorseWeightChangeClass(0).key).toBe("even");
  expect(getHorseWeightChangeClass(-2).key).toBe("even");
  expect(getHorseWeightChangeClass(-3).key).toBe("minus2");
  expect(getHorseWeightChangeClass(-5).key).toBe("minus2");
  expect(getHorseWeightChangeClass(-6).key).toBe("minus5");
  expect(getHorseWeightChangeClass(-10).key).toBe("minus5");
  expect(getHorseWeightChangeClass(-11).key).toBe("minus10");
  expect(getHorseWeightChangeClass(-20).key).toBe("minus10");
  expect(getHorseWeightChangeClass(-21).key).toBe("minus20");
});

it("parses decimal changes with their sign and treats 000 as no change", () => {
  expect(parseHorseWeightChangeKg({ keibajoCode: "05", zogenFugo: "+", zogenSa: "012" })).toBe(12);
  expect(parseHorseWeightChangeKg({ keibajoCode: "05", zogenFugo: "-", zogenSa: "004" })).toBe(-4);
  expect(parseHorseWeightChangeKg({ keibajoCode: "44", zogenFugo: " ", zogenSa: "000" })).toBe(0);
  expect(parseHorseWeightChangeKg({ keibajoCode: "44", zogenFugo: null, zogenSa: "6" })).toBe(6);
});

it("rejects decimal debut, unmeasured, and malformed changes", () => {
  expect(parseHorseWeightChangeKg({ keibajoCode: "05", zogenFugo: " ", zogenSa: "   " })).toBe(
    null,
  );
  expect(parseHorseWeightChangeKg({ keibajoCode: "05", zogenFugo: " ", zogenSa: "999" })).toBe(
    null,
  );
  expect(parseHorseWeightChangeKg({ keibajoCode: "05", zogenFugo: "+", zogenSa: "00A" })).toBe(
    null,
  );
  expect(parseHorseWeightChangeKg({ keibajoCode: "05", zogenFugo: "+", zogenSa: "1234" })).toBe(
    null,
  );
  expect(parseHorseWeightChangeKg({ keibajoCode: "05", zogenFugo: "+", zogenSa: null })).toBe(null);
});

it("parses Ban-ei hex changes and rejects FFF and blank", () => {
  expect(parseHorseWeightChangeKg({ keibajoCode: "83", zogenFugo: "+", zogenSa: "00A" })).toBe(10);
  expect(parseHorseWeightChangeKg({ keibajoCode: "83", zogenFugo: "-", zogenSa: "01f" })).toBe(-31);
  expect(parseHorseWeightChangeKg({ keibajoCode: "83", zogenFugo: " ", zogenSa: "000" })).toBe(0);
  expect(parseHorseWeightChangeKg({ keibajoCode: "83", zogenFugo: " ", zogenSa: "FFF" })).toBe(
    null,
  );
  expect(parseHorseWeightChangeKg({ keibajoCode: "83", zogenFugo: " ", zogenSa: "   " })).toBe(
    null,
  );
  expect(parseHorseWeightChangeKg({ keibajoCode: "83", zogenFugo: "+", zogenSa: "0G1" })).toBe(
    null,
  );
});

it("indexes live changes and skips scratches, debuts, and undisplayable numbers", () => {
  expect(
    indexLiveHorseWeightChangeKg([
      { changeAmount: 6, changeSign: "+", horseNumber: "01", weight: 480 },
      { changeAmount: 4, changeSign: "-", horseNumber: "2", weight: 452 },
      { changeAmount: 0, changeSign: null, horseNumber: "3", weight: 500 },
      { changeAmount: null, changeSign: null, horseNumber: "4", weight: 470 },
      { horseNumber: "5", weight: 466 },
      { changeAmount: 8, changeSign: "+", horseNumber: "6", weight: null },
      { changeAmount: Number.NaN, changeSign: "+", horseNumber: "7", weight: 490 },
      { changeAmount: 2, changeSign: "+", horseNumber: "", weight: 490 },
    ]),
  ).toStrictEqual(
    new Map([
      ["1", 6],
      ["2", -4],
      ["3", 0],
    ]),
  );
});

it("prefers the live change and falls back to the runner zogen fields", () => {
  expect(
    resolveCurrentHorseWeightChangeKg({
      horseNumber: "01",
      keibajoCode: "05",
      liveWeightChangeKgByHorse: new Map([["1", -8]]),
      zogenFugo: "+",
      zogenSa: "002",
    }),
  ).toBe(-8);
  expect(
    resolveCurrentHorseWeightChangeKg({
      horseNumber: "02",
      keibajoCode: "05",
      liveWeightChangeKgByHorse: new Map([["1", -8]]),
      zogenFugo: "+",
      zogenSa: "002",
    }),
  ).toBe(2);
  expect(
    resolveCurrentHorseWeightChangeKg({
      horseNumber: "02",
      keibajoCode: "05",
      liveWeightChangeKgByHorse: undefined,
      zogenFugo: " ",
      zogenSa: "   ",
    }),
  ).toBeNull();
});
