// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  getHorseWeightClass,
  HORSE_WEIGHT_CLASSES,
  indexLiveHorseWeightKg,
  parseHorseWeightKg,
  resolveCurrentHorseWeightKg,
} from "./horse-weight-class";

it("defines nine 20kg horse-weight classes from 399kg and below through 540kg and above", () => {
  expect(HORSE_WEIGHT_CLASSES).toStrictEqual([
    { key: "le399", label: "399kg以下", maxKg: 400, minKg: null },
    { key: "400-419", label: "400-419kg", maxKg: 420, minKg: 400 },
    { key: "420-439", label: "420-439kg", maxKg: 440, minKg: 420 },
    { key: "440-459", label: "440-459kg", maxKg: 460, minKg: 440 },
    { key: "460-479", label: "460-479kg", maxKg: 480, minKg: 460 },
    { key: "480-499", label: "480-499kg", maxKg: 500, minKg: 480 },
    { key: "500-519", label: "500-519kg", maxKg: 520, minKg: 500 },
    { key: "520-539", label: "520-539kg", maxKg: 540, minKg: 520 },
    { key: "ge540", label: "540kg以上", maxKg: null, minKg: 540 },
  ]);
});

it("parses decimal JRA and NAR horse weights and Ban-ei hex weights", () => {
  expect(parseHorseWeightKg({ bataiju: "480", keibajoCode: "05" })).toBe(480);
  expect(parseHorseWeightKg({ bataiju: "4AE", keibajoCode: "83" })).toBe(1198);
});

it("treats blank, 000, FFF, and non-positive horse weights as missing", () => {
  expect(parseHorseWeightKg({ bataiju: null, keibajoCode: "05" })).toBe(null);
  expect(parseHorseWeightKg({ bataiju: " ", keibajoCode: "05" })).toBe(null);
  expect(parseHorseWeightKg({ bataiju: "000", keibajoCode: "05" })).toBe(null);
  expect(parseHorseWeightKg({ bataiju: "FFF", keibajoCode: "05" })).toBe(null);
  expect(parseHorseWeightKg({ bataiju: "fff", keibajoCode: "83" })).toBe(null);
  expect(parseHorseWeightKg({ bataiju: "0", keibajoCode: "05" })).toBe(null);
  expect(parseHorseWeightKg({ bataiju: "abc", keibajoCode: "05" })).toBe(null);
});

it("classifies horse weights on the inclusive 20kg class boundaries", () => {
  expect(getHorseWeightClass(399)).toStrictEqual({
    key: "le399",
    label: "399kg以下",
    maxKg: 400,
    minKg: null,
  });
  expect(getHorseWeightClass(400)).toStrictEqual({
    key: "400-419",
    label: "400-419kg",
    maxKg: 420,
    minKg: 400,
  });
  expect(getHorseWeightClass(419)).toStrictEqual({
    key: "400-419",
    label: "400-419kg",
    maxKg: 420,
    minKg: 400,
  });
  expect(getHorseWeightClass(420)).toStrictEqual({
    key: "420-439",
    label: "420-439kg",
    maxKg: 440,
    minKg: 420,
  });
  expect(getHorseWeightClass(440)).toStrictEqual({
    key: "440-459",
    label: "440-459kg",
    maxKg: 460,
    minKg: 440,
  });
  expect(getHorseWeightClass(460)).toStrictEqual({
    key: "460-479",
    label: "460-479kg",
    maxKg: 480,
    minKg: 460,
  });
  expect(getHorseWeightClass(480)).toStrictEqual({
    key: "480-499",
    label: "480-499kg",
    maxKg: 500,
    minKg: 480,
  });
  expect(getHorseWeightClass(500)).toStrictEqual({
    key: "500-519",
    label: "500-519kg",
    maxKg: 520,
    minKg: 500,
  });
  expect(getHorseWeightClass(520)).toStrictEqual({
    key: "520-539",
    label: "520-539kg",
    maxKg: 540,
    minKg: 520,
  });
  expect(getHorseWeightClass(539)).toStrictEqual({
    key: "520-539",
    label: "520-539kg",
    maxKg: 540,
    minKg: 520,
  });
  expect(getHorseWeightClass(540)).toStrictEqual({
    key: "ge540",
    label: "540kg以上",
    maxKg: null,
    minKg: 540,
  });
  expect(getHorseWeightClass(Number.NaN)).toStrictEqual({
    key: "ge540",
    label: "540kg以上",
    maxKg: null,
    minKg: 540,
  });
});

it("indexes live horse weights by display horse number and skips missing values", () => {
  expect([
    ...indexLiveHorseWeightKg([
      { horseNumber: "01", weight: 485 },
      { horseNumber: "02", weight: null },
      { horseNumber: "03", weight: 0 },
      { horseNumber: "00", weight: 500 },
      { horseNumber: "2", weight: 460 },
    ]),
  ]).toStrictEqual([
    ["1", 485],
    ["2", 460],
  ]);
});

it("prefers a live kilogram weight over the stored bataiju string", () => {
  expect(
    resolveCurrentHorseWeightKg({
      bataiju: "410",
      horseNumber: "01",
      keibajoCode: "05",
      liveWeightKgByHorse: new Map([["1", 485]]),
    }),
  ).toBe(485);
  expect(
    resolveCurrentHorseWeightKg({
      bataiju: "480",
      horseNumber: "01",
      keibajoCode: "05",
      liveWeightKgByHorse: new Map(),
    }),
  ).toBe(480);
});
