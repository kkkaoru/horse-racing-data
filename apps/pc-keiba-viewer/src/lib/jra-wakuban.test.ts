// Run with bun.
import { expect, it } from "vitest";

import { deriveJraWakuban } from "./jra-wakuban";

it("N=8 u=1 maps to wakuban 1", () => {
  expect(deriveJraWakuban({ horseCount: 8, horseNumber: 1 })).toBe(1);
});

it("N=8 u=4 maps to wakuban 4", () => {
  expect(deriveJraWakuban({ horseCount: 8, horseNumber: 4 })).toBe(4);
});

it("N=8 u=8 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 8, horseNumber: 8 })).toBe(8);
});

it("N=9 u=7 maps to wakuban 7", () => {
  expect(deriveJraWakuban({ horseCount: 9, horseNumber: 7 })).toBe(7);
});

it("N=9 u=8 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 9, horseNumber: 8 })).toBe(8);
});

it("N=9 u=9 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 9, horseNumber: 9 })).toBe(8);
});

it("N=10 u=7 maps to wakuban 7", () => {
  expect(deriveJraWakuban({ horseCount: 10, horseNumber: 7 })).toBe(7);
});

it("N=10 u=8 maps to wakuban 7", () => {
  expect(deriveJraWakuban({ horseCount: 10, horseNumber: 8 })).toBe(7);
});

it("N=10 u=9 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 10, horseNumber: 9 })).toBe(8);
});

it("N=10 u=10 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 10, horseNumber: 10 })).toBe(8);
});

it("N=11 u=6 maps to wakuban 6", () => {
  expect(deriveJraWakuban({ horseCount: 11, horseNumber: 6 })).toBe(6);
});

it("N=11 u=7 maps to wakuban 6", () => {
  expect(deriveJraWakuban({ horseCount: 11, horseNumber: 7 })).toBe(6);
});

it("N=11 u=8 maps to wakuban 7", () => {
  expect(deriveJraWakuban({ horseCount: 11, horseNumber: 8 })).toBe(7);
});

it("N=11 u=9 maps to wakuban 7", () => {
  expect(deriveJraWakuban({ horseCount: 11, horseNumber: 9 })).toBe(7);
});

it("N=11 u=10 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 11, horseNumber: 10 })).toBe(8);
});

it("N=11 u=11 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 11, horseNumber: 11 })).toBe(8);
});

it("N=12 u=4 maps to wakuban 4", () => {
  expect(deriveJraWakuban({ horseCount: 12, horseNumber: 4 })).toBe(4);
});

it("N=12 u=5 maps to wakuban 5", () => {
  expect(deriveJraWakuban({ horseCount: 12, horseNumber: 5 })).toBe(5);
});

it("N=12 u=6 maps to wakuban 5", () => {
  expect(deriveJraWakuban({ horseCount: 12, horseNumber: 6 })).toBe(5);
});

it("N=12 u=7 maps to wakuban 6", () => {
  expect(deriveJraWakuban({ horseCount: 12, horseNumber: 7 })).toBe(6);
});

it("N=12 u=8 maps to wakuban 6", () => {
  expect(deriveJraWakuban({ horseCount: 12, horseNumber: 8 })).toBe(6);
});

it("N=12 u=9 maps to wakuban 7", () => {
  expect(deriveJraWakuban({ horseCount: 12, horseNumber: 9 })).toBe(7);
});

it("N=12 u=10 maps to wakuban 7", () => {
  expect(deriveJraWakuban({ horseCount: 12, horseNumber: 10 })).toBe(7);
});

it("N=12 u=11 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 12, horseNumber: 11 })).toBe(8);
});

it("N=12 u=12 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 12, horseNumber: 12 })).toBe(8);
});

it("N=16 u=1 maps to wakuban 1", () => {
  expect(deriveJraWakuban({ horseCount: 16, horseNumber: 1 })).toBe(1);
});

it("N=16 u=2 maps to wakuban 1", () => {
  expect(deriveJraWakuban({ horseCount: 16, horseNumber: 2 })).toBe(1);
});

it("N=16 u=3 maps to wakuban 2", () => {
  expect(deriveJraWakuban({ horseCount: 16, horseNumber: 3 })).toBe(2);
});

it("N=16 u=8 maps to wakuban 4", () => {
  expect(deriveJraWakuban({ horseCount: 16, horseNumber: 8 })).toBe(4);
});

it("N=16 u=15 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 16, horseNumber: 15 })).toBe(8);
});

it("N=16 u=16 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 16, horseNumber: 16 })).toBe(8);
});

it("N=18 u=1 maps to wakuban 1", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 1 })).toBe(1);
});

it("N=18 u=2 maps to wakuban 1", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 2 })).toBe(1);
});

it("N=18 u=3 maps to wakuban 2", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 3 })).toBe(2);
});

it("N=18 u=4 maps to wakuban 2", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 4 })).toBe(2);
});

it("N=18 u=5 maps to wakuban 3", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 5 })).toBe(3);
});

it("N=18 u=6 maps to wakuban 3", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 6 })).toBe(3);
});

it("N=18 u=7 maps to wakuban 4", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 7 })).toBe(4);
});

it("N=18 u=8 maps to wakuban 4", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 8 })).toBe(4);
});

it("N=18 u=9 maps to wakuban 5", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 9 })).toBe(5);
});

it("N=18 u=10 maps to wakuban 5", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 10 })).toBe(5);
});

it("N=18 u=11 maps to wakuban 6", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 11 })).toBe(6);
});

it("N=18 u=12 maps to wakuban 6", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 12 })).toBe(6);
});

it("N=18 u=13 maps to wakuban 7", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 13 })).toBe(7);
});

it("N=18 u=14 maps to wakuban 7", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 14 })).toBe(7);
});

it("N=18 u=15 maps to wakuban 7", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 15 })).toBe(7);
});

it("N=18 u=16 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 16 })).toBe(8);
});

it("N=18 u=17 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 17 })).toBe(8);
});

it("N=18 u=18 maps to wakuban 8", () => {
  expect(deriveJraWakuban({ horseCount: 18, horseNumber: 18 })).toBe(8);
});

it("horseCount=0 returns null", () => {
  expect(deriveJraWakuban({ horseCount: 0, horseNumber: 1 })).toBe(null);
});

it("horseCount=19 (over JRA max) returns null", () => {
  expect(deriveJraWakuban({ horseCount: 19, horseNumber: 1 })).toBe(null);
});

it("horseNumber=0 returns null", () => {
  expect(deriveJraWakuban({ horseCount: 10, horseNumber: 0 })).toBe(null);
});

it("horseNumber greater than horseCount returns null", () => {
  expect(deriveJraWakuban({ horseCount: 10, horseNumber: 11 })).toBe(null);
});

it("non-integer horseNumber returns null", () => {
  expect(deriveJraWakuban({ horseCount: 10, horseNumber: 3.5 })).toBe(null);
});

it("non-integer horseCount returns null", () => {
  expect(deriveJraWakuban({ horseCount: 10.5, horseNumber: 3 })).toBe(null);
});

it("NaN horseNumber returns null", () => {
  expect(deriveJraWakuban({ horseCount: 10, horseNumber: Number.NaN })).toBe(null);
});

it("NaN horseCount returns null", () => {
  expect(deriveJraWakuban({ horseCount: Number.NaN, horseNumber: 3 })).toBe(null);
});

it("negative horseNumber returns null", () => {
  expect(deriveJraWakuban({ horseCount: 10, horseNumber: -1 })).toBe(null);
});

it("N=1 u=1 maps to wakuban 1 (lower boundary)", () => {
  expect(deriveJraWakuban({ horseCount: 1, horseNumber: 1 })).toBe(1);
});
