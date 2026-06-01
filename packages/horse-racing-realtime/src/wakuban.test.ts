// run with: bun run test
import { expect, it } from "vitest";

import { deriveWakuban, deriveWakubanString } from "./wakuban";

it("derive-wakuban-shussotosu-8-umaban-3-returns-3", () => {
  expect(deriveWakuban({ horseCount: 8, horseNumber: 3 })).toBe(3);
});

it("derive-wakuban-shussotosu-8-umaban-1-returns-1", () => {
  expect(deriveWakuban({ horseCount: 8, horseNumber: 1 })).toBe(1);
});

it("derive-wakuban-shussotosu-8-umaban-8-returns-8", () => {
  expect(deriveWakuban({ horseCount: 8, horseNumber: 8 })).toBe(8);
});

it("derive-wakuban-shussotosu-9-umaban-8-returns-8", () => {
  expect(deriveWakuban({ horseCount: 9, horseNumber: 8 })).toBe(8);
});

it("derive-wakuban-shussotosu-9-umaban-9-returns-8", () => {
  expect(deriveWakuban({ horseCount: 9, horseNumber: 9 })).toBe(8);
});

it("derive-wakuban-shussotosu-10-umaban-7-returns-7", () => {
  expect(deriveWakuban({ horseCount: 10, horseNumber: 7 })).toBe(7);
});

it("derive-wakuban-shussotosu-10-umaban-8-returns-7", () => {
  expect(deriveWakuban({ horseCount: 10, horseNumber: 8 })).toBe(7);
});

it("derive-wakuban-shussotosu-10-umaban-9-returns-8", () => {
  expect(deriveWakuban({ horseCount: 10, horseNumber: 9 })).toBe(8);
});

it("derive-wakuban-shussotosu-10-umaban-10-returns-8", () => {
  expect(deriveWakuban({ horseCount: 10, horseNumber: 10 })).toBe(8);
});

it("derive-wakuban-shussotosu-12-umaban-5-returns-5", () => {
  expect(deriveWakuban({ horseCount: 12, horseNumber: 5 })).toBe(5);
});

it("derive-wakuban-shussotosu-12-umaban-10-returns-7", () => {
  expect(deriveWakuban({ horseCount: 12, horseNumber: 10 })).toBe(7);
});

it("derive-wakuban-shussotosu-16-umaban-1-returns-1", () => {
  expect(deriveWakuban({ horseCount: 16, horseNumber: 1 })).toBe(1);
});

it("derive-wakuban-shussotosu-16-umaban-16-returns-8", () => {
  expect(deriveWakuban({ horseCount: 16, horseNumber: 16 })).toBe(8);
});

it("derive-wakuban-shussotosu-18-umaban-13-returns-7", () => {
  expect(deriveWakuban({ horseCount: 18, horseNumber: 13 })).toBe(7);
});

it("derive-wakuban-shussotosu-18-umaban-18-returns-8", () => {
  expect(deriveWakuban({ horseCount: 18, horseNumber: 18 })).toBe(8);
});

it("derive-wakuban-empty-shussotosu-returns-null", () => {
  expect(deriveWakuban({ horseCount: 0, horseNumber: 1 })).toBe(null);
});

it("derive-wakuban-too-large-shussotosu-returns-null", () => {
  expect(deriveWakuban({ horseCount: 19, horseNumber: 1 })).toBe(null);
});

it("derive-wakuban-umaban-zero-returns-null", () => {
  expect(deriveWakuban({ horseCount: 10, horseNumber: 0 })).toBe(null);
});

it("derive-wakuban-umaban-exceeds-horse-count-returns-null", () => {
  expect(deriveWakuban({ horseCount: 10, horseNumber: 11 })).toBe(null);
});

it("derive-wakuban-non-integer-umaban-returns-null", () => {
  expect(deriveWakuban({ horseCount: 10, horseNumber: 3.5 })).toBe(null);
});

it("derive-wakuban-non-integer-horse-count-returns-null", () => {
  expect(deriveWakuban({ horseCount: 10.5, horseNumber: 3 })).toBe(null);
});

it("derive-wakuban-nan-umaban-returns-null", () => {
  expect(deriveWakuban({ horseCount: 10, horseNumber: Number.NaN })).toBe(null);
});

it("derive-wakuban-nan-horse-count-returns-null", () => {
  expect(deriveWakuban({ horseCount: Number.NaN, horseNumber: 3 })).toBe(null);
});

it("derive-wakuban-negative-umaban-returns-null", () => {
  expect(deriveWakuban({ horseCount: 10, horseNumber: -1 })).toBe(null);
});

it("derive-wakuban-single-horse-returns-1", () => {
  expect(deriveWakuban({ horseCount: 1, horseNumber: 1 })).toBe(1);
});

it("derive-wakuban-string-shussotosu-12-umaban-5-returns-5", () => {
  expect(deriveWakubanString({ horseCount: 12, horseNumber: 5 })).toBe("5");
});

it("derive-wakuban-string-shussotosu-16-umaban-16-returns-8", () => {
  expect(deriveWakubanString({ horseCount: 16, horseNumber: 16 })).toBe("8");
});

it("derive-wakuban-string-empty-shussotosu-returns-null", () => {
  expect(deriveWakubanString({ horseCount: 0, horseNumber: 1 })).toBe(null);
});

it("derive-wakuban-string-nar-shussotosu-16-umaban-10-returns-5", () => {
  expect(deriveWakubanString({ horseCount: 16, horseNumber: 10 })).toBe("5");
});
