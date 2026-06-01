// Run with: bunx vitest run src/lib/race-trend-display.test.ts
import { expect, test } from "vitest";

import { shouldRestrictTrendDisplayToToday } from "./race-trend-display";

test("shouldRestrictTrendDisplayToToday returns true for race-bango 02", () => {
  expect(shouldRestrictTrendDisplayToToday({ raceBango: "02" })).toStrictEqual(true);
});

test("shouldRestrictTrendDisplayToToday returns false for race-bango 01", () => {
  expect(shouldRestrictTrendDisplayToToday({ raceBango: "01" })).toStrictEqual(false);
});

test("shouldRestrictTrendDisplayToToday returns true for race-bango 2 without zero padding", () => {
  expect(shouldRestrictTrendDisplayToToday({ raceBango: "2" })).toStrictEqual(true);
});

test("shouldRestrictTrendDisplayToToday returns false for race-bango 1 without zero padding", () => {
  expect(shouldRestrictTrendDisplayToToday({ raceBango: "1" })).toStrictEqual(false);
});

test("shouldRestrictTrendDisplayToToday returns true for mid-card race-bango 05", () => {
  expect(shouldRestrictTrendDisplayToToday({ raceBango: "05" })).toStrictEqual(true);
});

test("shouldRestrictTrendDisplayToToday returns true for late race-bango 11", () => {
  expect(shouldRestrictTrendDisplayToToday({ raceBango: "11" })).toStrictEqual(true);
});

test("shouldRestrictTrendDisplayToToday returns true for double-digit race-bango 12", () => {
  expect(shouldRestrictTrendDisplayToToday({ raceBango: "12" })).toStrictEqual(true);
});

test("shouldRestrictTrendDisplayToToday returns false for a non-numeric race-bango", () => {
  expect(shouldRestrictTrendDisplayToToday({ raceBango: "abc" })).toStrictEqual(false);
});

test("shouldRestrictTrendDisplayToToday returns false for an empty race-bango", () => {
  expect(shouldRestrictTrendDisplayToToday({ raceBango: "" })).toStrictEqual(false);
});

test("shouldRestrictTrendDisplayToToday returns false for race-bango 00", () => {
  expect(shouldRestrictTrendDisplayToToday({ raceBango: "00" })).toStrictEqual(false);
});
