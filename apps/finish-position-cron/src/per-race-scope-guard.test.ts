// Run with bun. Unit tests for the production per-race-only scope guard.

import { expect, test } from "vitest";
import {
  hasRequiredPerRaceScope,
  hasValidPerRaceScope,
  normalizePerRaceScope,
  PER_RACE_SCOPE_INVALID_ERROR,
  PER_RACE_SCOPE_REQUIRED_ERROR,
} from "./per-race-scope-guard";

test("hasRequiredPerRaceScope accepts both keibajoCode and raceBango", () => {
  expect(hasRequiredPerRaceScope({ keibajoCode: "05", raceBango: "11" })).toBe(true);
});

test("hasRequiredPerRaceScope rejects missing keibajoCode", () => {
  expect(hasRequiredPerRaceScope({ raceBango: "11" })).toBe(false);
});

test("hasRequiredPerRaceScope rejects missing raceBango", () => {
  expect(hasRequiredPerRaceScope({ keibajoCode: "05" })).toBe(false);
});

test("hasRequiredPerRaceScope rejects both missing", () => {
  expect(hasRequiredPerRaceScope({})).toBe(false);
});

test("hasRequiredPerRaceScope rejects blank keibajoCode or raceBango", () => {
  expect(hasRequiredPerRaceScope({ keibajoCode: "   ", raceBango: "11" })).toBe(false);
  expect(hasRequiredPerRaceScope({ keibajoCode: "05", raceBango: "  " })).toBe(false);
});

test("PER_RACE_SCOPE_REQUIRED_ERROR is a stable non-empty message", () => {
  expect(PER_RACE_SCOPE_REQUIRED_ERROR.length).toBeGreaterThan(0);
  expect(PER_RACE_SCOPE_REQUIRED_ERROR).toContain("keibajoCode");
  expect(PER_RACE_SCOPE_REQUIRED_ERROR).toContain("raceBango");
});

test("hasValidPerRaceScope accepts canonical two-digit race targets", () => {
  expect(hasValidPerRaceScope({ keibajoCode: "01", raceBango: "01" })).toBe(true);
  expect(hasValidPerRaceScope({ keibajoCode: "83", raceBango: "12" })).toBe(true);
});

test("hasValidPerRaceScope rejects malformed and out-of-range race targets", () => {
  expect(hasValidPerRaceScope({ keibajoCode: "00", raceBango: "01" })).toBe(false);
  expect(hasValidPerRaceScope({ keibajoCode: "A6", raceBango: "01" })).toBe(false);
  expect(hasValidPerRaceScope({ keibajoCode: "01", raceBango: "00" })).toBe(false);
  expect(hasValidPerRaceScope({ keibajoCode: "01", raceBango: "13" })).toBe(false);
  expect(hasValidPerRaceScope({ keibajoCode: "01", raceBango: "12 nar:43:01" })).toBe(false);
});

test("normalizePerRaceScope zero-pads backward-compatible one-digit codes", () => {
  expect(normalizePerRaceScope({ keibajoCode: "5", raceBango: "1" })).toStrictEqual({
    keibajoCode: "05",
    raceBango: "01",
  });
  expect(normalizePerRaceScope({ keibajoCode: " 83 ", raceBango: " 12 " })).toStrictEqual({
    keibajoCode: "83",
    raceBango: "12",
  });
});

test("PER_RACE_SCOPE_INVALID_ERROR describes the canonical target requirement", () => {
  expect(PER_RACE_SCOPE_INVALID_ERROR).toBe(
    "invalid per-race scope: keibajoCode must be a non-zero numeric code and raceBango must be 1..12",
  );
});
