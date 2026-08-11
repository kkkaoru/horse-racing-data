// Run with bun. Unit tests for the production per-race-only scope guard.

import { expect, test } from "vitest";
import { hasRequiredPerRaceScope, PER_RACE_SCOPE_REQUIRED_ERROR } from "./per-race-scope-guard";

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
