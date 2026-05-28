// Run with bun.
import { expect, it } from "vitest";

import { shouldRunOddsCron } from "./polling-window-gate";

it("returns true at JST 06:00 (UTC 21:00 of previous day)", () => {
  expect(shouldRunOddsCron(new Date("2026-05-27T21:00:00Z"))).toBe(true);
});

it("returns true at JST 21:30", () => {
  expect(shouldRunOddsCron(new Date("2026-05-28T12:30:00Z"))).toBe(true);
});

it("returns false at JST 22:00", () => {
  expect(shouldRunOddsCron(new Date("2026-05-28T13:00:00Z"))).toBe(false);
});

it("returns false at JST 05:59", () => {
  expect(shouldRunOddsCron(new Date("2026-05-27T20:59:00Z"))).toBe(false);
});
