// Run with: bun run --filter sync-realtime-data-features test
import { expect, it } from "vitest";

import { shouldRunFeaturesCron } from "./polling-window-gate";

it("runs cron during JST polling window", () => {
  expect(shouldRunFeaturesCron(new Date("2026-05-29T03:00:00Z"))).toBe(true);
});

it("skips cron outside JST polling window", () => {
  expect(shouldRunFeaturesCron(new Date("2026-05-29T20:00:00Z"))).toBe(false);
});
