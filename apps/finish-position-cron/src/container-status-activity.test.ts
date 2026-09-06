// Run with bun. Activity renewal is bounded by real progress and absolute age.
import { expect, test } from "vitest";
import { shouldRenewFocusedFullActivity } from "./container-status-activity";
const running = {
  status: "running",
  raceKey: "nar:20260906:54:09",
  startedAtMs: 1000000,
  lastProgressAtMs: 1950000,
};

test("renews a live matching race with recent progress", () => {
  expect(
    shouldRenewFocusedFullActivity({
      nowMs: 2000000,
      raceKey: "nar:20260906:54:09",
      payload: running,
    }),
  ).toBe(true);
});
test.each([
  null,
  "bad",
  {},
  { status: "running" },
  { status: "running", raceKey: "x" },
  { status: "running", raceKey: "x", startedAtMs: 1 },
  { ...running, status: "success" },
  { ...running, status: "error" },
  { ...running, status: "missing" },
  { ...running, raceKey: "nar:20260906:54:10" },
  { ...running, startedAtMs: null },
  { ...running, startedAtMs: Number.NaN },
  { ...running, lastProgressAtMs: null },
  { ...running, lastProgressAtMs: Number.POSITIVE_INFINITY },
  { ...running, startedAtMs: 2000001 },
  { ...running, lastProgressAtMs: 2000001 },
  { ...running, lastProgressAtMs: 999999 },
  { ...running, lastProgressAtMs: 1759999 },
  { ...running, startedAtMs: 140000 },
])("does not renew idle, expired or invalid status %#", (payload) => {
  expect(
    shouldRenewFocusedFullActivity({ nowMs: 2000000, raceKey: "nar:20260906:54:09", payload }),
  ).toBe(false);
});
test("accepts the progress boundary but not the absolute deadline boundary", () => {
  expect(
    shouldRenewFocusedFullActivity({
      nowMs: 2000000,
      raceKey: "nar:20260906:54:09",
      payload: { ...running, lastProgressAtMs: 1760000 },
    }),
  ).toBe(true);
});
