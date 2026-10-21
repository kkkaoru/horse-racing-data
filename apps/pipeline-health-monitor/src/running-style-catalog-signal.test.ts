// Run with: bun run --filter pipeline-health-monitor test
import { expect, it } from "vitest";
import { buildRunningStyleCatalogSignal } from "./running-style-catalog-signal";

const NOW = new Date("2026-10-22T00:00:00.000Z");

it("reports a healthy sync that succeeded within 26 hours", () => {
  expect(
    buildRunningStyleCatalogSignal('{"at":"2026-10-21T20:30:00.000Z","objects":3}', NOW),
  ).toStrictEqual({
    description: "The running-style Catalog sync succeeded within the last 26 hours.",
    fields: [{ name: "KV key", value: "running-style-catalog:last-success" }],
    key: "running-style-catalog-sync",
    ok: true,
    severity: "critical",
    stage: "healthy",
    title: "running-style Catalog sync recovered",
  });
});

it("flags a sync whose last success is older than 26 hours", () => {
  expect(
    buildRunningStyleCatalogSignal('{"at":"2026-10-20T20:30:00.000Z","objects":3}', NOW),
  ).toStrictEqual({
    description: "The last successful running-style Catalog sync was at 2026-10-20T20:30:00.000Z.",
    fields: [{ name: "KV key", value: "running-style-catalog:last-success" }],
    key: "running-style-catalog-sync",
    ok: false,
    severity: "critical",
    stage: "stale",
    title: "running-style Catalog sync stalled",
  });
});

it("flags a sync that found no Parquet objects", () => {
  expect(
    buildRunningStyleCatalogSignal('{"at":"2026-10-21T20:30:00.000Z","objects":0}', NOW),
  ).toStrictEqual({
    description:
      "The running-style Catalog sync at 2026-10-21T20:30:00.000Z found no Parquet objects in its lookback window.",
    fields: [{ name: "KV key", value: "running-style-catalog:last-success" }],
    key: "running-style-catalog-sync",
    ok: false,
    severity: "critical",
    stage: "stale",
    title: "running-style Catalog sync stalled",
  });
});

it("flags a missing health record", () => {
  expect(buildRunningStyleCatalogSignal(null, NOW)).toStrictEqual({
    description: "No successful running-style Catalog sync is recorded.",
    fields: [{ name: "KV key", value: "running-style-catalog:last-success" }],
    key: "running-style-catalog-sync",
    ok: false,
    severity: "critical",
    stage: "stale",
    title: "running-style Catalog sync stalled",
  });
});

it("rejects a malformed health record", () => {
  expect(() => buildRunningStyleCatalogSignal('{"at":"not-a-date","objects":3}', NOW)).toThrow(
    "Running-style Catalog health record is malformed",
  );
});
