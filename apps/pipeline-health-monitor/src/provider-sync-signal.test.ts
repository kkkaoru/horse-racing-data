// Run with: bun run --filter pipeline-health-monitor test
import { expect, it } from "vitest";
import { buildProviderSyncSignal } from "./provider-sync-signal";

const NOW = new Date("2026-10-22T00:00:00.000Z");

it("reports a healthy NV-Link sync that succeeded within 26 hours", () => {
  expect(
    buildProviderSyncSignal("nv", '{"at":"2026-10-21T16:01:20.000Z","runId":"r1"}', NOW),
  ).toStrictEqual({
    description: "The scheduled NV-Link sync succeeded within the last 26 hours.",
    fields: [{ name: "KV key", value: "provider-sync:last-success:nv" }],
    key: "provider-sync:nv",
    ok: true,
    severity: "critical",
    stage: "healthy",
    title: "NV-Link daily sync recovered",
  });
});

it("flags a JV-Link sync whose last success is older than 26 hours", () => {
  expect(
    buildProviderSyncSignal("jv", '{"at":"2026-10-20T11:00:09.000Z","runId":"r1"}', NOW),
  ).toStrictEqual({
    description: "The last successful scheduled JV-Link sync was at 2026-10-20T11:00:09.000Z.",
    fields: [{ name: "KV key", value: "provider-sync:last-success:jv" }],
    key: "provider-sync:jv",
    ok: false,
    severity: "critical",
    stage: "stale",
    title: "JV-Link daily sync stalled",
  });
});

it("flags a missing provider sync record", () => {
  expect(buildProviderSyncSignal("nv", null, NOW)).toStrictEqual({
    description: "No successful scheduled NV-Link sync is recorded.",
    fields: [{ name: "KV key", value: "provider-sync:last-success:nv" }],
    key: "provider-sync:nv",
    ok: false,
    severity: "critical",
    stage: "stale",
    title: "NV-Link daily sync stalled",
  });
});

it("rejects a malformed provider sync record", () => {
  expect(() => buildProviderSyncSignal("jv", '{"at":"soon"}', NOW)).toThrow(
    "Provider sync health record is malformed",
  );
});
