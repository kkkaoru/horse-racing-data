// Run with bun (vitest).
import { expect, it, vi } from "vitest";

import { HeatmapExecutionOwner } from "./heatmap-execution-owner";

it("persists ownership before scheduling and preserves it across instances", async () => {
  const rows: Record<string, unknown>[] = [];
  const exec = vi.fn<
    (query: string, ...bindings: string[]) => { toArray(): Record<string, unknown>[] }
  >((query: string, ...bindings: string[]) => {
    if (query.startsWith("INSERT")) rows[0] = { state: bindings[0] };
    return { toArray: () => rows };
  });
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<(...args: never[]) => unknown>(),
    blockConcurrencyWhile: async (callback) => callback(),
    getWebSockets: () => [],
    storage: { get: async () => undefined, put: async () => undefined, sql: { exec } },
  };
  const createBatch = vi.fn<() => Promise<unknown[]>>(async () => []);
  const env: CloudflareEnv = { HEATMAP_WARM_WORKFLOW: { createBatch } };
  const request = {
    year: "2026",
    month: "10",
    day: "04",
    keibajoCode: "08",
    raceNumber: "01",
    source: "jra",
    query: "",
  } satisfies Parameters<HeatmapExecutionOwner["ensure"]>[0];
  const owner = new HeatmapExecutionOwner(ctx, env);
  expect(await owner.ensure(request)).toBe("queued");
  expect(await new HeatmapExecutionOwner(ctx, env).ensure(request)).toBe("queued");
  expect(await owner.claim("wrong")).toBe(false);
  expect(await owner.settle("wrong", true)).toBe(false);
  expect(createBatch).toHaveBeenCalledTimes(1);
  rows[0] = { state: "malformed" };
  expect(await new HeatmapExecutionOwner(ctx, env).ensure(request)).toBe("unknown");
});
it("requires SQLite and Workflow bindings", () => {
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<(...args: never[]) => unknown>(),
    blockConcurrencyWhile: async (callback) => callback(),
    getWebSockets: () => [],
    storage: { get: async () => undefined, put: async () => undefined },
  };
  expect(() => new HeatmapExecutionOwner(ctx, {})).toThrow("bindings unavailable");
});
