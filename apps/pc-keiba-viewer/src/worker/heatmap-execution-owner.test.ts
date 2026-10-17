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
  const send = vi.fn<(message: unknown) => Promise<void>>(async () => undefined);
  const env: CloudflareEnv = {
    HEATMAP_BUILD_QUEUE: { send, sendBatch: vi.fn<() => Promise<void>>() },
  };
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
  expect(send).toHaveBeenCalledTimes(1);
  const message: unknown = send.mock.calls[0]?.[0];
  expect(message).toMatchObject({
    params: {
      day: "04",
      keibajoCode: "08",
      month: "10",
      query: "",
      raceNumbers: ["01"],
      source: "jra",
      year: "2026",
    },
  });
  rows[0] = { state: "malformed" };
  expect(await new HeatmapExecutionOwner(ctx, env).ensure(request)).toBe("unknown");
});
it("requires SQLite and build queue bindings", () => {
  const ctx: PcKeibaDurableObjectState = {
    acceptWebSocket: vi.fn<(...args: never[]) => unknown>(),
    blockConcurrencyWhile: async (callback) => callback(),
    getWebSockets: () => [],
    storage: { get: async () => undefined, put: async () => undefined },
  };
  expect(() => new HeatmapExecutionOwner(ctx, {})).toThrow("bindings unavailable");
});
