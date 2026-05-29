// Run with: bun run --filter sync-realtime-data-hot test
import { afterEach, expect, it, vi } from "vitest";

vi.mock("pg-cloudflare", () => ({}));

vi.mock("pg", () => ({
  Pool: vi.fn(),
}));

import { Pool } from "pg";

import type { Env } from "./types";

afterEach(() => {
  vi.resetModules();
  vi.restoreAllMocks();
});

it("getHotPool throws when HYPERDRIVE binding is missing", async () => {
  const { getHotPool } = await import("./postgres-pool");
  const env = {} as unknown as Env;
  expect(() => getHotPool(env)).toThrowError(
    "HYPERDRIVE binding is required for hot self-discovery",
  );
});

it("getHotPool constructs a pg Pool with the Hyperdrive connection string on first call and caches it", async () => {
  vi.resetModules();
  const PoolCtor = vi.mocked(Pool);
  PoolCtor.mockClear();
  const { getHotPool } = await import("./postgres-pool");
  const env = {
    HYPERDRIVE: { connectionString: "postgres://hyperdrive.test/db" },
  } as unknown as Env;
  const first = getHotPool(env);
  const second = getHotPool(env);
  expect(first).toBe(second);
  expect(PoolCtor).toHaveBeenCalledTimes(1);
});
