// run with: bun run test -- src/finish-position-lite-pool.test.ts
import { beforeEach, expect, it, vi } from "vitest";

import type { Env } from "./types";

const pgMock = vi.hoisted(() => ({
  Pool: vi.fn(function Pool(options: { connectionString: string; max: number }) {
    return { options };
  }),
}));

vi.mock("pg", () => pgMock);
vi.mock("pg-cloudflare", () => ({}));

beforeEach(() => {
  vi.resetModules();
  pgMock.Pool.mockClear();
});

it("keeps the read pool on Hyperdrive when a writable Neon secret is also present", async () => {
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  const env = {
    DATABASE_URL_NEON: "postgres://writable-neon",
    HYPERDRIVE: { connectionString: "postgres://readonly-hyperdrive" },
  } as unknown as Env;

  getFinishPositionPool(env);

  expect(pgMock.Pool).toHaveBeenCalledTimes(1);
  expect(pgMock.Pool.mock.calls[0]?.[0]).toStrictEqual({
    connectionString: "postgres://readonly-hyperdrive",
    max: 24,
  });
});

it("uses DATABASE_URL_NEON for the write pool before Hyperdrive", async () => {
  const { getFinishPositionWritePool } = await import("./finish-position-lite-pool");
  const env = {
    DATABASE_URL_NEON: "postgres://writable-neon",
    HYPERDRIVE: { connectionString: "postgres://readonly-hyperdrive" },
  } as unknown as Env;

  getFinishPositionWritePool(env);

  expect(pgMock.Pool).toHaveBeenCalledTimes(1);
  expect(pgMock.Pool.mock.calls[0]?.[0]).toStrictEqual({
    connectionString: "postgres://writable-neon",
    max: 24,
  });
});

it("uses NEON_DATABASE_URL for the write pool when DATABASE_URL_NEON is absent", async () => {
  const { getFinishPositionWritePool } = await import("./finish-position-lite-pool");
  const env = {
    HYPERDRIVE: { connectionString: "postgres://readonly-hyperdrive" },
    NEON_DATABASE_URL: "postgres://secondary-writable-neon",
  } as unknown as Env;

  getFinishPositionWritePool(env);

  expect(pgMock.Pool).toHaveBeenCalledTimes(1);
  expect(pgMock.Pool.mock.calls[0]?.[0]).toStrictEqual({
    connectionString: "postgres://secondary-writable-neon",
    max: 24,
  });
});

it("falls back to the existing Hyperdrive pool when no writable secret exists", async () => {
  const { getFinishPositionPool, getFinishPositionWritePool } =
    await import("./finish-position-lite-pool");
  const env = {
    HYPERDRIVE: { connectionString: "postgres://readonly-hyperdrive" },
  } as unknown as Env;

  const readPool = getFinishPositionPool(env);
  const writePool = getFinishPositionWritePool(env);

  expect(readPool).toBe(writePool);
  expect(pgMock.Pool).toHaveBeenCalledTimes(1);
  expect(pgMock.Pool.mock.calls[0]?.[0]).toStrictEqual({
    connectionString: "postgres://readonly-hyperdrive",
    max: 24,
  });
});
