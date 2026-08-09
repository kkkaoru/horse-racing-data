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

it("reuses the existing write pool after the first Neon connection", async () => {
  const { getFinishPositionWritePool } = await import("./finish-position-lite-pool");
  const env = {
    DATABASE_URL_NEON: "postgres://writable-neon",
  } as unknown as Env;

  const firstPool = getFinishPositionWritePool(env);
  const secondPool = getFinishPositionWritePool(env);

  expect(secondPool).toBe(firstPool);
  expect(pgMock.Pool).toHaveBeenCalledTimes(1);
  expect(pgMock.Pool.mock.calls[0]?.[0]).toStrictEqual({
    connectionString: "postgres://writable-neon",
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

it("prefers DATABASE_URL_NEON over NEON_DATABASE_URL for the write pool", async () => {
  const { getFinishPositionWritePool } = await import("./finish-position-lite-pool");
  const env = {
    DATABASE_URL_NEON: "postgres://writable-neon",
    NEON_DATABASE_URL: "postgres://secondary-writable-neon",
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

it("fails fast for DML when Hyperdrive exists but no writable Neon secret is configured", async () => {
  const { getFinishPositionWritePool } = await import("./finish-position-lite-pool");
  const env = {
    HYPERDRIVE: { connectionString: "postgres://readonly-hyperdrive" },
  } as unknown as Env;

  expect(() => getFinishPositionWritePool(env)).toThrow(
    "DATABASE_URL_NEON or NEON_DATABASE_URL is required for finish-position write pool",
  );
  expect(pgMock.Pool).toHaveBeenCalledTimes(0);
});

it("fails fast for DML when neither Hyperdrive nor a writable Neon secret is configured", async () => {
  const { getFinishPositionWritePool } = await import("./finish-position-lite-pool");
  const env = {} as unknown as Env;

  expect(() => getFinishPositionWritePool(env)).toThrow(
    "DATABASE_URL_NEON or NEON_DATABASE_URL is required for finish-position write pool",
  );
  expect(pgMock.Pool).toHaveBeenCalledTimes(0);
});

it("fails fast for DML when writable Neon secrets are empty strings", async () => {
  const { getFinishPositionWritePool } = await import("./finish-position-lite-pool");
  const env = {
    DATABASE_URL_NEON: "",
    HYPERDRIVE: { connectionString: "postgres://readonly-hyperdrive" },
    NEON_DATABASE_URL: "",
  } as unknown as Env;

  expect(() => getFinishPositionWritePool(env)).toThrow(
    "DATABASE_URL_NEON or NEON_DATABASE_URL is required for finish-position write pool",
  );
  expect(pgMock.Pool).toHaveBeenCalledTimes(0);
});

it("does not reuse the Hyperdrive read pool for writes when Neon secrets are missing", async () => {
  const { getFinishPositionPool, getFinishPositionWritePool } =
    await import("./finish-position-lite-pool");
  const env = {
    HYPERDRIVE: { connectionString: "postgres://readonly-hyperdrive" },
  } as unknown as Env;

  getFinishPositionPool(env);

  expect(() => getFinishPositionWritePool(env)).toThrow(
    "DATABASE_URL_NEON or NEON_DATABASE_URL is required for finish-position write pool",
  );
  expect(pgMock.Pool).toHaveBeenCalledTimes(1);
  expect(pgMock.Pool.mock.calls[0]?.[0]).toStrictEqual({
    connectionString: "postgres://readonly-hyperdrive",
    max: 24,
  });
});
