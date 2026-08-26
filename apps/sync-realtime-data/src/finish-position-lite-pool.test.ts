// run with: bun run test -- src/finish-position-lite-pool.test.ts
import { beforeEach, expect, it, vi } from "vitest";

import type { Env } from "./types";

interface MockPoolClient {
  query: (sql: string) => Promise<unknown>;
}

interface MockPoolOptions {
  connectionTimeoutMillis: number;
  connectionString: string;
  max: number;
  onConnect?: (client: MockPoolClient) => unknown;
  query_timeout: number;
  statement_timeout: number;
}

const pgMock = vi.hoisted(() => ({
  Pool: vi.fn(function Pool(options: MockPoolOptions) {
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
    connectionTimeoutMillis: 15_000,
    connectionString: "postgres://readonly-hyperdrive",
    max: 24,
    query_timeout: 90_000,
    statement_timeout: 90_000,
  });
});

it("does not attach onConnect to the Hyperdrive read pool", async () => {
  const query = vi.fn(async () => undefined);
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  const env = {
    HYPERDRIVE: { connectionString: "postgres://readonly-hyperdrive" },
  } as unknown as Env;

  getFinishPositionPool(env);

  expect(pgMock.Pool.mock.calls[0]?.[0]?.onConnect).toBeUndefined();
  expect(query).not.toHaveBeenCalled();
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
  expect(pgMock.Pool.mock.calls[0]?.[0]?.connectionString).toBe("postgres://writable-neon");
  expect(pgMock.Pool.mock.calls[0]?.[0]?.connectionTimeoutMillis).toBe(15_000);
  expect(pgMock.Pool.mock.calls[0]?.[0]?.max).toBe(2);
  expect(typeof pgMock.Pool.mock.calls[0]?.[0]?.onConnect).toBe("function");
  expect(pgMock.Pool.mock.calls[0]?.[0]?.query_timeout).toBe(30_000);
  expect(pgMock.Pool.mock.calls[0]?.[0]?.statement_timeout).toBe(30_000);
});

it("uses DATABASE_URL_NEON for the write pool before Hyperdrive", async () => {
  const { getFinishPositionWritePool } = await import("./finish-position-lite-pool");
  const env = {
    DATABASE_URL_NEON: "postgres://writable-neon",
    HYPERDRIVE: { connectionString: "postgres://readonly-hyperdrive" },
  } as unknown as Env;

  getFinishPositionWritePool(env);

  expect(pgMock.Pool).toHaveBeenCalledTimes(1);
  expect(pgMock.Pool.mock.calls[0]?.[0]?.connectionString).toBe("postgres://writable-neon");
  expect(pgMock.Pool.mock.calls[0]?.[0]?.max).toBe(2);
  expect(typeof pgMock.Pool.mock.calls[0]?.[0]?.onConnect).toBe("function");
});

it("prefers DATABASE_URL_NEON over NEON_DATABASE_URL for the write pool", async () => {
  const { getFinishPositionWritePool } = await import("./finish-position-lite-pool");
  const env = {
    DATABASE_URL_NEON: "postgres://writable-neon",
    NEON_DATABASE_URL: "postgres://secondary-writable-neon",
  } as unknown as Env;

  getFinishPositionWritePool(env);

  expect(pgMock.Pool).toHaveBeenCalledTimes(1);
  expect(pgMock.Pool.mock.calls[0]?.[0]?.connectionString).toBe("postgres://writable-neon");
  expect(pgMock.Pool.mock.calls[0]?.[0]?.max).toBe(2);
  expect(typeof pgMock.Pool.mock.calls[0]?.[0]?.onConnect).toBe("function");
});

it("uses NEON_DATABASE_URL for the write pool when DATABASE_URL_NEON is absent", async () => {
  const { getFinishPositionWritePool } = await import("./finish-position-lite-pool");
  const env = {
    HYPERDRIVE: { connectionString: "postgres://readonly-hyperdrive" },
    NEON_DATABASE_URL: "postgres://secondary-writable-neon",
  } as unknown as Env;

  getFinishPositionWritePool(env);

  expect(pgMock.Pool).toHaveBeenCalledTimes(1);
  expect(pgMock.Pool.mock.calls[0]?.[0]?.connectionString).toBe(
    "postgres://secondary-writable-neon",
  );
  expect(pgMock.Pool.mock.calls[0]?.[0]?.max).toBe(2);
  expect(typeof pgMock.Pool.mock.calls[0]?.[0]?.onConnect).toBe("function");
});

it("sets default_transaction_read_only off on new write-pool connects", async () => {
  const query = vi.fn(async () => undefined);
  const { getFinishPositionWritePool } = await import("./finish-position-lite-pool");
  const env = {
    DATABASE_URL_NEON: "postgres://writable-neon",
  } as unknown as Env;

  getFinishPositionWritePool(env);
  await pgMock.Pool.mock.calls[0]?.[0]?.onConnect?.({ query });

  expect(query.mock.calls).toStrictEqual([["SET default_transaction_read_only TO off"]]);
});

it("swallows Error from write-pool onConnect SET without throwing", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const query = vi.fn(async () => {
    throw new Error("cannot set");
  });
  const { getFinishPositionWritePool } = await import("./finish-position-lite-pool");
  const env = {
    DATABASE_URL_NEON: "postgres://writable-neon",
  } as unknown as Env;

  getFinishPositionWritePool(env);
  await expect(pgMock.Pool.mock.calls[0]?.[0]?.onConnect?.({ query })).resolves.toBeUndefined();

  const line = String(errorSpy.mock.calls[0]?.[0]);
  expect(
    line.slice(
      0,
      "Finish-position write pool SET failed setting=default_transaction_read_only name=Error message=cannot set stack="
        .length,
    ),
  ).toBe(
    "Finish-position write pool SET failed setting=default_transaction_read_only name=Error message=cannot set stack=",
  );
  expect(line.split("postgres://")).toStrictEqual([line]);
  errorSpy.mockRestore();
});

it("swallows non-Error from write-pool onConnect SET without throwing", async () => {
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const query = vi.fn(async () => {
    throw "cannot set";
  });
  const { getFinishPositionWritePool } = await import("./finish-position-lite-pool");
  const env = {
    DATABASE_URL_NEON: "postgres://writable-neon",
  } as unknown as Env;

  getFinishPositionWritePool(env);
  await expect(pgMock.Pool.mock.calls[0]?.[0]?.onConnect?.({ query })).resolves.toBeUndefined();

  expect(errorSpy.mock.calls[0]?.[0]).toBe(
    "Finish-position write pool SET failed setting=default_transaction_read_only name=unknown message=cannot set stack=",
  );
  expect(String(errorSpy.mock.calls[0]?.[0]).split("postgres://")).toStrictEqual([
    "Finish-position write pool SET failed setting=default_transaction_read_only name=unknown message=cannot set stack=",
  ]);
  errorSpy.mockRestore();
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
    connectionTimeoutMillis: 15_000,
    connectionString: "postgres://readonly-hyperdrive",
    max: 24,
    query_timeout: 90_000,
    statement_timeout: 90_000,
  });
});
