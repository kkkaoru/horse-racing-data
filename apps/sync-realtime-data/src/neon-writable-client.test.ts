// run with: bun run test
import { expect, it, vi } from "vitest";
import type { Pool } from "pg";

import { NeonTransactionReadOnlyError, withWritableClient } from "./neon-writable-client";

type QueryFn = (sql: string, values?: unknown[]) => Promise<unknown>;

const createPool = (
  queryFn: QueryFn,
  release = vi.fn(),
): { clientRelease: ReturnType<typeof vi.fn>; pool: Pool; poolQuery: ReturnType<typeof vi.fn> } => {
  const client = { query: queryFn, release };
  const poolQuery = vi.fn();
  return {
    clientRelease: release,
    pool: {
      connect: vi.fn(async () => client),
      query: poolQuery,
    } as unknown as Pool,
    poolQuery,
  };
};

it("runs fn on the checked-out client and commits when transaction_read_only is off", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SHOW transaction_read_only") {
      return { rows: [{ transaction_read_only: "off" }] };
    }
    return undefined;
  });
  const { clientRelease, pool, poolQuery } = createPool(queryFn);
  const result = await withWritableClient(pool, async (client) => {
    await client.query("INSERT INTO demo VALUES (1)");
    return 42;
  });
  expect(result).toBe(42);
  expect(vi.mocked(queryFn).mock.calls.map(([sql]) => sql)).toStrictEqual([
    "BEGIN",
    "SET TRANSACTION READ WRITE",
    "SHOW transaction_read_only",
    "INSERT INTO demo VALUES (1)",
    "COMMIT",
  ]);
  expect(clientRelease).toHaveBeenCalledTimes(1);
  expect(poolQuery).not.toHaveBeenCalled();
});

it("fails closed without running fn when transaction_read_only stays on", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SHOW transaction_read_only") {
      return { rows: [{ transaction_read_only: "on" }] };
    }
    return undefined;
  });
  const { clientRelease, pool, poolQuery } = createPool(queryFn);
  const fn = vi.fn(async () => "ok");
  await expect(withWritableClient(pool, fn)).rejects.toBeInstanceOf(NeonTransactionReadOnlyError);
  expect(fn).not.toHaveBeenCalled();
  expect(vi.mocked(queryFn).mock.calls.map(([sql]) => sql)).toStrictEqual([
    "BEGIN",
    "SET TRANSACTION READ WRITE",
    "SHOW transaction_read_only",
    "ROLLBACK",
  ]);
  expect(clientRelease).toHaveBeenCalledTimes(1);
  expect(poolQuery).not.toHaveBeenCalled();
});

it("fails closed when SHOW transaction_read_only returns no rows", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SHOW transaction_read_only") {
      return { rows: [] };
    }
    return undefined;
  });
  const { clientRelease, pool } = createPool(queryFn);
  const fn = vi.fn(async () => "ok");
  await expect(withWritableClient(pool, fn)).rejects.toBeInstanceOf(NeonTransactionReadOnlyError);
  expect(fn).not.toHaveBeenCalled();
  expect(vi.mocked(queryFn).mock.calls.map(([sql]) => sql)).toStrictEqual([
    "BEGIN",
    "SET TRANSACTION READ WRITE",
    "SHOW transaction_read_only",
    "ROLLBACK",
  ]);
  expect(clientRelease).toHaveBeenCalledTimes(1);
});

it("fails closed when SHOW transaction_read_only omits the setting", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SHOW transaction_read_only") {
      return { rows: [{}] };
    }
    return undefined;
  });
  const { clientRelease, pool } = createPool(queryFn);
  await expect(withWritableClient(pool, async () => "ok")).rejects.toBeInstanceOf(
    NeonTransactionReadOnlyError,
  );
  expect(vi.mocked(queryFn).mock.calls.map(([sql]) => sql)).toStrictEqual([
    "BEGIN",
    "SET TRANSACTION READ WRITE",
    "SHOW transaction_read_only",
    "ROLLBACK",
  ]);
  expect(clientRelease).toHaveBeenCalledTimes(1);
});

it("fails closed when SHOW transaction_read_only omits rows", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SHOW transaction_read_only") {
      return {};
    }
    return undefined;
  });
  const { clientRelease, pool } = createPool(queryFn);
  const fn = vi.fn(async () => "ok");
  await expect(withWritableClient(pool, fn)).rejects.toBeInstanceOf(NeonTransactionReadOnlyError);
  expect(fn).not.toHaveBeenCalled();
  expect(vi.mocked(queryFn).mock.calls.map(([sql]) => sql)).toStrictEqual([
    "BEGIN",
    "SET TRANSACTION READ WRITE",
    "SHOW transaction_read_only",
    "ROLLBACK",
  ]);
  expect(clientRelease).toHaveBeenCalledTimes(1);
});

it("rolls back and rethrows when fn fails mid-transaction", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SHOW transaction_read_only") {
      return { rows: [{ transaction_read_only: "off" }] };
    }
    return undefined;
  });
  const { clientRelease, pool } = createPool(queryFn);
  await expect(
    withWritableClient(pool, async () => {
      throw new Error("dml failed");
    }),
  ).rejects.toThrow("dml failed");
  expect(vi.mocked(queryFn).mock.calls.map(([sql]) => sql)).toStrictEqual([
    "BEGIN",
    "SET TRANSACTION READ WRITE",
    "SHOW transaction_read_only",
    "ROLLBACK",
  ]);
  expect(clientRelease).toHaveBeenCalledTimes(1);
});

it("releases the client when BEGIN fails", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "BEGIN") {
      throw new Error("begin failed");
    }
    return undefined;
  });
  const { clientRelease, pool } = createPool(queryFn);
  await expect(withWritableClient(pool, async () => "ok")).rejects.toThrow("begin failed");
  expect(vi.mocked(queryFn).mock.calls.map(([sql]) => sql)).toStrictEqual(["BEGIN", "ROLLBACK"]);
  expect(clientRelease).toHaveBeenCalledTimes(1);
});

it("releases the client when SET TRANSACTION READ WRITE fails", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SET TRANSACTION READ WRITE") {
      throw new Error("set failed");
    }
    return undefined;
  });
  const { clientRelease, pool } = createPool(queryFn);
  await expect(withWritableClient(pool, async () => "ok")).rejects.toThrow("set failed");
  expect(vi.mocked(queryFn).mock.calls.map(([sql]) => sql)).toStrictEqual([
    "BEGIN",
    "SET TRANSACTION READ WRITE",
    "ROLLBACK",
  ]);
  expect(clientRelease).toHaveBeenCalledTimes(1);
});

it("releases the client when SHOW transaction_read_only fails", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SHOW transaction_read_only") {
      throw new Error("show failed");
    }
    return undefined;
  });
  const { clientRelease, pool } = createPool(queryFn);
  await expect(withWritableClient(pool, async () => "ok")).rejects.toThrow("show failed");
  expect(vi.mocked(queryFn).mock.calls.map(([sql]) => sql)).toStrictEqual([
    "BEGIN",
    "SET TRANSACTION READ WRITE",
    "SHOW transaction_read_only",
    "ROLLBACK",
  ]);
  expect(clientRelease).toHaveBeenCalledTimes(1);
});

it("rolls back and rethrows when COMMIT fails", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SHOW transaction_read_only") {
      return { rows: [{ transaction_read_only: "off" }] };
    }
    if (sql === "COMMIT") {
      throw new Error("commit failed");
    }
    return undefined;
  });
  const { clientRelease, pool } = createPool(queryFn);
  await expect(withWritableClient(pool, async () => "ok")).rejects.toThrow("commit failed");
  expect(vi.mocked(queryFn).mock.calls.map(([sql]) => sql)).toStrictEqual([
    "BEGIN",
    "SET TRANSACTION READ WRITE",
    "SHOW transaction_read_only",
    "COMMIT",
    "ROLLBACK",
  ]);
  expect(clientRelease).toHaveBeenCalledTimes(1);
});

it("preserves the original error when rollback fails", async () => {
  const queryFn: QueryFn = vi.fn(async (sql: string) => {
    if (sql === "SHOW transaction_read_only") {
      return { rows: [{ transaction_read_only: "off" }] };
    }
    if (sql === "ROLLBACK") {
      throw new Error("rollback failed");
    }
    return undefined;
  });
  const { clientRelease, pool } = createPool(queryFn);
  await expect(
    withWritableClient(pool, async () => {
      throw new Error("dml failed");
    }),
  ).rejects.toThrow("dml failed");
  expect(clientRelease).toHaveBeenCalledTimes(1);
});

it("does not run fn when pool.connect fails", async () => {
  const poolQuery = vi.fn();
  const pool = {
    connect: vi.fn(async () => {
      throw new Error("connect failed");
    }),
    query: poolQuery,
  } as unknown as Pool;
  const fn = vi.fn(async () => "ok");
  await expect(withWritableClient(pool, fn)).rejects.toThrow("connect failed");
  expect(fn).not.toHaveBeenCalled();
  expect(poolQuery).not.toHaveBeenCalled();
});

it("constructs NeonTransactionReadOnlyError with the observed setting", () => {
  const error = new NeonTransactionReadOnlyError("on");
  expect(error.name).toBe("NeonTransactionReadOnlyError");
  expect(error.message).toBe("Neon transaction_read_only is on; refusing DML");
});

it("constructs NeonTransactionReadOnlyError for a missing setting", () => {
  const error = new NeonTransactionReadOnlyError(undefined);
  expect(error.name).toBe("NeonTransactionReadOnlyError");
  expect(error.message).toBe("Neon transaction_read_only is missing; refusing DML");
});
