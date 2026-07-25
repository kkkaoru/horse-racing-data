// This test runs with Bun and Vitest.
import { beforeEach, expect, test, vi } from "vitest";

const pgMock = vi.hoisted(() => {
  const clientQuery = vi.fn();
  const release = vi.fn();
  const poolQuery = vi.fn();
  const end = vi.fn();
  const connect = vi.fn(async () => ({
    query: clientQuery,
    release,
  }));
  const Pool = vi.fn(function Pool(_config: {
    host: string;
    port: number;
    database: string;
    user: string;
    password: string;
  }) {
    return {
      connect,
      query: poolQuery,
      end,
    };
  });
  return { Pool, connect, clientQuery, release, poolQuery, end };
});

vi.mock("pg", () => ({
  Pool: pgMock.Pool,
}));

import {
  createPostgresClient,
  resolvePostgresConfig,
  type CreatePostgresClientInput,
  type ExecutableSqlStatement,
  type PostgresConnectionConfig,
  type PostgresPool,
  type PostgresPoolClient,
  type PostgresPoolFactory,
  type PostgresQueryResult,
  type QueryParameter,
  type SqlExecutor,
} from "./pg-client";

beforeEach(() => {
  pgMock.Pool.mockClear();
  pgMock.connect.mockClear();
  pgMock.clientQuery.mockReset();
  pgMock.release.mockReset();
  pgMock.poolQuery.mockReset();
  pgMock.end.mockReset();
  pgMock.clientQuery.mockResolvedValue({ rowCount: 0, rows: [] });
  pgMock.poolQuery.mockResolvedValue({ rowCount: 0, rows: [] });
  pgMock.end.mockResolvedValue(undefined);
});

const BASE_CONFIG: PostgresConnectionConfig = {
  host: "127.0.0.1",
  port: 15432,
  database: "horse_racing",
  user: "postgres",
  password: "secret",
};

const SELECT_STATEMENT: ExecutableSqlStatement = {
  text: "SELECT code FROM jvd_um WHERE ketto_toroku_bango = $1",
  values: ["2019101234"],
};

const emptyResult = (): PostgresQueryResult => ({
  rowCount: 0,
  rows: [],
});

const rowResult = (): PostgresQueryResult => ({
  rowCount: 1,
  rows: [{ code: "12345", score: 7, flag: true, missing: null }],
});

interface FakeClientOptions {
  readonly queryImpl?: (
    text: string,
    values?: readonly QueryParameter[],
  ) => Promise<PostgresQueryResult>;
  readonly releaseImpl?: () => void;
}

interface FakePoolOptions {
  readonly connectImpl?: () => Promise<PostgresPoolClient>;
  readonly queryImpl?: (
    text: string,
    values?: readonly QueryParameter[],
  ) => Promise<PostgresQueryResult>;
  readonly endImpl?: () => Promise<void>;
}

interface RecordedQuery {
  readonly text: string;
  readonly values: readonly QueryParameter[] | undefined;
}

const createRecordingClient = (
  queries: RecordedQuery[],
  options: FakeClientOptions = {},
): PostgresPoolClient => ({
  query: async (text: string, values?: readonly QueryParameter[]): Promise<PostgresQueryResult> => {
    queries.push({ text, values });
    if (options.queryImpl !== undefined) {
      return options.queryImpl(text, values);
    }
    return emptyResult();
  },
  release: (): void => {
    if (options.releaseImpl !== undefined) {
      options.releaseImpl();
    }
  },
});

const createRecordingPool = (
  queries: RecordedQuery[],
  options: FakePoolOptions = {},
): PostgresPool => ({
  connect: async (): Promise<PostgresPoolClient> => {
    if (options.connectImpl !== undefined) {
      return options.connectImpl();
    }
    return createRecordingClient(queries);
  },
  query: async (text: string, values?: readonly QueryParameter[]): Promise<PostgresQueryResult> => {
    queries.push({ text, values });
    if (options.queryImpl !== undefined) {
      return options.queryImpl(text, values);
    }
    return emptyResult();
  },
  end: async (): Promise<void> => {
    if (options.endImpl !== undefined) {
      await options.endImpl();
    }
  },
});

const createClientWithPool = (
  pool: PostgresPool,
  config: PostgresConnectionConfig = BASE_CONFIG,
): ReturnType<typeof createPostgresClient> => {
  const createPool: PostgresPoolFactory = (): PostgresPool => pool;
  const input: CreatePostgresClientInput = { config, createPool };
  return createPostgresClient(input);
};

test("resolvePostgresConfig uses defaults for host and port when omitted", () => {
  const config: PostgresConnectionConfig = resolvePostgresConfig({
    POSTGRES_DB: "horse_racing",
    POSTGRES_USER: "postgres",
    POSTGRES_PASSWORD: "secret",
  });
  expect(config).toStrictEqual({
    host: "127.0.0.1",
    port: 15432,
    database: "horse_racing",
    user: "postgres",
    password: "secret",
  });
});

test("resolvePostgresConfig uses defaults for host and port when empty strings", () => {
  const config: PostgresConnectionConfig = resolvePostgresConfig({
    POSTGRES_HOST: "",
    POSTGRES_PORT: "",
    POSTGRES_DB: "horse_racing",
    POSTGRES_USER: "postgres",
    POSTGRES_PASSWORD: "secret",
  });
  expect(config).toStrictEqual({
    host: "127.0.0.1",
    port: 15432,
    database: "horse_racing",
    user: "postgres",
    password: "secret",
  });
});

test("resolvePostgresConfig accepts explicit host and port", () => {
  const config: PostgresConnectionConfig = resolvePostgresConfig({
    POSTGRES_HOST: "db.internal",
    POSTGRES_PORT: "5432",
    POSTGRES_DB: "horse_racing",
    POSTGRES_USER: "app",
    POSTGRES_PASSWORD: "secret",
  });
  expect(config).toStrictEqual({
    host: "db.internal",
    port: 5432,
    database: "horse_racing",
    user: "app",
    password: "secret",
  });
});

test("resolvePostgresConfig rejects missing POSTGRES_DB", () => {
  expect(() =>
    resolvePostgresConfig({
      POSTGRES_USER: "postgres",
      POSTGRES_PASSWORD: "secret",
    }),
  ).toThrowError("Missing required environment variable: POSTGRES_DB");
});

test("resolvePostgresConfig rejects empty POSTGRES_DB", () => {
  expect(() =>
    resolvePostgresConfig({
      POSTGRES_DB: "",
      POSTGRES_USER: "postgres",
      POSTGRES_PASSWORD: "secret",
    }),
  ).toThrowError("Missing required environment variable: POSTGRES_DB");
});

test("resolvePostgresConfig rejects missing POSTGRES_USER", () => {
  expect(() =>
    resolvePostgresConfig({
      POSTGRES_DB: "horse_racing",
      POSTGRES_PASSWORD: "secret",
    }),
  ).toThrowError("Missing required environment variable: POSTGRES_USER");
});

test("resolvePostgresConfig rejects empty POSTGRES_USER", () => {
  expect(() =>
    resolvePostgresConfig({
      POSTGRES_DB: "horse_racing",
      POSTGRES_USER: "",
      POSTGRES_PASSWORD: "secret",
    }),
  ).toThrowError("Missing required environment variable: POSTGRES_USER");
});

test("resolvePostgresConfig rejects missing POSTGRES_PASSWORD", () => {
  expect(() =>
    resolvePostgresConfig({
      POSTGRES_DB: "horse_racing",
      POSTGRES_USER: "postgres",
    }),
  ).toThrowError("Missing required environment variable: POSTGRES_PASSWORD");
});

test("resolvePostgresConfig rejects empty POSTGRES_PASSWORD", () => {
  expect(() =>
    resolvePostgresConfig({
      POSTGRES_DB: "horse_racing",
      POSTGRES_USER: "postgres",
      POSTGRES_PASSWORD: "",
    }),
  ).toThrowError("Missing required environment variable: POSTGRES_PASSWORD");
});

test("resolvePostgresConfig rejects non-integer POSTGRES_PORT without echoing the value", () => {
  expect(() =>
    resolvePostgresConfig({
      POSTGRES_PORT: "not-a-port",
      POSTGRES_DB: "horse_racing",
      POSTGRES_USER: "postgres",
      POSTGRES_PASSWORD: "secret",
    }),
  ).toThrowError("Invalid POSTGRES_PORT: must be an integer between 1 and 65535");
});

test("resolvePostgresConfig rejects out-of-range POSTGRES_PORT", () => {
  expect(() =>
    resolvePostgresConfig({
      POSTGRES_PORT: "70000",
      POSTGRES_DB: "horse_racing",
      POSTGRES_USER: "postgres",
      POSTGRES_PASSWORD: "secret",
    }),
  ).toThrowError("Invalid POSTGRES_PORT: must be an integer between 1 and 65535");
});

test("resolvePostgresConfig rejects zero POSTGRES_PORT", () => {
  expect(() =>
    resolvePostgresConfig({
      POSTGRES_PORT: "0",
      POSTGRES_DB: "horse_racing",
      POSTGRES_USER: "postgres",
      POSTGRES_PASSWORD: "secret",
    }),
  ).toThrowError("Invalid POSTGRES_PORT: must be an integer between 1 and 65535");
});

test("execute returns zero rows and zero rowCount", async () => {
  const queries: RecordedQuery[] = [];
  const pool: PostgresPool = createRecordingPool(queries, {
    queryImpl: async (): Promise<PostgresQueryResult> => emptyResult(),
  });
  const client = createClientWithPool(pool);
  const outcome = await client.execute(SELECT_STATEMENT);
  expect(outcome).toStrictEqual({
    rowCount: 0,
    rows: [],
  });
  expect(queries).toStrictEqual([
    {
      text: "SELECT code FROM jvd_um WHERE ketto_toroku_bango = $1",
      values: ["2019101234"],
    },
  ]);
});

test("execute returns rows and normalizes cell values", async () => {
  const queries: RecordedQuery[] = [];
  const pool: PostgresPool = createRecordingPool(queries, {
    queryImpl: async (): Promise<PostgresQueryResult> => rowResult(),
  });
  const client = createClientWithPool(pool);
  const outcome = await client.execute(SELECT_STATEMENT);
  expect(outcome).toStrictEqual({
    rowCount: 1,
    rows: [{ code: "12345", score: "7", flag: "true", missing: "" }],
  });
});

test("execute treats null rowCount as zero", async () => {
  const queries: RecordedQuery[] = [];
  const pool: PostgresPool = createRecordingPool(queries, {
    queryImpl: async (): Promise<PostgresQueryResult> => ({
      rowCount: null,
      rows: [],
    }),
  });
  const client = createClientWithPool(pool);
  const outcome = await client.execute(SELECT_STATEMENT);
  expect(outcome).toStrictEqual({
    rowCount: 0,
    rows: [],
  });
});

test("execute accepts nested array query parameters used by master lookup", async () => {
  const queries: RecordedQuery[] = [];
  const pool: PostgresPool = createRecordingPool(queries, {
    queryImpl: async (): Promise<PostgresQueryResult> => ({
      rowCount: 2,
      rows: [{ code: "a" }, { code: "b" }],
    }),
  });
  const client = createClientWithPool(pool);
  const statement: ExecutableSqlStatement = {
    text: "SELECT code FROM jvd_ks WHERE kishu_code = ANY($1::text[])",
    values: [["00001", "00002"]],
  };
  const outcome = await client.execute(statement);
  expect(outcome).toStrictEqual({
    rowCount: 2,
    rows: [{ code: "a" }, { code: "b" }],
  });
  expect(queries).toStrictEqual([
    {
      text: "SELECT code FROM jvd_ks WHERE kishu_code = ANY($1::text[])",
      values: [["00001", "00002"]],
    },
  ]);
});

test("connect probes the database and releases the client", async () => {
  const queries: RecordedQuery[] = [];
  const releaseCalls: string[] = [];
  const pool: PostgresPool = createRecordingPool(queries, {
    connectImpl: async (): Promise<PostgresPoolClient> =>
      createRecordingClient(queries, {
        releaseImpl: (): void => {
          releaseCalls.push("released");
        },
      }),
  });
  const client = createClientWithPool(pool);
  await client.connect();
  expect(queries).toStrictEqual([
    {
      text: "SELECT 1",
      values: undefined,
    },
  ]);
  expect(releaseCalls).toStrictEqual(["released"]);
});

test("connect rethrows probe failure after release", async () => {
  const queries: RecordedQuery[] = [];
  const releaseCalls: string[] = [];
  const pool: PostgresPool = createRecordingPool(queries, {
    connectImpl: async (): Promise<PostgresPoolClient> =>
      createRecordingClient(queries, {
        queryImpl: async (): Promise<PostgresQueryResult> => {
          throw new Error("probe failed");
        },
        releaseImpl: (): void => {
          releaseCalls.push("released");
        },
      }),
  });
  const client = createClientWithPool(pool);
  await expect(client.connect()).rejects.toThrowError("probe failed");
  expect(releaseCalls).toStrictEqual(["released"]);
});

test("end closes the pool", async () => {
  const queries: RecordedQuery[] = [];
  const endCalls: string[] = [];
  const pool: PostgresPool = createRecordingPool(queries, {
    endImpl: async (): Promise<void> => {
      endCalls.push("ended");
    },
  });
  const client = createClientWithPool(pool);
  await client.end();
  expect(endCalls).toStrictEqual(["ended"]);
});

test("withTransaction commits when the callback succeeds", async () => {
  const queries: RecordedQuery[] = [];
  const releaseCalls: string[] = [];
  const pool: PostgresPool = createRecordingPool(queries, {
    connectImpl: async (): Promise<PostgresPoolClient> =>
      createRecordingClient(queries, {
        queryImpl: async (
          text: string,
          values?: readonly QueryParameter[],
        ): Promise<PostgresQueryResult> => {
          if (text === "SELECT code FROM jvd_um WHERE ketto_toroku_bango = $1") {
            return {
              rowCount: 1,
              rows: [{ code: "ok" }],
            };
          }
          if (values !== undefined) {
            return emptyResult();
          }
          return emptyResult();
        },
        releaseImpl: (): void => {
          releaseCalls.push("released");
        },
      }),
  });
  const client = createClientWithPool(pool);
  const value: string = await client.withTransaction(async (executor: SqlExecutor) => {
    const outcome = await executor.execute(SELECT_STATEMENT);
    expect(outcome).toStrictEqual({
      rowCount: 1,
      rows: [{ code: "ok" }],
    });
    return "done";
  });
  expect(value).toStrictEqual("done");
  expect(queries).toStrictEqual([
    {
      text: "BEGIN",
      values: undefined,
    },
    {
      text: "SELECT code FROM jvd_um WHERE ketto_toroku_bango = $1",
      values: ["2019101234"],
    },
    {
      text: "COMMIT",
      values: undefined,
    },
  ]);
  expect(releaseCalls).toStrictEqual(["released"]);
});

test("withTransaction rolls back and rethrows when the callback fails", async () => {
  const queries: RecordedQuery[] = [];
  const releaseCalls: string[] = [];
  const pool: PostgresPool = createRecordingPool(queries, {
    connectImpl: async (): Promise<PostgresPoolClient> =>
      createRecordingClient(queries, {
        releaseImpl: (): void => {
          releaseCalls.push("released");
        },
      }),
  });
  const client = createClientWithPool(pool);
  await expect(
    client.withTransaction(async (): Promise<string> => {
      throw new Error("callback failed");
    }),
  ).rejects.toThrowError("callback failed");
  expect(queries).toStrictEqual([
    {
      text: "BEGIN",
      values: undefined,
    },
    {
      text: "ROLLBACK",
      values: undefined,
    },
  ]);
  expect(releaseCalls).toStrictEqual(["released"]);
});

test("withTransaction does not roll back when BEGIN fails", async () => {
  const queries: RecordedQuery[] = [];
  const releaseCalls: string[] = [];
  const pool: PostgresPool = createRecordingPool(queries, {
    connectImpl: async (): Promise<PostgresPoolClient> =>
      createRecordingClient(queries, {
        queryImpl: async (text: string): Promise<PostgresQueryResult> => {
          if (text === "BEGIN") {
            throw new Error("begin failed");
          }
          return emptyResult();
        },
        releaseImpl: (): void => {
          releaseCalls.push("released");
        },
      }),
  });
  const client = createClientWithPool(pool);
  await expect(client.withTransaction(async (): Promise<string> => "unused")).rejects.toThrowError(
    "begin failed",
  );
  expect(queries).toStrictEqual([
    {
      text: "BEGIN",
      values: undefined,
    },
  ]);
  expect(releaseCalls).toStrictEqual(["released"]);
});

test("execute propagates pool query errors", async () => {
  const queries: RecordedQuery[] = [];
  const pool: PostgresPool = createRecordingPool(queries, {
    queryImpl: async (): Promise<PostgresQueryResult> => {
      throw new Error("query failed");
    },
  });
  const client = createClientWithPool(pool);
  await expect(client.execute(SELECT_STATEMENT)).rejects.toThrowError("query failed");
});

test("normalizeCell maps unsupported object values to empty string", async () => {
  const queries: RecordedQuery[] = [];
  const pool: PostgresPool = createRecordingPool(queries, {
    queryImpl: async (): Promise<PostgresQueryResult> => ({
      rowCount: 1,
      rows: [{ nested: { a: 1 }, symbolish: undefined }],
    }),
  });
  const client = createClientWithPool(pool);
  const outcome = await client.execute(SELECT_STATEMENT);
  expect(outcome).toStrictEqual({
    rowCount: 1,
    rows: [{ nested: "", symbolish: "" }],
  });
});

test("normalizeCell maps bigint cells to strings", async () => {
  const queries: RecordedQuery[] = [];
  const pool: PostgresPool = createRecordingPool(queries, {
    queryImpl: async (): Promise<PostgresQueryResult> => ({
      rowCount: 1,
      rows: [{ big: 10n }],
    }),
  });
  const client = createClientWithPool(pool);
  const outcome = await client.execute(SELECT_STATEMENT);
  expect(outcome).toStrictEqual({
    rowCount: 1,
    rows: [{ big: "10" }],
  });
});

test("default pool factory constructs Pool and supports connect execute and end", async () => {
  pgMock.clientQuery.mockResolvedValue({ rowCount: 1, rows: [{ ok: "1" }] });
  pgMock.poolQuery.mockResolvedValue({
    rowCount: 1,
    rows: [{ code: "12345" }],
  });
  const client = createPostgresClient({ config: BASE_CONFIG });
  await client.connect();
  const outcome = await client.execute(SELECT_STATEMENT);
  await client.end();
  expect(pgMock.Pool).toHaveBeenCalledTimes(1);
  expect(pgMock.Pool.mock.calls[0]?.[0]).toStrictEqual({
    host: "127.0.0.1",
    port: 15432,
    database: "horse_racing",
    user: "postgres",
    password: "secret",
  });
  expect(pgMock.clientQuery.mock.calls[0]?.[0]).toStrictEqual("SELECT 1");
  expect(pgMock.clientQuery.mock.calls[0]?.[1]).toStrictEqual([]);
  expect(outcome).toStrictEqual({
    rowCount: 1,
    rows: [{ code: "12345" }],
  });
  expect(pgMock.poolQuery.mock.calls[0]?.[0]).toStrictEqual(
    "SELECT code FROM jvd_um WHERE ketto_toroku_bango = $1",
  );
  expect(pgMock.poolQuery.mock.calls[0]?.[1]).toStrictEqual(["2019101234"]);
  expect(pgMock.end).toHaveBeenCalledTimes(1);
  expect(pgMock.release).toHaveBeenCalledTimes(1);
});

test("default pool factory runs withTransaction through the Pool client", async () => {
  pgMock.clientQuery.mockImplementation(async (text: string) => {
    if (text === "SELECT code FROM jvd_um WHERE ketto_toroku_bango = $1") {
      return {
        rowCount: 1,
        rows: [{ code: "ok" }],
      };
    }
    return { rowCount: 0, rows: [] };
  });
  const client = createPostgresClient({ config: BASE_CONFIG });
  const value: string = await client.withTransaction(async (executor: SqlExecutor) => {
    const outcome = await executor.execute(SELECT_STATEMENT);
    expect(outcome).toStrictEqual({
      rowCount: 1,
      rows: [{ code: "ok" }],
    });
    return "committed";
  });
  expect(value).toStrictEqual("committed");
  expect(pgMock.clientQuery.mock.calls[0]?.[0]).toStrictEqual("BEGIN");
  expect(pgMock.clientQuery.mock.calls[0]?.[1]).toStrictEqual([]);
  expect(pgMock.clientQuery.mock.calls[1]?.[0]).toStrictEqual(
    "SELECT code FROM jvd_um WHERE ketto_toroku_bango = $1",
  );
  expect(pgMock.clientQuery.mock.calls[1]?.[1]).toStrictEqual(["2019101234"]);
  expect(pgMock.clientQuery.mock.calls[2]?.[0]).toStrictEqual("COMMIT");
  expect(pgMock.clientQuery.mock.calls[2]?.[1]).toStrictEqual([]);
  expect(pgMock.release).toHaveBeenCalledTimes(1);
});

test("default pool factory rolls back through the Pool client when callback fails", async () => {
  const client = createPostgresClient({ config: BASE_CONFIG });
  await expect(
    client.withTransaction(async (): Promise<string> => {
      throw new Error("default pool callback failed");
    }),
  ).rejects.toThrowError("default pool callback failed");
  expect(pgMock.clientQuery.mock.calls[0]?.[0]).toStrictEqual("BEGIN");
  expect(pgMock.clientQuery.mock.calls[1]?.[0]).toStrictEqual("ROLLBACK");
  expect(pgMock.release).toHaveBeenCalledTimes(1);
});
