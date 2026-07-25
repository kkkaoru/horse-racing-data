// This file runs with Bun.
import { Pool } from "pg";

const DEFAULT_POSTGRES_HOST: string = "127.0.0.1";
const DEFAULT_POSTGRES_PORT_TEXT: string = "15432";
const MIN_TCP_PORT: number = 1;
const MAX_TCP_PORT: number = 65535;
const RADIX_DECIMAL: number = 10;
const EMPTY_ROW_COUNT: number = 0;
const BEGIN_SQL: string = "BEGIN";
const COMMIT_SQL: string = "COMMIT";
const ROLLBACK_SQL: string = "ROLLBACK";
const CONNECT_PROBE_SQL: string = "SELECT 1";

const ENV_POSTGRES_HOST: string = "POSTGRES_HOST";
const ENV_POSTGRES_PORT: string = "POSTGRES_PORT";
const ENV_POSTGRES_DB: string = "POSTGRES_DB";
const ENV_POSTGRES_USER: string = "POSTGRES_USER";
const ENV_POSTGRES_PASSWORD: string = "POSTGRES_PASSWORD";

export type QueryParameter = string | readonly string[];

export interface ExecutableSqlStatement {
  readonly text: string;
  readonly values: readonly QueryParameter[];
}

export interface PostgresConnectionConfig {
  readonly host: string;
  readonly port: number;
  readonly database: string;
  readonly user: string;
  readonly password: string;
}

export interface PostgresQueryOutcome {
  readonly rowCount: number;
  readonly rows: readonly Readonly<Record<string, string>>[];
}

export interface SqlExecutor {
  readonly execute: (statement: ExecutableSqlStatement) => Promise<PostgresQueryOutcome>;
}

export interface PostgresQueryResult {
  readonly rowCount: number | null;
  readonly rows: readonly Readonly<Record<string, unknown>>[];
}

export interface PostgresPoolClient {
  readonly query: (
    text: string,
    values?: readonly QueryParameter[],
  ) => Promise<PostgresQueryResult>;
  readonly release: () => void;
}

export interface PostgresPool {
  readonly connect: () => Promise<PostgresPoolClient>;
  readonly query: (
    text: string,
    values?: readonly QueryParameter[],
  ) => Promise<PostgresQueryResult>;
  readonly end: () => Promise<void>;
}

export interface PostgresPoolFactory {
  (config: PostgresConnectionConfig): PostgresPool;
}

export interface CreatePostgresClientInput {
  readonly config: PostgresConnectionConfig;
  readonly createPool?: PostgresPoolFactory;
}

export interface PostgresClient extends SqlExecutor {
  readonly connect: () => Promise<void>;
  readonly end: () => Promise<void>;
  readonly withTransaction: <T>(callback: (executor: SqlExecutor) => Promise<T>) => Promise<T>;
}

interface TransactionState {
  began: boolean;
}

interface RequiredEnvInput {
  readonly env: Record<string, string | undefined>;
  readonly name: string;
}

const readRequiredEnv = ({ env, name }: RequiredEnvInput): string => {
  const value: string | undefined = env[name];
  if (value === undefined || value.length === 0) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
};

const resolveHost = (env: Record<string, string | undefined>): string => {
  const value: string | undefined = env[ENV_POSTGRES_HOST];
  if (value === undefined || value.length === 0) {
    return DEFAULT_POSTGRES_HOST;
  }
  return value;
};

const resolvePort = (env: Record<string, string | undefined>): number => {
  const value: string | undefined = env[ENV_POSTGRES_PORT];
  const portText: string =
    value === undefined || value.length === 0 ? DEFAULT_POSTGRES_PORT_TEXT : value;
  const port: number = Number.parseInt(portText, RADIX_DECIMAL);
  if (!Number.isInteger(port) || port < MIN_TCP_PORT || port > MAX_TCP_PORT) {
    throw new Error(
      `Invalid ${ENV_POSTGRES_PORT}: must be an integer between ${String(MIN_TCP_PORT)} and ${String(MAX_TCP_PORT)}`,
    );
  }
  return port;
};

export const resolvePostgresConfig = (
  env: Record<string, string | undefined>,
): PostgresConnectionConfig => ({
  host: resolveHost(env),
  port: resolvePort(env),
  database: readRequiredEnv({ env, name: ENV_POSTGRES_DB }),
  user: readRequiredEnv({ env, name: ENV_POSTGRES_USER }),
  password: readRequiredEnv({ env, name: ENV_POSTGRES_PASSWORD }),
});

const normalizeCell = (value: unknown): string => {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return String(value);
  }
  return "";
};

const normalizeRow = (row: Readonly<Record<string, unknown>>): Readonly<Record<string, string>> =>
  Object.fromEntries(
    Object.entries(row).map(([key, value]: [string, unknown]): [string, string] => [
      key,
      normalizeCell(value),
    ]),
  );

const toQueryOutcome = (result: PostgresQueryResult): PostgresQueryOutcome => ({
  rowCount: result.rowCount === null ? EMPTY_ROW_COUNT : result.rowCount,
  rows: result.rows.map(normalizeRow),
});

const executeOnTarget = async (
  target: PostgresPool | PostgresPoolClient,
  statement: ExecutableSqlStatement,
): Promise<PostgresQueryOutcome> => {
  const result: PostgresQueryResult = await target.query(statement.text, statement.values);
  return toQueryOutcome(result);
};

const createDefaultPool: PostgresPoolFactory = (config: PostgresConnectionConfig): PostgresPool => {
  const pool: Pool = new Pool({
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
    password: config.password,
  });
  return {
    connect: async (): Promise<PostgresPoolClient> => {
      const client = await pool.connect();
      return {
        query: async (
          text: string,
          values: readonly QueryParameter[] = [],
        ): Promise<PostgresQueryResult> => {
          const result = await client.query(text, [...values]);
          return {
            rowCount: result.rowCount,
            rows: result.rows,
          };
        },
        release: (): void => {
          client.release();
        },
      };
    },
    query: async (
      text: string,
      values: readonly QueryParameter[] = [],
    ): Promise<PostgresQueryResult> => {
      const result = await pool.query(text, [...values]);
      return {
        rowCount: result.rowCount,
        rows: result.rows,
      };
    },
    end: (): Promise<void> => pool.end(),
  };
};

export const createPostgresClient = (input: CreatePostgresClientInput): PostgresClient => {
  const factory: PostgresPoolFactory =
    input.createPool === undefined ? createDefaultPool : input.createPool;
  const pool: PostgresPool = factory(input.config);

  const execute = (statement: ExecutableSqlStatement): Promise<PostgresQueryOutcome> =>
    executeOnTarget(pool, statement);

  const connect = async (): Promise<void> => {
    const client: PostgresPoolClient = await pool.connect();
    try {
      await client.query(CONNECT_PROBE_SQL);
    } finally {
      client.release();
    }
  };

  const end = (): Promise<void> => pool.end();

  const withTransaction = async <T>(
    callback: (executor: SqlExecutor) => Promise<T>,
  ): Promise<T> => {
    const client: PostgresPoolClient = await pool.connect();
    const state: TransactionState = { began: false };
    try {
      await client.query(BEGIN_SQL);
      state.began = true;
      const executor: SqlExecutor = {
        execute: (statement: ExecutableSqlStatement): Promise<PostgresQueryOutcome> =>
          executeOnTarget(client, statement),
      };
      const value: T = await callback(executor);
      await client.query(COMMIT_SQL);
      return value;
    } catch (error: unknown) {
      if (state.began) {
        await client.query(ROLLBACK_SQL);
      }
      throw error;
    } finally {
      client.release();
    }
  };

  return {
    execute,
    connect,
    end,
    withTransaction,
  };
};
