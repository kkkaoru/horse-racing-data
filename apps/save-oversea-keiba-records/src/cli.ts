// This CLI module runs with Bun.
import {
  createPostgresClient,
  resolvePostgresConfig,
  type PostgresClient,
  type PostgresConnectionConfig,
  type SqlExecutor,
} from "./storage/pg-client";
import {
  createMasterLookupRunnerFromExecutor,
  parseSaveCliArgs,
  runSave,
  EXIT_FAILURE,
  type LoggerPort,
  type ParseSaveCliArgsResult,
  type RunSaveInput,
  type RunSaveResult,
  type SaveExitCode,
  type SaveSqlExecutor,
} from "./run-save";
import type { MasterLookupQueryRunner } from "./storage/master-lookup";
import type { FileReadPort, HtmlFetchPort } from "./sources/source-loader";

const HTTP_OK_MINIMUM: number = 200;
const HTTP_OK_MAXIMUM_EXCLUSIVE: number = 300;
const UNKNOWN_FAILURE_MESSAGE: string = "Unknown CLI failure.";

export interface HttpResponsePort {
  readonly status: number;
  readonly text: () => Promise<string>;
}

export interface FileHandlePort {
  readonly exists: () => Promise<boolean>;
  readonly text: () => Promise<string>;
}

export interface FetchRequest {
  (url: string): Promise<HttpResponsePort>;
}

export interface FileHandleFactory {
  (path: string): FileHandlePort;
}

export interface PostgresConfigResolver {
  (env: Record<string, string | undefined>): PostgresConnectionConfig;
}

export interface PostgresClientFactory {
  (config: PostgresConnectionConfig): PostgresClient;
}

export interface MasterLookupRunnerFactory {
  (executor: SaveSqlExecutor): MasterLookupQueryRunner;
}

export interface SaveRunner {
  (input: RunSaveInput): Promise<RunSaveResult>;
}

export interface CliDependencies {
  readonly fetchRequest: FetchRequest;
  readonly openFile: FileHandleFactory;
  readonly parseArgs: (argv: readonly string[]) => ParseSaveCliArgsResult;
  readonly resolveConfig: PostgresConfigResolver;
  readonly createClient: PostgresClientFactory;
  readonly createMasterLookupRunner: MasterLookupRunnerFactory;
  readonly executeSave: SaveRunner;
  readonly logger: LoggerPort;
}

export interface RunCliInput {
  readonly argv: readonly string[];
  readonly env: Record<string, string | undefined>;
  readonly dependencies: CliDependencies;
}

interface CreateSavePortsInput {
  readonly client: PostgresClient;
  readonly fetchRequest: FetchRequest;
  readonly openFile: FileHandleFactory;
  readonly createMasterLookupRunner: MasterLookupRunnerFactory;
  readonly logger: LoggerPort;
}

interface RunConnectedCliInput {
  readonly argv: readonly string[];
  readonly env: Record<string, string | undefined>;
  readonly client: PostgresClient;
  readonly dependencies: CliDependencies;
}

const toSaveExecutor = (executor: SqlExecutor): SaveSqlExecutor => ({
  execute: async (statement) => {
    const outcome = await executor.execute({
      text: statement.text,
      values: statement.values,
    });
    return {
      rowCount: outcome.rowCount,
      rows: outcome.rows,
    };
  },
});

const createFetchPort = (fetchRequest: FetchRequest): HtmlFetchPort => ({
  fetchHtml: async (url: string): Promise<string> => {
    const response: HttpResponsePort = await fetchRequest(url);
    if (response.status < HTTP_OK_MINIMUM || response.status >= HTTP_OK_MAXIMUM_EXCLUSIVE) {
      throw new Error(`HTTP ${String(response.status)} while fetching a race source document.`);
    }
    return response.text();
  },
});

const createFileReadPort = (openFile: FileHandleFactory): FileReadPort => ({
  readFile: async (path: string): Promise<string> => {
    const file: FileHandlePort = openFile(path);
    const exists: boolean = await file.exists();
    if (!exists) {
      throw new Error("Local cache file does not exist.");
    }
    return file.text();
  },
});

const createSavePorts = ({
  client,
  fetchRequest,
  openFile,
  createMasterLookupRunner,
  logger,
}: CreateSavePortsInput): RunSaveInput["ports"] => {
  const executor: SaveSqlExecutor = toSaveExecutor(client);
  return {
    fetchPort: createFetchPort(fetchRequest),
    fileReadPort: createFileReadPort(openFile),
    masterLookupRunner: createMasterLookupRunner(executor),
    executor,
    withTransaction: async <T>(
      callback: (transactionExecutor: SaveSqlExecutor) => Promise<T>,
    ): Promise<T> =>
      client.withTransaction(
        async (transactionExecutor: SqlExecutor): Promise<T> =>
          callback(toSaveExecutor(transactionExecutor)),
      ),
    logger,
  };
};

const runConnectedCli = async ({
  argv,
  env,
  client,
  dependencies,
}: RunConnectedCliInput): Promise<SaveExitCode> => {
  try {
    await client.connect();
    const result: RunSaveResult = await dependencies.executeSave({
      argv,
      env,
      ports: createSavePorts({
        client,
        fetchRequest: dependencies.fetchRequest,
        openFile: dependencies.openFile,
        createMasterLookupRunner: dependencies.createMasterLookupRunner,
        logger: dependencies.logger,
      }),
    });
    return result.exitCode;
  } finally {
    await client.end();
  }
};

const normalizeError = (error: unknown): string =>
  error instanceof Error ? error.message : UNKNOWN_FAILURE_MESSAGE;

export const runCli = async ({ argv, env, dependencies }: RunCliInput): Promise<SaveExitCode> => {
  const parsed: ParseSaveCliArgsResult = dependencies.parseArgs(argv);
  if (!parsed.ok) {
    dependencies.logger.error(parsed.message);
    return EXIT_FAILURE;
  }

  try {
    const config: PostgresConnectionConfig = dependencies.resolveConfig(env);
    const client: PostgresClient = dependencies.createClient(config);
    return await runConnectedCli({ argv, env, client, dependencies });
  } catch (error: unknown) {
    dependencies.logger.error(normalizeError(error));
    return EXIT_FAILURE;
  }
};

export const createRealCliDependencies = (): CliDependencies => ({
  fetchRequest: (url: string): Promise<HttpResponsePort> => fetch(url),
  openFile: (path: string): FileHandlePort => Bun.file(path),
  parseArgs: parseSaveCliArgs,
  resolveConfig: resolvePostgresConfig,
  createClient: (config: PostgresConnectionConfig): PostgresClient =>
    createPostgresClient({ config }),
  createMasterLookupRunner: createMasterLookupRunnerFromExecutor,
  executeSave: runSave,
  logger: {
    info: (message: string): void => {
      console.log(message);
    },
    error: (message: string): void => {
      console.error(message);
    },
  },
});
