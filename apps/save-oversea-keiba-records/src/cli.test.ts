// This test runs with Bun and Vitest.
import { afterEach, expect, test, vi } from "vitest";
import {
  createRealCliDependencies,
  runCli,
  type CliDependencies,
  type FileHandlePort,
  type HttpResponsePort,
  type SaveRunner,
} from "./cli";
import type { MasterLookupStatement } from "./master-lookup";
import type {
  ExecutableSqlStatement,
  PostgresClient,
  PostgresConnectionConfig,
  SqlExecutor,
} from "./pg-client";
import type {
  ParseSaveCliArgsResult,
  RunSaveResult,
  SaveSqlExecutor,
  SaveSqlStatement,
} from "./run-save";

interface PoolConstructorConfig {
  readonly host: string;
  readonly port: number;
  readonly database: string;
  readonly user: string;
  readonly password: string;
}

const pgMock = vi.hoisted(() => ({
  Pool: vi.fn(function Pool(_config: PoolConstructorConfig) {
    return {
      connect: vi.fn(),
      query: vi.fn(),
      end: vi.fn(async (): Promise<void> => undefined),
    };
  }),
}));

vi.mock("pg", () => ({
  Pool: pgMock.Pool,
}));

const SUCCESS_RESULT: RunSaveResult = {
  exitCode: 0,
  wrote: false,
  dryRunVerdict: "safe",
  writeSummary: null,
  networkRequestCount: 0,
};

const FAILURE_RESULT: RunSaveResult = {
  exitCode: 1,
  wrote: false,
  dryRunVerdict: "blocked",
  writeSummary: null,
  networkRequestCount: 0,
};

const SUCCESSFUL_PARSE: ParseSaveCliArgsResult = {
  ok: true,
  args: {
    jraRacecardId: "jra-race",
    secondaryRaceId: "secondary-race",
    apply: false,
    jraCachePath: null,
    secondaryCachePath: null,
    venueCode: "A6",
    raceNumber: "05",
  },
};

interface HarnessInput {
  readonly parseResult: ParseSaveCliArgsResult;
  readonly responseStatus: number;
  readonly fileExists: boolean;
  readonly executeSave: SaveRunner;
}

interface HarnessState {
  parseCalls: number;
  resolveConfigCalls: number;
  createClientCalls: number;
  connectCalls: number;
  endCalls: number;
  transactionCalls: number;
  masterFactoryCalls: number;
  fetchUrls: string[];
  filePaths: string[];
  errors: string[];
  infos: string[];
  statements: SaveSqlStatement[];
  transactionStatements: SaveSqlStatement[];
  masterStatements: MasterLookupStatement[];
}

interface Harness {
  readonly dependencies: CliDependencies;
  readonly state: HarnessState;
}

const createHarness = (input: HarnessInput): Harness => {
  const state: HarnessState = {
    parseCalls: 0,
    resolveConfigCalls: 0,
    createClientCalls: 0,
    connectCalls: 0,
    endCalls: 0,
    transactionCalls: 0,
    masterFactoryCalls: 0,
    fetchUrls: [],
    filePaths: [],
    errors: [],
    infos: [],
    statements: [],
    transactionStatements: [],
    masterStatements: [],
  };

  const client: PostgresClient = {
    execute: async (statement: ExecutableSqlStatement) => {
      state.statements.push(statement);
      return {
        rowCount: 1,
        rows: [{ code: "record" }],
      };
    },
    connect: async (): Promise<void> => {
      state.connectCalls += 1;
    },
    end: async (): Promise<void> => {
      state.endCalls += 1;
    },
    withTransaction: async <T>(callback: (executor: SqlExecutor) => Promise<T>): Promise<T> => {
      state.transactionCalls += 1;
      const transactionExecutor: SqlExecutor = {
        execute: async (statement: ExecutableSqlStatement) => {
          state.transactionStatements.push(statement);
          return {
            rowCount: 2,
            rows: [{ code: "transaction-record" }],
          };
        },
      };
      return callback(transactionExecutor);
    },
  };

  return {
    state,
    dependencies: {
      fetchRequest: async (url: string): Promise<HttpResponsePort> => {
        state.fetchUrls.push(url);
        return {
          status: input.responseStatus,
          text: async (): Promise<string> => "network document",
        };
      },
      openFile: (path: string): FileHandlePort => {
        state.filePaths.push(path);
        return {
          exists: async (): Promise<boolean> => input.fileExists,
          text: async (): Promise<string> => "cached document",
        };
      },
      parseArgs: (): ParseSaveCliArgsResult => {
        state.parseCalls += 1;
        return input.parseResult;
      },
      resolveConfig: (): PostgresConnectionConfig => {
        state.resolveConfigCalls += 1;
        return {
          host: "database-host",
          port: 15432,
          database: "database-name",
          user: "database-user",
          password: "",
        };
      },
      createClient: (): PostgresClient => {
        state.createClientCalls += 1;
        return client;
      },
      createMasterLookupRunner: (executor: SaveSqlExecutor) => {
        state.masterFactoryCalls += 1;
        return async (statement: MasterLookupStatement) => {
          state.masterStatements.push(statement);
          const outcome = await executor.execute({
            text: statement.text,
            values: statement.values,
          });
          return { rows: outcome.rows };
        };
      },
      executeSave: input.executeSave,
      logger: {
        info: (message: string): void => {
          state.infos.push(message);
        },
        error: (message: string): void => {
          state.errors.push(message);
        },
      },
    },
  };
};

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

test("runCli returns success when the HTTP response status is successful", async () => {
  const harness: Harness = createHarness({
    parseResult: SUCCESSFUL_PARSE,
    responseStatus: 200,
    fileExists: true,
    executeSave: async (input): Promise<RunSaveResult> => {
      const html: string = await input.ports.fetchPort.fetchHtml("race-source-document");
      expect(html).toStrictEqual("network document");
      return SUCCESS_RESULT;
    },
  });

  const exitCode: number = await runCli({
    argv: ["jra-race", "secondary-race"],
    env: {},
    dependencies: harness.dependencies,
  });

  expect(exitCode).toBe(0);
  expect(harness.state.fetchUrls).toStrictEqual(["race-source-document"]);
  expect(harness.state.errors).toStrictEqual([]);
  expect(harness.state.connectCalls).toBe(1);
  expect(harness.state.endCalls).toBe(1);
});

test("runCli returns failure for a non-successful HTTP response status", async () => {
  const harness: Harness = createHarness({
    parseResult: SUCCESSFUL_PARSE,
    responseStatus: 503,
    fileExists: true,
    executeSave: async (input): Promise<RunSaveResult> => {
      await input.ports.fetchPort.fetchHtml("unavailable-race-document");
      return SUCCESS_RESULT;
    },
  });

  const exitCode: number = await runCli({
    argv: ["jra-race", "secondary-race"],
    env: {},
    dependencies: harness.dependencies,
  });

  expect(exitCode).toBe(1);
  expect(harness.state.fetchUrls).toStrictEqual(["unavailable-race-document"]);
  expect(harness.state.errors).toStrictEqual(["HTTP 503 while fetching a race source document."]);
  expect(harness.state.endCalls).toBe(1);
});

test("runCli reads a local file when it exists", async () => {
  const harness: Harness = createHarness({
    parseResult: SUCCESSFUL_PARSE,
    responseStatus: 200,
    fileExists: true,
    executeSave: async (input): Promise<RunSaveResult> => {
      const html: string = await input.ports.fileReadPort.readFile("race-cache");
      expect(html).toStrictEqual("cached document");
      return SUCCESS_RESULT;
    },
  });

  const exitCode: number = await runCli({
    argv: ["jra-race", "secondary-race"],
    env: {},
    dependencies: harness.dependencies,
  });

  expect(exitCode).toBe(0);
  expect(harness.state.filePaths).toStrictEqual(["race-cache"]);
  expect(harness.state.errors).toStrictEqual([]);
  expect(harness.state.endCalls).toBe(1);
});

test("runCli returns failure when a local file does not exist", async () => {
  const harness: Harness = createHarness({
    parseResult: SUCCESSFUL_PARSE,
    responseStatus: 200,
    fileExists: false,
    executeSave: async (input): Promise<RunSaveResult> => {
      await input.ports.fileReadPort.readFile("missing-race-cache");
      return SUCCESS_RESULT;
    },
  });

  const exitCode: number = await runCli({
    argv: ["jra-race", "secondary-race"],
    env: {},
    dependencies: harness.dependencies,
  });

  expect(exitCode).toBe(1);
  expect(harness.state.filePaths).toStrictEqual(["missing-race-cache"]);
  expect(harness.state.errors).toStrictEqual(["Local cache file does not exist."]);
  expect(harness.state.endCalls).toBe(1);
});

test("runCli reports argument failure without constructing a database client", async () => {
  const harness: Harness = createHarness({
    parseResult: {
      ok: false,
      message: "Invalid CLI arguments.",
    },
    responseStatus: 200,
    fileExists: true,
    executeSave: async (): Promise<RunSaveResult> => SUCCESS_RESULT,
  });

  const exitCode: number = await runCli({
    argv: [],
    env: {},
    dependencies: harness.dependencies,
  });

  expect(exitCode).toBe(1);
  expect(harness.state.errors).toStrictEqual(["Invalid CLI arguments."]);
  expect(harness.state.resolveConfigCalls).toBe(0);
  expect(harness.state.createClientCalls).toBe(0);
  expect(harness.state.connectCalls).toBe(0);
  expect(harness.state.endCalls).toBe(0);
});

test("runCli wires the executor, master lookup, and transaction adapters", async () => {
  const harness: Harness = createHarness({
    parseResult: SUCCESSFUL_PARSE,
    responseStatus: 200,
    fileExists: true,
    executeSave: async (input): Promise<RunSaveResult> => {
      const direct = await input.ports.executor.execute({
        text: "SELECT direct",
        values: ["direct-value"],
      });
      const master = await input.ports.masterLookupRunner({
        text: "SELECT master",
        values: [["master-value"]],
      });
      const transaction = await input.ports.withTransaction(async (executor: SaveSqlExecutor) =>
        executor.execute({
          text: "SELECT transaction",
          values: ["transaction-value"],
        }),
      );
      expect(direct).toStrictEqual({
        rowCount: 1,
        rows: [{ code: "record" }],
      });
      expect(master).toStrictEqual({
        rows: [{ code: "record" }],
      });
      expect(transaction).toStrictEqual({
        rowCount: 2,
        rows: [{ code: "transaction-record" }],
      });
      return SUCCESS_RESULT;
    },
  });

  const exitCode: number = await runCli({
    argv: ["jra-race", "secondary-race"],
    env: { MARKUP_PROFILE_ENV: "operator-supplied" },
    dependencies: harness.dependencies,
  });

  expect(exitCode).toBe(0);
  expect(harness.state.masterFactoryCalls).toBe(1);
  expect(harness.state.transactionCalls).toBe(1);
  expect(harness.state.statements).toStrictEqual([
    {
      text: "SELECT direct",
      values: ["direct-value"],
    },
    {
      text: "SELECT master",
      values: [["master-value"]],
    },
  ]);
  expect(harness.state.transactionStatements).toStrictEqual([
    {
      text: "SELECT transaction",
      values: ["transaction-value"],
    },
  ]);
  expect(harness.state.masterStatements).toStrictEqual([
    {
      text: "SELECT master",
      values: [["master-value"]],
    },
  ]);
});

test("runCli preserves a failure exit code returned by the save runner", async () => {
  const harness: Harness = createHarness({
    parseResult: SUCCESSFUL_PARSE,
    responseStatus: 200,
    fileExists: true,
    executeSave: async (): Promise<RunSaveResult> => FAILURE_RESULT,
  });

  const exitCode: number = await runCli({
    argv: ["jra-race", "secondary-race"],
    env: {},
    dependencies: harness.dependencies,
  });

  expect(exitCode).toBe(1);
  expect(harness.state.errors).toStrictEqual([]);
  expect(harness.state.endCalls).toBe(1);
});

test("runCli normalizes an Error and closes the database client", async () => {
  const harness: Harness = createHarness({
    parseResult: SUCCESSFUL_PARSE,
    responseStatus: 200,
    fileExists: true,
    executeSave: async (): Promise<RunSaveResult> => {
      throw new Error("Save runner failed.");
    },
  });

  const exitCode: number = await runCli({
    argv: ["jra-race", "secondary-race"],
    env: {},
    dependencies: harness.dependencies,
  });

  expect(exitCode).toBe(1);
  expect(harness.state.errors).toStrictEqual(["Save runner failed."]);
  expect(harness.state.connectCalls).toBe(1);
  expect(harness.state.endCalls).toBe(1);
});

test("runCli normalizes a thrown non-Error value and closes the database client", async () => {
  const harness: Harness = createHarness({
    parseResult: SUCCESSFUL_PARSE,
    responseStatus: 200,
    fileExists: true,
    executeSave: async (): Promise<RunSaveResult> => {
      throw 42;
    },
  });

  const exitCode: number = await runCli({
    argv: ["jra-race", "secondary-race"],
    env: {},
    dependencies: harness.dependencies,
  });

  expect(exitCode).toBe(1);
  expect(harness.state.errors).toStrictEqual(["Unknown CLI failure."]);
  expect(harness.state.endCalls).toBe(1);
});

test("createRealCliDependencies binds Bun, fetch, console, and the client factory", async () => {
  const virtualFile: FileHandlePort = {
    exists: async (): Promise<boolean> => true,
    text: async (): Promise<string> => "runtime cache",
  };
  const fileMock = vi.fn((_path: string): FileHandlePort => virtualFile);
  vi.stubGlobal("Bun", {
    argv: ["bun", "src/main.ts"],
    file: fileMock,
  });
  const fetchSpy = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("runtime response", { status: 200 }));
  const consoleLog = vi.spyOn(console, "log").mockImplementation((): void => undefined);
  const consoleError = vi.spyOn(console, "error").mockImplementation((): void => undefined);
  const dependencies: CliDependencies = createRealCliDependencies();

  const response: HttpResponsePort = await dependencies.fetchRequest("runtime-document");
  dependencies.openFile("runtime-cache");
  dependencies.createClient({
    host: "database-host",
    port: 15432,
    database: "database-name",
    user: "database-user",
    password: "",
  });
  dependencies.logger.info("runtime info");
  dependencies.logger.error("runtime error");

  expect(response.status).toBe(200);
  expect(fetchSpy).toHaveBeenCalledWith("runtime-document");
  expect(fileMock).toHaveBeenCalledWith("runtime-cache");
  expect(pgMock.Pool).toHaveBeenCalledTimes(1);
  expect(consoleLog).toHaveBeenCalledWith("runtime info");
  expect(consoleError).toHaveBeenCalledWith("runtime error");
});

test("main assigns the argument-failure exit code without opening I/O", async () => {
  vi.stubGlobal("Bun", {
    argv: ["bun", "src/main.ts"],
  });
  const consoleError = vi.spyOn(console, "error").mockImplementation((): void => undefined);

  await import("./main");

  expect(process.exitCode).toBe(1);
  expect(consoleError).toHaveBeenCalledTimes(1);
  process.exitCode = undefined;
});
