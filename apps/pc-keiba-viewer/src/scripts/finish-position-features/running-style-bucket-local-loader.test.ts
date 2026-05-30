// Run with: bunx vitest run src/scripts/finish-position-features/running-style-bucket-local-loader.test.ts
import { expect, test, vi } from "vitest";

import type { SpawnFn, SpawnOptions, SpawnedChildLike } from "./running-style-bucket-local-loader";
import {
  DEFAULT_TEMP_TABLE_NAME,
  buildRunningStyleLoaderArgv,
  createRunningStyleBucketEvalRpcClient,
} from "./running-style-bucket-local-loader";

interface PendingRead {
  resolve: (chunk: { done: boolean; value: Uint8Array | undefined }) => void;
}

interface MockChild {
  writes: string[];
  ended: boolean;
  exitResolve: (code: number) => void;
  emit: (chunk: string) => void;
  finish: () => void;
}

const buildMockSpawn = (): { mock: MockChild; spawn: SpawnFn } => {
  const writes: string[] = [];
  const mockState: MockChild = {
    writes,
    ended: false,
    exitResolve: () => {},
    emit: () => {},
    finish: () => {},
  };
  const queue: Uint8Array[] = [];
  const pending: PendingRead[] = [];
  let finished = false;
  const drain = (): void => {
    while (queue.length > 0 && pending.length > 0) {
      const next = queue.shift();
      const waiter = pending.shift();
      if (next !== undefined && waiter !== undefined) {
        waiter.resolve({ done: false, value: next });
      }
    }
    if (finished) {
      pending.forEach((waiter) => {
        waiter.resolve({ done: true, value: undefined });
      });
      pending.length = 0;
    }
  };
  mockState.emit = (chunk: string) => {
    queue.push(new TextEncoder().encode(chunk));
    drain();
  };
  mockState.finish = () => {
    finished = true;
    drain();
  };
  const exited = new Promise<number>((resolve) => {
    mockState.exitResolve = resolve;
  });
  const spawn: SpawnFn = (cmd, options) => {
    void cmd;
    void options;
    const reader = {
      read: (): Promise<{ done: boolean; value: Uint8Array | undefined }> => {
        const next = queue.shift();
        if (next !== undefined) {
          return Promise.resolve({ done: false, value: next });
        }
        if (finished) {
          return Promise.resolve({ done: true, value: undefined });
        }
        return new Promise((resolve) => {
          pending.push({ resolve });
        });
      },
    };
    const proc: SpawnedChildLike = {
      stdin: {
        write: (chunk) => {
          writes.push(chunk);
        },
        end: () => {
          mockState.ended = true;
        },
      },
      stdout: {
        getReader: () => reader,
      },
      exited,
    };
    return proc;
  };
  return { mock: mockState, spawn };
};

test("DEFAULT_TEMP_TABLE_NAME is bucket_running_style_predictions_loaded", () => {
  expect(DEFAULT_TEMP_TABLE_NAME).toBe("bucket_running_style_predictions_loaded");
});

test("buildRunningStyleLoaderArgv emits flags for jra category", () => {
  expect(
    buildRunningStyleLoaderArgv({
      pythonPath: "uv",
      pgUrl: "postgres://x",
      predictionsParquetGlob: "/tmp/p/**/*.parquet",
      tempTableName: "bucket_running_style_predictions_loaded",
      runningStyleFeatureVersion: "v1",
      category: "jra",
      yearFrom: 2005,
      yearTo: 2026,
    }),
  ).toStrictEqual([
    "uv",
    "run",
    "python",
    "src/scripts/load_running_style_predictions.py",
    "--pg-url",
    "postgres://x",
    "--predictions-parquet-glob",
    "/tmp/p/**/*.parquet",
    "--temp-table-name",
    "bucket_running_style_predictions_loaded",
    "--category",
    "jra",
    "--year-from",
    "2005",
    "--year-to",
    "2026",
    "--running-style-feature-version",
    "v1",
  ]);
});

test("buildRunningStyleLoaderArgv emits flags for nar category", () => {
  expect(
    buildRunningStyleLoaderArgv({
      pythonPath: "uv",
      pgUrl: "postgres://x",
      predictionsParquetGlob: "/p/**/*.parquet",
      tempTableName: "bucket_running_style_predictions_loaded",
      runningStyleFeatureVersion: "v2",
      category: "nar",
      yearFrom: 2010,
      yearTo: 2015,
    }),
  ).toStrictEqual([
    "uv",
    "run",
    "python",
    "src/scripts/load_running_style_predictions.py",
    "--pg-url",
    "postgres://x",
    "--predictions-parquet-glob",
    "/p/**/*.parquet",
    "--temp-table-name",
    "bucket_running_style_predictions_loaded",
    "--category",
    "nar",
    "--year-from",
    "2010",
    "--year-to",
    "2015",
    "--running-style-feature-version",
    "v2",
  ]);
});

test("createRunningStyleBucketEvalRpcClient invokes spawn with python-loader argv", () => {
  const { spawn } = buildMockSpawn();
  const spawnSpy = vi.fn<SpawnFn>(spawn);
  createRunningStyleBucketEvalRpcClient(
    {
      pythonPath: "uv",
      pgUrl: "postgres://x",
      predictionsParquetGlob: "/tmp/p/**/*.parquet",
      runningStyleFeatureVersion: "v1",
      category: "jra",
      yearFrom: 2005,
      yearTo: 2026,
    },
    { spawn: spawnSpy },
  );
  expect(spawnSpy).toHaveBeenCalledWith(
    [
      "uv",
      "run",
      "python",
      "src/scripts/load_running_style_predictions.py",
      "--pg-url",
      "postgres://x",
      "--predictions-parquet-glob",
      "/tmp/p/**/*.parquet",
      "--temp-table-name",
      "bucket_running_style_predictions_loaded",
      "--category",
      "jra",
      "--year-from",
      "2005",
      "--year-to",
      "2026",
      "--running-style-feature-version",
      "v1",
    ],
    {
      stdin: "pipe",
      stdout: "pipe",
      stderr: "inherit",
    } satisfies SpawnOptions,
  );
});

test("createRunningStyleBucketEvalRpcClient honours custom temp-table-name", () => {
  const { spawn } = buildMockSpawn();
  const spawnSpy = vi.fn<SpawnFn>(spawn);
  createRunningStyleBucketEvalRpcClient(
    {
      pythonPath: "uv",
      pgUrl: "postgres://x",
      predictionsParquetGlob: "/p/**/*.parquet",
      tempTableName: "custom_table",
      runningStyleFeatureVersion: "v1",
      category: "nar",
      yearFrom: 2020,
      yearTo: 2021,
    },
    { spawn: spawnSpy },
  );
  const argv = spawnSpy.mock.calls[0]?.[0] ?? [];
  expect(argv[9]).toBe("custom_table");
});

test("createRunningStyleBucketEvalRpcClient returns BucketEvalRpcClient with ready/query/exec/close", () => {
  const { spawn } = buildMockSpawn();
  const client = createRunningStyleBucketEvalRpcClient(
    {
      pythonPath: "uv",
      pgUrl: "postgres://x",
      predictionsParquetGlob: "/p/**/*.parquet",
      runningStyleFeatureVersion: "v1",
      category: "jra",
      yearFrom: 2024,
      yearTo: 2024,
    },
    { spawn },
  );
  expect(typeof client.query).toBe("function");
  expect(typeof client.exec).toBe("function");
  expect(typeof client.close).toBe("function");
  expect(client.ready instanceof Promise).toBe(true);
});

test("createRunningStyleBucketEvalRpcClient close awaits both inner client close and proc exited", async () => {
  const { mock, spawn } = buildMockSpawn();
  const client = createRunningStyleBucketEvalRpcClient(
    {
      pythonPath: "uv",
      pgUrl: "postgres://x",
      predictionsParquetGlob: "/p/**/*.parquet",
      runningStyleFeatureVersion: "v1",
      category: "jra",
      yearFrom: 2024,
      yearTo: 2024,
    },
    { spawn },
  );
  mock.emit('{"type":"ready","loadedRows":3}\n');
  await client.ready;
  const closing = client.close();
  expect(mock.writes.at(-1)).toBe('{"type":"exit"}\n');
  mock.emit('{"type":"closed"}\n');
  mock.exitResolve(0);
  await closing;
  expect(mock.ended).toBe(true);
});

test("createRunningStyleBucketEvalRpcClient pumps stdout chunks into the inner client", async () => {
  const { mock, spawn } = buildMockSpawn();
  const client = createRunningStyleBucketEvalRpcClient(
    {
      pythonPath: "uv",
      pgUrl: "postgres://x",
      predictionsParquetGlob: "/p/**/*.parquet",
      runningStyleFeatureVersion: "v1",
      category: "jra",
      yearFrom: 2024,
      yearTo: 2024,
    },
    { spawn },
  );
  mock.emit('{"type":"ready","loadedRows":7}\n');
  await expect(client.ready).resolves.toStrictEqual({ type: "ready", loadedRows: 7 });
});

// Note: TypeScript type signatures do not allow category: "ban-ei" because
// the CLI args union is "jra" | "nar". This restricts ban-ei at the
// compile-time level; runtime validation lives in the Python loader's
// argparse choices. We cannot dynamically test a compile error, so this is
// documented by the type union above and verified by tsc on commit.
