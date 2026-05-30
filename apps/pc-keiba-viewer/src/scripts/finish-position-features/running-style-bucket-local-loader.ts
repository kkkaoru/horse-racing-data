// Run with: imported by Agent RS-A's evaluate-running-style-bucket-21y.ts (bun runtime).
// Spawns the Python loader (load_running_style_predictions.py) as a child
// process and wraps its stdin/stdout into the generic BucketEvalRpcClient so
// the TypeScript driver can route bucket aggregate / upsert SQL into the same
// PG session that owns the temp table. The returned client's close() awaits
// both the inner RPC close handshake and proc.exited so the driver does not
// have to track the child process explicitly.

import type { BucketEvalRpcChildLike, BucketEvalRpcClient } from "./bucket-eval-rpc-client";
import { createBucketEvalRpcClient } from "./bucket-eval-rpc-client";

export interface CreateRunningStyleBucketEvalRpcClientArgs {
  pythonPath: string;
  pgUrl: string;
  predictionsParquetGlob: string;
  tempTableName?: string;
  runningStyleFeatureVersion: string;
  category: "jra" | "nar";
  yearFrom: number;
  yearTo: number;
}

export interface RunningStyleLoaderArgv {
  pythonPath: string;
  pgUrl: string;
  predictionsParquetGlob: string;
  tempTableName: string;
  runningStyleFeatureVersion: string;
  category: "jra" | "nar";
  yearFrom: number;
  yearTo: number;
}

export interface SpawnedChildStdoutLike {
  getReader: () => {
    read: () => Promise<{ done: boolean; value: Uint8Array | undefined }>;
  };
}

export interface SpawnedChildLike {
  stdin: { write: (chunk: string) => void; end: () => void };
  stdout: SpawnedChildStdoutLike;
  exited: Promise<number>;
}

export interface SpawnOptions {
  stdin: "pipe";
  stdout: "pipe";
  stderr: "inherit";
}

export type SpawnFn = (cmd: string[], options: SpawnOptions) => SpawnedChildLike;

export interface CreateRunningStyleBucketEvalRpcClientDeps {
  spawn?: SpawnFn;
}

export const DEFAULT_TEMP_TABLE_NAME = "bucket_running_style_predictions_loaded";

const PYTHON_LOADER_SCRIPT = "src/scripts/load_running_style_predictions.py";

const SPAWN_OPTIONS: SpawnOptions = {
  stdin: "pipe",
  stdout: "pipe",
  stderr: "inherit",
};

const resolveTempTableName = (override: string | undefined): string =>
  override ?? DEFAULT_TEMP_TABLE_NAME;

export const buildRunningStyleLoaderArgv = (args: RunningStyleLoaderArgv): string[] => [
  args.pythonPath,
  "run",
  "python",
  PYTHON_LOADER_SCRIPT,
  "--pg-url",
  args.pgUrl,
  "--predictions-parquet-glob",
  args.predictionsParquetGlob,
  "--temp-table-name",
  args.tempTableName,
  "--category",
  args.category,
  "--year-from",
  String(args.yearFrom),
  "--year-to",
  String(args.yearTo),
  "--running-style-feature-version",
  args.runningStyleFeatureVersion,
];

const wrapChildStdio = (proc: SpawnedChildLike): BucketEvalRpcChildLike => ({
  stdin: {
    write: (chunk: string) => {
      proc.stdin.write(chunk);
      return true;
    },
    end: () => {
      proc.stdin.end();
    },
  },
  stdout: {
    on: (_event, listener) => {
      const reader = proc.stdout.getReader();
      const decoder = new TextDecoder();
      const pump = (): Promise<void> =>
        reader.read().then((next) => {
          if (next.done) return Promise.resolve();
          listener(decoder.decode(next.value));
          return pump();
        });
      void pump();
    },
  },
});

const buildLoaderArgvFromCliArgs = (args: CreateRunningStyleBucketEvalRpcClientArgs): string[] =>
  buildRunningStyleLoaderArgv({
    pythonPath: args.pythonPath,
    pgUrl: args.pgUrl,
    predictionsParquetGlob: args.predictionsParquetGlob,
    tempTableName: resolveTempTableName(args.tempTableName),
    runningStyleFeatureVersion: args.runningStyleFeatureVersion,
    category: args.category,
    yearFrom: args.yearFrom,
    yearTo: args.yearTo,
  });

const defaultSpawn: SpawnFn = (cmd, options) => Bun.spawn(cmd, options) satisfies SpawnedChildLike;

const wrapCloseWithProcExit = (
  inner: BucketEvalRpcClient,
  exited: Promise<number>,
): BucketEvalRpcClient => ({
  ready: inner.ready,
  query: inner.query,
  exec: inner.exec,
  close: async () => {
    await inner.close();
    await exited;
  },
});

export const createRunningStyleBucketEvalRpcClient = (
  args: CreateRunningStyleBucketEvalRpcClientArgs,
  deps?: CreateRunningStyleBucketEvalRpcClientDeps,
): BucketEvalRpcClient => {
  const spawn = deps?.spawn ?? defaultSpawn;
  const argv = buildLoaderArgvFromCliArgs(args);
  const proc = spawn(argv, SPAWN_OPTIONS);
  const child = wrapChildStdio(proc);
  const inner = createBucketEvalRpcClient({ child });
  return wrapCloseWithProcExit(inner, proc.exited);
};
