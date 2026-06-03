// Run with: bun run src/scripts/finish-position-features/evaluate-bucket-21y-v8.ts \
//   --pg-url $DATABASE_URL_LOCAL --running-style-feature-version v3 --finish-position-version v1 \
//   --model-version X --category jra --predictions-root tmp/bucket-eval/finish-position/X/predictions
//
// Stage 0B v8 helper script. Generalized fork of evaluate-bucket-21y-v7lineage
// that runs the existing single-category bucket aggregate + REPLACE upsert for
// ONE model_version + ONE category at a time. Idempotent: the underlying
// bucket-upsert SQL already uses ON CONFLICT DO UPDATE so re-running the same
// model_version + category window simply refreshes the per-bucket sums (no
// DELETE, per project hard rule against odds_snapshots / DB-wide deletes).
// Reuses the shared chunk loader, bucket aggregate SQL, and DDL helpers from
// evaluate-bucket-21y.ts verbatim.

import { Pool } from "pg";

import { createBucketEvalRpcClient } from "./bucket-eval-rpc-client";
import type { BucketEvalRpcChildLike } from "./bucket-eval-rpc-client";
import type {
  BucketChunkClient,
  BucketChunkLoaderArgs,
  BucketEvalCliOptions,
  BucketQueryRunner,
  CategoryYearWindow,
  RunBucketEvalDeps,
} from "./evaluate-bucket-21y";
import {
  buildPythonLoaderArgv,
  initialOptions as baseInitialOptions,
  runBucketEval,
} from "./evaluate-bucket-21y";
import { buildBucketEvaluationsDdl } from "./evaluate-bucket-predictions-sql";

export type V8Category = "jra" | "nar" | "ban-ei";

export interface V8CliOptions {
  pgUrl: string;
  runningStyleFeatureVersion: string;
  finishPositionVersion: string;
  modelVersion: string;
  category: V8Category;
  predictionsRoot: string;
  maxYearsPerRun: number;
  statementTimeoutMs: number;
  ignoreNightWindow: boolean;
}

const DEFAULT_PG_URL = "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing";
const DEFAULT_MAX_YEARS_PER_RUN = 5;
const DEFAULT_STATEMENT_TIMEOUT_MS = 900_000;

const JRA_YEARS = [
  2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022,
  2023, 2024, 2025, 2026,
];
const NAR_YEARS = [
  2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022,
  2023, 2024, 2025, 2026,
];
const BAN_EI_YEARS = [
  2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023,
  2024, 2025, 2026,
];

interface ApplyArgResult {
  advanceBy: number;
}

export const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/evaluate-bucket-21y-v8.ts \\",
    "    --running-style-feature-version <v3> \\",
    "    --finish-position-version <v1> \\",
    "    --model-version <model-version> \\",
    "    --category <jra|nar|ban-ei> \\",
    "    --predictions-root <dir> \\",
    "    [--pg-url <connection-string>] \\",
    "    [--max-years-per-run 5] \\",
    "    [--statement-timeout-ms 900000] \\",
    "    [--ignore-night-window]",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

const isV8Category = (value: string): value is V8Category =>
  value === "jra" || value === "nar" || value === "ban-ei";

const parseV8Category = (raw: string): V8Category => {
  if (isV8Category(raw)) return raw;
  throw new Error(`--category must be one of jra | nar | ban-ei. Got: ${raw}`);
};

export const initialOptions = (): V8CliOptions => ({
  pgUrl: process.env.DATABASE_URL_LOCAL ?? DEFAULT_PG_URL,
  runningStyleFeatureVersion: "",
  finishPositionVersion: "",
  modelVersion: "",
  category: "jra",
  predictionsRoot: "",
  maxYearsPerRun: DEFAULT_MAX_YEARS_PER_RUN,
  statementTimeoutMs: DEFAULT_STATEMENT_TIMEOUT_MS,
  ignoreNightWindow: false,
});

const applyArg = (
  options: V8CliOptions,
  name: string,
  value: string | undefined,
): ApplyArgResult => {
  if (name === "--pg-url") {
    options.pgUrl = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--running-style-feature-version") {
    options.runningStyleFeatureVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--finish-position-version") {
    options.finishPositionVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version") {
    options.modelVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--category") {
    options.category = parseV8Category(requireValue(name, value));
    return { advanceBy: 2 };
  }
  if (name === "--predictions-root") {
    options.predictionsRoot = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--max-years-per-run") {
    options.maxYearsPerRun = Number.parseInt(requireValue(name, value), 10);
    return { advanceBy: 2 };
  }
  if (name === "--statement-timeout-ms") {
    options.statementTimeoutMs = Number.parseInt(requireValue(name, value), 10);
    return { advanceBy: 2 };
  }
  if (name === "--ignore-night-window") {
    options.ignoreNightWindow = true;
    return { advanceBy: 1 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

const consumeArgs = (
  options: V8CliOptions,
  argv: readonly string[],
  cursor: number,
): V8CliOptions => {
  const name = argv[cursor];
  if (name === undefined) return options;
  const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
  return consumeArgs(options, argv, cursor + advanceBy);
};

export const parseArgs = (argv: readonly string[]): V8CliOptions => {
  const options = consumeArgs(initialOptions(), argv, 0);
  if (options.runningStyleFeatureVersion === "") {
    throw new Error("--running-style-feature-version is required.");
  }
  if (options.finishPositionVersion === "") {
    throw new Error("--finish-position-version is required.");
  }
  if (options.modelVersion === "") throw new Error("--model-version is required.");
  if (options.predictionsRoot === "") throw new Error("--predictions-root is required.");
  return options;
};

export const resolveCategoryYears = (category: V8Category): number[] => {
  if (category === "jra") return JRA_YEARS;
  if (category === "nar") return NAR_YEARS;
  return BAN_EI_YEARS;
};

export const buildCategoryYearWindow = (options: V8CliOptions): CategoryYearWindow => ({
  category: options.category,
  years: resolveCategoryYears(options.category),
});

export const buildBaseBucketOptions = (options: V8CliOptions): BucketEvalCliOptions => ({
  ...baseInitialOptions(),
  pgUrl: options.pgUrl,
  runningStyleFeatureVersion: options.runningStyleFeatureVersion,
  finishPositionVersion: options.finishPositionVersion,
  modelVersion: options.modelVersion,
  maxYearsPerRun: options.maxYearsPerRun,
  statementTimeoutMs: options.statementTimeoutMs,
  ignoreNightWindow: options.ignoreNightWindow,
  predictionsRoot: options.predictionsRoot,
});

export interface RunV8Deps {
  pool: BucketQueryRunner;
  openChunkClient: (args: BucketChunkLoaderArgs) => Promise<BucketChunkClient>;
  sleep: (ms: number) => Promise<void>;
  log: (message: string) => void;
}

export interface V8RunResult {
  category: V8Category;
  modelVersion: string;
  totalRows: number;
  totalRaces: number;
}

const ensureBucketTable = async (pool: BucketQueryRunner): Promise<void> => {
  await pool.query(buildBucketEvaluationsDdl());
};

export const runV8BucketEval = async (
  deps: RunV8Deps,
  options: V8CliOptions,
): Promise<V8RunResult> => {
  await ensureBucketTable(deps.pool);
  const window = buildCategoryYearWindow(options);
  const bucketOptions = buildBaseBucketOptions(options);
  const bucketDeps: RunBucketEvalDeps = {
    pool: deps.pool,
    openChunkClient: deps.openChunkClient,
    sleep: deps.sleep,
    log: deps.log,
  };
  deps.log(`Begin v8 category=${options.category} model_version=${options.modelVersion}`);
  const result = await runBucketEval(bucketDeps, {
    options: bucketOptions,
    windows: [window],
  });
  return {
    category: options.category,
    modelVersion: options.modelVersion,
    totalRows: result.totalRows,
    totalRaces: result.totalRaces,
  };
};

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((r) => {
    setTimeout(r, ms);
  });

const defaultLog = (message: string): void => {
  console.log(`[bucket-eval-v8] ${message}`);
};

const buildChildFromProc = (proc: ReturnType<typeof Bun.spawn>): BucketEvalRpcChildLike => ({
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

const openChunkClientImpl = async (args: BucketChunkLoaderArgs): Promise<BucketChunkClient> => {
  const argv = buildPythonLoaderArgv(args);
  const proc = Bun.spawn(argv, { stdin: "pipe", stdout: "pipe", stderr: "inherit" });
  const client = createBucketEvalRpcClient({ child: buildChildFromProc(proc) });
  const ready = await client.ready;
  return {
    runner: client,
    loadedRows: ready.loadedRows,
    close: async () => {
      await client.close();
      await proc.exited;
    },
  };
};

const main = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  const pool = new Pool({ connectionString: options.pgUrl });
  try {
    const result = await runV8BucketEval(
      { pool, openChunkClient: openChunkClientImpl, sleep: defaultSleep, log: defaultLog },
      options,
    );
    console.log(JSON.stringify(result));
  } finally {
    await pool.end();
  }
};

if (import.meta.main) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}

export { applyArg, isV8Category, parseV8Category };
