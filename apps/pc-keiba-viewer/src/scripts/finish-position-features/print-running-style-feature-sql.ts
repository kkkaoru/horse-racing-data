// Run with: bun run src/scripts/finish-position-features/print-running-style-feature-sql.ts \
//   --source jra --from-date 20060101 --to-date 20061231 --feature-version v1
//
// Thin wrapper that imports the production batch builder
// `buildRunningStyleBatchFeatureSql` from `apps/sync-realtime-data` and
// writes the resulting SQL string to stdout. This keeps the Python DuckDB
// execute path in sync with the production Postgres/Hyperdrive feature SQL —
// the CLI is the single source of truth for the SQL text used by the
// running-style bucket eval pipeline.

import { buildRunningStyleBatchFeatureSql } from "../../../../sync-realtime-data/src/running-style-feature-sql";

const DATE_PATTERN = /^[0-9]{8}$/;
const ARG_NAME_SOURCE = "--source";
const ARG_NAME_FROM_DATE = "--from-date";
const ARG_NAME_TO_DATE = "--to-date";
const ARG_NAME_FEATURE_VERSION = "--feature-version";
const ARG_ADVANCE_PAIR = 2;

export interface PrintRunningStyleFeatureSqlOptions {
  source: "jra" | "nar";
  fromDate: string;
  toDate: string;
  featureVersion: string;
}

interface PartialOptions {
  source?: string;
  fromDate?: string;
  toDate?: string;
  featureVersion?: string;
}

interface ApplyArgInput {
  options: PartialOptions;
  name: string;
  value: string | undefined;
}

interface ApplyResult {
  next: PartialOptions;
  advance: number;
}

interface ConsumeArgsInput {
  options: PartialOptions;
  argv: readonly string[];
  cursor: number;
}

interface PrintRunningStyleFeatureSqlDeps {
  write: (chunk: string) => void;
}

interface PrintRunningStyleFeatureSqlInput {
  deps: PrintRunningStyleFeatureSqlDeps;
  options: PrintRunningStyleFeatureSqlOptions;
}

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value`);
  return value;
};

const applyArg = ({ options, name, value }: ApplyArgInput): ApplyResult => {
  if (name === ARG_NAME_SOURCE) {
    return { advance: ARG_ADVANCE_PAIR, next: { ...options, source: requireValue(name, value) } };
  }
  if (name === ARG_NAME_FROM_DATE) {
    return { advance: ARG_ADVANCE_PAIR, next: { ...options, fromDate: requireValue(name, value) } };
  }
  if (name === ARG_NAME_TO_DATE) {
    return { advance: ARG_ADVANCE_PAIR, next: { ...options, toDate: requireValue(name, value) } };
  }
  if (name === ARG_NAME_FEATURE_VERSION) {
    return {
      advance: ARG_ADVANCE_PAIR,
      next: { ...options, featureVersion: requireValue(name, value) },
    };
  }
  throw new Error(`Unknown argument: ${name}`);
};

const consumeArgs = ({ options, argv, cursor }: ConsumeArgsInput): PartialOptions => {
  if (cursor >= argv.length) return options;
  const name = argv[cursor];
  if (name === undefined) return options;
  const { next, advance } = applyArg({ name, options, value: argv[cursor + 1] });
  return consumeArgs({ argv, cursor: cursor + advance, options: next });
};

const assertSource = (source: string): "jra" | "nar" => {
  if (source === "jra") return "jra";
  if (source === "nar") return "nar";
  throw new Error(`${ARG_NAME_SOURCE} must be one of jra | nar; got ${source}`);
};

const assertDate = (name: string, value: string): string => {
  if (!DATE_PATTERN.test(value)) {
    throw new Error(`${name} must be 8 digits (YYYYMMDD); got ${value}`);
  }
  return value;
};

export const parseArgs = (argv: readonly string[]): PrintRunningStyleFeatureSqlOptions => {
  const collected = consumeArgs({ argv, cursor: 0, options: {} });
  if (collected.source === undefined) throw new Error(`${ARG_NAME_SOURCE} is required`);
  if (collected.fromDate === undefined) throw new Error(`${ARG_NAME_FROM_DATE} is required`);
  if (collected.toDate === undefined) throw new Error(`${ARG_NAME_TO_DATE} is required`);
  if (collected.featureVersion === undefined) {
    throw new Error(`${ARG_NAME_FEATURE_VERSION} is required`);
  }
  return {
    featureVersion: collected.featureVersion,
    fromDate: assertDate(ARG_NAME_FROM_DATE, collected.fromDate),
    source: assertSource(collected.source),
    toDate: assertDate(ARG_NAME_TO_DATE, collected.toDate),
  };
};

export const printRunningStyleFeatureSql = ({
  deps,
  options,
}: PrintRunningStyleFeatureSqlInput): void => {
  // The production batch builder embeds source / fromDate / toDate /
  // featureSchemaVersion directly into the SQL so the Python DuckDB
  // execute path can run the query without further binding.
  const sql = buildRunningStyleBatchFeatureSql({
    featureSchemaVersion: options.featureVersion,
    fromDate: options.fromDate,
    source: options.source,
    toDate: options.toDate,
  });
  deps.write(sql);
};

const main = (argv: readonly string[]): void => {
  // parseArgs validates --source / --from-date / --to-date / --feature-version
  // before any SQL is emitted; the resulting options are forwarded straight
  // to the production batch builder.
  const options = parseArgs(argv);
  printRunningStyleFeatureSql({
    deps: {
      write: (chunk) => {
        process.stdout.write(chunk);
      },
    },
    options,
  });
};

if (import.meta.main) {
  main(process.argv.slice(2));
}
