// Run with: bun run src/scripts/finish-position-features/export-finish-position-dataset.ts \
//   --target local --category jra --from-date 20160101 --to-date 20251231 \
//   --output tmp/finish-position-training/jra/2016-2025.csv

import { createWriteStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import { dirname, isAbsolute, resolve } from "node:path";

import { Pool } from "pg";

import { getConnectionString, loadEnv } from "../compare-corner-predictions";
import type { FeatureCategory, FeatureTarget } from "./build-finish-position-features-types";
import { buildExportSelectSql, EXPORT_COLUMN_ORDER } from "./export-finish-position-dataset-sql";

const DEFAULT_FEATURE_SCHEMA_VERSION = "v1";
const DEFAULT_FROM_DATE = "20160101";
const DEFAULT_TO_DATE = "20251231";

const CATEGORY_SET = new Set<FeatureCategory>(["all", "ban-ei", "jra", "nar"]);
const TARGET_SET = new Set<FeatureTarget>(["local", "neon"]);

interface ExportOptions {
  category: FeatureCategory;
  featureSchemaVersion: string;
  fromDate: string;
  output: string;
  target: FeatureTarget;
  toDate: string;
}

const isCategory = (value: string): value is FeatureCategory => {
  for (const candidate of CATEGORY_SET) {
    if (candidate === value) return true;
  }
  return false;
};

const isTarget = (value: string): value is FeatureTarget => {
  for (const candidate of TARGET_SET) {
    if (candidate === value) return true;
  }
  return false;
};

const initialOptions = (): ExportOptions => ({
  category: "jra",
  featureSchemaVersion: DEFAULT_FEATURE_SCHEMA_VERSION,
  fromDate: DEFAULT_FROM_DATE,
  output: "",
  target: "local",
  toDate: DEFAULT_TO_DATE,
});

const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/export-finish-position-dataset.ts \\",
    "    --output <path.csv> [--target local|neon] [--category jra|nar|ban-ei|all] \\",
    "    [--from-date YYYYMMDD] [--to-date YYYYMMDD] [--feature-schema-version v1]",
    "",
    "Streams race_finish_position_features rows as CSV (no quoting on numerics).",
    "Default range is 2016-01-01..2025-12-31 (10 years).",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

const applyArg = (
  options: ExportOptions,
  name: string,
  value: string | undefined,
): { advanceBy: number } => {
  if (name === "--target") {
    const raw = requireValue(name, value);
    if (!isTarget(raw)) throw new Error("--target must be local or neon.");
    options.target = raw;
    return { advanceBy: 2 };
  }
  if (name === "--category") {
    const raw = requireValue(name, value);
    if (!isCategory(raw)) throw new Error("--category must be all, jra, nar, or ban-ei.");
    options.category = raw;
    return { advanceBy: 2 };
  }
  if (name === "--from-date") {
    options.fromDate = requireValue(name, value).replaceAll("-", "");
    return { advanceBy: 2 };
  }
  if (name === "--to-date") {
    options.toDate = requireValue(name, value).replaceAll("-", "");
    return { advanceBy: 2 };
  }
  if (name === "--feature-schema-version") {
    options.featureSchemaVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--output") {
    options.output = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

const parseArgs = (argv: readonly string[]): ExportOptions => {
  const options = initialOptions();
  let cursor = 0;
  while (cursor < argv.length) {
    const name = argv[cursor];
    if (name === undefined) break;
    const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
    cursor += advanceBy;
  }
  if (options.output === "") throw new Error("--output is required.");
  return options;
};

const coerceToString = (value: unknown): string => {
  if (typeof value === "string") return value;
  if (typeof value === "boolean") return value ? "1" : "0";
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "";
  return JSON.stringify(value);
};

const csvEscape = (value: unknown): string => {
  if (value === null || value === undefined) return "";
  const raw = coerceToString(value);
  if (raw.includes(",") || raw.includes('"') || raw.includes("\n") || raw.includes("\r")) {
    return `"${raw.replaceAll('"', '""')}"`;
  }
  return raw;
};

const resolveOutputPath = (relativeOrAbsolute: string): string =>
  isAbsolute(relativeOrAbsolute) ? relativeOrAbsolute : resolve(process.cwd(), relativeOrAbsolute);

const writeHeader = (stream: NodeJS.WritableStream): void => {
  stream.write(`${EXPORT_COLUMN_ORDER.join(",")}\n`);
};

const writeRow = (stream: NodeJS.WritableStream, row: Record<string, unknown>): void => {
  const cells = EXPORT_COLUMN_ORDER.map((column) => csvEscape(row[column]));
  stream.write(`${cells.join(",")}\n`);
};

const ensureOutputDir = async (outputPath: string): Promise<void> => {
  await mkdir(dirname(outputPath), { recursive: true });
};

const streamToFile = async (
  pool: Pool,
  options: ExportOptions,
  outputPath: string,
): Promise<number> => {
  const stream = createWriteStream(outputPath, "utf8");
  writeHeader(stream);
  const sql = buildExportSelectSql(options.category, options.featureSchemaVersion);
  const result = await pool.query<Record<string, unknown>>(sql, [options.fromDate, options.toDate]);
  result.rows.forEach((row) => writeRow(stream, row));
  await new Promise<void>((resolvePromise, rejectPromise) => {
    stream.end((error?: Error | null) => {
      if (error === undefined || error === null) resolvePromise();
      else rejectPromise(error);
    });
  });
  return result.rowCount ?? 0;
};

const logSummary = (options: ExportOptions, outputPath: string, exportedCount: number): void => {
  console.log(
    `[export-finish-position-dataset] target=${options.target} category=${options.category} range=${options.fromDate}..${options.toDate} schema=${options.featureSchemaVersion} exported=${exportedCount} output=${outputPath}`,
  );
};

const main = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  await loadEnv();
  const outputPath = resolveOutputPath(options.output);
  await ensureOutputDir(outputPath);
  const pool = new Pool({ connectionString: getConnectionString(options.target) });
  try {
    const count = await streamToFile(pool, options, outputPath);
    logSummary(options, outputPath, count);
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

export {
  applyArg,
  buildUsageText,
  csvEscape,
  initialOptions,
  isCategory,
  isTarget,
  parseArgs,
  resolveOutputPath,
};
