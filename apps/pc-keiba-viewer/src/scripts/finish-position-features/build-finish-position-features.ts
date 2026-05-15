// Run with: bun run src/scripts/finish-position-features/build-finish-position-features.ts \
//   --target local --category jra --from-date 20160101 --to-date 20261231

import { Pool } from "pg";

import { getConnectionString, loadEnv } from "../compare-corner-predictions";
import {
  buildCreateTableSql,
  buildIndexSqls,
  buildSkeletonUpsertSql,
  FEATURE_TABLE_NAME,
} from "./build-finish-position-features-sql";
import type {
  BuildOptions,
  FeatureCategory,
  FeatureTarget,
} from "./build-finish-position-features-types";
import { buildHorseCareerUpdateSql } from "./build-horse-career-sql";

const DEFAULT_FEATURE_SCHEMA_VERSION = "v1";
const DEFAULT_FROM_DATE = "20160101";
const DEFAULT_TO_DATE = "20261231";

const CATEGORY_SET = new Set<FeatureCategory>(["all", "ban-ei", "jra", "nar"]);
const TARGET_SET = new Set<FeatureTarget>(["local", "neon"]);

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

const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/build-finish-position-features.ts \\",
    "    [--target local|neon] [--category jra|nar|ban-ei|all] \\",
    "    [--from-date YYYYMMDD] [--to-date YYYYMMDD] [--feature-schema-version v1] [--dry-run]",
    "",
    "Creates the race_finish_position_features table (idempotent) and upserts the",
    "meta columns for the given range. Feature columns are NULL in this commit;",
    "subsequent commits fill them in.",
  ].join("\n");

const initialOptions = (): BuildOptions => ({
  category: "jra",
  dryRun: false,
  featureSchemaVersion: DEFAULT_FEATURE_SCHEMA_VERSION,
  fromDate: DEFAULT_FROM_DATE,
  target: "local",
  toDate: DEFAULT_TO_DATE,
});

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

const applyArg = (
  options: BuildOptions,
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
  if (name === "--dry-run") {
    options.dryRun = true;
    return { advanceBy: 1 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

const parseArgs = (argv: readonly string[]): BuildOptions => {
  const options = initialOptions();
  let cursor = 0;
  while (cursor < argv.length) {
    const name = argv[cursor];
    if (name === undefined) break;
    const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
    cursor += advanceBy;
  }
  return options;
};

const ensureTable = async (pool: Pool): Promise<void> => {
  await pool.query(buildCreateTableSql());
};

const ensureIndexes = async (pool: Pool): Promise<void> => {
  const statements = buildIndexSqls();
  await Promise.all(statements.map((statement) => pool.query(statement)));
};

const upsertSkeletonBatch = async (pool: Pool, options: BuildOptions): Promise<number> => {
  const sql = buildSkeletonUpsertSql(options.category, options.featureSchemaVersion);
  const result = await pool.query(sql, [options.fromDate, options.toDate]);
  return result.rowCount ?? 0;
};

const applyHorseCareerStage = async (pool: Pool, options: BuildOptions): Promise<number> => {
  const sql = buildHorseCareerUpdateSql(options.category);
  const result = await pool.query(sql, [options.fromDate, options.toDate]);
  return result.rowCount ?? 0;
};

const logSummary = (options: BuildOptions, stageCounts: Record<string, number>): void => {
  const stages = Object.entries(stageCounts)
    .map(([stage, count]) => `${stage}=${count}`)
    .join(" ");
  console.log(
    `[build-finish-position-features] table=${FEATURE_TABLE_NAME} target=${options.target} category=${options.category} range=${options.fromDate}..${options.toDate} ${stages}`,
  );
};

const main = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  await loadEnv();
  const pool = new Pool({ connectionString: getConnectionString(options.target) });
  try {
    await ensureTable(pool);
    await ensureIndexes(pool);
    if (options.dryRun) {
      logSummary(options, { skeleton: 0, horse_career: 0 });
      return;
    }
    const skeletonCount = await upsertSkeletonBatch(pool, options);
    const horseCareerCount = await applyHorseCareerStage(pool, options);
    logSummary(options, { horse_career: horseCareerCount, skeleton: skeletonCount });
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

export { applyArg, buildUsageText, initialOptions, isCategory, isTarget, parseArgs };
