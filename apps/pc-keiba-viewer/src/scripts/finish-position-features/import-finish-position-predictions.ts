// Run with: bun run src/scripts/finish-position-features/import-finish-position-predictions.ts \
//   --target local --input tmp/predictions/jra-2025.jsonl \
//   --model-version lambdarank-jra-20260520-v1 \
//   --activate-category jra

import { createReadStream } from "node:fs";
import { resolve } from "node:path";
import { createInterface } from "node:readline";

import { Pool } from "pg";

import { getConnectionString, loadEnv } from "../compare-corner-predictions";
import type { FeatureCategory, FeatureTarget } from "./build-finish-position-features-types";
import {
  buildActivateModelSql,
  buildActiveModelsTableDdl,
  buildBatchInsertSql,
  buildPredictionsLookupIndexSql,
  buildPredictionsTableDdl,
  INSERT_COLUMNS,
} from "./import-predictions-sql";

const DEFAULT_BATCH_SIZE = 1000;
const RACE_ID_PART_COUNT = 5;

const CATEGORY_SET = new Set<FeatureCategory>(["all", "ban-ei", "jra", "nar"]);
const TARGET_SET = new Set<FeatureTarget>(["local", "neon"]);

interface ImportOptions {
  activateCategory: FeatureCategory | null;
  batchSize: number;
  inputPath: string;
  modelVersion: string;
  target: FeatureTarget;
}

interface PredictionRecord {
  race_id: string;
  ketto_toroku_bango: string;
  umaban: number;
  predicted_score: number;
  predicted_rank: number;
}

interface RaceIdParts {
  source: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
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

const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/finish-position-features/import-finish-position-predictions.ts \\",
    "    --input <predictions.jsonl> --model-version <id> \\",
    "    [--target local|neon] [--activate-category jra|nar|ban-ei] [--batch-size N]",
    "",
    "Streams a Python-produced JSONL of predictions into",
    "race_finish_position_model_predictions. Each line is one runner.",
  ].join("\n");

const initialOptions = (): ImportOptions => ({
  activateCategory: null,
  batchSize: DEFAULT_BATCH_SIZE,
  inputPath: "",
  modelVersion: "",
  target: "local",
});

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

export const parseRaceId = (raceId: string): RaceIdParts => {
  const parts = raceId.split(":");
  if (parts.length !== RACE_ID_PART_COUNT) {
    throw new Error(`Invalid race_id format: ${raceId}`);
  }
  return {
    kaisai_nen: parts[1] ?? "",
    kaisai_tsukihi: parts[2] ?? "",
    keibajo_code: parts[3] ?? "",
    race_bango: parts[4] ?? "",
    source: parts[0] ?? "",
  };
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const parsePredictionLine = (line: string): PredictionRecord => {
  const raw: unknown = JSON.parse(line);
  if (!isRecord(raw)) throw new Error(`Prediction line is not an object: ${line}`);
  if (typeof raw.race_id !== "string") throw new Error("predicted record missing race_id");
  if (typeof raw.ketto_toroku_bango !== "string") {
    throw new Error("predicted record missing ketto_toroku_bango");
  }
  if (typeof raw.umaban !== "number") throw new Error("predicted record missing umaban");
  if (typeof raw.predicted_score !== "number") {
    throw new Error("predicted record missing predicted_score");
  }
  if (typeof raw.predicted_rank !== "number") {
    throw new Error("predicted record missing predicted_rank");
  }
  return {
    ketto_toroku_bango: raw.ketto_toroku_bango,
    predicted_rank: raw.predicted_rank,
    predicted_score: raw.predicted_score,
    race_id: raw.race_id,
    umaban: raw.umaban,
  };
};

export const dedupeBatch = (batch: readonly PredictionRecord[]): PredictionRecord[] => {
  const byPrimaryKey = new Map<string, PredictionRecord>();
  for (const record of batch) {
    byPrimaryKey.set(`${record.race_id}|${record.ketto_toroku_bango}`, record);
  }
  return Array.from(byPrimaryKey.values());
};

export const flattenForInsert = (record: PredictionRecord, modelVersion: string): unknown[] => {
  const parts = parseRaceId(record.race_id);
  return [
    modelVersion,
    parts.source,
    parts.kaisai_nen,
    parts.kaisai_tsukihi,
    parts.keibajo_code,
    parts.race_bango,
    record.ketto_toroku_bango,
    record.umaban,
    record.predicted_score,
    record.predicted_rank,
    null,
    null,
    record.predicted_rank,
  ];
};

const applyArg = (
  options: ImportOptions,
  name: string,
  value: string | undefined,
): { advanceBy: number } => {
  if (name === "--target") {
    const raw = requireValue(name, value);
    if (!isTarget(raw)) throw new Error("--target must be local or neon.");
    options.target = raw;
    return { advanceBy: 2 };
  }
  if (name === "--input") {
    options.inputPath = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version") {
    options.modelVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--activate-category") {
    const raw = requireValue(name, value);
    if (!isCategory(raw) || raw === "all") {
      throw new Error("--activate-category must be jra, nar, or ban-ei.");
    }
    options.activateCategory = raw;
    return { advanceBy: 2 };
  }
  if (name === "--batch-size") {
    options.batchSize = Math.max(1, Number.parseInt(requireValue(name, value), 10));
    return { advanceBy: 2 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

export const parseArgs = (argv: readonly string[]): ImportOptions => {
  const options = initialOptions();
  let cursor = 0;
  while (cursor < argv.length) {
    const name = argv[cursor];
    if (name === undefined) break;
    const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
    cursor += advanceBy;
  }
  if (options.inputPath === "") throw new Error("--input is required.");
  if (options.modelVersion === "") throw new Error("--model-version is required.");
  return options;
};

const ensureTables = async (pool: Pool): Promise<void> => {
  await Promise.all([
    pool.query(buildPredictionsTableDdl()),
    pool.query(buildActiveModelsTableDdl()),
  ]);
  await pool.query(buildPredictionsLookupIndexSql());
};

const flushBatch = async (
  pool: Pool,
  modelVersion: string,
  batch: PredictionRecord[],
): Promise<number> => {
  const deduped = dedupeBatch(batch);
  if (deduped.length === 0) return 0;
  const sql = buildBatchInsertSql(deduped.length);
  const flat = deduped.flatMap((record) => flattenForInsert(record, modelVersion));
  const result = await pool.query(sql, flat);
  return result.rowCount ?? deduped.length;
};

const streamFromJsonl = async (
  pool: Pool,
  options: ImportOptions,
): Promise<{ insertedCount: number; recordCount: number }> => {
  const absolutePath = resolve(process.cwd(), options.inputPath);
  const readStream = createReadStream(absolutePath, { encoding: "utf8" });
  const lineReader = createInterface({ input: readStream, crlfDelay: Infinity });
  let recordCount = 0;
  let insertedCount = 0;
  let batch: PredictionRecord[] = [];
  for await (const line of lineReader) {
    const trimmed = line.trim();
    if (trimmed === "") continue;
    batch.push(parsePredictionLine(trimmed));
    recordCount += 1;
    if (batch.length >= options.batchSize) {
      insertedCount += await flushBatch(pool, options.modelVersion, batch);
      batch = [];
    }
  }
  insertedCount += await flushBatch(pool, options.modelVersion, batch);
  return { insertedCount, recordCount };
};

const activateModel = async (pool: Pool, options: ImportOptions): Promise<void> => {
  if (options.activateCategory === null) return;
  await pool.query(buildActivateModelSql(), [options.activateCategory, options.modelVersion]);
};

const main = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  await loadEnv();
  const pool = new Pool({ connectionString: getConnectionString(options.target) });
  try {
    await ensureTables(pool);
    const { insertedCount, recordCount } = await streamFromJsonl(pool, options);
    await activateModel(pool, options);
    console.log(
      `[import-finish-position-predictions] target=${options.target} model_version=${options.modelVersion} read=${recordCount} inserted=${insertedCount} activated=${options.activateCategory ?? "no"}`,
    );
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

export { buildUsageText, DEFAULT_BATCH_SIZE, INSERT_COLUMNS, initialOptions, isCategory, isTarget };
