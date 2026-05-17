// Run with: bun run src/scripts/finish-position-features/import-running-style-predictions.ts \
//   --target local --input tmp/finish-position-eval/predictions-jra/running-style-lgbm/2024-2025.jsonl \
//   --model-version jra-running-style-lgbm-v1.0 \
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
  buildHorseLookupIndexSql,
  buildPredictionsLookupIndexSql,
  buildPredictionsTableDdl,
} from "./import-running-style-sql";

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

interface RunningStylePredictionRecord {
  race_id: string;
  ketto_toroku_bango: string;
  umaban: number;
  p_nige: number;
  p_senkou: number;
  p_sashi: number;
  p_oikomi: number;
  predicted_label: string;
  predicted_class: number;
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
    "  bun run src/scripts/finish-position-features/import-running-style-predictions.ts \\",
    "    --input <predictions.jsonl> --model-version <id> \\",
    "    [--target local|neon] [--activate-category jra|nar|ban-ei] [--batch-size N]",
    "",
    "Streams a Python-produced JSONL of running-style predictions into",
    "race_running_style_model_predictions. Each line carries p_nige/p_senkou/",
    "p_sashi/p_oikomi summing to ~1.0 plus predicted_label and predicted_class.",
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

const parseRaceId = (raceId: string): RaceIdParts => {
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

const requireNumeric = (raw: unknown, field: string): number => {
  if (typeof raw === "number" && Number.isFinite(raw)) return raw;
  throw new Error(`predicted record missing or invalid ${field}`);
};

const parsePredictionLine = (line: string): RunningStylePredictionRecord => {
  const raw: unknown = JSON.parse(line);
  if (!isRecord(raw)) throw new Error(`Prediction line is not an object: ${line}`);
  if (typeof raw.race_id !== "string") throw new Error("predicted record missing race_id");
  if (typeof raw.ketto_toroku_bango !== "string") {
    throw new Error("predicted record missing ketto_toroku_bango");
  }
  if (typeof raw.umaban !== "number") throw new Error("predicted record missing umaban");
  if (typeof raw.predicted_label !== "string") {
    throw new Error("predicted record missing predicted_label");
  }
  if (typeof raw.predicted_class !== "number") {
    throw new Error("predicted record missing predicted_class");
  }
  return {
    ketto_toroku_bango: raw.ketto_toroku_bango,
    p_nige: requireNumeric(raw.p_nige, "p_nige"),
    p_oikomi: requireNumeric(raw.p_oikomi, "p_oikomi"),
    p_sashi: requireNumeric(raw.p_sashi, "p_sashi"),
    p_senkou: requireNumeric(raw.p_senkou, "p_senkou"),
    predicted_class: raw.predicted_class,
    predicted_label: raw.predicted_label,
    race_id: raw.race_id,
    umaban: raw.umaban,
  };
};

const flattenForInsert = (
  record: RunningStylePredictionRecord,
  modelVersion: string,
): unknown[] => {
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
    record.p_nige,
    record.p_senkou,
    record.p_sashi,
    record.p_oikomi,
    record.predicted_label,
    record.predicted_class,
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

const parseArgs = (argv: readonly string[]): ImportOptions => {
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
  await Promise.all([
    pool.query(buildPredictionsLookupIndexSql()),
    pool.query(buildHorseLookupIndexSql()),
  ]);
};

const flushBatch = async (
  pool: Pool,
  modelVersion: string,
  batch: RunningStylePredictionRecord[],
): Promise<number> => {
  if (batch.length === 0) return 0;
  const sql = buildBatchInsertSql(batch.length);
  const flat = batch.flatMap((record) => flattenForInsert(record, modelVersion));
  const result = await pool.query(sql, flat);
  return result.rowCount ?? batch.length;
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
  let batch: RunningStylePredictionRecord[] = [];
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
      `[import-running-style-predictions] target=${options.target} model_version=${options.modelVersion} read=${recordCount} inserted=${insertedCount} activated=${options.activateCategory ?? "no"}`,
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

export {
  buildUsageText,
  DEFAULT_BATCH_SIZE,
  flattenForInsert,
  initialOptions,
  isCategory,
  isTarget,
  parseArgs,
  parsePredictionLine,
  parseRaceId,
};
