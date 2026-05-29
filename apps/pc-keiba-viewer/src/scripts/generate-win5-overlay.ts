// Run with bun:
//   bun run src/scripts/generate-win5-overlay.ts [--date YYYYMMDD] [--force]
//
// End-to-end overlay generation for one WIN5 race day. Designed to be
// scheduled by launchd/cron in the early morning before WIN5 race start.
//
// Steps:
//   1. Refresh race_entry_corner_features for the date window
//   2. Rebuild base finish-position features parquet up to the date
//   3. Apply v7 lineage layer
//   4. Train XGB on history through (date - 1) and predict the date
//   5. Upsert into race_finish_position_model_predictions with model_version
//      `<WIN5_MODEL_VERSION>-rs-overlay-<YYYYMMDD>`

import { Pool } from "pg";

import { WIN5_MODEL_VERSION } from "../lib/win5/types";

const PROJECT_ROOT = new URL("../../", import.meta.url).pathname;
const PYTHON_BIN = `${PROJECT_ROOT}.venv/bin/python`;
const BASE_FEATURES_DIR = "tmp/feat-jra-2026-base";
const LINEAGE_FEATURES_DIR = "tmp/feat-jra-2026-lineage";
const LINEAGE_CONFIG_PATH = "src/scripts/finish-position-features/lineage-races/jra.json";
const PREDICTIONS_OUTPUT_DIR = "tmp/finish-position-eval/predictions-jra-xgb-v7-overlay/jra";
const DEFAULT_LOCAL_DATABASE_URL =
  "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing";
const CORNER_LOOKBACK_DAYS = 14;
const TRAIN_START_DATE = "20070101";
const FEATURES_HISTORY_START_DATE = "20060101";

interface CliArgs {
  date: string;
  force: boolean;
}

interface DateParts {
  year: string;
  monthDay: string;
  yyyymmdd: string;
}

const formatTodayYyyymmddJst = (now = new Date()): string => {
  const jstMs = now.getTime() + 9 * 60 * 60 * 1000;
  const jst = new Date(jstMs);
  const year = jst.getUTCFullYear();
  const month = String(jst.getUTCMonth() + 1).padStart(2, "0");
  const day = String(jst.getUTCDate()).padStart(2, "0");
  return `${year}${month}${day}`;
};

const offsetYyyymmdd = (yyyymmdd: string, days: number): string => {
  const year = Number.parseInt(yyyymmdd.slice(0, 4), 10);
  const month = Number.parseInt(yyyymmdd.slice(4, 6), 10) - 1;
  const day = Number.parseInt(yyyymmdd.slice(6, 8), 10);
  const date = new Date(Date.UTC(year, month, day));
  date.setUTCDate(date.getUTCDate() + days);
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  return `${y}${m}${d}`;
};

const splitDate = (yyyymmdd: string): DateParts => ({
  year: yyyymmdd.slice(0, 4),
  monthDay: yyyymmdd.slice(4, 8),
  yyyymmdd,
});

const buildOverlayModelVersion = (yyyymmdd: string): string =>
  `${WIN5_MODEL_VERSION}-rs-overlay-${yyyymmdd}`;

const parseArgs = (): CliArgs => {
  const args = process.argv.slice(2);
  const parsed: CliArgs = { date: formatTodayYyyymmddJst(), force: false };
  const handlers = new Map<string, (value: string) => void>([
    [
      "--date",
      (value) => {
        parsed.date = value;
      },
    ],
  ]);
  args.forEach((arg, index) => {
    if (arg === "--force") {
      parsed.force = true;
      return;
    }
    const handler = handlers.get(arg);
    if (handler) {
      handler(args[index + 1] ?? "");
    }
  });
  if (!/^\d{8}$/u.test(parsed.date)) {
    throw new Error(`--date must be YYYYMMDD, got "${parsed.date}"`);
  }
  return parsed;
};

const getConnectionString = (): string =>
  process.env.DATABASE_URL_LOCAL ?? process.env.DATABASE_URL ?? DEFAULT_LOCAL_DATABASE_URL;

const hasWin5Schedule = async (params: { pool: Pool; parts: DateParts }): Promise<boolean> => {
  const fromJvdWf = await params.pool.query<{ count: string }>(
    `select count(*)::text as count from jvd_wf where kaisai_nen = $1 and kaisai_tsukihi = $2`,
    [params.parts.year, params.parts.monthDay],
  );
  if (Number(fromJvdWf.rows[0]?.count ?? 0) > 0) {
    return true;
  }
  // jvd_wf may not have the row before race start; fall back to jvd_ra heuristic:
  // only Saturday/Sunday with at least 5 races at a JRA venue is a candidate
  // (a stricter check would be JRA Web schedule, but the daily cron is
  // expected to know its target date from the scheduler).
  const fromJvdRa = await params.pool.query<{ races: string }>(
    `select count(*)::text as races from jvd_ra where kaisai_nen = $1 and kaisai_tsukihi = $2`,
    [params.parts.year, params.parts.monthDay],
  );
  return Number(fromJvdRa.rows[0]?.races ?? 0) >= 5;
};

const overlayAlreadyExists = async (params: {
  pool: Pool;
  parts: DateParts;
  modelVersion: string;
}): Promise<boolean> => {
  const result = await params.pool.query<{ count: string }>(
    `
      select count(*)::text as count
      from race_finish_position_model_predictions
      where model_version = $1 and source = 'jra'
        and kaisai_nen = $2 and kaisai_tsukihi = $3
    `,
    [params.modelVersion, params.parts.year, params.parts.monthDay],
  );
  return Number(result.rows[0]?.count ?? 0) > 0;
};

interface RunStepParams {
  label: string;
  cmd: readonly string[];
  env?: Record<string, string>;
}

const runStep = async (params: RunStepParams): Promise<void> => {
  console.log(`\n[step] ${params.label}: ${params.cmd.join(" ")}`);
  const proc = Bun.spawn({
    cmd: [...params.cmd],
    cwd: PROJECT_ROOT,
    env: { ...process.env, ...params.env },
    stdout: "inherit",
    stderr: "inherit",
  });
  const code = await proc.exited;
  if (code !== 0) {
    throw new Error(`${params.label} failed with exit code ${code}`);
  }
};

const refreshCornerFeatures = async (parts: DateParts): Promise<void> => {
  const from = offsetYyyymmdd(parts.yyyymmdd, -CORNER_LOOKBACK_DAYS);
  await runStep({
    label: "build-corner-feature-table",
    cmd: [
      "bun",
      "run",
      "src/scripts/build-corner-feature-table.ts",
      "--from-date",
      from,
      "--to-date",
      parts.yyyymmdd,
    ],
    env: { DATABASE_URL_LOCAL: getConnectionString() },
  });
};

const rebuildBaseFeatures = async (parts: DateParts): Promise<void> => {
  await runStep({
    label: "finish_position_features_duckdb",
    cmd: [
      PYTHON_BIN,
      "src/scripts/finish_position_features_duckdb.py",
      "--category",
      "jra",
      "--from-date",
      FEATURES_HISTORY_START_DATE,
      "--to-date",
      parts.yyyymmdd,
      "--output-dir",
      BASE_FEATURES_DIR,
      "--force-clean-output",
    ],
    env: { DATABASE_URL_LOCAL: getConnectionString() },
  });
};

const applyLineageLayer = async (parts: DateParts): Promise<void> => {
  await runStep({
    label: "add-grade-race-lineage-features",
    cmd: [
      PYTHON_BIN,
      "src/scripts/finish-position-features/add-grade-race-lineage-features.py",
      "--input-dir",
      BASE_FEATURES_DIR,
      "--output-dir",
      LINEAGE_FEATURES_DIR,
      "--config",
      LINEAGE_CONFIG_PATH,
      "--from-date",
      FEATURES_HISTORY_START_DATE,
      "--to-date",
      parts.yyyymmdd,
    ],
    env: {
      DATABASE_URL_LOCAL: getConnectionString(),
      LOCAL_PG_URL: getConnectionString(),
    },
  });
};

const runXgbInference = async (parts: DateParts): Promise<string> => {
  const trainEnd = offsetYyyymmdd(parts.yyyymmdd, -1);
  const outputPath = `${PREDICTIONS_OUTPUT_DIR}/${parts.yyyymmdd}.jsonl`;
  await runStep({
    label: "finish_position_xgboost_predict_only",
    cmd: [
      PYTHON_BIN,
      "src/scripts/finish_position_xgboost_predict_only.py",
      "--csv",
      LINEAGE_FEATURES_DIR,
      "--train-start-date",
      TRAIN_START_DATE,
      "--train-end-date",
      trainEnd,
      "--predict-date",
      parts.yyyymmdd,
      "--output-jsonl",
      outputPath,
    ],
  });
  return outputPath;
};

const importPredictions = async (params: {
  outputPath: string;
  modelVersion: string;
}): Promise<void> => {
  await runStep({
    label: "import-finish-position-predictions",
    cmd: [
      "bun",
      "run",
      "src/scripts/finish-position-features/import-finish-position-predictions.ts",
      "--target",
      "local",
      "--input",
      params.outputPath,
      "--model-version",
      params.modelVersion,
    ],
    env: { DATABASE_URL_LOCAL: getConnectionString() },
  });
};

const main = async (): Promise<void> => {
  const args = parseArgs();
  const parts = splitDate(args.date);
  const modelVersion = buildOverlayModelVersion(parts.yyyymmdd);
  const pool = new Pool({ connectionString: getConnectionString() });
  try {
    console.log(`[generate-win5-overlay] date=${parts.yyyymmdd} model_version=${modelVersion}`);
    if (!(await hasWin5Schedule({ pool, parts }))) {
      console.log(`[generate-win5-overlay] no WIN5 schedule for ${parts.yyyymmdd}; skipping`);
      return;
    }
    if (!args.force && (await overlayAlreadyExists({ pool, parts, modelVersion }))) {
      console.log(`[generate-win5-overlay] overlay already exists for ${parts.yyyymmdd}; skipping`);
      return;
    }
    await refreshCornerFeatures(parts);
    await rebuildBaseFeatures(parts);
    await applyLineageLayer(parts);
    const outputPath = await runXgbInference(parts);
    await importPredictions({ outputPath, modelVersion });
    console.log(`[generate-win5-overlay] done: ${modelVersion}`);
  } finally {
    await pool.end();
  }
};

export {
  buildOverlayModelVersion,
  formatTodayYyyymmddJst,
  hasWin5Schedule,
  offsetYyyymmdd,
  overlayAlreadyExists,
  parseArgs,
  splitDate,
};

if (import.meta.main) {
  await main();
}
