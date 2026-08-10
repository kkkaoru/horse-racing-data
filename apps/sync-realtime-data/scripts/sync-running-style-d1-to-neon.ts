// Run with:
//   bun run apps/sync-realtime-data/scripts/sync-running-style-d1-to-neon.ts \
//     --neon-url postgresql://... \
//     --from-date 20260608 \
//     --to-date 20260619
//
// Reads D1 race_running_styles rows for the given date range (inclusive) and
// upserts them into the Neon race_running_style_model_predictions table.
// predicted_class is derived from predicted_label since D1 does not store it.

import { spawn } from "node:child_process";
import { Pool } from "pg";

import type { RaceRunningStyleRow } from "../src/running-style-d1";
import type { RunningStyleClassLabel } from "../src/running-style-lightgbm-tree";
import * as runningStyleNeon from "../src/running-style-neon";

const WRANGLER_COMMAND = "bunx";
const DATABASE_NAME = "sync-realtime-data";
const D1_BATCH_SIZE = 500;

export const ensureRunningStylePredictionNeonSchema =
  runningStyleNeon.ensureRunningStylePredictionNeonSchema;
export const upsertRunningStylePredictionsToNeon =
  runningStyleNeon.upsertRunningStylePredictionsToNeon;

const LABEL_CLASS_INDEX: Record<string, number> = {
  nige: 0,
  oikomi: 3,
  sashi: 2,
  senkou: 1,
};

interface CliArgs {
  neonUrl: string;
  fromDate: string;
  toDate: string;
}

interface D1Row {
  race_key: string;
  horse_number: number;
  ketto_toroku_bango: string;
  kaisai_nen: string;
  model_version: string;
  cell_model_key: string | null;
  cell_variant_id: string | null;
  p_nige: number;
  p_senkou: number;
  p_sashi: number;
  p_oikomi: number;
  predicted_corner_front_score: number | null;
  predicted_corner_rank: number | null;
  predicted_label: string;
  predicted_at: string;
}

interface NeonRow {
  model_version: string;
  source: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  umaban: number;
  cell_model_key: string | null;
  cell_variant_id: string | null;
  p_nige: number;
  p_senkou: number;
  p_sashi: number;
  p_oikomi: number;
  predicted_corner_front_score: number;
  predicted_corner_rank: number;
  predicted_label: string;
  predicted_class: number;
  predicted_at: string;
}

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined || value.length === 0) {
    throw new Error(`${name} requires a value.`);
  }
  return value;
};

const parseArgs = (argv: readonly string[]): CliArgs => {
  let neonUrl: string | undefined;
  let fromDate: string | undefined;
  let toDate: string | undefined;
  for (let index = 0; index < argv.length; index += 1) {
    const name = argv[index]!;
    const value = argv[index + 1];
    if (name === "--neon-url") {
      neonUrl = requireValue(name, value);
      index += 1;
      continue;
    }
    if (name === "--from-date") {
      fromDate = requireValue(name, value);
      index += 1;
      continue;
    }
    if (name === "--to-date") {
      toDate = requireValue(name, value);
      index += 1;
      continue;
    }
    throw new Error(`Unknown argument: ${name}`);
  }
  if (neonUrl === undefined) throw new Error("--neon-url is required.");
  if (fromDate === undefined) throw new Error("--from-date is required.");
  if (toDate === undefined) toDate = fromDate;
  return { fromDate, neonUrl, toDate };
};

const spawnJson = async (args: readonly string[]): Promise<unknown> => {
  const stdoutChunks: Buffer[] = [];
  const stderrChunks: Buffer[] = [];
  const child = spawn(WRANGLER_COMMAND, [...args], { stdio: ["ignore", "pipe", "pipe"] });
  child.stdout.on("data", (chunk: Buffer) => stdoutChunks.push(chunk));
  child.stderr.on("data", (chunk: Buffer) => stderrChunks.push(chunk));
  const exitCode = await new Promise<number>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code) => resolve(code ?? 0));
  });
  if (exitCode !== 0) {
    throw new Error(
      `wrangler failed (exit ${exitCode}): ${Buffer.concat(stderrChunks).toString("utf8")}`,
    );
  }
  return JSON.parse(Buffer.concat(stdoutChunks).toString("utf8"));
};

const fetchD1Batch = async (datePattern: string, offset: number): Promise<D1Row[]> => {
  const command = [
    "wrangler",
    "d1",
    "execute",
    DATABASE_NAME,
    "--remote",
    "--json",
    "--command",
    `select race_key, horse_number, ketto_toroku_bango, kaisai_nen, model_version, cell_model_key, cell_variant_id, p_nige, p_senkou, p_sashi, p_oikomi, predicted_corner_front_score, predicted_corner_rank, predicted_label, predicted_at from race_running_styles where race_key like '${datePattern}' order by race_key, horse_number limit ${D1_BATCH_SIZE} offset ${offset}`,
  ];
  const result = (await spawnJson(command)) as Array<{ results: D1Row[] }>;
  return result[0]?.results ?? [];
};

const parseRaceKey = (
  raceKey: string,
): { source: string; kaisaiTsukihi: string; keibajoCode: string; raceBango: string } | null => {
  const parts = raceKey.split(":");
  if (parts.length !== 4) return null;
  const [source, datePart, keibajoCode, raceBango] = parts;
  if (
    source === undefined ||
    datePart === undefined ||
    keibajoCode === undefined ||
    raceBango === undefined ||
    datePart.length !== 8
  )
    return null;
  return { keibajoCode, raceBango, source, kaisaiTsukihi: datePart.slice(4, 8) };
};

const computePredictedCornerFrontScore = (row: D1Row): number =>
  Number(row.p_senkou) + 2 * Number(row.p_sashi) + 3 * Number(row.p_oikomi);

const getPredictedCornerFrontScore = (row: D1Row): number =>
  row.predicted_corner_front_score === null
    ? computePredictedCornerFrontScore(row)
    : Number(row.predicted_corner_front_score);

const isRunningStyleClassLabel = (label: string): label is RunningStyleClassLabel =>
  label === "nige" || label === "oikomi" || label === "sashi" || label === "senkou";

const toNeonRow = (row: D1Row, predictedCornerRank: number): NeonRow | null => {
  const parsed = parseRaceKey(row.race_key);
  if (parsed === null) return null;
  const predictedClass = LABEL_CLASS_INDEX[row.predicted_label];
  if (predictedClass === undefined) return null;
  return {
    kaisai_nen: row.kaisai_nen,
    kaisai_tsukihi: parsed.kaisaiTsukihi,
    keibajo_code: parsed.keibajoCode,
    ketto_toroku_bango: row.ketto_toroku_bango,
    model_version: row.model_version,
    cell_model_key: row.cell_model_key,
    cell_variant_id: row.cell_variant_id,
    p_nige: Number(row.p_nige),
    p_oikomi: Number(row.p_oikomi),
    p_sashi: Number(row.p_sashi),
    p_senkou: Number(row.p_senkou),
    predicted_corner_front_score: getPredictedCornerFrontScore(row),
    predicted_corner_rank:
      row.predicted_corner_rank === null ? predictedCornerRank : Number(row.predicted_corner_rank),
    predicted_at: row.predicted_at,
    predicted_class: predictedClass,
    predicted_label: row.predicted_label,
    race_bango: parsed.raceBango,
    source: parsed.source,
    umaban: Number(row.horse_number),
  };
};

export const mapD1RowsToNeonRows = (rows: readonly D1Row[]): NeonRow[] => {
  const grouped = new Map<string, D1Row[]>();
  rows.forEach((row) => {
    const raceRows = grouped.get(row.race_key);
    if (raceRows === undefined) {
      grouped.set(row.race_key, [row]);
      return;
    }
    raceRows.push(row);
  });
  const ranks = new Map<string, number>();
  grouped.forEach((raceRows) => {
    [...raceRows]
      .sort(
        (left, right) =>
          getPredictedCornerFrontScore(left) - getPredictedCornerFrontScore(right) ||
          Number(right.p_nige) - Number(left.p_nige) ||
          left.ketto_toroku_bango.localeCompare(right.ketto_toroku_bango) ||
          Number(left.horse_number) - Number(right.horse_number),
      )
      .forEach((row, index) => ranks.set(`${row.race_key}:${row.horse_number}`, index + 1));
  });
  return rows.flatMap((row) => {
    const mapped = toNeonRow(row, ranks.get(`${row.race_key}:${row.horse_number}`)!);
    return mapped === null ? [] : [mapped];
  });
};

const toRaceRunningStyleRows = (rows: readonly NeonRow[]): RaceRunningStyleRow[] =>
  rows.flatMap((row) => {
    if (!isRunningStyleClassLabel(row.predicted_label)) return [];
    return [
      {
        bamei: null,
        category: row.source,
        cellModelKey: row.cell_model_key,
        cellVariantId: row.cell_variant_id,
        horseNumber: row.umaban,
        kaisaiNen: row.kaisai_nen,
        kettoTorokuBango: row.ketto_toroku_bango,
        modelVersion: row.model_version,
        pNige: row.p_nige,
        pOikomi: row.p_oikomi,
        pSashi: row.p_sashi,
        pSenkou: row.p_senkou,
        predictedAt: row.predicted_at,
        predictedCornerFrontScore: row.predicted_corner_front_score,
        predictedCornerRank: row.predicted_corner_rank,
        predictedLabel: row.predicted_label,
        raceKey: `${row.source}:${row.kaisai_nen}${row.kaisai_tsukihi}:${row.keibajo_code}:${row.race_bango}`,
      },
    ];
  });

export const upsertNeonRows = async (pool: Pool, rows: readonly NeonRow[]): Promise<number> =>
  runningStyleNeon.upsertRunningStylePredictionsToNeon(pool, toRaceRunningStyleRows(rows));

const buildDatePatterns = (fromDate: string, toDate: string): string[] => {
  const from = new Date(`${fromDate.slice(0, 4)}-${fromDate.slice(4, 6)}-${fromDate.slice(6, 8)}`);
  const to = new Date(`${toDate.slice(0, 4)}-${toDate.slice(4, 6)}-${toDate.slice(6, 8)}`);
  const patterns: string[] = [];
  const cursor = new Date(from);
  while (cursor <= to) {
    const year = cursor.getFullYear();
    const month = String(cursor.getMonth() + 1).padStart(2, "0");
    const day = String(cursor.getDate()).padStart(2, "0");
    patterns.push(`%${year}${month}${day}%`);
    cursor.setDate(cursor.getDate() + 1);
  }
  return patterns;
};

const syncDate = async (pool: Pool, pattern: string): Promise<number> => {
  const dateStr = pattern.replace(/%/g, "");
  let offset = 0;
  let totalUpserted = 0;
  for (;;) {
    const d1Rows = await fetchD1Batch(pattern, offset);
    if (d1Rows.length === 0) break;
    const neonRows = mapD1RowsToNeonRows(d1Rows);
    totalUpserted += await upsertNeonRows(pool, neonRows);
    offset += D1_BATCH_SIZE;
    if (d1Rows.length < D1_BATCH_SIZE) break;
  }
  console.log(`[sync] ${dateStr}: upserted ${totalUpserted} rows`);
  return totalUpserted;
};

const run = async (): Promise<void> => {
  const args = parseArgs(process.argv.slice(2));
  const patterns = buildDatePatterns(args.fromDate, args.toDate);
  console.log(`[sync] dates=${patterns.length} from=${args.fromDate} to=${args.toDate}`);
  const pool = new Pool({ connectionString: args.neonUrl, ssl: { rejectUnauthorized: false } });
  try {
    await runningStyleNeon.ensureRunningStylePredictionNeonSchema(pool);
    let total = 0;
    for (const pattern of patterns) {
      total += await syncDate(pool, pattern);
    }
    console.log(`[sync] done total=${total} rows`);
  } finally {
    await pool.end();
  }
};

if (import.meta.main) {
  run().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
