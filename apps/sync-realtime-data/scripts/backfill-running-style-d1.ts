// Run with:
//   bun run apps/sync-realtime-data/scripts/backfill-running-style-d1.ts \
//     --pg-url postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing \
//     --from-year 2025 --to-year 2026 \
//     --output tmp/d1-running-style-backfill.sql
// Then apply with:
//   cd apps/sync-realtime-data && \
//     bunx wrangler d1 execute REALTIME_DB --remote --file=../../tmp/d1-running-style-backfill.sql

import { writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { mkdir } from "node:fs/promises";

import { Pool } from "pg";

const BATCH_SIZE = 500;
const DEFAULT_FROM_YEAR = 2025;
const DEFAULT_TO_YEAR = 2026;

interface BackfillArgs {
  pgUrl: string;
  fromYear: number;
  toYear: number;
  outputPath: string;
}

interface PredictionJoinedRow {
  source: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  umaban: number;
  bamei: string | null;
  category: string;
  model_version: string;
  p_nige: string;
  p_senkou: string;
  p_sashi: string;
  p_oikomi: string;
  predicted_label: string;
  predicted_at: string;
}

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

const parseArgs = (argv: readonly string[]): BackfillArgs => {
  const args: Partial<BackfillArgs> = {};
  let cursor = 0;
  while (cursor < argv.length) {
    const name = argv[cursor];
    const value = argv[cursor + 1];
    if (name === "--pg-url") {
      args.pgUrl = requireValue(name, value);
      cursor += 2;
      continue;
    }
    if (name === "--from-year") {
      args.fromYear = Number.parseInt(requireValue(name, value), 10);
      cursor += 2;
      continue;
    }
    if (name === "--to-year") {
      args.toYear = Number.parseInt(requireValue(name, value), 10);
      cursor += 2;
      continue;
    }
    if (name === "--output") {
      args.outputPath = requireValue(name, value);
      cursor += 2;
      continue;
    }
    if (name === undefined) break;
    throw new Error(`Unknown argument: ${name}`);
  }
  if (args.pgUrl === undefined) throw new Error("--pg-url is required.");
  if (args.outputPath === undefined) throw new Error("--output is required.");
  return {
    fromYear: args.fromYear ?? DEFAULT_FROM_YEAR,
    outputPath: args.outputPath,
    pgUrl: args.pgUrl,
    toYear: args.toYear ?? DEFAULT_TO_YEAR,
  };
};

export const buildRaceKey = (
  source: string,
  kaisaiNen: string,
  kaisaiTsukihi: string,
  keibajoCode: string,
  raceBango: string,
): string => `${source}:${kaisaiNen}${kaisaiTsukihi}:${keibajoCode}:${raceBango}`;

const escapeSqlString = (value: string): string => value.replaceAll("'", "''");

const formatStringValue = (value: string | null): string => {
  if (value === null) return "NULL";
  return `'${escapeSqlString(value)}'`;
};

export const buildInsertSqlForRow = (row: PredictionJoinedRow): string => {
  const raceKey = buildRaceKey(
    row.source,
    row.kaisai_nen,
    row.kaisai_tsukihi,
    row.keibajo_code,
    row.race_bango,
  );
  const columns = [
    "race_key",
    "horse_number",
    "ketto_toroku_bango",
    "bamei",
    "category",
    "kaisai_nen",
    "model_version",
    "p_nige",
    "p_senkou",
    "p_sashi",
    "p_oikomi",
    "predicted_label",
    "predicted_at",
  ].join(", ");
  const values = [
    formatStringValue(raceKey),
    String(row.umaban),
    formatStringValue(row.ketto_toroku_bango),
    formatStringValue(row.bamei),
    formatStringValue(row.category),
    formatStringValue(row.kaisai_nen),
    formatStringValue(row.model_version),
    String(Number(row.p_nige)),
    String(Number(row.p_senkou)),
    String(Number(row.p_sashi)),
    String(Number(row.p_oikomi)),
    formatStringValue(row.predicted_label),
    formatStringValue(row.predicted_at),
  ].join(", ");
  return `insert or replace into race_running_styles (${columns}) values (${values});`;
};

export const buildFetchSql = (): string => `
  with active_categories as (
    select category, model_version from running_style_active_models
  )
  select
    p.source,
    p.kaisai_nen,
    p.kaisai_tsukihi,
    p.keibajo_code,
    p.race_bango,
    p.ketto_toroku_bango,
    p.umaban,
    coalesce(jvd.bamei, nvd.bamei) as bamei,
    case
      when p.source = 'nar' and p.keibajo_code = '83' then 'ban-ei'
      else p.source
    end as category,
    p.model_version,
    p.p_nige,
    p.p_senkou,
    p.p_sashi,
    p.p_oikomi,
    p.predicted_label,
    p.prediction_generated_at::text as predicted_at
  from race_running_style_model_predictions p
  join active_categories ac
    on ac.model_version = p.model_version
   and ac.category = case
     when p.source = 'nar' and p.keibajo_code = '83' then 'ban-ei'
     else p.source
   end
  left join jvd_se jvd
    on p.source = 'jra'
   and jvd.kaisai_nen = p.kaisai_nen
   and jvd.kaisai_tsukihi = p.kaisai_tsukihi
   and jvd.keibajo_code = p.keibajo_code
   and jvd.race_bango = p.race_bango
   and jvd.ketto_toroku_bango = p.ketto_toroku_bango
  left join nvd_se nvd
    on p.source = 'nar'
   and nvd.kaisai_nen = p.kaisai_nen
   and nvd.kaisai_tsukihi = p.kaisai_tsukihi
   and nvd.keibajo_code = p.keibajo_code
   and nvd.race_bango = p.race_bango
   and nvd.ketto_toroku_bango = p.ketto_toroku_bango
  where p.kaisai_nen between $1 and $2
    and (
      p.source = 'jra'
      or (p.source = 'nar' and p.keibajo_code <> '83')
    )
  order by p.source, p.kaisai_nen, p.kaisai_tsukihi, p.keibajo_code, p.race_bango, p.umaban
`;

const writeSqlOutput = async (outputPath: string, statements: string[]): Promise<void> => {
  await mkdir(dirname(outputPath), { recursive: true });
  const body = statements.join("\n");
  await writeFile(outputPath, `${body}\n`, "utf8");
};

const run = async (): Promise<void> => {
  const args = parseArgs(process.argv.slice(2));
  const pool = new Pool({ connectionString: args.pgUrl });
  try {
    const fromYear = String(args.fromYear);
    const toYear = String(args.toYear);
    const result = await pool.query<PredictionJoinedRow>(buildFetchSql(), [fromYear, toYear]);
    const statements = result.rows.map(buildInsertSqlForRow);
    const absolute = resolve(process.cwd(), args.outputPath);
    await writeSqlOutput(absolute, statements);
    console.log(
      `[backfill-running-style-d1] rows=${result.rowCount} output=${absolute} batchHint=${BATCH_SIZE}`,
    );
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

export { BATCH_SIZE, DEFAULT_FROM_YEAR, DEFAULT_TO_YEAR, escapeSqlString, formatStringValue, parseArgs };
export type { BackfillArgs, PredictionJoinedRow };
