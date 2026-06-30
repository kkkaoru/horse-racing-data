// Run with bun. Neon-backed completion guard for focused per-race full messages.

import { neon } from "@neondatabase/serverless";
import type { Env, PredictCategory } from "./types";

interface CompletionParams {
  env: Env;
  category: PredictCategory;
  runYmd: string;
  keibajoCode: string;
  raceBango: string;
}

interface CompletionRow {
  actual_rows: unknown;
  complete: unknown;
  expected_rows: unknown;
}

const RUN_YMD_YEAR_END = 4;
const RUN_YMD_MONTH_START = 4;
const RUN_YMD_DAY_END = 8;
const NAR_SOURCE = "nar";
const JRA_SOURCE = "jra";

const sourceForCategory = (category: PredictCategory): string => {
  if (category === "jra") return JRA_SOURCE;
  return NAR_SOURCE;
};

const toCount = (value: unknown): number => {
  if (typeof value === "number" && Number.isFinite(value)) return Math.max(0, Math.trunc(value));
  if (typeof value === "bigint") return Number(value > 0n ? value : 0n);
  if (typeof value !== "string") return 0;
  const parsed = Number.parseInt(value, 10);
  if (Number.isFinite(parsed)) return Math.max(0, parsed);
  return 0;
};

const toBoolean = (value: unknown): boolean => {
  if (typeof value === "boolean") return value;
  if (typeof value !== "string") return false;
  return value === "t" || value.toLowerCase() === "true";
};

export const isFocusedFullPredictionComplete = async (
  params: CompletionParams,
): Promise<boolean> => {
  const source = sourceForCategory(params.category);
  const kaisaiNen = params.runYmd.slice(0, RUN_YMD_YEAR_END);
  const kaisaiTsukihi = params.runYmd.slice(RUN_YMD_MONTH_START, RUN_YMD_DAY_END);
  const sql = neon(params.env.NEON_DATABASE_URL);
  const result = await sql.query(
    `
      with expected as (
        select distinct ketto_toroku_bango
        from race_entry_corner_features
        where source = $1
          and kaisai_nen = $2
          and kaisai_tsukihi = $3
          and keibajo_code = $4
          and race_bango = $5
      ),
      expected_total as (
        select count(*)::int as expected_rows
        from expected
      ),
      model_counts as (
        select p.model_version, count(distinct p.ketto_toroku_bango)::int as actual_rows
        from race_finish_position_model_predictions p
        join expected e on e.ketto_toroku_bango = p.ketto_toroku_bango
        where p.source = $1
          and p.kaisai_nen = $2
          and p.kaisai_tsukihi = $3
          and p.keibajo_code = $4
          and p.race_bango = $5
        group by p.model_version
      )
      select
        expected_total.expected_rows,
        coalesce(max(model_counts.actual_rows), 0)::int as actual_rows,
        coalesce(bool_or(model_counts.actual_rows = expected_total.expected_rows), false) as complete
      from expected_total
      left join model_counts on true
      group by expected_total.expected_rows
    `,
    [source, kaisaiNen, kaisaiTsukihi, params.keibajoCode, params.raceBango],
  );
  const row = (result as CompletionRow[])[0];
  if (row === undefined) return false;
  const expectedRows = toCount(row.expected_rows);
  return expectedRows > 0 && toBoolean(row.complete);
};
