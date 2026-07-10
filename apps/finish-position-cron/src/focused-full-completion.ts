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
  expected_model_version: unknown;
}

const RUN_YMD_YEAR_END = 4;
const RUN_YMD_MONTH_START = 4;
const RUN_YMD_DAY_END = 8;
const NAR_SOURCE = "nar";
const JRA_SOURCE = "jra";
const JRA_DEFAULT_MODEL_VERSION = "jra-cb-v9-sim-2013-clean";
const JRA_703_MODEL_VERSION = "jra-cb-v9-sim-2013-clean-jockey-pedigree269";
const JRA_PRIOR_CORNER_005_MODEL_VERSION = "jra-cb-v10-prior-corner274-2013";
const NAR_DEFAULT_MODEL_VERSION = "iter12-nar-xgb-hpo-v8-clean188";
const NAR_TRANSFORMER_MODEL_VERSION = "iter40-nar-settransformer-blend-v1";
const BANEI_DEFAULT_MODEL_VERSION = "banei-cb-v9-sim-2011";
const BANEI_GRADE_E_MODEL_VERSION = "banei-cb-v8-window2011-wf-15y";

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

const isNarTransformerBlendEnabled = (env: Env): boolean => {
  const raw = env.NAR_TRANSFORMER_BLEND_ENABLED;
  if (raw === undefined) return true;
  const normalized = raw.trim().toLowerCase();
  return normalized !== "0" && normalized !== "false" && normalized !== "off";
};

export const isFocusedFullPredictionComplete = async (
  params: CompletionParams,
): Promise<boolean> => {
  const source = sourceForCategory(params.category);
  const kaisaiNen = params.runYmd.slice(0, RUN_YMD_YEAR_END);
  const kaisaiTsukihi = params.runYmd.slice(RUN_YMD_MONTH_START, RUN_YMD_DAY_END);
  const narExpectedModelVersion = isNarTransformerBlendEnabled(params.env)
    ? NAR_TRANSFORMER_MODEL_VERSION
    : NAR_DEFAULT_MODEL_VERSION;
  const sql = neon(params.env.NEON_DATABASE_URL);
  const result = await sql.query(
    `
      with expected as (
        select distinct ketto_toroku_bango, kyoso_joken_code, grade_code, track_code, shusso_tosu
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
      expected_model as (
        select
          case
            when $6 = 'jra'
              and bool_or(trim(coalesce(track_code::text, '')) like '2%')
              and bool_or(trim(coalesce(kyoso_joken_code::text, '')) = '005')
              and bool_or(coalesce(shusso_tosu, 0) <= 10)
              then '${JRA_PRIOR_CORNER_005_MODEL_VERSION}'
            when $6 = 'jra'
              and bool_or(trim(coalesce(kyoso_joken_code::text, '')) = '703')
              then '${JRA_703_MODEL_VERSION}'
            when $6 = 'jra'
              then '${JRA_DEFAULT_MODEL_VERSION}'
            when $6 = 'ban-ei'
              and bool_or(trim(coalesce(grade_code::text, '')) = 'E')
              then '${BANEI_GRADE_E_MODEL_VERSION}'
            when $6 = 'ban-ei'
              then '${BANEI_DEFAULT_MODEL_VERSION}'
            else $7
          end as expected_model_version
        from expected
      ),
      model_counts as (
        select p.model_version, count(distinct p.ketto_toroku_bango)::int as actual_rows
        from race_finish_position_model_predictions p
        join (select distinct ketto_toroku_bango from expected) e
          on e.ketto_toroku_bango = p.ketto_toroku_bango
        cross join expected_model
        where p.source = $1
          and p.kaisai_nen = $2
          and p.kaisai_tsukihi = $3
          and p.keibajo_code = $4
          and p.race_bango = $5
          and p.model_version = expected_model.expected_model_version
        group by p.model_version
      )
      select
        expected_total.expected_rows,
        expected_model.expected_model_version,
        coalesce(max(model_counts.actual_rows), 0)::int as actual_rows,
        coalesce(bool_or(model_counts.actual_rows = expected_total.expected_rows), false) as complete
      from expected_total
      cross join expected_model
      left join model_counts on true
      group by expected_total.expected_rows, expected_model.expected_model_version
    `,
    [
      source,
      kaisaiNen,
      kaisaiTsukihi,
      params.keibajoCode,
      params.raceBango,
      params.category,
      narExpectedModelVersion,
    ],
  );
  const row = (result as CompletionRow[])[0];
  if (row === undefined) return false;
  const expectedRows = toCount(row.expected_rows);
  return expectedRows > 0 && toBoolean(row.complete);
};
