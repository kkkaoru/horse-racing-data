// Run with bun. Raw-Catalog-backed completion guard for focused per-race full messages.

import { neon } from "@neondatabase/serverless";
import type { Env, PredictCategory } from "./types";

interface CompletionParams {
  env: Env;
  category: PredictCategory;
  runYmd: string;
  keibajoCode: string;
  raceBango: string;
}

interface CatalogEntry {
  gradeCode: string | null;
  kettoTorokuBango: string;
  kyosoJokenCode: string | null;
  shussoTosu: number | null;
  trackCode: string | null;
}

const RUN_YMD_YEAR_END = 4;
const RUN_YMD_MONTH_START = 4;
const RUN_YMD_DAY_END = 8;
const CATALOG_ORIGIN = "https://pc-keiba-r2-catalog.internal";
const NAR_SOURCE = "nar";
const JRA_SOURCE = "jra";
const JRA_DEFAULT_MODEL_VERSION = "jra-cb-v9-sim-2013-clean";
const JRA_703_MODEL_VERSION = "jra-cb-v9-sim-2013-clean-jockey-pedigree269";
const JRA_PRIOR_CORNER_005_MODEL_VERSION = "jra-cb-v10-prior-corner274-2013";
const NAR_DEFAULT_MODEL_VERSION = "iter12-nar-xgb-hpo-v8-clean188";
const NAR_TRANSFORMER_MODEL_VERSION = "iter40-nar-settransformer-blend-v1";
const BANEI_DEFAULT_MODEL_VERSION = "banei-cb-v9-sim-2011";
const BANEI_GRADE_E_MODEL_VERSION = "banei-cb-v8-window2011-wf-15y";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const sourceForCategory = (category: PredictCategory): string => {
  if (category === "jra") return JRA_SOURCE;
  return NAR_SOURCE;
};

const catalogSourceForCategory = (category: PredictCategory): string =>
  category === "ban-ei" ? "ban-ei" : category;

const toCount = (value: unknown): number => {
  if (typeof value === "number" && Number.isFinite(value)) return Math.max(0, Math.trunc(value));
  if (typeof value === "bigint") return Number(value > 0n ? value : 0n);
  if (typeof value !== "string") return 0;
  const parsed = Number.parseInt(value, 10);
  if (Number.isFinite(parsed)) return Math.max(0, parsed);
  return 0;
};

const optionalString = (value: unknown): string | null =>
  typeof value === "string" && value.length > 0 ? value : null;

const optionalNumber = (value: unknown): number | null =>
  typeof value === "number" && Number.isFinite(value) ? value : null;

const parseCatalogEntry = (value: unknown): CatalogEntry => {
  if (!isRecord(value) || typeof value.ketto_toroku_bango !== "string") {
    throw new Error("Catalog race-feature response contains an invalid entry");
  }
  return {
    gradeCode: optionalString(value.grade_code),
    kettoTorokuBango: value.ketto_toroku_bango,
    kyosoJokenCode: optionalString(value.kyoso_joken_code),
    shussoTosu: optionalNumber(value.shusso_tosu),
    trackCode: optionalString(value.track_code),
  };
};

const fetchExpectedEntries = async (params: CompletionParams): Promise<CatalogEntry[]> => {
  const catalog = params.env.PC_KEIBA_R2_CATALOG;
  if (catalog === undefined) {
    throw new Error("PC_KEIBA_R2_CATALOG binding is required for completion checks");
  }
  const url = new URL("/v1/race-features", CATALOG_ORIGIN);
  url.searchParams.set("date", params.runYmd);
  url.searchParams.set("source", catalogSourceForCategory(params.category));
  url.searchParams.set("keibajoCode", params.keibajoCode.padStart(2, "0"));
  url.searchParams.set("raceBango", params.raceBango.padStart(2, "0"));
  const response = await catalog.fetch(new Request(url));
  if (!response.ok) {
    throw new Error(`PC_KEIBA_R2_CATALOG race-features failed with HTTP ${response.status}`);
  }
  const payload: unknown = await response.json();
  if (!isRecord(payload) || !Array.isArray(payload.rows)) {
    throw new Error("Catalog race-feature response has invalid rows");
  }
  const entries = payload.rows.map(parseCatalogEntry);
  return [...new Map(entries.map((entry) => [entry.kettoTorokuBango, entry])).values()];
};

const isNarTransformerBlendEnabled = (env: Env): boolean => {
  const raw = env.NAR_TRANSFORMER_BLEND_ENABLED;
  if (raw === undefined) return true;
  const normalized = raw.trim().toLowerCase();
  return normalized !== "0" && normalized !== "false" && normalized !== "off";
};

const expectedModelVersion = (
  category: PredictCategory,
  entries: readonly CatalogEntry[],
  env: Env,
): string => {
  if (category === "nar") {
    return isNarTransformerBlendEnabled(env)
      ? NAR_TRANSFORMER_MODEL_VERSION
      : NAR_DEFAULT_MODEL_VERSION;
  }
  if (category === "ban-ei") {
    return entries.some((entry) => entry.gradeCode?.trim() === "E")
      ? BANEI_GRADE_E_MODEL_VERSION
      : BANEI_DEFAULT_MODEL_VERSION;
  }
  const isPriorCorner005 = entries.some(
    (entry) =>
      entry.trackCode?.trim().startsWith("2") === true &&
      entry.kyosoJokenCode?.trim() === "005" &&
      entry.shussoTosu !== null &&
      entry.shussoTosu <= 10,
  );
  if (isPriorCorner005) return JRA_PRIOR_CORNER_005_MODEL_VERSION;
  if (entries.some((entry) => entry.kyosoJokenCode?.trim() === "703")) {
    return JRA_703_MODEL_VERSION;
  }
  return JRA_DEFAULT_MODEL_VERSION;
};

export const isFocusedFullPredictionComplete = async (
  params: CompletionParams,
): Promise<boolean> => {
  const entries = await fetchExpectedEntries(params);
  if (entries.length === 0) return false;
  const source = sourceForCategory(params.category);
  const kaisaiNen = params.runYmd.slice(0, RUN_YMD_YEAR_END);
  const kaisaiTsukihi = params.runYmd.slice(RUN_YMD_MONTH_START, RUN_YMD_DAY_END);
  const modelVersion = expectedModelVersion(params.category, entries, params.env);
  const kettoTorokuBangos = entries.map((entry) => entry.kettoTorokuBango);
  const sql = neon(params.env.NEON_DATABASE_URL);
  const result: unknown = await sql.query(
    `select count(distinct ketto_toroku_bango)::int as actual_rows
       from race_finish_position_model_predictions
      where source = $1
        and kaisai_nen = $2
        and kaisai_tsukihi = $3
        and keibajo_code = $4
        and race_bango = $5
        and model_version = $6
        and ketto_toroku_bango = any($7::text[])`,
    [
      source,
      kaisaiNen,
      kaisaiTsukihi,
      params.keibajoCode.padStart(2, "0"),
      params.raceBango.padStart(2, "0"),
      modelVersion,
      kettoTorokuBangos,
    ],
  );
  if (!Array.isArray(result) || !isRecord(result[0])) return false;
  return toCount(result[0].actual_rows) === entries.length;
};
