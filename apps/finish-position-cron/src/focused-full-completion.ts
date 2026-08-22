// Run with bun. Raw-Catalog-backed completion guard for focused per-race full messages.

import { neon } from "@neondatabase/serverless";
import { buildPerRaceFeatCacheKey } from "./scoring/feature-cache";
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

// Mirrors predict_lib/stage1_routing.json's per-category fallback
// model_version (both entries `enabled: true` there today). resolve_stage1_gate
// is a race-level decision -- every horse in a gated race is written under this
// SAME model_version, never the category default -- so a race that took the
// stage1 gate never has any rows under expectedModelVersion()'s return value.
// Before this map existed, isFocusedFullPredictionComplete only ever checked
// the primary/default model_version, so a stage1-gated race could never be
// observed "complete": the redelivery poll retried until max_retries and the
// message was dropped, and pickUpFocusedFullCache (only reachable from the
// "already complete" branch) never ran -- the per-race R2 feat-cache was
// never seeded for any race the freshness gate routed to stage1, which is
// most of NAR on days a live coordinator rescore never re-touches an odds
// column (2026-07-24 finding: this is why COORDINATOR_ENABLED's re-enable
// attempt on 2026-07-22 always hit an empty per-race cache -- see git log for
// this file's ONE commit around 2026-07-24 for the incident trail). ban-ei
// has no stage1_routing.json entry, so it stays without a fallback here too.
const STAGE1_MARKET_FREE_MODEL_VERSIONS: Partial<Record<PredictCategory, string>> = {
  jra: "jra-cb-stage1-marketfree235-2013",
  nar: "iter12-nar-xgb-hpo-v8-stage1-marketfree-184",
};

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

interface ExpectedModelVersionParams {
  category: PredictCategory;
  entries: readonly CatalogEntry[];
  env: Env;
  keibajoCode: string;
}

// Mirrors cell_routing.json's JRA rules, in the SAME first-match-wins order:
// (1) kyoso_joken_code=703, (2) dirt + f_le10 + kyoso_joken_code=005, (3)
// venue=02. Rules 1/2 are mutually exclusive (a race carries exactly one
// kyoso_joken_code) so their relative check order never matters, but rule 3
// (venue) is a strictly LOWER-priority fallback -- a venue=02 race that ALSO
// matches rule 1 or 2 must still resolve to that higher-priority variant, not
// jockey-pedigree269 by virtue of venue alone.
const JRA_VENUE_703_KEIBAJO_CODE = "02";

const expectedModelVersion = (params: ExpectedModelVersionParams): string => {
  const { category, entries, env, keibajoCode } = params;
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
  if (keibajoCode.padStart(2, "0") === JRA_VENUE_703_KEIBAJO_CODE) {
    return JRA_703_MODEL_VERSION;
  }
  return JRA_DEFAULT_MODEL_VERSION;
};

interface CountMatchParams {
  env: Env;
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  modelVersion: string;
  kettoTorokuBangos: readonly string[];
  expectedCount: number;
  runYmd: string;
}

const buildRunDateStartUtc = (runYmd: string): string | null => {
  if (runYmd.length !== 8) return null;
  const year = runYmd.slice(0, 4);
  const month = runYmd.slice(4, 6);
  const day = runYmd.slice(6, 8);
  const dateStr = `${year}-${month}-${day}`;
  const jstDate = new Date(`${dateStr}T00:00:00+09:00`);
  if (Number.isNaN(jstDate.getTime())) return null;
  return jstDate.toISOString().slice(0, 19);
};

const countMatchesModelVersion = async (params: CountMatchParams): Promise<boolean> => {
  const sql = neon(params.env.NEON_DATABASE_URL);
  const runDateStartUtc = buildRunDateStartUtc(params.runYmd);
  if (runDateStartUtc === null) return false;
  const result: unknown = await sql.query(
    `select count(distinct ketto_toroku_bango)::int as actual_rows
       from race_finish_position_model_predictions
      where source = $1
        and kaisai_nen = $2
        and kaisai_tsukihi = $3
        and keibajo_code = $4
        and race_bango = $5
        and model_version = $6
        and ketto_toroku_bango = any($7::text[])
        and prediction_generated_at >= $8::timestamp`,
    [
      params.source,
      params.kaisaiNen,
      params.kaisaiTsukihi,
      params.keibajoCode,
      params.raceBango,
      params.modelVersion,
      params.kettoTorokuBangos,
      runDateStartUtc,
    ],
  );
  if (!Array.isArray(result) || !isRecord(result[0])) return false;
  return toCount(result[0].actual_rows) === params.expectedCount;
};

export const isFocusedFullPredictionComplete = async (
  params: CompletionParams,
): Promise<boolean> => {
  const entries = await fetchExpectedEntries(params);
  if (entries.length === 0) return false;
  const modelVersion = expectedModelVersion({
    category: params.category,
    entries,
    env: params.env,
    keibajoCode: params.keibajoCode,
  });
  const shared = {
    env: params.env,
    expectedCount: entries.length,
    kaisaiNen: params.runYmd.slice(0, RUN_YMD_YEAR_END),
    kaisaiTsukihi: params.runYmd.slice(RUN_YMD_MONTH_START, RUN_YMD_DAY_END),
    keibajoCode: params.keibajoCode.padStart(2, "0"),
    kettoTorokuBangos: entries.map((entry) => entry.kettoTorokuBango),
    raceBango: params.raceBango.padStart(2, "0"),
    runYmd: params.runYmd,
    source: sourceForCategory(params.category),
  };
  if (await countMatchesModelVersion({ ...shared, modelVersion })) return true;
  // A race the freshness gate routed to stage1 (predict_upcoming.py's
  // resolve_stage1_gate) never has any rows under expectedModelVersion()'s
  // return value -- every horse in that race was written under the
  // category's stage1_routing.json fallback model_version instead. Without
  // this second check, such a race can never be observed "complete" here:
  // see STAGE1_MARKET_FREE_MODEL_VERSIONS' comment for the full incident.
  const stage1ModelVersion = STAGE1_MARKET_FREE_MODEL_VERSIONS[params.category];
  if (stage1ModelVersion === undefined) return false;
  return countMatchesModelVersion({ ...shared, modelVersion: stage1ModelVersion });
};

export const isPerRaceFeatureCachePresent = async (params: CompletionParams): Promise<boolean> => {
  const cacheKey = buildPerRaceFeatCacheKey(
    params.category,
    params.runYmd,
    params.keibajoCode,
    params.raceBango,
  );
  return (await params.env.FEATURES_CACHE.head(cacheKey)) !== null;
};

// Rescore must never be the first prediction pass. It also requires the
// per-race R2 feature cache: Neon completion alone is insufficient because the
// Python rescore path falls back to a long full rebuild when that cache is
// absent, monopolizing the canonical container and delaying later races.
export const isPerRaceRescoreReady = async (params: CompletionParams): Promise<boolean> => {
  if (!(await isFocusedFullPredictionComplete(params))) return false;
  return isPerRaceFeatureCachePresent(params);
};
