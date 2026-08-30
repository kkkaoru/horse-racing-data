// Run with bun. Raw-Catalog-backed completion guard for focused per-race full messages.

import { neon } from "@neondatabase/serverless";
import { buildFinishPositionPredictionKvKey } from "./prediction-kv-keys";
import { buildPerRaceFeatCacheKey } from "./scoring/feature-cache";
import type { Env, PredictCategory } from "./types";

interface CompletionParams {
  env: Env;
  category: PredictCategory;
  runYmd: string;
  keibajoCode: string;
  raceBango: string;
  notBefore?: string;
}

interface CatalogEntry {
  gradeCode: string | null;
  kettoTorokuBango: string;
  kyori: number | null;
  kyosoJokenCode: string | null;
  shussoTosu: number | null;
  trackCode: string | null;
}

const RUN_YMD_YEAR_END = 4;
const RUN_YMD_MONTH_START = 4;
const RUN_YMD_DAY_END = 8;
const MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000;
const CATALOG_ORIGIN = "https://pc-keiba-r2-catalog.internal";
const NAR_SOURCE = "nar";
const JRA_SOURCE = "jra";
const JRA_DEFAULT_MODEL_VERSION = "jra-cb-v9-sim-2013-clean";
const JRA_703_MODEL_VERSION = "jra-cb-v9-sim-2013-clean-jockey-pedigree269";
const JRA_PRIOR_CORNER_005_MODEL_VERSION = "jra-cb-v10-prior-corner274-2013";
const JRA_703_DIRT_MILE_SUMMER_MODEL_VERSION = "jra-joken-703-querysoftmax-maxrange-v1";
const JRA_005_TURF_LONG_MODEL_VERSION = "jra-joken-005-turf-long-hierarchical-qsm-gated-v2";
const JRA_005_DIRT_1200_WINTER_SUMMER_MODEL_VERSION =
  "jra-joken-005-dirt-1200-winter-summer-qsm-gated-v1";
const JRA_005_DIRT_1700_SUMMER_MODEL_VERSION = "jra-joken-005-dirt-1700-summer-qsm-gated-v1";
const JRA_005_DIRT_1800_NONAUTUMN_MODEL_VERSION = "jra-joken-005-dirt-1800-nonautumn-qsm-gated-v1";
const JRA_005_DIRT_INTERMEDIATE_AUTUMN_MODEL_VERSION =
  "jra-joken-005-dirt-intermediate-autumn-yeti-gated-v1";
const JRA_005_DIRT_MILE_AUTUMN_MODEL_VERSION = "jra-joken-005-dirt-mile-autumn-yeti-gated-v1";
const JRA_005_DIRT_MILE_SPRING_MODEL_VERSION = "jra-joken-005-dirt-mile-spring-qsm-gated-v1";
const JRA_005_TURF_INTERMEDIATE_SPRING_MODEL_VERSION =
  "jra-joken-005-turf-intermediate-spring-qsm-gated-v1";
const JRA_005_TURF_MILE_MODEL_VERSION = "jra-joken-005-turf-mile-yeti-gated-v1";
const JRA_010_DIRT_INTERMEDIATE_MODEL_VERSION = "jra-joken-010-dirt-intermediate-yeti-gated-v1";
const JRA_701_TURF_INTERMEDIATE_MODEL_VERSION = "jra-joken-701-turf-intermediate-qsm-gated-v1";
const JRA_701_TURF_LONG_MODEL_VERSION = "jra-joken-701-turf-long-qsm-gated-v1";
const JRA_701_TURF_MILE_MODEL_VERSION = "jra-joken-701-turf-mile-qsm-gated-v1";
const JRA_703_DIRT_INTERMEDIATE_MODEL_VERSION = "jra-joken-703-dirt-intermediate-qsm-gated-v1";
const JRA_703_DIRT_SPRINT_MODEL_VERSION = "jra-joken-703-dirt-sprint-yeti-gated-v1";
const JRA_703_OTHER_EXTENDED_MODEL_VERSION = "jra-joken-703-other-extended-qsm-gated-v1";
const JRA_703_TURF_1200_LARGEFIELD_MODEL_VERSION =
  "jra-joken-703-turf-1200-largefield-yeti-gated-v1";
const JRA_703_TURF_1400_MODEL_VERSION = "jra-joken-703-turf-1400-qsm-gated-v1";
const JRA_703_TURF_INTERMEDIATE_MODEL_VERSION = "jra-joken-703-turf-intermediate-qsm-gated-v1";
const JRA_703_TURF_LONG_SPRING_MODEL_VERSION = "jra-joken-703-turf-long-spring-qsm-gated-v1";
const JRA_703_TURF_LONG_SUMMER_MODEL_VERSION = "jra-joken-703-turf-long-summer-yeti-gated-v1";
const JRA_TURF_TRACK_CODES: ReadonlySet<string> = new Set([
  "10",
  "11",
  "12",
  "13",
  "14",
  "15",
  "16",
  "17",
  "18",
  "19",
  "20",
  "21",
  "22",
]);
const JRA_DIRT_TRACK_CODES: ReadonlySet<string> = new Set([
  "23",
  "24",
  "25",
  "26",
  "27",
  "28",
  "29",
]);
const JRA_AUTUMN_MONTHS: ReadonlySet<string> = new Set(["09", "10", "11"]);
const JRA_SPRING_MONTHS: ReadonlySet<string> = new Set(["03", "04", "05"]);
const JRA_WINTER_MONTHS: ReadonlySet<string> = new Set(["12", "01", "02"]);
const JRA_SUMMER_MONTHS: ReadonlySet<string> = new Set(["06", "07", "08"]);
const JRA_JOKEN_MODEL_VERSIONS: ReadonlyMap<string, string> = new Map([
  ["005", "jra-joken-005-pooled-yetirank-v2"],
  ["010", "jra-joken-010-pooled-yetirank-v2"],
  ["016", "jra-joken-016-pooled-yetirank-v2"],
  ["701", "jra-joken-701-pooled-yetirank-v2"],
  ["703", "jra-joken-703-pooled-yetirank-v2"],
  ["999", "jra-joken-999-pooled-yetirank-v2"],
]);
const NAR_DEFAULT_MODEL_VERSION = "iter12-nar-xgb-hpo-v8-clean188";
const NAR_TRANSFORMER_MODEL_VERSION = "iter40-nar-settransformer-blend-v1";
const NAR_CELL_TOP1_MODEL_VERSIONS: readonly string[] = [
  "nar-cell-top1-30-mukatsu-sprint-summer-tc1-v1",
  "nar-cell-top1-42-c-sprint-summer-tc1-v1",
  "nar-cell-top1-30-c-sprint-summer-tc1-v1",
  "nar-cell-top1-30-c-sprint-summer-tc2-adaptive-v1",
  "nar-cell-top1-50-c-sprint-summer-tc1-rolling-v1",
  "nar-cell-top1-43-c-sprint-winter-tc1-rolling-v1",
  "nar-cell-top1-43-c-mile-summer-tc1-rolling-v1",
  "nar-cell-top1-43-c-1400-1500-tc1-rolling-v1",
];
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
    kyori: optionalNumber(value.kyori),
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
  runYmd: string;
}

// Mirrors cell_routing.json's JRA first-match-wins rules. The ungraded 005
// turf-mile/turf-long, 701 turf-long, and 703 other-extended/dirt-intermediate/
// dirt-mile-summer specialists come first,
// followed by the six class rules.
// Container derive_class gives a non-empty grade_code precedence; only an
// ungraded race derives class=joken-{kyoso_joken_code}. The legacy 703,
// dirt-small-field-005, and venue=02 rules follow.
const JRA_VENUE_703_KEIBAJO_CODE = "02";

const expectedModelVersion = (params: ExpectedModelVersionParams): string => {
  const { category, entries, env, keibajoCode, runYmd } = params;
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
  const entry = entries[0];
  const gradeCode = entry?.gradeCode?.trim() ?? "";
  const kyosoJokenCode = entry?.kyosoJokenCode?.trim() ?? "";
  const use005DirtMileAutumn =
    gradeCode.length === 0 &&
    kyosoJokenCode === "005" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_DIRT_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 1200 &&
    entry.kyori < 1600 &&
    JRA_AUTUMN_MONTHS.has(runYmd.slice(RUN_YMD_MONTH_START, 6));
  if (use005DirtMileAutumn) return JRA_005_DIRT_MILE_AUTUMN_MODEL_VERSION;
  const use005DirtMileSpring =
    gradeCode.length === 0 &&
    kyosoJokenCode === "005" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_DIRT_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 1200 &&
    entry.kyori < 1600 &&
    JRA_SPRING_MONTHS.has(runYmd.slice(RUN_YMD_MONTH_START, 6));
  if (use005DirtMileSpring) return JRA_005_DIRT_MILE_SPRING_MODEL_VERSION;
  const use005DirtIntermediateAutumn =
    gradeCode.length === 0 &&
    kyosoJokenCode === "005" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_DIRT_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 1600 &&
    entry.kyori < 2000 &&
    JRA_AUTUMN_MONTHS.has(runYmd.slice(RUN_YMD_MONTH_START, 6));
  if (use005DirtIntermediateAutumn) return JRA_005_DIRT_INTERMEDIATE_AUTUMN_MODEL_VERSION;
  const use005Dirt1700Summer =
    gradeCode.length === 0 &&
    kyosoJokenCode === "005" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_DIRT_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori === 1700 &&
    JRA_SUMMER_MONTHS.has(runYmd.slice(RUN_YMD_MONTH_START, 6));
  if (use005Dirt1700Summer) return JRA_005_DIRT_1700_SUMMER_MODEL_VERSION;
  const runMonth = runYmd.slice(RUN_YMD_MONTH_START, 6);
  const use005Dirt1200WinterSummer =
    gradeCode.length === 0 &&
    kyosoJokenCode === "005" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_DIRT_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori === 1200 &&
    (JRA_WINTER_MONTHS.has(runMonth) || JRA_SUMMER_MONTHS.has(runMonth));
  if (use005Dirt1200WinterSummer) return JRA_005_DIRT_1200_WINTER_SUMMER_MODEL_VERSION;
  const use005Dirt1800NonAutumn =
    gradeCode.length === 0 &&
    kyosoJokenCode === "005" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_DIRT_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori === 1800 &&
    (JRA_WINTER_MONTHS.has(runMonth) ||
      JRA_SPRING_MONTHS.has(runMonth) ||
      JRA_SUMMER_MONTHS.has(runMonth));
  if (use005Dirt1800NonAutumn) return JRA_005_DIRT_1800_NONAUTUMN_MODEL_VERSION;
  const use005TurfIntermediateSpring =
    gradeCode.length === 0 &&
    kyosoJokenCode === "005" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_TURF_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 1600 &&
    entry.kyori < 2000 &&
    JRA_SPRING_MONTHS.has(runYmd.slice(RUN_YMD_MONTH_START, 6));
  if (use005TurfIntermediateSpring) return JRA_005_TURF_INTERMEDIATE_SPRING_MODEL_VERSION;
  const use005TurfMile =
    gradeCode.length === 0 &&
    kyosoJokenCode === "005" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_TURF_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 1200 &&
    entry.kyori < 1600;
  if (use005TurfMile) return JRA_005_TURF_MILE_MODEL_VERSION;
  const use005TurfLong =
    gradeCode.length === 0 &&
    kyosoJokenCode === "005" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_TURF_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 2000 &&
    entry.kyori < 2400;
  if (use005TurfLong) return JRA_005_TURF_LONG_MODEL_VERSION;
  const use010DirtIntermediate =
    gradeCode.length === 0 &&
    kyosoJokenCode === "010" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_DIRT_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 1600 &&
    entry.kyori < 2000;
  if (use010DirtIntermediate) return JRA_010_DIRT_INTERMEDIATE_MODEL_VERSION;
  const use701TurfLong =
    gradeCode.length === 0 &&
    kyosoJokenCode === "701" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_TURF_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 2000 &&
    entry.kyori < 2400;
  if (use701TurfLong) return JRA_701_TURF_LONG_MODEL_VERSION;
  const use701TurfMile =
    gradeCode.length === 0 &&
    kyosoJokenCode === "701" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_TURF_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 1200 &&
    entry.kyori < 1600;
  if (use701TurfMile) return JRA_701_TURF_MILE_MODEL_VERSION;
  const use701TurfIntermediate =
    gradeCode.length === 0 &&
    kyosoJokenCode === "701" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_TURF_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 1600 &&
    entry.kyori < 2000;
  if (use701TurfIntermediate) return JRA_701_TURF_INTERMEDIATE_MODEL_VERSION;
  const use703TurfLongSpring =
    gradeCode.length === 0 &&
    kyosoJokenCode === "703" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_TURF_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 2000 &&
    entry.kyori < 2400 &&
    JRA_SPRING_MONTHS.has(runYmd.slice(RUN_YMD_MONTH_START, 6));
  if (use703TurfLongSpring) return JRA_703_TURF_LONG_SPRING_MODEL_VERSION;
  const use703TurfLongSummer =
    gradeCode.length === 0 &&
    kyosoJokenCode === "703" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_TURF_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 2000 &&
    entry.kyori < 2400 &&
    JRA_SUMMER_MONTHS.has(runYmd.slice(RUN_YMD_MONTH_START, 6));
  if (use703TurfLongSummer) return JRA_703_TURF_LONG_SUMMER_MODEL_VERSION;
  const use703Turf1400 =
    gradeCode.length === 0 &&
    kyosoJokenCode === "703" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_TURF_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori === 1400;
  if (use703Turf1400) return JRA_703_TURF_1400_MODEL_VERSION;
  const use703Turf1200LargeField =
    gradeCode.length === 0 &&
    kyosoJokenCode === "703" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_TURF_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori === 1200 &&
    entries.length >= 14;
  if (use703Turf1200LargeField) return JRA_703_TURF_1200_LARGEFIELD_MODEL_VERSION;
  const use703TurfIntermediate =
    gradeCode.length === 0 &&
    kyosoJokenCode === "703" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_TURF_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 1600 &&
    entry.kyori < 2000;
  if (use703TurfIntermediate) return JRA_703_TURF_INTERMEDIATE_MODEL_VERSION;
  const use703OtherExtended =
    gradeCode.length === 0 &&
    kyosoJokenCode === "703" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    !JRA_TURF_TRACK_CODES.has(entry.trackCode.trim()) &&
    !JRA_DIRT_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 2400;
  if (use703OtherExtended) return JRA_703_OTHER_EXTENDED_MODEL_VERSION;
  const use703DirtSprint =
    gradeCode.length === 0 &&
    kyosoJokenCode === "703" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_DIRT_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori < 1200;
  if (use703DirtSprint) return JRA_703_DIRT_SPRINT_MODEL_VERSION;
  const use703DirtIntermediate =
    gradeCode.length === 0 &&
    kyosoJokenCode === "703" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_DIRT_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 1600 &&
    entry.kyori < 2000;
  if (use703DirtIntermediate) return JRA_703_DIRT_INTERMEDIATE_MODEL_VERSION;
  const use703DirtMileSummer =
    gradeCode.length === 0 &&
    kyosoJokenCode === "703" &&
    entry?.trackCode !== null &&
    entry?.trackCode !== undefined &&
    JRA_DIRT_TRACK_CODES.has(entry.trackCode.trim()) &&
    entry.kyori !== null &&
    entry.kyori >= 1200 &&
    entry.kyori < 1600 &&
    JRA_SUMMER_MONTHS.has(runYmd.slice(RUN_YMD_MONTH_START, 6));
  if (use703DirtMileSummer) return JRA_703_DIRT_MILE_SUMMER_MODEL_VERSION;
  const jokenModelVersion =
    gradeCode.length === 0 ? JRA_JOKEN_MODEL_VERSIONS.get(kyosoJokenCode) : undefined;
  if (jokenModelVersion !== undefined) return jokenModelVersion;
  if (kyosoJokenCode === "703") {
    return JRA_703_MODEL_VERSION;
  }
  const isPriorCorner005 =
    entry?.trackCode?.trim().startsWith("2") === true &&
    kyosoJokenCode === "005" &&
    entry.shussoTosu !== null &&
    entry.shussoTosu <= 10;
  if (isPriorCorner005) return JRA_PRIOR_CORNER_005_MODEL_VERSION;
  if (keibajoCode.padStart(2, "0") === JRA_VENUE_703_KEIBAJO_CODE) {
    return JRA_703_MODEL_VERSION;
  }
  return JRA_DEFAULT_MODEL_VERSION;
};

interface CachedPredictionFeature {
  horseNumber: string;
  modelVersion: string;
  predictionGeneratedAt: string;
}

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
  notBefore: string;
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

const buildPreviousDateStartUtc = (runYmd: string): string | null => {
  const runDateStart = buildRunDateStartUtc(runYmd);
  if (runDateStart === null) return null;
  return new Date(Date.parse(`${runDateStart}Z`) - MILLISECONDS_PER_DAY).toISOString();
};

const resolveCompletionNotBefore = (runYmd: string, notBefore?: string): string | null => {
  if (notBefore === undefined) return buildRunDateStartUtc(runYmd);
  const parsed = new Date(notBefore);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString().slice(0, 19);
};

const parseCachedPredictionFeature = (value: unknown): CachedPredictionFeature | null => {
  if (!isRecord(value)) return null;
  if (
    typeof value.horseNumber !== "string" ||
    value.horseNumber.length === 0 ||
    typeof value.modelVersion !== "string" ||
    value.modelVersion.length === 0 ||
    typeof value.predictionGeneratedAt !== "string" ||
    !Number.isFinite(Date.parse(value.predictionGeneratedAt))
  ) {
    return null;
  }
  return {
    horseNumber: value.horseNumber,
    modelVersion: value.modelVersion,
    predictionGeneratedAt: value.predictionGeneratedAt,
  };
};

const kvMatchesCompletion = async (
  params: CompletionParams,
  expectedCount: number,
  modelVersions: ReadonlySet<string>,
  notBefore: string,
): Promise<boolean> => {
  const kv = params.env.DETAIL_SECTION_CACHE_KV;
  if (kv === undefined) return false;
  const key = buildFinishPositionPredictionKvKey({
    keibajoCode: params.keibajoCode,
    mmdd: params.runYmd.slice(RUN_YMD_MONTH_START, RUN_YMD_DAY_END),
    raceBango: params.raceBango,
    year: params.runYmd.slice(0, RUN_YMD_YEAR_END),
  });
  const value: unknown = await kv.get(key, "json");
  if (!Array.isArray(value) || value.length !== expectedCount) return false;
  const rows = value.map(parseCachedPredictionFeature);
  if (rows.some((row) => row === null)) return false;
  const features = rows.filter((row): row is CachedPredictionFeature => row !== null);
  if (new Set(features.map((row) => row.horseNumber)).size !== expectedCount) return false;
  if (new Set(features.map((row) => row.predictionGeneratedAt)).size !== 1) return false;
  const notBeforeMs = Date.parse(`${notBefore}Z`);
  return features.every(
    (row) =>
      modelVersions.has(row.modelVersion) && Date.parse(row.predictionGeneratedAt) >= notBeforeMs,
  );
};

const countMatchesModelVersion = async (params: CountMatchParams): Promise<boolean> => {
  const sql = neon(params.env.NEON_DATABASE_URL);
  const result: unknown = await sql.query(
    `select count(distinct prediction.ketto_toroku_bango)::int as actual_rows
       from race_finish_position_model_predictions as prediction
      where prediction.source = $1
        and prediction.kaisai_nen = $2
        and prediction.kaisai_tsukihi = $3
        and prediction.keibajo_code = $4
        and prediction.race_bango = $5
        and prediction.model_version = $6
        and prediction.ketto_toroku_bango = any($7::text[])
        and prediction.prediction_generated_at >= $8::timestamp
        and not exists (
          select 1
            from race_finish_position_model_predictions as unexpected
           where unexpected.source = $1
             and unexpected.kaisai_nen = $2
             and unexpected.kaisai_tsukihi = $3
             and unexpected.keibajo_code = $4
             and unexpected.race_bango = $5
             and unexpected.model_version = $6
             and unexpected.prediction_generated_at >= $8::timestamp
             and not (unexpected.ketto_toroku_bango = any($7::text[]))
        )`,
    [
      params.source,
      params.kaisaiNen,
      params.kaisaiTsukihi,
      params.keibajoCode,
      params.raceBango,
      params.modelVersion,
      params.kettoTorokuBangos,
      params.notBefore,
    ],
  );
  if (!Array.isArray(result) || !isRecord(result[0])) return false;
  return toCount(result[0].actual_rows) === params.expectedCount;
};

export const isFocusedFullPredictionComplete = async (
  params: CompletionParams,
): Promise<boolean> => {
  const notBefore = resolveCompletionNotBefore(params.runYmd, params.notBefore);
  if (notBefore === null) return false;
  const entries = await fetchExpectedEntries(params);
  if (entries.length === 0) return false;
  const modelVersion = expectedModelVersion({
    category: params.category,
    entries,
    env: params.env,
    keibajoCode: params.keibajoCode,
    runYmd: params.runYmd,
  });
  const stage1ModelVersion = STAGE1_MARKET_FREE_MODEL_VERSIONS[params.category];
  const acceptableModelVersions = new Set([
    modelVersion,
    ...(stage1ModelVersion === undefined ? [] : [stage1ModelVersion]),
    ...(params.category === "nar" ? NAR_CELL_TOP1_MODEL_VERSIONS : []),
  ]);
  const kvComplete = await kvMatchesCompletion(
    params,
    entries.length,
    acceptableModelVersions,
    notBefore,
  ).catch((error: unknown) => {
    console.warn(
      `[focused-full-completion] KV preflight failed category=${params.category} runYmd=${params.runYmd} keibajo=${params.keibajoCode} race=${params.raceBango}: ${String(error)}`,
    );
    return false;
  });
  if (kvComplete) return true;
  const shared = {
    env: params.env,
    expectedCount: entries.length,
    kaisaiNen: params.runYmd.slice(0, RUN_YMD_YEAR_END),
    kaisaiTsukihi: params.runYmd.slice(RUN_YMD_MONTH_START, RUN_YMD_DAY_END),
    keibajoCode: params.keibajoCode.padStart(2, "0"),
    kettoTorokuBangos: entries.map((entry) => entry.kettoTorokuBango),
    raceBango: params.raceBango.padStart(2, "0"),
    notBefore,
    source: sourceForCategory(params.category),
  };
  if (await countMatchesModelVersion({ ...shared, modelVersion })) return true;
  // A race the freshness gate routed to stage1 (predict_upcoming.py's
  // resolve_stage1_gate) never has any rows under expectedModelVersion()'s
  // return value -- every horse in that race was written under the
  // category's stage1_routing.json fallback model_version instead. Without
  // this second check, such a race can never be observed "complete" here:
  // see STAGE1_MARKET_FREE_MODEL_VERSIONS' comment for the full incident.
  if (
    stage1ModelVersion !== undefined &&
    (await countMatchesModelVersion({ ...shared, modelVersion: stage1ModelVersion }))
  ) {
    return true;
  }
  if (params.category !== "nar") return false;
  const cellMatches = await Promise.all(
    NAR_CELL_TOP1_MODEL_VERSIONS.map((modelVersion) =>
      countMatchesModelVersion({ ...shared, modelVersion }),
    ),
  );
  return cellMatches.some((matches) => matches);
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
  const notBefore =
    params.notBefore === undefined ? buildPreviousDateStartUtc(params.runYmd) : params.notBefore;
  if (notBefore === null) return false;
  if (!(await isFocusedFullPredictionComplete({ ...params, notBefore }))) return false;
  return isPerRaceFeatureCachePresent(params);
};
