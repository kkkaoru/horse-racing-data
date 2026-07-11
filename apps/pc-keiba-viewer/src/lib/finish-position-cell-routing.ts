// Run with bun (bunx vitest)
//
// TypeScript port of the finish-position-predict-container's cell-level model
// routing (apps/finish-position-predict-container/src/predict_lib/cell_router.py
// + cell_routing.json), so the viewer's display-priority query can tell when a
// race was cell-routed to a non-default model and must surface that
// prediction instead of a later/duplicate write of the plain category
// default. See docs/finish-position-prediction-system.md for the incident
// this guards against (2026-07-11: a routed Hakodate prediction was shadowed
// by a stale-image fallback write of the champion model).
//
// FINISH_POSITION_CELL_ROUTING_CONFIG below is a hand-kept mirror of the
// container's cell_routing.json contract, not a runtime import of it (the
// container package is a separate deploy target with its own bundling, and
// this viewer's tsconfig does not enable resolveJsonModule). Its
// finish-position-cell-routing.test.ts parity test reads the real container
// file at test time and fails the build if the two diverge, so this mirror
// can never silently drift the way the 2026-07-03 hardcoded single-rule
// mechanism did before it was deleted wholesale on 2026-07-07.
import type { RaceDetail } from "./race-types";

export interface CellRoutingCondition {
  dimension: string;
  values: string[];
}

export interface CellRoutingRule {
  conditions: CellRoutingCondition[];
  variant: string;
}

export interface CellRoutingVariant {
  model_version: string;
  feature_count: number;
  architecture: string;
  feature_set_hash?: string;
}

export interface CellRoutingCategoryConfig {
  default_variant: string;
  variants: Record<string, CellRoutingVariant>;
  rules: CellRoutingRule[];
}

export type CellRoutingConfig = Record<string, CellRoutingCategoryConfig>;

interface ResolveDimensionParams {
  race: RaceDetail;
  dimension: string;
  category: string;
}

interface AllConditionsMatchParams {
  race: RaceDetail;
  conditions: CellRoutingCondition[];
  category: string;
}

export interface ResolveCellRoutingParams {
  race: RaceDetail;
  category: string;
}

const SPRINT_MAX_KYORI = 1200;
const MILE_MAX_KYORI = 1600;
const INTERMEDIATE_MAX_KYORI = 2000;
const LONG_MAX_KYORI = 2400;
const SMALL_FIELD_MAX_SHUSSO_TOSU = 10;
const MEDIUM_FIELD_MAX_SHUSSO_TOSU = 13;
const LARGE_FIELD_MAX_SHUSSO_TOSU = 15;
const MONTH_PREFIX_LENGTH = 2;
const MONTH_PATTERN = /^\d{2}$/u;
const SPRING_MONTHS = new Set([3, 4, 5]);
const SUMMER_MONTHS = new Set([6, 7, 8]);
const AUTUMN_MONTHS = new Set([9, 10, 11]);

// Mirrors cell_routing.json exactly (jra / ban-ei rules currently deployed).
// See the module comment above for why this is a hand-kept copy rather than
// a runtime import, and finish-position-cell-routing.test.ts for the parity
// guard that keeps it honest.
const FINISH_POSITION_CELL_ROUTING_CONFIG: CellRoutingConfig = {
  "ban-ei": {
    default_variant: "sim",
    rules: [
      {
        conditions: [{ dimension: "grade_code", values: ["E"] }],
        variant: "base",
      },
    ],
    variants: {
      base: {
        architecture: "catboost",
        feature_count: 111,
        model_version: "banei-cb-v8-window2011-wf-15y",
      },
      sim: {
        architecture: "catboost",
        feature_count: 130,
        model_version: "banei-cb-v9-sim-2011",
      },
    },
  },
  jra: {
    default_variant: "sim",
    rules: [
      {
        conditions: [{ dimension: "kyoso_joken_code", values: ["703"] }],
        variant: "jockey_pedigree_703",
      },
      {
        conditions: [
          { dimension: "surface", values: ["dirt"] },
          { dimension: "field_band", values: ["f_le10"] },
          { dimension: "kyoso_joken_code", values: ["005"] },
        ],
        variant: "prior_corner_dirt_smallfield_005",
      },
      {
        conditions: [{ dimension: "venue", values: ["02"] }],
        variant: "jockey_pedigree_703",
      },
    ],
    variants: {
      jockey_pedigree_703: {
        architecture: "catboost",
        feature_count: 269,
        feature_set_hash: "1f70d678d48b485d4fcf593de786880c8fcf748e464174279f1dfe1251c9ef07",
        model_version: "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
      },
      prior_corner_dirt_smallfield_005: {
        architecture: "catboost",
        feature_count: 274,
        feature_set_hash: "0b90ab1c7e19ef8d61c2b5419bd034bf277600c73b3f4a05e3b1ff1d99bbbb22",
        model_version: "jra-cb-v10-prior-corner274-2013",
      },
      sim: {
        architecture: "catboost",
        feature_count: 250,
        model_version: "jra-cb-v9-sim-2013-clean",
      },
    },
  },
};

const parseIntOrNull = (value: string | null): number | null => {
  if (value === null) return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : null;
};

const trimmedOrNull = (value: string | null): string | null =>
  value === null ? null : value.trim();

const resolveDistanceBand = (kyori: string | null): string | null => {
  const parsed = parseIntOrNull(kyori);
  if (parsed === null) return null;
  if (parsed < SPRINT_MAX_KYORI) return "sprint";
  if (parsed < MILE_MAX_KYORI) return "mile";
  if (parsed < INTERMEDIATE_MAX_KYORI) return "intermediate";
  if (parsed < LONG_MAX_KYORI) return "long";
  return "extended";
};

const resolveFieldBand = (shussoTosu: string | null): string | null => {
  const parsed = parseIntOrNull(shussoTosu);
  if (parsed === null) return null;
  if (parsed <= SMALL_FIELD_MAX_SHUSSO_TOSU) return "f_le10";
  if (parsed <= MEDIUM_FIELD_MAX_SHUSSO_TOSU) return "f11_13";
  if (parsed <= LARGE_FIELD_MAX_SHUSSO_TOSU) return "f14_15";
  return "f16p";
};

const resolveSurface = (race: RaceDetail, category: string): string | null => {
  if (category !== "jra") return "dirt";
  const trackCode = trimmedOrNull(race.trackCode);
  if (trackCode === null) return null;
  if (trackCode.startsWith("1")) return "turf";
  if (trackCode.startsWith("2")) return "dirt";
  return "other";
};

const deriveSeasonFromMonth = (month: number): string => {
  if (SPRING_MONTHS.has(month)) return "spring";
  if (SUMMER_MONTHS.has(month)) return "summer";
  if (AUTUMN_MONTHS.has(month)) return "autumn";
  return "winter";
};

const resolveSeason = (kaisaiTsukihi: string): string | null => {
  const monthText = kaisaiTsukihi.trim().slice(0, MONTH_PREFIX_LENGTH);
  return MONTH_PATTERN.test(monthText)
    ? deriveSeasonFromMonth(Number.parseInt(monthText, 10))
    : null;
};

const resolveClass = (gradeCode: string | null): string | null => {
  if (gradeCode === null) return null;
  const trimmed = gradeCode.trim();
  return trimmed === "" ? "unknown" : trimmed;
};

const SPECIAL_DIMENSION_RESOLVERS = new Map<
  string,
  (race: RaceDetail, category: string) => string | null
>([
  ["venue", (race) => trimmedOrNull(race.keibajoCode)],
  ["surface", (race, category) => resolveSurface(race, category)],
  ["distance_band", (race) => resolveDistanceBand(race.kyori)],
  ["field_band", (race) => resolveFieldBand(race.shussoTosu)],
  ["season", (race) => resolveSeason(race.kaisaiTsukihi)],
  ["class", (race) => resolveClass(race.gradeCode)],
]);

// Raw (non-derived) dimensions: any rule dimension not covered above reads
// this race column directly, mirroring cell_router.py's resolve_dimension
// fallback (`entry.get(dimension)`). A dimension name used by a future
// cell_routing.json rule that has no accessor here resolves to null (the
// condition simply never matches, degrading to "no routing" rather than
// throwing) -- the parity test flags this loudly by failing on divergence
// before it can reach production silently.
const RAW_DIMENSION_ACCESSORS = new Map<string, (race: RaceDetail) => string | null>([
  ["grade_code", (race) => trimmedOrNull(race.gradeCode)],
  ["kaisai_tsukihi", (race) => race.kaisaiTsukihi.trim()],
  ["keibajo_code", (race) => trimmedOrNull(race.keibajoCode)],
  ["kyori", (race) => trimmedOrNull(race.kyori)],
  ["kyoso_joken_code", (race) => trimmedOrNull(race.kyosoJokenCode)],
  ["shusso_tosu", (race) => trimmedOrNull(race.shussoTosu)],
  ["track_code", (race) => trimmedOrNull(race.trackCode)],
]);

// Exported so finish-position-cell-routing.test.ts can exercise every
// dimension branch directly, independent of which dimensions today's real
// cell_routing.json rules happen to reference (the parity test still keeps
// the config mirror itself honest).
export const resolveDimension = (params: ResolveDimensionParams): string | null => {
  const special = SPECIAL_DIMENSION_RESOLVERS.get(params.dimension);
  if (special) return special(params.race, params.category);
  const raw = RAW_DIMENSION_ACCESSORS.get(params.dimension);
  return raw ? raw(params.race) : null;
};

const allConditionsMatch = (params: AllConditionsMatchParams): boolean =>
  params.conditions.every((condition) => {
    const value = resolveDimension({
      category: params.category,
      dimension: condition.dimension,
      race: params.race,
    });
    return value !== null && condition.values.includes(value);
  });

const findMatchingRule = (
  params: ResolveCellRoutingParams,
  rules: CellRoutingRule[],
): CellRoutingRule | undefined =>
  rules.find((rule) =>
    allConditionsMatch({
      category: params.category,
      conditions: rule.conditions,
      race: params.race,
    }),
  );

export interface ResolveCellRoutingForConfigParams extends ResolveCellRoutingParams {
  config: CellRoutingConfig;
}

/**
 * Config-parameterized core of resolveFinishPositionCellRoutingModelVersion,
 * exported so finish-position-cell-routing.test.ts can exercise the
 * missing-variant guard directly: with the real, hand-verified
 * FINISH_POSITION_CELL_ROUTING_CONFIG every rule's `variant` always names a
 * key present in that category's `variants`, so a synthetic config is the
 * only way to legitimately reach that guard (required by
 * noUncheckedIndexedAccess) instead of leaving it as untested dead code.
 */
export const resolveCellRoutingModelVersionForConfig = (
  params: ResolveCellRoutingForConfigParams,
): string | null => {
  const categoryConfig = params.config[params.category];
  if (categoryConfig === undefined) return null;
  const matchedRule = findMatchingRule(params, categoryConfig.rules);
  const variantName = matchedRule?.variant ?? categoryConfig.default_variant;
  if (variantName === categoryConfig.default_variant) return null;
  return categoryConfig.variants[variantName]?.model_version ?? null;
};

/**
 * Return the cell-routing variant's model_version for `race`, or null when
 * the category has no routing config or the race resolves to the default
 * variant (i.e. the plain category-default model already covers it, so no
 * display-priority override is needed).
 */
export const resolveFinishPositionCellRoutingModelVersion = (
  params: ResolveCellRoutingParams,
): string | null =>
  resolveCellRoutingModelVersionForConfig({
    category: params.category,
    config: FINISH_POSITION_CELL_ROUTING_CONFIG,
    race: params.race,
  });

/** Every distinct model_version referenced by any category's variants, deduped. */
export const getAllFinishPositionCellRoutingModelVersions = (): string[] => {
  const versions = Object.values(FINISH_POSITION_CELL_ROUTING_CONFIG).flatMap((categoryConfig) =>
    Object.values(categoryConfig.variants).map((variant) => variant.model_version),
  );
  return Array.from(new Set(versions));
};

export const FINISH_POSITION_CELL_ROUTING_CONFIG_FOR_TESTS: CellRoutingConfig =
  FINISH_POSITION_CELL_ROUTING_CONFIG;
