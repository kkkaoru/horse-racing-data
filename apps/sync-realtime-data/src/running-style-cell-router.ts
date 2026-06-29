// Run with bun. Pure cell-level model routing for per-race running-style
// inference. The default route preserves the historical source-level model key;
// callers can pass a data-driven config to route specific race cells to a
// different flatbin model.

import {
  buildRunningStyleRaceKey,
  normalizeKeibajoCode,
  normalizeRaceBango,
  type RunningStyleRaceParams,
  type RunningStyleSource,
} from "./running-style-features";
import { buildRunningStyleFlatModelKey } from "./running-style-model-binary";

export type RunningStyleCellCategory = "ban-ei" | "jra" | "nar";

export interface RunningStyleCellRouteInput extends RunningStyleRaceParams {
  category?: string | null;
  gradeCode?: string | null;
  kyori?: number | null;
  kyosoJokenCode?: string | null;
  narSubClass?: string | null;
  shussoTosu?: number | null;
  trackCode?: string | null;
}

export interface RunningStyleCell {
  category: RunningStyleCellCategory;
  class: string;
  distanceBand: string | null;
  gradeCode: string | null;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  kyori: number | null;
  kyosoJokenCode: string | null;
  narSubClass: string | null;
  raceBango: string;
  raceKey: string;
  racetrack: string;
  season: string | null;
  shussoTosu: number | null;
  source: RunningStyleSource;
  subgroup: string | null;
  surface: string | null;
  trackCode: string | null;
  venue: string;
}

export interface RunningStyleCellCondition {
  dimension: string;
  values: ReadonlyArray<string>;
}

export interface RunningStyleCellRouteRule {
  conditions: ReadonlyArray<RunningStyleCellCondition>;
  variantId: string;
}

export interface RunningStyleCellVariantSpec {
  modelKey: string;
}

export interface RunningStyleCategoryRouting {
  defaultVariantId: string;
  rules: ReadonlyArray<RunningStyleCellRouteRule>;
  variants: Readonly<Record<string, RunningStyleCellVariantSpec>>;
}

export type RunningStyleCellRoutingConfig = Partial<
  Record<RunningStyleCellCategory, RunningStyleCategoryRouting>
>;

export interface RunningStyleCellRoute {
  cell: RunningStyleCell;
  modelKey: string;
  variantId: string;
}

export const DEFAULT_RUNNING_STYLE_VARIANT_ID = "latest";
export const BAN_EI_RUNNING_STYLE_KEIBAJO_CODES = ["65", "83"] as const;
const BAN_EI_KEIBAJO_CODE_SET: ReadonlySet<string> = new Set(BAN_EI_RUNNING_STYLE_KEIBAJO_CODES);

const DISTANCE_SPRINT_MAX = 1200;
const DISTANCE_MILE_MAX = 1600;
const DISTANCE_INTERMEDIATE_MAX = 2000;
const DISTANCE_LONG_MAX = 2400;

const normalizeOptionalString = (value: string | null | undefined): string | null => {
  if (value === null || value === undefined) return null;
  const text = value.trim();
  if (text.length === 0) return null;
  return text;
};

const normalizeOptionalNumber = (value: number | null | undefined): number | null => {
  if (typeof value !== "number") return null;
  if (!Number.isFinite(value)) return null;
  return value;
};

export const isBanEiRunningStyleKeibajoCode = (keibajoCode: string): boolean =>
  BAN_EI_KEIBAJO_CODE_SET.has(normalizeKeibajoCode(keibajoCode));

export const deriveRunningStyleCategory = (
  input: Pick<RunningStyleCellRouteInput, "keibajoCode" | "source">,
): RunningStyleCellCategory => {
  if (input.source === "jra") return "jra";
  if (isBanEiRunningStyleKeibajoCode(input.keibajoCode)) return "ban-ei";
  return "nar";
};

export const deriveRunningStyleSurface = (
  trackCode: string | null,
  category: RunningStyleCellCategory,
): string | null => {
  if (category !== "jra") return "dirt";
  if (trackCode === null) return null;
  if (trackCode.startsWith("1")) return "turf";
  if (trackCode.startsWith("2")) return "dirt";
  return "other";
};

export const deriveRunningStyleDistanceBand = (kyori: number | null): string | null => {
  if (kyori === null) return null;
  if (kyori < DISTANCE_SPRINT_MAX) return "sprint";
  if (kyori < DISTANCE_MILE_MAX) return "mile";
  if (kyori < DISTANCE_INTERMEDIATE_MAX) return "intermediate";
  if (kyori < DISTANCE_LONG_MAX) return "long";
  return "extended";
};

export const deriveRunningStyleSeason = (kaisaiTsukihi: string): string | null => {
  const monthText = kaisaiTsukihi.trim().slice(0, 2);
  if (!/^\d{2}$/u.test(monthText)) return null;
  const month = Number(monthText);
  if (month >= 3 && month <= 5) return "spring";
  if (month >= 6 && month <= 8) return "summer";
  if (month >= 9 && month <= 11) return "autumn";
  return "winter";
};

const deriveRunningStyleClass = (gradeCode: string | null): string => {
  if (gradeCode === null) return "unknown";
  return gradeCode;
};

const deriveRunningStyleSubgroup = (
  input: Pick<RunningStyleCellRouteInput, "kyosoJokenCode" | "narSubClass">,
  category: RunningStyleCellCategory,
): string | null => {
  if (category === "nar") return normalizeOptionalString(input.narSubClass);
  return normalizeOptionalString(input.kyosoJokenCode);
};

export const deriveRunningStyleCell = (input: RunningStyleCellRouteInput): RunningStyleCell => {
  const keibajoCode = normalizeKeibajoCode(input.keibajoCode);
  const raceBango = normalizeRaceBango(input.raceBango);
  const params = {
    kaisaiNen: input.kaisaiNen,
    kaisaiTsukihi: input.kaisaiTsukihi,
    keibajoCode,
    raceBango,
    source: input.source,
  } satisfies RunningStyleRaceParams;
  const category = deriveRunningStyleCategory(params);
  const kyori = normalizeOptionalNumber(input.kyori);
  const trackCode = normalizeOptionalString(input.trackCode);
  const gradeCode = normalizeOptionalString(input.gradeCode);
  const kyosoJokenCode = normalizeOptionalString(input.kyosoJokenCode);
  const narSubClass = normalizeOptionalString(input.narSubClass);
  return {
    category,
    class: deriveRunningStyleClass(gradeCode),
    distanceBand: deriveRunningStyleDistanceBand(kyori),
    gradeCode,
    kaisaiNen: input.kaisaiNen,
    kaisaiTsukihi: input.kaisaiTsukihi,
    keibajoCode,
    kyori,
    kyosoJokenCode,
    narSubClass,
    raceBango,
    raceKey: buildRunningStyleRaceKey(params),
    racetrack: keibajoCode,
    season: deriveRunningStyleSeason(input.kaisaiTsukihi),
    shussoTosu: normalizeOptionalNumber(input.shussoTosu),
    source: input.source,
    subgroup: deriveRunningStyleSubgroup({ kyosoJokenCode, narSubClass }, category),
    surface: deriveRunningStyleSurface(trackCode, category),
    trackCode,
    venue: keibajoCode,
  };
};

export const resolveRunningStyleCellDimension = (
  cell: RunningStyleCell,
  dimension: string,
): string | null => {
  if (dimension === "category") return cell.category;
  if (dimension === "venue") return cell.venue;
  if (dimension === "racetrack") return cell.racetrack;
  if (dimension === "keibajo_code") return cell.keibajoCode;
  if (dimension === "surface") return cell.surface;
  if (dimension === "distance_band") return cell.distanceBand;
  if (dimension === "season") return cell.season;
  if (dimension === "class") return cell.class;
  if (dimension === "grade_code") return cell.gradeCode;
  if (dimension === "subgroup") return cell.subgroup;
  if (dimension === "kyoso_joken_code") return cell.kyosoJokenCode;
  if (dimension === "nar_subclass") return cell.narSubClass;
  if (dimension === "shusso_tosu") {
    if (cell.shussoTosu === null) return null;
    return String(cell.shussoTosu);
  }
  return null;
};

export const runningStyleCellConditionMatches = (
  cell: RunningStyleCell,
  condition: RunningStyleCellCondition,
): boolean => {
  const value = resolveRunningStyleCellDimension(cell, condition.dimension);
  if (value === null) return false;
  return condition.values.includes(value);
};

export const runningStyleCellRuleMatches = (
  cell: RunningStyleCell,
  rule: RunningStyleCellRouteRule,
): boolean =>
  rule.conditions.every((condition) => runningStyleCellConditionMatches(cell, condition));

const defaultRunningStyleRoute = (
  input: RunningStyleCellRouteInput,
  cell: RunningStyleCell,
): RunningStyleCellRoute => ({
  cell,
  modelKey: buildRunningStyleFlatModelKey(input.source),
  variantId: DEFAULT_RUNNING_STYLE_VARIANT_ID,
});

const selectVariantId = (cell: RunningStyleCell, routing: RunningStyleCategoryRouting): string => {
  const matched = routing.rules.find((rule) => runningStyleCellRuleMatches(cell, rule));
  if (matched === undefined) return routing.defaultVariantId;
  return matched.variantId;
};

export const resolveRunningStyleCellRoute = (
  input: RunningStyleCellRouteInput,
  config: RunningStyleCellRoutingConfig = {},
): RunningStyleCellRoute => {
  const cell = deriveRunningStyleCell(input);
  const routing = config[cell.category];
  if (routing === undefined) return defaultRunningStyleRoute(input, cell);
  const variantId = selectVariantId(cell, routing);
  const variant = routing.variants[variantId];
  if (variant === undefined) {
    throw new Error(`running-style cell route variant is not configured: ${variantId}`);
  }
  return { cell, modelKey: variant.modelKey, variantId };
};
