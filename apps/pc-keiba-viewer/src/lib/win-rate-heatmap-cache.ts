// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)
import type {
  BloodlineStatsRow,
  FrameStatsRow,
  Runner,
  SimilarRaceStatsRow,
  WeightClassStatsRow,
} from "./race-types";
import type { WinRateHeatmapHorseRateRow, WinRateHeatmapHorseResult } from "./win-rate-heatmap";

export const WIN_RATE_HEATMAP_CACHE_TTL_SECONDS = 36 * 60 * 60;
// v18 stores one small manifest plus independently addressable display-column
// fragments. It also distinguishes an available numeric zero from a missing
// fragment and omits history details that the heatmap never renders.
export const WIN_RATE_HEATMAP_CACHE_NAMESPACE = "pc-keiba-viewer:win-rate-heatmap:v18";
export const WIN_RATE_HEATMAP_CACHE_FALLBACK_NAMESPACE = "pc-keiba-viewer:win-rate-heatmap:v17";
export const WIN_RATE_HEATMAP_CACHE_URL_BASE =
  "https://pc-keiba-viewer.local/win-rate-heatmap-cache/";
const WIN_RATE_HEATMAP_CACHE_QUERY_DEFAULT = "default";

export const WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS = [
  "frame",
  "weight",
  "carriedWeight",
  "horse",
  "jockeyFrame",
  "jockey",
  "trainer",
  "sire",
  "damSire",
  "sireSire",
  "sireDamSire",
  "sireSireSire",
  "damSireSire",
  "damDamSire",
] as const;

export type WinRateHeatmapCacheFragmentKind =
  (typeof WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS)[number];

export interface WinRateHeatmapCacheKeyInput {
  day: string;
  keibajoCode: string;
  month: string;
  query: string;
  raceNumber: string;
  year: string;
}

export interface WinRateHeatmapSectionPayload {
  bloodlineRows: BloodlineStatsRow[];
  carriedWeightClassStats: WeightClassStatsRow[];
  frameStats: FrameStatsRow[];
  horseRateStats?: WinRateHeatmapHorseRateRow[];
  horseResults: WinRateHeatmapHorseResult[];
  runners: Runner[];
  similarRows: SimilarRaceStatsRow[];
  type: "win-rate-heatmap";
  weightClassStats: WeightClassStatsRow[];
}

export interface WinRateHeatmapRunnerIdentity {
  bamei?: unknown;
  bataiju?: unknown;
  chokyoshimeiRyakusho?: unknown;
  damSireName?: unknown;
  futanJuryo?: unknown;
  kettoTorokuBango?: unknown;
  kishumeiRyakusho?: unknown;
  sireName?: unknown;
  sireSireName?: unknown;
  sourceHorseId?: unknown;
  umaban?: unknown;
  wakuban?: unknown;
}

export interface WinRateHeatmapCacheManifest {
  fragmentKinds: readonly WinRateHeatmapCacheFragmentKind[];
  generation: string;
  runnerSignature: string;
  type: "win-rate-heatmap-manifest";
}

export interface WinRateHeatmapCacheFragment {
  generation: string;
  kind: WinRateHeatmapCacheFragmentKind;
  rows: unknown[];
  type: "win-rate-heatmap-fragment";
}

interface QueryEntry {
  name: string;
  value: string;
}

const CACHE_KEY_PART_WIDTH = 2;
const CACHE_GENERATION_PATTERN = /^[0-9a-f-]{16,64}$/u;

const padCacheKeyPart = (value: string): string => value.padStart(CACHE_KEY_PART_WIDTH, "0");

const compareQueryEntries = (left: QueryEntry, right: QueryEntry): number => {
  if (left.name !== right.name) return left.name.localeCompare(right.name);
  return left.value.localeCompare(right.value);
};

export const serializeWinRateHeatmapCacheQuery = (searchParams: URLSearchParams): string => {
  const serialized = new URLSearchParams(
    [...searchParams.entries()]
      .map(([name, value]) => ({ name, value }))
      .toSorted(compareQueryEntries)
      .map((entry) => [entry.name, entry.value]),
  ).toString();
  return serialized === "" ? WIN_RATE_HEATMAP_CACHE_QUERY_DEFAULT : serialized;
};

const joinWinRateHeatmapCacheKey = (
  input: WinRateHeatmapCacheKeyInput,
  namespace: string,
): string =>
  [
    namespace,
    input.year,
    padCacheKeyPart(input.month),
    padCacheKeyPart(input.day),
    input.keibajoCode,
    padCacheKeyPart(input.raceNumber),
    input.query === "" ? WIN_RATE_HEATMAP_CACHE_QUERY_DEFAULT : input.query,
  ].join(":");

export const buildWinRateHeatmapCacheKey = (input: WinRateHeatmapCacheKeyInput): string =>
  joinWinRateHeatmapCacheKey(input, WIN_RATE_HEATMAP_CACHE_NAMESPACE);

export const buildWinRateHeatmapCacheFallbackKeys = (
  input: WinRateHeatmapCacheKeyInput,
): string[] => [joinWinRateHeatmapCacheKey(input, WIN_RATE_HEATMAP_CACHE_FALLBACK_NAMESPACE)];

export const expandWinRateHeatmapCacheReadKeys = (cacheKey: string): [string] => [cacheKey];

export const buildWinRateHeatmapFragmentCacheKey = (
  cacheKey: string,
  generation: string,
  kind: WinRateHeatmapCacheFragmentKind,
): string => `${cacheKey}:fragment:${generation}:${kind}`;

export const createWinRateHeatmapCacheRequest = (cacheKey: string): Request =>
  new Request(`${WIN_RATE_HEATMAP_CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isFragmentKind = (value: unknown): value is WinRateHeatmapCacheFragmentKind =>
  typeof value === "string" && WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS.some((kind) => kind === value);

const isGeneration = (value: unknown): value is string =>
  typeof value === "string" && CACHE_GENERATION_PATTERN.test(value);

const normalizeRunnerIdentityPart = (value: unknown): string | null => {
  if (typeof value !== "string" && typeof value !== "number") return null;
  const normalized = String(value).trim();
  return normalized.length === 0 ? null : normalized;
};

export const buildWinRateHeatmapRunnerSignature = (
  runners: readonly WinRateHeatmapRunnerIdentity[],
): string | null => {
  const identities = runners.map((runner) => {
    const horseNumber = normalizeRunnerIdentityPart(runner.umaban);
    const horseIdentity =
      normalizeRunnerIdentityPart(runner.kettoTorokuBango) ??
      normalizeRunnerIdentityPart(runner.sourceHorseId) ??
      normalizeRunnerIdentityPart(runner.bamei);
    return horseNumber === null || horseIdentity === null
      ? null
      : JSON.stringify([
          horseNumber.padStart(2, "0"),
          horseIdentity,
          normalizeRunnerIdentityPart(runner.wakuban) ?? "",
          normalizeRunnerIdentityPart(runner.kishumeiRyakusho) ?? "",
          normalizeRunnerIdentityPart(runner.chokyoshimeiRyakusho) ?? "",
          normalizeRunnerIdentityPart(runner.futanJuryo) ?? "",
          normalizeRunnerIdentityPart(runner.bataiju) ?? "",
          normalizeRunnerIdentityPart(runner.sireName) ?? "",
          normalizeRunnerIdentityPart(runner.sireSireName) ?? "",
          normalizeRunnerIdentityPart(runner.damSireName) ?? "",
        ]);
  });
  if (!identities.every((identity): identity is string => identity !== null)) return null;
  return identities.length === 0
    ? "[]"
    : identities.toSorted((left, right) => left.localeCompare(right)).join("|");
};

export const isWinRateHeatmapCacheManifest = (
  value: unknown,
): value is WinRateHeatmapCacheManifest => {
  if (
    !isRecord(value) ||
    value.type !== "win-rate-heatmap-manifest" ||
    !isGeneration(value.generation) ||
    typeof value.runnerSignature !== "string" ||
    value.runnerSignature.length === 0 ||
    !Array.isArray(value.fragmentKinds) ||
    value.fragmentKinds.length !== WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS.length
  ) {
    return false;
  }
  const fragmentKinds: unknown[] = value.fragmentKinds;
  return WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS.every(
    (kind) => fragmentKinds.filter((candidate) => candidate === kind).length === 1,
  );
};

export const isWinRateHeatmapCacheFragment = (
  value: unknown,
): value is WinRateHeatmapCacheFragment =>
  isRecord(value) &&
  value.type === "win-rate-heatmap-fragment" &&
  isGeneration(value.generation) &&
  isFragmentKind(value.kind) &&
  Array.isArray(value.rows);

export const isWinRateHeatmapSectionPayload = (
  value: unknown,
): value is WinRateHeatmapSectionPayload => {
  if (!isRecord(value) || value.type !== "win-rate-heatmap") return false;
  return (
    Array.isArray(value.bloodlineRows) &&
    Array.isArray(value.carriedWeightClassStats) &&
    Array.isArray(value.frameStats) &&
    (value.horseRateStats === undefined || Array.isArray(value.horseRateStats)) &&
    Array.isArray(value.horseResults) &&
    Array.isArray(value.runners) &&
    Array.isArray(value.similarRows) &&
    Array.isArray(value.weightClassStats)
  );
};
