// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)
import type {
  BloodlineStatsRow,
  FrameStatsRow,
  HorseRaceResult,
  Runner,
  SimilarRaceStatsRow,
  WeightClassStatsRow,
} from "./race-types";

export const WIN_RATE_HEATMAP_CACHE_TTL_SECONDS = 36 * 60 * 60;
// Bumped v9->v10 because heatmap Catalog history now uses cell-matching class,
// age, condition-key, race-title, and ungraded-OP empty-grade filters.
export const WIN_RATE_HEATMAP_CACHE_NAMESPACE = "pc-keiba-viewer:win-rate-heatmap:v10";
export const WIN_RATE_HEATMAP_CACHE_URL_BASE =
  "https://pc-keiba-viewer.local/win-rate-heatmap-cache/";
const WIN_RATE_HEATMAP_CACHE_QUERY_DEFAULT = "default";

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
  horseResults: HorseRaceResult[];
  runners: Runner[];
  similarRows: SimilarRaceStatsRow[];
  type: "win-rate-heatmap";
  weightClassStats: WeightClassStatsRow[];
}

interface QueryEntry {
  name: string;
  value: string;
}

const CACHE_KEY_PART_WIDTH = 2;

const padCacheKeyPart = (value: string): string => value.padStart(CACHE_KEY_PART_WIDTH, "0");

const compareQueryEntries = (left: QueryEntry, right: QueryEntry): number => {
  if (left.name !== right.name) {
    return left.name.localeCompare(right.name);
  }
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

export const buildWinRateHeatmapCacheKey = (input: WinRateHeatmapCacheKeyInput): string =>
  [
    WIN_RATE_HEATMAP_CACHE_NAMESPACE,
    input.year,
    padCacheKeyPart(input.month),
    padCacheKeyPart(input.day),
    input.keibajoCode,
    padCacheKeyPart(input.raceNumber),
    input.query === "" ? WIN_RATE_HEATMAP_CACHE_QUERY_DEFAULT : input.query,
  ].join(":");

export const createWinRateHeatmapCacheRequest = (cacheKey: string): Request =>
  new Request(`${WIN_RATE_HEATMAP_CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

export const isWinRateHeatmapSectionPayload = (
  value: unknown,
): value is WinRateHeatmapSectionPayload => {
  if (!isRecord(value) || value.type !== "win-rate-heatmap") {
    return false;
  }
  return (
    Array.isArray(value.bloodlineRows) &&
    Array.isArray(value.carriedWeightClassStats) &&
    Array.isArray(value.frameStats) &&
    Array.isArray(value.horseResults) &&
    Array.isArray(value.runners) &&
    Array.isArray(value.similarRows) &&
    Array.isArray(value.weightClassStats)
  );
};
