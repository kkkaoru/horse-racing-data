import { isArcQualifiedStats, type ArcQualifiedStats } from "./arc-qualified-stats";
import type { LiveHorseWeight } from "./horse-weight-class";
// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)
import type {
  BloodlineStatsRow,
  FrameStatsRow,
  Runner,
  SimilarRaceStatsRow,
  WeightClassStatsRow,
} from "./race-types";
import { isOverseasKeibajoCode } from "./runner-format";
import { ARC_HEATMAP_COHORT, type ArcHeatmapCohort } from "./win-rate-heatmap";
import type {
  WinRateHeatmapHorseRateRow,
  WinRateHeatmapHorseResult,
  WinRateHeatmapPartnershipRow,
} from "./win-rate-heatmap";
import { isHeatmapPresentation, type HeatmapPresentation } from "./win-rate-heatmap-presentation";

// An early tomorrow warm must remain valid through the end of that race day in JST.
export const WIN_RATE_HEATMAP_CACHE_TTL_SECONDS = 60 * 60 * 60;
// v24 invalidates presentations with a falsely bounded upper carried-weight class.
// v23 invalidates pre-ID bloodlines and pre-corrected same-venue conditions, even at unchanged profile snapshots.
// v22 also invalidates owner-triple presentations limited to one data source.
// It retains the compact manifest and independently addressable column fragments.
export const WIN_RATE_HEATMAP_CACHE_NAMESPACE = "pc-keiba-viewer:win-rate-heatmap:v24";
export const WIN_RATE_HEATMAP_CACHE_FALLBACK_NAMESPACE = "pc-keiba-viewer:win-rate-heatmap:v19";
export const WIN_RATE_HEATMAP_CACHE_URL_BASE =
  "https://pc-keiba-viewer.local/win-rate-heatmap-cache/";
const WIN_RATE_HEATMAP_CACHE_QUERY_DEFAULT = "default";

export const WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS = [
  "frame",
  "weight",
  "weightChange",
  "carriedWeight",
  "horse",
  "horseJockey",
  "jockeyVenue",
  "jockeyTrainerVenue",
  "jockeyFrame",
  "jockey",
  "trainer",
  "ownerVenue",
  "jockeyTrainerOwner",
  "owner",
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
  arcQualifiedStats?: ArcQualifiedStats;
  arcCohort?: ArcHeatmapCohort;
  mappingRevision?: string;
  producerDataRevision?: 1;
  liveHorseWeights?: LiveHorseWeight[];
  presentation?: HeatmapPresentation;
  partnershipRows?: WinRateHeatmapPartnershipRow[];
  bloodlineRows: BloodlineStatsRow[];
  carriedWeightClassStats: WeightClassStatsRow[];
  frameStats: FrameStatsRow[];
  horseRateStats?: WinRateHeatmapHorseRateRow[];
  horseResults: WinRateHeatmapHorseResult[];
  runners: Runner[];
  similarRows: SimilarRaceStatsRow[];
  type: "win-rate-heatmap";
  weightChangeClassStats?: WeightClassStatsRow[];
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
  identitySource?: unknown;
  sourceHorseProvider?: unknown;
  ownerNameFull?: unknown;
  banushimei?: unknown;
  sourceOwnerId?: unknown;
  sourceHorseId?: unknown;
  umaban?: unknown;
  wakuban?: unknown;
}

export interface WinRateHeatmapCacheManifest {
  arcQualifiedStats?: ArcQualifiedStats;
  arcCohort?: ArcHeatmapCohort;
  mappingRevision?: string;
  producerDataRevision?: 1;
  hasPresentation?: boolean;
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
      // The route resolves source from race identity; MCP's explicit source is
      // not a statistics option and must share the UI's warmed cache.
      .filter(([name]) => name !== "source")
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
    isOverseasKeibajoCode(input.keibajoCode)
      ? `${namespace}:overseas-readers-v3-arc-qualified-v2`
      : namespace,
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
): string[] =>
  isOverseasKeibajoCode(input.keibajoCode)
    ? []
    : [joinWinRateHeatmapCacheKey(input, WIN_RATE_HEATMAP_CACHE_FALLBACK_NAMESPACE)];

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

const normalizeNonzeroIdentity = (value: unknown): string | null => {
  const normalized = normalizeRunnerIdentityPart(value);
  return normalized === null || /^0+$/u.test(normalized) ? null : normalized;
};

export const buildWinRateHeatmapRunnerSignature = (
  runners: readonly WinRateHeatmapRunnerIdentity[],
): string | null => {
  const identities = runners.map((runner) => {
    const horseNumber = normalizeRunnerIdentityPart(runner.umaban);
    const registration = normalizeNonzeroIdentity(runner.kettoTorokuBango);
    const source = normalizeRunnerIdentityPart(runner.sourceHorseProvider ?? runner.identitySource);
    const sourceHorseId = normalizeNonzeroIdentity(runner.sourceHorseId);
    const hasSource = source !== null || runner.sourceHorseId != null;
    if (hasSource && (source === null || sourceHorseId === null)) return null;
    const horseIdentity = hasSource
      ? JSON.stringify(["source", source, sourceHorseId, "registration", registration])
      : (registration ?? normalizeRunnerIdentityPart(runner.bamei));
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
          ...(hasSource
            ? [
                "arc-qualified-v2",
                normalizeRunnerIdentityPart(runner.ownerNameFull) ?? "",
                normalizeRunnerIdentityPart(runner.banushimei) ?? "",
                normalizeNonzeroIdentity(runner.sourceOwnerId) ?? "",
              ]
            : []),
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
    (value.arcCohort !== undefined &&
      (value.arcCohort !== ARC_HEATMAP_COHORT || !isArcQualifiedStats(value.arcQualifiedStats))) ||
    (value.producerDataRevision !== undefined && value.producerDataRevision !== 1) ||
    (value.hasPresentation !== undefined && typeof value.hasPresentation !== "boolean") ||
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
    (value.arcCohort === undefined ||
      (value.arcCohort === ARC_HEATMAP_COHORT && isArcQualifiedStats(value.arcQualifiedStats))) &&
    (value.producerDataRevision === undefined || value.producerDataRevision === 1) &&
    (value.presentation === undefined || isHeatmapPresentation(value.presentation)) &&
    (value.partnershipRows === undefined || Array.isArray(value.partnershipRows)) &&
    Array.isArray(value.bloodlineRows) &&
    Array.isArray(value.carriedWeightClassStats) &&
    Array.isArray(value.frameStats) &&
    (value.horseRateStats === undefined || Array.isArray(value.horseRateStats)) &&
    Array.isArray(value.horseResults) &&
    Array.isArray(value.runners) &&
    Array.isArray(value.similarRows) &&
    (value.weightChangeClassStats === undefined || Array.isArray(value.weightChangeClassStats)) &&
    Array.isArray(value.weightClassStats)
  );
};
