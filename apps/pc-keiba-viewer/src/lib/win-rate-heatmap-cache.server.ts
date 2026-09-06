import "server-only";
import { safeGetCloudflareRuntime } from "./cloudflare-context.server";
import type {
  BloodlineStatsRow,
  FrameStatsRow,
  Runner,
  SimilarRaceStatsRow,
  WeightClassStatsRow,
} from "./race-types";
import { formatRunnerNumber } from "./runner-format";
import type { WinRateHeatmapHorseRateRow } from "./win-rate-heatmap";
import {
  WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS,
  WIN_RATE_HEATMAP_CACHE_TTL_SECONDS,
  buildWinRateHeatmapFragmentCacheKey,
  buildWinRateHeatmapRunnerSignature,
  createWinRateHeatmapCacheRequest,
  expandWinRateHeatmapCacheReadKeys,
  isWinRateHeatmapCacheFragment,
  isWinRateHeatmapCacheManifest,
  type WinRateHeatmapCacheFragment,
  type WinRateHeatmapCacheFragmentKind,
  type WinRateHeatmapCacheManifest,
  type WinRateHeatmapSectionPayload,
} from "./win-rate-heatmap-cache";

const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";

interface CompactRateRow {
  category: string;
  currentHorseNumbers: string;
  name: string;
  quinellaCount: number;
  quinellaRate: number;
  showCount: number;
  showRate: number;
  starts: number;
  winCount: number;
  winRate: number;
}

interface CompactFrameRow {
  count: number;
  frameNumber: string;
  quinellaCount: number;
  quinellaRate: number | null;
  showCount: number;
  showRate: number | null;
  winCount: number;
  winRate: number | null;
}

interface CompactWeightRow {
  key: string;
  quinellaCount: number;
  quinellaRate: number | null;
  showCount: number;
  showRate: number | null;
  starts: number;
  winCount: number;
  winRate: number | null;
}

type CompactHorseRateRow = WinRateHeatmapHorseRateRow;

declare global {
  interface CacheStorage {
    readonly default?: Cache;
  }
}

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isFiniteNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);

const isNonnegativeNumber = (value: unknown): value is number =>
  isFiniteNumber(value) && value >= 0;

const isRate = (value: unknown): value is number =>
  isFiniteNumber(value) && value >= 0 && value <= 100;

const isNullableRate = (value: unknown): value is number | null => value === null || isRate(value);

const isCompactRateRow = (value: unknown): value is CompactRateRow =>
  isRecord(value) &&
  typeof value.category === "string" &&
  typeof value.currentHorseNumbers === "string" &&
  typeof value.name === "string" &&
  isNonnegativeNumber(value.starts) &&
  isNonnegativeNumber(value.winCount) &&
  value.winCount <= value.starts &&
  isNonnegativeNumber(value.quinellaCount) &&
  value.quinellaCount <= value.starts &&
  isNonnegativeNumber(value.showCount) &&
  value.showCount <= value.starts &&
  isRate(value.winRate) &&
  isRate(value.quinellaRate) &&
  isRate(value.showRate);

const isCompactFrameRow = (value: unknown): value is CompactFrameRow =>
  isRecord(value) &&
  typeof value.frameNumber === "string" &&
  isNonnegativeNumber(value.count) &&
  isNonnegativeNumber(value.winCount) &&
  value.winCount <= value.count &&
  isNonnegativeNumber(value.quinellaCount) &&
  value.quinellaCount <= value.count &&
  isNonnegativeNumber(value.showCount) &&
  value.showCount <= value.count &&
  isNullableRate(value.winRate) &&
  isNullableRate(value.quinellaRate) &&
  isNullableRate(value.showRate);

const isCompactWeightRow = (value: unknown): value is CompactWeightRow =>
  isRecord(value) &&
  typeof value.key === "string" &&
  isNonnegativeNumber(value.starts) &&
  isNonnegativeNumber(value.winCount) &&
  value.winCount <= value.starts &&
  isNonnegativeNumber(value.quinellaCount) &&
  value.quinellaCount <= value.starts &&
  isNonnegativeNumber(value.showCount) &&
  value.showCount <= value.starts &&
  isNullableRate(value.winRate) &&
  isNullableRate(value.quinellaRate) &&
  isNullableRate(value.showRate);

const isCompactHorseRateRow = (value: unknown): value is CompactHorseRateRow =>
  isRecord(value) &&
  typeof value.horseNumber === "string" &&
  value.horseNumber.length > 0 &&
  isNonnegativeNumber(value.starts) &&
  isNonnegativeNumber(value.winCount) &&
  value.winCount <= value.starts &&
  isNonnegativeNumber(value.quinellaCount) &&
  value.quinellaCount <= value.starts &&
  isNonnegativeNumber(value.showCount) &&
  value.showCount <= value.starts;

const tryParseJson = (text: string): unknown => {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
};

const writeCacheApi = async (
  defaultCache: Cache | null,
  cacheRequest: Request,
  body: string,
): Promise<void> => {
  if (defaultCache === null) return;
  await defaultCache.put(
    cacheRequest,
    new Response(body, {
      headers: {
        "Cache-Control": `public, max-age=${WIN_RATE_HEATMAP_CACHE_TTL_SECONDS}`,
        "Content-Type": DEFAULT_CONTENT_TYPE,
      },
    }),
  );
};

interface HeatmapCacheStorage {
  defaultCache: Cache | null;
  runtime: Awaited<ReturnType<typeof safeGetCloudflareRuntime>>;
}

const readCachedJson = async <T>(
  cacheKey: string,
  parse: (value: unknown) => T | null,
  storage: HeatmapCacheStorage,
): Promise<T | null> => {
  const cacheRequest = createWinRateHeatmapCacheRequest(cacheKey);
  const { defaultCache } = storage;
  if (defaultCache !== null) {
    const cachedResponse = await defaultCache.match(cacheRequest);
    if (cachedResponse?.ok) {
      try {
        const parsed = parse(await cachedResponse.json());
        if (parsed !== null) return parsed;
      } catch {
        // Delete malformed Cache API values before trying the durable KV copy.
      }
      await defaultCache.delete(cacheRequest);
    }
  }

  const { ctx, env } = storage.runtime;
  const body = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (body === null || body === undefined) return null;
  const parsed = parse(tryParseJson(body));
  if (parsed === null) return null;
  const populate = writeCacheApi(defaultCache, cacheRequest, body);
  if (ctx === null) await populate;
  else ctx.waitUntil(populate);
  return parsed;
};

const parseManifest = (value: unknown): WinRateHeatmapCacheManifest | null =>
  isWinRateHeatmapCacheManifest(value) ? value : null;

const parseFragment = (
  value: unknown,
  generation: string,
  kind: WinRateHeatmapCacheFragmentKind,
): WinRateHeatmapCacheFragment | null => {
  if (
    !isWinRateHeatmapCacheFragment(value) ||
    value.generation !== generation ||
    value.kind !== kind
  ) {
    return null;
  }
  const rowIsValid =
    kind === "frame"
      ? isCompactFrameRow
      : kind === "weight" || kind === "carriedWeight"
        ? isCompactWeightRow
        : kind === "horse"
          ? isCompactHorseRateRow
          : (row: unknown): row is CompactRateRow => isCompactRateRow(row) && row.category === kind;
  return value.rows.every(rowIsValid) ? value : null;
};

const compactRateRows = (fragment: WinRateHeatmapCacheFragment): CompactRateRow[] =>
  fragment.rows.filter(isCompactRateRow);

const restoreFrameRows = (fragment: WinRateHeatmapCacheFragment): FrameStatsRow[] =>
  fragment.rows.filter(isCompactFrameRow).map((row) => ({
    averageFinish: null,
    averagePopularity: null,
    count: row.count,
    details: [],
    frameNumber: row.frameNumber,
    medianFinish: null,
    medianPopularity: null,
    quinellaCount: row.quinellaCount,
    quinellaRate: row.quinellaRate,
    runnerCount: null,
    score: 0,
    showCount: row.showCount,
    showRate: row.showRate,
    winCount: row.winCount,
    winRate: row.winRate,
  }));

const restoreWeightRows = (fragment: WinRateHeatmapCacheFragment): WeightClassStatsRow[] =>
  fragment.rows.filter(isCompactWeightRow);

const restoreHorseRateRows = (
  fragment: WinRateHeatmapCacheFragment,
): WinRateHeatmapHorseRateRow[] => fragment.rows.filter(isCompactHorseRateRow);

const restoreBloodlineRows = (
  fragment: WinRateHeatmapCacheFragment,
  category: BloodlineStatsRow["category"],
): BloodlineStatsRow[] =>
  compactRateRows(fragment).map((row) => ({
    category,
    currentHorseNumbers: row.currentHorseNumbers,
    details: [],
    horseCount: 0,
    name: row.name,
    quinellaCount: row.quinellaCount,
    quinellaRate: row.quinellaRate,
    showCount: row.showCount,
    showRate: row.showRate,
    starts: row.starts,
    winCount: row.winCount,
    winRate: row.winRate,
  }));

const restoreSimilarRows = (
  fragment: WinRateHeatmapCacheFragment,
  category: SimilarRaceStatsRow["category"],
): SimilarRaceStatsRow[] =>
  compactRateRows(fragment).map((row) => ({
    category,
    currentHorseNumbers: row.currentHorseNumbers,
    details: [],
    horseCount: 0,
    name: row.name,
    quinellaCount: row.quinellaCount,
    quinellaRate: row.quinellaRate,
    showCount: row.showCount,
    showRate: row.showRate,
    starts: row.starts,
    winCount: row.winCount,
    winRate: row.winRate,
  }));

const assemblePayload = (
  fragments: ReadonlyMap<WinRateHeatmapCacheFragmentKind, WinRateHeatmapCacheFragment>,
  runners: Runner[],
): WinRateHeatmapSectionPayload => {
  const similarKinds = ["jockeyFrame", "jockey", "trainer"] as const;
  const bloodlineKinds = [
    "sire",
    "damSire",
    "sireSire",
    "sireDamSire",
    "sireSireSire",
    "damSireSire",
    "damDamSire",
  ] as const;
  return {
    bloodlineRows: bloodlineKinds.flatMap((kind) =>
      restoreBloodlineRows(fragments.get(kind)!, kind),
    ),
    carriedWeightClassStats: restoreWeightRows(fragments.get("carriedWeight")!),
    frameStats: restoreFrameRows(fragments.get("frame")!),
    horseRateStats: restoreHorseRateRows(fragments.get("horse")!),
    horseResults: [],
    runners,
    similarRows: similarKinds.flatMap((kind) => restoreSimilarRows(fragments.get(kind)!, kind)),
    type: "win-rate-heatmap",
    weightClassStats: restoreWeightRows(fragments.get("weight")!),
  };
};

export const getCachedWinRateHeatmapPayload = async (
  cacheKey: string,
  currentRunners: Runner[],
): Promise<WinRateHeatmapSectionPayload | null> => {
  const runnerSignature = buildWinRateHeatmapRunnerSignature(currentRunners);
  if (runnerSignature === null) return null;
  const [currentKey] = expandWinRateHeatmapCacheReadKeys(cacheKey);
  const storage: HeatmapCacheStorage = {
    defaultCache: getDefaultCache(),
    runtime: await safeGetCloudflareRuntime(),
  };
  const manifest = await readCachedJson(currentKey, parseManifest, storage);
  if (manifest === null || manifest.runnerSignature !== runnerSignature) return null;
  const fragments = await Promise.all(
    manifest.fragmentKinds.map(async (kind) => {
      const fragmentKey = buildWinRateHeatmapFragmentCacheKey(
        currentKey,
        manifest.generation,
        kind,
      );
      return [
        kind,
        await readCachedJson(
          fragmentKey,
          (value) => parseFragment(value, manifest.generation, kind),
          storage,
        ),
      ] as const;
    }),
  );
  if (fragments.some(([, fragment]) => fragment === null)) return null;
  return assemblePayload(
    new Map(fragments.map(([kind, fragment]) => [kind, fragment!] as const)),
    currentRunners,
  );
};

const compactRateRow = (row: BloodlineStatsRow | SimilarRaceStatsRow): CompactRateRow => ({
  category: row.category,
  currentHorseNumbers: row.currentHorseNumbers,
  name: row.name,
  quinellaCount: row.quinellaCount,
  quinellaRate: row.quinellaRate,
  showCount: row.showCount,
  showRate: row.showRate,
  starts: row.starts,
  winCount: row.winCount,
  winRate: row.winRate,
});

const compactFrameRow = (row: FrameStatsRow): CompactFrameRow => ({
  count: row.count,
  frameNumber: row.frameNumber,
  quinellaCount: row.quinellaCount,
  quinellaRate: row.quinellaRate,
  showCount: row.showCount,
  showRate: row.showRate,
  winCount: row.winCount,
  winRate: row.winRate,
});

const compactWeightRow = (row: WeightClassStatsRow): CompactWeightRow => ({
  key: row.key,
  quinellaCount: row.quinellaCount,
  quinellaRate: row.quinellaRate,
  showCount: row.showCount,
  showRate: row.showRate,
  starts: row.starts,
  winCount: row.winCount,
  winRate: row.winRate,
});

const parseCachedFinishRank = (value: string | null): number | null => {
  const normalized = value?.trim() ?? "";
  if (normalized === "" || /^0+$/u.test(normalized)) return null;
  const rank = Number(normalized);
  return Number.isFinite(rank) && rank > 0 ? rank : null;
};

const buildHorseRateRows = (
  payload: WinRateHeatmapSectionPayload,
): WinRateHeatmapHorseRateRow[] => {
  if (payload.horseRateStats !== undefined) return [...payload.horseRateStats];
  const ranksByHorse = new Map<string, number[]>();
  payload.horseResults.forEach((result) => {
    const horseNumber = formatRunnerNumber(result.currentUmaban);
    const rank = parseCachedFinishRank(result.kakuteiChakujun);
    if (horseNumber === "-" || rank === null) return;
    ranksByHorse.set(horseNumber, [...(ranksByHorse.get(horseNumber) ?? []), rank]);
  });
  return [...ranksByHorse].map(([horseNumber, ranks]) => ({
    horseNumber,
    quinellaCount: ranks.filter((rank) => rank <= 2).length,
    showCount: ranks.filter((rank) => rank <= 3).length,
    starts: ranks.length,
    winCount: ranks.filter((rank) => rank === 1).length,
  }));
};

const buildFragments = (
  payload: WinRateHeatmapSectionPayload,
  generation: string,
): WinRateHeatmapCacheFragment[] => {
  const similarRows = new Map(
    (["jockeyFrame", "jockey", "trainer"] as const).map((kind) => [
      kind,
      payload.similarRows.filter((row) => row.category === kind).map(compactRateRow),
    ]),
  );
  const bloodlineRows = new Map(
    (
      [
        "sire",
        "damSire",
        "sireSire",
        "sireDamSire",
        "sireSireSire",
        "damSireSire",
        "damDamSire",
      ] as const
    ).map((kind) => [
      kind,
      payload.bloodlineRows.filter((row) => row.category === kind).map(compactRateRow),
    ]),
  );
  const rowsByKind = new Map<WinRateHeatmapCacheFragmentKind, unknown[]>([
    ["frame", payload.frameStats.map(compactFrameRow)],
    ["weight", payload.weightClassStats.map(compactWeightRow)],
    ["carriedWeight", payload.carriedWeightClassStats.map(compactWeightRow)],
    ["horse", buildHorseRateRows(payload)],
    ...similarRows,
    ...bloodlineRows,
  ]);
  return WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS.map((kind) => ({
    generation,
    kind,
    rows: rowsByKind.get(kind) ?? [],
    type: "win-rate-heatmap-fragment" as const,
  }));
};

export const putWinRateHeatmapCache = async ({
  cacheKey,
  payload,
}: {
  cacheKey: string;
  payload: WinRateHeatmapSectionPayload;
}): Promise<void> => {
  const runnerSignature = buildWinRateHeatmapRunnerSignature(payload.runners);
  if (runnerSignature === null) throw new Error("Heatmap runner signature is unavailable");
  const generation = crypto.randomUUID();
  const fragments = buildFragments(payload, generation);
  const manifest: WinRateHeatmapCacheManifest = {
    fragmentKinds: WIN_RATE_HEATMAP_CACHE_FRAGMENT_KINDS,
    generation,
    runnerSignature,
    type: "win-rate-heatmap-manifest",
  };
  const defaultCache = getDefaultCache();
  const { env } = await safeGetCloudflareRuntime();
  const kv = env?.DETAIL_SECTION_CACHE_KV;
  if (!kv) throw new Error("DETAIL_SECTION_CACHE_KV is unavailable");

  await Promise.all(
    fragments.map(async (fragment) => {
      const fragmentKey = buildWinRateHeatmapFragmentCacheKey(cacheKey, generation, fragment.kind);
      const body = JSON.stringify(fragment);
      await kv.put(fragmentKey, body, { expirationTtl: WIN_RATE_HEATMAP_CACHE_TTL_SECONDS });
      await writeCacheApi(defaultCache, createWinRateHeatmapCacheRequest(fragmentKey), body).catch(
        () => undefined,
      );
    }),
  );

  const manifestBody = JSON.stringify(manifest);
  await kv.put(cacheKey, manifestBody, { expirationTtl: WIN_RATE_HEATMAP_CACHE_TTL_SECONDS });
  await writeCacheApi(defaultCache, createWinRateHeatmapCacheRequest(cacheKey), manifestBody).catch(
    () => undefined,
  );
};
