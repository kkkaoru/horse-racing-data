// Run with bun. Reads aggregated trend rows from D1.
// * `getRaceTrendD1StarterRows`  -> snapshot tables (race_result_snapshots /
//   race_entry_snapshots / horse_weight_snapshots / realtime_race_sources):
//   the realtime-derived path that catches today's finishes before cron.
// * `getRaceTrendDailyStarterRows` -> new features worker `/api/features/race-trend`
//   (backed by R2 Parquet). Phase E cutover replaced the legacy direct read
//   of REALTIME_DB.daily_race_entries (Phase 0 rule 3) with a service binding
//   to `sync-realtime-data-features`. While the worker endpoint still returns
//   the stub aggregate, the helper degrades to an empty starter-row list.
// * `getRaceTrendRunningStylesFromD1` -> REALTIME_FEATURES_DB.race_running_styles
//   (new D1, Phase E). Falls back to REALTIME_DB.race_running_styles only when
//   the new binding is missing (preview / pre-cutover deploys).
import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import type { RaceSource } from "../lib/codes";
import type {
  RaceTrendRunningStyle,
  RaceTrendRunningStyleCache,
  RaceTrendStarterRow,
} from "../lib/race-types";

// 200 race keys per IN clause keeps the prepared statement well under
// SQLite's parameter limit while shrinking the number of parallel D1
// round trips by 4x compared with the previous 50-batch tuning.
const RUNNING_STYLE_BATCH_SIZE = 200;
// Run at most 3 chunk queries in flight at once. Higher values used to
// blow through the viewer worker's subrequest concurrency budget when
// trend payloads referenced 1500+ historicalRaceKeys, surfacing as
// 30-second HTTP timeouts.
const RUNNING_STYLE_CHUNK_CONCURRENCY = 3;

interface RawRunningStyleD1Row {
  predicted_label: string;
  horse_number: number;
  race_key: string;
}

const isRunningStyle = (value: unknown): value is RaceTrendRunningStyle =>
  value === "nige" || value === "senkou" || value === "sashi" || value === "oikomi";

const isRawRunningStyleD1Row = (value: unknown): value is RawRunningStyleD1Row => {
  if (typeof value !== "object" || value === null) return false;
  const row = value as Record<string, unknown>;
  return (
    typeof row.race_key === "string" &&
    typeof row.horse_number === "number" &&
    isRunningStyle(row.predicted_label)
  );
};

const chunkArray = <T>(items: ReadonlyArray<T>, size: number): T[][] => {
  if (items.length === 0 || size <= 0) return [];
  const chunkCount = Math.ceil(items.length / size);
  return Array.from({ length: chunkCount }, (_, index) =>
    items.slice(index * size, (index + 1) * size),
  );
};

const toRunningStyleCache = (raw: RawRunningStyleD1Row): RaceTrendRunningStyleCache => ({
  horseNumber: String(raw.horse_number),
  predictedLabel: raw.predicted_label as RaceTrendRunningStyle,
  raceKey: raw.race_key,
});

const buildRunningStyleSelectSql = (placeholders: string): string =>
  `select race_key, horse_number, predicted_label from race_running_styles where race_key in (${placeholders})`;

const RUNNING_STYLES_KV_PREFIX = "race-trend-running-styles:v1";

const buildRunningStylesCacheKey = (sortedKeys: ReadonlyArray<string>): string => {
  // Embed the count so two race lookups that happen to share a prefix in
  // the future can't collide on the key.
  return `${RUNNING_STYLES_KV_PREFIX}:${sortedKeys.length}:${sortedKeys.join(",")}`;
};

const readRunningStylesFromKv = async (
  env: CloudflareEnv | null,
  cacheKey: string,
): Promise<RaceTrendRunningStyleCache[] | null> => {
  const kv = env?.DETAIL_SECTION_CACHE_KV;
  if (!kv) return null;
  const body = await kv.get(cacheKey);
  if (!body) return null;
  try {
    return JSON.parse(body) as RaceTrendRunningStyleCache[];
  } catch {
    return null;
  }
};

const writeRunningStylesToKv = async (
  env: CloudflareEnv | null,
  cacheKey: string,
  rows: RaceTrendRunningStyleCache[],
): Promise<void> => {
  const kv = env?.DETAIL_SECTION_CACHE_KV;
  if (!kv) return;
  await kv.put(cacheKey, JSON.stringify(rows), { expirationTtl: KV_TTL_SECONDS });
};

// Prefer REALTIME_FEATURES_DB (new D1, post Phase E). Fall back to REALTIME_DB
// only when the new binding is missing — eg preview deploys that have not yet
// picked up the features worker binding. The new D1 has the same
// `race_running_styles` schema as the legacy D1 because the features worker
// migration was a clean schema copy.
const pickRunningStylesDb = (env: CloudflareEnv | null): PcKeibaD1Database | null =>
  env?.REALTIME_FEATURES_DB ?? env?.REALTIME_DB ?? null;

export const getRaceTrendRunningStylesFromD1 = async (
  raceKeys: ReadonlyArray<string>,
): Promise<RaceTrendRunningStyleCache[]> => {
  const uniqueKeys = Array.from(new Set(raceKeys.filter((key) => key.length > 0))).toSorted();
  if (uniqueKeys.length === 0) return [];
  const { env } = await getCloudflareContext({ async: true });
  const db = pickRunningStylesDb(env ?? null);
  if (!db) return [];
  const cacheKey = buildRunningStylesCacheKey(uniqueKeys);
  const cached = await readRunningStylesFromKv(env, cacheKey);
  if (cached !== null) return cached;
  const queryChunk = async (
    chunk: ReadonlyArray<string>,
  ): Promise<RaceTrendRunningStyleCache[]> => {
    const placeholders = chunk.map(() => "?").join(",");
    const result = await db
      .prepare(buildRunningStyleSelectSql(placeholders))
      .bind(...chunk)
      .all();
    return result.results.filter(isRawRunningStyleD1Row).map(toRunningStyleCache);
  };
  try {
    const chunks = chunkArray(uniqueKeys, RUNNING_STYLE_BATCH_SIZE);
    const results: RaceTrendRunningStyleCache[] = [];
    const indexState = { value: 0 };
    const runWorker = async (): Promise<void> => {
      const next = indexState.value;
      indexState.value = next + 1;
      const chunk = chunks[next];
      if (!chunk) return;
      const chunkResult = await queryChunk(chunk);
      results.push(...chunkResult);
      await runWorker();
    };
    const workerCount = Math.min(RUNNING_STYLE_CHUNK_CONCURRENCY, chunks.length);
    await Promise.all(Array.from({ length: workerCount }, () => runWorker()));
    // Persist non-empty results so a saturation event on the next viewer
    // request doesn't surface as historicalRunningStyles: 0 — the cache
    // write path elsewhere only stores the merged trend payload when
    // both starters and running-style history are populated, but we still
    // want the per-call running-style fetch to short-circuit while data
    // is fresh.
    if (results.length > 0) {
      await writeRunningStylesToKv(env, cacheKey, results);
    }
    return results;
  } catch (error) {
    console.error("D1 race_running_styles query failed", error);
    return [];
  }
};

interface RaceTrendD1RowsParams {
  endYmd: string;
  source: RaceSource;
  startYmd: string;
}

interface RawD1Row {
  source: string;
  raceKey: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  raceName: string | null;
  hassoJikoku: string | null;
  umaban: string | null;
  bamei: string | null;
  jockeyName: string | null;
  finishPosition: number;
  sohaTime: string | null;
  bataijuInt: number | null;
  zogenFugo: string | null;
  zogenSaInt: number | null;
}

interface RawTanshoOddsRow {
  race_key: string;
  combination: string;
  odds: number | null;
  rank: number | null;
}

interface TanshoOddsEntry {
  odds: number | null;
  rank: number | null;
}

type TanshoOddsMap = Map<string, Map<string, TanshoOddsEntry>>;

const CACHE_URL_BASE = "https://pc-keiba-viewer.local/d1-trend-cache/";
const DAILY_CACHE_URL_BASE = "https://pc-keiba-viewer.local/d1-trend-daily-cache/";
// Edge Cache API is colo-local so a 60s TTL is enough for the common
// "user reloads the same race" path.
const CACHE_TTL_SECONDS = 60;
// KV is global and propagates across colos, so a populated D1 daily /
// snapshot result should outlive the 5min legacy value to keep every
// race on the day sharing one upstream D1 round trip. 30min still
// expires well before the next day's NAR Neon sync, and stale data
// only delays a freshly-finished race result by minutes — which the
// race-finish cache-bust hook already invalidates explicitly.
const KV_TTL_SECONDS = 30 * 60;

// Phase E: the daily starter rows now come from the features worker, which
// proxies the R2 Parquet aggregate. Hard-code the production hostname so
// `env.REALTIME_FEATURES.fetch` resolves to the bound service worker
// regardless of which Cloudflare colo handles the request.
const FEATURES_WORKER_BASE = "https://sync-realtime-data-features.kkk4oru.com";
const FEATURES_RACE_TREND_PATH = "/api/features/race-trend";

const isRaceSource = (value: unknown): value is RaceSource => value === "jra" || value === "nar";

const isRawD1Row = (value: unknown): value is RawD1Row => {
  if (typeof value !== "object" || value === null) return false;
  const row = value as Record<string, unknown>;
  return (
    isRaceSource(row.source) &&
    typeof row.raceKey === "string" &&
    typeof row.kaisaiNen === "string" &&
    typeof row.kaisaiTsukihi === "string" &&
    typeof row.keibajoCode === "string" &&
    typeof row.raceBango === "string" &&
    typeof row.finishPosition === "number"
  );
};

const isNumberOrNull = (value: unknown): value is number | null =>
  value === null || typeof value === "number";

const isRawTanshoOddsRow = (value: unknown): value is RawTanshoOddsRow =>
  typeof value === "object" &&
  value !== null &&
  "race_key" in value &&
  typeof value.race_key === "string" &&
  "combination" in value &&
  typeof value.combination === "string" &&
  "odds" in value &&
  isNumberOrNull(value.odds) &&
  "rank" in value &&
  isNumberOrNull(value.rank);

// v5 bumped 2026-05-29 for the Phase E features-worker cutover. The snapshot
// query no longer reads legacy daily_race_entries (Phase 0 rule 3) so any
// pre-cutover cache entry would surface stale rows with the v4 schema.
const buildCacheKey = ({ source, startYmd, endYmd }: RaceTrendD1RowsParams): string =>
  `race-trend-d1:v5:${source}:${startYmd}:${endYmd}`;

const formatHassoJikoku = (raceStartAtJst: string | null): string | null => {
  if (typeof raceStartAtJst !== "string" || raceStartAtJst.length < 16) return null;
  return `${raceStartAtJst.slice(11, 13)}${raceStartAtJst.slice(14, 16)}`;
};

// Phase E removed the LEFT JOIN to daily_race_entries — the snapshot-derived
// result keeps `wakuban` / `tansho*` as null and lets the HOT D1 odds overlay
// and the features-worker daily payload supply the missing fields. Legacy
// daily_race_entries is NEVER read from this worker (Phase 0 rule 3).
const SELECT_SQL = `
  with latest_result as (
    select race_key, horse_number, finish_position, time
    from race_result_snapshots r1
    where fetched_at = (
      select max(fetched_at) from race_result_snapshots r2
      where r2.race_key = r1.race_key and r2.horse_number = r1.horse_number
    )
  ),
  latest_entry as (
    select race_key, horse_number, horse_name, jockey_name
    from race_entry_snapshots e1
    where fetched_at = (
      select max(fetched_at) from race_entry_snapshots e2
      where e2.race_key = e1.race_key and e2.horse_number = e1.horse_number
    )
  ),
  latest_weight as (
    select race_key, horse_number, weight, change_sign, change_amount
    from horse_weight_snapshots w1
    where fetched_at = (
      select max(fetched_at) from horse_weight_snapshots w2
      where w2.race_key = w1.race_key and w2.horse_number = w1.horse_number
    )
  )
  select
    s.source as source,
    r.race_key as raceKey,
    s.kaisai_nen as kaisaiNen,
    s.kaisai_tsukihi as kaisaiTsukihi,
    s.keibajo_code as keibajoCode,
    s.race_bango as raceBango,
    s.race_name as raceName,
    s.race_start_at_jst as hassoJikoku,
    r.horse_number as umaban,
    e.horse_name as bamei,
    e.jockey_name as jockeyName,
    cast(nullif(replace(r.finish_position, ' ', ''), '') as integer) as finishPosition,
    r.time as sohaTime,
    w.weight as bataijuInt,
    w.change_sign as zogenFugo,
    w.change_amount as zogenSaInt
  from latest_result r
  join realtime_race_sources s on s.race_key = r.race_key
  left join latest_entry e on e.race_key = r.race_key and e.horse_number = r.horse_number
  left join latest_weight w on w.race_key = r.race_key and w.horse_number = r.horse_number
  where s.source = ?
    and s.kaisai_nen || s.kaisai_tsukihi between ? and ?
    and cast(nullif(replace(r.finish_position, ' ', ''), '') as integer) > 0
  order by s.kaisai_nen desc, s.kaisai_tsukihi desc, s.keibajo_code asc, s.race_bango asc, cast(nullif(r.horse_number, '') as integer) asc
`;

const queryD1 = async (params: RaceTrendD1RowsParams): Promise<RawD1Row[]> => {
  const { env } = await getCloudflareContext({ async: true });
  const db = env?.REALTIME_DB;
  if (!db) return [];
  const result = await db
    .prepare(SELECT_SQL)
    .bind(params.source, params.startYmd, params.endYmd)
    .all();
  return result.results.filter(isRawD1Row);
};

// Latest tansho odds live in the hot D1 (`sync-realtime-data-hot`). The
// query selects all combinations at the most recent fetched_at per race,
// matching the legacy `latest_tansho_odds` CTE behavior from REALTIME_DB.
const buildHotTanshoSelectSql = (placeholders: string): string =>
  `select race_key, combination, odds, rank from odds_snapshots where race_key in (${placeholders}) and odds_type = 'tansho' and fetched_at = (select max(fetched_at) from odds_snapshots o2 where o2.race_key = odds_snapshots.race_key and o2.odds_type = 'tansho')`;

interface GetLatestTanshoOddsFromHotD1Params {
  env: CloudflareEnv | null;
  raceKeys: ReadonlyArray<string>;
}

// Normalize combination / umaban so a stored "05" matches a lookup "5".
// Returns null when the value is empty or non-numeric, in which case the
// odds entry is dropped.
const normalizeHorseCombination = (combination: string): string | null => {
  const trimmed = combination.trim();
  if (trimmed.length === 0) return null;
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isNaN(parsed) ? null : String(parsed);
};

const accumulateTanshoOddsRow = (acc: TanshoOddsMap, row: RawTanshoOddsRow): TanshoOddsMap => {
  const normalizedCombination = normalizeHorseCombination(row.combination);
  if (normalizedCombination === null) return acc;
  const existing = acc.get(row.race_key) ?? new Map<string, TanshoOddsEntry>();
  existing.set(normalizedCombination, { odds: row.odds, rank: row.rank });
  acc.set(row.race_key, existing);
  return acc;
};

export const getLatestTanshoOddsFromHotD1 = async ({
  env,
  raceKeys,
}: GetLatestTanshoOddsFromHotD1Params): Promise<TanshoOddsMap> => {
  const uniqueKeys = Array.from(new Set(raceKeys.filter((key) => key.length > 0)));
  if (uniqueKeys.length === 0) return new Map();
  const db = env?.REALTIME_HOT_DB;
  if (!db) return new Map();
  try {
    const placeholders = uniqueKeys.map(() => "?").join(",");
    const result = await db
      .prepare(buildHotTanshoSelectSql(placeholders))
      .bind(...uniqueKeys)
      .all();
    const validRows = result.results.filter(isRawTanshoOddsRow);
    return validRows.reduce(
      accumulateTanshoOddsRow,
      new Map<string, Map<string, TanshoOddsEntry>>(),
    );
  } catch (error) {
    console.error("D1 hot tansho odds query failed", error);
    return new Map();
  }
};

const padString = (value: number | null, width: number): string | null =>
  value === null ? null : String(value).padStart(width, "0");

const pickTanshoOdds = (
  raw: RawD1Row,
  oddsMap: TanshoOddsMap,
): { tanshoOddsTenth: number | null; tanshoPopularity: number | null } => {
  const combinationKey = raw.umaban === null ? null : normalizeHorseCombination(raw.umaban);
  if (combinationKey === null) {
    return { tanshoOddsTenth: null, tanshoPopularity: null };
  }
  const entry = oddsMap.get(raw.raceKey)?.get(combinationKey);
  if (entry === undefined) {
    return { tanshoOddsTenth: null, tanshoPopularity: null };
  }
  const oddsTenth = entry.odds === null ? null : Math.round(entry.odds * 10);
  return { tanshoOddsTenth: oddsTenth, tanshoPopularity: entry.rank };
};

const toStarterRow = (raw: RawD1Row, oddsMap: TanshoOddsMap): RaceTrendStarterRow => {
  const { tanshoOddsTenth, tanshoPopularity } = pickTanshoOdds(raw, oddsMap);
  return {
    source: raw.source as RaceSource,
    kaisaiNen: raw.kaisaiNen,
    kaisaiTsukihi: raw.kaisaiTsukihi,
    keibajoCode: raw.keibajoCode,
    raceBango: raw.raceBango,
    raceName: raw.raceName,
    hassoJikoku: formatHassoJikoku(raw.hassoJikoku),
    runnerCount: null,
    wakuban: null,
    umaban: raw.umaban,
    bamei: raw.bamei,
    jockeyName: raw.jockeyName,
    tanshoOdds: padString(tanshoOddsTenth, 4),
    tanshoPopularity: padString(tanshoPopularity, 2),
    finishPosition: raw.finishPosition,
    sohaTime: raw.sohaTime,
    corner1: null,
    corner2: null,
    corner3: null,
    corner4: null,
    bataiju: raw.bataijuInt === null ? null : String(raw.bataijuInt),
    zogenFugo: raw.zogenFugo,
    zogenSa: raw.zogenSaInt === null ? null : String(raw.zogenSaInt),
  };
};

const getCachedResponse = async (cacheKey: string): Promise<RaceTrendStarterRow[] | null> => {
  const cache = typeof caches === "undefined" ? null : caches.default;
  const cacheRequest = new Request(`${CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);
  const cached = await cache?.match(cacheRequest);
  if (cached?.ok) {
    return cached.json() as Promise<RaceTrendStarterRow[]>;
  }
  const { env } = await getCloudflareContext({ async: true });
  const body = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!body) return null;
  return JSON.parse(body) as RaceTrendStarterRow[];
};

const putCache = async (cacheKey: string, rows: RaceTrendStarterRow[]): Promise<void> => {
  const body = JSON.stringify(rows);
  const cache = typeof caches === "undefined" ? null : caches.default;
  const { env } = await getCloudflareContext({ async: true });
  await Promise.all([
    cache?.put(
      new Request(`${CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`),
      new Response(body, {
        headers: {
          "Cache-Control": `public, max-age=${CACHE_TTL_SECONDS}`,
          "Content-Type": "application/json; charset=utf-8",
        },
      }),
    ),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, {
      expirationTtl: KV_TTL_SECONDS,
    }),
  ]);
};

export const getRaceTrendD1StarterRows = async (
  params: RaceTrendD1RowsParams,
): Promise<RaceTrendStarterRow[]> => {
  const cacheKey = buildCacheKey(params);
  const cached = await getCachedResponse(cacheKey);
  if (cached !== null) return cached;
  try {
    const raw = await queryD1(params);
    if (raw.length === 0) return [];
    const uniqueRaceKeys = Array.from(new Set(raw.map((row) => row.raceKey)));
    const { env } = await getCloudflareContext({ async: true });
    // Preview / pre-binding deploys degrade gracefully: when REALTIME_HOT_DB
    // is missing the helper short-circuits to an empty map and we fall back
    // to nulls for tansho fields.
    const oddsMap = await getLatestTanshoOddsFromHotD1({
      env: env ?? null,
      raceKeys: uniqueRaceKeys,
    });
    const rows = raw.map((row) => toStarterRow(row, oddsMap));
    // Do not cache empty result sets — the most common reason for an empty
    // .all() is a D1 CPU / connection saturation event where the binding
    // silently returns `results: []` instead of throwing, and persisting
    // that poisons the layer until the natural TTL expires. Let the next
    // request retry the upstream query.
    if (rows.length > 0) {
      await putCache(cacheKey, rows);
    }
    return rows;
  } catch (error) {
    console.error("D1 trend query failed", error);
    return [];
  }
};

interface FeaturesWorkerStarterPayload {
  starterRows?: unknown;
}

const isRaceTrendStarterRow = (value: unknown): value is RaceTrendStarterRow => {
  if (typeof value !== "object" || value === null) return false;
  const row = value as Record<string, unknown>;
  return (
    isRaceSource(row.source) &&
    typeof row.kaisaiNen === "string" &&
    typeof row.kaisaiTsukihi === "string" &&
    typeof row.keibajoCode === "string" &&
    typeof row.raceBango === "string" &&
    typeof row.finishPosition === "number"
  );
};

const parseFeaturesWorkerPayload = (payload: unknown): RaceTrendStarterRow[] => {
  if (typeof payload !== "object" || payload === null) return [];
  const { starterRows } = payload as FeaturesWorkerStarterPayload;
  if (!Array.isArray(starterRows)) return [];
  return starterRows.filter(isRaceTrendStarterRow);
};

const buildFeaturesWorkerUrl = (params: RaceTrendD1RowsParams): string => {
  const query = new URLSearchParams({
    source: params.source,
    from: params.startYmd,
    to: params.endYmd,
  });
  return `${FEATURES_WORKER_BASE}${FEATURES_RACE_TREND_PATH}?${query.toString()}`;
};

const fetchDailyStarterRowsFromFeaturesWorker = async (
  params: RaceTrendD1RowsParams,
): Promise<RaceTrendStarterRow[]> => {
  const { env } = await getCloudflareContext({ async: true });
  const features = env?.REALTIME_FEATURES;
  if (!features) return [];
  try {
    const response = await features.fetch(buildFeaturesWorkerUrl(params));
    if (!response.ok) return [];
    const payload: unknown = await response.json();
    return parseFeaturesWorkerPayload(payload);
  } catch (error) {
    console.error("features worker race-trend fetch failed", error);
    return [];
  }
};

// v5 bumped in lock-step with `buildCacheKey` so a single deploy invalidates
// every layer that participates in the trend payload.
const buildDailyCacheKey = ({ source, startYmd, endYmd }: RaceTrendD1RowsParams): string =>
  `race-trend-d1-daily:v5:${source}:${startYmd}:${endYmd}`;

const getCachedDailyResponse = async (cacheKey: string): Promise<RaceTrendStarterRow[] | null> => {
  const cache = typeof caches === "undefined" ? null : caches.default;
  const cacheRequest = new Request(`${DAILY_CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);
  const cached = await cache?.match(cacheRequest);
  if (cached?.ok) {
    return cached.json() as Promise<RaceTrendStarterRow[]>;
  }
  const { env } = await getCloudflareContext({ async: true });
  const body = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!body) return null;
  return JSON.parse(body) as RaceTrendStarterRow[];
};

const putDailyCache = async (cacheKey: string, rows: RaceTrendStarterRow[]): Promise<void> => {
  const body = JSON.stringify(rows);
  const cache = typeof caches === "undefined" ? null : caches.default;
  const { env } = await getCloudflareContext({ async: true });
  await Promise.all([
    cache?.put(
      new Request(`${DAILY_CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`),
      new Response(body, {
        headers: {
          "Cache-Control": `public, max-age=${CACHE_TTL_SECONDS}`,
          "Content-Type": "application/json; charset=utf-8",
        },
      }),
    ),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, {
      expirationTtl: KV_TTL_SECONDS,
    }),
  ]);
};

// Phase E cutover: the canonical historical daily-rows source is the
// features worker (R2 Parquet aggregate). Legacy direct D1 reads of
// daily_race_entries are forbidden by Phase 0 rule 3. While the worker
// endpoint still returns the stub aggregate (`{ raceCount: 0, ... }`) the
// parsed `starterRows` field comes back empty and this helper returns an
// empty array — the viewer falls back to the snapshot-derived path in
// `getRaceTrendD1StarterRows` and renders a "preparing" empty state.
export const getRaceTrendDailyStarterRows = async (
  params: RaceTrendD1RowsParams,
): Promise<RaceTrendStarterRow[]> => {
  const cacheKey = buildDailyCacheKey(params);
  const cached = await getCachedDailyResponse(cacheKey);
  if (cached !== null) return cached;
  const rows = await fetchDailyStarterRowsFromFeaturesWorker(params);
  // Skip caching empty results so the next call retries the worker once it
  // moves off the stub aggregate. Caching `[]` would pin the empty payload
  // for the 30 min KV TTL and hide the cutover transition entirely.
  if (rows.length > 0) {
    await putDailyCache(cacheKey, rows);
  }
  return rows;
};
