// Run with bun. Reads aggregated trend rows from D1.
// Phase B (2026-05-29) split the single `race-trend-d1:v5` cache key into
// two namespaces with very different freshness / scope characteristics:
//
// * `getRaceTrendTodayStarterRows`  -> snapshot tables (race_result_snapshots
//   / race_entry_snapshots / horse_weight_snapshots / realtime_race_sources)
//   on REALTIME_DB. Returns every completed starter row for the target day
//   regardless of venue / race, so the route handler can render sibling
//   races' results. Backed only by edge Cache API (30s TTL) — the 5min
//   poller refreshes upstream rows on a tight cadence and KV mirror would
//   pin stale data across colos.
// * `getRaceTrendPast14StarterRows` -> features worker `/api/features/race-trend`
//   (R2 Parquet aggregate). The endpoint now requires source / keibajoCode /
//   raceBango / from / to and returns the per-race past-14 aggregate.
//   Backed by cross-colo KV (30 min) + edge Cache API (5 min).
// * `getRaceTrendRunningStylesFromD1` / `getRaceTrendTodayRunningStylesFromD1`
//   -> REALTIME_FEATURES_DB.race_running_styles (new D1, Phase E). The
//   past-14 helper caches the result in KV; the today helper skips KV so
//   freshly-inferred sibling rows are visible within the Cache API TTL.
import "server-only";
import type {
  RaceTrendRunningStyleCache,
  RaceTrendStarterRow,
} from "horse-racing-realtime/race-trend-daily-track-types";
import { deriveWakubanString } from "horse-racing-realtime/wakuban";

import { safeGetCloudflareEnv } from "../lib/cloudflare-context.server";
import type { RaceSource } from "../lib/codes";
import {
  RACE_TREND_PAST14_LOOKBACK_DAYS,
  buildRaceTrendPast14CacheKey,
  buildRaceTrendTodayCacheKey,
} from "../lib/race-trend-cache";
import type { RaceTrendRunningStyle } from "../lib/race-types";

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
  predicted_label: RaceTrendRunningStyle;
  horse_number: number;
  race_key: string;
}

const isRunningStyle = (value: unknown): value is RaceTrendRunningStyle =>
  value === "nige" || value === "senkou" || value === "sashi" || value === "oikomi";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isRawRunningStyleD1Row = (value: unknown): value is RawRunningStyleD1Row => {
  if (!isRecord(value)) return false;
  return (
    typeof value.race_key === "string" &&
    typeof value.horse_number === "number" &&
    isRunningStyle(value.predicted_label)
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
  predictedLabel: raw.predicted_label,
  raceKey: raw.race_key,
});

const buildRunningStyleSelectSql = (placeholders: string): string =>
  `select race_key, horse_number, predicted_label from race_running_styles where race_key in (${placeholders})`;

const RUNNING_STYLES_KV_PREFIX = "race-trend-running-styles:v1";
const HEX_BYTE_WIDTH = 2;
const HEX_RADIX = 16;

// Hash the joined race-key list with Web Crypto SHA-1 so the cache key stays
// well under Cloudflare KV's 512 byte key length limit, even when callers
// supply 200+ historicalRaceKeys (joining them raw blew past 3KB and caused
// `kv.get` to throw 414). SHA-1 is fine here — collision resistance does
// not matter, we only need a deterministic short fingerprint per input.
const toHex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(HEX_RADIX).padStart(HEX_BYTE_WIDTH, "0")).join("");

const hashJoinedKeys = async (joined: string): Promise<string> => {
  const encoded = new TextEncoder().encode(joined);
  const digest = await crypto.subtle.digest("SHA-1", encoded);
  return toHex(new Uint8Array(digest));
};

const buildRunningStylesCacheKey = async (sortedKeys: ReadonlyArray<string>): Promise<string> => {
  // Embed the count so two race lookups that happen to share a prefix in
  // the future can't collide on the key. The hash keeps the key length
  // bounded regardless of how many historicalRaceKeys were supplied.
  const hash = await hashJoinedKeys(sortedKeys.join(","));
  return `${RUNNING_STYLES_KV_PREFIX}:${sortedKeys.length}:${hash}`;
};

const isRaceTrendRunningStyleCache = (value: unknown): value is RaceTrendRunningStyleCache => {
  if (!isRecord(value)) return false;
  return (
    typeof value.horseNumber === "string" &&
    typeof value.raceKey === "string" &&
    isRunningStyle(value.predictedLabel)
  );
};

const parseRunningStylesBody = (body: string): RaceTrendRunningStyleCache[] | null => {
  try {
    const parsed: unknown = JSON.parse(body);
    if (!Array.isArray(parsed)) return null;
    return parsed.filter(isRaceTrendRunningStyleCache);
  } catch {
    return null;
  }
};

const readRunningStylesFromKv = async (
  env: CloudflareEnv | null,
  cacheKey: string,
): Promise<RaceTrendRunningStyleCache[] | null> => {
  const kv = env?.DETAIL_SECTION_CACHE_KV;
  if (!kv) return null;
  // Defensive try/catch: if KV throws (eg. key length limit, transient
  // network), degrade to D1 fallback instead of bubbling 500 to the route.
  try {
    const body = await kv.get(cacheKey);
    if (!body) return null;
    return parseRunningStylesBody(body);
  } catch (error) {
    console.error("KV get for running-styles failed", error);
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
  // KV write failure must not propagate — the in-memory result is still
  // valid, we just lose the cache-write side-effect for this call.
  try {
    await kv.put(cacheKey, JSON.stringify(rows), { expirationTtl: KV_TTL_PAST14_SECONDS });
  } catch (error) {
    console.error("KV put for running-styles failed", error);
  }
};

// Prefer REALTIME_FEATURES_DB (new D1, post Phase E). Fall back to REALTIME_DB
// only when the new binding is missing — eg preview deploys that have not yet
// picked up the features worker binding. The new D1 has the same
// `race_running_styles` schema as the legacy D1 because the features worker
// migration was a clean schema copy.
const pickRunningStylesDb = (env: CloudflareEnv | null): PcKeibaD1Database | null =>
  env?.REALTIME_FEATURES_DB ?? env?.REALTIME_DB ?? null;

interface RunWorkerArgs {
  chunks: ReadonlyArray<ReadonlyArray<string>>;
  indexState: { value: number };
  queryChunk: (chunk: ReadonlyArray<string>) => Promise<RaceTrendRunningStyleCache[]>;
  results: RaceTrendRunningStyleCache[];
}

const runWorkerLoop = async ({
  chunks,
  indexState,
  queryChunk,
  results,
}: RunWorkerArgs): Promise<void> => {
  const next = indexState.value;
  indexState.value = next + 1;
  const chunk = chunks[next];
  if (!chunk) return;
  const chunkResult = await queryChunk(chunk);
  results.push(...chunkResult);
  await runWorkerLoop({ chunks, indexState, queryChunk, results });
};

const queryRunningStylesFromDb = async (
  db: PcKeibaD1Database,
  uniqueKeys: ReadonlyArray<string>,
): Promise<RaceTrendRunningStyleCache[]> => {
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
  const chunks = chunkArray(uniqueKeys, RUNNING_STYLE_BATCH_SIZE);
  const results: RaceTrendRunningStyleCache[] = [];
  const indexState = { value: 0 };
  const workerCount = Math.min(RUNNING_STYLE_CHUNK_CONCURRENCY, chunks.length);
  await Promise.all(
    Array.from({ length: workerCount }, () =>
      runWorkerLoop({ chunks, indexState, queryChunk, results }),
    ),
  );
  return results;
};

export const getRaceTrendRunningStylesFromD1 = async (
  raceKeys: ReadonlyArray<string>,
): Promise<RaceTrendRunningStyleCache[]> => {
  const uniqueKeys = Array.from(new Set(raceKeys.filter((key) => key.length > 0))).toSorted();
  if (uniqueKeys.length === 0) return [];
  const env = await safeGetCloudflareEnv();
  const db = pickRunningStylesDb(env ?? null);
  if (!db) return [];
  const cacheKey = await buildRunningStylesCacheKey(uniqueKeys);
  const cached = await readRunningStylesFromKv(env, cacheKey);
  if (cached !== null) return cached;
  try {
    const results = await queryRunningStylesFromDb(db, uniqueKeys);
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

// Today running-styles helper: skips KV (the 5 min poller refreshes
// freshly-inferred rows for the day and pinning them in KV would defeat
// the short TTL) and goes straight to D1.
export const getRaceTrendTodayRunningStylesFromD1 = async (
  raceKeys: ReadonlyArray<string>,
): Promise<RaceTrendRunningStyleCache[]> => {
  const uniqueKeys = Array.from(new Set(raceKeys.filter((key) => key.length > 0))).toSorted();
  if (uniqueKeys.length === 0) return [];
  const env = await safeGetCloudflareEnv();
  const db = pickRunningStylesDb(env ?? null);
  if (!db) return [];
  try {
    return await queryRunningStylesFromDb(db, uniqueKeys);
  } catch (error) {
    console.error("D1 race_running_styles today query failed", error);
    return [];
  }
};

interface RawD1Row {
  source: RaceSource;
  raceKey: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  raceName: string | null;
  hassoJikoku: string | null;
  umaban: string | null;
  horseCount: number;
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

const TODAY_CACHE_URL_BASE = "https://pc-keiba-viewer.local/d1-trend-today-cache/";
const PAST14_CACHE_URL_BASE = "https://pc-keiba-viewer.local/d1-trend-past14-cache/";
// Today cache: edge Cache API only with a 30s TTL. The 5 min snapshot
// poller pushes finishes through on a tight cadence and a longer TTL
// would hide newly-completed sibling races for too long.
const CACHE_TTL_TODAY_SECONDS = 30;
// Past-14 cache: edge Cache API for the colo-local hot path. KV mirror
// expires after 30 min, matching the cross-colo `pickRunningStylesDb`
// rollup window.
const CACHE_TTL_PAST14_SECONDS = 5 * 60;
const KV_TTL_PAST14_SECONDS = 30 * 60;

// Phase B: the past-14 endpoint expects per-race query params so the
// features worker can read only the relevant R2 prefix. Hard-code the
// production hostname so `env.REALTIME_FEATURES.fetch` resolves to the
// bound service worker regardless of which Cloudflare colo handles the
// request.
const FEATURES_WORKER_BASE = "https://sync-realtime-data-features.kkk4oru.com";
const FEATURES_RACE_TREND_PATH = "/api/features/race-trend";

const isRaceSource = (value: unknown): value is RaceSource => value === "jra" || value === "nar";

const isRawD1Row = (value: unknown): value is RawD1Row => {
  if (!isRecord(value)) return false;
  return (
    isRaceSource(value.source) &&
    typeof value.raceKey === "string" &&
    typeof value.kaisaiNen === "string" &&
    typeof value.kaisaiTsukihi === "string" &&
    typeof value.keibajoCode === "string" &&
    typeof value.raceBango === "string" &&
    typeof value.finishPosition === "number" &&
    typeof value.horseCount === "number"
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

const formatHassoJikoku = (raceStartAtJst: string | null): string | null => {
  if (typeof raceStartAtJst !== "string" || raceStartAtJst.length < 16) return null;
  return `${raceStartAtJst.slice(11, 13)}${raceStartAtJst.slice(14, 16)}`;
};

// Phase E removed the LEFT JOIN to daily_race_entries — the snapshot-derived
// result keeps `tansho*` as null and lets the HOT D1 odds overlay and the
// features-worker past-14 payload supply the missing fields. Legacy
// daily_race_entries is NEVER read from this worker (Phase 0 rule 3).
//
// 2026-06-01: wakuban is now derived for both JRA and NAR rows from
// `umaban` + the race's actual horse_count via the shared
// `deriveWakubanString` helper. The snapshot tables don't carry wakuban
// directly, and the trend page's today-only default filter (e.g.
// `?raceTrendTargets=frame`) would otherwise drop every NAR row when
// grouping by frame. The frame distribution rule is the same official
// algorithm for JRA / NAR / Ban-ei, so the same helper covers all sources.
//
// 2026-06-02: horse_count source switched from `count(distinct horse_number)
// over latest_result` to `realtime_race_sources.result_expected_horse_count`.
// NAR result snapshots only persist top-3 finishers (12-horse race ->
// 3 rows), so the prior CTE was reporting horseCount=3 and the wakuban
// bounds check (`horseNumber <= horseCount`) was failing for every umaban
// >= 4 -> wakuban=null -> the aggregator dropped the row from the frame
// target. `result_expected_horse_count` is the authoritative source written
// by the result writer before any partial snapshot lands. When the column is
// null (very early in the cycle), horseCount falls back to 0 and wakuban
// stays null, preserving the prior degrade-gracefully behavior.
//
// 2026-06-02 (race 43/09 hotfix): base table switched from
// `race_result_snapshots` to `race_entry_snapshots` and result rows joined
// in via LEFT JOIN. Reason: NAR result snapshots only persist top-3
// finishers, so a sibling race that never landed any of its top-3 finishers
// in a given frame was completely dropped from the response (the user
// reported枠1 only showing R01 because R02-R08 top-3 didn't include any
// umaban 1-2 horses). With the entry-based base, every starter shows up
// once per race regardless of whether result data has arrived. Result fields
// (finishPosition, sohaTime, bataiju, zogen) coalesce to 0 / null for
// entry-only rows. The frame aggregator only counts >= 1 as ranked, so
// entry-only rows contribute to `starts` (frame participation) without
// inflating show/quinella/win rates.
const SELECT_SQL = `
  with latest_entry as (
    select race_key, horse_number, horse_name, jockey_name, fetched_at
    from race_entry_snapshots e1
    where fetched_at = (
      select max(fetched_at) from race_entry_snapshots e2
      where e2.race_key = e1.race_key and e2.horse_number = e1.horse_number
    )
  ),
  latest_result as (
    select race_key, horse_number, finish_position, time
    from race_result_snapshots r1
    where fetched_at = (
      select max(fetched_at) from race_result_snapshots r2
      where r2.race_key = r1.race_key and r2.horse_number = r1.horse_number
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
    e.race_key as raceKey,
    s.kaisai_nen as kaisaiNen,
    s.kaisai_tsukihi as kaisaiTsukihi,
    s.keibajo_code as keibajoCode,
    s.race_bango as raceBango,
    s.race_name as raceName,
    s.race_start_at_jst as hassoJikoku,
    e.horse_number as umaban,
    coalesce(s.result_expected_horse_count, 0) as horseCount,
    e.horse_name as bamei,
    e.jockey_name as jockeyName,
    coalesce(cast(nullif(replace(r.finish_position, ' ', ''), '') as integer), 0) as finishPosition,
    r.time as sohaTime,
    w.weight as bataijuInt,
    w.change_sign as zogenFugo,
    w.change_amount as zogenSaInt
  from latest_entry e
  join realtime_race_sources s on s.race_key = e.race_key
  left join latest_result r on r.race_key = e.race_key and r.horse_number = e.horse_number
  left join latest_weight w on w.race_key = e.race_key and w.horse_number = e.horse_number
  where s.source = ?
    and s.kaisai_nen || s.kaisai_tsukihi between ? and ?
    and s.keibajo_code = ?
  order by s.kaisai_nen desc, s.kaisai_tsukihi desc, s.keibajo_code asc, s.race_bango asc, cast(nullif(e.horse_number, '') as integer) asc
`;

interface SnapshotQueryParams {
  endYmd: string;
  keibajoCode: string;
  source: RaceSource;
  startYmd: string;
}

const queryD1 = async (params: SnapshotQueryParams): Promise<RawD1Row[]> => {
  const env = await safeGetCloudflareEnv();
  const db = env?.REALTIME_DB;
  if (!db) return [];
  const result = await db
    .prepare(SELECT_SQL)
    .bind(params.source, params.startYmd, params.endYmd, params.keibajoCode)
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

const deriveStarterWakuban = (raw: RawD1Row): string | null => {
  if (raw.umaban === null) return null;
  const horseNumber = Number.parseInt(raw.umaban, 10);
  if (!Number.isFinite(horseNumber)) return null;
  return deriveWakubanString({ horseCount: raw.horseCount, horseNumber });
};

const toStarterRow = (raw: RawD1Row, oddsMap: TanshoOddsMap): RaceTrendStarterRow => {
  const { tanshoOddsTenth, tanshoPopularity } = pickTanshoOdds(raw, oddsMap);
  return {
    source: raw.source,
    kaisaiNen: raw.kaisaiNen,
    kaisaiTsukihi: raw.kaisaiTsukihi,
    keibajoCode: raw.keibajoCode,
    raceBango: raw.raceBango,
    raceName: raw.raceName,
    hassoJikoku: formatHassoJikoku(raw.hassoJikoku),
    runnerCount: null,
    wakuban: deriveStarterWakuban(raw),
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

const isRaceTrendStarterRow = (value: unknown): value is RaceTrendStarterRow => {
  if (!isRecord(value)) return false;
  return (
    isRaceSource(value.source) &&
    typeof value.kaisaiNen === "string" &&
    typeof value.kaisaiTsukihi === "string" &&
    typeof value.keibajoCode === "string" &&
    typeof value.raceBango === "string" &&
    typeof value.finishPosition === "number"
  );
};

const parseStarterRowArray = (value: unknown): RaceTrendStarterRow[] | null => {
  if (!Array.isArray(value)) return null;
  return value.filter(isRaceTrendStarterRow);
};

const parseFeaturesWorkerPayload = (payload: unknown): RaceTrendStarterRow[] => {
  if (!isRecord(payload)) return [];
  const { starterRows } = payload;
  if (!Array.isArray(starterRows)) return [];
  return starterRows.filter(isRaceTrendStarterRow);
};

const readCachedStarterRowsResponse = async (
  cached: Response,
): Promise<RaceTrendStarterRow[] | null> => {
  try {
    const parsed: unknown = await cached.json();
    return parseStarterRowArray(parsed);
  } catch {
    return null;
  }
};

const parseCachedStarterRowsBody = (body: string): RaceTrendStarterRow[] | null => {
  try {
    const parsed: unknown = JSON.parse(body);
    return parseStarterRowArray(parsed);
  } catch {
    return null;
  }
};

// ---- Today starter rows (snapshot-derived, sibling auto-included) ----

const getCachedTodayResponse = async (cacheKey: string): Promise<RaceTrendStarterRow[] | null> => {
  const cache = typeof caches === "undefined" ? null : caches.default;
  if (!cache) return null;
  const cacheRequest = new Request(`${TODAY_CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);
  const cached = await cache.match(cacheRequest);
  if (!cached?.ok) return null;
  return readCachedStarterRowsResponse(cached);
};

const putTodayCache = async (cacheKey: string, rows: RaceTrendStarterRow[]): Promise<void> => {
  const cache = typeof caches === "undefined" ? null : caches.default;
  if (!cache) return;
  const body = JSON.stringify(rows);
  await cache.put(
    new Request(`${TODAY_CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`),
    new Response(body, {
      headers: {
        "Cache-Control": `public, max-age=${CACHE_TTL_TODAY_SECONDS}`,
        "Content-Type": "application/json; charset=utf-8",
      },
    }),
  );
};

interface RaceTrendTodayParams {
  keibajoCode: string;
  source: RaceSource;
  targetYmd: string;
}

export const getRaceTrendTodayStarterRows = async (
  params: RaceTrendTodayParams,
): Promise<RaceTrendStarterRow[]> => {
  const cacheKey = buildRaceTrendTodayCacheKey({
    keibajoCode: params.keibajoCode,
    source: params.source,
    targetYmd: params.targetYmd,
  });
  const cached = await getCachedTodayResponse(cacheKey);
  if (cached !== null) return cached;
  try {
    const raw = await queryD1({
      endYmd: params.targetYmd,
      keibajoCode: params.keibajoCode,
      source: params.source,
      startYmd: params.targetYmd,
    });
    if (raw.length === 0) return [];
    const uniqueRaceKeys = Array.from(new Set(raw.map((row) => row.raceKey)));
    const env = await safeGetCloudflareEnv();
    // Preview / pre-binding deploys degrade gracefully: when REALTIME_HOT_DB
    // is missing the helper short-circuits to an empty map and we fall back
    // to nulls for tansho fields.
    const oddsMap = await getLatestTanshoOddsFromHotD1({
      env: env ?? null,
      raceKeys: uniqueRaceKeys,
    });
    const rows = raw.map((row) => toStarterRow(row, oddsMap));
    // Do not cache empty result sets — the most common reason for an empty
    // `.all()` is a D1 CPU / connection saturation event where the binding
    // silently returns `results: []` instead of throwing, and persisting
    // that poisons the layer until the natural TTL expires.
    if (rows.length > 0) {
      await putTodayCache(cacheKey, rows);
    }
    return rows;
  } catch (error) {
    console.error("D1 today trend query failed", error);
    return [];
  }
};

// ---- Past-14 starter rows (features worker, per-race) ----

const buildFeaturesWorkerUrl = (params: RaceTrendPast14Params): string => {
  const query = new URLSearchParams({
    source: params.source,
    keibajoCode: params.keibajoCode,
    raceBango: params.raceBango,
    from: params.startYmd,
    to: params.endYmd,
  });
  return `${FEATURES_WORKER_BASE}${FEATURES_RACE_TREND_PATH}?${query.toString()}`;
};

const fetchPast14StarterRowsFromFeaturesWorker = async (
  params: RaceTrendPast14Params,
): Promise<RaceTrendStarterRow[]> => {
  const env = await safeGetCloudflareEnv();
  const features = env?.REALTIME_FEATURES;
  if (!features) return [];
  try {
    const response = await features.fetch(buildFeaturesWorkerUrl(params));
    if (!response.ok) return [];
    const payload: unknown = await response.json();
    return parseFeaturesWorkerPayload(payload);
  } catch (error) {
    console.error("features worker past-14 race-trend fetch failed", error);
    return [];
  }
};

const getCachedPast14Response = async (cacheKey: string): Promise<RaceTrendStarterRow[] | null> => {
  const cache = typeof caches === "undefined" ? null : caches.default;
  const cacheRequest = new Request(`${PAST14_CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);
  const cached = await cache?.match(cacheRequest);
  if (cached?.ok) {
    return readCachedStarterRowsResponse(cached);
  }
  const env = await safeGetCloudflareEnv();
  const body = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!body) return null;
  return parseCachedStarterRowsBody(body);
};

const putPast14Cache = async (cacheKey: string, rows: RaceTrendStarterRow[]): Promise<void> => {
  const body = JSON.stringify(rows);
  const cache = typeof caches === "undefined" ? null : caches.default;
  const env = await safeGetCloudflareEnv();
  await Promise.all([
    cache?.put(
      new Request(`${PAST14_CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`),
      new Response(body, {
        headers: {
          "Cache-Control": `public, max-age=${CACHE_TTL_PAST14_SECONDS}`,
          "Content-Type": "application/json; charset=utf-8",
        },
      }),
    ),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, {
      expirationTtl: KV_TTL_PAST14_SECONDS,
    }),
  ]);
};

export interface RaceTrendPast14Params {
  endYmd: string;
  keibajoCode: string;
  raceBango: string;
  source: RaceSource;
  startYmd: string;
}

export const getRaceTrendPast14StarterRows = async (
  params: RaceTrendPast14Params,
): Promise<RaceTrendStarterRow[]> => {
  const cacheKey = buildRaceTrendPast14CacheKey({
    endYmd: params.endYmd,
    keibajoCode: params.keibajoCode,
    raceBango: params.raceBango,
    source: params.source,
    startYmd: params.startYmd,
  });
  const cached = await getCachedPast14Response(cacheKey);
  if (cached !== null) return cached;
  const rows = await fetchPast14StarterRowsFromFeaturesWorker(params);
  // Skip caching empty results so the next call retries the worker once
  // upstream R2 data lands. Caching `[]` would pin the empty payload for
  // the 30 min KV TTL.
  if (rows.length > 0) {
    await putPast14Cache(cacheKey, rows);
  }
  return rows;
};

// Helper for callers that already have the target Ymd and need the
// past-14 window endpoints. Centralizes the lookback constant so the
// route handler does not encode it twice.
const addDaysToYmd = (ymd: string, days: number): string => {
  const date = new Date(
    Date.UTC(Number(ymd.slice(0, 4)), Number(ymd.slice(4, 6)) - 1, Number(ymd.slice(6, 8))),
  );
  date.setUTCDate(date.getUTCDate() + days);
  return [
    String(date.getUTCFullYear()).padStart(4, "0"),
    String(date.getUTCMonth() + 1).padStart(2, "0"),
    String(date.getUTCDate()).padStart(2, "0"),
  ].join("");
};

export const buildPast14WindowForTarget = (
  targetYmd: string,
): { endYmd: string; startYmd: string } => ({
  endYmd: addDaysToYmd(targetYmd, -1),
  startYmd: addDaysToYmd(targetYmd, -RACE_TREND_PAST14_LOOKBACK_DAYS),
});
