// Run with bun. Reads aggregated trend rows from D1.
// * `getRaceTrendD1StarterRows`  -> snapshot tables (race_result_snapshots /
//   race_entry_snapshots / horse_weight_snapshots / realtime_race_sources):
//   the realtime-derived path that catches today's finishes before cron.
// * `getRaceTrendDailyStarterRows` -> `daily_race_entries`: the canonical
//   Neon-mirror populated by the build-daily-features cron. Source of truth
//   for prior-day finishes and replaces the legacy Neon trend query.
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

export const getRaceTrendRunningStylesFromD1 = async (
  raceKeys: ReadonlyArray<string>,
): Promise<RaceTrendRunningStyleCache[]> => {
  const uniqueKeys = Array.from(new Set(raceKeys.filter((key) => key.length > 0)));
  if (uniqueKeys.length === 0) return [];
  const { env } = await getCloudflareContext({ async: true });
  const db = env?.REALTIME_DB;
  if (!db) return [];
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
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  raceName: string | null;
  hassoJikoku: string | null;
  wakuban: string | null;
  umaban: string | null;
  bamei: string | null;
  jockeyName: string | null;
  tanshoOddsTenth: number | null;
  tanshoPopularity: number | null;
  finishPosition: number;
  sohaTime: string | null;
  bataijuInt: number | null;
  zogenFugo: string | null;
  zogenSaInt: number | null;
}

interface RawDailyD1Row {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  raceName: string | null;
  hassoJikoku: string | null;
  runnerCount: number | null;
  wakuban: string | null;
  umaban: number | null;
  bamei: string | null;
  jockeyName: string | null;
  tanshoOddsTenth: number | null;
  tanshoPopularity: number | null;
  finishPosition: number;
  sohaTime: number | null;
  corner1: number | null;
  corner2: number | null;
  corner3: number | null;
  corner4: number | null;
  bataijuInt: number | null;
  zogenFugo: string | null;
  zogenSaInt: number | null;
}

const CACHE_URL_BASE = "https://pc-keiba-viewer.local/d1-trend-cache/";
const DAILY_CACHE_URL_BASE = "https://pc-keiba-viewer.local/d1-trend-daily-cache/";
const CACHE_TTL_SECONDS = 60;
const KV_TTL_SECONDS = 5 * 60;

const isRaceSource = (value: unknown): value is RaceSource => value === "jra" || value === "nar";

const isRawD1Row = (value: unknown): value is RawD1Row => {
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

const buildCacheKey = ({ source, startYmd, endYmd }: RaceTrendD1RowsParams): string =>
  `race-trend-d1:v3:${source}:${startYmd}:${endYmd}`;

const formatHassoJikoku = (raceStartAtJst: string | null): string | null => {
  if (typeof raceStartAtJst !== "string" || raceStartAtJst.length < 16) return null;
  return `${raceStartAtJst.slice(11, 13)}${raceStartAtJst.slice(14, 16)}`;
};

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
  ),
  latest_tansho_odds as (
    select race_key, combination, odds, rank
    from odds_snapshots o1
    where odds_type = 'tansho' and fetched_at = (
      select max(fetched_at) from odds_snapshots o2
      where o2.race_key = o1.race_key
        and o2.odds_type = 'tansho'
        and o2.combination = o1.combination
    )
  )
  select
    s.source as source,
    s.kaisai_nen as kaisaiNen,
    s.kaisai_tsukihi as kaisaiTsukihi,
    s.keibajo_code as keibajoCode,
    s.race_bango as raceBango,
    s.race_name as raceName,
    s.race_start_at_jst as hassoJikoku,
    de.wakuban as wakuban,
    r.horse_number as umaban,
    coalesce(e.horse_name, de.bamei) as bamei,
    coalesce(e.jockey_name, de.kishumei_ryakusho) as jockeyName,
    cast(round(coalesce(o.odds, de.tansho_odds) * 10) as integer) as tanshoOddsTenth,
    coalesce(o.rank, de.tansho_ninkijun) as tanshoPopularity,
    cast(nullif(replace(r.finish_position, ' ', ''), '') as integer) as finishPosition,
    r.time as sohaTime,
    w.weight as bataijuInt,
    w.change_sign as zogenFugo,
    w.change_amount as zogenSaInt
  from latest_result r
  join realtime_race_sources s on s.race_key = r.race_key
  left join latest_entry e on e.race_key = r.race_key and e.horse_number = r.horse_number
  left join latest_weight w on w.race_key = r.race_key and w.horse_number = r.horse_number
  left join latest_tansho_odds o
    on o.race_key = r.race_key
    and cast(nullif(o.combination, '') as integer) = cast(nullif(r.horse_number, '') as integer)
  left join daily_race_entries de
    on de.source = s.source
    and de.kaisai_nen = s.kaisai_nen
    and de.kaisai_tsukihi = s.kaisai_tsukihi
    and de.keibajo_code = s.keibajo_code
    and de.race_bango = s.race_bango
    and de.umaban = cast(nullif(r.horse_number, '') as integer)
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

const padString = (value: number | null, width: number): string | null =>
  value === null ? null : String(value).padStart(width, "0");

const toStarterRow = (raw: RawD1Row): RaceTrendStarterRow => ({
  source: raw.source as RaceSource,
  kaisaiNen: raw.kaisaiNen,
  kaisaiTsukihi: raw.kaisaiTsukihi,
  keibajoCode: raw.keibajoCode,
  raceBango: raw.raceBango,
  raceName: raw.raceName,
  hassoJikoku: formatHassoJikoku(raw.hassoJikoku),
  runnerCount: null,
  wakuban: raw.wakuban,
  umaban: raw.umaban,
  bamei: raw.bamei,
  jockeyName: raw.jockeyName,
  tanshoOdds: padString(raw.tanshoOddsTenth, 4),
  tanshoPopularity: padString(raw.tanshoPopularity, 2),
  finishPosition: raw.finishPosition,
  sohaTime: raw.sohaTime,
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  bataiju: raw.bataijuInt === null ? null : String(raw.bataijuInt),
  zogenFugo: raw.zogenFugo,
  zogenSa: raw.zogenSaInt === null ? null : String(raw.zogenSaInt),
});

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
    const rows = raw.map(toStarterRow);
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

const DAILY_SELECT_SQL = `
  select
    source,
    kaisai_nen as kaisaiNen,
    kaisai_tsukihi as kaisaiTsukihi,
    keibajo_code as keibajoCode,
    race_bango as raceBango,
    race_name as raceName,
    hasso_jikoku as hassoJikoku,
    shusso_tosu as runnerCount,
    wakuban,
    umaban,
    bamei,
    kishumei_ryakusho as jockeyName,
    cast(round(tansho_odds * 10) as integer) as tanshoOddsTenth,
    tansho_ninkijun as tanshoPopularity,
    finish_position as finishPosition,
    soha_time as sohaTime,
    corner_1 as corner1,
    corner_2 as corner2,
    corner_3 as corner3,
    corner_4 as corner4,
    bataiju as bataijuInt,
    zogen_fugo as zogenFugo,
    zogen_sa as zogenSaInt
  from daily_race_entries
  where source = ?
    and race_date between ? and ?
    and finish_position is not null
  order by race_date desc, keibajo_code asc, race_bango asc, umaban asc
`;

const isRawDailyD1Row = (value: unknown): value is RawDailyD1Row => {
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

const buildDailyCacheKey = ({ source, startYmd, endYmd }: RaceTrendD1RowsParams): string =>
  `race-trend-d1-daily:v3:${source}:${startYmd}:${endYmd}`;

const queryDailyD1 = async (params: RaceTrendD1RowsParams): Promise<RawDailyD1Row[]> => {
  const { env } = await getCloudflareContext({ async: true });
  const db = env?.REALTIME_DB;
  if (!db) return [];
  const result = await db
    .prepare(DAILY_SELECT_SQL)
    .bind(params.source, params.startYmd, params.endYmd)
    .all();
  return result.results.filter(isRawDailyD1Row);
};

const intStringOrNull = (value: number | null): string | null =>
  value === null ? null : String(value);

const toDailyStarterRow = (raw: RawDailyD1Row): RaceTrendStarterRow => ({
  source: raw.source as RaceSource,
  kaisaiNen: raw.kaisaiNen,
  kaisaiTsukihi: raw.kaisaiTsukihi,
  keibajoCode: raw.keibajoCode,
  raceBango: raw.raceBango,
  raceName: raw.raceName,
  hassoJikoku: raw.hassoJikoku,
  runnerCount: intStringOrNull(raw.runnerCount),
  wakuban: raw.wakuban,
  umaban: padString(raw.umaban, 2),
  bamei: raw.bamei,
  jockeyName: raw.jockeyName,
  tanshoOdds: padString(raw.tanshoOddsTenth, 4),
  tanshoPopularity: padString(raw.tanshoPopularity, 2),
  finishPosition: raw.finishPosition,
  sohaTime: intStringOrNull(raw.sohaTime),
  corner1: padString(raw.corner1, 2),
  corner2: padString(raw.corner2, 2),
  corner3: padString(raw.corner3, 2),
  corner4: padString(raw.corner4, 2),
  bataiju: raw.bataijuInt === null ? null : String(raw.bataijuInt),
  zogenFugo: raw.zogenFugo,
  zogenSa: raw.zogenSaInt === null ? null : String(raw.zogenSaInt),
});

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

export const getRaceTrendDailyStarterRows = async (
  params: RaceTrendD1RowsParams,
): Promise<RaceTrendStarterRow[]> => {
  const cacheKey = buildDailyCacheKey(params);
  const cached = await getCachedDailyResponse(cacheKey);
  if (cached !== null) return cached;
  try {
    const raw = await queryDailyD1(params);
    const rows = raw.map(toDailyStarterRow);
    // Skip caching empty results: a D1 saturation event can surface as
    // `results: []` from .all() and we don't want to pin a poisoned empty
    // payload until the TTL expires.
    if (rows.length > 0) {
      await putDailyCache(cacheKey, rows);
    }
    return rows;
  } catch (error) {
    console.error("D1 daily trend query failed", error);
    return [];
  }
};
