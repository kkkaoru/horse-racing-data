// Run with bun. Reads aggregated trend rows from D1 snapshot tables
// (race_result_snapshots / race_entry_snapshots / horse_weight_snapshots /
// realtime_race_sources). Used as a complementary cache to the Neon trend
// query in apps/pc-keiba-viewer/src/db/queries.ts.
import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import type { RaceSource } from "../lib/codes";
import type { RaceTrendStarterRow } from "../lib/race-types";

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
  umaban: string | null;
  bamei: string | null;
  jockeyName: string | null;
  finishPosition: number;
  sohaTime: string | null;
  bataijuInt: number | null;
  zogenFugo: string | null;
  zogenSaInt: number | null;
}

const CACHE_URL_BASE = "https://pc-keiba-viewer.local/d1-trend-cache/";
const CACHE_TTL_SECONDS = 60;
const KV_TTL_SECONDS = 5 * 60;

const isRaceSource = (value: unknown): value is RaceSource =>
  value === "jra" || value === "nar";

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
  `race-trend-d1:v1:${source}:${startYmd}:${endYmd}`;

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
    select race_key, horse_number, jockey_name
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
    s.kaisai_nen as kaisaiNen,
    s.kaisai_tsukihi as kaisaiTsukihi,
    s.keibajo_code as keibajoCode,
    s.race_bango as raceBango,
    s.race_name as raceName,
    s.race_start_at_jst as hassoJikoku,
    r.horse_number as umaban,
    e.jockey_name as jockeyName,
    cast(nullif(replace(r.finish_position, ' ', ''), '') as integer) as finishPosition,
    r.time as sohaTime,
    w.weight as bataijuInt,
    w.change_sign as zogenFugo,
    w.change_amount as zogenSaInt,
    null as bamei
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

const toStarterRow = (raw: RawD1Row): RaceTrendStarterRow => ({
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
  tanshoOdds: null,
  tanshoPopularity: null,
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

const getCachedResponse = async (
  cacheKey: string,
): Promise<RaceTrendStarterRow[] | null> => {
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

const putCache = async (
  cacheKey: string,
  rows: RaceTrendStarterRow[],
): Promise<void> => {
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
    await putCache(cacheKey, rows);
    return rows;
  } catch (error) {
    console.error("D1 trend query failed", error);
    return [];
  }
};
