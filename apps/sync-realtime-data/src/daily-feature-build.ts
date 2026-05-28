// Run with bun. Worker-side daily feature builder. Reads raw race entries from
// Neon (jvd_se/jvd_ra/nvd_se/nvd_ra) via Hyperdrive, normalises the same way
// apps/pc-keiba-viewer/src/scripts/build-corner-feature-table.ts does, then
// writes the per-day per-horse rows into D1 `daily_race_entries`. Downstream
// inference reads daily entries from D1 going forward.

import type { Pool } from "pg";

import { getFinishPositionPool } from "./finish-position-lite-pool";
import { formatError } from "./format-error";
import { getTodayJst } from "./time";
import type { Env } from "./types";

// Skip daily-feature-build when the youngest D1 row for the requested
// window was upserted within this many milliseconds. Today's data races
// finish throughout the day so we still want a re-check after the gap,
// but back-to-back hourly cron ticks against unchanged data become
// no-op SELECT max(updated_at) instead of full Neon re-queries.
const SKIP_IF_FRESH_MILLIS = 60 * 60 * 1000;

export type DailyFeatureBuildSourceScope = "all" | "ban-ei" | "jra" | "nar";

export interface DailyFeatureBuildOptions {
  // Force the upsert pipeline to run even if the freshness guard would
  // otherwise short-circuit (used by the backfill CLI and manual ops).
  forceRefresh?: boolean;
  fromDate: string;
  sourceScope?: DailyFeatureBuildSourceScope;
  toDate?: string;
}

export interface DailyFeatureBuildSkipReason {
  kind: "past-date-already-populated" | "today-recently-refreshed";
  latestUpdatedAt?: string;
  rowCount?: number;
}

export interface DailyRaceEntryRow {
  source: "jra" | "nar";
  race_date: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  wakuban: string | null;
  umaban: number | null;
  bamei: string | null;
  race_name: string | null;
  hasso_jikoku: string | null;
  track_code: string | null;
  grade_code: string | null;
  kyoso_shubetsu_code: string | null;
  juryo_shubetsu_code: string | null;
  kyoso_joken_code: string | null;
  babajotai_code_shiba: string | null;
  babajotai_code_dirt: string | null;
  kyori: number | null;
  shusso_tosu: number | null;
  seibetsu_code: string | null;
  barei: number | null;
  futan_juryo: number | null;
  kishumei_ryakusho: string | null;
  chokyoshimei_ryakusho: string | null;
  banushimei: string | null;
  finish_position: number | null;
  finish_norm: number | null;
  tansho_ninkijun: number | null;
  tansho_odds: number | null;
  soha_time: number | null;
  time_sa: number | null;
  kohan_3f: number | null;
  corner1_norm: number | null;
  corner2_norm: number | null;
  corner3_norm: number | null;
  corner4_norm: number | null;
  corner_1: number | null;
  corner_2: number | null;
  corner_3: number | null;
  corner_4: number | null;
  bataiju: number | null;
  zogen_fugo: string | null;
  zogen_sa: number | null;
}

export interface DailyFeatureBuildResult {
  cacheWarm?: CacheWarmOutcome;
  fromDate: string;
  rowsFetched: number;
  rowsWritten: number;
  sourceScope: DailyFeatureBuildSourceScope;
  toDate: string;
}

export interface CacheWarmOutcome {
  raceCount?: number;
  status: "ok" | "error" | "skipped";
  warmed?: number;
  message?: string;
}

const DEFAULT_VIEWER_CACHE_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const CACHE_WARM_PATH = "/api/cache-warm/race-detail-ssr";
const CACHE_WARM_HEADER = "X-PC-Keiba-Cache-Warm" satisfies string;
const CACHE_WARM_HEADER_VALUE = "scheduled";

const DEFAULT_SCOPE = "all" satisfies DailyFeatureBuildSourceScope;
const YYYYMMDD_PATTERN = /^\d{8}$/u;
const D1_INSERT_BATCH_SIZE = 50;

// Runs hourly between JST 09:00 and 22:00 so that any Neon nvd_se sync the
// operator triggers ends up covered without manual feature builds. Each tick
// performs an UPSERT against D1 so unchanged rows are no-op.
export const DAILY_FEATURE_BUILD_CRON = "0 0-13 * * *";

const requireYYYYMMDD = (value: string, label: string): string => {
  if (!YYYYMMDD_PATTERN.test(value)) {
    throw new Error(`${label} must match YYYYMMDD: ${value}`);
  }
  return value;
};

const buildJraSelectSql = (fromDate: string, toDate: string): string => `
  select
    'jra' source,
    ra.kaisai_nen,
    ra.kaisai_tsukihi,
    ra.keibajo_code,
    ra.race_bango,
    se.ketto_toroku_bango,
    se.wakuban,
    se.umaban,
    se.bamei,
    ra.kyosomei_hondai,
    ra.kyosomei_fukudai,
    ra.hasso_jikoku,
    ra.track_code,
    ra.grade_code,
    ra.kyoso_shubetsu_code,
    ra.juryo_shubetsu_code,
    ra.kyoso_joken_code,
    ra.babajotai_code_shiba,
    ra.babajotai_code_dirt,
    ra.kyori,
    ra.shusso_tosu,
    se.seibetsu_code,
    se.barei,
    se.futan_juryo,
    se.kishumei_ryakusho,
    se.chokyoshimei_ryakusho,
    se.banushimei,
    se.kakutei_chakujun,
    se.tansho_ninkijun,
    se.tansho_odds,
    se.soha_time,
    se.time_sa,
    se.kohan_3f,
    se.corner_1,
    se.corner_2,
    se.corner_3,
    se.corner_4,
    se.bataiju,
    se.zogen_fugo,
    se.zogen_sa
  from jvd_se se
  join jvd_ra ra
    on ra.kaisai_nen = se.kaisai_nen
    and ra.kaisai_tsukihi = se.kaisai_tsukihi
    and ra.keibajo_code = se.keibajo_code
    and ra.race_bango = se.race_bango
  where
    se.ketto_toroku_bango is not null
    and btrim(se.ketto_toroku_bango) <> ''
    and se.kaisai_nen || se.kaisai_tsukihi >= '${fromDate}'
    and se.kaisai_nen || se.kaisai_tsukihi <= '${toDate}'
`;

const banEiFilterFor = (sourceScope: DailyFeatureBuildSourceScope): string => {
  if (sourceScope === "ban-ei") return "and ra.keibajo_code = '83'";
  if (sourceScope === "nar") return "and ra.keibajo_code <> '83'";
  return "";
};

const buildNarSelectSql = (
  fromDate: string,
  toDate: string,
  sourceScope: DailyFeatureBuildSourceScope,
): string => `
  select
    'nar' source,
    ra.kaisai_nen,
    ra.kaisai_tsukihi,
    ra.keibajo_code,
    ra.race_bango,
    se.ketto_toroku_bango,
    se.wakuban,
    se.umaban,
    se.bamei,
    ra.kyosomei_hondai,
    ra.kyosomei_fukudai,
    ra.hasso_jikoku,
    ra.track_code,
    ra.grade_code,
    ra.kyoso_shubetsu_code,
    ra.juryo_shubetsu_code,
    ra.kyoso_joken_code,
    ra.babajotai_code_shiba,
    ra.babajotai_code_dirt,
    ra.kyori,
    ra.shusso_tosu,
    se.seibetsu_code,
    se.barei,
    se.futan_juryo,
    se.kishumei_ryakusho,
    se.chokyoshimei_ryakusho,
    se.banushimei,
    se.kakutei_chakujun,
    se.tansho_ninkijun,
    se.tansho_odds,
    se.soha_time,
    se.time_sa,
    se.kohan_3f,
    se.corner_1,
    se.corner_2,
    se.corner_3,
    se.corner_4,
    se.bataiju,
    se.zogen_fugo,
    se.zogen_sa
  from nvd_se se
  join nvd_ra ra
    on ra.kaisai_nen = se.kaisai_nen
    and ra.kaisai_tsukihi = se.kaisai_tsukihi
    and ra.keibajo_code = se.keibajo_code
    and ra.race_bango = se.race_bango
  where
    se.ketto_toroku_bango is not null
    and btrim(se.ketto_toroku_bango) <> ''
    and se.kaisai_nen || se.kaisai_tsukihi >= '${fromDate}'
    and se.kaisai_nen || se.kaisai_tsukihi <= '${toDate}'
    ${banEiFilterFor(sourceScope)}
`;

export const buildDailyFeatureSelectSql = (options: DailyFeatureBuildOptions): string => {
  const fromDate = requireYYYYMMDD(options.fromDate, "fromDate");
  const toDate = requireYYYYMMDD(options.toDate ?? options.fromDate, "toDate");
  const sourceScope = options.sourceScope ?? DEFAULT_SCOPE;
  const includeJra = sourceScope === "all" || sourceScope === "jra";
  const includeNar = sourceScope === "all" || sourceScope === "nar" || sourceScope === "ban-ei";
  const selects: string[] = [];
  if (includeJra) {
    selects.push(buildJraSelectSql(fromDate, toDate));
  }
  if (includeNar) {
    selects.push(buildNarSelectSql(fromDate, toDate, sourceScope));
  }
  if (selects.length === 0) {
    throw new Error(`No source selects for scope: ${sourceScope}`);
  }
  return `
    with raw_rows as (
      ${selects.join("\n      union all\n")}
    )
    select
      source,
      kaisai_nen || kaisai_tsukihi as race_date,
      kaisai_nen,
      kaisai_tsukihi,
      lpad(keibajo_code::text, 2, '0') as keibajo_code,
      lpad(race_bango::text, 2, '0') as race_bango,
      ketto_toroku_bango,
      nullif(btrim(coalesce(wakuban, '')), '') as wakuban,
      case when umaban ~ '^[0-9]+$' then nullif(umaban, '')::integer else null end as umaban,
      bamei,
      coalesce(
        nullif(regexp_replace(coalesce(kyosomei_hondai, ''), '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
        nullif(regexp_replace(coalesce(kyosomei_fukudai, ''), '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
        '一般競走'
      ) as race_name,
      nullif(btrim(coalesce(hasso_jikoku, '')), '') as hasso_jikoku,
      track_code,
      grade_code,
      kyoso_shubetsu_code,
      juryo_shubetsu_code,
      kyoso_joken_code,
      babajotai_code_shiba,
      babajotai_code_dirt,
      case when kyori ~ '^[0-9]+$' then nullif(kyori, '')::integer else null end as kyori,
      case when shusso_tosu ~ '^[0-9]+$' then nullif(shusso_tosu, '00')::integer else null end as shusso_tosu,
      seibetsu_code,
      case when barei ~ '^[0-9]+$' then nullif(barei, '00')::integer else null end as barei,
      case when futan_juryo ~ '^[0-9]+$' then nullif(futan_juryo, '000')::numeric / 10 else null end as futan_juryo,
      kishumei_ryakusho,
      chokyoshimei_ryakusho,
      banushimei,
      case when kakutei_chakujun ~ '^[0-9]+$' then nullif(kakutei_chakujun, '00')::integer else null end as finish_position,
      case
        when shusso_tosu ~ '^[0-9]+$' and kakutei_chakujun ~ '^[0-9]+$' then
          case when nullif(kakutei_chakujun, '00') is not null and nullif(shusso_tosu, '00')::numeric > 1
            then (nullif(kakutei_chakujun, '00')::numeric - 1) / (nullif(shusso_tosu, '00')::numeric - 1)
            else null
          end
        else null
      end as finish_norm,
      case when tansho_ninkijun ~ '^[0-9]+$' then nullif(tansho_ninkijun, '00')::integer else null end as tansho_ninkijun,
      case when tansho_odds ~ '^[0-9]+$' then nullif(tansho_odds, '0000')::numeric / 10 else null end as tansho_odds,
      case when soha_time ~ '^[0-9]+$' then nullif(soha_time, '0000')::integer else null end as soha_time,
      case when time_sa ~ '^[0-9]+$' then nullif(time_sa, '0000')::numeric / 10 else null end as time_sa,
      case when kohan_3f ~ '^[0-9]+$' then nullif(kohan_3f, '000')::numeric / 10 else null end as kohan_3f,
      case
        when shusso_tosu ~ '^[0-9]+$' and corner_1 ~ '^[0-9]+$' then
          case when nullif(corner_1, '00') is not null and nullif(shusso_tosu, '00')::numeric > 1
            then (nullif(corner_1, '00')::numeric - 1) / (nullif(shusso_tosu, '00')::numeric - 1)
            else null
          end
        else null
      end as corner1_norm,
      case
        when shusso_tosu ~ '^[0-9]+$' and corner_2 ~ '^[0-9]+$' then
          case when nullif(corner_2, '00') is not null and nullif(shusso_tosu, '00')::numeric > 1
            then (nullif(corner_2, '00')::numeric - 1) / (nullif(shusso_tosu, '00')::numeric - 1)
            else null
          end
        else null
      end as corner2_norm,
      case
        when shusso_tosu ~ '^[0-9]+$' and corner_3 ~ '^[0-9]+$' then
          case when nullif(corner_3, '00') is not null and nullif(shusso_tosu, '00')::numeric > 1
            then (nullif(corner_3, '00')::numeric - 1) / (nullif(shusso_tosu, '00')::numeric - 1)
            else null
          end
        else null
      end as corner3_norm,
      case
        when shusso_tosu ~ '^[0-9]+$' and corner_4 ~ '^[0-9]+$' then
          case when nullif(corner_4, '00') is not null and nullif(shusso_tosu, '00')::numeric > 1
            then (nullif(corner_4, '00')::numeric - 1) / (nullif(shusso_tosu, '00')::numeric - 1)
            else null
          end
        else null
      end as corner4_norm,
      case when corner_1 ~ '^[0-9]+$' then nullif(corner_1, '00')::integer else null end as corner_1,
      case when corner_2 ~ '^[0-9]+$' then nullif(corner_2, '00')::integer else null end as corner_2,
      case when corner_3 ~ '^[0-9]+$' then nullif(corner_3, '00')::integer else null end as corner_3,
      case when corner_4 ~ '^[0-9]+$' then nullif(corner_4, '00')::integer else null end as corner_4,
      case when bataiju ~ '^[0-9]+$' then nullif(bataiju, '000')::integer else null end as bataiju,
      nullif(btrim(coalesce(zogen_fugo, '')), '') as zogen_fugo,
      case when zogen_sa ~ '^[0-9]+$' then nullif(zogen_sa, '000')::integer else null end as zogen_sa
    from raw_rows
    where
      nullif(umaban, '') is not null
      and umaban ~ '^[0-9]+$'
      and nullif(kyori, '') is not null
      and kyori ~ '^[0-9]+$'
      and shusso_tosu ~ '^[0-9]+$'
      and keibajo_code ~ '^[0-9]+$'
      and race_bango ~ '^[0-9]+$'
  `;
};

const buildRaceKey = (row: DailyRaceEntryRow): string =>
  `${row.source}:${row.race_date}:${row.keibajo_code}:${row.race_bango}`;

const numericOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const normaliseRow = (raw: Record<string, unknown>): DailyRaceEntryRow => {
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion
  const source = raw.source as "jra" | "nar";
  return {
    babajotai_code_dirt: raw.babajotai_code_dirt as string | null,
    babajotai_code_shiba: raw.babajotai_code_shiba as string | null,
    bamei: raw.bamei as string | null,
    banushimei: raw.banushimei as string | null,
    barei: numericOrNull(raw.barei),
    bataiju: numericOrNull(raw.bataiju),
    chokyoshimei_ryakusho: raw.chokyoshimei_ryakusho as string | null,
    corner1_norm: numericOrNull(raw.corner1_norm),
    corner2_norm: numericOrNull(raw.corner2_norm),
    corner3_norm: numericOrNull(raw.corner3_norm),
    corner4_norm: numericOrNull(raw.corner4_norm),
    corner_1: numericOrNull(raw.corner_1),
    corner_2: numericOrNull(raw.corner_2),
    corner_3: numericOrNull(raw.corner_3),
    corner_4: numericOrNull(raw.corner_4),
    finish_norm: numericOrNull(raw.finish_norm),
    finish_position: numericOrNull(raw.finish_position),
    futan_juryo: numericOrNull(raw.futan_juryo),
    grade_code: raw.grade_code as string | null,
    hasso_jikoku: raw.hasso_jikoku as string | null,
    juryo_shubetsu_code: raw.juryo_shubetsu_code as string | null,
    kaisai_nen: String(raw.kaisai_nen),
    kaisai_tsukihi: String(raw.kaisai_tsukihi),
    keibajo_code: String(raw.keibajo_code),
    ketto_toroku_bango: String(raw.ketto_toroku_bango),
    kishumei_ryakusho: raw.kishumei_ryakusho as string | null,
    kohan_3f: numericOrNull(raw.kohan_3f),
    kyori: numericOrNull(raw.kyori),
    kyoso_joken_code: raw.kyoso_joken_code as string | null,
    kyoso_shubetsu_code: raw.kyoso_shubetsu_code as string | null,
    race_bango: String(raw.race_bango),
    race_date: String(raw.race_date),
    race_name: raw.race_name as string | null,
    seibetsu_code: raw.seibetsu_code as string | null,
    shusso_tosu: numericOrNull(raw.shusso_tosu),
    soha_time: numericOrNull(raw.soha_time),
    source,
    tansho_ninkijun: numericOrNull(raw.tansho_ninkijun),
    tansho_odds: numericOrNull(raw.tansho_odds),
    time_sa: numericOrNull(raw.time_sa),
    track_code: raw.track_code as string | null,
    umaban: numericOrNull(raw.umaban),
    wakuban: raw.wakuban as string | null,
    zogen_fugo: raw.zogen_fugo as string | null,
    zogen_sa: numericOrNull(raw.zogen_sa),
  };
};

export const fetchDailyRaceEntriesFromPostgres = async (
  pool: Pool,
  options: DailyFeatureBuildOptions,
): Promise<DailyRaceEntryRow[]> => {
  const sql = buildDailyFeatureSelectSql(options);
  const result = await pool.query<Record<string, unknown>>(sql);
  return result.rows.map(normaliseRow);
};

const INSERT_SQL = `INSERT OR REPLACE INTO daily_race_entries (
  race_key, race_date, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
  ketto_toroku_bango, umaban, bamei, track_code, grade_code, kyoso_shubetsu_code,
  juryo_shubetsu_code, kyoso_joken_code, babajotai_code_shiba, babajotai_code_dirt,
  kyori, shusso_tosu, seibetsu_code, barei, futan_juryo, kishumei_ryakusho,
  chokyoshimei_ryakusho, banushimei, finish_position, finish_norm, tansho_ninkijun,
  tansho_odds, soha_time, time_sa, kohan_3f, corner1_norm, corner2_norm,
  corner3_norm, corner4_norm,
  wakuban, race_name, hasso_jikoku, corner_1, corner_2, corner_3, corner_4,
  bataiju, zogen_fugo, zogen_sa,
  updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

const buildBindParams = (row: DailyRaceEntryRow, updatedAt: string): unknown[] => [
  buildRaceKey(row),
  row.race_date,
  row.source,
  row.kaisai_nen,
  row.kaisai_tsukihi,
  row.keibajo_code,
  row.race_bango,
  row.ketto_toroku_bango,
  row.umaban,
  row.bamei,
  row.track_code,
  row.grade_code,
  row.kyoso_shubetsu_code,
  row.juryo_shubetsu_code,
  row.kyoso_joken_code,
  row.babajotai_code_shiba,
  row.babajotai_code_dirt,
  row.kyori,
  row.shusso_tosu,
  row.seibetsu_code,
  row.barei,
  row.futan_juryo,
  row.kishumei_ryakusho,
  row.chokyoshimei_ryakusho,
  row.banushimei,
  row.finish_position,
  row.finish_norm,
  row.tansho_ninkijun,
  row.tansho_odds,
  row.soha_time,
  row.time_sa,
  row.kohan_3f,
  row.corner1_norm,
  row.corner2_norm,
  row.corner3_norm,
  row.corner4_norm,
  row.wakuban,
  row.race_name,
  row.hasso_jikoku,
  row.corner_1,
  row.corner_2,
  row.corner_3,
  row.corner_4,
  row.bataiju,
  row.zogen_fugo,
  row.zogen_sa,
  updatedAt,
];

export const upsertDailyRaceEntriesToD1 = async (
  db: D1Database,
  rows: ReadonlyArray<DailyRaceEntryRow>,
  now: Date = new Date(),
): Promise<number> => {
  if (rows.length === 0) return 0;
  const updatedAt = now.toISOString();
  const prepared = db.prepare(INSERT_SQL);
  let written = 0;
  for (let index = 0; index < rows.length; index += D1_INSERT_BATCH_SIZE) {
    const chunk = rows.slice(index, index + D1_INSERT_BATCH_SIZE);
    const statements = chunk.map((row) => prepared.bind(...buildBindParams(row, updatedAt)));
    await db.batch(statements);
    written += chunk.length;
  }
  return written;
};

export interface DailyRaceEntryQueryParams {
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

const LIST_DAILY_ENTRIES_SQL = `select
  race_key, race_date, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
  ketto_toroku_bango, umaban, bamei, track_code, grade_code, kyoso_shubetsu_code,
  juryo_shubetsu_code, kyoso_joken_code, babajotai_code_shiba, babajotai_code_dirt,
  kyori, shusso_tosu, seibetsu_code, barei, futan_juryo, kishumei_ryakusho,
  chokyoshimei_ryakusho, banushimei, finish_position, finish_norm, tansho_ninkijun,
  tansho_odds, soha_time, time_sa, kohan_3f, corner1_norm, corner2_norm,
  corner3_norm, corner4_norm,
  wakuban, race_name, hasso_jikoku, corner_1, corner_2, corner_3, corner_4,
  bataiju, zogen_fugo, zogen_sa
from daily_race_entries
where race_key = ?
order by umaban`;

const numericFromRaw = (value: unknown): number | null => numericOrNull(value);

const stringFromRaw = (value: unknown): string | null =>
  value === null || value === undefined ? null : String(value);

const requireSource = (value: unknown): "jra" | "nar" => {
  if (value === "jra" || value === "nar") return value;
  throw new Error(`unexpected source value: ${String(value)}`);
};

const mapDailyEntryRow = (raw: Record<string, unknown>): DailyRaceEntryRow => ({
  babajotai_code_dirt: stringFromRaw(raw.babajotai_code_dirt),
  babajotai_code_shiba: stringFromRaw(raw.babajotai_code_shiba),
  bamei: stringFromRaw(raw.bamei),
  banushimei: stringFromRaw(raw.banushimei),
  barei: numericFromRaw(raw.barei),
  bataiju: numericFromRaw(raw.bataiju),
  chokyoshimei_ryakusho: stringFromRaw(raw.chokyoshimei_ryakusho),
  corner1_norm: numericFromRaw(raw.corner1_norm),
  corner2_norm: numericFromRaw(raw.corner2_norm),
  corner3_norm: numericFromRaw(raw.corner3_norm),
  corner4_norm: numericFromRaw(raw.corner4_norm),
  corner_1: numericFromRaw(raw.corner_1),
  corner_2: numericFromRaw(raw.corner_2),
  corner_3: numericFromRaw(raw.corner_3),
  corner_4: numericFromRaw(raw.corner_4),
  finish_norm: numericFromRaw(raw.finish_norm),
  finish_position: numericFromRaw(raw.finish_position),
  futan_juryo: numericFromRaw(raw.futan_juryo),
  grade_code: stringFromRaw(raw.grade_code),
  hasso_jikoku: stringFromRaw(raw.hasso_jikoku),
  juryo_shubetsu_code: stringFromRaw(raw.juryo_shubetsu_code),
  kaisai_nen: String(raw.kaisai_nen),
  kaisai_tsukihi: String(raw.kaisai_tsukihi),
  keibajo_code: String(raw.keibajo_code),
  ketto_toroku_bango: String(raw.ketto_toroku_bango),
  kishumei_ryakusho: stringFromRaw(raw.kishumei_ryakusho),
  kohan_3f: numericFromRaw(raw.kohan_3f),
  kyori: numericFromRaw(raw.kyori),
  kyoso_joken_code: stringFromRaw(raw.kyoso_joken_code),
  kyoso_shubetsu_code: stringFromRaw(raw.kyoso_shubetsu_code),
  race_bango: String(raw.race_bango),
  race_date: String(raw.race_date),
  race_name: stringFromRaw(raw.race_name),
  seibetsu_code: stringFromRaw(raw.seibetsu_code),
  shusso_tosu: numericFromRaw(raw.shusso_tosu),
  soha_time: numericFromRaw(raw.soha_time),
  source: requireSource(raw.source),
  tansho_ninkijun: numericFromRaw(raw.tansho_ninkijun),
  tansho_odds: numericFromRaw(raw.tansho_odds),
  time_sa: numericFromRaw(raw.time_sa),
  track_code: stringFromRaw(raw.track_code),
  umaban: numericFromRaw(raw.umaban),
  wakuban: stringFromRaw(raw.wakuban),
  zogen_fugo: stringFromRaw(raw.zogen_fugo),
  zogen_sa: numericFromRaw(raw.zogen_sa),
});

const buildDailyEntryRaceKey = (params: DailyRaceEntryQueryParams): string =>
  `${params.source}:${params.kaisaiNen}${params.kaisaiTsukihi}:${params.keibajoCode.padStart(2, "0")}:${params.raceBango.padStart(2, "0")}`;

export const listDailyRaceEntriesForRace = async (
  db: D1Database,
  params: DailyRaceEntryQueryParams,
): Promise<DailyRaceEntryRow[]> => {
  const result = await db
    .prepare(LIST_DAILY_ENTRIES_SQL)
    .bind(buildDailyEntryRaceKey(params))
    .all<Record<string, unknown>>();
  return result.results.map(mapDailyEntryRow);
};

const formatIsoDateFromYyyymmdd = (yyyymmdd: string): string =>
  `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}`;

const resolveViewerCacheOrigin = (env: Env): string => {
  const configured = env.RUNNING_STYLE_CACHE_ORIGIN?.trim();
  return configured && configured.length > 0 ? configured : DEFAULT_VIEWER_CACHE_ORIGIN;
};

const parseWarmResponse = (text: string): { raceCount?: number; warmed?: number } => {
  try {
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion
    return JSON.parse(text) as { raceCount?: number; warmed?: number };
  } catch {
    return {};
  }
};

export const triggerViewerCacheWarmForDate = async (
  env: Env,
  date: string,
): Promise<CacheWarmOutcome> => {
  const isoDate = formatIsoDateFromYyyymmdd(date);
  const origin = resolveViewerCacheOrigin(env);
  const url = `${origin}${CACHE_WARM_PATH}?date=${isoDate}`;
  const request = new Request(url, {
    headers: { [CACHE_WARM_HEADER]: CACHE_WARM_HEADER_VALUE },
    method: "POST",
  });
  try {
    const response = await fetch(request);
    if (!response.ok) {
      return { message: `HTTP ${response.status}`, status: "error" };
    }
    const text = await response.text();
    const payload = parseWarmResponse(text);
    return { raceCount: payload.raceCount, status: "ok", warmed: payload.warmed };
  } catch (error) {
    return {
      message: formatError(error),
      status: "error",
    };
  }
};

export interface DailyFeatureBuildFreshnessProbe {
  latestUpdatedAt: string | null;
  rowCount: number;
}

const probeDailyRaceEntriesFreshness = async (
  db: D1Database,
  fromDate: string,
  toDate: string,
): Promise<DailyFeatureBuildFreshnessProbe> => {
  const row = await db
    .prepare(
      `select count(*) as row_count, max(updated_at) as latest_updated_at
       from daily_race_entries
       where race_date between ? and ?`,
    )
    .bind(fromDate, toDate)
    .first<{ latest_updated_at: string | null; row_count: number }>();
  return {
    latestUpdatedAt: row?.latest_updated_at ?? null,
    rowCount: row?.row_count ?? 0,
  };
};

export interface ShouldSkipDailyFeatureBuildInput {
  fromDate: string;
  now: Date;
  probe: DailyFeatureBuildFreshnessProbe;
  toDate: string;
}

export const shouldSkipDailyFeatureBuild = ({
  fromDate,
  now,
  probe,
  toDate,
}: ShouldSkipDailyFeatureBuildInput): DailyFeatureBuildSkipReason | null => {
  if (probe.rowCount <= 0) return null;
  const todayYmd = getTodayJst(now);
  // Date ranges that end before today are immutable once populated — racing
  // results don't retroactively change, so any existing row count means we
  // can skip the Neon → D1 re-upsert entirely.
  const targetsPastOnly = toDate < todayYmd;
  if (targetsPastOnly) {
    return { kind: "past-date-already-populated", rowCount: probe.rowCount };
  }
  // Ranges that overlap today: only skip when a recent upsert proves the
  // mirror is fresh. Older snapshots fall through and re-run.
  if (!probe.latestUpdatedAt) return null;
  const latestMs = Date.parse(probe.latestUpdatedAt);
  if (!Number.isFinite(latestMs)) return null;
  if (now.getTime() - latestMs >= SKIP_IF_FRESH_MILLIS) return null;
  // Guard against runs where toDate has been pulled in from a future day —
  // those windows can legitimately have no Neon source yet, so we still
  // re-run unless the existing rows cover the whole requested span.
  if (fromDate > todayYmd) return null;
  return {
    kind: "today-recently-refreshed",
    latestUpdatedAt: probe.latestUpdatedAt,
    rowCount: probe.rowCount,
  };
};

export const runDailyFeatureBuildForEnv = async (
  env: Env,
  options: DailyFeatureBuildOptions,
): Promise<DailyFeatureBuildResult> => {
  const sourceScope = options.sourceScope ?? DEFAULT_SCOPE;
  const fromDate = requireYYYYMMDD(options.fromDate, "fromDate");
  const toDate = requireYYYYMMDD(options.toDate ?? options.fromDate, "toDate");
  if (options.forceRefresh !== true) {
    const probe = await probeDailyRaceEntriesFreshness(env.REALTIME_DB, fromDate, toDate);
    const skipReason = shouldSkipDailyFeatureBuild({ fromDate, now: new Date(), probe, toDate });
    if (skipReason !== null) {
      return {
        cacheWarm: {
          message: `daily_race_entries already populated (${skipReason.kind})`,
          status: "skipped" as const,
        },
        fromDate,
        rowsFetched: 0,
        rowsWritten: 0,
        sourceScope,
        toDate,
      };
    }
  }
  const pool = getFinishPositionPool(env);
  const rows = await fetchDailyRaceEntriesFromPostgres(pool, { fromDate, sourceScope, toDate });
  const rowsWritten = await upsertDailyRaceEntriesToD1(env.REALTIME_DB, rows);
  const cacheWarm =
    rowsWritten > 0
      ? await triggerViewerCacheWarmForDate(env, fromDate)
      : {
          message: "no rows written",
          status: "skipped" as const,
        };
  return { cacheWarm, fromDate, rowsFetched: rows.length, rowsWritten, sourceScope, toDate };
};
