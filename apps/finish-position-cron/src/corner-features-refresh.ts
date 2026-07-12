// Run with bun. Independent refresh of race_entry_corner_features against
// Neon directly, decoupled from the Win5-overlay pipeline that used to be
// its only writer.
//
// docs/cf-only-serving-architecture.md §4.4: race_entry_corner_features had
// no refresh path of its own -- it was populated only as a side effect of
// apps/pc-keiba-viewer/src/scripts/generate-win5-overlay.ts (JRA Win5 gate +
// a 14-day backward lookback + a local-PG-only write via build-corner-
// feature-table.ts, never pushed to Neon). That table is also the
// expected-entrant source in isFocusedFullPredictionComplete's own SQL
// (focused-full-completion.ts), so a NULL/stale row there degrades the
// completion check the coverage self-heal cron and the redelivery poll both
// depend on, independent of anything else in this reliability wave.
//
// This module ports build-corner-feature-table.ts's SQL verbatim (it turned
// out to be pure Postgres SQL against jvd_se/jvd_ra + nvd_se/nvd_ra -- no
// DuckDB, no local compute) so it can run as plain Neon HTTP queries from
// this Worker, with two changes: the date window is forward-looking (today
// through PREDICT_DAYS_AHEAD, matching what the day-base prewarm and
// coverage self-heal cron both need) instead of the Win5 job's 14-day
// backward lookback, and it always covers all three categories rather than
// only JRA. Every statement here is upsert-only (CREATE TABLE/INDEX IF NOT
// EXISTS, ALTER TABLE ADD COLUMN IF NOT EXISTS, INSERT ... ON CONFLICT DO
// UPDATE) -- no DELETE/TRUNCATE, matching this repo's no-data-delete
// convention and the safety already verified for the source script.
//
// Freshness dependency: jvd_se/nvd_se on Neon are themselves populated via a
// local-PG-to-Neon replica push, not written directly by JRA-VAN/NAR-VAN
// ingestion. If that push stalls (e.g. the 2026-07-08 jvd_se settle-feed
// incident), this refresh will read a Neon table that is behind the local
// source until the push recovers -- no special handling needed here: the
// next scheduled run after the push catches up will upsert the
// now-available rows on its own, since every statement above is
// idempotent/upsert-only.

import { neon } from "@neondatabase/serverless";
import type { Env } from "./types";

const YYYYMMDD_YEAR_END = 4;
const MS_PER_DAY = 24 * 60 * 60 * 1000;

interface RefreshCornerFeaturesParams {
  env: Env;
  runYmd: string;
  daysAhead: number;
}

const addDaysToYyyymmdd = (yyyymmdd: string, days: number): string => {
  const year = Number.parseInt(yyyymmdd.slice(0, YYYYMMDD_YEAR_END), 10);
  const month = Number.parseInt(yyyymmdd.slice(4, 6), 10) - 1;
  const day = Number.parseInt(yyyymmdd.slice(6, 8), 10);
  const shifted = new Date(Date.UTC(year, month, day) + days * MS_PER_DAY);
  const y = shifted.getUTCFullYear();
  const m = String(shifted.getUTCMonth() + 1).padStart(2, "0");
  const d = String(shifted.getUTCDate()).padStart(2, "0");
  return `${y}${m}${d}`;
};

const CORNER_FEATURES_TABLE_DDL = `
  create extension if not exists vector;

  create table if not exists race_entry_corner_features (
    source text not null,
    race_date text not null,
    kaisai_nen text not null,
    kaisai_tsukihi text not null,
    keibajo_code text not null,
    race_bango text not null,
    ketto_toroku_bango text not null,
    umaban integer not null,
    bamei text,
    track_code text,
    grade_code text,
    kyoso_shubetsu_code text,
    juryo_shubetsu_code text,
    kyoso_joken_code text,
    babajotai_code_shiba text,
    babajotai_code_dirt text,
    kyori integer,
    shusso_tosu integer,
    seibetsu_code text,
    barei integer,
    futan_juryo numeric,
    kishumei_ryakusho text,
    chokyoshimei_ryakusho text,
    banushimei text,
    finish_position integer,
    finish_norm numeric,
    tansho_ninkijun integer,
    tansho_odds numeric,
    soha_time integer,
    time_sa numeric,
    kohan_3f numeric,
    corner1_norm numeric,
    corner2_norm numeric,
    corner3_norm numeric,
    corner4_norm numeric,
    feature_vector vector(8) not null,
    updated_at timestamptz not null default now(),
    primary key (
      source,
      kaisai_nen,
      kaisai_tsukihi,
      keibajo_code,
      race_bango,
      ketto_toroku_bango
    )
  )
`;

const CORNER_FEATURES_ALTER_STATEMENTS: readonly string[] = [
  "alter table race_entry_corner_features add column if not exists grade_code text",
  "alter table race_entry_corner_features add column if not exists kyoso_shubetsu_code text",
  "alter table race_entry_corner_features add column if not exists juryo_shubetsu_code text",
  "alter table race_entry_corner_features add column if not exists kyoso_joken_code text",
  "alter table race_entry_corner_features add column if not exists babajotai_code_shiba text",
  "alter table race_entry_corner_features add column if not exists babajotai_code_dirt text",
  "alter table race_entry_corner_features add column if not exists seibetsu_code text",
  "alter table race_entry_corner_features add column if not exists barei integer",
  "alter table race_entry_corner_features add column if not exists futan_juryo numeric",
  "alter table race_entry_corner_features add column if not exists kishumei_ryakusho text",
  "alter table race_entry_corner_features add column if not exists chokyoshimei_ryakusho text",
  "alter table race_entry_corner_features add column if not exists banushimei text",
  "alter table race_entry_corner_features add column if not exists finish_position integer",
  "alter table race_entry_corner_features add column if not exists finish_norm numeric",
  "alter table race_entry_corner_features add column if not exists soha_time integer",
  "alter table race_entry_corner_features add column if not exists time_sa numeric",
  "alter table race_entry_corner_features add column if not exists kohan_3f numeric",
];

const ENTRY_COLUMNS = `
  source,
  kaisai_nen,
  kaisai_tsukihi,
  keibajo_code,
  race_bango,
  se.ketto_toroku_bango,
  se.umaban,
  se.bamei,
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
  se.corner_4
`;

const buildRaceDateFilterSql = (fromDate: string, toDate: string): string =>
  `and se.kaisai_nen || se.kaisai_tsukihi >= '${fromDate}'\n      and se.kaisai_nen || se.kaisai_tsukihi <= '${toDate}'`;

const buildJraSelectSql = (dateFilter: string): string => `
  select 'jra' source, ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango,
    ${ENTRY_COLUMNS}
  from jvd_se se
  join jvd_ra ra
    on ra.kaisai_nen = se.kaisai_nen
    and ra.kaisai_tsukihi = se.kaisai_tsukihi
    and ra.keibajo_code = se.keibajo_code
    and ra.race_bango = se.race_bango
  where
    se.ketto_toroku_bango is not null
    and btrim(se.ketto_toroku_bango) <> ''
    ${dateFilter}
`;

const buildNarSelectSql = (dateFilter: string): string => `
  select 'nar' source, ra.kaisai_nen, ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango,
    ${ENTRY_COLUMNS}
  from nvd_se se
  join nvd_ra ra
    on ra.kaisai_nen = se.kaisai_nen
    and ra.kaisai_tsukihi = se.kaisai_tsukihi
    and ra.keibajo_code = se.keibajo_code
    and ra.race_bango = se.race_bango
  where
    se.ketto_toroku_bango is not null
    and btrim(se.ketto_toroku_bango) <> ''
    ${dateFilter}
`;

// Normalization + upsert, verbatim from build-corner-feature-table.ts's
// buildSql (see that file for the full derivation of each *_norm / the
// 8-dimension feature_vector) -- only the raw_rows source selects differ
// (date-window-parameterized here instead of CLI-flag-parameterized there).
const buildCornerFeaturesUpsertSql = (fromDate: string, toDate: string): string => {
  const dateFilter = buildRaceDateFilterSql(fromDate, toDate);
  return `
    with raw_rows as (
      ${buildJraSelectSql(dateFilter)}
      union all
      ${buildNarSelectSql(dateFilter)}
    ),
    normalized_rows as (
      select
        source,
        kaisai_nen || kaisai_tsukihi race_date,
        kaisai_nen,
        kaisai_tsukihi,
        keibajo_code,
        race_bango,
        ketto_toroku_bango,
        case when umaban ~ '^[0-9]+$' then nullif(umaban, '')::integer else null end umaban,
        bamei,
        track_code,
        grade_code,
        kyoso_shubetsu_code,
        juryo_shubetsu_code,
        kyoso_joken_code,
        babajotai_code_shiba,
        babajotai_code_dirt,
        case when kyori ~ '^[0-9]+$' then nullif(kyori, '')::integer else null end kyori,
        case when shusso_tosu ~ '^[0-9]+$' then nullif(shusso_tosu, '00')::integer else null end shusso_tosu,
        seibetsu_code,
        case when barei ~ '^[0-9]+$' then nullif(barei, '00')::integer else null end barei,
        case when futan_juryo ~ '^[0-9]+$' then nullif(futan_juryo, '000')::numeric / 10 else null end futan_juryo,
        kishumei_ryakusho,
        chokyoshimei_ryakusho,
        banushimei,
        case when kakutei_chakujun ~ '^[0-9]+$' then nullif(kakutei_chakujun, '00')::integer else null end finish_position,
        case
          when shusso_tosu ~ '^[0-9]+$' and kakutei_chakujun ~ '^[0-9]+$' then
            case when nullif(kakutei_chakujun, '00') is not null and nullif(shusso_tosu, '00')::numeric > 1
              then (nullif(kakutei_chakujun, '00')::numeric - 1) / (nullif(shusso_tosu, '00')::numeric - 1)
              else null
            end
          else null
        end finish_norm,
        case when tansho_ninkijun ~ '^[0-9]+$' then nullif(tansho_ninkijun, '00')::integer else null end tansho_ninkijun,
        case when tansho_odds ~ '^[0-9]+$' then nullif(tansho_odds, '0000')::numeric / 10 else null end tansho_odds,
        case when soha_time ~ '^[0-9]+$' then nullif(soha_time, '0000')::integer else null end soha_time,
        case when time_sa ~ '^[0-9]+$' then nullif(time_sa, '0000')::numeric / 10 else null end time_sa,
        case when kohan_3f ~ '^[0-9]+$' then nullif(kohan_3f, '000')::numeric / 10 else null end kohan_3f,
        case
          when shusso_tosu ~ '^[0-9]+$' and corner_1 ~ '^[0-9]+$' then
            case when nullif(corner_1, '00') is not null and nullif(shusso_tosu, '00')::numeric > 1
              then (nullif(corner_1, '00')::numeric - 1) / (nullif(shusso_tosu, '00')::numeric - 1)
              else null
            end
          else null
        end corner1_norm,
        case
          when shusso_tosu ~ '^[0-9]+$' and corner_2 ~ '^[0-9]+$' then
            case when nullif(corner_2, '00') is not null and nullif(shusso_tosu, '00')::numeric > 1
              then (nullif(corner_2, '00')::numeric - 1) / (nullif(shusso_tosu, '00')::numeric - 1)
              else null
            end
          else null
        end corner2_norm,
        case
          when shusso_tosu ~ '^[0-9]+$' and corner_3 ~ '^[0-9]+$' then
            case when nullif(corner_3, '00') is not null and nullif(shusso_tosu, '00')::numeric > 1
              then (nullif(corner_3, '00')::numeric - 1) / (nullif(shusso_tosu, '00')::numeric - 1)
              else null
            end
          else null
        end corner3_norm,
        case
          when shusso_tosu ~ '^[0-9]+$' and corner_4 ~ '^[0-9]+$' then
            case when nullif(corner_4, '00') is not null and nullif(shusso_tosu, '00')::numeric > 1
              then (nullif(corner_4, '00')::numeric - 1) / (nullif(shusso_tosu, '00')::numeric - 1)
              else null
            end
          else null
        end corner4_norm
      from raw_rows
      where
        nullif(umaban, '') is not null
        and umaban ~ '^[0-9]+$'
        and nullif(kyori, '') is not null
        and kyori ~ '^[0-9]+$'
        and shusso_tosu ~ '^[0-9]+$'
        and keibajo_code ~ '^[0-9]+$'
        and race_bango ~ '^[0-9]+$'
    )
    insert into race_entry_corner_features (
      source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
      ketto_toroku_bango, umaban, bamei, track_code, grade_code, kyoso_shubetsu_code,
      juryo_shubetsu_code, kyoso_joken_code, babajotai_code_shiba, babajotai_code_dirt,
      kyori, shusso_tosu, seibetsu_code, barei, futan_juryo, kishumei_ryakusho,
      chokyoshimei_ryakusho, banushimei, finish_position, finish_norm, tansho_ninkijun,
      tansho_odds, soha_time, time_sa, kohan_3f, corner1_norm, corner2_norm,
      corner3_norm, corner4_norm, feature_vector, updated_at
    )
    select
      source, race_date, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
      ketto_toroku_bango, umaban, bamei, track_code, grade_code, kyoso_shubetsu_code,
      juryo_shubetsu_code, kyoso_joken_code, babajotai_code_shiba, babajotai_code_dirt,
      kyori, shusso_tosu, seibetsu_code, barei, futan_juryo, kishumei_ryakusho,
      chokyoshimei_ryakusho, banushimei, finish_position, finish_norm, tansho_ninkijun,
      tansho_odds, soha_time, time_sa, kohan_3f, corner1_norm, corner2_norm,
      corner3_norm, corner4_norm,
      array[
        least(1, greatest(0, coalesce(kyori, 0)::numeric / 3600)),
        least(1, greatest(0, coalesce(shusso_tosu, 0)::numeric / 18)),
        least(1, greatest(0, coalesce(umaban, 0)::numeric / greatest(coalesce(shusso_tosu, 1), 1))),
        least(1, greatest(0, coalesce(tansho_ninkijun, shusso_tosu, 0)::numeric / greatest(coalesce(shusso_tosu, 1), 1))),
        least(1, greatest(0, ln(greatest(coalesce(tansho_odds, 1), 1)) / ln(300))),
        case when left(coalesce(track_code, ''), 1) = '1' then 0 else 1 end,
        least(1, greatest(0, coalesce(case when keibajo_code ~ '^[0-9]+$' then nullif(keibajo_code, '')::numeric else null end, 0) / 99)),
        least(1, greatest(0, coalesce(case when race_bango ~ '^[0-9]+$' then nullif(race_bango, '')::numeric else null end, 0) / 12))
      ]::vector,
      now()
    from normalized_rows
    on conflict (
      source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango
    )
    do update set
      race_date = excluded.race_date,
      umaban = excluded.umaban,
      bamei = excluded.bamei,
      track_code = excluded.track_code,
      grade_code = excluded.grade_code,
      kyoso_shubetsu_code = excluded.kyoso_shubetsu_code,
      juryo_shubetsu_code = excluded.juryo_shubetsu_code,
      kyoso_joken_code = excluded.kyoso_joken_code,
      babajotai_code_shiba = excluded.babajotai_code_shiba,
      babajotai_code_dirt = excluded.babajotai_code_dirt,
      kyori = excluded.kyori,
      shusso_tosu = excluded.shusso_tosu,
      seibetsu_code = excluded.seibetsu_code,
      barei = excluded.barei,
      futan_juryo = excluded.futan_juryo,
      kishumei_ryakusho = excluded.kishumei_ryakusho,
      chokyoshimei_ryakusho = excluded.chokyoshimei_ryakusho,
      banushimei = excluded.banushimei,
      finish_position = excluded.finish_position,
      finish_norm = excluded.finish_norm,
      tansho_ninkijun = excluded.tansho_ninkijun,
      tansho_odds = excluded.tansho_odds,
      soha_time = excluded.soha_time,
      time_sa = excluded.time_sa,
      kohan_3f = excluded.kohan_3f,
      corner1_norm = excluded.corner1_norm,
      corner2_norm = excluded.corner2_norm,
      corner3_norm = excluded.corner3_norm,
      corner4_norm = excluded.corner4_norm,
      feature_vector = excluded.feature_vector,
      updated_at = now()
  `;
};

const CORNER_FEATURES_INDEX_STATEMENTS: readonly string[] = [
  `create index if not exists race_entry_corner_features_lookup_idx
     on race_entry_corner_features (source, race_date, track_code, kyori)`,
  `create index if not exists race_entry_corner_features_prefilter_idx
     on race_entry_corner_features (source, left(coalesce(track_code, ''), 1), kyori, race_date desc)`,
  `create index if not exists race_entry_corner_features_venue_prefilter_idx
     on race_entry_corner_features (source, left(coalesce(track_code, ''), 1), keibajo_code, kyori, race_date desc)`,
  `create index if not exists race_entry_corner_features_finish_prefilter_idx
     on race_entry_corner_features (source, race_date desc, left(coalesce(track_code, ''), 1), kyori, keibajo_code)
     where finish_norm is not null`,
  `create index if not exists race_entry_corner_features_horse_history_idx
     on race_entry_corner_features (source, ketto_toroku_bango, race_date desc)
     where finish_norm is not null`,
];

// Best-effort, same contract as day-base-prewarm.ts's own fix for the
// 2026-07-12 silent-failure incident: log an UNCONDITIONAL start line before
// any work (including the date-window computation) so a throw before the
// try block still leaves evidence in the logs, then wrap every fallible step
// in one try/catch so a failure anywhere in the statement sequence is caught
// and logged here, never propagated -- a stale/missing corner-features row
// degrades the completion check's accuracy (the status quo before this
// refresh existed) but must never block the day-base prewarm cron it runs
// alongside.
export const refreshCornerFeatures = async (params: RefreshCornerFeaturesParams): Promise<void> => {
  const { env, runYmd, daysAhead } = params;
  console.log(`[corner-features-refresh] start runYmd=${runYmd} daysAhead=${daysAhead}`);
  try {
    const toDate = addDaysToYyyymmdd(runYmd, daysAhead);
    const sql = neon(env.NEON_DATABASE_URL);
    await sql.query(CORNER_FEATURES_TABLE_DDL);
    for (const statement of CORNER_FEATURES_ALTER_STATEMENTS) {
      await sql.query(statement);
    }
    await sql.query(buildCornerFeaturesUpsertSql(runYmd, toDate));
    for (const statement of CORNER_FEATURES_INDEX_STATEMENTS) {
      await sql.query(statement);
    }
    console.log(`[corner-features-refresh] ok runYmd=${runYmd} toDate=${toDate}`);
  } catch (err) {
    console.error(
      `[corner-features-refresh] failed runYmd=${runYmd} daysAhead=${daysAhead}: ${String(err)}`,
    );
  }
};
