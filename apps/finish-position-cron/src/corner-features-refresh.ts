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

import { neon, type NeonQueryFunction } from "@neondatabase/serverless";
import type { Env } from "./types";

const YYYYMMDD_YEAR_END = 4;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const NO_LOOKBACK_DAYS = 0;

interface RefreshCornerFeaturesParams {
  env: Env;
  runYmd: string;
  daysAhead: number;
  // Optional backward extension of the upsert's date window, so a run that
  // fires after the primary window (see CORNER_FEATURES_REFRESH_CRON_MORNING/
  // _EVENING below) also re-touches the last N days. This is the self-heal
  // mechanism for the 2026-07-17 finding (docs/probes/
  // corner-features-settlement-backfill-heal-2026-07-17.md): a day whose
  // settlement columns were still NULL when this cron last visited it (e.g.
  // this cron was never wired at all before, or a single day's run failed)
  // gets swept up again by any later run within lookbackDays, instead of
  // being permanently stuck once it ages out of [runYmd, runYmd+daysAhead].
  // Absent/0 preserves the original forward-only window (fromDate = runYmd).
  lookbackDays?: number;
}

// JST 09:15, 5 min before the existing JRA pre-wake (WARM_CRON_PRE_JRA,
// "25 0 * * *") and feature-build (FEATURE_BUILD_CRON, "30 0 * * *") crons in
// cron-decision.ts, so today's pre-race entrant rows exist before those
// crons' work reads them. Named/scoped locally (not added to
// cron-decision.ts) mirroring coverage-self-heal.ts's own
// COVERAGE_SELF_HEAL_CRON precedent -- a self-contained module owns its own
// cron string rather than centralizing every schedule in one file.
export const CORNER_FEATURES_REFRESH_CRON_MORNING = "15 0 * * *";
// JST 22:00, after the last JRA/NAR race-hours tick (WARM_CRON_RACE_HOURS ends
// at JST 20:59) with a buffer for kakutei_chakujun to settle into jvd_se/
// nvd_se, so today's races' settlement columns get upserted the same day they
// run instead of waiting for lookbackDays on a future run to catch them.
export const CORNER_FEATURES_REFRESH_CRON_EVENING = "0 13 * * *";

const CORNER_FEATURES_REFRESH_CRONS: ReadonlySet<string> = new Set([
  CORNER_FEATURES_REFRESH_CRON_MORNING,
  CORNER_FEATURES_REFRESH_CRON_EVENING,
]);

// Returns true when the cron string matches one of the two corner-features
// refresh schedules (morning pre-race populate, evening settlement catch-up).
export const shouldRunCornerFeaturesRefreshCron = (cron: string): boolean =>
  CORNER_FEATURES_REFRESH_CRONS.has(cron);

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

// Two separate statements (not one semicolon-joined string): neon()'s
// serverless HTTP driver runs each sql.query() call as a single prepared
// statement and rejects "cannot insert multiple commands into a prepared
// statement" for a string containing more than one command -- confirmed
// against real production Neon on 2026-07-17 while backfilling the
// zero-row NAR gap (docs/probes/corner-features-settlement-backfill-heal-
// 2026-07-17.md); the previous single combined string had never actually
// succeeded against real Neon, only against the mocked driver in tests.
const CORNER_FEATURES_EXTENSION_DDL = "create extension if not exists vector";

const CORNER_FEATURES_TABLE_DDL = `
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

// Excludes source/kaisai_nen/kaisai_tsukihi/keibajo_code/race_bango: both
// callers (buildJraSelectSql/buildNarSelectSql) already select those in the
// SELECT list this constant is appended to ('jra'/'nar' source, ra.kaisai_nen,
// ra.kaisai_tsukihi, ra.keibajo_code, ra.race_bango). A prior version of this
// constant repeated them here as bare, unqualified names -- a select-list item
// cannot reference another item's own alias (source here is a string literal
// alias, not a real jvd_se/nvd_se/jvd_ra/nvd_ra column), so every raw_rows
// query failed with "column \"source\" does not exist" and
// refreshCornerFeatures() never wrote a single row, independent of the
// multi-statement DDL and CREATE EXTENSION permission issues fixed alongside
// this (all three found together while backfilling the 2026-07-17 NAR
// zero-row gap, see corner-features-settlement-backfill-heal-2026-07-17.md).
const ENTRY_COLUMNS = `
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

// Normalization + upsert, ported from build-corner-feature-table.ts's
// buildSql (see that file for the full derivation of each *_norm / the
// 8-dimension feature_vector) -- the raw_rows source selects differ
// (date-window-parameterized here instead of CLI-flag-parameterized there),
// and time_sa's regex was widened to `^[+-]?[0-9]+$`: jvd_se/nvd_se.time_sa is
// sign-prefixed ('+012', '-000'), unlike every other raw column normalized
// here, so the original unsigned-only regex silently matched zero rows (found
// 2026-07-17 while healing 0523/0524/0627/0711/0712's permanently-NULL
// settlement columns -- see corner-features-settlement-backfill-heal-2026-07-17.md).
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
        case when time_sa ~ '^[+-]?[0-9]+$' then nullif(time_sa, '0000')::numeric / 10 else null end time_sa,
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
const resolveFromDate = (runYmd: string, lookbackDays: number | undefined): string =>
  lookbackDays ? addDaysToYyyymmdd(runYmd, -lookbackDays) : runYmd;

// Best-effort only: the connecting role for env.NEON_DATABASE_URL is a
// regular application role, not a superuser, and Neon reserves CREATE
// EXTENSION for elevated roles (confirmed against real production Neon on
// 2026-07-17: "cannot execute CREATE EXTENSION in a read-only transaction",
// see corner-features-settlement-backfill-heal-2026-07-17.md). The pgvector
// extension this statement installs is already active in production (the
// table's existing rows already use the vector(8) feature_vector column), so
// a permission failure here is expected steady-state, not a real problem --
// swallowing it (unlike every other statement in refreshCornerFeatures,
// which aborts the whole run on any failure) lets a genuinely fresh
// environment's real failure still surface in the outer catch below while a
// known-redundant permission denial never blocks the rest of the refresh.
const ensureVectorExtension = async (sql: NeonQueryFunction<false, false>): Promise<void> => {
  try {
    await sql.query(CORNER_FEATURES_EXTENSION_DDL);
  } catch (err) {
    console.warn(`[corner-features-refresh] create extension vector skipped: ${String(err)}`);
  }
};

export const refreshCornerFeatures = async (params: RefreshCornerFeaturesParams): Promise<void> => {
  const { env, runYmd, daysAhead, lookbackDays } = params;
  const effectiveLookbackDays = lookbackDays ?? NO_LOOKBACK_DAYS;
  console.log(
    `[corner-features-refresh] start runYmd=${runYmd} daysAhead=${daysAhead} lookbackDays=${effectiveLookbackDays}`,
  );
  try {
    const fromDate = resolveFromDate(runYmd, lookbackDays);
    const toDate = addDaysToYyyymmdd(runYmd, daysAhead);
    const sql = neon(env.NEON_DATABASE_URL);
    await ensureVectorExtension(sql);
    await sql.query(CORNER_FEATURES_TABLE_DDL);
    for (const statement of CORNER_FEATURES_ALTER_STATEMENTS) {
      await sql.query(statement);
    }
    await sql.query(buildCornerFeaturesUpsertSql(fromDate, toDate));
    for (const statement of CORNER_FEATURES_INDEX_STATEMENTS) {
      await sql.query(statement);
    }
    console.log(
      `[corner-features-refresh] ok runYmd=${runYmd} fromDate=${fromDate} toDate=${toDate}`,
    );
  } catch (err) {
    console.error(
      `[corner-features-refresh] failed runYmd=${runYmd} daysAhead=${daysAhead} lookbackDays=${effectiveLookbackDays}: ${String(err)}`,
    );
  }
};
