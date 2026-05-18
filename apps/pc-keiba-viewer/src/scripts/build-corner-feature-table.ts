import { Pool } from "pg";

import { getConnectionString, loadEnv } from "./compare-corner-predictions";

type Target = "local" | "neon";

type Options = {
  buildVectorIndex: boolean;
  sourceScope: "all" | "ban-ei" | "jra" | "nar";
  target: Target;
};

const parseArgs = (args: string[]): Options => {
  const options: Options = {
    buildVectorIndex: false,
    sourceScope: "all",
    target: "local",
  };
  for (let index = 0; index < args.length; index += 1) {
    const name = args[index];
    const value = args[index + 1];
    if (name === "--target") {
      if (value !== "local" && value !== "neon") {
        throw new Error("--target must be local or neon.");
      }
      options.target = value;
      index += 1;
    } else if (name === "--source-scope") {
      if (value !== "all" && value !== "jra" && value !== "nar" && value !== "ban-ei") {
        throw new Error("--source-scope must be all, jra, nar, or ban-ei.");
      }
      options.sourceScope = value;
      index += 1;
    } else if (name === "--help" || name === "-h") {
      console.log(`Usage:
  bun run src/scripts/build-corner-feature-table.ts [options]

Options:
  --target local|neon
  --source-scope all|jra|nar|ban-ei
  --with-vector-index
`);
      process.exit(0);
    } else if (name === "--with-vector-index") {
      options.buildVectorIndex = true;
    } else {
      throw new Error(`Unknown argument: ${name}`);
    }
  }
  return options;
};

const buildSql = (sourceScope: Options["sourceScope"], buildVectorIndex: boolean): string => {
  const includeJra = sourceScope === "all" || sourceScope === "jra";
  const includeNar = sourceScope === "all" || sourceScope === "nar" || sourceScope === "ban-ei";
  const selects: string[] = [];
  if (includeJra) {
    selects.push(`
      select
        'jra' source,
        ra.kaisai_nen,
        ra.kaisai_tsukihi,
        ra.keibajo_code,
        ra.race_bango,
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
      from jvd_se se
      join jvd_ra ra
        on ra.kaisai_nen = se.kaisai_nen
        and ra.kaisai_tsukihi = se.kaisai_tsukihi
        and ra.keibajo_code = se.keibajo_code
        and ra.race_bango = se.race_bango
      where
        se.ketto_toroku_bango is not null
        and btrim(se.ketto_toroku_bango) <> ''
    `);
  }
  if (includeNar) {
    selects.push(`
      select
        'nar' source,
        ra.kaisai_nen,
        ra.kaisai_tsukihi,
        ra.keibajo_code,
        ra.race_bango,
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
      from nvd_se se
      join nvd_ra ra
        on ra.kaisai_nen = se.kaisai_nen
        and ra.kaisai_tsukihi = se.kaisai_tsukihi
        and ra.keibajo_code = se.keibajo_code
        and ra.race_bango = se.race_bango
      where
        se.ketto_toroku_bango is not null
        and btrim(se.ketto_toroku_bango) <> ''
        ${
          sourceScope === "ban-ei"
            ? "and ra.keibajo_code = '83'"
            : sourceScope === "nar"
              ? "and ra.keibajo_code <> '83'"
              : ""
        }
    `);
  }

  return `
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
    );

    alter table race_entry_corner_features add column if not exists grade_code text;
    alter table race_entry_corner_features add column if not exists kyoso_shubetsu_code text;
    alter table race_entry_corner_features add column if not exists juryo_shubetsu_code text;
    alter table race_entry_corner_features add column if not exists kyoso_joken_code text;
    alter table race_entry_corner_features add column if not exists babajotai_code_shiba text;
    alter table race_entry_corner_features add column if not exists babajotai_code_dirt text;
    alter table race_entry_corner_features add column if not exists seibetsu_code text;
    alter table race_entry_corner_features add column if not exists barei integer;
    alter table race_entry_corner_features add column if not exists futan_juryo numeric;
    alter table race_entry_corner_features add column if not exists kishumei_ryakusho text;
    alter table race_entry_corner_features add column if not exists chokyoshimei_ryakusho text;
    alter table race_entry_corner_features add column if not exists banushimei text;
    alter table race_entry_corner_features add column if not exists finish_position integer;
    alter table race_entry_corner_features add column if not exists finish_norm numeric;
    alter table race_entry_corner_features add column if not exists soha_time integer;
    alter table race_entry_corner_features add column if not exists time_sa numeric;
    alter table race_entry_corner_features add column if not exists kohan_3f numeric;

    with raw_rows as (
      ${selects.join("\n      union all\n")}
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
      source,
      race_date,
      kaisai_nen,
      kaisai_tsukihi,
      keibajo_code,
      race_bango,
      ketto_toroku_bango,
      umaban,
      bamei,
      track_code,
      grade_code,
      kyoso_shubetsu_code,
      juryo_shubetsu_code,
      kyoso_joken_code,
      babajotai_code_shiba,
      babajotai_code_dirt,
      kyori,
      shusso_tosu,
      seibetsu_code,
      barei,
      futan_juryo,
      kishumei_ryakusho,
      chokyoshimei_ryakusho,
      banushimei,
      finish_position,
      finish_norm,
      tansho_ninkijun,
      tansho_odds,
      soha_time,
      time_sa,
      kohan_3f,
      corner1_norm,
      corner2_norm,
      corner3_norm,
      corner4_norm,
      feature_vector,
      updated_at
    )
    select
      source,
      race_date,
      kaisai_nen,
      kaisai_tsukihi,
      keibajo_code,
      race_bango,
      ketto_toroku_bango,
      umaban,
      bamei,
      track_code,
      grade_code,
      kyoso_shubetsu_code,
      juryo_shubetsu_code,
      kyoso_joken_code,
      babajotai_code_shiba,
      babajotai_code_dirt,
      kyori,
      shusso_tosu,
      seibetsu_code,
      barei,
      futan_juryo,
      kishumei_ryakusho,
      chokyoshimei_ryakusho,
      banushimei,
      finish_position,
      finish_norm,
      tansho_ninkijun,
      tansho_odds,
      soha_time,
      time_sa,
      kohan_3f,
      corner1_norm,
      corner2_norm,
      corner3_norm,
      corner4_norm,
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
      source,
      kaisai_nen,
      kaisai_tsukihi,
      keibajo_code,
      race_bango,
      ketto_toroku_bango
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
      updated_at = now();

    create index if not exists race_entry_corner_features_lookup_idx
      on race_entry_corner_features (source, race_date, track_code, kyori);

    create index if not exists race_entry_corner_features_prefilter_idx
      on race_entry_corner_features (source, left(coalesce(track_code, ''), 1), kyori, race_date desc);

    create index if not exists race_entry_corner_features_venue_prefilter_idx
      on race_entry_corner_features (source, left(coalesce(track_code, ''), 1), keibajo_code, kyori, race_date desc);

    create index if not exists race_entry_corner_features_finish_prefilter_idx
      on race_entry_corner_features (source, race_date desc, left(coalesce(track_code, ''), 1), kyori, keibajo_code)
      where finish_norm is not null;

    create index if not exists race_entry_corner_features_horse_history_idx
      on race_entry_corner_features (source, ketto_toroku_bango, race_date desc)
      where finish_norm is not null;
    ${
      buildVectorIndex
        ? `
    create index if not exists race_entry_corner_features_hnsw_idx
      on race_entry_corner_features
      using hnsw (feature_vector vector_l2_ops);
    `
        : ""
    }
  `;
};

const main = async () => {
  await loadEnv();
  const options = parseArgs(process.argv.slice(2));
  const pool = new Pool({ connectionString: getConnectionString(options.target), max: 2 });
  try {
    await pool.query(buildSql(options.sourceScope, options.buildVectorIndex));
    const result = await pool.query<{ count: string }>(
      "select count(*)::text count from race_entry_corner_features",
    );
    console.log(`race_entry_corner_features=${result.rows[0]?.count ?? "0"}`);
  } finally {
    await pool.end();
  }
};

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
