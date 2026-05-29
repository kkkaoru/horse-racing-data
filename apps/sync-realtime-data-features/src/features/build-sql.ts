// Run with bun. SQL builder for daily feature build, ported from the old
// daily-feature-build.ts. Hyperdrive (Postgres) only — no legacy D1 read.

export type DailyFeatureBuildSourceScope = "all" | "ban-ei" | "jra" | "nar";

export interface DailyFeatureBuildOptions {
  fromDate: string;
  toDate?: string;
  sourceScope?: DailyFeatureBuildSourceScope;
}

const YYYYMMDD_PATTERN = /^\d{8}$/u;
const DEFAULT_SCOPE: DailyFeatureBuildSourceScope = "all";

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

const NORMALISATION_TAIL = `
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
  return `with raw_rows as (
      ${selects.join("\n      union all\n")}
    )${NORMALISATION_TAIL}`;
};
