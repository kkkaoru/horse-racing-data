import "server-only";
import { and, asc, desc, eq, sql } from "drizzle-orm";
import { cache } from "react";

import { TRACK_LABELS, type RaceSource } from "../lib/codes";
import { isRunningStyleLabel, type RunningStyleLabel } from "./corner-running-style-parsers";
import type {
  AbilityTest,
  BloodlineStatsRow,
  ConditionCorrelationDetail,
  ConditionCorrelationRow,
  CourseInfo,
  EntityDetailSummary,
  EntityListQuery,
  EntityRaceResult,
  FinishPositionModelPredictionFeature,
  FinishPositionSimilarityFeature,
  FinishPositionStatsRow,
  FrameStatsRow,
  HorseListRow,
  HorseRaceResult,
  PayoutStatsDetail,
  PayoutStatsRow,
  PersonListRow,
  RaceDaySummary,
  RaceDetail,
  RaceListItem,
  RacePaceModelPredictionFeature,
  RacePaceSimilarityFeature,
  RaceTimeStats,
  RaceTimeTargetRace,
  RaceTrendStarterRow,
  RaceYearSummary,
  Runner,
  SimilarRaceStatsRow,
  SimilarRaceStatsSettings,
  StatsDetail,
  TopRaceSummary,
  Training,
  TimeScoreDetail,
  TimeScoreRow,
} from "../lib/race-types";
import { getDb } from "./client";
import { withDbQueryCache } from "./query-cache";
import { jvdCs, jvdRa, jvdSe, jvdUm, nvdNu, nvdRa, nvdSe, nvdUm } from "./schema";

export interface ActiveRunningStylePrediction {
  horseNumber: number;
  predictedLabel: RunningStyleLabel;
}

export const getActiveRunningStylePredictions = cache(
  async ({
    category,
    day,
    keibajoCode,
    month,
    raceNumber,
    source,
    year,
  }: {
    category: string;
    day: string;
    keibajoCode: string;
    month: string;
    raceNumber: string;
    source: RaceSource;
    year: string;
  }): Promise<ActiveRunningStylePrediction[]> =>
    withDbQueryCache(
      [
        "getActiveRunningStylePredictions",
        category,
        source,
        year,
        month,
        day,
        keibajoCode,
        raceNumber,
      ],
      async () => {
        try {
          const monthDay = `${month.padStart(2, "0")}${day.padStart(2, "0")}`;
          const normalizedRaceNumber = raceNumber.padStart(2, "0");
          const result = await getDb().execute<{
            predicted_label: string;
            umaban: number | string;
          }>(sql`
            with race_predictions as (
              select p.umaban, p.predicted_label, p.model_version, p.prediction_generated_at
                from race_running_style_model_predictions p
               where p.source = ${source}
                 and p.kaisai_nen = ${year}
                 and p.kaisai_tsukihi = ${monthDay}
                 and p.keibajo_code = ${keibajoCode}
                 and p.race_bango = ${normalizedRaceNumber}
            ),
            active as (
              select model_version
                from running_style_active_models
               where category = ${category}
               limit 1
            ),
            active_rows as (
              select p.umaban, p.predicted_label
                from race_predictions p
                join active on active.model_version = p.model_version
            ),
            latest_model as (
              select model_version
                from race_predictions
               order by prediction_generated_at desc, model_version desc
               limit 1
            )
            select umaban, predicted_label
              from active_rows
            union all
            select p.umaban, p.predicted_label
              from race_predictions p
              join latest_model on latest_model.model_version = p.model_version
             where not exists (select 1 from active_rows)
            order by umaban
          `);

          return result.rows.flatMap((row) => {
            if (!isRunningStyleLabel(row.predicted_label)) {
              return [];
            }
            const horseNumber = Number(row.umaban);
            return Number.isFinite(horseNumber)
              ? [{ horseNumber, predictedLabel: row.predicted_label }]
              : [];
          });
        } catch {
          return [];
        }
      },
    ),
);

export const getRaceYears = cache(
  async (): Promise<RaceYearSummary[]> =>
    withDbQueryCache(["getRaceYears"], async () => {
      const result = await getDb().execute<{
        year: string;
        race_count: string;
        day_count: string;
      }>(sql`
    select
      kaisai_nen as year,
      sum(race_count) as race_count,
      count(distinct kaisai_tsukihi) as day_count
    from (
      select kaisai_nen, kaisai_tsukihi, count(*) as race_count
      from ${jvdRa}
      group by kaisai_nen, kaisai_tsukihi
      union all
      select kaisai_nen, kaisai_tsukihi, count(*) as race_count
      from ${nvdRa}
      group by kaisai_nen, kaisai_tsukihi
    ) race_days
    group by kaisai_nen
    order by kaisai_nen desc
  `);

      return result.rows.map((row) => ({
        year: row.year,
        raceCount: Number(row.race_count),
        dayCount: Number(row.day_count),
      }));
    }),
);

export const getRaceDaySummaries = cache(
  async (year: string): Promise<RaceDaySummary[]> =>
    withDbQueryCache(["getRaceDaySummaries", year], async () => {
      const result = await getDb().execute<{
        year: string;
        month: string;
        day: string;
        jra_count: string;
        nar_count: string;
      }>(sql`
    select
      kaisai_nen as year,
      substring(kaisai_tsukihi from 1 for 2) as month,
      substring(kaisai_tsukihi from 3 for 2) as day,
      sum(jra_count) as jra_count,
      sum(nar_count) as nar_count
    from (
      select kaisai_nen, kaisai_tsukihi, count(*) as jra_count, 0 as nar_count
      from ${jvdRa}
      where kaisai_nen = ${year}
      group by kaisai_nen, kaisai_tsukihi
      union all
      select kaisai_nen, kaisai_tsukihi, 0 as jra_count, count(*) as nar_count
      from ${nvdRa}
      where kaisai_nen = ${year}
      group by kaisai_nen, kaisai_tsukihi
    ) race_days
    group by kaisai_nen, kaisai_tsukihi
    order by kaisai_nen desc, kaisai_tsukihi desc
  `);

      return result.rows.map((row) => ({
        year: row.year,
        month: row.month,
        day: row.day,
        jraCount: Number(row.jra_count),
        narCount: Number(row.nar_count),
      }));
    }),
);

export const getRacesByDate = cache(
  async (year: string, month: string, day: string): Promise<RaceListItem[]> => {
    return withDbQueryCache(["getRacesByDate", year, month, day], async () => {
      const monthDay = `${month}${day}`;
      const result = await getDb().execute<RaceListItem>(sql`
    select *
    from (
      select
        'jra' as source,
        ra.kaisai_nen as "kaisaiNen",
        ra.kaisai_tsukihi as "kaisaiTsukihi",
        ra.keibajo_code as "keibajoCode",
        ra.race_bango as "raceBango",
        ra.kyosomei_hondai as "kyosomeiHondai",
        ra.kyosomei_fukudai as "kyosomeiFukudai",
        ra.grade_code as "gradeCode",
        ra.kyoso_shubetsu_code as "kyosoShubetsuCode",
        ra.kyoso_kigo_code as "kyosoKigoCode",
        ra.juryo_shubetsu_code as "juryoShubetsuCode",
        coalesce((
          select array_remove(array_agg(distinct nullif(btrim(se.kishumei_ryakusho), '')), null)
          from ${jvdSe} se
          where se.kaisai_nen = ra.kaisai_nen
            and se.kaisai_tsukihi = ra.kaisai_tsukihi
            and se.keibajo_code = ra.keibajo_code
            and se.race_bango = ra.race_bango
        ), array[]::text[]) as "jockeyNames",
        ra.kyoso_joken_code as "kyosoJokenCode",
        ra.kyoso_joken_meisho as "kyosoJokenMeisho",
        ra.kyori,
        ra.track_code as "trackCode",
        ra.hasso_jikoku as "hassoJikoku",
        ra.shusso_tosu as "shussoTosu"
      from ${jvdRa} ra
      where ra.kaisai_nen = ${year} and ra.kaisai_tsukihi = ${monthDay}
      union all
      select
        'nar' as source,
        ra.kaisai_nen as "kaisaiNen",
        ra.kaisai_tsukihi as "kaisaiTsukihi",
        ra.keibajo_code as "keibajoCode",
        ra.race_bango as "raceBango",
        ra.kyosomei_hondai as "kyosomeiHondai",
        ra.kyosomei_fukudai as "kyosomeiFukudai",
        ra.grade_code as "gradeCode",
        ra.kyoso_shubetsu_code as "kyosoShubetsuCode",
        ra.kyoso_kigo_code as "kyosoKigoCode",
        ra.juryo_shubetsu_code as "juryoShubetsuCode",
        coalesce((
          select array_remove(array_agg(distinct nullif(btrim(se.kishumei_ryakusho), '')), null)
          from ${nvdSe} se
          where se.kaisai_nen = ra.kaisai_nen
            and se.kaisai_tsukihi = ra.kaisai_tsukihi
            and se.keibajo_code = ra.keibajo_code
            and se.race_bango = ra.race_bango
        ), array[]::text[]) as "jockeyNames",
        ra.kyoso_joken_code as "kyosoJokenCode",
        ra.kyoso_joken_meisho as "kyosoJokenMeisho",
        ra.kyori,
        ra.track_code as "trackCode",
        ra.hasso_jikoku as "hassoJikoku",
        ra.shusso_tosu as "shussoTosu"
      from ${nvdRa} ra
      where ra.kaisai_nen = ${year} and ra.kaisai_tsukihi = ${monthDay}
    ) races
    order by "hassoJikoku" asc nulls last, "keibajoCode" asc, "raceBango" asc, source asc
  `);

      return result.rows;
    });
  },
);

export const getRacesByDateWithoutJockeyNames = cache(
  async (year: string, month: string, day: string): Promise<RaceListItem[]> => {
    return withDbQueryCache(["getRacesByDateWithoutJockeyNames", year, month, day], async () => {
      const monthDay = `${month}${day}`;
      const result = await getDb().execute<RaceListItem>(sql`
    select *
    from (
      select
        'jra' as source,
        ra.kaisai_nen as "kaisaiNen",
        ra.kaisai_tsukihi as "kaisaiTsukihi",
        ra.keibajo_code as "keibajoCode",
        ra.race_bango as "raceBango",
        ra.kyosomei_hondai as "kyosomeiHondai",
        ra.kyosomei_fukudai as "kyosomeiFukudai",
        ra.grade_code as "gradeCode",
        ra.kyoso_shubetsu_code as "kyosoShubetsuCode",
        ra.kyoso_kigo_code as "kyosoKigoCode",
        ra.juryo_shubetsu_code as "juryoShubetsuCode",
        array[]::text[] as "jockeyNames",
        ra.kyoso_joken_code as "kyosoJokenCode",
        ra.kyoso_joken_meisho as "kyosoJokenMeisho",
        ra.kyori,
        ra.track_code as "trackCode",
        ra.hasso_jikoku as "hassoJikoku",
        ra.shusso_tosu as "shussoTosu"
      from ${jvdRa} ra
      where ra.kaisai_nen = ${year} and ra.kaisai_tsukihi = ${monthDay}
      union all
      select
        'nar' as source,
        ra.kaisai_nen as "kaisaiNen",
        ra.kaisai_tsukihi as "kaisaiTsukihi",
        ra.keibajo_code as "keibajoCode",
        ra.race_bango as "raceBango",
        ra.kyosomei_hondai as "kyosomeiHondai",
        ra.kyosomei_fukudai as "kyosomeiFukudai",
        ra.grade_code as "gradeCode",
        ra.kyoso_shubetsu_code as "kyosoShubetsuCode",
        ra.kyoso_kigo_code as "kyosoKigoCode",
        ra.juryo_shubetsu_code as "juryoShubetsuCode",
        array[]::text[] as "jockeyNames",
        ra.kyoso_joken_code as "kyosoJokenCode",
        ra.kyoso_joken_meisho as "kyosoJokenMeisho",
        ra.kyori,
        ra.track_code as "trackCode",
        ra.hasso_jikoku as "hassoJikoku",
        ra.shusso_tosu as "shussoTosu"
      from ${nvdRa} ra
      where ra.kaisai_nen = ${year} and ra.kaisai_tsukihi = ${monthDay}
    ) races
    order by "hassoJikoku" asc nulls last, "keibajoCode" asc, "raceBango" asc, source asc
  `);

      return result.rows;
    });
  },
);

export const getRaceSourceByRoute = cache(
  async (
    year: string,
    month: string,
    day: string,
    keibajoCode: string,
    raceNumber: string,
  ): Promise<RaceSource | null> =>
    withDbQueryCache(
      ["getRaceSourceByRoute", year, month, day, keibajoCode, raceNumber],
      async () => {
        const monthDay = `${month}${day}`;
        const result = await getDb().execute<{ source: RaceSource }>(sql`
    select source
    from (
      select 'jra'::text as source
      from ${jvdRa}
      where
        kaisai_nen = ${year}
        and kaisai_tsukihi = ${monthDay}
        and keibajo_code = ${keibajoCode}
        and race_bango = ${raceNumber}
      union all
      select 'nar'::text as source
      from ${nvdRa}
      where
        kaisai_nen = ${year}
        and kaisai_tsukihi = ${monthDay}
        and keibajo_code = ${keibajoCode}
        and race_bango = ${raceNumber}
    ) races
    order by source asc
    limit 1
  `);

        return result.rows[0]?.source ?? null;
      },
    ),
);

export const getRaceDetail = cache(
  async (
    source: RaceSource,
    year: string,
    month: string,
    day: string,
    keibajoCode: string,
    raceNumber: string,
  ): Promise<RaceDetail | null> => {
    return withDbQueryCache(
      ["getRaceDetail", source, year, month, day, keibajoCode, raceNumber],
      async () => {
        const table = source === "jra" ? jvdRa : nvdRa;
        const [race] = await getDb()
          .select({
            kaisaiNen: table.kaisaiNen,
            kaisaiTsukihi: table.kaisaiTsukihi,
            keibajoCode: table.keibajoCode,
            kaisaiKai: table.kaisaiKai,
            kaisaiNichime: table.kaisaiNichime,
            raceBango: table.raceBango,
            kyosomeiHondai: table.kyosomeiHondai,
            kyosomeiFukudai: table.kyosomeiFukudai,
            kyosomeiKakkonai: table.kyosomeiKakkonai,
            gradeCode: table.gradeCode,
            kyosoShubetsuCode: table.kyosoShubetsuCode,
            kyosoKigoCode: table.kyosoKigoCode,
            juryoShubetsuCode: table.juryoShubetsuCode,
            kyosoJokenCode: table.kyosoJokenCode,
            kyosoJokenMeisho: table.kyosoJokenMeisho,
            kyori: table.kyori,
            trackCode: table.trackCode,
            hassoJikoku: table.hassoJikoku,
            torokuTosu: table.torokuTosu,
            shussoTosu: table.shussoTosu,
            tenkoCode: table.tenkoCode,
            babajotaiCodeShiba: table.babajotaiCodeShiba,
            babajotaiCodeDirt: table.babajotaiCodeDirt,
          })
          .from(table)
          .where(
            and(
              eq(table.kaisaiNen, year),
              eq(table.kaisaiTsukihi, `${month}${day}`),
              eq(table.keibajoCode, keibajoCode),
              eq(table.raceBango, raceNumber),
            ),
          )
          .limit(1);

        return race ? { ...race, source } : null;
      },
    );
  },
);

export const getRaceRunners = cache(
  async (
    source: RaceSource,
    year: string,
    month: string,
    day: string,
    keibajoCode: string,
    raceNumber: string,
  ): Promise<Runner[]> => {
    return withDbQueryCache(
      ["getRaceRunners", source, year, month, day, keibajoCode, raceNumber],
      async () => {
        const table = source === "jra" ? jvdSe : nvdSe;
        const monthDay = `${month}${day}`;

        if (source === "nar") {
          const result = await getDb().execute<Runner & Record<string, unknown>>(sql`
            select
              se.wakuban,
              se.umaban,
              se.ketto_toroku_bango as "kettoTorokuBango",
              se.bamei,
              se.moshoku_code as "moshokuCode",
              se.seibetsu_code as "seibetsuCode",
              se.barei,
              se.futan_juryo as "futanJuryo",
              se.kishumei_ryakusho as "kishumeiRyakusho",
              se.chokyoshimei_ryakusho as "chokyoshimeiRyakusho",
              se.banushimei,
              coalesce(nullif(btrim(se.bataiju), ''), latest_weight.bataiju) as bataiju,
              coalesce(nullif(btrim(se.zogen_fugo), ''), latest_weight.zogen_fugo) as "zogenFugo",
              coalesce(nullif(btrim(se.zogen_sa), ''), latest_weight.zogen_sa) as "zogenSa",
              se.kakutei_chakujun as "kakuteiChakujun",
              se.tansho_odds as "tanshoOdds",
              se.tansho_ninkijun as "tanshoNinkijun",
              se.soha_time as "sohaTime",
              se.time_sa as "timeSa",
              se.corner_1 as "corner1",
              se.corner_2 as "corner2",
              se.corner_3 as "corner3",
              se.corner_4 as "corner4",
              se.kohan_3f as "kohan3f"
            from ${nvdSe} se
            left join lateral (
              select
                hist.bataiju,
                hist.zogen_fugo,
                hist.zogen_sa
              from ${nvdSe} hist
              where
                hist.ketto_toroku_bango = se.ketto_toroku_bango
                and hist.ketto_toroku_bango is not null
                and btrim(hist.ketto_toroku_bango) <> ''
                and (hist.kaisai_nen, hist.kaisai_tsukihi, hist.race_bango) < (${year}, ${monthDay}, ${raceNumber})
                and nullif(btrim(hist.bataiju), '') is not null
                and upper(btrim(hist.bataiju)) <> 'FFF'
              order by hist.kaisai_nen desc, hist.kaisai_tsukihi desc, hist.race_bango desc
              limit 1
            ) latest_weight on true
            where
              se.kaisai_nen = ${year}
              and se.kaisai_tsukihi = ${monthDay}
              and se.keibajo_code = ${keibajoCode}
              and se.race_bango = ${raceNumber}
            order by cast(se.umaban as integer) asc
          `);
          return result.rows;
        }

        return getDb()
          .select({
            wakuban: table.wakuban,
            umaban: table.umaban,
            kettoTorokuBango: table.kettoTorokuBango,
            bamei: table.bamei,
            moshokuCode: table.moshokuCode,
            seibetsuCode: table.seibetsuCode,
            barei: table.barei,
            futanJuryo: table.futanJuryo,
            kishumeiRyakusho: table.kishumeiRyakusho,
            chokyoshimeiRyakusho: table.chokyoshimeiRyakusho,
            banushimei: table.banushimei,
            bataiju: table.bataiju,
            zogenFugo: table.zogenFugo,
            zogenSa: table.zogenSa,
            kakuteiChakujun: table.kakuteiChakujun,
            tanshoOdds: table.tanshoOdds,
            tanshoNinkijun: table.tanshoNinkijun,
            sohaTime: table.sohaTime,
            timeSa: table.timeSa,
            corner1: table.corner1,
            corner2: table.corner2,
            corner3: table.corner3,
            corner4: table.corner4,
            kohan3f: table.kohan3f,
          })
          .from(table)
          .where(
            and(
              eq(table.kaisaiNen, year),
              eq(table.kaisaiTsukihi, monthDay),
              eq(table.keibajoCode, keibajoCode),
              eq(table.raceBango, raceNumber),
            ),
          )
          .orderBy(asc(table.umaban), asc(table.kettoTorokuBango));
      },
    );
  },
);

interface RaceTrendHistoricalRowsParams {
  frameEndYmd: string;
  frameNumbers: string[];
  frameStartYmd: string;
  includeAllRows: boolean;
  jockeyEndYmd: string;
  jockeyNames: string[];
  jockeySameVenue: boolean;
  jockeyStartYmd: string;
}

const normalizeTextSql = (value: unknown) =>
  sql`nullif(regexp_replace(coalesce(${value}, ''), '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')`;

export const getRaceTrendHistoricalStarterRows = cache(
  async (
    race: RaceDetail,
    params: RaceTrendHistoricalRowsParams,
  ): Promise<RaceTrendStarterRow[]> => {
    return withDbQueryCache(["getRaceTrendHistoricalStarterRows", race, params], async () => {
      const raceTable = race.source === "jra" ? jvdRa : nvdRa;
      const runnerTable = race.source === "jra" ? jvdSe : nvdSe;
      const jockeyNames = Array.from(new Set(params.jockeyNames.map((name) => name.trim()))).filter(
        Boolean,
      );
      const frameNumbers = Array.from(
        new Set(params.frameNumbers.map((frameNumber) => frameNumber.trim())),
      ).filter(Boolean);
      const jockeyNameCondition =
        jockeyNames.length > 0
          ? sql`${normalizeTextSql(sql`${runnerTable}.kishumei_ryakusho`)} in (${sql.join(
              jockeyNames,
              sql`, `,
            )})`
          : sql`false`;
      const frameNumberCondition =
        frameNumbers.length > 0
          ? sql`${normalizeTextSql(sql`${runnerTable}.wakuban`)} in (${sql.join(
              frameNumbers,
              sql`, `,
            )})`
          : sql`false`;
      const result = await getDb().execute<RaceTrendStarterRow>(sql`
        select
          ${race.source}::text as source,
          ra.kaisai_nen as "kaisaiNen",
          ra.kaisai_tsukihi as "kaisaiTsukihi",
          ra.keibajo_code as "keibajoCode",
          ra.race_bango as "raceBango",
          coalesce(
            ${normalizeTextSql(sql`ra.kyosomei_hondai`)},
            ${normalizeTextSql(sql`ra.kyosomei_fukudai`)},
            '一般競走'
          ) as "raceName",
          ${normalizeTextSql(sql`ra.hasso_jikoku`)} as "hassoJikoku",
          ${normalizeTextSql(sql`ra.toroku_tosu`)} as "runnerCount",
          ${normalizeTextSql(sql`${runnerTable}.wakuban`)} as wakuban,
          ${normalizeTextSql(sql`${runnerTable}.umaban`)} as umaban,
          ${normalizeTextSql(sql`${runnerTable}.bamei`)} as bamei,
          ${normalizeTextSql(sql`${runnerTable}.kishumei_ryakusho`)} as "jockeyName",
          ${normalizeTextSql(sql`${runnerTable}.tansho_odds`)} as "tanshoOdds",
          ${normalizeTextSql(sql`${runnerTable}.tansho_ninkijun`)} as "tanshoPopularity",
          nullif(
            regexp_replace(coalesce(${runnerTable}.kakutei_chakujun, ''), '[^0-9]', '', 'g'),
            ''
          )::int as "finishPosition",
          ${normalizeTextSql(sql`${runnerTable}.soha_time`)} as "sohaTime",
          ${normalizeTextSql(sql`${runnerTable}.corner_1`)} as "corner1",
          ${normalizeTextSql(sql`${runnerTable}.corner_2`)} as "corner2",
          ${normalizeTextSql(sql`${runnerTable}.corner_3`)} as "corner3",
          ${normalizeTextSql(sql`${runnerTable}.corner_4`)} as "corner4"
        from ${runnerTable}
        join ${raceTable} ra
          on ra.kaisai_nen = ${runnerTable}.kaisai_nen
          and ra.kaisai_tsukihi = ${runnerTable}.kaisai_tsukihi
          and ra.keibajo_code = ${runnerTable}.keibajo_code
          and ra.race_bango = ${runnerTable}.race_bango
        where
          nullif(
            regexp_replace(coalesce(${runnerTable}.kakutei_chakujun, ''), '[^0-9]', '', 'g'),
            ''
          )::int > 0
          and (
            (
              ra.kaisai_nen || ra.kaisai_tsukihi between ${params.jockeyStartYmd} and ${params.jockeyEndYmd}
              and ${jockeyNameCondition}
              and (${params.jockeySameVenue} = false or ra.keibajo_code = ${race.keibajoCode})
            )
            or (
              ra.kaisai_nen || ra.kaisai_tsukihi between ${params.frameStartYmd} and ${params.frameEndYmd}
              and ra.keibajo_code = ${race.keibajoCode}
              and ${frameNumberCondition}
            )
            or (
              ${params.includeAllRows} = true
              and
              ra.kaisai_nen || ra.kaisai_tsukihi between ${params.jockeyStartYmd} and ${params.jockeyEndYmd}
              and (${params.jockeySameVenue} = false or ra.keibajo_code = ${race.keibajoCode})
            )
          )
        order by ra.kaisai_nen desc, ra.kaisai_tsukihi desc, ra.keibajo_code asc, ra.race_bango asc, ${runnerTable}.umaban asc
      `);

      return result.rows;
    });
  },
);

const getEntitySourceCondition = (source: EntityListQuery["source"]) =>
  source === "jra" || source === "nar" ? sql`source = ${source}` : sql`true`;

const getHorseOrder = (order: string) => {
  switch (order) {
    case "name":
      return sql`bamei asc`;
    case "starts":
      return sql`starts::numeric desc, "latestDate" desc`;
    case "winRate":
      return sql`"winRate"::numeric desc, starts::numeric desc`;
    case "showRate":
      return sql`"showRate"::numeric desc, starts::numeric desc`;
    default:
      return sql`"latestDate" desc, starts::numeric desc`;
  }
};

const getPersonOrder = (order: string) => {
  switch (order) {
    case "name":
      return sql`name asc`;
    case "starts":
      return sql`starts::numeric desc, "latestDate" desc`;
    case "winRate":
      return sql`"winRate"::numeric desc, starts::numeric desc`;
    case "showRate":
      return sql`"showRate"::numeric desc, starts::numeric desc`;
    default:
      return sql`"latestDate" desc, starts::numeric desc`;
  }
};

const entityRaceRowsSql = sql`
  select
    'jra'::text as source,
    ra.kaisai_nen,
    ra.kaisai_tsukihi,
    ra.keibajo_code,
    ra.race_bango,
    coalesce(nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '一般競走') as race_name,
    ra.hasso_jikoku,
    ra.kyori,
    ra.track_code,
    se.wakuban,
    se.umaban,
    se.ketto_toroku_bango,
    coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as bamei,
    coalesce(nullif(regexp_replace(se.kishumei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as jockey_name,
    coalesce(nullif(regexp_replace(se.chokyoshimei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as trainer_name,
    coalesce(nullif(regexp_replace(se.banushimei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') owner_name,
    se.kakutei_chakujun,
    se.tansho_ninkijun,
    se.tansho_odds,
    se.soha_time,
    se.kohan_3f,
    se.corner_1,
    se.corner_2,
    se.corner_3,
    se.corner_4,
    (
      ra.kaisai_nen || ra.kaisai_tsukihi >= to_char((now() at time zone 'Asia/Tokyo')::date, 'YYYYMMDD')
      and coalesce(nullif(se.kakutei_chakujun, '00'), '') = ''
    ) as is_upcoming
  from ${jvdSe} se
  join ${jvdRa} ra
    on ra.kaisai_nen = se.kaisai_nen
    and ra.kaisai_tsukihi = se.kaisai_tsukihi
    and ra.keibajo_code = se.keibajo_code
    and ra.race_bango = se.race_bango
  union all
  select
    'nar'::text as source,
    ra.kaisai_nen,
    ra.kaisai_tsukihi,
    ra.keibajo_code,
    ra.race_bango,
    coalesce(nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '一般競走') as race_name,
    ra.hasso_jikoku,
    ra.kyori,
    ra.track_code,
    se.wakuban,
    se.umaban,
    se.ketto_toroku_bango,
    coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as bamei,
    coalesce(nullif(regexp_replace(se.kishumei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as jockey_name,
    coalesce(nullif(regexp_replace(se.chokyoshimei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as trainer_name,
    coalesce(nullif(regexp_replace(se.banushimei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') owner_name,
    se.kakutei_chakujun,
    se.tansho_ninkijun,
    se.tansho_odds,
    se.soha_time,
    se.kohan_3f,
    se.corner_1,
    se.corner_2,
    se.corner_3,
    se.corner_4,
    (
      ra.kaisai_nen || ra.kaisai_tsukihi >= to_char((now() at time zone 'Asia/Tokyo')::date, 'YYYYMMDD')
      and coalesce(nullif(se.kakutei_chakujun, '00'), '') = ''
    ) as is_upcoming
  from ${nvdSe} se
  join ${nvdRa} ra
    on ra.kaisai_nen = se.kaisai_nen
    and ra.kaisai_tsukihi = se.kaisai_tsukihi
    and ra.keibajo_code = se.keibajo_code
    and ra.race_bango = se.race_bango
`;

export const getHorseList = cache(
  async (query: EntityListQuery): Promise<HorseListRow[]> =>
    withDbQueryCache(["getHorseList", query], async () => {
      if (query.q === "" && query.order === "latest") {
        const result = await getDb().execute<{
          kettoTorokuBango: string;
          bamei: string;
          starts: string;
          winCount: string;
          showCount: string;
          winRate: string;
          showRate: string;
          latestDate: string;
          latestKeibajoCode: string;
          latestRaceBango: string;
          latestRaceName: string;
          latestSource: RaceSource;
          primarySource: RaceSource;
        }>(sql`
          with recent as (
            (
              select
                'jra'::text as source,
                kaisai_nen,
                kaisai_tsukihi,
                keibajo_code,
                race_bango,
                ketto_toroku_bango,
                coalesce(nullif(regexp_replace(bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as bamei,
                kakutei_chakujun
              from ${jvdSe}
              where ${query.source === "nar" ? sql`false` : sql`true`}
                and btrim(coalesce(ketto_toroku_bango, '')) <> ''
              order by kaisai_nen || kaisai_tsukihi desc, race_bango desc
              limit 8000
            )
            union all
            (
              select
                'nar'::text as source,
                kaisai_nen,
                kaisai_tsukihi,
                keibajo_code,
                race_bango,
                ketto_toroku_bango,
                coalesce(nullif(regexp_replace(bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as bamei,
                kakutei_chakujun
              from ${nvdSe}
              where ${query.source === "jra" ? sql`false` : sql`true`}
                and btrim(coalesce(ketto_toroku_bango, '')) <> ''
              order by kaisai_nen || kaisai_tsukihi desc, race_bango desc
              limit 8000
            )
          ),
          latest as (
            select distinct on (ketto_toroku_bango)
              ketto_toroku_bango,
              bamei,
              source,
              kaisai_nen,
              kaisai_tsukihi,
              keibajo_code,
              race_bango,
              kaisai_nen || kaisai_tsukihi as latest_date
            from recent
            order by ketto_toroku_bango, kaisai_nen desc, kaisai_tsukihi desc, race_bango desc
          ),
          candidates as (
            select *
            from latest
            order by latest_date desc, race_bango desc
            limit 200
          ),
          stat_rows as (
            select se.ketto_toroku_bango, se.kakutei_chakujun
            from ${jvdSe} se
            join candidates c on c.ketto_toroku_bango = se.ketto_toroku_bango
            where ${query.source === "nar" ? sql`false` : sql`true`}
            union all
            select se.ketto_toroku_bango, se.kakutei_chakujun
            from ${nvdSe} se
            join candidates c on c.ketto_toroku_bango = se.ketto_toroku_bango
            where ${query.source === "jra" ? sql`false` : sql`true`}
          ),
          stats as (
            select
              ketto_toroku_bango as "kettoTorokuBango",
              count(*)::text as starts,
              count(*) filter (where kakutei_chakujun = '01')::text as "winCount",
              count(*) filter (where kakutei_chakujun in ('01','02','03'))::text as "showCount",
              round(count(*) filter (where kakutei_chakujun = '01') * 100.0 / nullif(count(*), 0), 1)::text as "winRate",
              round(count(*) filter (where kakutei_chakujun in ('01','02','03')) * 100.0 / nullif(count(*), 0), 1)::text as "showRate"
            from stat_rows
            group by ketto_toroku_bango
          )
          select
            c.ketto_toroku_bango as "kettoTorokuBango",
            c.bamei,
            coalesce(stats.starts, '0') as starts,
            coalesce(stats."winCount", '0') as "winCount",
            coalesce(stats."showCount", '0') as "showCount",
            coalesce(stats."winRate", '0') as "winRate",
            coalesce(stats."showRate", '0') as "showRate",
            c.latest_date as "latestDate",
            c.keibajo_code "latestKeibajoCode",
            c.race_bango "latestRaceBango",
            c.source as "latestSource",
            coalesce(nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '一般競走') as "latestRaceName"
          from candidates c
          left join stats on stats."kettoTorokuBango" = c.ketto_toroku_bango
          left join ${jvdRa} jra
            on c.source = 'jra'
            and jra.kaisai_nen = c.kaisai_nen
            and jra.kaisai_tsukihi = c.kaisai_tsukihi
            and jra.keibajo_code = c.keibajo_code
            and jra.race_bango = c.race_bango
          left join ${nvdRa} nar
            on c.source = 'nar'
            and nar.kaisai_nen = c.kaisai_nen
            and nar.kaisai_tsukihi = c.kaisai_tsukihi
            and nar.keibajo_code = c.keibajo_code
            and nar.race_bango = c.race_bango
          cross join lateral (select coalesce(jra.kyosomei_hondai, nar.kyosomei_hondai) as kyosomei_hondai) ra
          order by c.latest_date desc, c.race_bango desc
        `);

        return result.rows.map((row) => ({
          bamei: row.bamei,
          kettoTorokuBango: row.kettoTorokuBango,
          latestDate: row.latestDate,
          latestKeibajoCode: row.latestKeibajoCode,
          latestRaceBango: row.latestRaceBango,
          latestRaceName: row.latestRaceName,
          latestSource: row.latestSource,
          primarySource: row.latestSource,
          showCount: toCount(row.showCount),
          showRate: toRate(row.showRate),
          starts: toCount(row.starts),
          winCount: toCount(row.winCount),
          winRate: toRate(row.winRate),
        }));
      }

      const result = await getDb().execute<{
        kettoTorokuBango: string;
        bamei: string;
        starts: string;
        winCount: string;
        showCount: string;
        winRate: string;
        showRate: string;
        latestDate: string;
        latestKeibajoCode: string;
        latestRaceBango: string;
        latestRaceName: string;
        latestSource: RaceSource;
      }>(sql`
        with stat_source as (
          select
            'jra'::text source,
            ketto_toroku_bango,
            min(coalesce(nullif(regexp_replace(bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-')) bamei,
            count(*) starts,
            count(*) filter (where kakutei_chakujun = '01') win_count,
            count(*) filter (where kakutei_chakujun in ('01','02','03')) show_count,
            max(kaisai_nen || kaisai_tsukihi) latest_date
          from ${jvdSe}
          where ${query.source === "nar" ? sql`false` : sql`true`}
            and btrim(coalesce(ketto_toroku_bango, '')) <> ''
            and (
              ${query.q} = ''
              or coalesce(nullif(regexp_replace(bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') ilike ${`%${query.q}%`}
              or ketto_toroku_bango = ${query.q}
            )
          group by ketto_toroku_bango
          union
          select
            'nar'::text source,
            ketto_toroku_bango,
            min(coalesce(nullif(regexp_replace(bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-')) bamei,
            count(*) starts,
            count(*) filter (where kakutei_chakujun = '01') win_count,
            count(*) filter (where kakutei_chakujun in ('01','02','03')) show_count,
            max(kaisai_nen || kaisai_tsukihi) latest_date
          from ${nvdSe}
          where ${query.source === "jra" ? sql`false` : sql`true`}
            and btrim(coalesce(ketto_toroku_bango, '')) <> ''
            and (
              ${query.q} = ''
              or coalesce(nullif(regexp_replace(bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') ilike ${`%${query.q}%`}
              or ketto_toroku_bango = ${query.q}
            )
          group by ketto_toroku_bango
        ),
        stats as (
          select
            ketto_toroku_bango "kettoTorokuBango",
            min(bamei) bamei,
            sum(starts)::text starts,
            sum(win_count)::text "winCount",
            sum(show_count)::text "showCount",
            round(sum(win_count) * 100.0 / nullif(sum(starts), 0), 1)::text "winRate",
            round(sum(show_count) * 100.0 / nullif(sum(starts), 0), 1)::text "showRate",
            max(latest_date) "latestDate"
          from stat_source
          group by ketto_toroku_bango
        ),
        ordered as (
          select
            *
          from stats
          order by ${getHorseOrder(query.order)}
          limit 200
        ),
        latest_rows as (
          select
            'jra'::text source,
            se.kaisai_nen,
            se.kaisai_tsukihi,
            se.keibajo_code,
            se.race_bango,
            se.ketto_toroku_bango,
            coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') bamei
          from ${jvdSe} se
          join ordered stats on stats."kettoTorokuBango" = se.ketto_toroku_bango
          where ${query.source === "nar" ? sql`false` : sql`true`}
          union all
          select
            'nar'::text source,
            se.kaisai_nen,
            se.kaisai_tsukihi,
            se.keibajo_code,
            se.race_bango,
            se.ketto_toroku_bango,
            coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') bamei
          from ${nvdSe} se
          join ordered stats on stats."kettoTorokuBango" = se.ketto_toroku_bango
          where ${query.source === "jra" ? sql`false` : sql`true`}
        ),
        latest as (
          select distinct on (ketto_toroku_bango)
            ketto_toroku_bango,
            bamei,
            source,
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango
          from latest_rows
          order by ketto_toroku_bango, kaisai_nen desc, kaisai_tsukihi desc, race_bango desc
        )
        select
          stats."kettoTorokuBango",
          coalesce(latest.bamei, stats.bamei) bamei,
          stats.starts,
          stats."winCount",
          stats."showCount",
          stats."winRate",
          stats."showRate",
          stats."latestDate",
          latest.keibajo_code "latestKeibajoCode",
          latest.race_bango "latestRaceBango",
          latest.source "latestSource",
          coalesce(nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '一般競走') "latestRaceName"
        from ordered stats
        left join latest on latest.ketto_toroku_bango = stats."kettoTorokuBango"
        left join ${jvdRa} jra
          on latest.source = 'jra'
          and jra.kaisai_nen = latest.kaisai_nen
          and jra.kaisai_tsukihi = latest.kaisai_tsukihi
          and jra.keibajo_code = latest.keibajo_code
          and jra.race_bango = latest.race_bango
        left join ${nvdRa} nar
          on latest.source = 'nar'
          and nar.kaisai_nen = latest.kaisai_nen
          and nar.kaisai_tsukihi = latest.kaisai_tsukihi
          and nar.keibajo_code = latest.keibajo_code
          and nar.race_bango = latest.race_bango
        cross join lateral (select coalesce(jra.kyosomei_hondai, nar.kyosomei_hondai) kyosomei_hondai) ra
      `);

      return result.rows.map((row) => ({
        bamei: row.bamei,
        kettoTorokuBango: row.kettoTorokuBango,
        latestDate: row.latestDate,
        latestKeibajoCode: row.latestKeibajoCode,
        latestRaceBango: row.latestRaceBango,
        latestRaceName: row.latestRaceName,
        latestSource: row.latestSource,
        primarySource: row.latestSource,
        showCount: toCount(row.showCount),
        showRate: toRate(row.showRate),
        starts: toCount(row.starts),
        winCount: toCount(row.winCount),
        winRate: toRate(row.winRate),
      }));
    }),
);

export const getPersonList = cache(
  async (
    kind: "jockeys" | "owners" | "trainers",
    query: EntityListQuery,
  ): Promise<PersonListRow[]> =>
    withDbQueryCache(["getPersonList", kind, query], async () => {
      const rawColumn =
        kind === "jockeys"
          ? sql`kishumei_ryakusho`
          : kind === "trainers"
            ? sql`chokyoshimei_ryakusho`
            : sql`banushimei`;
      const column =
        kind === "jockeys"
          ? sql`jockey_name`
          : kind === "trainers"
            ? sql`trainer_name`
            : sql`owner_name`;
      if (query.q === "" && query.order === "latest") {
        const result = await getDb().execute<{
          name: string;
          starts: string;
          winCount: string;
          showCount: string;
          winRate: string;
          showRate: string;
          latestDate: string;
          latestKeibajoCode: string;
          latestRaceBango: string;
          latestRaceName: string;
          latestSource: RaceSource;
          primarySource: RaceSource;
        }>(sql`
          with recent as (
            (
              select
                'jra'::text as source,
                kaisai_nen,
                kaisai_tsukihi,
                keibajo_code,
                race_bango,
                coalesce(nullif(regexp_replace(${rawColumn}, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as ${column},
                kakutei_chakujun
              from ${jvdSe}
              where ${query.source === "nar" ? sql`false` : sql`true`}
              order by kaisai_nen || kaisai_tsukihi desc, race_bango desc
              limit 8000
            )
            union all
            (
              select
                'nar'::text as source,
                kaisai_nen,
                kaisai_tsukihi,
                keibajo_code,
                race_bango,
                coalesce(nullif(regexp_replace(${rawColumn}, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as ${column},
                kakutei_chakujun
              from ${nvdSe}
              where ${query.source === "jra" ? sql`false` : sql`true`}
              order by kaisai_nen || kaisai_tsukihi desc, race_bango desc
              limit 8000
            )
          ),
          latest as (
            select distinct on (${column})
              ${column} as name,
              source,
              kaisai_nen,
              kaisai_tsukihi,
              keibajo_code,
              race_bango,
              kaisai_nen || kaisai_tsukihi as latest_date
            from recent
            where ${column} <> '-'
            order by ${column}, kaisai_nen desc, kaisai_tsukihi desc, race_bango desc
          ),
          candidates as (
            select *
            from latest
            order by latest_date desc, race_bango desc
            limit 200
          ),
          stats as (
            select
              ${column} as name,
              count(*)::text as starts,
              count(*) filter (where kakutei_chakujun = '01')::text as "winCount",
              count(*) filter (where kakutei_chakujun in ('01','02','03'))::text as "showCount",
              case
                when count(*) filter (where source = 'jra') >= count(*) filter (where source = 'nar')
                then 'jra'
                else 'nar'
              end as "primarySource",
              round(count(*) filter (where kakutei_chakujun = '01') * 100.0 / nullif(count(*), 0), 1)::text as "winRate",
              round(count(*) filter (where kakutei_chakujun in ('01','02','03')) * 100.0 / nullif(count(*), 0), 1)::text as "showRate"
            from recent
            where ${column} in (select name from candidates)
            group by ${column}
          )
          select
            c.name,
            coalesce(stats.starts, '0') as starts,
            coalesce(stats."winCount", '0') as "winCount",
            coalesce(stats."showCount", '0') as "showCount",
            coalesce(stats."winRate", '0') as "winRate",
            coalesce(stats."showRate", '0') as "showRate",
            c.latest_date as "latestDate",
            c.keibajo_code "latestKeibajoCode",
            c.race_bango "latestRaceBango",
            c.source as "latestSource",
            coalesce(stats."primarySource", c.source) as "primarySource",
            coalesce(nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '一般競走') as "latestRaceName"
          from candidates c
          left join stats on stats.name = c.name
          left join ${jvdRa} jra
            on c.source = 'jra'
            and jra.kaisai_nen = c.kaisai_nen
            and jra.kaisai_tsukihi = c.kaisai_tsukihi
            and jra.keibajo_code = c.keibajo_code
            and jra.race_bango = c.race_bango
          left join ${nvdRa} nar
            on c.source = 'nar'
            and nar.kaisai_nen = c.kaisai_nen
            and nar.kaisai_tsukihi = c.kaisai_tsukihi
            and nar.keibajo_code = c.keibajo_code
            and nar.race_bango = c.race_bango
          cross join lateral (select coalesce(jra.kyosomei_hondai, nar.kyosomei_hondai) as kyosomei_hondai) ra
          order by c.latest_date desc, c.race_bango desc
        `);

        return result.rows.map((row) => ({
          latestDate: row.latestDate,
          latestKeibajoCode: row.latestKeibajoCode,
          latestRaceBango: row.latestRaceBango,
          latestRaceName: row.latestRaceName,
          latestSource: row.latestSource,
          name: row.name,
          primarySource: row.primarySource,
          showCount: toCount(row.showCount),
          showRate: toRate(row.showRate),
          starts: toCount(row.starts),
          winCount: toCount(row.winCount),
          winRate: toRate(row.winRate),
        }));
      }

      const result = await getDb().execute<{
        name: string;
        starts: string;
        winCount: string;
        showCount: string;
        winRate: string;
        showRate: string;
        latestDate: string;
        latestKeibajoCode: string;
        latestRaceBango: string;
        latestRaceName: string;
        latestSource: RaceSource;
        primarySource: RaceSource;
      }>(sql`
        with entries as (
          select
            'jra'::text as source,
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            coalesce(nullif(regexp_replace(${rawColumn}, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as ${column},
            kakutei_chakujun
          from ${jvdSe}
          union all
          select
            'nar'::text as source,
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            coalesce(nullif(regexp_replace(${rawColumn}, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as ${column},
            kakutei_chakujun
          from ${nvdSe}
        ),
        filtered as (
          select *
          from entries
          where
            ${getEntitySourceCondition(query.source)}
            and ${column} <> '-'
            and (${query.q} = '' or ${column} ilike ${`%${query.q}%`})
        ),
        latest as (
          select distinct on (${column})
            ${column} as name,
            source,
            kaisai_nen,
            kaisai_tsukihi,
            keibajo_code,
            race_bango,
            kaisai_nen || kaisai_tsukihi as latest_date
          from filtered
          order by ${column}, kaisai_nen desc, kaisai_tsukihi desc, race_bango desc
        ),
        stats as (
          select
            ${column} as name,
            count(*)::text as starts,
            count(*) filter (where kakutei_chakujun = '01')::text as "winCount",
            count(*) filter (where kakutei_chakujun in ('01','02','03'))::text as "showCount",
            case
              when count(*) filter (where source = 'jra') >= count(*) filter (where source = 'nar')
              then 'jra'
              else 'nar'
            end as "primarySource",
            round(count(*) filter (where kakutei_chakujun = '01') * 100.0 / nullif(count(*), 0), 1)::text as "winRate",
            round(count(*) filter (where kakutei_chakujun in ('01','02','03')) * 100.0 / nullif(count(*), 0), 1)::text as "showRate"
          from filtered
          group by ${column}
        ),
        ordered as (
          select
            stats.name,
            stats.starts,
            stats."winCount",
            stats."showCount",
            stats."winRate",
            stats."showRate",
            stats."primarySource",
            latest.latest_date as "latestDate",
            latest.source as "latestSource",
            latest.kaisai_nen,
            latest.kaisai_tsukihi,
            latest.keibajo_code,
            latest.race_bango
          from stats
          join latest on latest.name = stats.name
          order by ${getPersonOrder(query.order)}
          limit 200
        )
        select
          stats.name,
          stats.starts,
          stats."winCount",
          stats."showCount",
          stats."winRate",
          stats."showRate",
          stats."primarySource",
          stats."latestDate",
          stats.keibajo_code "latestKeibajoCode",
          stats.race_bango "latestRaceBango",
          stats."latestSource",
          coalesce(nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '一般競走') as "latestRaceName"
        from ordered stats
        left join ${jvdRa} jra
          on stats."latestSource" = 'jra'
          and jra.kaisai_nen = stats.kaisai_nen
          and jra.kaisai_tsukihi = stats.kaisai_tsukihi
          and jra.keibajo_code = stats.keibajo_code
          and jra.race_bango = stats.race_bango
        left join ${nvdRa} nar
          on stats."latestSource" = 'nar'
          and nar.kaisai_nen = stats.kaisai_nen
          and nar.kaisai_tsukihi = stats.kaisai_tsukihi
          and nar.keibajo_code = stats.keibajo_code
          and nar.race_bango = stats.race_bango
        cross join lateral (select coalesce(jra.kyosomei_hondai, nar.kyosomei_hondai) as kyosomei_hondai) ra
      `);

      return result.rows.map((row) => ({
        latestDate: row.latestDate,
        latestKeibajoCode: row.latestKeibajoCode,
        latestRaceBango: row.latestRaceBango,
        latestRaceName: row.latestRaceName,
        latestSource: row.latestSource,
        name: row.name,
        primarySource: row.primarySource,
        showCount: toCount(row.showCount),
        showRate: toRate(row.showRate),
        starts: toCount(row.starts),
        winCount: toCount(row.winCount),
        winRate: toRate(row.winRate),
      }));
    }),
);

export interface FavoriteSearchCandidate {
  id: string;
  label: string;
  latestDate: string;
  starts: number;
}

const favoriteSearchPattern = (q: string) => `%${q}%`;
const favoriteSearchPrefixPattern = (q: string) => `${q}%`;
const favoriteSearchRecentRowLimit = 60_000;

export const searchFavoriteHorses = cache(
  async (q: string, limit = 20): Promise<FavoriteSearchCandidate[]> =>
    withDbQueryCache(["searchFavoriteHorses", q, limit], async () => {
      const result = await getDb().execute<{
        id: string;
        label: string;
        latestDate: string;
        starts: string;
      }>(sql`
        with entries as (
          (
            select
              ketto_toroku_bango as id,
              nullif(btrim(coalesce(bamei, '')), '') as label,
              kaisai_nen || kaisai_tsukihi as race_date
            from ${jvdSe}
            where btrim(coalesce(ketto_toroku_bango, '')) <> ''
            order by kaisai_nen || kaisai_tsukihi desc, race_bango desc
            limit ${favoriteSearchRecentRowLimit}
          )
          union all
          (
            select
              ketto_toroku_bango as id,
              nullif(btrim(coalesce(bamei, '')), '') as label,
              kaisai_nen || kaisai_tsukihi as race_date
            from ${nvdSe}
            where btrim(coalesce(ketto_toroku_bango, '')) <> ''
            order by kaisai_nen || kaisai_tsukihi desc, race_bango desc
            limit ${favoriteSearchRecentRowLimit}
          )
        ),
        filtered as (
          select *
          from entries
          where
            id = ${q}
            or label ilike ${favoriteSearchPattern(q)}
        ),
        grouped as (
          select
            id,
            coalesce(min(label) filter (where label is not null), id) as label,
            max(race_date) as "latestDate",
            count(*)::text as starts,
            case
              when coalesce(min(label) filter (where label is not null), '') ilike ${favoriteSearchPrefixPattern(q)}
                or id = ${q}
              then 0
              else 1
            end as priority
          from filtered
          group by id
        )
        select id, label, "latestDate", starts
        from grouped
        order by priority asc, "latestDate" desc, starts::numeric desc, label asc
        limit ${limit}
      `);

      if (result.rows.length > 0) {
        return result.rows.map((row) => ({
          id: row.id,
          label: row.label,
          latestDate: row.latestDate,
          starts: toCount(row.starts),
        }));
      }

      const fallback = await getDb().execute<{
        id: string;
        label: string;
        latestDate: string;
        starts: string;
      }>(sql`
        with entries as (
          select
            ketto_toroku_bango as id,
            nullif(btrim(coalesce(bamei, '')), '') as label,
            kaisai_nen || kaisai_tsukihi as race_date
          from ${jvdSe}
          where btrim(coalesce(ketto_toroku_bango, '')) <> ''
            and (
              ketto_toroku_bango = ${q}
              or bamei ilike ${favoriteSearchPattern(q)}
            )
          union all
          select
            ketto_toroku_bango as id,
            nullif(btrim(coalesce(bamei, '')), '') as label,
            kaisai_nen || kaisai_tsukihi as race_date
          from ${nvdSe}
          where btrim(coalesce(ketto_toroku_bango, '')) <> ''
            and (
              ketto_toroku_bango = ${q}
              or bamei ilike ${favoriteSearchPattern(q)}
            )
        ),
        grouped as (
          select
            id,
            coalesce(min(label) filter (where label is not null), id) as label,
            max(race_date) as "latestDate",
            count(*)::text as starts,
            case
              when coalesce(min(label) filter (where label is not null), '') ilike ${favoriteSearchPrefixPattern(q)}
                or id = ${q}
              then 0
              else 1
            end as priority
          from entries
          group by id
        )
        select id, label, "latestDate", starts
        from grouped
        order by priority asc, "latestDate" desc, starts::numeric desc, label asc
        limit ${limit}
      `);

      return fallback.rows.map((row) => ({
        id: row.id,
        label: row.label,
        latestDate: row.latestDate,
        starts: toCount(row.starts),
      }));
    }),
);

export const searchFavoritePeople = cache(
  async (
    kind: "jockeys" | "owners" | "trainers",
    q: string,
    limit = 20,
  ): Promise<FavoriteSearchCandidate[]> =>
    withDbQueryCache(["searchFavoritePeople", kind, q, limit], async () => {
      const rawColumn =
        kind === "jockeys"
          ? sql`kishumei_ryakusho`
          : kind === "trainers"
            ? sql`chokyoshimei_ryakusho`
            : sql`banushimei`;
      const result = await getDb().execute<{
        id: string;
        label: string;
        latestDate: string;
        starts: string;
      }>(sql`
        with entries as (
          (
            select
              nullif(btrim(coalesce(${rawColumn}, '')), '') as name,
              kaisai_nen || kaisai_tsukihi as race_date
            from ${jvdSe}
            order by kaisai_nen || kaisai_tsukihi desc, race_bango desc
            limit ${favoriteSearchRecentRowLimit}
          )
          union all
          (
            select
              nullif(btrim(coalesce(${rawColumn}, '')), '') as name,
              kaisai_nen || kaisai_tsukihi as race_date
            from ${nvdSe}
            order by kaisai_nen || kaisai_tsukihi desc, race_bango desc
            limit ${favoriteSearchRecentRowLimit}
          )
        ),
        filtered as (
          select *
          from entries
          where name ilike ${favoriteSearchPattern(q)}
        ),
        grouped as (
          select
            name as id,
            name as label,
            max(race_date) as "latestDate",
            count(*)::text as starts,
            case when name ilike ${favoriteSearchPrefixPattern(q)} then 0 else 1 end as priority
          from filtered
          where name is not null
          group by name
        )
        select id, label, "latestDate", starts
        from grouped
        order by priority asc, "latestDate" desc, starts::numeric desc, label asc
        limit ${limit}
      `);

      if (result.rows.length > 0) {
        return result.rows.map((row) => ({
          id: row.id,
          label: row.label,
          latestDate: row.latestDate,
          starts: toCount(row.starts),
        }));
      }

      const fallback = await getDb().execute<{
        id: string;
        label: string;
        latestDate: string;
        starts: string;
      }>(sql`
        with entries as (
          select
            nullif(btrim(coalesce(${rawColumn}, '')), '') as name,
            kaisai_nen || kaisai_tsukihi as race_date
          from ${jvdSe}
          where ${rawColumn} ilike ${favoriteSearchPattern(q)}
          union all
          select
            nullif(btrim(coalesce(${rawColumn}, '')), '') as name,
            kaisai_nen || kaisai_tsukihi as race_date
          from ${nvdSe}
          where ${rawColumn} ilike ${favoriteSearchPattern(q)}
        ),
        grouped as (
          select
            name as id,
            name as label,
            max(race_date) as "latestDate",
            count(*)::text as starts,
            case when name ilike ${favoriteSearchPrefixPattern(q)} then 0 else 1 end as priority
          from entries
          where name is not null
          group by name
        )
        select id, label, "latestDate", starts
        from grouped
        order by priority asc, "latestDate" desc, starts::numeric desc, label asc
        limit ${limit}
      `);

      return fallback.rows.map((row) => ({
        id: row.id,
        label: row.label,
        latestDate: row.latestDate,
        starts: toCount(row.starts),
      }));
    }),
);

const entityResultsOrder = (order: string) => {
  switch (order) {
    case "rank":
      return sql`nullif(kakutei_chakujun, '00')::int asc nulls last, race_date desc`;
    case "odds":
      return sql`nullif(tansho_odds, '0000')::int asc nulls last, race_date desc`;
    case "time":
      return sql`nullif(soha_time, '0000')::int asc nulls last, race_date desc`;
    default:
      return sql`race_date desc, race_bango desc`;
  }
};

const normalizeEntityDate = (value: string): string =>
  /^\d{4}-\d{2}-\d{2}$/.test(value) ? value.replaceAll("-", "") : "";

const normalizeEntityNumber = (value: string): number | null => {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
};

const normalizeEntityDecimalTenths = (value: string): number | null => {
  const trimmed = value.trim();
  if (trimmed === "") {
    return null;
  }
  if (trimmed.includes(":")) {
    const parts = trimmed.split(":");
    if (parts.length !== 2) {
      return null;
    }
    const minutes = Number(parts[0]);
    const seconds = Number(parts[1]);
    return Number.isFinite(minutes) && Number.isFinite(seconds)
      ? Math.round((minutes * 60 + seconds) * 10)
      : null;
  }
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) && parsed > 0 ? Math.round(parsed * 10) : null;
};

const getEntityDetailFilterCondition = (query: EntityListQuery) => {
  const date = normalizeEntityDate(query.date);
  const dateFrom = normalizeEntityDate(query.dateFrom);
  const dateTo = normalizeEntityDate(query.dateTo);
  const distanceMin = normalizeEntityNumber(query.distanceMin);
  const distanceMax = normalizeEntityNumber(query.distanceMax);
  const jockeyName = query.jockeyName.trim();
  const keibajoCode = query.keibajoCode.trim();
  const last3fMin = normalizeEntityDecimalTenths(query.last3fMin);
  const last3fMax = normalizeEntityDecimalTenths(query.last3fMax);
  const oddsMin = normalizeEntityDecimalTenths(query.oddsMin);
  const oddsMax = normalizeEntityDecimalTenths(query.oddsMax);
  const popularityMin = normalizeEntityNumber(query.popularityMin);
  const popularityMax = normalizeEntityNumber(query.popularityMax);
  const rawRaceNumber = query.raceNumber.trim();
  const raceNumber = rawRaceNumber.padStart(2, "0");
  const raceTimeMin = normalizeEntityDecimalTenths(query.raceTimeMin);
  const raceTimeMax = normalizeEntityDecimalTenths(query.raceTimeMax);
  const trainerName = query.trainerName.trim();

  return sql`
    and (${keibajoCode} = '' or keibajo_code = ${keibajoCode})
    and (${date} = '' or race_date = ${date})
    and (${dateFrom} = '' or race_date >= ${dateFrom})
    and (${dateTo} = '' or race_date <= ${dateTo})
    and (${rawRaceNumber} = '' or race_bango = ${raceNumber})
    and (${jockeyName} = '' or jockey_name ilike ${`%${jockeyName}%`})
    and (${trainerName} = '' or trainer_name ilike ${`%${trainerName}%`})
    and (${distanceMin}::int is null or nullif(kyori, '')::int >= ${distanceMin})
    and (${distanceMax}::int is null or nullif(kyori, '')::int <= ${distanceMax})
    and (
      ${query.surface} = 'all'
      or (${query.surface} = 'turf' and track_code like '1%')
      or (${query.surface} = 'dirt' and track_code like '2%')
      or (${query.surface} = 'obstacle' and track_code like '3%')
    )
    and (
      ${query.turn} = 'all'
      or (${query.turn} = 'left' and track_code in ('11', '12', '13', '14', '15', '16', '23', '25', '27'))
      or (${query.turn} = 'right' and track_code in ('17', '18', '19', '20', '21', '22', '24', '26', '28'))
    )
    and (${raceTimeMin}::int is null or nullif(soha_time, '0000')::int >= ${raceTimeMin})
    and (${raceTimeMax}::int is null or nullif(soha_time, '0000')::int <= ${raceTimeMax})
    and (${last3fMin}::int is null or nullif(kohan_3f, '000')::int >= ${last3fMin})
    and (${last3fMax}::int is null or nullif(kohan_3f, '000')::int <= ${last3fMax})
    and (${popularityMin}::int is null or nullif(tansho_ninkijun, '00')::int >= ${popularityMin})
    and (${popularityMax}::int is null or nullif(tansho_ninkijun, '00')::int <= ${popularityMax})
    and (${oddsMin}::int is null or nullif(tansho_odds, '0000')::int >= ${oddsMin})
    and (${oddsMax}::int is null or nullif(tansho_odds, '0000')::int <= ${oddsMax})
    and (
      ${query.rank} = 'all'
      or (${query.rank} = 'win' and kakutei_chakujun = '01')
      or (${query.rank} = 'top2' and kakutei_chakujun in ('01', '02'))
      or (${query.rank} = 'top3' and kakutei_chakujun in ('01', '02', '03'))
      or (
        ${query.rank} = 'out'
        and nullif(kakutei_chakujun, '00') ~ '^[0-9]+$'
        and nullif(kakutei_chakujun, '00')::int >= 4
      )
      or (${query.rank} = 'upcoming' and is_upcoming)
    )
  `;
};

const getEntityResultRows = async (
  whereSql: ReturnType<typeof sql>,
  order: string,
): Promise<EntityRaceResult[]> => {
  const result = await getDb().execute<{
    source: RaceSource;
    kaisaiNen: string;
    kaisaiTsukihi: string;
    keibajoCode: string;
    raceBango: string;
    raceName: string;
    hassoJikoku: string | null;
    kyori: string | null;
    trackCode: string | null;
    kettoTorokuBango: string | null;
    horseName: string;
    jockeyName: string;
    trainerName: string;
    ownerName: string;
    horseNumber: string | null;
    frameNumber: string | null;
    rank: string | null;
    popularity: string | null;
    winOdds: string | null;
    raceTime: string | null;
    last3f: string | null;
    corner1: string | null;
    corner2: string | null;
    corner3: string | null;
    corner4: string | null;
    isUpcoming: boolean;
  }>(sql`
    with rows as (${entityRaceRowsSql}),
    dated as (
      select *, kaisai_nen || kaisai_tsukihi as race_date
      from rows
    ),
    filtered as (
      select *
      from dated
      where ${whereSql}
    )
    select
      source,
      kaisai_nen as "kaisaiNen",
      kaisai_tsukihi as "kaisaiTsukihi",
      keibajo_code as "keibajoCode",
      race_bango as "raceBango",
      race_name as "raceName",
      hasso_jikoku as "hassoJikoku",
      kyori,
      track_code as "trackCode",
      ketto_toroku_bango "kettoTorokuBango",
      bamei as "horseName",
      jockey_name as "jockeyName",
      trainer_name as "trainerName",
      owner_name "ownerName",
      umaban as "horseNumber",
      wakuban as "frameNumber",
      kakutei_chakujun as rank,
      tansho_ninkijun as popularity,
      tansho_odds as "winOdds",
      soha_time as "raceTime",
      kohan_3f as "last3f",
      corner_1 as "corner1",
      corner_2 as "corner2",
      corner_3 as "corner3",
      corner_4 as "corner4",
      is_upcoming as "isUpcoming"
    from filtered
    order by ${entityResultsOrder(order)}
    limit 300
  `);
  return result.rows;
};

const getPersonResultRows = async (
  kind: "jockeys" | "owners" | "trainers",
  name: string,
  query: EntityListQuery,
): Promise<EntityRaceResult[]> => {
  const rawColumn =
    kind === "jockeys"
      ? sql`se.kishumei_ryakusho`
      : kind === "trainers"
        ? sql`se.chokyoshimei_ryakusho`
        : sql`se.banushimei`;
  const result = await getDb().execute<{
    source: RaceSource;
    kaisaiNen: string;
    kaisaiTsukihi: string;
    keibajoCode: string;
    raceBango: string;
    raceName: string;
    hassoJikoku: string | null;
    kyori: string | null;
    trackCode: string | null;
    kettoTorokuBango: string | null;
    horseName: string;
    jockeyName: string;
    trainerName: string;
    ownerName: string;
    horseNumber: string | null;
    frameNumber: string | null;
    rank: string | null;
    popularity: string | null;
    winOdds: string | null;
    raceTime: string | null;
    last3f: string | null;
    corner1: string | null;
    corner2: string | null;
    corner3: string | null;
    corner4: string | null;
    isUpcoming: boolean;
  }>(sql`
    with rows as (
      select
        'jra'::text as source,
        ra.kaisai_nen,
        ra.kaisai_tsukihi,
        ra.keibajo_code,
        ra.race_bango,
        coalesce(nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '一般競走') as race_name,
        ra.hasso_jikoku,
        ra.kyori,
        ra.track_code,
        se.wakuban,
        se.umaban,
        se.ketto_toroku_bango,
        coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as bamei,
        coalesce(nullif(regexp_replace(se.kishumei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as jockey_name,
        coalesce(nullif(regexp_replace(se.chokyoshimei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as trainer_name,
        coalesce(nullif(regexp_replace(se.banushimei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') owner_name,
        se.kakutei_chakujun,
        se.tansho_ninkijun,
        se.tansho_odds,
        se.soha_time,
        se.kohan_3f,
        se.corner_1,
        se.corner_2,
        se.corner_3,
        se.corner_4,
        (
          ra.kaisai_nen || ra.kaisai_tsukihi >= to_char((now() at time zone 'Asia/Tokyo')::date, 'YYYYMMDD')
          and coalesce(nullif(se.kakutei_chakujun, '00'), '') = ''
        ) as is_upcoming
      from ${jvdSe} se
      join ${jvdRa} ra
        on ra.kaisai_nen = se.kaisai_nen
        and ra.kaisai_tsukihi = se.kaisai_tsukihi
        and ra.keibajo_code = se.keibajo_code
        and ra.race_bango = se.race_bango
      where
        ${query.source === "nar" ? sql`false` : sql`true`}
        and coalesce(nullif(btrim(${rawColumn}, ' 　'), ''), '-') = ${name}
      union all
      select
        'nar'::text as source,
        ra.kaisai_nen,
        ra.kaisai_tsukihi,
        ra.keibajo_code,
        ra.race_bango,
        coalesce(nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '一般競走') as race_name,
        ra.hasso_jikoku,
        ra.kyori,
        ra.track_code,
        se.wakuban,
        se.umaban,
        se.ketto_toroku_bango,
        coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as bamei,
        coalesce(nullif(regexp_replace(se.kishumei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as jockey_name,
        coalesce(nullif(regexp_replace(se.chokyoshimei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as trainer_name,
        coalesce(nullif(regexp_replace(se.banushimei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') owner_name,
        se.kakutei_chakujun,
        se.tansho_ninkijun,
        se.tansho_odds,
        se.soha_time,
        se.kohan_3f,
        se.corner_1,
        se.corner_2,
        se.corner_3,
        se.corner_4,
        (
          ra.kaisai_nen || ra.kaisai_tsukihi >= to_char((now() at time zone 'Asia/Tokyo')::date, 'YYYYMMDD')
          and coalesce(nullif(se.kakutei_chakujun, '00'), '') = ''
        ) as is_upcoming
      from ${nvdSe} se
      join ${nvdRa} ra
        on ra.kaisai_nen = se.kaisai_nen
        and ra.kaisai_tsukihi = se.kaisai_tsukihi
        and ra.keibajo_code = se.keibajo_code
        and ra.race_bango = se.race_bango
      where
        ${query.source === "jra" ? sql`false` : sql`true`}
        and coalesce(nullif(btrim(${rawColumn}, ' 　'), ''), '-') = ${name}
    ),
    dated as (
      select *, kaisai_nen || kaisai_tsukihi as race_date
      from rows
    ),
    filtered as (
      select *
      from dated
      where
        (${query.q} = '' or bamei ilike ${`%${query.q}%`} or race_name ilike ${`%${query.q}%`})
        ${getEntityDetailFilterCondition(query)}
    )
    select
      source,
      kaisai_nen as "kaisaiNen",
      kaisai_tsukihi as "kaisaiTsukihi",
      keibajo_code as "keibajoCode",
      race_bango as "raceBango",
      race_name as "raceName",
      hasso_jikoku as "hassoJikoku",
      kyori,
      track_code as "trackCode",
      ketto_toroku_bango "kettoTorokuBango",
      bamei as "horseName",
      jockey_name as "jockeyName",
      trainer_name as "trainerName",
      owner_name "ownerName",
      umaban as "horseNumber",
      wakuban as "frameNumber",
      kakutei_chakujun as rank,
      tansho_ninkijun as popularity,
      tansho_odds as "winOdds",
      soha_time as "raceTime",
      kohan_3f as "last3f",
      corner_1 as "corner1",
      corner_2 as "corner2",
      corner_3 as "corner3",
      corner_4 as "corner4",
      is_upcoming as "isUpcoming"
    from filtered
    order by ${entityResultsOrder(query.order)}
    limit 300
  `);
  return result.rows;
};

const averageEntityMetric = (values: number[]): number | null =>
  values.length > 0
    ? Math.round((values.reduce((sum, value) => sum + value, 0) / values.length) * 10) / 10
    : null;

const summarizeEntityResults = (name: string, rows: EntityRaceResult[]): EntityDetailSummary => {
  const starts = rows.length;
  const winCount = rows.filter((row) => row.rank === "01").length;
  const quinellaCount = rows.filter((row) => row.rank === "01" || row.rank === "02").length;
  const showCount = rows.filter((row) => ["01", "02", "03"].includes(row.rank ?? "")).length;
  const popularities = rows
    .map((row) => Number(row.popularity))
    .filter((value) => Number.isFinite(value) && value > 0);
  const odds = rows
    .map((row) => Number(row.winOdds))
    .filter((value) => Number.isFinite(value) && value > 0)
    .map((value) => value / 10);

  return {
    averageOdds: averageEntityMetric(odds),
    averagePopularity: averageEntityMetric(popularities),
    name,
    quinellaCount,
    quinellaRate: starts > 0 ? Math.round((quinellaCount * 1000) / starts) / 10 : 0,
    showCount,
    showRate: starts > 0 ? Math.round((showCount * 1000) / starts) / 10 : 0,
    starts,
    winCount,
    winRate: starts > 0 ? Math.round((winCount * 1000) / starts) / 10 : 0,
  };
};

export const getHorseDetailData = cache(
  async (
    kettoTorokuBango: string,
    query: EntityListQuery,
  ): Promise<{ results: EntityRaceResult[]; summary: EntityDetailSummary } | null> =>
    withDbQueryCache(["getHorseDetailData", kettoTorokuBango, query], async () => {
      const rows = await getEntityResultRows(
        sql`
          ketto_toroku_bango = ${kettoTorokuBango}
          and ${getEntitySourceCondition(query.source)}
          ${getEntityDetailFilterCondition(query)}
        `,
        query.order,
      );
      if (rows.length === 0) {
        const nameResult = await getDb().execute<{ bamei: string }>(sql`
          (
            select coalesce(nullif(regexp_replace(bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), ${kettoTorokuBango}) bamei
            from ${jvdSe}
            where ketto_toroku_bango = ${kettoTorokuBango}
            limit 1
          )
          union all
          (
            select coalesce(nullif(regexp_replace(bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), ${kettoTorokuBango}) bamei
            from ${nvdSe}
            where ketto_toroku_bango = ${kettoTorokuBango}
            limit 1
          )
          limit 1
        `);
        const name = nameResult.rows[0]?.bamei;
        return name
          ? {
              results: [],
              summary: summarizeEntityResults(name, []),
            }
          : null;
      }
      return {
        results: rows,
        summary: summarizeEntityResults(rows[0]?.horseName ?? kettoTorokuBango, rows),
      };
    }),
);

export const getPersonDetailData = cache(
  async (
    kind: "jockeys" | "owners" | "trainers",
    name: string,
    query: EntityListQuery,
  ): Promise<{ results: EntityRaceResult[]; summary: EntityDetailSummary } | null> =>
    withDbQueryCache(["getPersonDetailData", kind, name, query], async () => {
      const rows = await getPersonResultRows(kind, name, query);
      if (rows.length === 0) {
        return null;
      }
      return { results: rows, summary: summarizeEntityResults(name, rows) };
    }),
);

const getJstMinuteKey = (): string => {
  const parts = new Intl.DateTimeFormat("ja-JP", {
    day: "2-digit",
    hour: "2-digit",
    hourCycle: "h23",
    minute: "2-digit",
    month: "2-digit",
    timeZone: "Asia/Tokyo",
    year: "numeric",
  }).formatToParts(new Date());
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${values.year}${values.month}${values.day}${values.hour}${values.minute}`;
};

export const getTopRaceWindows = cache(
  async (): Promise<{ finished: TopRaceSummary[]; upcoming: TopRaceSummary[] }> => {
    const nowKey = getJstMinuteKey();
    return withDbQueryCache(["getTopRaceWindows", nowKey], async () => {
      const result = await getDb().execute<TopRaceSummary & { bucket: number }>(sql`
        with candidates as (
          (
            select
              'jra'::text source,
              kaisai_nen "kaisaiNen",
              kaisai_tsukihi "kaisaiTsukihi",
              keibajo_code "keibajoCode",
              race_bango "raceBango",
              kyosomei_hondai "kyosomeiHondai",
              kyosomei_fukudai "kyosomeiFukudai",
              grade_code "gradeCode",
              kyoso_shubetsu_code "kyosoShubetsuCode",
              kyoso_kigo_code "kyosoKigoCode",
              juryo_shubetsu_code "juryoShubetsuCode",
              kyoso_joken_code "kyosoJokenCode",
              kyoso_joken_meisho "kyosoJokenMeisho",
              kyori,
              track_code "trackCode",
              hasso_jikoku "hassoJikoku",
              shusso_tosu "shussoTosu",
              kaisai_nen || kaisai_tsukihi || right('0000' || coalesce(nullif(regexp_replace(coalesce(hasso_jikoku, ''), '[^0-9]', '', 'g'), ''), '0000'), 4) start_key,
              0 bucket
            from ${jvdRa}
            where kaisai_nen || kaisai_tsukihi || right('0000' || coalesce(nullif(regexp_replace(coalesce(hasso_jikoku, ''), '[^0-9]', '', 'g'), ''), '0000'), 4) >= ${nowKey}
            order by kaisai_nen || kaisai_tsukihi || right('0000' || coalesce(nullif(regexp_replace(coalesce(hasso_jikoku, ''), '[^0-9]', '', 'g'), ''), '0000'), 4) asc,
              keibajo_code asc, race_bango asc
            limit 240
          )
          union all
          (
            select
              'nar'::text source,
              kaisai_nen "kaisaiNen",
              kaisai_tsukihi "kaisaiTsukihi",
              keibajo_code "keibajoCode",
              race_bango "raceBango",
              kyosomei_hondai "kyosomeiHondai",
              kyosomei_fukudai "kyosomeiFukudai",
              grade_code "gradeCode",
              kyoso_shubetsu_code "kyosoShubetsuCode",
              kyoso_kigo_code "kyosoKigoCode",
              juryo_shubetsu_code "juryoShubetsuCode",
              kyoso_joken_code "kyosoJokenCode",
              kyoso_joken_meisho "kyosoJokenMeisho",
              kyori,
              track_code "trackCode",
              hasso_jikoku "hassoJikoku",
              shusso_tosu "shussoTosu",
              kaisai_nen || kaisai_tsukihi || right('0000' || coalesce(nullif(regexp_replace(coalesce(hasso_jikoku, ''), '[^0-9]', '', 'g'), ''), '0000'), 4) start_key,
              0 bucket
            from ${nvdRa}
            where kaisai_nen || kaisai_tsukihi || right('0000' || coalesce(nullif(regexp_replace(coalesce(hasso_jikoku, ''), '[^0-9]', '', 'g'), ''), '0000'), 4) >= ${nowKey}
            order by kaisai_nen || kaisai_tsukihi || right('0000' || coalesce(nullif(regexp_replace(coalesce(hasso_jikoku, ''), '[^0-9]', '', 'g'), ''), '0000'), 4) asc,
              keibajo_code asc, race_bango asc
            limit 240
          )
          union all
          (
            select
              'jra'::text source,
              kaisai_nen "kaisaiNen",
              kaisai_tsukihi "kaisaiTsukihi",
              keibajo_code "keibajoCode",
              race_bango "raceBango",
              kyosomei_hondai "kyosomeiHondai",
              kyosomei_fukudai "kyosomeiFukudai",
              grade_code "gradeCode",
              kyoso_shubetsu_code "kyosoShubetsuCode",
              kyoso_kigo_code "kyosoKigoCode",
              juryo_shubetsu_code "juryoShubetsuCode",
              kyoso_joken_code "kyosoJokenCode",
              kyoso_joken_meisho "kyosoJokenMeisho",
              kyori,
              track_code "trackCode",
              hasso_jikoku "hassoJikoku",
              shusso_tosu "shussoTosu",
              kaisai_nen || kaisai_tsukihi || right('0000' || coalesce(nullif(regexp_replace(coalesce(hasso_jikoku, ''), '[^0-9]', '', 'g'), ''), '0000'), 4) start_key,
              1 bucket
            from ${jvdRa}
            where kaisai_nen || kaisai_tsukihi || right('0000' || coalesce(nullif(regexp_replace(coalesce(hasso_jikoku, ''), '[^0-9]', '', 'g'), ''), '0000'), 4) < ${nowKey}
            order by kaisai_nen || kaisai_tsukihi || right('0000' || coalesce(nullif(regexp_replace(coalesce(hasso_jikoku, ''), '[^0-9]', '', 'g'), ''), '0000'), 4) desc,
              keibajo_code desc, race_bango desc
            limit 60
          )
          union all
          (
            select
              'nar'::text source,
              kaisai_nen "kaisaiNen",
              kaisai_tsukihi "kaisaiTsukihi",
              keibajo_code "keibajoCode",
              race_bango "raceBango",
              kyosomei_hondai "kyosomeiHondai",
              kyosomei_fukudai "kyosomeiFukudai",
              grade_code "gradeCode",
              kyoso_shubetsu_code "kyosoShubetsuCode",
              kyoso_kigo_code "kyosoKigoCode",
              juryo_shubetsu_code "juryoShubetsuCode",
              kyoso_joken_code "kyosoJokenCode",
              kyoso_joken_meisho "kyosoJokenMeisho",
              kyori,
              track_code "trackCode",
              hasso_jikoku "hassoJikoku",
              shusso_tosu "shussoTosu",
              kaisai_nen || kaisai_tsukihi || right('0000' || coalesce(nullif(regexp_replace(coalesce(hasso_jikoku, ''), '[^0-9]', '', 'g'), ''), '0000'), 4) start_key,
              1 bucket
            from ${nvdRa}
            where kaisai_nen || kaisai_tsukihi || right('0000' || coalesce(nullif(regexp_replace(coalesce(hasso_jikoku, ''), '[^0-9]', '', 'g'), ''), '0000'), 4) < ${nowKey}
            order by kaisai_nen || kaisai_tsukihi || right('0000' || coalesce(nullif(regexp_replace(coalesce(hasso_jikoku, ''), '[^0-9]', '', 'g'), ''), '0000'), 4) desc,
              keibajo_code desc, race_bango desc
            limit 60
          )
        )
        select
          source,
          "kaisaiNen",
          "kaisaiTsukihi",
          "keibajoCode",
          "raceBango",
          "kyosomeiHondai",
          "kyosomeiFukudai",
          "gradeCode",
          "kyosoShubetsuCode",
          "kyosoKigoCode",
          "juryoShubetsuCode",
          "kyosoJokenCode",
          "kyosoJokenMeisho",
          kyori,
          "trackCode",
          "hassoJikoku",
          "shussoTosu",
          (
            substring(start_key from 1 for 4) || '-' ||
            substring(start_key from 5 for 2) || '-' ||
            substring(start_key from 7 for 2) || 'T' ||
            substring(start_key from 9 for 2) || ':' ||
            substring(start_key from 11 for 2) || ':00+09:00'
          ) "raceStartAt",
          bucket
        from candidates
        order by bucket asc, start_key asc, "keibajoCode" asc, "raceBango" asc, source asc
      `);
      return {
        finished: result.rows
          .filter((row) => row.bucket === 1)
          .slice(-60)
          .toReversed(),
        upcoming: result.rows.filter((row) => row.bucket === 0).slice(0, 240),
      };
    });
  },
);

export const getRaceCourseInfo = cache(
  async (
    keibajoCode: string,
    kyori: string | null | undefined,
    trackCode: string | null | undefined,
  ): Promise<CourseInfo | null> => {
    if (!kyori || !trackCode) {
      return null;
    }

    return withDbQueryCache(["getRaceCourseInfo", keibajoCode, kyori, trackCode], async () => {
      const [course] = await getDb()
        .select({
          courseKaishuNengappi: jvdCs.courseKaishuNengappi,
          courseSetsumei: jvdCs.courseSetsumei,
        })
        .from(jvdCs)
        .where(
          and(
            eq(jvdCs.keibajoCode, keibajoCode),
            eq(jvdCs.kyori, kyori),
            eq(jvdCs.trackCode, trackCode),
          ),
        )
        .orderBy(desc(jvdCs.courseKaishuNengappi))
        .limit(1);

      return course ?? null;
    });
  },
);

export const getHorseRaceResults = async (
  source: RaceSource,
  year: string,
  month: string,
  day: string,
  keibajoCode: string,
  raceNumber: string,
  sourceScope: RaceSource | "all" = "all",
): Promise<HorseRaceResult[]> => {
  return withDbQueryCache(
    ["getHorseRaceResults", source, year, month, day, keibajoCode, raceNumber, sourceScope],
    async () => {
      const currentRunnerTable = source === "jra" ? jvdSe : nvdSe;
      const includeJraHistory = sourceScope === "all" || sourceScope === "jra";
      const includeNarHistory = sourceScope === "all" || sourceScope === "nar";
      const monthDay = `${month}${day}`;
      const raceDate = `${year}${monthDay}`;

      const result = await getDb().execute<HorseRaceResult>(sql`
      with current_horses as (
        select
          umaban as "currentUmaban",
          ketto_toroku_bango,
          seibetsu_code as "currentSeibetsuCode",
          barei as "currentBarei",
          coalesce(nullif(regexp_replace(kishumei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') as "currentJockey"
        from ${currentRunnerTable}
        where
          kaisai_nen = ${year}
          and kaisai_tsukihi = ${monthDay}
          and keibajo_code = ${keibajoCode}
          and race_bango = ${raceNumber}
          and ketto_toroku_bango is not null
          and btrim(ketto_toroku_bango) <> ''
      ),
      history as (
        select
          ch."currentJockey",
          ch."currentBarei",
          ch."currentSeibetsuCode",
          ch."currentUmaban",
          past."kaisaiNen",
          past."kaisaiTsukihi",
          past."keibajoCode",
          past."raceBango",
          past."kyosomeiHondai",
          past."kyosomeiFukudai",
          past."kyosomeiKakkonai",
          past."gradeCode",
          past."kyosoShubetsuCode",
          past."kyosoKigoCode",
          past."juryoShubetsuCode",
          past."kyosoJokenCode",
          past."kyosoJokenMeisho",
          past.kyori,
          past."trackCode",
          past."hassoJikoku",
          past."shussoTosu",
          past."tenkoCode",
          past."babajotaiCodeShiba",
          past."babajotaiCodeDirt",
          past.wakuban,
          past.umaban,
          past."kettoTorokuBango",
          past.bamei,
          past."seibetsuCode",
          past.barei,
          past."futanJuryo",
          past."kishumeiRyakusho",
          past."chokyoshimeiRyakusho",
          past.banushimei,
          past.bataiju,
          past."zogenFugo",
          past."zogenSa",
          past."kakuteiChakujun",
          past."tanshoOdds",
          past."tanshoNinkijun",
          past."sohaTime",
          past."timeSa",
          past."corner1",
          past."corner2",
          past."corner3",
          past."corner4",
          past."kohan3f",
          row_number() over (
            partition by ch."currentUmaban"
            order by past."kaisaiNen" desc, past."kaisaiTsukihi" desc, past."raceBango" desc
          ) as rn
        from current_horses ch
        join (
          select
            ra.kaisai_nen || ra.kaisai_tsukihi raceDate,
            ra.kaisai_nen "kaisaiNen",
            ra.kaisai_tsukihi "kaisaiTsukihi",
            ra.keibajo_code "keibajoCode",
            ra.race_bango "raceBango",
            ra.kyosomei_hondai "kyosomeiHondai",
            ra.kyosomei_fukudai "kyosomeiFukudai",
            ra.kyosomei_kakkonai "kyosomeiKakkonai",
            ra.grade_code "gradeCode",
            ra.kyoso_shubetsu_code "kyosoShubetsuCode",
            ra.kyoso_kigo_code "kyosoKigoCode",
            ra.juryo_shubetsu_code "juryoShubetsuCode",
            ra.kyoso_joken_code "kyosoJokenCode",
            ra.kyoso_joken_meisho "kyosoJokenMeisho",
            ra.kyori,
            ra.track_code "trackCode",
            ra.hasso_jikoku "hassoJikoku",
            ra.shusso_tosu "shussoTosu",
            ra.tenko_code "tenkoCode",
            ra.babajotai_code_shiba "babajotaiCodeShiba",
            ra.babajotai_code_dirt "babajotaiCodeDirt",
            se.wakuban,
            se.umaban,
            se.ketto_toroku_bango "kettoTorokuBango",
            se.bamei,
            se.seibetsu_code "seibetsuCode",
            se.barei,
            se.futan_juryo "futanJuryo",
            se.kishumei_ryakusho "kishumeiRyakusho",
            se.chokyoshimei_ryakusho "chokyoshimeiRyakusho",
            se.banushimei,
            se.bataiju,
            se.zogen_fugo "zogenFugo",
            se.zogen_sa "zogenSa",
            se.kakutei_chakujun "kakuteiChakujun",
            se.tansho_odds "tanshoOdds",
            se.tansho_ninkijun "tanshoNinkijun",
            se.soha_time "sohaTime",
            se.time_sa "timeSa",
            se.corner_1 "corner1",
            se.corner_2 "corner2",
            se.corner_3 "corner3",
            se.corner_4 "corner4",
            se.kohan_3f "kohan3f"
          from ${jvdSe} se
          join ${jvdRa} ra
            on ra.kaisai_nen = se.kaisai_nen
            and ra.kaisai_tsukihi = se.kaisai_tsukihi
            and ra.keibajo_code = se.keibajo_code
            and ra.race_bango = se.race_bango
          where
            ${includeJraHistory} = true
            and se.ketto_toroku_bango in (select ketto_toroku_bango from current_horses)
          union all
          select
            ra.kaisai_nen || ra.kaisai_tsukihi raceDate,
            ra.kaisai_nen "kaisaiNen",
            ra.kaisai_tsukihi "kaisaiTsukihi",
            ra.keibajo_code "keibajoCode",
            ra.race_bango "raceBango",
            ra.kyosomei_hondai "kyosomeiHondai",
            ra.kyosomei_fukudai "kyosomeiFukudai",
            ra.kyosomei_kakkonai "kyosomeiKakkonai",
            ra.grade_code "gradeCode",
            ra.kyoso_shubetsu_code "kyosoShubetsuCode",
            ra.kyoso_kigo_code "kyosoKigoCode",
            ra.juryo_shubetsu_code "juryoShubetsuCode",
            ra.kyoso_joken_code "kyosoJokenCode",
            ra.kyoso_joken_meisho "kyosoJokenMeisho",
            ra.kyori,
            ra.track_code "trackCode",
            ra.hasso_jikoku "hassoJikoku",
            ra.shusso_tosu "shussoTosu",
            ra.tenko_code "tenkoCode",
            ra.babajotai_code_shiba "babajotaiCodeShiba",
            ra.babajotai_code_dirt "babajotaiCodeDirt",
            se.wakuban,
            se.umaban,
            se.ketto_toroku_bango "kettoTorokuBango",
            se.bamei,
            se.seibetsu_code "seibetsuCode",
            se.barei,
            se.futan_juryo "futanJuryo",
            se.kishumei_ryakusho "kishumeiRyakusho",
            se.chokyoshimei_ryakusho "chokyoshimeiRyakusho",
            se.banushimei,
            se.bataiju,
            se.zogen_fugo "zogenFugo",
            se.zogen_sa "zogenSa",
            se.kakutei_chakujun "kakuteiChakujun",
            se.tansho_odds "tanshoOdds",
            se.tansho_ninkijun "tanshoNinkijun",
            se.soha_time "sohaTime",
            se.time_sa "timeSa",
            se.corner_1 "corner1",
            se.corner_2 "corner2",
            se.corner_3 "corner3",
            se.corner_4 "corner4",
            se.kohan_3f "kohan3f"
          from ${nvdSe} se
          join ${nvdRa} ra
            on ra.kaisai_nen = se.kaisai_nen
            and ra.kaisai_tsukihi = se.kaisai_tsukihi
            and ra.keibajo_code = se.keibajo_code
            and ra.race_bango = se.race_bango
          where
            ${includeNarHistory} = true
            and se.ketto_toroku_bango in (select ketto_toroku_bango from current_horses)
        ) past
          on past."kettoTorokuBango" = ch.ketto_toroku_bango
        where past.raceDate < ${raceDate}
      )
      select
        "currentJockey",
        "currentBarei",
        "currentSeibetsuCode",
        "currentUmaban",
        "kaisaiNen",
        "kaisaiTsukihi",
        "keibajoCode",
        "raceBango",
        "kyosomeiHondai",
        "kyosomeiFukudai",
        "kyosomeiKakkonai",
        "gradeCode",
        "kyosoShubetsuCode",
        "kyosoKigoCode",
        "juryoShubetsuCode",
        "kyosoJokenCode",
        "kyosoJokenMeisho",
        kyori,
        "trackCode",
        "hassoJikoku",
        "shussoTosu",
        "tenkoCode",
        "babajotaiCodeShiba",
        "babajotaiCodeDirt",
        wakuban,
        umaban,
        "kettoTorokuBango",
        bamei,
        "seibetsuCode",
        barei,
        "futanJuryo",
        "kishumeiRyakusho",
        "chokyoshimeiRyakusho",
        banushimei,
        bataiju,
        "zogenFugo",
        "zogenSa",
        "kakuteiChakujun",
        "tanshoOdds",
        "tanshoNinkijun",
        "sohaTime",
        "timeSa",
        "corner1",
        "corner2",
        "corner3",
        "corner4",
        "kohan3f"
      from history
      order by "currentUmaban"::int asc, "kaisaiNen" desc, "kaisaiTsukihi" desc, "raceBango" desc
    `);
      const rowsByRaceKey = new Map<string, HorseRaceResult>();
      for (const row of result.rows) {
        const raceKey = [
          row.currentUmaban,
          row.kaisaiNen,
          row.kaisaiTsukihi,
          row.keibajoCode,
          row.raceBango,
          row.kettoTorokuBango,
        ].join("-");
        if (!rowsByRaceKey.has(raceKey)) {
          rowsByRaceKey.set(raceKey, row);
        }
      }

      return [...rowsByRaceKey.values()];
    },
  );
};

const parseNumericText = (value: string | null | undefined, emptyValue: string): number | null => {
  const cleaned = value?.trim() ?? "";
  if (!cleaned || cleaned === emptyValue) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const buildCornerSimilarityVector = (
  race: RaceDetail,
  runner: Runner,
  runnerCount: number,
): string => {
  const distance = parseNumericText(race.kyori, "") ?? 0;
  const horseNumber = parseNumericText(runner.umaban, "") ?? 0;
  const popularity = parseNumericText(runner.tanshoNinkijun, "00") ?? runnerCount;
  const odds = (parseNumericText(runner.tanshoOdds, "0000") ?? 10) / 10;
  const trackCode = race.trackCode?.trim() ?? "";
  const venue = parseNumericText(race.keibajoCode, "") ?? 0;
  const raceNumber = parseNumericText(race.raceBango, "") ?? 0;
  const values = [
    Math.min(1, Math.max(0, distance / 3600)),
    Math.min(1, Math.max(0, runnerCount / 18)),
    Math.min(1, Math.max(0, horseNumber / Math.max(runnerCount, 1))),
    Math.min(1, Math.max(0, popularity / Math.max(runnerCount, 1))),
    Math.min(1, Math.max(0, Math.log(Math.max(odds, 1)) / Math.log(300))),
    trackCode.startsWith("1") ? 0 : 1,
    Math.min(1, Math.max(0, venue / 99)),
    Math.min(1, Math.max(0, raceNumber / 12)),
  ];
  return `[${values.map((value) => value.toFixed(6)).join(",")}]`;
};

export const getRacePaceSimilarityFeatures = cache(
  async (race: RaceDetail, runners: Runner[]): Promise<RacePaceSimilarityFeature[]> => {
    return withDbQueryCache(
      [
        "getRacePaceSimilarityFeatures",
        race.source,
        race.kaisaiNen,
        race.kaisaiTsukihi,
        race.keibajoCode,
        race.raceBango,
        runners.map((runner) => runner.umaban).join(","),
        runners.map((runner) => runner.tanshoNinkijun).join(","),
        runners.map((runner) => runner.tanshoOdds).join(","),
      ],
      async () => {
        const runnerCount = runners.length;
        if (runnerCount <= 1) {
          return [];
        }
        const rows = await Promise.all(
          runners.map(async (runner): Promise<RacePaceSimilarityFeature | null> => {
            const horseNumber = runner.umaban?.replace(/^0+/u, "") || runner.umaban || "";
            if (!horseNumber) {
              return null;
            }
            const distance = parseNumericText(race.kyori, "");
            const vector = buildCornerSimilarityVector(race, runner, runnerCount);
            try {
              const result = await getDb().execute<{
                corner1: string | null;
                corner2: string | null;
                corner3: string | null;
                corner4: string | null;
                neighbor_count: string;
                similarity_score: string | null;
              }>(sql`
              with nearest as (
                select *
                from (
                  select
                    corner1_norm,
                    corner2_norm,
                    corner3_norm,
                    corner4_norm,
                    feature_vector
                  from race_entry_corner_features
                  where
                    source = ${race.source}
                    and race_date < ${`${race.kaisaiNen}${race.kaisaiTsukihi}`}
                    and (${distance}::integer is null or kyori between ${distance}::integer - 400 and ${distance}::integer + 400)
                    and left(coalesce(track_code, ''), 1) = left(coalesce(${race.trackCode}, ''), 1)
                    and keibajo_code = ${race.keibajoCode}
                    and race_date >= ${`${Number(race.kaisaiNen) - 3}${race.kaisaiTsukihi}`}
                  order by race_date desc
                  limit 2500
                ) candidates
                order by feature_vector <-> ${vector}::vector
                limit 40
              ),
              weighted_nearest as (
                select
                  corner1_norm,
                  corner2_norm,
                  corner3_norm,
                  corner4_norm,
                  1 / (1 + (feature_vector <-> ${vector}::vector)) weight
                from nearest
              )
              select
                sum(corner1_norm * weight) / nullif(sum(weight), 0) corner1,
                sum(corner2_norm * weight) / nullif(sum(weight), 0) corner2,
                sum(corner3_norm * weight) / nullif(sum(weight), 0) corner3,
                sum(corner4_norm * weight) / nullif(sum(weight), 0) corner4,
                count(*)::text neighbor_count,
                avg(weight)::text similarity_score
              from weighted_nearest
            `);
              const row = result.rows[0];
              const neighborCount = Number(row?.neighbor_count ?? 0);
              if (!row || neighborCount === 0) {
                return null;
              }
              const scaleCorner = (value: string | null): number | null => {
                if (value === null) {
                  return null;
                }
                const parsed = Number(value);
                return Number.isFinite(parsed) ? parsed * (runnerCount - 1) + 1 : null;
              };
              return {
                corner1: scaleCorner(row.corner1),
                corner2: scaleCorner(row.corner2),
                corner3: scaleCorner(row.corner3),
                corner4: scaleCorner(row.corner4),
                horseNumber,
                neighborCount,
                similarityScore: Number(row.similarity_score ?? 0),
              };
            } catch {
              return null;
            }
          }),
        );
        return rows.filter((row): row is RacePaceSimilarityFeature => row !== null);
      },
    );
  },
);

const getCornerModelVersion = (): string =>
  process.env.PC_KEIBA_CORNER_MODEL_VERSION?.trim() || "lightgbm-jra-20260508";

export const getRacePaceModelPredictionFeatures = cache(
  async (race: RaceDetail, runners: Runner[]): Promise<RacePaceModelPredictionFeature[]> => {
    return withDbQueryCache(
      [
        "getRacePaceModelPredictionFeatures",
        getCornerModelVersion(),
        race.source,
        race.kaisaiNen,
        race.kaisaiTsukihi,
        race.keibajoCode,
        race.raceBango,
        runners.map((runner) => runner.umaban).join(","),
      ],
      async () => {
        if (runners.length <= 1) {
          return [];
        }
        try {
          const result = await getDb().execute<{
            model_version: string;
            umaban: number;
            predicted_corner1_norm: string | null;
            predicted_corner2_norm: string | null;
            predicted_corner3_norm: string | null;
            predicted_corner4_norm: string | null;
          }>(sql`
            select
              model_version,
              umaban,
              predicted_corner1_norm,
              predicted_corner2_norm,
              predicted_corner3_norm,
              predicted_corner4_norm
            from race_entry_corner_model_predictions
            where
              model_version = ${getCornerModelVersion()}
              and source = ${race.source}
              and kaisai_nen = ${race.kaisaiNen}
              and kaisai_tsukihi = ${race.kaisaiTsukihi}
              and keibajo_code = ${race.keibajoCode}
              and race_bango = ${race.raceBango}
          `);
          const scaleCorner = (value: string | null): number | null => {
            if (value === null) {
              return null;
            }
            const parsed = Number(value);
            return Number.isFinite(parsed) ? parsed * (runners.length - 1) + 1 : null;
          };
          return result.rows.map((row) => ({
            corner1: scaleCorner(row.predicted_corner1_norm),
            corner2: scaleCorner(row.predicted_corner2_norm),
            corner3: scaleCorner(row.predicted_corner3_norm),
            corner4: scaleCorner(row.predicted_corner4_norm),
            horseNumber: String(row.umaban),
            modelVersion: row.model_version,
          }));
        } catch {
          return [];
        }
      },
    );
  },
);

export const getFinishPositionSimilarityFeatures = cache(
  async (race: RaceDetail, runners: Runner[]): Promise<FinishPositionSimilarityFeature[]> => {
    return withDbQueryCache(
      [
        "getFinishPositionSimilarityFeatures",
        race.source,
        race.kaisaiNen,
        race.kaisaiTsukihi,
        race.keibajoCode,
        race.raceBango,
        runners.map((runner) => runner.umaban).join(","),
        runners.map((runner) => runner.tanshoNinkijun).join(","),
        runners.map((runner) => runner.tanshoOdds).join(","),
      ],
      async () => {
        const runnerCount = runners.length;
        if (runnerCount <= 1) {
          return [];
        }
        const rows = await Promise.all(
          runners.map(async (runner): Promise<FinishPositionSimilarityFeature | null> => {
            const horseNumber = runner.umaban?.replace(/^0+/u, "") || runner.umaban || "";
            if (!horseNumber) {
              return null;
            }
            const distance = parseNumericText(race.kyori, "");
            const vector = buildCornerSimilarityVector(race, runner, runnerCount);
            const isBanEi = race.source === "nar" && race.keibajoCode === "83";
            try {
              const result = await getDb().execute<{
                average_finish_norm: string | null;
                neighbor_count: string;
                show_rate: string | null;
                similarity_score: string | null;
                win_rate: string | null;
              }>(sql`
              with nearest as (
                select *
                from (
                  select
                    feature_vector,
                    finish_norm,
                    finish_position
                  from race_entry_corner_features
                  where
                    source = ${race.source}
                    and race_date < ${`${race.kaisaiNen}${race.kaisaiTsukihi}`}
                    and race_date >= ${`${Number(race.kaisaiNen) - 10}${race.kaisaiTsukihi}`}
                    and finish_norm is not null
                    and (${distance}::integer is null or kyori between ${distance}::integer - 500 and ${distance}::integer + 500)
                    and (${isBanEi}::boolean or left(coalesce(track_code, ''), 1) = left(coalesce(${race.trackCode}, ''), 1))
                    and (
                      ${race.source} <> 'nar'
                      or (${isBanEi}::boolean and keibajo_code = '83')
                      or (not ${isBanEi}::boolean and keibajo_code <> '83')
                    )
                  order by race_date desc
                  limit 8000
                ) candidates
                order by feature_vector <-> ${vector}::vector
                limit 80
              ),
              weighted_nearest as (
                select
                  finish_norm,
                  finish_position,
                  1 / (1 + (feature_vector <-> ${vector}::vector)) weight
                from nearest
              )
              select
                sum(finish_norm * weight) / nullif(sum(weight), 0) average_finish_norm,
                count(*)::text neighbor_count,
                avg(case when finish_position = 1 then 1 else 0 end)::text win_rate,
                avg(case when finish_position between 1 and 3 then 1 else 0 end)::text show_rate,
                avg(weight)::text similarity_score
              from weighted_nearest
            `);
              const row = result.rows[0];
              const neighborCount = Number(row?.neighbor_count ?? 0);
              if (!row || neighborCount === 0) {
                return null;
              }
              const averageFinishNorm =
                row.average_finish_norm === null ? null : Number(row.average_finish_norm);
              return {
                averageFinishPosition:
                  averageFinishNorm === null || !Number.isFinite(averageFinishNorm)
                    ? null
                    : averageFinishNorm * (runnerCount - 1) + 1,
                horseNumber,
                neighborCount,
                showRate: row.show_rate === null ? null : Number(row.show_rate),
                similarityScore: Number(row.similarity_score ?? 0),
                winRate: row.win_rate === null ? null : Number(row.win_rate),
              };
            } catch {
              return null;
            }
          }),
        );
        return rows.filter((row): row is FinishPositionSimilarityFeature => row !== null);
      },
    );
  },
);

const getFinishModelVersions = (): string[] => {
  const configured = process.env.PC_KEIBA_FINISH_MODEL_VERSION?.trim();
  const versions = (configured || "finish-ensemble-10y-20260514")
    .split(",")
    .map((version) => version.trim())
    .filter(Boolean);
  return versions.length > 0 ? versions : ["finish-ensemble-10y-20260514"];
};

export const getFinishPositionModelPredictionFeatures = cache(
  async (race: RaceDetail, runners: Runner[]): Promise<FinishPositionModelPredictionFeature[]> => {
    return withDbQueryCache(
      [
        "getFinishPositionModelPredictionFeatures",
        getFinishModelVersions().join(","),
        race.source,
        race.kaisaiNen,
        race.kaisaiTsukihi,
        race.keibajoCode,
        race.raceBango,
        runners.map((runner) => runner.umaban).join(","),
      ],
      async () => {
        if (runners.length <= 1) {
          return [];
        }
        const modelVersions = getFinishModelVersions();
        try {
          const result = await getDb().execute<{
            model_version: string;
            predicted_finish_norm: string | null;
            show_probability: string | null;
            umaban: number;
            win_probability: string | null;
          }>(sql`
            select
              model_version,
              umaban,
              predicted_finish_norm,
              win_probability,
              show_probability
            from race_entry_finish_model_predictions
            where
              model_version in (${sql.join(modelVersions, sql`, `)})
              and source = ${race.source}
              and kaisai_nen = ${race.kaisaiNen}
              and kaisai_tsukihi = ${race.kaisaiTsukihi}
              and keibajo_code = ${race.keibajoCode}
              and race_bango = ${race.raceBango}
          `);
          return result.rows.map((row) => ({
            horseNumber: String(row.umaban),
            modelVersion: row.model_version,
            predictedFinishNorm:
              row.predicted_finish_norm === null ? null : Number(row.predicted_finish_norm),
            showProbability: row.show_probability === null ? null : Number(row.show_probability),
            winProbability: row.win_probability === null ? null : Number(row.win_probability),
          }));
        } catch {
          return [];
        }
      },
    );
  },
);

const CATEGORY_FROM_RACE = (race: RaceDetail): string => {
  if (race.source === "jra") return "jra";
  if (race.keibajoCode === "83") return "ban-ei";
  return "nar";
};

export const getFinishPositionLambdarankPredictions = cache(
  async (race: RaceDetail, runners: Runner[]): Promise<FinishPositionModelPredictionFeature[]> => {
    return withDbQueryCache(
      [
        "getFinishPositionLambdarankPredictions",
        race.source,
        race.kaisaiNen,
        race.kaisaiTsukihi,
        race.keibajoCode,
        race.raceBango,
        runners.map((runner) => runner.umaban).join(","),
      ],
      async () => {
        if (runners.length <= 1) return [];
        const category = CATEGORY_FROM_RACE(race);
        try {
          const result = await getDb().execute<{
            model_version: string;
            predicted_rank: number;
            predicted_score: string | null;
            shusso_tosu: number | null;
            umaban: number;
          }>(sql`
            with active as (
              select model_version
              from finish_position_active_models
              where category = ${category}
              limit 1
            ),
            selected_model as (
              select model_version
              from (
                select p.model_version, 0 as priority
                from race_finish_position_model_predictions p
                join active on p.model_version =
                  active.model_version || '-rs-overlay-' || ${race.kaisaiNen} || ${race.kaisaiTsukihi}
                where p.source = ${race.source}
                  and p.kaisai_nen = ${race.kaisaiNen}
                  and p.kaisai_tsukihi = ${race.kaisaiTsukihi}
                  and p.keibajo_code = ${race.keibajoCode}
                  and p.race_bango = ${race.raceBango}
                union all
                select active.model_version, 1 as priority
                from active
              ) candidates
              order by priority
              limit 1
            )
            select
              p.model_version,
              p.umaban,
              p.predicted_score,
              p.predicted_rank,
              (
                select count(*)
                from race_finish_position_model_predictions p2
                where p2.model_version = p.model_version
                  and p2.source = p.source
                  and p2.kaisai_nen = p.kaisai_nen
                  and p2.kaisai_tsukihi = p.kaisai_tsukihi
                  and p2.keibajo_code = p.keibajo_code
                  and p2.race_bango = p.race_bango
              )::integer as shusso_tosu
            from race_finish_position_model_predictions p
            join selected_model on selected_model.model_version = p.model_version
            where p.source = ${race.source}
              and p.kaisai_nen = ${race.kaisaiNen}
              and p.kaisai_tsukihi = ${race.kaisaiTsukihi}
              and p.keibajo_code = ${race.keibajoCode}
              and p.race_bango = ${race.raceBango}
          `);
          return result.rows.map((row) => {
            const fieldSize = Math.max(1, row.shusso_tosu ?? runners.length);
            const denominator = Math.max(1, fieldSize - 1);
            const predictedFinishNorm = Math.min(
              1,
              Math.max(0, (row.predicted_rank - 1) / denominator),
            );
            return {
              horseNumber: String(row.umaban),
              modelVersion: row.model_version,
              predictedFinishNorm,
              showProbability: null,
              winProbability: null,
            };
          });
        } catch {
          return [];
        }
      },
    );
  },
);

export const getActiveFinishPositionPredictions = cache(
  async (race: RaceDetail, runners: Runner[]): Promise<FinishPositionModelPredictionFeature[]> => {
    const lambdaRows = await getFinishPositionLambdarankPredictions(race, runners);
    if (lambdaRows.length > 0) return lambdaRows;
    return getFinishPositionModelPredictionFeatures(race, runners);
  },
);

export interface DbFinishPredictionEvaluation {
  evaluationWindowFrom: string;
  evaluationWindowTo: string;
  modelVersion: string;
  ndcgAt3: number | null;
  pairScore: number | null;
  place1Accuracy: number | null;
  place2Accuracy: number | null;
  place3Accuracy: number | null;
  predictionCount: number;
  raceCount: number;
  top1Accuracy: number | null;
  top3BoxAccuracy: number | null;
  top3ExactAccuracy: number | null;
  top3PlaceRelation: number | null;
  top3WinnerCapture: number | null;
  top5WinnerCapture: number | null;
}

const toNullableNumberFromText = (value: string | null): number | null =>
  value === null ? null : Number(value);

export const getActiveFinishPredictionEvaluation = cache(
  async (category: string): Promise<DbFinishPredictionEvaluation | null> => {
    return withDbQueryCache(["getActiveFinishPredictionEvaluation", category], async () => {
      try {
        const result = await getDb().execute<{
          evaluation_window_from: string;
          evaluation_window_to: string;
          model_version: string;
          ndcg_at_3: string | null;
          pair_score: string | null;
          place1_accuracy: string | null;
          place2_accuracy: string | null;
          place3_accuracy: string | null;
          prediction_count: number;
          race_count: number;
          top1_accuracy: string | null;
          top3_box_accuracy: string | null;
          top3_exact_accuracy: string | null;
          top3_place_relation: string | null;
          top3_winner_capture: string | null;
          top5_winner_capture: string | null;
        }>(sql`
          with active as (
            select model_version from finish_position_active_models where category = ${category} limit 1
          )
          select e.*
          from model_prediction_evaluations e
          join active on active.model_version = e.model_version
          where e.category = ${category}
          order by e.evaluated_at desc
          limit 1
        `);
        const row = result.rows[0];
        if (row === undefined) return null;
        return {
          evaluationWindowFrom: row.evaluation_window_from,
          evaluationWindowTo: row.evaluation_window_to,
          modelVersion: row.model_version,
          ndcgAt3: toNullableNumberFromText(row.ndcg_at_3),
          pairScore: toNullableNumberFromText(row.pair_score),
          place1Accuracy: toNullableNumberFromText(row.place1_accuracy),
          place2Accuracy: toNullableNumberFromText(row.place2_accuracy),
          place3Accuracy: toNullableNumberFromText(row.place3_accuracy),
          predictionCount: row.prediction_count,
          raceCount: row.race_count,
          top1Accuracy: toNullableNumberFromText(row.top1_accuracy),
          top3BoxAccuracy: toNullableNumberFromText(row.top3_box_accuracy),
          top3ExactAccuracy: toNullableNumberFromText(row.top3_exact_accuracy),
          top3PlaceRelation: toNullableNumberFromText(row.top3_place_relation),
          top3WinnerCapture: toNullableNumberFromText(row.top3_winner_capture),
          top5WinnerCapture: toNullableNumberFromText(row.top5_winner_capture),
        };
      } catch {
        return null;
      }
    });
  },
);

export const getRaceTrainings = cache(
  async (
    source: RaceSource,
    year: string,
    month: string,
    day: string,
    keibajoCode: string,
    raceNumber: string,
  ): Promise<Training[]> => {
    if (source !== "jra") {
      return [];
    }

    return withDbQueryCache(
      ["getRaceTrainings", source, year, month, day, keibajoCode, raceNumber],
      async () => {
        const monthDay = `${month}${day}`;
        const result = await getDb().execute<Training>(sql`
      with runners as (
        select
          umaban,
          bamei,
          kishumei_ryakusho as "currentJockeyName",
          chokyoshimei_ryakusho as "trainerName",
          ketto_toroku_bango
        from ${jvdSe}
        where
          kaisai_nen = ${year}
          and kaisai_tsukihi = ${monthDay}
          and keibajo_code = ${keibajoCode}
          and race_bango = ${raceNumber}
      ),
      race_window as (
        select
          to_char(to_date(${year} || ${monthDay}, 'YYYYMMDD') - interval '14 days', 'YYYYMMDD') as start_date,
          ${year} || ${monthDay} as end_date
      ),
      workouts as (
        select
          r.umaban,
          r.bamei,
          r."currentJockeyName",
          r."trainerName",
          null::varchar as "trainingRiderName",
          '坂路' as "trainingType",
          h.tracen_kubun as "tracenKubun",
          h.chokyo_nengappi as "chokyoNengappi",
          h.chokyo_jikoku as "chokyoJikoku",
          null::varchar as course,
          null::varchar as babamawari,
          null::varchar as "timeGokei10f",
          null::varchar as "lapTime10f",
          null::varchar as "timeGokei9f",
          null::varchar as "lapTime9f",
          null::varchar as "timeGokei8f",
          null::varchar as "lapTime8f",
          null::varchar as "timeGokei7f",
          null::varchar as "lapTime7f",
          null::varchar as "timeGokei6f",
          null::varchar as "lapTime6f",
          null::varchar as "timeGokei5f",
          null::varchar as "lapTime5f",
          h.time_gokei_4f as "timeGokei4f",
          h.lap_time_4f as "lapTime4f",
          h.time_gokei_3f as "timeGokei3f",
          h.lap_time_3f as "lapTime3f",
          h.time_gokei_2f as "timeGokei2f",
          h.lap_time_2f as "lapTime2f",
          h.lap_time_1f as "lapTime1f",
          r.ketto_toroku_bango
        from runners r
        join ${sql.identifier("jvd_hc")} h
          on h.ketto_toroku_bango = r.ketto_toroku_bango
        cross join race_window w
        where h.chokyo_nengappi between w.start_date and w.end_date
        union all
        select
          r.umaban,
          r.bamei,
          r."currentJockeyName",
          r."trainerName",
          null::varchar as "trainingRiderName",
          'ウッド' as "trainingType",
          w.tracen_kubun as "tracenKubun",
          w.chokyo_nengappi as "chokyoNengappi",
          w.chokyo_jikoku as "chokyoJikoku",
          w.course,
          w.babamawari,
          w.time_gokei_10f as "timeGokei10f",
          w.lap_time_10f as "lapTime10f",
          w.time_gokei_9f as "timeGokei9f",
          w.lap_time_9f as "lapTime9f",
          w.time_gokei_8f as "timeGokei8f",
          w.lap_time_8f as "lapTime8f",
          w.time_gokei_7f as "timeGokei7f",
          w.lap_time_7f as "lapTime7f",
          w.time_gokei_6f as "timeGokei6f",
          w.lap_time_6f as "lapTime6f",
          w.time_gokei_5f as "timeGokei5f",
          w.lap_time_5f as "lapTime5f",
          w.time_gokei_4f as "timeGokei4f",
          w.lap_time_4f as "lapTime4f",
          w.time_gokei_3f as "timeGokei3f",
          w.lap_time_3f as "lapTime3f",
          w.time_gokei_2f as "timeGokei2f",
          w.lap_time_2f as "lapTime2f",
          w.lap_time_1f as "lapTime1f",
          r.ketto_toroku_bango
        from runners r
        join ${sql.identifier("jvd_wc")} w
          on w.ketto_toroku_bango = r.ketto_toroku_bango
        cross join race_window rw
        where w.chokyo_nengappi between rw.start_date and rw.end_date
      ),
      ranked as (
        select
          *,
          row_number() over (
            partition by ketto_toroku_bango, "trainingType"
            order by "chokyoNengappi" desc, "chokyoJikoku" desc
          ) as rn
        from workouts
      )
      select
        umaban,
        bamei,
        "currentJockeyName",
        "trainerName",
        "trainingRiderName",
        "trainingType",
        "tracenKubun",
        "chokyoNengappi",
        "chokyoJikoku",
        course,
        babamawari,
        "timeGokei10f",
        "lapTime10f",
        "timeGokei9f",
        "lapTime9f",
        "timeGokei8f",
        "lapTime8f",
        "timeGokei7f",
        "lapTime7f",
        "timeGokei6f",
        "lapTime6f",
        "timeGokei5f",
        "lapTime5f",
        "timeGokei4f",
        "lapTime4f",
        "timeGokei3f",
        "lapTime3f",
        "timeGokei2f",
        "lapTime2f",
        "lapTime1f"
      from ranked
      where rn <= 3
      order by umaban asc, "chokyoNengappi" desc, "chokyoJikoku" desc, "trainingType" asc
    `);

        return result.rows;
      },
    );
  },
);

export const getRaceAbilityTests = cache(
  async (
    source: RaceSource,
    year: string,
    month: string,
    day: string,
    keibajoCode: string,
    raceNumber: string,
  ): Promise<AbilityTest[]> => {
    if (source !== "nar") {
      return [];
    }

    return withDbQueryCache(
      ["getRaceAbilityTests", source, year, month, day, keibajoCode, raceNumber],
      async () => {
        const monthDay = `${month}${day}`;
        const raceDate = `${year}${monthDay}`;
        const result = await getDb().execute<AbilityTest>(sql`
          with current_runners as (
            select
              umaban as "currentUmaban",
              bamei as "currentBamei",
              ketto_toroku_bango
            from ${nvdSe}
            where
              kaisai_nen = ${year}
              and kaisai_tsukihi = ${monthDay}
              and keibajo_code = ${keibajoCode}
              and race_bango = ${raceNumber}
              and nullif(ketto_toroku_bango, '') is not null
          )
          select
            current_runners."currentUmaban",
            current_runners."currentBamei",
            ns.kaisai_nen as "kaisaiNen",
            ns.kaisai_tsukihi as "kaisaiTsukihi",
            ns.keibajo_code as "keibajoCode",
            ns.race_bango as "raceBango",
            ns.umaban,
            ns.ketto_toroku_bango as "kettoTorokuBango",
            ns.bamei,
            ns.seibetsu_code as "seibetsuCode",
            ns.barei,
            ns.chokyoshimei_ryakusho as "chokyoshimeiRyakusho",
            ns.futan_juryo as "futanJuryo",
            ns.kishumei_ryakusho as "kishumeiRyakusho",
            ns.bataiju,
            ns.zogen_fugo as "zogenFugo",
            ns.zogen_sa as "zogenSa",
            ns.ijo_kubun_code as "ijoKubunCode",
            ns.juni,
            ns.soha_time as "sohaTime",
            ns.chakusa_code_1 as "chakusaCode1",
            ns.chakusa_code_2 as "chakusaCode2",
            ns.chakusa_code_3 as "chakusaCode3",
            ns.noryoku_shiken_code as "noryokuShikenCode",
            ns.gohi_code as "gohiCode",
            ns.riyu_code as "riyuCode",
            ns.gohi_nengappi as "gohiNengappi",
            ns.ashiiro_code as "ashiiroCode",
            ns.corner_1 as "corner1",
            ns.corner_2 as "corner2",
            ns.corner_3 as "corner3",
            ns.corner_4 as "corner4",
            ns.kohan_4f as "kohan4f",
            ns.kohan_3f as "kohan3f",
            ns.aiteuma_joho_1 as "aiteumaJoho1",
            ns.aiteuma_joho_2 as "aiteumaJoho2",
            ns.aiteuma_joho_3 as "aiteumaJoho3",
            ns.kyakushitsu_hantei as "kyakushitsuHantei",
            nr.kyori,
            nr.track_code as "trackCode",
            nr.hasso_jikoku as "hassoJikoku",
            nr.tenko_code as "tenkoCode",
            nr.babajotai_code_dirt as "babajotaiCodeDirt"
          from current_runners
          join nvd_ns ns
            on ns.ketto_toroku_bango = current_runners.ketto_toroku_bango
          left join nvd_nr nr
            on nr.kaisai_nen = ns.kaisai_nen
            and nr.kaisai_tsukihi = ns.kaisai_tsukihi
            and nr.keibajo_code = ns.keibajo_code
            and nr.race_bango = ns.race_bango
          where ns.kaisai_nen || ns.kaisai_tsukihi <= ${raceDate}
          order by
            current_runners."currentUmaban"::int asc,
            ns.kaisai_nen desc,
            ns.kaisai_tsukihi desc,
            ns.race_bango::int desc,
            ns.umaban::int asc
        `);

        return result.rows;
      },
    );
  },
);

const toCount = (value: string | number | bigint | null | undefined): number => Number(value ?? 0);
const toRate = (value: string | number | null | undefined): number => Number(value ?? 0);
const toNullableNumber = (value: string | number | null | undefined): number | null => {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};
const toNullableUnknownNumber = (value: unknown): number | null => {
  if (typeof value !== "string" && typeof value !== "number") {
    return null;
  }
  return toNullableNumber(value);
};
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const toStringValue = (value: unknown): string => (typeof value === "string" ? value : "");
const parseJsonValue = (value: unknown): unknown => {
  if (typeof value !== "string") {
    return value;
  }
  try {
    return JSON.parse(value);
  } catch {
    return [];
  }
};
const toStatsDetails = (value: unknown): StatsDetail[] => {
  const parsedValue = parseJsonValue(value);
  if (!Array.isArray(parsedValue)) {
    return [];
  }

  return parsedValue.filter(isRecord).map((detail) => ({
    damSireName: toStringValue(detail.damSireName),
    date: toStringValue(detail.date),
    frameNumber: toStringValue(detail.frameNumber),
    horseName: toStringValue(detail.horseName),
    horseNumber: toStringValue(detail.horseNumber),
    jockeyName: toStringValue(detail.jockeyName),
    keibajoCode: toStringValue(detail.keibajoCode),
    popularity: toStringValue(detail.popularity),
    raceName: toStringValue(detail.raceName),
    raceNumber: toStringValue(detail.raceNumber),
    raceTime: toStringValue(detail.raceTime),
    rank: toStringValue(detail.rank),
    sireName: toStringValue(detail.sireName),
    sireSireName: toStringValue(detail.sireSireName),
    winOdds: toStringValue(detail.winOdds),
  }));
};
const toPayoutStatsDetails = (value: unknown): PayoutStatsDetail[] => {
  const parsedValue = parseJsonValue(value);
  if (!Array.isArray(parsedValue)) {
    return [];
  }

  return parsedValue.filter(isRecord).map((detail) => ({
    date: toStringValue(detail.date),
    keibajoCode: toStringValue(detail.keibajoCode),
    payout: Number(detail.payout ?? 0),
    raceName: toStringValue(detail.raceName),
    raceNumber: toStringValue(detail.raceNumber),
  }));
};
const toRaceTimeTargetRaces = (value: unknown): RaceTimeTargetRace[] => {
  const parsedValue = parseJsonValue(value);
  if (!Array.isArray(parsedValue)) {
    return [];
  }

  return parsedValue.filter(isRecord).map((race) => ({
    date: toStringValue(race.date),
    horseName: toStringValue(race.horseName),
    horseNumber: toStringValue(race.horseNumber),
    jockeyName: toStringValue(race.jockeyName),
    keibajoCode: toStringValue(race.keibajoCode),
    kohan3f: toStringValue(race.kohan3f),
    ownerName: toStringValue(race.ownerName),
    popularity: toStringValue(race.popularity),
    raceName: toStringValue(race.raceName),
    raceNumber: toStringValue(race.raceNumber),
    raceTime: toStringValue(race.raceTime),
    trainerName: toStringValue(race.trainerName),
  }));
};
const isCorrelationDetailKey = (value: string): value is ConditionCorrelationDetail["key"] =>
  value === "horseShow" ||
  value === "horseWin" ||
  value === "jockeyShow" ||
  value === "odds" ||
  value === "ownerShow" ||
  value === "popularity" ||
  value === "trainerShow";
const toConditionCorrelationRows = (value: unknown): ConditionCorrelationRow[] => {
  const parsedValue = parseJsonValue(value);
  if (!Array.isArray(parsedValue)) {
    return [];
  }

  return parsedValue.filter(isRecord).map((row) => {
    const rawDetails = Array.isArray(row.details) ? row.details : [];
    return {
      details: rawDetails.filter(isRecord).map((detail) => {
        const key = toStringValue(detail.key);
        return {
          key: isCorrelationDetailKey(key) ? key : "horseShow",
          label: toStringValue(detail.label),
          reason: toStringValue(detail.reason),
          score: Number(detail.score ?? 0),
          target: toNullableUnknownNumber(detail.target),
          value: toNullableUnknownNumber(detail.value),
          weight: Number(detail.weight ?? 0),
        };
      }),
      horseName: toStringValue(row.horseName),
      horseNumber: toStringValue(row.horseNumber),
      score: Number(row.score ?? 0),
    };
  });
};
const toTimeScoreRows = (value: unknown): TimeScoreRow[] => {
  const parsedValue = parseJsonValue(value);
  if (!Array.isArray(parsedValue)) {
    return [];
  }

  return parsedValue.filter(isRecord).map((row) => {
    const rawDetails = Array.isArray(row.details) ? row.details : [];
    return {
      details: rawDetails.filter(isRecord).map(
        (detail): TimeScoreDetail => ({
          label: toStringValue(detail.label),
          reason: toStringValue(detail.reason),
          score: Number(detail.score ?? 0),
          target: toNullableUnknownNumber(detail.target),
          value: toNullableUnknownNumber(detail.value),
          weight: Number(detail.weight ?? 0),
        }),
      ),
      horseName: toStringValue(row.horseName),
      horseNumber: toStringValue(row.horseNumber),
      jockeyName: toStringValue(row.jockeyName),
      score: Number(row.score ?? 0),
    };
  });
};
const cleanDbText = (value: string | null | undefined): string =>
  (value ?? "").replace(/\s+/g, " ").replace(/　+/g, " ").trim();

const RACE_NAME_TOKEN_PATTERN = /[\p{L}\p{N}ー・－-]+(?:杯|賞|記念|ステークス|カップ)/gu;

const getStatsRaceNameToken = (race: RaceDetail): string | null => {
  const subtitle = `${cleanDbText(race.kyosomeiFukudai)} ${cleanDbText(race.kyosomeiKakkonai)}`;
  const combined = `${cleanDbText(race.kyosomeiHondai)} ${subtitle}`;
  if (combined.includes("ジョッキーズカップ")) {
    return "ジョッキーズカップ";
  }

  const subtitleMatch = [...subtitle.matchAll(RACE_NAME_TOKEN_PATTERN)].at(-1)?.[0] ?? "";
  if (subtitleMatch) {
    return subtitleMatch;
  }

  return (
    [...cleanDbText(race.kyosomeiHondai).matchAll(RACE_NAME_TOKEN_PATTERN)].at(-1)?.[0] ?? null
  );
};

const escapePostgresRegex = (value: string): string => value.replace(/[\\^$.*+?()[\]{}|]/g, "\\$&");

const getStatsRaceTitleCondition = (race: RaceDetail, tableName = "ra") => {
  const table = sql.raw(tableName);
  const token = getStatsRaceNameToken(race);
  if (token) {
    if (token === "ジョッキーズカップ") {
      const pattern = `%${token}%`;
      return sql`(
        ${table}.kyosomei_hondai like ${pattern}
        or ${table}.kyosomei_fukudai like ${pattern}
        or ${table}.kyosomei_kakkonai like ${pattern}
      )`;
    }

    const pattern = `(^|[[:space:]　])${escapePostgresRegex(token)}([[:space:]　]|$)`;
    return sql`(
      ${table}.kyosomei_hondai ~ ${pattern}
      or ${table}.kyosomei_fukudai ~ ${pattern}
      or ${table}.kyosomei_kakkonai ~ ${pattern}
    )`;
  }

  return cleanDbText(race.kyosomeiHondai)
    ? sql`${table}.kyosomei_hondai = ${race.kyosomeiHondai}`
    : sql`false`;
};

const getStatsRaceSubtitleCondition = (race: RaceDetail, tableName = "ra") => {
  const table = sql.raw(tableName);
  return cleanDbText(race.kyosomeiFukudai)
    ? sql`${table}.kyosomei_fukudai = ${race.kyosomeiFukudai}`
    : cleanDbText(race.kyosomeiKakkonai)
      ? sql`${table}.kyosomei_kakkonai = ${race.kyosomeiKakkonai}`
      : sql`false`;
};

const getTrackSurface = (trackCode: string | null | undefined): string => {
  const label = trackCode ? TRACK_LABELS[trackCode.trim()] : undefined;
  return label?.split("・")[0] ?? "";
};

const getTrackTurn = (trackCode: string | null | undefined): string => {
  const label = trackCode ? TRACK_LABELS[trackCode.trim()] : undefined;
  if (label?.includes("左")) {
    return "左";
  }
  if (label?.includes("右")) {
    return "右";
  }
  if (label?.includes("直線")) {
    return "直線";
  }
  return "";
};

const getTrackCodesBySurface = (surface: string): string[] =>
  Object.entries(TRACK_LABELS)
    .filter(([, label]) => label.split("・")[0] === surface)
    .map(([code]) => code);

const getTrackCodesByTurn = (turn: string): string[] =>
  Object.entries(TRACK_LABELS)
    .filter(([, label]) => {
      if (turn === "左" || turn === "右") {
        return label.includes(turn);
      }
      return turn === "直線" ? label.includes("直線") : false;
    })
    .map(([code]) => code);

const trackCodeIn = (codes: string[], tableName = "ra") => {
  const table = sql.raw(tableName);
  return codes.length > 0 ? sql`${table}.track_code in (${sql.join(codes, sql`, `)})` : sql`false`;
};

const monthWindowCondition = (raceDate: string, enabled: boolean, tableName = "ra") => {
  const table = sql.raw(tableName);
  return sql`
  (
    ${enabled} = false
    or substring(${table}.kaisai_tsukihi from 1 for 2) in (
      to_char(to_date(${raceDate}, 'YYYYMMDD') - interval '1 month', 'MM'),
      to_char(to_date(${raceDate}, 'YYYYMMDD'), 'MM'),
      to_char(to_date(${raceDate}, 'YYYYMMDD') + interval '1 month', 'MM')
    )
  )
`;
};

const runnerCountCondition = (
  runnerTable: typeof jvdSe | typeof nvdSe,
  settings: SimilarRaceStatsSettings,
) => sql`
  (
    ${settings.includeRunnerCount} = false
    or ${settings.runnerCount}::int is null
    or (
      select count(*)
      from ${runnerTable} runner_count_se
      where
        runner_count_se.kaisai_nen = ra.kaisai_nen
        and runner_count_se.kaisai_tsukihi = ra.kaisai_tsukihi
        and runner_count_se.keibajo_code = ra.keibajo_code
        and runner_count_se.race_bango = ra.race_bango
    ) = ${settings.runnerCount}::int
  )
`;

const JRA_STATS_GRADE_CODES = new Set(["A", "B", "C", "D", "F", "G", "H", "L", "S"]);

const normalizeClassConditionName = (value: string): string =>
  value
    .replace(/[Ａ-Ｚａ-ｚ０-９]/g, (char) => String.fromCharCode(char.charCodeAt(0) - 0xfee0))
    .replace(/[－ー―‐]/g, "-")
    .trim();

const getStatsClassCondition = (
  race: RaceDetail,
  classConditionName: string | null,
  tableName = "ra",
) => {
  const table = sql.raw(tableName);
  if (race.source === "jra" && JRA_STATS_GRADE_CODES.has(cleanDbText(race.gradeCode))) {
    return sql`${table}.grade_code = ${race.gradeCode}`;
  }

  const normalizedClassConditionName = classConditionName
    ? normalizeClassConditionName(classConditionName)
    : null;

  return cleanDbText(race.kyosoJokenCode) === "000" && classConditionName
    ? sql`
        translate(
          regexp_replace(${table}.kyoso_joken_meisho, '[[:space:]　]+', ' ', 'g'),
          'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ０１２３４５６７８９－ー―‐',
          'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789----'
        ) like ${`%${normalizedClassConditionName}%`}
      `
    : sql`${table}.kyoso_joken_code = ${race.kyosoJokenCode}`;
};

const shouldUseJraStats = (race: RaceDetail, settings: SimilarRaceStatsSettings): boolean => {
  if (settings.sourceScope === "jra") {
    return true;
  }
  if (settings.sourceScope === "nar") {
    return false;
  }
  return race.source === "jra" || !settings.includeVenue;
};

const shouldUseNarStats = (race: RaceDetail, settings: SimilarRaceStatsSettings): boolean => {
  if (settings.sourceScope === "nar") {
    return true;
  }
  if (settings.sourceScope === "jra") {
    return false;
  }
  return race.source === "nar" || !settings.includeVenue;
};

const getSingleStatsSource = (
  race: RaceDetail,
  settings: SimilarRaceStatsSettings,
): RaceDetail["source"] =>
  settings.sourceScope === "jra" || settings.sourceScope === "nar"
    ? settings.sourceScope
    : race.source;

export const getBloodlineStats = cache(
  async (race: RaceDetail, settings: SimilarRaceStatsSettings): Promise<BloodlineStatsRow[]> => {
    return withDbQueryCache(["getBloodlineStats", race, settings], async () => {
      const runnerTable = race.source === "jra" ? jvdSe : nvdSe;
      const primaryHorseTable = race.source === "jra" ? jvdUm : nvdNu;
      const secondaryHorseTable = nvdUm;
      const tertiaryHorseTable = race.source === "jra" ? nvdNu : jvdUm;
      const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
      const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
      const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
      const classCondition = getStatsClassCondition(race, settings.classConditionName);
      const raceTitleCondition = getStatsRaceTitleCondition(race);
      const raceSubtitleCondition = getStatsRaceSubtitleCondition(race);
      const result = await getDb().execute<{
        category: "damSire" | "sire" | "sireSire";
        currentHorseNumbers: string;
        name: string;
        details: unknown;
        starts: string;
        horseCount: string;
        winCount: string;
        quinellaCount: string;
        showCount: string;
        winRate: string;
        quinellaRate: string;
        showRate: string;
      }>(sql`
      with current_entries as (
        select
          coalesce(nullif(regexp_replace(se.umaban, '^0+', ''), ''), '0') as umaban,
          se.umaban::int as "umabanSort",
          se.wakuban,
          se.ketto_toroku_bango,
          coalesce(
            nullif(regexp_replace(primary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            nullif(regexp_replace(secondary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            nullif(regexp_replace(tertiary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            '不明'
          ) as sire,
          coalesce(
            nullif(regexp_replace(primary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            nullif(regexp_replace(secondary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            nullif(regexp_replace(tertiary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            '不明'
          ) as "sireSire",
          coalesce(
            nullif(regexp_replace(primary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            nullif(regexp_replace(secondary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            nullif(regexp_replace(tertiary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            '不明'
          ) as "damSire"
        from ${runnerTable} se
        left join ${primaryHorseTable} primary_um
          on primary_um.ketto_toroku_bango = se.ketto_toroku_bango
        left join ${secondaryHorseTable} secondary_um
          on secondary_um.ketto_toroku_bango = se.ketto_toroku_bango
        left join ${tertiaryHorseTable} tertiary_um
          on tertiary_um.ketto_toroku_bango = se.ketto_toroku_bango
        where
          se.kaisai_nen = ${race.kaisaiNen}
          and se.kaisai_tsukihi = ${race.kaisaiTsukihi}
          and se.keibajo_code = ${race.keibajoCode}
          and se.race_bango = ${race.raceBango}
      ),
      target_entries as (
        select 'sire'::text as category, sire as name, umaban, "umabanSort", wakuban, ketto_toroku_bango
        from current_entries
        union all
        select 'damSire'::text as category, "damSire" as name, umaban, "umabanSort", wakuban, ketto_toroku_bango
        from current_entries
        union all
        select 'sireSire'::text as category, "sireSire" as name, umaban, "umabanSort", wakuban, ketto_toroku_bango
        from current_entries
      ),
      targets as (
        select
          category,
          name,
          string_agg(umaban, ', ' order by "umabanSort") as "currentHorseNumbers"
        from target_entries
        where name <> '不明'
        group by category, name
      ),
      target_names as (
        select distinct name
        from targets
      ),
      filtered_horse_keys as (
        select primary_um.ketto_toroku_bango
        from ${primaryHorseTable} primary_um
        join targets
          on targets.category = 'sire'
          and targets.name = nullif(regexp_replace(primary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')
        union
        select primary_um.ketto_toroku_bango
        from ${primaryHorseTable} primary_um
        join targets
          on targets.category = 'sireSire'
          and targets.name = nullif(regexp_replace(primary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')
        union
        select primary_um.ketto_toroku_bango
        from ${primaryHorseTable} primary_um
        join targets
          on targets.category = 'damSire'
          and targets.name = nullif(regexp_replace(primary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')
        union
        select secondary_um.ketto_toroku_bango
        from ${secondaryHorseTable} secondary_um
        join targets
          on targets.category = 'sire'
          and targets.name = nullif(regexp_replace(secondary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')
        union
        select secondary_um.ketto_toroku_bango
        from ${secondaryHorseTable} secondary_um
        join targets
          on targets.category = 'sireSire'
          and targets.name = nullif(regexp_replace(secondary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')
        union
        select secondary_um.ketto_toroku_bango
        from ${secondaryHorseTable} secondary_um
        join targets
          on targets.category = 'damSire'
          and targets.name = nullif(regexp_replace(secondary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')
        union
        select tertiary_um.ketto_toroku_bango
        from ${tertiaryHorseTable} tertiary_um
        join targets
          on targets.category = 'sire'
          and targets.name = nullif(regexp_replace(tertiary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')
        union
        select tertiary_um.ketto_toroku_bango
        from ${tertiaryHorseTable} tertiary_um
        join targets
          on targets.category = 'sireSire'
          and targets.name = nullif(regexp_replace(tertiary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')
        union
        select tertiary_um.ketto_toroku_bango
        from ${tertiaryHorseTable} tertiary_um
        join targets
          on targets.category = 'damSire'
          and targets.name = nullif(regexp_replace(tertiary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')
      ),
      filtered_horse_bloodlines as materialized (
        select
          horse_keys.ketto_toroku_bango,
          bloodline.sire,
          bloodline."sireSire",
          bloodline."damSire"
        from filtered_horse_keys horse_keys
        left join ${primaryHorseTable} primary_um
          on primary_um.ketto_toroku_bango = horse_keys.ketto_toroku_bango
        left join ${secondaryHorseTable} secondary_um
          on secondary_um.ketto_toroku_bango = horse_keys.ketto_toroku_bango
        left join ${tertiaryHorseTable} tertiary_um
          on tertiary_um.ketto_toroku_bango = horse_keys.ketto_toroku_bango
        cross join lateral (
          select
            coalesce(
              nullif(regexp_replace(primary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(secondary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(tertiary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              '不明'
            ) sire,
            coalesce(
              nullif(regexp_replace(primary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(secondary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(tertiary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              '不明'
            ) "sireSire",
            coalesce(
              nullif(regexp_replace(primary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(secondary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(tertiary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              '不明'
            ) "damSire"
        ) bloodline
      ),
      ancestor_horse_keys as (
        select targets.category, targets.name, primary_um.ketto_toroku_bango
        from ${primaryHorseTable} primary_um
        join targets
          on targets.name = nullif(regexp_replace(primary_um.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')
        where ${settings.includeBloodlineAncestors} = true
        union
        select targets.category, targets.name, secondary_um.ketto_toroku_bango
        from ${secondaryHorseTable} secondary_um
        join targets
          on targets.name = nullif(regexp_replace(secondary_um.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')
        where ${settings.includeBloodlineAncestors} = true
        union
        select targets.category, targets.name, tertiary_um.ketto_toroku_bango
        from ${tertiaryHorseTable} tertiary_um
        join targets
          on targets.name = nullif(regexp_replace(tertiary_um.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), '')
        where ${settings.includeBloodlineAncestors} = true
      ),
      ancestor_horse_bloodlines as materialized (
        select
          horse_keys.category,
          horse_keys.name,
          horse_keys.ketto_toroku_bango,
          bloodline.sire,
          bloodline."sireSire",
          bloodline."damSire"
        from ancestor_horse_keys horse_keys
        left join ${primaryHorseTable} primary_um
          on primary_um.ketto_toroku_bango = horse_keys.ketto_toroku_bango
        left join ${secondaryHorseTable} secondary_um
          on secondary_um.ketto_toroku_bango = horse_keys.ketto_toroku_bango
        left join ${tertiaryHorseTable} tertiary_um
          on tertiary_um.ketto_toroku_bango = horse_keys.ketto_toroku_bango
        cross join lateral (
          select
            coalesce(
              nullif(regexp_replace(primary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(secondary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(tertiary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              '不明'
            ) sire,
            coalesce(
              nullif(regexp_replace(primary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(secondary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(tertiary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              '不明'
            ) "sireSire",
            coalesce(
              nullif(regexp_replace(primary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(secondary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(tertiary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              '不明'
            ) "damSire"
        ) bloodline
      ),
      matched_entries as (
        select
          targets.category,
          targets.name,
          ra.kaisai_nen,
          ra.kaisai_tsukihi,
          ra.keibajo_code,
          se.wakuban,
          se.umaban,
          coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') bamei,
          ra.race_bango,
          coalesce(
            nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            '一般競走'
          ) as race_name,
          se.kakutei_chakujun,
          se.soha_time,
          se.tansho_ninkijun,
          se.tansho_odds,
          se.ketto_toroku_bango,
          filtered_horse_bloodlines.sire,
          filtered_horse_bloodlines."sireSire",
          filtered_horse_bloodlines."damSire"
        from filtered_horse_bloodlines
        join targets
          on (
            targets.category = 'sire'
            and targets.name = filtered_horse_bloodlines.sire
          )
          or (
            targets.category = 'sireSire'
            and targets.name = filtered_horse_bloodlines."sireSire"
          )
          or (
            targets.category = 'damSire'
            and targets.name = filtered_horse_bloodlines."damSire"
          )
        join ${jvdSe} se
          on se.ketto_toroku_bango = filtered_horse_bloodlines.ketto_toroku_bango
          and se.kaisai_nen <= ${race.kaisaiNen}
          and (
            ${settings.years}::int is null
            or se.kaisai_nen >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYY'
            )
          )
          and se.kaisai_nen || se.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or se.kaisai_nen || se.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
        join ${jvdRa} ra
          on ra.kaisai_nen = se.kaisai_nen
          and ra.kaisai_tsukihi = se.kaisai_tsukihi
          and ra.keibajo_code = se.keibajo_code
          and ra.race_bango = se.race_bango
        where
          ${shouldUseJraStats(race, settings)} = true
          and
          ra.kaisai_nen || ra.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or ra.kaisai_nen || ra.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
          and (${settings.includeVenue} = false or ra.keibajo_code = ${race.keibajoCode})
          and ${monthWindowCondition(raceDate, settings.includeMonthWindow)}
          and (${settings.includeRaceTitle} = false or ${raceTitleCondition})
          and (${settings.includeRaceSubtitle} = false or ${raceSubtitleCondition})
          and (${settings.includeAge} = false or ra.kyoso_shubetsu_code = ${race.kyosoShubetsuCode})
          and (
            ${settings.includeClass} = false
            or ${classCondition}
          )
          and (${settings.includeSex} = false or ra.kyoso_kigo_code = ${race.kyosoKigoCode})
          and (${settings.includeWeight} = false or ra.juryo_shubetsu_code = ${race.juryoShubetsuCode})
          and (${settings.includeSurface} = false or ${trackCodeIn(surfaceCodes)})
          and (${settings.includeTurn} = false or ${trackCodeIn(turnCodes)})
          and (${settings.includeDistance} = false or ra.kyori = ${race.kyori})
          and nullif(regexp_replace(coalesce(se.kakutei_chakujun, ''), '[^0-9]', '', 'g'), '') !~ '^0+$'
        union all
        select
          targets.category,
          targets.name,
          ra.kaisai_nen,
          ra.kaisai_tsukihi,
          ra.keibajo_code,
          se.wakuban,
          se.umaban,
          coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') bamei,
          ra.race_bango,
          coalesce(
            nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            '一般競走'
          ) race_name,
          se.kakutei_chakujun,
          se.soha_time,
          se.tansho_ninkijun,
          se.tansho_odds,
          se.ketto_toroku_bango,
          filtered_horse_bloodlines.sire,
          filtered_horse_bloodlines."sireSire",
          filtered_horse_bloodlines."damSire"
        from filtered_horse_bloodlines
        join targets
          on (
            targets.category = 'sire'
            and targets.name = filtered_horse_bloodlines.sire
          )
          or (
            targets.category = 'sireSire'
            and targets.name = filtered_horse_bloodlines."sireSire"
          )
          or (
            targets.category = 'damSire'
            and targets.name = filtered_horse_bloodlines."damSire"
          )
        join ${nvdSe} se
          on se.ketto_toroku_bango = filtered_horse_bloodlines.ketto_toroku_bango
          and se.kaisai_nen <= ${race.kaisaiNen}
          and (
            ${settings.years}::int is null
            or se.kaisai_nen >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYY'
            )
          )
          and se.kaisai_nen || se.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or se.kaisai_nen || se.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
        join ${nvdRa} ra
          on ra.kaisai_nen = se.kaisai_nen
          and ra.kaisai_tsukihi = se.kaisai_tsukihi
          and ra.keibajo_code = se.keibajo_code
          and ra.race_bango = se.race_bango
        where
          ${shouldUseNarStats(race, settings)} = true
          and
          ra.kaisai_nen || ra.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or ra.kaisai_nen || ra.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
          and (${settings.includeVenue} = false or ra.keibajo_code = ${race.keibajoCode})
          and ${monthWindowCondition(raceDate, settings.includeMonthWindow)}
          and (${settings.includeRaceTitle} = false or ${raceTitleCondition})
          and (${settings.includeRaceSubtitle} = false or ${raceSubtitleCondition})
          and (${settings.includeAge} = false or ra.kyoso_shubetsu_code = ${race.kyosoShubetsuCode})
          and (
            ${settings.includeClass} = false
            or ${classCondition}
          )
          and (${settings.includeSex} = false or ra.kyoso_kigo_code = ${race.kyosoKigoCode})
          and (${settings.includeWeight} = false or ra.juryo_shubetsu_code = ${race.juryoShubetsuCode})
          and (${settings.includeSurface} = false or ${trackCodeIn(surfaceCodes)})
          and (${settings.includeTurn} = false or ${trackCodeIn(turnCodes)})
          and (${settings.includeDistance} = false or ra.kyori = ${race.kyori})
          and nullif(regexp_replace(coalesce(se.kakutei_chakujun, ''), '[^0-9]', '', 'g'), '') !~ '^0+$'
        union all
        select
          ancestor_horse_bloodlines.category,
          ancestor_horse_bloodlines.name,
          ra.kaisai_nen,
          ra.kaisai_tsukihi,
          ra.keibajo_code,
          se.wakuban,
          se.umaban,
          coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') bamei,
          ra.race_bango,
          coalesce(
            nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            '一般競走'
          ) race_name,
          se.kakutei_chakujun,
          se.soha_time,
          se.tansho_ninkijun,
          se.tansho_odds,
          se.ketto_toroku_bango,
          ancestor_horse_bloodlines.sire,
          ancestor_horse_bloodlines."sireSire",
          ancestor_horse_bloodlines."damSire"
        from ancestor_horse_bloodlines
        join ${jvdSe} se
          on se.ketto_toroku_bango = ancestor_horse_bloodlines.ketto_toroku_bango
          and se.kaisai_nen <= ${race.kaisaiNen}
          and (
            ${settings.years}::int is null
            or se.kaisai_nen >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYY'
            )
          )
          and se.kaisai_nen || se.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or se.kaisai_nen || se.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
        join ${jvdRa} ra
          on ra.kaisai_nen = se.kaisai_nen
          and ra.kaisai_tsukihi = se.kaisai_tsukihi
          and ra.keibajo_code = se.keibajo_code
          and ra.race_bango = se.race_bango
        where
          ${shouldUseJraStats(race, settings)} = true
          and
          ra.kaisai_nen || ra.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or ra.kaisai_nen || ra.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
          and (${settings.includeVenue} = false or ra.keibajo_code = ${race.keibajoCode})
          and ${monthWindowCondition(raceDate, settings.includeMonthWindow)}
          and (${settings.includeRaceTitle} = false or ${raceTitleCondition})
          and (${settings.includeRaceSubtitle} = false or ${raceSubtitleCondition})
          and (${settings.includeAge} = false or ra.kyoso_shubetsu_code = ${race.kyosoShubetsuCode})
          and (
            ${settings.includeClass} = false
            or ${classCondition}
          )
          and (${settings.includeSex} = false or ra.kyoso_kigo_code = ${race.kyosoKigoCode})
          and (${settings.includeWeight} = false or ra.juryo_shubetsu_code = ${race.juryoShubetsuCode})
          and (${settings.includeSurface} = false or ${trackCodeIn(surfaceCodes)})
          and (${settings.includeTurn} = false or ${trackCodeIn(turnCodes)})
          and (${settings.includeDistance} = false or ra.kyori = ${race.kyori})
          and nullif(regexp_replace(coalesce(se.kakutei_chakujun, ''), '[^0-9]', '', 'g'), '') !~ '^0+$'
        union all
        select
          ancestor_horse_bloodlines.category,
          ancestor_horse_bloodlines.name,
          ra.kaisai_nen,
          ra.kaisai_tsukihi,
          ra.keibajo_code,
          se.wakuban,
          se.umaban,
          coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') bamei,
          ra.race_bango,
          coalesce(
            nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            '一般競走'
          ) race_name,
          se.kakutei_chakujun,
          se.soha_time,
          se.tansho_ninkijun,
          se.tansho_odds,
          se.ketto_toroku_bango,
          ancestor_horse_bloodlines.sire,
          ancestor_horse_bloodlines."sireSire",
          ancestor_horse_bloodlines."damSire"
        from ancestor_horse_bloodlines
        join ${nvdSe} se
          on se.ketto_toroku_bango = ancestor_horse_bloodlines.ketto_toroku_bango
          and se.kaisai_nen <= ${race.kaisaiNen}
          and (
            ${settings.years}::int is null
            or se.kaisai_nen >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYY'
            )
          )
          and se.kaisai_nen || se.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or se.kaisai_nen || se.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
        join ${nvdRa} ra
          on ra.kaisai_nen = se.kaisai_nen
          and ra.kaisai_tsukihi = se.kaisai_tsukihi
          and ra.keibajo_code = se.keibajo_code
          and ra.race_bango = se.race_bango
        where
          ${shouldUseNarStats(race, settings)} = true
          and
          ra.kaisai_nen || ra.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or ra.kaisai_nen || ra.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
          and (${settings.includeVenue} = false or ra.keibajo_code = ${race.keibajoCode})
          and ${monthWindowCondition(raceDate, settings.includeMonthWindow)}
          and (${settings.includeRaceTitle} = false or ${raceTitleCondition})
          and (${settings.includeRaceSubtitle} = false or ${raceSubtitleCondition})
          and (${settings.includeAge} = false or ra.kyoso_shubetsu_code = ${race.kyosoShubetsuCode})
          and (
            ${settings.includeClass} = false
            or ${classCondition}
          )
          and (${settings.includeSex} = false or ra.kyoso_kigo_code = ${race.kyosoKigoCode})
          and (${settings.includeWeight} = false or ra.juryo_shubetsu_code = ${race.juryoShubetsuCode})
          and (${settings.includeSurface} = false or ${trackCodeIn(surfaceCodes)})
          and (${settings.includeTurn} = false or ${trackCodeIn(turnCodes)})
          and (${settings.includeDistance} = false or ra.kyori = ${race.kyori})
          and nullif(regexp_replace(coalesce(se.kakutei_chakujun, ''), '[^0-9]', '', 'g'), '') !~ '^0+$'
      ),
      grouped_entries as (
        select distinct
          matched_entries.category,
          matched_entries.name,
          matched_entries.kaisai_nen,
          matched_entries.kaisai_tsukihi,
          matched_entries.keibajo_code,
          matched_entries.wakuban,
          matched_entries.umaban,
          matched_entries.bamei,
          matched_entries.race_bango,
          matched_entries.race_name,
          matched_entries.kakutei_chakujun,
          matched_entries.soha_time,
          matched_entries.tansho_ninkijun,
          matched_entries.tansho_odds,
          matched_entries.ketto_toroku_bango,
          matched_entries.sire,
          matched_entries."sireSire",
          matched_entries."damSire"
        from matched_entries
      ),
      stats_source as (
        select
          grouped_entries.category,
          grouped_entries.name,
          targets."currentHorseNumbers",
          grouped_entries.kaisai_nen,
          grouped_entries.kaisai_tsukihi,
          grouped_entries.keibajo_code,
          grouped_entries.race_bango,
          grouped_entries.race_name,
          grouped_entries.wakuban,
          grouped_entries.umaban,
          grouped_entries.bamei,
          grouped_entries.kakutei_chakujun,
          grouped_entries.soha_time,
          grouped_entries.tansho_ninkijun,
          grouped_entries.tansho_odds,
          grouped_entries.ketto_toroku_bango,
          grouped_entries.sire,
          grouped_entries."sireSire",
          grouped_entries."damSire"
        from grouped_entries
        join targets
          on targets.category = grouped_entries.category
          and targets.name = grouped_entries.name
        where
          grouped_entries.name <> '不明'
          and not exists (
            select 1
            from target_entries
            where
              target_entries.category = grouped_entries.category
              and target_entries.name = grouped_entries.name
              and target_entries.ketto_toroku_bango = grouped_entries.ketto_toroku_bango
          )
          and (
            ${settings.includeFrame} = false
            or exists (
              select 1
              from target_entries
              where
                target_entries.category = grouped_entries.category
                and target_entries.name = grouped_entries.name
                and target_entries.wakuban = grouped_entries.wakuban
            )
          )
          and (${settings.includeRaceNumber} = false or grouped_entries.race_bango = ${race.raceBango})
      ),
      ranked_details as (
        select
          *,
          row_number() over (
            partition by category, name
            order by kaisai_nen desc, kaisai_tsukihi desc, race_bango asc, umaban asc
          ) as "detailRank"
        from stats_source
      ),
      stats as (
        select
          targets.category,
          targets.name,
          targets."currentHorseNumbers",
          coalesce(
            jsonb_agg(
              jsonb_build_object(
                'date', ranked_details.kaisai_nen || ranked_details.kaisai_tsukihi,
                'sireName', ranked_details.sire,
                'sireSireName', ranked_details."sireSire",
                'damSireName', ranked_details."damSire",
                'keibajoCode', ranked_details.keibajo_code,
                'raceNumber', ranked_details.race_bango,
                'raceName', ranked_details.race_name,
                'horseName', ranked_details.bamei,
                'frameNumber', ranked_details.wakuban,
                'horseNumber', ranked_details.umaban,
                'jockeyName', '',
                'rank', ranked_details.kakutei_chakujun,
                'raceTime', ranked_details.soha_time,
                'popularity', ranked_details.tansho_ninkijun,
                'winOdds', ranked_details.tansho_odds
              )
              order by ranked_details.kaisai_nen desc, ranked_details.kaisai_tsukihi desc, ranked_details.race_bango asc, ranked_details.umaban asc
            ) filter (where ranked_details."detailRank" <= 200),
            '[]'::jsonb
          ) as details,
          count(ranked_details.ketto_toroku_bango)::text "starts",
          count(distinct ranked_details.ketto_toroku_bango)::text "horseCount",
          count(ranked_details.ketto_toroku_bango) filter (where ranked_details.kakutei_chakujun = '01')::text "winCount",
          count(ranked_details.ketto_toroku_bango) filter (where ranked_details.kakutei_chakujun in ('01', '02'))::text "quinellaCount",
          count(ranked_details.ketto_toroku_bango) filter (where ranked_details.kakutei_chakujun in ('01', '02', '03'))::text "showCount",
          coalesce(
            round(
              count(ranked_details.ketto_toroku_bango) filter (where ranked_details.kakutei_chakujun = '01') * 100.0 / nullif(count(ranked_details.ketto_toroku_bango), 0),
              1
            ),
            0
          )::text "winRate",
          coalesce(
            round(
              count(ranked_details.ketto_toroku_bango) filter (where ranked_details.kakutei_chakujun in ('01', '02')) * 100.0 / nullif(count(ranked_details.ketto_toroku_bango), 0),
              1
            ),
            0
          )::text "quinellaRate",
          coalesce(
            round(
              count(ranked_details.ketto_toroku_bango) filter (where ranked_details.kakutei_chakujun in ('01', '02', '03')) * 100.0 / nullif(count(ranked_details.ketto_toroku_bango), 0),
              1
            ),
            0
          )::text "showRate"
        from targets
        left join ranked_details
          on ranked_details.category = targets.category
          and ranked_details.name = targets.name
        where targets.name <> '不明'
        group by
          targets.category,
          targets.name,
          targets."currentHorseNumbers"
      ),
      ranked as (
        select
          *,
          row_number() over (
            partition by category
            order by "showRate"::numeric desc, "starts"::numeric desc, name asc
          ) as rank
        from stats
      )
      select
          category,
          "currentHorseNumbers",
          name,
          details,
          "starts",
        "horseCount",
        "winCount",
        "quinellaCount",
        "showCount",
        "winRate",
        "quinellaRate",
        "showRate"
      from ranked
      where rank <= 100
      order by category asc, rank asc
    `);

      return result.rows.map((row) => ({
        category: row.category,
        currentHorseNumbers: row.currentHorseNumbers,
        details: toStatsDetails(row.details),
        horseCount: toCount(row.horseCount),
        name: row.name,
        quinellaCount: toCount(row.quinellaCount),
        quinellaRate: toRate(row.quinellaRate),
        showCount: toCount(row.showCount),
        showRate: toRate(row.showRate),
        starts: toCount(row.starts),
        winCount: toCount(row.winCount),
        winRate: toRate(row.winRate),
      }));
    });
  },
);

export const getSimilarRaceStats = cache(
  async (race: RaceDetail, settings: SimilarRaceStatsSettings): Promise<SimilarRaceStatsRow[]> => {
    return withDbQueryCache(["getSimilarRaceStats", race, settings], async () => {
      const runnerTable = race.source === "jra" ? jvdSe : nvdSe;
      const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
      const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
      const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
      const classCondition = getStatsClassCondition(race, settings.classConditionName, "history");
      const result = await getDb().execute<{
        category: "jockey" | "owner" | "trainer";
        currentHorseNumbers: string;
        details: unknown;
        name: string;
        starts: string;
        horseCount: string;
        winCount: string;
        quinellaCount: string;
        showCount: string;
        winRate: string;
        quinellaRate: string;
        showRate: string;
      }>(sql`
      with current_entries as (
        select
          coalesce(nullif(regexp_replace(umaban, '^0+', ''), ''), '0') as umaban,
          umaban::int as "umabanSort",
          wakuban,
          coalesce(nullif(regexp_replace(kishumei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') as jockey,
          coalesce(nullif(regexp_replace(chokyoshimei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') as trainer,
          coalesce(nullif(regexp_replace(banushimei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') as owner
        from ${runnerTable}
        where
          kaisai_nen = ${race.kaisaiNen}
          and kaisai_tsukihi = ${race.kaisaiTsukihi}
          and keibajo_code = ${race.keibajoCode}
          and race_bango = ${race.raceBango}
      ),
      target_entries as (
        select 'jockey'::text as category, jockey as name, umaban, "umabanSort", wakuban
        from current_entries
        union all
        select 'trainer'::text as category, trainer as name, umaban, "umabanSort", wakuban
        from current_entries
        union all
        select 'owner'::text as category, owner as name, umaban, "umabanSort", wakuban
        from current_entries
      ),
      targets as (
        select
          category,
          name,
          string_agg(umaban, ', ' order by "umabanSort") as "currentHorseNumbers"
        from target_entries
        group by category, name
      ),
      matched_entries as (
        select
          *
        from (
          select
            ra.kaisai_nen,
            ra.kaisai_tsukihi,
            ra.keibajo_code,
            ra.race_bango,
            ra.kyosomei_hondai,
            ra.kyosomei_fukudai,
            ra.kyosomei_kakkonai,
            ra.kyoso_shubetsu_code,
            ra.kyoso_joken_code,
            ra.kyoso_joken_meisho,
            ra.grade_code,
            ra.kyoso_kigo_code,
            ra.juryo_shubetsu_code,
            ra.kyori,
            ra.track_code,
            se.wakuban,
            se.umaban,
            coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') bamei,
            coalesce(
              nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              '一般競走'
            ) race_name,
            se.kakutei_chakujun,
            se.soha_time,
            se.tansho_ninkijun,
            se.tansho_odds,
            se.ketto_toroku_bango,
            coalesce(nullif(regexp_replace(se.kishumei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') jockey,
            coalesce(nullif(regexp_replace(se.chokyoshimei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') trainer,
            coalesce(nullif(regexp_replace(se.banushimei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') owner
          from ${jvdRa} ra
          join ${jvdSe} se
            on se.kaisai_nen = ra.kaisai_nen
            and se.kaisai_tsukihi = ra.kaisai_tsukihi
            and se.keibajo_code = ra.keibajo_code
            and se.race_bango = ra.race_bango
          where ${shouldUseJraStats(race, settings)} = true
          union all
          select
            ra.kaisai_nen,
            ra.kaisai_tsukihi,
            ra.keibajo_code,
            ra.race_bango,
            ra.kyosomei_hondai,
            ra.kyosomei_fukudai,
            ra.kyosomei_kakkonai,
            ra.kyoso_shubetsu_code,
            ra.kyoso_joken_code,
            ra.kyoso_joken_meisho,
            ra.grade_code,
            ra.kyoso_kigo_code,
            ra.juryo_shubetsu_code,
            ra.kyori,
            ra.track_code,
            se.wakuban,
            se.umaban,
            coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') bamei,
            coalesce(
              nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              '一般競走'
            ) race_name,
            se.kakutei_chakujun,
            se.soha_time,
            se.tansho_ninkijun,
            se.tansho_odds,
            se.ketto_toroku_bango,
            coalesce(nullif(regexp_replace(se.kishumei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') jockey,
            coalesce(nullif(regexp_replace(se.chokyoshimei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') trainer,
            coalesce(nullif(regexp_replace(se.banushimei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') owner
          from ${nvdRa} ra
          join ${nvdSe} se
            on se.kaisai_nen = ra.kaisai_nen
            and se.kaisai_tsukihi = ra.kaisai_tsukihi
            and se.keibajo_code = ra.keibajo_code
            and se.race_bango = ra.race_bango
          where ${shouldUseNarStats(race, settings)} = true
        ) history
        where
          history.kaisai_nen || history.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or history.kaisai_nen || history.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
          and (${settings.includeVenue} = false or history.keibajo_code = ${race.keibajoCode})
          and ${monthWindowCondition(raceDate, settings.includeMonthWindow, "history")}
          and (${settings.includeRaceTitle} = false or ${getStatsRaceTitleCondition(race, "history")})
          and (
            ${settings.includeRaceSubtitle} = false
            or ${getStatsRaceSubtitleCondition(race, "history")}
          )
          and (${settings.includeAge} = false or history.kyoso_shubetsu_code = ${race.kyosoShubetsuCode})
          and (
            ${settings.includeClass} = false
            or ${classCondition}
          )
          and (${settings.includeSex} = false or history.kyoso_kigo_code = ${race.kyosoKigoCode})
          and (${settings.includeWeight} = false or history.juryo_shubetsu_code = ${race.juryoShubetsuCode})
          and (${settings.includeSurface} = false or ${trackCodeIn(surfaceCodes, "history")})
          and (${settings.includeTurn} = false or ${trackCodeIn(turnCodes, "history")})
          and (${settings.includeDistance} = false or history.kyori = ${race.kyori})
      ),
      grouped_entries as (
        select
          'jockey'::text as category,
          jockey as name,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          wakuban,
          umaban,
          bamei,
          race_bango,
          race_name,
          kakutei_chakujun,
          soha_time,
          tansho_ninkijun,
          tansho_odds,
          ketto_toroku_bango
        from matched_entries
        union all
        select
          'trainer'::text as category,
          trainer as name,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          wakuban,
          umaban,
          bamei,
          race_bango,
          race_name,
          kakutei_chakujun,
          soha_time,
          tansho_ninkijun,
          tansho_odds,
          ketto_toroku_bango
        from matched_entries
        union all
        select
          'owner'::text as category,
          owner as name,
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          wakuban,
          umaban,
          bamei,
          race_bango,
          race_name,
          kakutei_chakujun,
          soha_time,
          tansho_ninkijun,
          tansho_odds,
          ketto_toroku_bango
        from matched_entries
      ),
      filtered_grouped_entries as (
        select *
        from grouped_entries
        where
          (
            ${settings.includeFrame} = false
            or exists (
              select 1
              from target_entries
              where
                target_entries.category = grouped_entries.category
                and target_entries.name = grouped_entries.name
                and target_entries.wakuban = grouped_entries.wakuban
            )
          )
          and (${settings.includeRaceNumber} = false or grouped_entries.race_bango = ${race.raceBango})
      ),
      ranked_grouped_entries as (
        select
          *,
          row_number() over (
            partition by category, name
            order by kaisai_nen desc, kaisai_tsukihi desc, race_bango asc, umaban asc
          ) as "detailRank"
        from filtered_grouped_entries
      ),
      stats as (
        select
          targets.category,
          targets.name,
          targets."currentHorseNumbers",
          coalesce(
            jsonb_agg(
              jsonb_build_object(
                'date', ranked_grouped_entries.kaisai_nen || ranked_grouped_entries.kaisai_tsukihi,
                'keibajoCode', ranked_grouped_entries.keibajo_code,
                'raceNumber', ranked_grouped_entries.race_bango,
                'raceName', ranked_grouped_entries.race_name,
                'horseName', ranked_grouped_entries.bamei,
                'frameNumber', ranked_grouped_entries.wakuban,
                'horseNumber', ranked_grouped_entries.umaban,
                'jockeyName', '',
                'rank', ranked_grouped_entries.kakutei_chakujun,
                'raceTime', ranked_grouped_entries.soha_time,
                'popularity', ranked_grouped_entries.tansho_ninkijun,
                'winOdds', ranked_grouped_entries.tansho_odds
              )
              order by
                ranked_grouped_entries.kaisai_nen desc,
                ranked_grouped_entries.kaisai_tsukihi desc,
                ranked_grouped_entries.race_bango asc,
                ranked_grouped_entries.umaban asc
            ) filter (where ranked_grouped_entries.name is not null and ranked_grouped_entries."detailRank" <= 200),
            '[]'::jsonb
          ) as details,
          count(ranked_grouped_entries.ketto_toroku_bango)::text as "starts",
          count(distinct ranked_grouped_entries.ketto_toroku_bango)::text as "horseCount",
          count(*) filter (where ranked_grouped_entries.kakutei_chakujun = '01')::text as "winCount",
          count(*) filter (where ranked_grouped_entries.kakutei_chakujun in ('01', '02'))::text as "quinellaCount",
          count(*) filter (where ranked_grouped_entries.kakutei_chakujun in ('01', '02', '03'))::text as "showCount",
          round(
            count(*) filter (where ranked_grouped_entries.kakutei_chakujun = '01') * 100.0 / nullif(count(ranked_grouped_entries.ketto_toroku_bango), 0),
            1
          )::text as "winRate",
          round(
            count(*) filter (where ranked_grouped_entries.kakutei_chakujun in ('01', '02')) * 100.0 / nullif(count(ranked_grouped_entries.ketto_toroku_bango), 0),
            1
          )::text as "quinellaRate",
          round(
            count(*) filter (where ranked_grouped_entries.kakutei_chakujun in ('01', '02', '03')) * 100.0 / nullif(count(ranked_grouped_entries.ketto_toroku_bango), 0),
            1
          )::text as "showRate"
        from targets
        left join ranked_grouped_entries
          on ranked_grouped_entries.category = targets.category
          and ranked_grouped_entries.name = targets.name
        group by
          targets.category,
          targets.name,
          targets."currentHorseNumbers"
      ),
      ranked as (
        select
          *,
          row_number() over (
            partition by category
            order by "showRate"::numeric desc, "starts"::numeric desc, name asc
          ) as rank
        from stats
      )
      select
        category,
        "currentHorseNumbers",
        name,
        details,
        "starts",
        "horseCount",
        "winCount",
        "quinellaCount",
        "showCount",
        "winRate",
        "quinellaRate",
        "showRate"
      from ranked
      where rank <= 100
      order by category asc, rank asc
    `);

      return result.rows.map((row) => ({
        category: row.category,
        currentHorseNumbers: row.currentHorseNumbers,
        details: toStatsDetails(row.details),
        horseCount: toCount(row.horseCount),
        name: row.name,
        quinellaCount: toCount(row.quinellaCount),
        quinellaRate: toRate(row.quinellaRate),
        showCount: toCount(row.showCount),
        showRate: toRate(row.showRate),
        starts: toCount(row.starts),
        winCount: toCount(row.winCount),
        winRate: toRate(row.winRate),
      }));
    });
  },
);

export const getTimeScoreRows = cache(
  async (race: RaceDetail, settings: SimilarRaceStatsSettings): Promise<TimeScoreRow[]> => {
    return withDbQueryCache(["getTimeScoreRows", race, settings], async () => {
      const statsSource = getSingleStatsSource(race, settings);
      const raceTable = statsSource === "jra" ? jvdRa : nvdRa;
      const runnerTable = statsSource === "jra" ? jvdSe : nvdSe;
      const currentRunnerTable = race.source === "jra" ? jvdSe : nvdSe;
      const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
      const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
      const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
      const classCondition = getStatsClassCondition(race, settings.classConditionName);
      const raceTitleCondition = getStatsRaceTitleCondition(race);
      const raceSubtitleCondition = getStatsRaceSubtitleCondition(race);
      const result = await getDb().execute<{ rows: unknown }>(sql`
      with current_horses as (
        select
          coalesce(nullif(regexp_replace(se.umaban, '^0+', ''), ''), '0') horse_number,
          se.umaban::int horse_number_sort,
          coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') horse_name,
          se.ketto_toroku_bango,
          nullif(regexp_replace(coalesce(se.barei, ''), '[^0-9]', '', 'g'), '')::numeric current_age
        from ${currentRunnerTable} se
        where
          se.kaisai_nen = ${race.kaisaiNen}
          and se.kaisai_tsukihi = ${race.kaisaiTsukihi}
          and se.keibajo_code = ${race.keibajoCode}
          and se.race_bango = ${race.raceBango}
          and btrim(coalesce(se.ketto_toroku_bango, '')) <> ''
      ),
      matched_races as (
        select
          ra.kaisai_nen,
          ra.kaisai_tsukihi,
          ra.keibajo_code,
          ra.race_bango
        from ${raceTable} ra
        where
          ra.kaisai_nen || ra.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or ra.kaisai_nen || ra.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
          and (${settings.includeVenue} = false or ra.keibajo_code = ${race.keibajoCode})
          and ${monthWindowCondition(raceDate, settings.includeMonthWindow)}
          and ${runnerCountCondition(runnerTable, settings)}
          and (${settings.includeRaceTitle} = false or ${raceTitleCondition})
          and (${settings.includeRaceSubtitle} = false or ${raceSubtitleCondition})
          and (${settings.includeAge} = false or ra.kyoso_shubetsu_code = ${race.kyosoShubetsuCode})
          and (${settings.includeClass} = false or ${classCondition})
          and (${settings.includeSex} = false or ra.kyoso_kigo_code = ${race.kyosoKigoCode})
          and (${settings.includeWeight} = false or ra.juryo_shubetsu_code = ${race.juryoShubetsuCode})
          and (${settings.includeSurface} = false or ${trackCodeIn(surfaceCodes)})
          and (${settings.includeTurn} = false or ${trackCodeIn(turnCodes)})
          and (${settings.includeDistance} = false or ra.kyori = ${race.kyori})
          and (${settings.includeRaceNumber} = false or ra.race_bango = ${race.raceBango})
      ),
      target_profile as (
        select
          avg(nullif(regexp_replace(coalesce(se.soha_time, ''), '[^0-9]', '', 'g'), '')::numeric) target_race_time,
          avg(nullif(regexp_replace(coalesce(se.kohan_3f, ''), '[^0-9]', '', 'g'), '')::numeric) target_last3f,
          avg(nullif(regexp_replace(coalesce(se.bataiju, ''), '[^0-9]', '', 'g'), '')::numeric) target_body_weight,
          avg(nullif(regexp_replace(coalesce(se.futan_juryo, ''), '[^0-9]', '', 'g'), '')::numeric) target_carried_weight,
          avg(nullif(regexp_replace(coalesce(se.time_sa, ''), '[^0-9]', '', 'g'), '')::numeric) target_margin
        from matched_races
        join ${runnerTable} se
          on se.kaisai_nen = matched_races.kaisai_nen
          and se.kaisai_tsukihi = matched_races.kaisai_tsukihi
          and se.keibajo_code = matched_races.keibajo_code
          and se.race_bango = matched_races.race_bango
        where se.kakutei_chakujun in ('01', '02', '03')
      ),
      history as (
        select
          current_horses.horse_number,
          current_horses.horse_number_sort,
          current_horses.horse_name,
          past.keibajo_code,
          past.race_date,
          past.distance,
          past.race_time,
          past.last3f,
          past.body_weight,
          past.carried_weight,
          past.margin,
          1.0 / (
            1.0 + greatest(
              0,
              to_date(${raceDate}, 'YYYYMMDD') - to_date(past.race_date, 'YYYYMMDD')
            ) / case
              when current_horses.current_age is null then 365.0
              when current_horses.current_age <= 3 then 180.0
              when current_horses.current_age = 4 then 270.0
              else 365.0
            end
          ) recency_weight,
          case
            when past.distance is null or nullif(regexp_replace(coalesce(${race.kyori}, ''), '[^0-9]', '', 'g'), '')::numeric is null then 0.5
            else greatest(
              0,
              1 - abs(past.distance - nullif(regexp_replace(coalesce(${race.kyori}, ''), '[^0-9]', '', 'g'), '')::numeric)
                / greatest(nullif(regexp_replace(coalesce(${race.kyori}, ''), '[^0-9]', '', 'g'), '')::numeric * 0.5, 400)
            )
          end distance_score
        from current_horses
        join (
          select
            se.ketto_toroku_bango,
            se.kaisai_nen || se.kaisai_tsukihi race_date,
            se.keibajo_code,
            nullif(regexp_replace(coalesce(ra.kyori, ''), '[^0-9]', '', 'g'), '')::numeric distance,
            nullif(regexp_replace(coalesce(se.soha_time, ''), '[^0-9]', '', 'g'), '')::numeric race_time,
            nullif(regexp_replace(coalesce(se.kohan_3f, ''), '[^0-9]', '', 'g'), '')::numeric last3f,
            nullif(regexp_replace(coalesce(se.bataiju, ''), '[^0-9]', '', 'g'), '')::numeric body_weight,
            nullif(regexp_replace(coalesce(se.futan_juryo, ''), '[^0-9]', '', 'g'), '')::numeric carried_weight,
            nullif(regexp_replace(coalesce(se.time_sa, ''), '[^0-9]', '', 'g'), '')::numeric margin
          from ${jvdSe} se
          join ${jvdRa} ra
            on ra.kaisai_nen = se.kaisai_nen
            and ra.kaisai_tsukihi = se.kaisai_tsukihi
            and ra.keibajo_code = se.keibajo_code
            and ra.race_bango = se.race_bango
          where se.ketto_toroku_bango in (select ketto_toroku_bango from current_horses)
          union all
          select
            se.ketto_toroku_bango,
            se.kaisai_nen || se.kaisai_tsukihi race_date,
            se.keibajo_code,
            nullif(regexp_replace(coalesce(ra.kyori, ''), '[^0-9]', '', 'g'), '')::numeric distance,
            nullif(regexp_replace(coalesce(se.soha_time, ''), '[^0-9]', '', 'g'), '')::numeric race_time,
            nullif(regexp_replace(coalesce(se.kohan_3f, ''), '[^0-9]', '', 'g'), '')::numeric last3f,
            nullif(regexp_replace(coalesce(se.bataiju, ''), '[^0-9]', '', 'g'), '')::numeric body_weight,
            nullif(regexp_replace(coalesce(se.futan_juryo, ''), '[^0-9]', '', 'g'), '')::numeric carried_weight,
            nullif(regexp_replace(coalesce(se.time_sa, ''), '[^0-9]', '', 'g'), '')::numeric margin
          from ${nvdSe} se
          join ${nvdRa} ra
            on ra.kaisai_nen = se.kaisai_nen
            and ra.kaisai_tsukihi = se.kaisai_tsukihi
            and ra.keibajo_code = se.keibajo_code
            and ra.race_bango = se.race_bango
          where se.ketto_toroku_bango in (select ketto_toroku_bango from current_horses)
        ) past
          on past.ketto_toroku_bango = current_horses.ketto_toroku_bango
        where past.race_date < ${raceDate}
      ),
      current_profile as (
        select
          current_horses.horse_number,
          current_horses.horse_number_sort,
          current_horses.horse_name,
          coalesce(
            sum(history.race_time * history.recency_weight * (0.5 + history.distance_score * 0.5))
              / nullif(sum(history.recency_weight * (0.5 + history.distance_score * 0.5)) filter (where history.race_time is not null), 0),
            null
          ) weighted_race_time,
          coalesce(
            sum(history.last3f * history.recency_weight * (0.5 + history.distance_score * 0.5))
              / nullif(sum(history.recency_weight * (0.5 + history.distance_score * 0.5)) filter (where history.last3f is not null), 0),
            null
          ) weighted_last3f,
          coalesce(
            sum(history.body_weight * history.recency_weight * (0.5 + history.distance_score * 0.5))
              / nullif(sum(history.recency_weight * (0.5 + history.distance_score * 0.5)) filter (where history.body_weight is not null), 0),
            null
          ) weighted_body_weight,
          coalesce(
            sum(history.carried_weight * history.recency_weight * (0.5 + history.distance_score * 0.5))
              / nullif(sum(history.recency_weight * (0.5 + history.distance_score * 0.5)) filter (where history.carried_weight is not null), 0),
            null
          ) weighted_carried_weight,
          coalesce(
            sum(history.margin * history.recency_weight * (0.5 + history.distance_score * 0.5))
              / nullif(sum(history.recency_weight * (0.5 + history.distance_score * 0.5)) filter (where history.margin is not null), 0),
            null
          ) weighted_margin,
          coalesce(
            sum(case when history.keibajo_code = ${race.keibajoCode} then history.recency_weight * (0.5 + history.distance_score * 0.5) else 0 end)
              / nullif(sum(history.recency_weight * (0.5 + history.distance_score * 0.5)), 0),
            0.5
          ) venue_score,
          coalesce(
            sum(history.distance_score * history.recency_weight)
              / nullif(sum(history.recency_weight), 0),
            0.5
          ) distance_score
        from current_horses
        left join history on history.horse_number = current_horses.horse_number
        group by current_horses.horse_number, current_horses.horse_number_sort, current_horses.horse_name
      ),
      score_base as (
        select
          current_profile.horse_number,
          current_profile.horse_number_sort,
          current_profile.horse_name,
          current_profile.weighted_race_time,
          current_profile.weighted_last3f,
          current_profile.weighted_body_weight,
          current_profile.weighted_carried_weight,
          current_profile.weighted_margin,
          current_profile.distance_score,
          target_profile.target_race_time,
          target_profile.target_last3f,
          target_profile.target_body_weight,
          target_profile.target_carried_weight,
          target_profile.target_margin,
          case
            when current_profile.weighted_race_time is null or target_profile.target_race_time is null then 0.5
            else greatest(0, 1 - abs(current_profile.weighted_race_time - target_profile.target_race_time) / greatest(target_profile.target_race_time * 0.08, 80))
          end race_time_score,
          case
            when current_profile.weighted_last3f is null or target_profile.target_last3f is null then 0.5
            else greatest(0, 1 - abs(current_profile.weighted_last3f - target_profile.target_last3f) / 30.0)
          end last3f_score,
          case
            when current_profile.weighted_body_weight is null or target_profile.target_body_weight is null then 0.5
            else greatest(0, 1 - abs(current_profile.weighted_body_weight - target_profile.target_body_weight) / 80.0)
          end body_weight_score,
          case
            when current_profile.weighted_carried_weight is null or target_profile.target_carried_weight is null then 0.5
            else greatest(0, 1 - abs(current_profile.weighted_carried_weight - target_profile.target_carried_weight) / 30.0)
          end carried_weight_score,
          case
            when current_profile.weighted_margin is null or target_profile.target_margin is null then 0.5
            else greatest(0, 1 - abs(current_profile.weighted_margin - target_profile.target_margin) / 50.0)
          end margin_score,
          current_profile.venue_score
        from current_profile
        cross join target_profile
      )
      select coalesce(
        jsonb_agg(
          jsonb_build_object(
            'horseNumber', score_base.horse_number,
            'horseName', score_base.horse_name,
            'score', round((
              score_base.race_time_score * 0.30 +
              score_base.last3f_score * 0.20 +
              score_base.distance_score * 0.15 +
              score_base.venue_score * 0.15 +
              score_base.body_weight_score * 0.10 +
              score_base.carried_weight_score * 0.05 +
              score_base.margin_score * 0.05
            )::numeric, 2),
            'details', jsonb_build_array(
              jsonb_build_object(
                'label', 'レースタイム',
                'value', round(score_base.weighted_race_time::numeric, 1),
                'target', round(score_base.target_race_time::numeric, 1),
                'score', round(score_base.race_time_score::numeric, 2),
                'weight', 0.30,
                'reason', '全ての過去成績を日付と今回距離への近さで重み付けし、同条件1〜3着馬の平均レースタイムに近いほど高評価'
              ),
              jsonb_build_object(
                'label', '上がり3F',
                'value', round(score_base.weighted_last3f::numeric, 1),
                'target', round(score_base.target_last3f::numeric, 1),
                'score', round(score_base.last3f_score::numeric, 2),
                'weight', 0.20,
                'reason', '全ての過去成績を日付と今回距離への近さで重み付けし、同条件1〜3着馬の平均上がり3Fに近いほど高評価'
              ),
              jsonb_build_object(
                'label', '距離適性',
                'value', round((score_base.distance_score * 100)::numeric, 1),
                'target', 100,
                'score', round(score_base.distance_score::numeric, 2),
                'weight', 0.15,
                'reason', '全ての過去成績について、今回レース距離に近い成績ほど高く評価'
              ),
              jsonb_build_object(
                'label', '競馬場',
                'value', round((score_base.venue_score * 100)::numeric, 1),
                'target', 100,
                'score', round(score_base.venue_score::numeric, 2),
                'weight', 0.15,
                'reason', '過去成績のうち今回と同じ競馬場の比率を日付の新しさで重み付け'
              ),
              jsonb_build_object(
                'label', '馬体重',
                'value', round(score_base.weighted_body_weight::numeric, 1),
                'target', round(score_base.target_body_weight::numeric, 1),
                'score', round(score_base.body_weight_score::numeric, 2),
                'weight', 0.10,
                'reason', '過去成績の馬体重を日付が新しいほど重く見て、同条件1〜3着馬の平均に近いほど高評価'
              ),
              jsonb_build_object(
                'label', '負担重量',
                'value', round(score_base.weighted_carried_weight::numeric, 1),
                'target', round(score_base.target_carried_weight::numeric, 1),
                'score', round(score_base.carried_weight_score::numeric, 2),
                'weight', 0.05,
                'reason', '全ての過去成績の負担重量を日付と今回距離への近さで重み付けし、同条件1〜3着馬の平均に近いほど高評価'
              ),
              jsonb_build_object(
                'label', '着差',
                'value', round(score_base.weighted_margin::numeric, 1),
                'target', round(score_base.target_margin::numeric, 1),
                'score', round(score_base.margin_score::numeric, 2),
                'weight', 0.05,
                'reason', '全ての過去成績の着差を日付と今回距離への近さで重み付けし、同条件1〜3着馬の平均に近いほど高評価'
              )
            )
          )
          order by
            (
              score_base.race_time_score * 0.30 +
              score_base.last3f_score * 0.20 +
              score_base.distance_score * 0.15 +
              score_base.venue_score * 0.15 +
              score_base.body_weight_score * 0.10 +
              score_base.carried_weight_score * 0.05 +
              score_base.margin_score * 0.05
            ) desc,
            score_base.horse_number_sort asc
        ),
        '[]'::jsonb
      ) rows
      from score_base
    `);

      return toTimeScoreRows(result.rows[0]?.rows);
    });
  },
);

export const getRaceTimeStats = cache(
  async (race: RaceDetail, settings: SimilarRaceStatsSettings): Promise<RaceTimeStats> => {
    return withDbQueryCache(["getRaceTimeStats", race, settings], async () => {
      const statsSource = getSingleStatsSource(race, settings);
      const raceTable = statsSource === "jra" ? jvdRa : nvdRa;
      const runnerTable = statsSource === "jra" ? jvdSe : nvdSe;
      const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
      const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
      const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
      const classCondition = getStatsClassCondition(race, settings.classConditionName);
      const raceTitleCondition = getStatsRaceTitleCondition(race);
      const raceSubtitleCondition = getStatsRaceSubtitleCondition(race);
      const result = await getDb().execute<{
        raceCount: string;
        fastestRaceTime: string | null;
        fastestKohan3f: string | null;
        averageRaceTime: string | null;
        averageKohan3f: string | null;
        medianRaceTime: string | null;
        medianKohan3f: string | null;
        fastestDate: string | null;
        fastestKeibajoCode: string | null;
        fastestRaceNumber: string | null;
        fastestRaceName: string | null;
        fastestHorseName: string | null;
        fastestFrameNumber: string | null;
        fastestHorseNumber: string | null;
        fastestJockeyName: string | null;
        fastestRank: string | null;
        fastestPopularity: string | null;
        fastestWinOdds: string | null;
        correlationRows: unknown;
        targetRaces: unknown;
      }>(sql`
      with matched_races as (
        select
          ra.kaisai_nen,
          ra.kaisai_tsukihi,
          ra.keibajo_code,
          ra.race_bango,
          coalesce(
            nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            '一般競走'
          ) as race_name
        from ${raceTable} ra
        where
          ra.kaisai_nen || ra.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or ra.kaisai_nen || ra.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
          and (${settings.includeVenue} = false or ra.keibajo_code = ${race.keibajoCode})
          and ${monthWindowCondition(raceDate, settings.includeMonthWindow)}
          and ${runnerCountCondition(runnerTable, settings)}
          and (${settings.includeRaceTitle} = false or ${raceTitleCondition})
          and (${settings.includeRaceSubtitle} = false or ${raceSubtitleCondition})
          and (${settings.includeAge} = false or ra.kyoso_shubetsu_code = ${race.kyosoShubetsuCode})
          and (${settings.includeClass} = false or ${classCondition})
          and (${settings.includeSex} = false or ra.kyoso_kigo_code = ${race.kyosoKigoCode})
          and (${settings.includeWeight} = false or ra.juryo_shubetsu_code = ${race.juryoShubetsuCode})
          and (${settings.includeSurface} = false or ${trackCodeIn(surfaceCodes)})
          and (${settings.includeTurn} = false or ${trackCodeIn(turnCodes)})
          and (${settings.includeDistance} = false or ra.kyori = ${race.kyori})
          and (${settings.includeRaceNumber} = false or ra.race_bango = ${race.raceBango})
      ),
      winner_rows as (
        select
          matched_races.kaisai_nen,
          matched_races.kaisai_tsukihi,
          matched_races.keibajo_code,
          matched_races.race_bango,
          matched_races.race_name,
          se.wakuban,
          se.umaban,
          coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as bamei,
          se.kakutei_chakujun,
          coalesce(nullif(regexp_replace(se.kishumei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as jockey_name,
          coalesce(nullif(regexp_replace(se.chokyoshimei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as trainer_name,
          coalesce(nullif(regexp_replace(se.banushimei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as owner_name,
          se.tansho_ninkijun,
          se.tansho_odds,
          nullif(regexp_replace(coalesce(se.soha_time, ''), '[^0-9]', '', 'g'), '')::numeric as race_time,
          nullif(regexp_replace(coalesce(se.kohan_3f, ''), '[^0-9]', '', 'g'), '')::numeric as kohan_3f
        from matched_races
        join ${runnerTable} se
          on se.kaisai_nen = matched_races.kaisai_nen
          and se.kaisai_tsukihi = matched_races.kaisai_tsukihi
          and se.keibajo_code = matched_races.keibajo_code
          and se.race_bango = matched_races.race_bango
        where
          se.kakutei_chakujun = '01'
          and nullif(regexp_replace(coalesce(se.soha_time, ''), '[^0-9]', '', 'g'), '') !~ '^0+$'
      ),
      target_top3 as (
        select
          matched_races.kaisai_nen,
          matched_races.kaisai_tsukihi,
          matched_races.kaisai_nen || matched_races.kaisai_tsukihi race_date,
          matched_races.keibajo_code,
          matched_races.race_bango,
          se.ketto_toroku_bango,
          se.kakutei_chakujun,
          coalesce(nullif(btrim(se.kishumei_ryakusho, ' 　'), ''), '-') jockey_name,
          coalesce(nullif(btrim(se.chokyoshimei_ryakusho, ' 　'), ''), '-') trainer_name,
          coalesce(nullif(btrim(se.banushimei, ' 　'), ''), '-') owner_name,
          nullif(regexp_replace(coalesce(se.tansho_ninkijun, ''), '[^0-9]', '', 'g'), '')::numeric popularity,
          nullif(regexp_replace(coalesce(se.tansho_odds, ''), '[^0-9]', '', 'g'), '')::numeric / 10.0 odds
        from matched_races
        join ${runnerTable} se
          on se.kaisai_nen = matched_races.kaisai_nen
          and se.kaisai_tsukihi = matched_races.kaisai_tsukihi
          and se.keibajo_code = matched_races.keibajo_code
          and se.race_bango = matched_races.race_bango
        where se.kakutei_chakujun in ('01', '02', '03')
      ),
      target_averages as (
        select
          count(*) filter (where target_top3.kakutei_chakujun = '01') * 100.0 / nullif(count(*), 0) target_horse_win,
          count(*) filter (where target_top3.kakutei_chakujun in ('01', '02', '03')) * 100.0 / nullif(count(*), 0) target_horse_show,
          avg(target_top3.popularity) target_popularity,
          avg(target_top3.odds) target_odds,
          null::numeric target_jockey_show,
          null::numeric target_trainer_show,
          null::numeric target_owner_show
        from target_top3
      ),
      current_entries as (
        select
          coalesce(nullif(regexp_replace(se.umaban, '^0+', ''), ''), '0') horse_number,
          se.umaban::int horse_number_sort,
          coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') horse_name,
          se.ketto_toroku_bango,
          coalesce(nullif(btrim(se.kishumei_ryakusho, ' 　'), ''), '-') jockey_name,
          coalesce(nullif(btrim(se.chokyoshimei_ryakusho, ' 　'), ''), '-') trainer_name,
          coalesce(nullif(btrim(se.banushimei, ' 　'), ''), '-') owner_name,
          nullif(regexp_replace(coalesce(se.tansho_ninkijun, ''), '[^0-9]', '', 'g'), '')::numeric popularity,
          nullif(regexp_replace(coalesce(se.tansho_odds, ''), '[^0-9]', '', 'g'), '')::numeric / 10.0 odds
        from ${runnerTable} se
        where
          se.kaisai_nen = ${race.kaisaiNen}
          and se.kaisai_tsukihi = ${race.kaisaiTsukihi}
          and se.keibajo_code = ${race.keibajoCode}
          and se.race_bango = ${race.raceBango}
      ),
      current_horse_stats as (
        select
          current_entries.horse_number,
          count(hist.*) starts,
          count(hist.*) filter (where hist.kakutei_chakujun = '01') win_count,
          count(hist.*) filter (where hist.kakutei_chakujun in ('01', '02', '03')) show_count
        from current_entries
        left join ${runnerTable} hist
          on hist.ketto_toroku_bango = current_entries.ketto_toroku_bango
          and (
            hist.kaisai_nen < ${race.kaisaiNen}
            or (hist.kaisai_nen = ${race.kaisaiNen} and hist.kaisai_tsukihi < ${race.kaisaiTsukihi})
          )
          and nullif(regexp_replace(coalesce(hist.kakutei_chakujun, ''), '[^0-9]', '', 'g'), '') !~ '^0+$'
        group by current_entries.horse_number
      ),
      current_jockey_stats as (
        select
          current_entries.jockey_name,
          count(hist.*) starts,
          count(hist.*) filter (where hist.kakutei_chakujun in ('01', '02', '03')) show_count
        from (select distinct jockey_name from current_entries) current_entries
        left join ${runnerTable} hist
          on coalesce(nullif(btrim(hist.kishumei_ryakusho, ' 　'), ''), '-') = current_entries.jockey_name
          and (
            hist.kaisai_nen < ${race.kaisaiNen}
            or (hist.kaisai_nen = ${race.kaisaiNen} and hist.kaisai_tsukihi < ${race.kaisaiTsukihi})
          )
          and nullif(regexp_replace(coalesce(hist.kakutei_chakujun, ''), '[^0-9]', '', 'g'), '') !~ '^0+$'
        group by current_entries.jockey_name
      ),
      current_trainer_stats as (
        select
          current_entries.trainer_name,
          count(hist.*) starts,
          count(hist.*) filter (where hist.kakutei_chakujun in ('01', '02', '03')) show_count
        from (select distinct trainer_name from current_entries) current_entries
        left join ${runnerTable} hist
          on coalesce(nullif(btrim(hist.chokyoshimei_ryakusho, ' 　'), ''), '-') = current_entries.trainer_name
          and (
            hist.kaisai_nen < ${race.kaisaiNen}
            or (hist.kaisai_nen = ${race.kaisaiNen} and hist.kaisai_tsukihi < ${race.kaisaiTsukihi})
          )
          and nullif(regexp_replace(coalesce(hist.kakutei_chakujun, ''), '[^0-9]', '', 'g'), '') !~ '^0+$'
        group by current_entries.trainer_name
      ),
      current_owner_stats as (
        select
          current_entries.owner_name,
          count(hist.*) starts,
          count(hist.*) filter (where hist.kakutei_chakujun in ('01', '02', '03')) show_count
        from (select distinct owner_name from current_entries) current_entries
        left join ${runnerTable} hist
          on coalesce(nullif(btrim(hist.banushimei, ' 　'), ''), '-') = current_entries.owner_name
          and (
            hist.kaisai_nen < ${race.kaisaiNen}
            or (hist.kaisai_nen = ${race.kaisaiNen} and hist.kaisai_tsukihi < ${race.kaisaiTsukihi})
          )
          and nullif(regexp_replace(coalesce(hist.kakutei_chakujun, ''), '[^0-9]', '', 'g'), '') !~ '^0+$'
        group by current_entries.owner_name
      ),
      current_profiles as (
        select
          current_entries.*,
          current_horse_stats.starts horse_starts,
          current_horse_stats.win_count horse_win_count,
          current_horse_stats.show_count horse_show_count,
          current_jockey_stats.starts jockey_starts,
          current_jockey_stats.show_count jockey_show_count,
          current_trainer_stats.starts trainer_starts,
          current_trainer_stats.show_count trainer_show_count,
          current_owner_stats.starts owner_starts,
          current_owner_stats.show_count owner_show_count
        from current_entries
        left join current_horse_stats on current_horse_stats.horse_number = current_entries.horse_number
        left join current_jockey_stats on current_jockey_stats.jockey_name = current_entries.jockey_name
        left join current_trainer_stats on current_trainer_stats.trainer_name = current_entries.trainer_name
        left join current_owner_stats on current_owner_stats.owner_name = current_entries.owner_name
      ),
      current_features as (
        select
          *,
          horse_win_count * 100.0 / nullif(horse_starts, 0) horse_win,
          horse_show_count * 100.0 / nullif(horse_starts, 0) horse_show,
          jockey_show_count * 100.0 / nullif(jockey_starts, 0) jockey_show,
          trainer_show_count * 100.0 / nullif(trainer_starts, 0) trainer_show,
          owner_show_count * 100.0 / nullif(owner_starts, 0) owner_show
        from current_profiles
      ),
      correlation_base as (
        select
          current_features.*,
          target_averages.*,
          case when current_features.horse_show is null or target_averages.target_horse_show is null then 0.5 else greatest(0, 1 - abs(current_features.horse_show - target_averages.target_horse_show) / 100.0) end horse_show_score,
          case when current_features.horse_win is null or target_averages.target_horse_win is null then 0.5 else greatest(0, 1 - abs(current_features.horse_win - target_averages.target_horse_win) / 100.0) end horse_win_score,
          case when current_features.jockey_show is null then 0.5 else greatest(0, least(1, current_features.jockey_show / 100.0)) end jockey_show_score,
          case when current_features.trainer_show is null then 0.5 else greatest(0, least(1, current_features.trainer_show / 100.0)) end trainer_show_score,
          case when current_features.owner_show is null then 0.5 else greatest(0, least(1, current_features.owner_show / 100.0)) end owner_show_score,
          case when current_features.popularity is null or target_averages.target_popularity is null then 0.5 else greatest(0, 1 - abs(current_features.popularity - target_averages.target_popularity) / greatest(target_averages.target_popularity, 5.0)) end popularity_score,
          case when current_features.odds is null or target_averages.target_odds is null then 0.5 else greatest(0, 1 - abs(current_features.odds - target_averages.target_odds) / greatest(target_averages.target_odds, 10.0)) end odds_score
        from current_features
        cross join target_averages
      ),
      correlation_rows as (
        select
          coalesce(
            jsonb_agg(
              jsonb_build_object(
                'horseNumber', correlation_base.horse_number,
                'horseName', correlation_base.horse_name,
                'score', round((
                  correlation_base.horse_show_score * 0.20 +
                  correlation_base.horse_win_score * 0.10 +
                  correlation_base.jockey_show_score * 0.15 +
                  correlation_base.trainer_show_score * 0.15 +
                  correlation_base.owner_show_score * 0.15 +
                  correlation_base.popularity_score * 0.125 +
                  correlation_base.odds_score * 0.125
                )::numeric, 2),
                'details', jsonb_build_array(
                  jsonb_build_object('key', 'horseShow', 'label', '出走馬の複勝率', 'value', round(correlation_base.horse_show::numeric, 1), 'target', round(correlation_base.target_horse_show::numeric, 1), 'score', round(correlation_base.horse_show_score::numeric, 2), 'weight', 0.20, 'reason', '対象レース1〜3着馬の対象レース前の複勝率平均との差'),
                  jsonb_build_object('key', 'horseWin', 'label', '出走馬の勝率', 'value', round(correlation_base.horse_win::numeric, 1), 'target', round(correlation_base.target_horse_win::numeric, 1), 'score', round(correlation_base.horse_win_score::numeric, 2), 'weight', 0.10, 'reason', '対象レース1〜3着馬の対象レース前の勝率平均との差'),
                  jsonb_build_object('key', 'jockeyShow', 'label', '騎手の複勝率', 'value', round(correlation_base.jockey_show::numeric, 1), 'target', null, 'score', round(correlation_base.jockey_show_score::numeric, 2), 'weight', 0.15, 'reason', '今回騎乗予定騎手の今回レース前の複勝率を評価'),
                  jsonb_build_object('key', 'trainerShow', 'label', '調教師の複勝率', 'value', round(correlation_base.trainer_show::numeric, 1), 'target', null, 'score', round(correlation_base.trainer_show_score::numeric, 2), 'weight', 0.15, 'reason', '今回出走馬の調教師の今回レース前の複勝率を評価'),
                  jsonb_build_object('key', 'ownerShow', 'label', '馬主の複勝率', 'value', round(correlation_base.owner_show::numeric, 1), 'target', null, 'score', round(correlation_base.owner_show_score::numeric, 2), 'weight', 0.15, 'reason', '今回出走馬の馬主の今回レース前の複勝率を評価'),
                  jsonb_build_object('key', 'popularity', 'label', '人気順', 'value', correlation_base.popularity, 'target', round(correlation_base.target_popularity::numeric, 1), 'score', round(correlation_base.popularity_score::numeric, 2), 'weight', 0.125, 'reason', '対象レース1〜3着馬の人気平均との差'),
                  jsonb_build_object('key', 'odds', 'label', '単勝オッズ', 'value', correlation_base.odds, 'target', round(correlation_base.target_odds::numeric, 1), 'score', round(correlation_base.odds_score::numeric, 2), 'weight', 0.125, 'reason', '対象レース1〜3着馬の単勝オッズ平均との差')
                )
              )
              order by
                (
                  correlation_base.horse_show_score * 0.20 +
                  correlation_base.horse_win_score * 0.10 +
                  correlation_base.jockey_show_score * 0.15 +
                  correlation_base.trainer_show_score * 0.15 +
                  correlation_base.owner_show_score * 0.15 +
                  correlation_base.popularity_score * 0.125 +
                  correlation_base.odds_score * 0.125
                ) desc,
                correlation_base.horse_number_sort asc
            ),
            '[]'::jsonb
          ) "correlationRows"
        from correlation_base
      ),
      stats as (
        select
          count(*)::text as "raceCount",
          min(race_time)::text as "fastestRaceTime",
          (array_agg(kohan_3f order by race_time asc, kohan_3f asc nulls last))[1]::text as "fastestKohan3f",
          round(avg(race_time), 1)::text as "averageRaceTime",
          round(avg(kohan_3f), 1)::text as "averageKohan3f",
          round((percentile_cont(0.5) within group (order by race_time))::numeric, 1)::text as "medianRaceTime",
          round((percentile_cont(0.5) within group (order by kohan_3f))::numeric, 1)::text as "medianKohan3f"
        from winner_rows
      ),
      fastest as (
        select *
        from winner_rows
        order by race_time asc, kohan_3f asc nulls last, kaisai_nen desc, kaisai_tsukihi desc
        limit 1
      ),
      target_races as (
        select
          coalesce(
            jsonb_agg(
              jsonb_build_object(
                'date', limited_winner_rows.kaisai_nen || limited_winner_rows.kaisai_tsukihi,
                'keibajoCode', limited_winner_rows.keibajo_code,
                'raceNumber', limited_winner_rows.race_bango,
                'raceName', limited_winner_rows.race_name,
                'horseNumber', limited_winner_rows.umaban,
                'horseName', limited_winner_rows.bamei,
                'jockeyName', limited_winner_rows.jockey_name,
                'trainerName', limited_winner_rows.trainer_name,
                'ownerName', limited_winner_rows.owner_name,
                'raceTime', limited_winner_rows.race_time::text,
                'kohan3f', limited_winner_rows.kohan_3f::text,
                'popularity', limited_winner_rows.tansho_ninkijun
              )
              order by limited_winner_rows.kaisai_nen desc, limited_winner_rows.kaisai_tsukihi desc, limited_winner_rows.race_bango asc
            ),
            '[]'::jsonb
          ) "targetRaces"
        from (
          select *
          from winner_rows
          order by kaisai_nen desc, kaisai_tsukihi desc, race_bango asc
          limit 500
        ) limited_winner_rows
      )
      select
        stats."raceCount",
        stats."fastestRaceTime",
        stats."fastestKohan3f",
        stats."averageRaceTime",
        stats."averageKohan3f",
        stats."medianRaceTime",
        stats."medianKohan3f",
        fastest.kaisai_nen || fastest.kaisai_tsukihi as "fastestDate",
        fastest.keibajo_code as "fastestKeibajoCode",
        fastest.race_bango as "fastestRaceNumber",
        fastest.race_name as "fastestRaceName",
        fastest.bamei as "fastestHorseName",
        fastest.wakuban as "fastestFrameNumber",
        fastest.umaban as "fastestHorseNumber",
        fastest.jockey_name as "fastestJockeyName",
        fastest.kakutei_chakujun as "fastestRank",
        fastest.tansho_ninkijun as "fastestPopularity",
        fastest.tansho_odds as "fastestWinOdds",
        target_races."targetRaces",
        correlation_rows."correlationRows"
      from stats
      left join fastest on true
      cross join target_races
      cross join correlation_rows
    `);

      const row = result.rows[0];
      const fastestDetail =
        row?.fastestDate && row.fastestKeibajoCode && row.fastestRaceNumber
          ? {
              date: row.fastestDate,
              frameNumber: row.fastestFrameNumber ?? "",
              horseName: row.fastestHorseName ?? "",
              horseNumber: row.fastestHorseNumber ?? "",
              jockeyName: row.fastestJockeyName ?? "",
              keibajoCode: row.fastestKeibajoCode,
              popularity: row.fastestPopularity ?? "",
              raceName: row.fastestRaceName ?? "",
              raceNumber: row.fastestRaceNumber,
              raceTime: row.fastestRaceTime ?? "",
              rank: row.fastestRank ?? "",
              winOdds: row.fastestWinOdds ?? "",
            }
          : null;

      return {
        averageKohan3f: toNullableNumber(row?.averageKohan3f),
        averageRaceTime: toNullableNumber(row?.averageRaceTime),
        fastestDetail,
        fastestKohan3f: toNullableNumber(row?.fastestKohan3f),
        fastestRaceTime: toNullableNumber(row?.fastestRaceTime),
        correlationRows: toConditionCorrelationRows(row?.correlationRows),
        medianKohan3f: toNullableNumber(row?.medianKohan3f),
        medianRaceTime: toNullableNumber(row?.medianRaceTime),
        raceCount: toCount(row?.raceCount),
        targetRaces: toRaceTimeTargetRaces(row?.targetRaces),
      };
    });
  },
);

export const getPayoutStats = cache(
  async (race: RaceDetail, settings: SimilarRaceStatsSettings): Promise<PayoutStatsRow[]> => {
    return withDbQueryCache(["getPayoutStats", race, settings], async () => {
      const statsSource = getSingleStatsSource(race, settings);
      const raceTable = statsSource === "jra" ? jvdRa : nvdRa;
      const runnerTable = statsSource === "jra" ? jvdSe : nvdSe;
      const payoutTable = sql.raw(statsSource === "jra" ? "jvd_hr" : "nvd_hr");
      const odds1Table = sql.raw(statsSource === "jra" ? "jvd_o1" : "nvd_o1");
      const odds2Table = sql.raw(statsSource === "jra" ? "jvd_o2" : "nvd_o2");
      const odds3Table = sql.raw(statsSource === "jra" ? "jvd_o3" : "nvd_o3");
      const odds4Table = sql.raw(statsSource === "jra" ? "jvd_o4" : "nvd_o4");
      const odds5Table = sql.raw(statsSource === "jra" ? "jvd_o5" : "nvd_o5");
      const odds6Table = sql.raw(statsSource === "jra" ? "jvd_o6" : "nvd_o6");
      const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
      const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
      const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
      const classCondition = getStatsClassCondition(race, settings.classConditionName);
      const raceTitleCondition = getStatsRaceTitleCondition(race);
      const raceSubtitleCondition = getStatsRaceSubtitleCondition(race);
      const result = await getDb().execute<{
        betType: string;
        count: string;
        minPayout: string | null;
        maxPayout: string | null;
        averagePayout: string | null;
        medianPayout: string | null;
        details: unknown;
      }>(sql`
      with strict_matched_races as (
        select
          ra.kaisai_nen,
          ra.kaisai_tsukihi,
          ra.keibajo_code,
          ra.race_bango,
          coalesce(
            nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            '一般競走'
          ) as race_name
        from ${raceTable} ra
        where
          ra.kaisai_nen || ra.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or ra.kaisai_nen || ra.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
          and (${settings.includeVenue} = false or ra.keibajo_code = ${race.keibajoCode})
          and ${monthWindowCondition(raceDate, settings.includeMonthWindow)}
          and ${runnerCountCondition(runnerTable, settings)}
          and (${settings.includeRaceTitle} = false or ${raceTitleCondition})
          and (${settings.includeRaceSubtitle} = false or ${raceSubtitleCondition})
          and (${settings.includeAge} = false or ra.kyoso_shubetsu_code = ${race.kyosoShubetsuCode})
          and (${settings.includeClass} = false or ${classCondition})
          and (${settings.includeSex} = false or ra.kyoso_kigo_code = ${race.kyosoKigoCode})
          and (${settings.includeWeight} = false or ra.juryo_shubetsu_code = ${race.juryoShubetsuCode})
          and (${settings.includeSurface} = false or ${trackCodeIn(surfaceCodes)})
          and (${settings.includeTurn} = false or ${trackCodeIn(turnCodes)})
          and (${settings.includeDistance} = false or ra.kyori = ${race.kyori})
          and (${settings.includeRaceNumber} = false or ra.race_bango = ${race.raceBango})
      ),
      strict_payout_values as (
        select
          payouts.bet_type,
          payouts.bet_order,
          strict_matched_races.kaisai_nen,
          strict_matched_races.kaisai_tsukihi,
          strict_matched_races.keibajo_code,
          strict_matched_races.race_bango,
          strict_matched_races.race_name,
          nullif(regexp_replace(coalesce(payouts.payout_text, ''), '[^0-9]', '', 'g'), '')::numeric as payout
        from strict_matched_races
        join ${payoutTable} hr
          on hr.kaisai_nen = strict_matched_races.kaisai_nen
          and hr.kaisai_tsukihi = strict_matched_races.kaisai_tsukihi
          and hr.keibajo_code = strict_matched_races.keibajo_code
          and hr.race_bango = strict_matched_races.race_bango
        cross join lateral (
          values
            ('単勝', 1, hr.haraimodoshi_tansho_1b),
            ('単勝', 1, hr.haraimodoshi_tansho_2b),
            ('単勝', 1, hr.haraimodoshi_tansho_3b),
            ('複勝', 2, hr.haraimodoshi_fukusho_1b),
            ('複勝', 2, hr.haraimodoshi_fukusho_2b),
            ('複勝', 2, hr.haraimodoshi_fukusho_3b),
            ('複勝', 2, hr.haraimodoshi_fukusho_4b),
            ('複勝', 2, hr.haraimodoshi_fukusho_5b),
            ('枠連', 3, hr.haraimodoshi_wakuren_1b),
            ('枠連', 3, hr.haraimodoshi_wakuren_2b),
            ('枠連', 3, hr.haraimodoshi_wakuren_3b),
            ('馬連', 4, hr.haraimodoshi_umaren_1b),
            ('馬連', 4, hr.haraimodoshi_umaren_2b),
            ('馬連', 4, hr.haraimodoshi_umaren_3b),
            ('ワイド', 5, hr.haraimodoshi_wide_1b),
            ('ワイド', 5, hr.haraimodoshi_wide_2b),
            ('ワイド', 5, hr.haraimodoshi_wide_3b),
            ('ワイド', 5, hr.haraimodoshi_wide_4b),
            ('ワイド', 5, hr.haraimodoshi_wide_5b),
            ('ワイド', 5, hr.haraimodoshi_wide_6b),
            ('ワイド', 5, hr.haraimodoshi_wide_7b),
            ('馬単', 6, hr.haraimodoshi_umatan_1b),
            ('馬単', 6, hr.haraimodoshi_umatan_2b),
            ('馬単', 6, hr.haraimodoshi_umatan_3b),
            ('馬単', 6, hr.haraimodoshi_umatan_4b),
            ('馬単', 6, hr.haraimodoshi_umatan_5b),
            ('馬単', 6, hr.haraimodoshi_umatan_6b),
            ('3連複', 7, hr.haraimodoshi_sanrenpuku_1b),
            ('3連複', 7, hr.haraimodoshi_sanrenpuku_2b),
            ('3連複', 7, hr.haraimodoshi_sanrenpuku_3b),
            ('3連単', 8, hr.haraimodoshi_sanrentan_1b),
            ('3連単', 8, hr.haraimodoshi_sanrentan_2b),
            ('3連単', 8, hr.haraimodoshi_sanrentan_3b),
            ('3連単', 8, hr.haraimodoshi_sanrentan_4b),
            ('3連単', 8, hr.haraimodoshi_sanrentan_5b),
            ('3連単', 8, hr.haraimodoshi_sanrentan_6b)
        ) as payouts(bet_type, bet_order, payout_text)
        where
          nullif(regexp_replace(coalesce(payouts.payout_text, ''), '[^0-9]', '', 'g'), '') !~ '^0+$'
      ),
      finish_rows as (
        select
          strict_matched_races.*,
          se.wakuban,
          se.umaban,
          nullif(regexp_replace(coalesce(se.kakutei_chakujun, ''), '[^0-9]', '', 'g'), '')::int finish_rank
        from strict_matched_races
        join ${runnerTable} se
          on se.kaisai_nen = strict_matched_races.kaisai_nen
          and se.kaisai_tsukihi = strict_matched_races.kaisai_tsukihi
          and se.keibajo_code = strict_matched_races.keibajo_code
          and se.race_bango = strict_matched_races.race_bango
        where nullif(regexp_replace(coalesce(se.kakutei_chakujun, ''), '[^0-9]', '', 'g'), '')::int between 1 and 3
      ),
      finish_orders as (
        select
          kaisai_nen,
          kaisai_tsukihi,
          keibajo_code,
          race_bango,
          race_name,
          max(nullif(regexp_replace(umaban, '[^0-9]', '', 'g'), '')::int) filter (where finish_rank = 1) winner_umaban,
          max(nullif(regexp_replace(umaban, '[^0-9]', '', 'g'), '')::int) filter (where finish_rank = 2) second_umaban,
          max(nullif(regexp_replace(umaban, '[^0-9]', '', 'g'), '')::int) filter (where finish_rank = 3) third_umaban,
          max(nullif(regexp_replace(wakuban, '[^0-9]', '', 'g'), '')::int) filter (where finish_rank = 1) winner_wakuban,
          max(nullif(regexp_replace(wakuban, '[^0-9]', '', 'g'), '')::int) filter (where finish_rank = 2) second_wakuban
        from finish_rows
        group by kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, race_name
      ),
      fallback_odds_values as (
        select
          fallback_rows.bet_type,
          fallback_rows.bet_order,
          fallback_rows.kaisai_nen,
          fallback_rows.kaisai_tsukihi,
          fallback_rows.keibajo_code,
          fallback_rows.race_bango,
          fallback_rows.race_name,
          fallback_rows.payout
        from finish_orders
        left join ${odds1Table} o1
          on o1.kaisai_nen = finish_orders.kaisai_nen
          and o1.kaisai_tsukihi = finish_orders.kaisai_tsukihi
          and o1.keibajo_code = finish_orders.keibajo_code
          and o1.race_bango = finish_orders.race_bango
        left join ${odds2Table} o2
          on o2.kaisai_nen = finish_orders.kaisai_nen
          and o2.kaisai_tsukihi = finish_orders.kaisai_tsukihi
          and o2.keibajo_code = finish_orders.keibajo_code
          and o2.race_bango = finish_orders.race_bango
        left join ${odds3Table} o3
          on o3.kaisai_nen = finish_orders.kaisai_nen
          and o3.kaisai_tsukihi = finish_orders.kaisai_tsukihi
          and o3.keibajo_code = finish_orders.keibajo_code
          and o3.race_bango = finish_orders.race_bango
        left join ${odds4Table} o4
          on o4.kaisai_nen = finish_orders.kaisai_nen
          and o4.kaisai_tsukihi = finish_orders.kaisai_tsukihi
          and o4.keibajo_code = finish_orders.keibajo_code
          and o4.race_bango = finish_orders.race_bango
        left join ${odds5Table} o5
          on o5.kaisai_nen = finish_orders.kaisai_nen
          and o5.kaisai_tsukihi = finish_orders.kaisai_tsukihi
          and o5.keibajo_code = finish_orders.keibajo_code
          and o5.race_bango = finish_orders.race_bango
        left join ${odds6Table} o6
          on o6.kaisai_nen = finish_orders.kaisai_nen
          and o6.kaisai_tsukihi = finish_orders.kaisai_tsukihi
          and o6.keibajo_code = finish_orders.keibajo_code
          and o6.race_bango = finish_orders.race_bango
        cross join lateral (
          select '単勝' bet_type, 1 bet_order, finish_orders.kaisai_nen, finish_orders.kaisai_tsukihi, finish_orders.keibajo_code, finish_orders.race_bango, finish_orders.race_name,
            nullif(regexp_replace(substr(coalesce(o1.odds_tansho, ''), ((finish_orders.winner_umaban - 1) * 8 + 3)::int, 4), '[^0-9]', '', 'g'), '')::numeric * 10 payout
          where finish_orders.winner_umaban is not null
          union all
          select '複勝', 2, finish_orders.kaisai_nen, finish_orders.kaisai_tsukihi, finish_orders.keibajo_code, finish_orders.race_bango, finish_orders.race_name,
            round((nullif(regexp_replace(substr(coalesce(o1.odds_fukusho, ''), ((hit_umaban - 1) * 12 + 3)::int, 4), '[^0-9]', '', 'g'), '')::numeric + nullif(regexp_replace(substr(coalesce(o1.odds_fukusho, ''), ((hit_umaban - 1) * 12 + 7)::int, 4), '[^0-9]', '', 'g'), '')::numeric) / 2) * 10
          from (
            values (finish_orders.winner_umaban), (finish_orders.second_umaban), (finish_orders.third_umaban)
          ) hit_rows(hit_umaban)
          where hit_umaban is not null
          union all
          select '枠連', 3, finish_orders.kaisai_nen, finish_orders.kaisai_tsukihi, finish_orders.keibajo_code, finish_orders.race_bango, finish_orders.race_name,
            nullif(regexp_replace(substr(coalesce(o1.odds_wakuren, ''), (((((least(finish_orders.winner_wakuban, finish_orders.second_wakuban) - 1) * 8 - ((least(finish_orders.winner_wakuban, finish_orders.second_wakuban) - 1) * least(finish_orders.winner_wakuban, finish_orders.second_wakuban)) / 2 + greatest(finish_orders.winner_wakuban, finish_orders.second_wakuban) - least(finish_orders.winner_wakuban, finish_orders.second_wakuban)) - 1) * 9 + 3))::int, 5), '[^0-9]', '', 'g'), '')::numeric * 10
          where finish_orders.winner_wakuban is not null and finish_orders.second_wakuban is not null and finish_orders.winner_wakuban <> finish_orders.second_wakuban
          union all
          select '馬連', 4, finish_orders.kaisai_nen, finish_orders.kaisai_tsukihi, finish_orders.keibajo_code, finish_orders.race_bango, finish_orders.race_name,
            nullif(regexp_replace(substr(coalesce(o2.odds_umaren, ''), (((((least(finish_orders.winner_umaban, finish_orders.second_umaban) - 1) * 18 - ((least(finish_orders.winner_umaban, finish_orders.second_umaban) - 1) * least(finish_orders.winner_umaban, finish_orders.second_umaban)) / 2 + greatest(finish_orders.winner_umaban, finish_orders.second_umaban) - least(finish_orders.winner_umaban, finish_orders.second_umaban)) - 1) * 13 + 5))::int, 6), '[^0-9]', '', 'g'), '')::numeric * 10
          where finish_orders.winner_umaban is not null and finish_orders.second_umaban is not null and finish_orders.winner_umaban <> finish_orders.second_umaban
          union all
          select 'ワイド', 5, finish_orders.kaisai_nen, finish_orders.kaisai_tsukihi, finish_orders.keibajo_code, finish_orders.race_bango, finish_orders.race_name,
            round((
              nullif(regexp_replace(substr(coalesce(o3.odds_wide, ''), (((((least(left_umaban, right_umaban) - 1) * 18 - ((least(left_umaban, right_umaban) - 1) * least(left_umaban, right_umaban)) / 2 + greatest(left_umaban, right_umaban) - least(left_umaban, right_umaban)) - 1) * 17 + 5))::int, 5), '[^0-9]', '', 'g'), '')::numeric +
              nullif(regexp_replace(substr(coalesce(o3.odds_wide, ''), (((((least(left_umaban, right_umaban) - 1) * 18 - ((least(left_umaban, right_umaban) - 1) * least(left_umaban, right_umaban)) / 2 + greatest(left_umaban, right_umaban) - least(left_umaban, right_umaban)) - 1) * 17 + 10))::int, 5), '[^0-9]', '', 'g'), '')::numeric
            ) / 2) * 10
          from (
            values
              (least(finish_orders.winner_umaban, finish_orders.second_umaban), greatest(finish_orders.winner_umaban, finish_orders.second_umaban)),
              (least(finish_orders.winner_umaban, finish_orders.third_umaban), greatest(finish_orders.winner_umaban, finish_orders.third_umaban)),
              (least(finish_orders.second_umaban, finish_orders.third_umaban), greatest(finish_orders.second_umaban, finish_orders.third_umaban))
          ) wide_hit_rows(left_umaban, right_umaban)
          where left_umaban is not null and right_umaban is not null and left_umaban <> right_umaban
          union all
          select '馬単', 6, finish_orders.kaisai_nen, finish_orders.kaisai_tsukihi, finish_orders.keibajo_code, finish_orders.race_bango, finish_orders.race_name,
            nullif(regexp_replace(substr(coalesce(o4.odds_umatan, ''), ((((finish_orders.winner_umaban - 1) * 17 + finish_orders.second_umaban - case when finish_orders.second_umaban > finish_orders.winner_umaban then 1 else 0 end) - 1) * 13 + 5)::int, 6), '[^0-9]', '', 'g'), '')::numeric * 10
          where finish_orders.winner_umaban is not null and finish_orders.second_umaban is not null and finish_orders.winner_umaban <> finish_orders.second_umaban
          union all
          select '3連複', 7, finish_orders.kaisai_nen, finish_orders.kaisai_tsukihi, finish_orders.keibajo_code, finish_orders.race_bango, finish_orders.race_name,
            nullif(regexp_replace(substr(coalesce(o5.odds_sanrenpuku, ''), ((trio_index - 1) * 15 + 7)::int, 6), '[^0-9]', '', 'g'), '')::numeric * 10
          from lateral (
            select
              least(finish_orders.winner_umaban, finish_orders.second_umaban, finish_orders.third_umaban) first_umaban,
              finish_orders.winner_umaban + finish_orders.second_umaban + finish_orders.third_umaban - least(finish_orders.winner_umaban, finish_orders.second_umaban, finish_orders.third_umaban) - greatest(finish_orders.winner_umaban, finish_orders.second_umaban, finish_orders.third_umaban) second_umaban,
              greatest(finish_orders.winner_umaban, finish_orders.second_umaban, finish_orders.third_umaban) third_umaban
          ) trio_numbers
          cross join lateral (
            select
              (
                coalesce((select sum((18 - first_index) * (17 - first_index) / 2) from generate_series(1, trio_numbers.first_umaban - 1) first_indexes(first_index)), 0) +
                coalesce((select sum(18 - second_index) from generate_series(trio_numbers.first_umaban + 1, trio_numbers.second_umaban - 1) second_indexes(second_index)), 0) +
                trio_numbers.third_umaban - trio_numbers.second_umaban +
                1
              ) trio_index
          ) trio_indexes
          where finish_orders.winner_umaban is not null and finish_orders.second_umaban is not null and finish_orders.third_umaban is not null
          union all
          select '3連単', 8, finish_orders.kaisai_nen, finish_orders.kaisai_tsukihi, finish_orders.keibajo_code, finish_orders.race_bango, finish_orders.race_name,
            nullif(regexp_replace(substr(coalesce(o6.odds_sanrentan, ''), ((((finish_orders.winner_umaban - 1) * 272 + (finish_orders.second_umaban - 1 - case when finish_orders.winner_umaban < finish_orders.second_umaban then 1 else 0 end) * 16 + finish_orders.third_umaban - 1 - case when finish_orders.winner_umaban < finish_orders.third_umaban then 1 else 0 end - case when finish_orders.second_umaban < finish_orders.third_umaban then 1 else 0 end + 1) - 1) * 17 + 7)::int, 7), '[^0-9]', '', 'g'), '')::numeric * 10
          where finish_orders.winner_umaban is not null and finish_orders.second_umaban is not null and finish_orders.third_umaban is not null
        ) fallback_rows
        where fallback_rows.payout is not null and fallback_rows.payout > 0
      ),
      payout_values as (
        select * from strict_payout_values
        union all
        select * from fallback_odds_values
        where not exists (select 1 from strict_payout_values)
      ),
      ranked_payout_values as (
        select
          *,
          row_number() over (
            partition by bet_type
            order by kaisai_nen desc, kaisai_tsukihi desc, race_bango asc, payout desc
          ) as "detailRank"
        from payout_values
      )
      select
        bet_type as "betType",
        count(*)::text as "count",
        min(payout)::text as "minPayout",
        max(payout)::text as "maxPayout",
        round(avg(payout), 1)::text as "averagePayout",
        percentile_cont(0.5) within group (order by payout)::text as "medianPayout",
        jsonb_agg(
          jsonb_build_object(
            'date', kaisai_nen || kaisai_tsukihi,
            'keibajoCode', keibajo_code,
            'raceNumber', race_bango,
            'raceName', race_name,
            'payout', payout
          )
          order by
            kaisai_nen desc,
            kaisai_tsukihi desc,
            race_bango asc,
            payout desc
        ) filter (where "detailRank" <= 100) as details
      from ranked_payout_values
      group by bet_type, bet_order
      order by bet_order asc
    `);

      return result.rows.map((row) => ({
        averagePayout: toNullableNumber(row.averagePayout),
        betType: row.betType,
        count: toCount(row.count),
        details: toPayoutStatsDetails(row.details),
        maxPayout: toNullableNumber(row.maxPayout),
        medianPayout: toNullableNumber(row.medianPayout),
        minPayout: toNullableNumber(row.minPayout),
      }));
    });
  },
);

export const getFinishPositionStats = cache(
  async (
    race: RaceDetail,
    settings: SimilarRaceStatsSettings,
  ): Promise<FinishPositionStatsRow[]> => {
    return withDbQueryCache(["getFinishPositionStats", race, settings], async () => {
      const statsSource = getSingleStatsSource(race, settings);
      const raceTable = statsSource === "jra" ? jvdRa : nvdRa;
      const runnerTable = statsSource === "jra" ? jvdSe : nvdSe;
      const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
      const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
      const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
      const classCondition = getStatsClassCondition(race, settings.classConditionName);
      const raceTitleCondition = getStatsRaceTitleCondition(race);
      const raceSubtitleCondition = getStatsRaceSubtitleCondition(race);
      const result = await getDb().execute<{
        finishPosition: string;
        count: string;
        averagePopularity: string | null;
        medianPopularity: string | null;
        averageOdds: string | null;
        medianOdds: string | null;
        details: unknown;
      }>(sql`
      with matched_races as (
        select
          ra.kaisai_nen,
          ra.kaisai_tsukihi,
          ra.keibajo_code,
          ra.race_bango,
          coalesce(
            nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            '一般競走'
          ) as race_name
        from ${raceTable} ra
        where
          ra.kaisai_nen || ra.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or ra.kaisai_nen || ra.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
          and (${settings.includeVenue} = false or ra.keibajo_code = ${race.keibajoCode})
          and ${monthWindowCondition(raceDate, settings.includeMonthWindow)}
          and ${runnerCountCondition(runnerTable, settings)}
          and (${settings.includeRaceTitle} = false or ${raceTitleCondition})
          and (${settings.includeRaceSubtitle} = false or ${raceSubtitleCondition})
          and (${settings.includeAge} = false or ra.kyoso_shubetsu_code = ${race.kyosoShubetsuCode})
          and (${settings.includeClass} = false or ${classCondition})
          and (${settings.includeSex} = false or ra.kyoso_kigo_code = ${race.kyosoKigoCode})
          and (${settings.includeWeight} = false or ra.juryo_shubetsu_code = ${race.juryoShubetsuCode})
          and (${settings.includeSurface} = false or ${trackCodeIn(surfaceCodes)})
          and (${settings.includeTurn} = false or ${trackCodeIn(turnCodes)})
          and (${settings.includeDistance} = false or ra.kyori = ${race.kyori})
          and (${settings.includeRaceNumber} = false or ra.race_bango = ${race.raceBango})
      ),
      finish_rows as (
        select
          matched_races.kaisai_nen,
          matched_races.kaisai_tsukihi,
          matched_races.keibajo_code,
          matched_races.race_bango,
          matched_races.race_name,
          se.wakuban,
          se.umaban,
          coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as bamei,
          coalesce(nullif(regexp_replace(se.kishumei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as jockey_name,
          se.kakutei_chakujun,
          se.soha_time,
          se.kakutei_chakujun::int as finish_position,
          se.tansho_ninkijun,
          se.tansho_odds,
          nullif(regexp_replace(coalesce(se.tansho_ninkijun, ''), '[^0-9]', '', 'g'), '')::numeric as popularity,
          nullif(regexp_replace(coalesce(se.tansho_odds, ''), '[^0-9]', '', 'g'), '')::numeric / 10.0 as odds
        from matched_races
        join ${runnerTable} se
          on se.kaisai_nen = matched_races.kaisai_nen
          and se.kaisai_tsukihi = matched_races.kaisai_tsukihi
          and se.keibajo_code = matched_races.keibajo_code
          and se.race_bango = matched_races.race_bango
        where
          se.kakutei_chakujun in ('01', '02', '03', '04', '05')
      ),
      ranked_finish_rows as (
        select
          *,
          row_number() over (
            partition by finish_position
            order by kaisai_nen desc, kaisai_tsukihi desc, race_bango asc, umaban asc
          ) as "detailRank"
        from finish_rows
      )
      select
        finish_position::text as "finishPosition",
        count(*)::text as "count",
        round(avg(nullif(popularity, 0)), 1)::text as "averagePopularity",
        round((percentile_cont(0.5) within group (order by nullif(popularity, 0)))::numeric, 1)::text
          as "medianPopularity",
        round(avg(nullif(odds, 0)), 1)::text as "averageOdds",
        round((percentile_cont(0.5) within group (order by nullif(odds, 0)))::numeric, 1)::text
          as "medianOdds",
        jsonb_agg(
          jsonb_build_object(
            'date', kaisai_nen || kaisai_tsukihi,
            'keibajoCode', keibajo_code,
            'raceNumber', race_bango,
            'raceName', race_name,
            'horseName', bamei,
            'frameNumber', wakuban,
            'horseNumber', umaban,
            'jockeyName', jockey_name,
            'rank', kakutei_chakujun,
            'raceTime', soha_time,
            'popularity', tansho_ninkijun,
            'winOdds', tansho_odds
          )
          order by
            kaisai_nen desc,
            kaisai_tsukihi desc,
            race_bango asc,
            umaban asc
        ) filter (where "detailRank" <= 100) as details
      from ranked_finish_rows
      group by finish_position
      order by finish_position asc
    `);

      return result.rows.map((row) => ({
        averageOdds: toNullableNumber(row.averageOdds),
        averagePopularity: toNullableNumber(row.averagePopularity),
        count: toCount(row.count),
        details: toStatsDetails(row.details),
        finishPosition: toCount(row.finishPosition),
        medianOdds: toNullableNumber(row.medianOdds),
        medianPopularity: toNullableNumber(row.medianPopularity),
      }));
    });
  },
);

export const getFrameStats = cache(
  async (race: RaceDetail, settings: SimilarRaceStatsSettings): Promise<FrameStatsRow[]> => {
    return withDbQueryCache(["getFrameStats", race, settings], async () => {
      const statsSource = getSingleStatsSource(race, settings);
      const raceTable = statsSource === "jra" ? jvdRa : nvdRa;
      const runnerTable = statsSource === "jra" ? jvdSe : nvdSe;
      const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
      const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
      const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
      const classCondition = getStatsClassCondition(race, settings.classConditionName);
      const raceTitleCondition = getStatsRaceTitleCondition(race);
      const raceSubtitleCondition = getStatsRaceSubtitleCondition(race);
      const result = await getDb().execute<{
        frameNumber: string;
        runnerCount: string | null;
        count: string;
        score: string | null;
        averageFinish: string | null;
        medianFinish: string | null;
        averagePopularity: string | null;
        medianPopularity: string | null;
        details: unknown;
      }>(sql`
      with current_frames as (
        select distinct wakuban
        from ${runnerTable}
        where
          kaisai_nen = ${race.kaisaiNen}
          and kaisai_tsukihi = ${race.kaisaiTsukihi}
          and keibajo_code = ${race.keibajoCode}
          and race_bango = ${race.raceBango}
          and nullif(wakuban, '') is not null
      ),
      matched_races as (
        select
          ra.kaisai_nen,
          ra.kaisai_tsukihi,
          ra.keibajo_code,
          ra.race_bango,
          coalesce(
            nullif(regexp_replace(ra.kyosomei_hondai, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            '一般競走'
          ) as race_name,
          (
            select count(*)
            from ${runnerTable} runner_count_se
            where
              runner_count_se.kaisai_nen = ra.kaisai_nen
              and runner_count_se.kaisai_tsukihi = ra.kaisai_tsukihi
              and runner_count_se.keibajo_code = ra.keibajo_code
              and runner_count_se.race_bango = ra.race_bango
          )::int as runner_count
        from ${raceTable} ra
        where
          ra.kaisai_nen || ra.kaisai_tsukihi < ${raceDate}
          and (
            ${settings.years}::int is null
            or ra.kaisai_nen || ra.kaisai_tsukihi >= to_char(
              to_date(${raceDate}, 'YYYYMMDD') - (${settings.years}::int * interval '1 year'),
              'YYYYMMDD'
            )
          )
          and (${settings.includeVenue} = false or ra.keibajo_code = ${race.keibajoCode})
          and ${monthWindowCondition(raceDate, settings.includeMonthWindow)}
          and ${runnerCountCondition(runnerTable, settings)}
          and (${settings.includeRaceTitle} = false or ${raceTitleCondition})
          and (${settings.includeRaceSubtitle} = false or ${raceSubtitleCondition})
          and (${settings.includeAge} = false or ra.kyoso_shubetsu_code = ${race.kyosoShubetsuCode})
          and (${settings.includeClass} = false or ${classCondition})
          and (${settings.includeSex} = false or ra.kyoso_kigo_code = ${race.kyosoKigoCode})
          and (${settings.includeWeight} = false or ra.juryo_shubetsu_code = ${race.juryoShubetsuCode})
          and (${settings.includeSurface} = false or ${trackCodeIn(surfaceCodes)})
          and (${settings.includeTurn} = false or ${trackCodeIn(turnCodes)})
          and (${settings.includeDistance} = false or ra.kyori = ${race.kyori})
          and (${settings.includeRaceNumber} = false or ra.race_bango = ${race.raceBango})
      ),
      frame_rows as (
        select
          matched_races.kaisai_nen,
          matched_races.kaisai_tsukihi,
          matched_races.keibajo_code,
          matched_races.race_bango,
          matched_races.race_name,
          matched_races.runner_count,
          se.wakuban,
          se.umaban,
          coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as bamei,
          coalesce(nullif(regexp_replace(se.kishumei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as jockey_name,
          se.kakutei_chakujun,
          se.soha_time,
          se.tansho_ninkijun,
          se.tansho_odds,
          nullif(regexp_replace(coalesce(se.kakutei_chakujun, ''), '[^0-9]', '', 'g'), '')::numeric as finish_position,
          nullif(regexp_replace(coalesce(se.tansho_ninkijun, ''), '[^0-9]', '', 'g'), '')::numeric as popularity
        from matched_races
        join ${runnerTable} se
          on se.kaisai_nen = matched_races.kaisai_nen
          and se.kaisai_tsukihi = matched_races.kaisai_tsukihi
          and se.keibajo_code = matched_races.keibajo_code
          and se.race_bango = matched_races.race_bango
        where
          nullif(se.wakuban, '') is not null
          and nullif(regexp_replace(coalesce(se.kakutei_chakujun, ''), '[^0-9]', '', 'g'), '') !~ '^0+$'
          and (
            ${settings.includeFrame} = false
            or exists (
              select 1
              from current_frames
              where current_frames.wakuban = se.wakuban
            )
          )
      ),
      ranked_frame_rows as (
        select
          *,
          row_number() over (
            partition by wakuban
            order by kaisai_nen desc, kaisai_tsukihi desc, race_bango asc, umaban asc
          ) as "detailRank"
        from frame_rows
      ),
      stats as (
        select
          wakuban as "frameNumber",
          case when ${settings.includeRunnerCount} then max(runner_count)::text else null end as "runnerCount",
          count(*)::text as "count",
          round(avg(finish_position), 1)::text as "averageFinish",
          round((percentile_cont(0.5) within group (order by finish_position))::numeric, 1)::text
            as "medianFinish",
          round(avg(nullif(popularity, 0)), 1)::text as "averagePopularity",
          round((percentile_cont(0.5) within group (order by nullif(popularity, 0)))::numeric, 1)::text
            as "medianPopularity",
          (
            1.0 / nullif(avg(finish_position), 0)
            + 1.0 / nullif((percentile_cont(0.5) within group (order by finish_position))::numeric, 0)
          ) as raw_score,
          jsonb_agg(
            jsonb_build_object(
              'date', kaisai_nen || kaisai_tsukihi,
              'keibajoCode', keibajo_code,
              'raceNumber', race_bango,
              'raceName', race_name,
              'horseName', bamei,
              'frameNumber', wakuban,
              'horseNumber', umaban,
              'jockeyName', jockey_name,
              'rank', kakutei_chakujun,
              'raceTime', soha_time,
              'popularity', tansho_ninkijun,
              'winOdds', tansho_odds
            )
            order by
              kaisai_nen desc,
              kaisai_tsukihi desc,
              race_bango asc,
              umaban asc
          ) filter (where "detailRank" <= 100) as details
        from ranked_frame_rows
        group by wakuban
      ),
      scored as (
        select
          *,
          case
            when max(raw_score) over () > min(raw_score) over ()
              then (raw_score - min(raw_score) over ()) / nullif(max(raw_score) over () - min(raw_score) over (), 0)
            when raw_score > 0 then 1
            else 0
          end as normalized_score
        from stats
      )
      select
        "frameNumber",
        "runnerCount",
        "count",
        round(normalized_score, 2)::text as "score",
        "averageFinish",
        "medianFinish",
        "averagePopularity",
        "medianPopularity",
        details
      from scored
      order by "frameNumber" asc
    `);

      return result.rows.map((row) => ({
        averageFinish: toNullableNumber(row.averageFinish),
        averagePopularity: toNullableNumber(row.averagePopularity),
        count: toCount(row.count),
        details: toStatsDetails(row.details),
        frameNumber: row.frameNumber,
        medianFinish: toNullableNumber(row.medianFinish),
        medianPopularity: toNullableNumber(row.medianPopularity),
        runnerCount: toNullableNumber(row.runnerCount),
        score: toNullableNumber(row.score) ?? 0,
      }));
    });
  },
);
