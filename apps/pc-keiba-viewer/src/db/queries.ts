import "server-only";
import { and, asc, desc, eq, sql } from "drizzle-orm";
import { cache } from "react";

import { TRACK_LABELS, type RaceSource } from "../lib/codes";
import type {
  AbilityTest,
  BloodlineStatsRow,
  CourseInfo,
  FinishPositionStatsRow,
  FrameStatsRow,
  HorseRaceResult,
  PayoutStatsDetail,
  PayoutStatsRow,
  RaceDaySummary,
  RaceDetail,
  RaceListItem,
  RaceTimeStats,
  RaceYearSummary,
  Runner,
  SimilarRaceStatsRow,
  SimilarRaceStatsSettings,
  StatsDetail,
  Training,
} from "../lib/race-types";
import { getDb } from "./client";
import { withDbQueryCache } from "./query-cache";
import { jvdCs, jvdRa, jvdSe, jvdUm, nvdRa, nvdSe, nvdUm } from "./schema";

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
        kaisai_nen as "kaisaiNen",
        kaisai_tsukihi as "kaisaiTsukihi",
        keibajo_code as "keibajoCode",
        race_bango as "raceBango",
        kyosomei_hondai as "kyosomeiHondai",
        kyosomei_fukudai as "kyosomeiFukudai",
        grade_code as "gradeCode",
        kyoso_shubetsu_code as "kyosoShubetsuCode",
        kyoso_kigo_code as "kyosoKigoCode",
        juryo_shubetsu_code as "juryoShubetsuCode",
        kyoso_joken_code as "kyosoJokenCode",
        kyoso_joken_meisho as "kyosoJokenMeisho",
        kyori,
        track_code as "trackCode",
        hasso_jikoku as "hassoJikoku",
        shusso_tosu as "shussoTosu"
      from ${jvdRa}
      where kaisai_nen = ${year} and kaisai_tsukihi = ${monthDay}
      union all
      select
        'nar' as source,
        kaisai_nen as "kaisaiNen",
        kaisai_tsukihi as "kaisaiTsukihi",
        keibajo_code as "keibajoCode",
        race_bango as "raceBango",
        kyosomei_hondai as "kyosomeiHondai",
        kyosomei_fukudai as "kyosomeiFukudai",
        grade_code as "gradeCode",
        kyoso_shubetsu_code as "kyosoShubetsuCode",
        kyoso_kigo_code as "kyosoKigoCode",
        juryo_shubetsu_code as "juryoShubetsuCode",
        kyoso_joken_code as "kyosoJokenCode",
        kyoso_joken_meisho as "kyosoJokenMeisho",
        kyori,
        track_code as "trackCode",
        hasso_jikoku as "hassoJikoku",
        shusso_tosu as "shussoTosu"
      from ${nvdRa}
      where kaisai_nen = ${year} and kaisai_tsukihi = ${monthDay}
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
        const raceDateKey = `${year}${monthDay}${raceNumber}`;

        if (source === "nar") {
          const result = await getDb().execute<Runner & Record<string, unknown>>(sql`
            with current_runners as (
              select
                se.*,
                row_number() over (
                  partition by se.ketto_toroku_bango
                  order by hist.kaisai_nen desc, hist.kaisai_tsukihi desc, hist.race_bango desc
                ) as latest_weight_rank,
                hist.bataiju as latest_bataiju,
                hist.zogen_fugo as latest_zogen_fugo,
                hist.zogen_sa as latest_zogen_sa
              from ${nvdSe} se
              left join ${nvdSe} hist
                on hist.ketto_toroku_bango = se.ketto_toroku_bango
                and hist.ketto_toroku_bango is not null
                and btrim(hist.ketto_toroku_bango) <> ''
                and hist.kaisai_nen || hist.kaisai_tsukihi || hist.race_bango < ${raceDateKey}
                and nullif(btrim(hist.bataiju), '') is not null
                and upper(btrim(hist.bataiju)) <> 'FFF'
              where
                se.kaisai_nen = ${year}
                and se.kaisai_tsukihi = ${monthDay}
                and se.keibajo_code = ${keibajoCode}
                and se.race_bango = ${raceNumber}
            )
            select
              wakuban,
              umaban,
              ketto_toroku_bango as "kettoTorokuBango",
              bamei,
              seibetsu_code as "seibetsuCode",
              barei,
              futan_juryo as "futanJuryo",
              kishumei_ryakusho as "kishumeiRyakusho",
              chokyoshimei_ryakusho as "chokyoshimeiRyakusho",
              banushimei,
              coalesce(nullif(btrim(bataiju), ''), latest_bataiju) as bataiju,
              coalesce(nullif(btrim(zogen_fugo), ''), latest_zogen_fugo) as "zogenFugo",
              coalesce(nullif(btrim(zogen_sa), ''), latest_zogen_sa) as "zogenSa",
              kakutei_chakujun as "kakuteiChakujun",
              tansho_odds as "tanshoOdds",
              tansho_ninkijun as "tanshoNinkijun",
              soha_time as "sohaTime",
              time_sa as "timeSa",
              kohan_3f as "kohan3f"
            from current_runners
            where latest_weight_rank = 1
            order by cast(umaban as integer) asc
          `);
          return result.rows;
        }

        return getDb()
          .select({
            wakuban: table.wakuban,
            umaban: table.umaban,
            kettoTorokuBango: table.kettoTorokuBango,
            bamei: table.bamei,
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

export const getHorseRaceResults = cache(
  async (
    source: RaceSource,
    year: string,
    month: string,
    day: string,
    keibajoCode: string,
    raceNumber: string,
  ): Promise<HorseRaceResult[]> => {
    return withDbQueryCache(
      ["getHorseRaceResults", source, year, month, day, keibajoCode, raceNumber],
      async () => {
        const raceTable = source === "jra" ? jvdRa : nvdRa;
        const runnerTable = source === "jra" ? jvdSe : nvdSe;
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
        from ${runnerTable}
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
          ra.kaisai_nen as "kaisaiNen",
          ra.kaisai_tsukihi as "kaisaiTsukihi",
          ra.keibajo_code as "keibajoCode",
          ra.race_bango as "raceBango",
          ra.kyosomei_hondai as "kyosomeiHondai",
          ra.kyosomei_fukudai as "kyosomeiFukudai",
          ra.kyosomei_kakkonai as "kyosomeiKakkonai",
          ra.grade_code as "gradeCode",
          ra.kyoso_shubetsu_code as "kyosoShubetsuCode",
          ra.kyoso_kigo_code as "kyosoKigoCode",
          ra.juryo_shubetsu_code as "juryoShubetsuCode",
          ra.kyoso_joken_code as "kyosoJokenCode",
          ra.kyoso_joken_meisho as "kyosoJokenMeisho",
          ra.kyori,
          ra.track_code as "trackCode",
          ra.hasso_jikoku as "hassoJikoku",
          ra.tenko_code as "tenkoCode",
          ra.babajotai_code_shiba as "babajotaiCodeShiba",
          ra.babajotai_code_dirt as "babajotaiCodeDirt",
          se.wakuban,
          se.umaban,
          se.ketto_toroku_bango as "kettoTorokuBango",
          se.bamei,
          se.seibetsu_code as "seibetsuCode",
          se.barei,
          se.futan_juryo as "futanJuryo",
          se.kishumei_ryakusho as "kishumeiRyakusho",
          se.chokyoshimei_ryakusho as "chokyoshimeiRyakusho",
          se.banushimei,
          se.bataiju,
          se.zogen_fugo as "zogenFugo",
          se.zogen_sa as "zogenSa",
          se.kakutei_chakujun as "kakuteiChakujun",
          se.tansho_odds as "tanshoOdds",
          se.tansho_ninkijun as "tanshoNinkijun",
          se.soha_time as "sohaTime",
          se.time_sa as "timeSa",
          se.kohan_3f as "kohan3f",
          row_number() over (
            partition by ch."currentUmaban"
            order by ra.kaisai_nen desc, ra.kaisai_tsukihi desc, ra.race_bango desc
          ) as rn
        from current_horses ch
        join ${runnerTable} se
          on se.ketto_toroku_bango = ch.ketto_toroku_bango
        join ${raceTable} ra
          on ra.kaisai_nen = se.kaisai_nen
          and ra.kaisai_tsukihi = se.kaisai_tsukihi
          and ra.keibajo_code = se.keibajo_code
          and ra.race_bango = se.race_bango
        where ra.kaisai_nen || ra.kaisai_tsukihi < ${raceDate}
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
        "kohan3f"
      from history
      order by "currentUmaban"::int asc, "kaisaiNen" desc, "kaisaiTsukihi" desc, "raceBango" desc
    `);

        return result.rows;
      },
    );
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
const cleanDbText = (value: string | null | undefined): string =>
  (value ?? "").replace(/\s+/g, " ").replace(/　+/g, " ").trim();

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

const trackCodeIn = (codes: string[]) =>
  codes.length > 0 ? sql`ra.track_code in (${sql.join(codes, sql`, `)})` : sql`false`;

const monthWindowCondition = (raceDate: string, enabled: boolean) => sql`
  (
    ${enabled} = false
    or substring(ra.kaisai_tsukihi from 1 for 2) in (
      to_char(to_date(${raceDate}, 'YYYYMMDD') - interval '1 month', 'MM'),
      to_char(to_date(${raceDate}, 'YYYYMMDD'), 'MM'),
      to_char(to_date(${raceDate}, 'YYYYMMDD') + interval '1 month', 'MM')
    )
  )
`;

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

const getStatsClassCondition = (race: RaceDetail, classConditionName: string | null) => {
  if (race.source === "jra" && JRA_STATS_GRADE_CODES.has(cleanDbText(race.gradeCode))) {
    return sql`ra.grade_code = ${race.gradeCode}`;
  }

  const normalizedClassConditionName = classConditionName
    ? normalizeClassConditionName(classConditionName)
    : null;

  return cleanDbText(race.kyosoJokenCode) === "000" && classConditionName
    ? sql`
        translate(
          regexp_replace(ra.kyoso_joken_meisho, '[[:space:]　]+', ' ', 'g'),
          'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ０１２３４５６７８９－ー―‐',
          'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789----'
        ) like ${`%${normalizedClassConditionName}%`}
      `
    : sql`ra.kyoso_joken_code = ${race.kyosoJokenCode}`;
};

export const getBloodlineStats = cache(
  async (race: RaceDetail, settings: SimilarRaceStatsSettings): Promise<BloodlineStatsRow[]> => {
    return withDbQueryCache(["getBloodlineStats", race, settings], async () => {
      const raceTable = race.source === "jra" ? jvdRa : nvdRa;
      const runnerTable = race.source === "jra" ? jvdSe : nvdSe;
      const primaryHorseTable = race.source === "jra" ? jvdUm : nvdUm;
      const secondaryHorseTable = race.source === "jra" ? nvdUm : jvdUm;
      const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
      const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
      const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
      const classCondition = getStatsClassCondition(race, settings.classConditionName);
      const raceTitleCondition = cleanDbText(race.kyosomeiHondai)
        ? sql`ra.kyosomei_hondai = ${race.kyosomeiHondai}`
        : sql`false`;
      const raceSubtitleCondition = cleanDbText(race.kyosomeiFukudai)
        ? sql`ra.kyosomei_fukudai = ${race.kyosomeiFukudai}`
        : cleanDbText(race.kyosomeiKakkonai)
          ? sql`ra.kyosomei_kakkonai = ${race.kyosomeiKakkonai}`
          : sql`false`;
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
            '不明'
          ) as sire,
          coalesce(
            nullif(regexp_replace(primary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            nullif(regexp_replace(secondary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            '不明'
          ) as "sireSire",
          coalesce(
            nullif(regexp_replace(primary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            nullif(regexp_replace(secondary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
            '不明'
          ) as "damSire"
        from ${runnerTable} se
        left join ${primaryHorseTable} primary_um
          on primary_um.ketto_toroku_bango = se.ketto_toroku_bango
        left join ${secondaryHorseTable} secondary_um
          on secondary_um.ketto_toroku_bango = se.ketto_toroku_bango
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
      filtered_races as materialized (
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
      ),
      filtered_runners as materialized (
        select
          ra.kaisai_nen,
          ra.kaisai_tsukihi,
          ra.keibajo_code,
          ra.race_bango,
          ra.race_name,
          se.wakuban,
          se.umaban,
          coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as bamei,
          se.kakutei_chakujun,
          se.soha_time,
          se.tansho_ninkijun,
          se.tansho_odds,
          se.ketto_toroku_bango
        from filtered_races ra
        join ${runnerTable} se
          on se.kaisai_nen = ra.kaisai_nen
          and se.kaisai_tsukihi = ra.kaisai_tsukihi
          and se.keibajo_code = ra.keibajo_code
          and se.race_bango = ra.race_bango
      ),
      filtered_horse_keys as (
        select distinct ketto_toroku_bango
        from filtered_runners
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
        cross join lateral (
          select
            coalesce(
              nullif(regexp_replace(primary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(secondary_um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              '不明'
            ) as sire,
            coalesce(
              nullif(regexp_replace(primary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(secondary_um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              '不明'
            ) as "sireSire",
            coalesce(
              nullif(regexp_replace(primary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              nullif(regexp_replace(secondary_um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''),
              '不明'
            ) as "damSire"
        ) bloodline
        where exists (
          select 1
          from target_names
          where target_names.name in (bloodline.sire, bloodline."sireSire", bloodline."damSire")
          )
      ),
      matched_entries as (
        select
          filtered_runners.kaisai_nen,
          filtered_runners.kaisai_tsukihi,
          filtered_runners.keibajo_code,
          filtered_runners.wakuban,
          filtered_runners.umaban,
          filtered_runners.bamei,
          filtered_runners.race_bango,
          filtered_runners.race_name,
          filtered_runners.kakutei_chakujun,
          filtered_runners.soha_time,
          filtered_runners.tansho_ninkijun,
          filtered_runners.tansho_odds,
          filtered_runners.ketto_toroku_bango,
          filtered_horse_bloodlines.sire,
          filtered_horse_bloodlines."sireSire",
          filtered_horse_bloodlines."damSire"
        from filtered_runners
        join filtered_horse_bloodlines
          on filtered_horse_bloodlines.ketto_toroku_bango = filtered_runners.ketto_toroku_bango
      ),
      grouped_entries as (
        select
          targets.category,
          targets.name,
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
        join targets
          on targets.name in (matched_entries.sire, matched_entries."sireSire", matched_entries."damSire")
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
          category,
          name,
          "currentHorseNumbers",
          coalesce(
            jsonb_agg(
              jsonb_build_object(
                'date', kaisai_nen || kaisai_tsukihi,
                'sireName', sire,
                'sireSireName', "sireSire",
                'damSireName', "damSire",
                'keibajoCode', keibajo_code,
                'raceNumber', race_bango,
                'raceName', race_name,
                'horseName', bamei,
                'frameNumber', wakuban,
                'horseNumber', umaban,
                'jockeyName', '',
                'rank', kakutei_chakujun,
                'raceTime', soha_time,
                'popularity', tansho_ninkijun,
                'winOdds', tansho_odds
              )
              order by kaisai_nen desc, kaisai_tsukihi desc, race_bango asc, umaban asc
            ) filter (where "detailRank" <= 200),
            '[]'::jsonb
          ) as details,
          count(*)::text as "starts",
          count(distinct ketto_toroku_bango)::text as "horseCount",
          count(*) filter (where kakutei_chakujun = '01')::text as "winCount",
          count(*) filter (where kakutei_chakujun in ('01', '02'))::text as "quinellaCount",
          count(*) filter (where kakutei_chakujun in ('01', '02', '03'))::text as "showCount",
          round(
            count(*) filter (where kakutei_chakujun = '01') * 100.0 / nullif(count(*), 0),
            1
          )::text as "winRate",
          round(
            count(*) filter (where kakutei_chakujun in ('01', '02')) * 100.0 / nullif(count(*), 0),
            1
          )::text as "quinellaRate",
          round(
            count(*) filter (where kakutei_chakujun in ('01', '02', '03')) * 100.0 / nullif(count(*), 0),
            1
          )::text as "showRate"
        from ranked_details
        group by
          category,
          name,
          "currentHorseNumbers"
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
      const raceTable = race.source === "jra" ? jvdRa : nvdRa;
      const runnerTable = race.source === "jra" ? jvdSe : nvdSe;
      const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
      const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
      const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
      const classCondition = getStatsClassCondition(race, settings.classConditionName);
      const raceTitleCondition = cleanDbText(race.kyosomeiHondai)
        ? sql`ra.kyosomei_hondai = ${race.kyosomeiHondai}`
        : sql`false`;
      const raceSubtitleCondition = cleanDbText(race.kyosomeiFukudai)
        ? sql`ra.kyosomei_fukudai = ${race.kyosomeiFukudai}`
        : cleanDbText(race.kyosomeiKakkonai)
          ? sql`ra.kyosomei_kakkonai = ${race.kyosomeiKakkonai}`
          : sql`false`;
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
          ra.kaisai_nen,
          ra.kaisai_tsukihi,
          ra.keibajo_code,
          se.wakuban,
          se.umaban,
          coalesce(nullif(regexp_replace(se.bamei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '-') as bamei,
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
          coalesce(nullif(regexp_replace(se.kishumei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') as jockey,
          coalesce(nullif(regexp_replace(se.chokyoshimei_ryakusho, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') as trainer,
          coalesce(nullif(regexp_replace(se.banushimei, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') as owner
        from ${raceTable} ra
        join ${runnerTable} se
          on se.kaisai_nen = ra.kaisai_nen
          and se.kaisai_tsukihi = ra.kaisai_tsukihi
          and se.keibajo_code = ra.keibajo_code
          and se.race_bango = ra.race_bango
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

export const getRaceTimeStats = cache(
  async (race: RaceDetail, settings: SimilarRaceStatsSettings): Promise<RaceTimeStats> => {
    return withDbQueryCache(["getRaceTimeStats", race, settings], async () => {
      const raceTable = race.source === "jra" ? jvdRa : nvdRa;
      const runnerTable = race.source === "jra" ? jvdSe : nvdSe;
      const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
      const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
      const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
      const classCondition = getStatsClassCondition(race, settings.classConditionName);
      const raceTitleCondition = cleanDbText(race.kyosomeiHondai)
        ? sql`ra.kyosomei_hondai = ${race.kyosomeiHondai}`
        : sql`false`;
      const raceSubtitleCondition = cleanDbText(race.kyosomeiFukudai)
        ? sql`ra.kyosomei_fukudai = ${race.kyosomeiFukudai}`
        : cleanDbText(race.kyosomeiKakkonai)
          ? sql`ra.kyosomei_kakkonai = ${race.kyosomeiKakkonai}`
          : sql`false`;
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
        fastest.tansho_odds as "fastestWinOdds"
      from stats
      left join fastest on true
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
        medianKohan3f: toNullableNumber(row?.medianKohan3f),
        medianRaceTime: toNullableNumber(row?.medianRaceTime),
        raceCount: toCount(row?.raceCount),
      };
    });
  },
);

export const getPayoutStats = cache(
  async (race: RaceDetail, settings: SimilarRaceStatsSettings): Promise<PayoutStatsRow[]> => {
    return withDbQueryCache(["getPayoutStats", race, settings], async () => {
      const raceTable = race.source === "jra" ? jvdRa : nvdRa;
      const runnerTable = race.source === "jra" ? jvdSe : nvdSe;
      const payoutTable = sql.raw(race.source === "jra" ? "jvd_hr" : "nvd_hr");
      const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
      const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
      const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
      const classCondition = getStatsClassCondition(race, settings.classConditionName);
      const raceTitleCondition = cleanDbText(race.kyosomeiHondai)
        ? sql`ra.kyosomei_hondai = ${race.kyosomeiHondai}`
        : sql`false`;
      const raceSubtitleCondition = cleanDbText(race.kyosomeiFukudai)
        ? sql`ra.kyosomei_fukudai = ${race.kyosomeiFukudai}`
        : cleanDbText(race.kyosomeiKakkonai)
          ? sql`ra.kyosomei_kakkonai = ${race.kyosomeiKakkonai}`
          : sql`false`;
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
      fallback_matched_races as (
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
          and (${settings.includeSurface} = false or ${trackCodeIn(surfaceCodes)})
          and (${settings.includeTurn} = false or ${trackCodeIn(turnCodes)})
      ),
      matched_races as (
        select * from strict_matched_races
        union all
        select * from fallback_matched_races
        where not exists (select 1 from strict_matched_races)
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
      fallback_payout_values as (
        select
          payouts.bet_type,
          payouts.bet_order,
          fallback_matched_races.kaisai_nen,
          fallback_matched_races.kaisai_tsukihi,
          fallback_matched_races.keibajo_code,
          fallback_matched_races.race_bango,
          fallback_matched_races.race_name,
          nullif(regexp_replace(coalesce(payouts.payout_text, ''), '[^0-9]', '', 'g'), '')::numeric as payout
        from fallback_matched_races
        join ${payoutTable} hr
          on hr.kaisai_nen = fallback_matched_races.kaisai_nen
          and hr.kaisai_tsukihi = fallback_matched_races.kaisai_tsukihi
          and hr.keibajo_code = fallback_matched_races.keibajo_code
          and hr.race_bango = fallback_matched_races.race_bango
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
      payout_values as (
        select * from strict_payout_values
        union all
        select * from fallback_payout_values
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
        ) filter (where "detailRank" <= 200) as details
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
      const raceTable = race.source === "jra" ? jvdRa : nvdRa;
      const runnerTable = race.source === "jra" ? jvdSe : nvdSe;
      const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
      const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
      const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
      const classCondition = getStatsClassCondition(race, settings.classConditionName);
      const raceTitleCondition = cleanDbText(race.kyosomeiHondai)
        ? sql`ra.kyosomei_hondai = ${race.kyosomeiHondai}`
        : sql`false`;
      const raceSubtitleCondition = cleanDbText(race.kyosomeiFukudai)
        ? sql`ra.kyosomei_fukudai = ${race.kyosomeiFukudai}`
        : cleanDbText(race.kyosomeiKakkonai)
          ? sql`ra.kyosomei_kakkonai = ${race.kyosomeiKakkonai}`
          : sql`false`;
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
        ) filter (where "detailRank" <= 200) as details
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
      const raceTable = race.source === "jra" ? jvdRa : nvdRa;
      const runnerTable = race.source === "jra" ? jvdSe : nvdSe;
      const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
      const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
      const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
      const classCondition = getStatsClassCondition(race, settings.classConditionName);
      const raceTitleCondition = cleanDbText(race.kyosomeiHondai)
        ? sql`ra.kyosomei_hondai = ${race.kyosomeiHondai}`
        : sql`false`;
      const raceSubtitleCondition = cleanDbText(race.kyosomeiFukudai)
        ? sql`ra.kyosomei_fukudai = ${race.kyosomeiFukudai}`
        : cleanDbText(race.kyosomeiKakkonai)
          ? sql`ra.kyosomei_kakkonai = ${race.kyosomeiKakkonai}`
          : sql`false`;
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
          ) filter (where "detailRank" <= 200) as details
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
