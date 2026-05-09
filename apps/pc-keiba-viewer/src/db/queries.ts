import "server-only";
import { and, asc, desc, eq, sql } from "drizzle-orm";
import { cache } from "react";

import { TRACK_LABELS, type RaceSource } from "../lib/codes";
import type {
  BloodlineStatsRow,
  CourseInfo,
  HorseRaceResult,
  RaceDaySummary,
  RaceDetail,
  RaceListItem,
  RaceYearSummary,
  Runner,
  SimilarRaceStatsRow,
  SimilarRaceStatsSettings,
  Training,
} from "../lib/race-types";
import { db } from "./client";
import { jvdCs, jvdRa, jvdSe, jvdUm, nvdRa, nvdSe, nvdUm } from "./schema";

export const getRaceYears = cache(async (): Promise<RaceYearSummary[]> => {
  const result = await db.execute<{
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
});

export const getRaceDaySummaries = cache(async (year: string): Promise<RaceDaySummary[]> => {
  const result = await db.execute<{
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
});

export const getRacesByDate = cache(
  async (year: string, month: string, day: string): Promise<RaceListItem[]> => {
    const monthDay = `${month}${day}`;
    const result = await db.execute<RaceListItem>(sql`
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
  },
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
    const table = source === "jra" ? jvdRa : nvdRa;
    const [race] = await db
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

export const getRaceRunners = cache(
  async (
    source: RaceSource,
    year: string,
    month: string,
    day: string,
    keibajoCode: string,
    raceNumber: string,
  ): Promise<Runner[]> => {
    const table = source === "jra" ? jvdSe : nvdSe;
    return db
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
          eq(table.kaisaiTsukihi, `${month}${day}`),
          eq(table.keibajoCode, keibajoCode),
          eq(table.raceBango, raceNumber),
        ),
      )
      .orderBy(asc(table.umaban), asc(table.kettoTorokuBango));
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

    const [course] = await db
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
    const raceTable = source === "jra" ? jvdRa : nvdRa;
    const runnerTable = source === "jra" ? jvdSe : nvdSe;
    const monthDay = `${month}${day}`;
    const raceDate = `${year}${monthDay}`;

    const result = await db.execute<HorseRaceResult>(sql`
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

    const monthDay = `${month}${day}`;
    const result = await db.execute<Training>(sql`
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

const toCount = (value: string | number | bigint | null | undefined): number => Number(value ?? 0);
const toRate = (value: string | number | null | undefined): number => Number(value ?? 0);
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

export const getBloodlineStats = cache(
  async (race: RaceDetail, settings: SimilarRaceStatsSettings): Promise<BloodlineStatsRow[]> => {
    const raceTable = race.source === "jra" ? jvdRa : nvdRa;
    const runnerTable = race.source === "jra" ? jvdSe : nvdSe;
    const horseTable = race.source === "jra" ? jvdUm : nvdUm;
    const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
    const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
    const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
    const classCondition =
      cleanDbText(race.kyosoJokenCode) === "000" && settings.classConditionName
        ? sql`regexp_replace(ra.kyoso_joken_meisho, '[[:space:]　]+', ' ', 'g') like ${`%${settings.classConditionName}%`}`
        : sql`ra.kyoso_joken_code = ${race.kyosoJokenCode}`;
    const raceTitleCondition = cleanDbText(race.kyosomeiHondai)
      ? sql`ra.kyosomei_hondai = ${race.kyosomeiHondai}`
      : sql`false`;
    const raceSubtitleCondition = cleanDbText(race.kyosomeiFukudai)
      ? sql`ra.kyosomei_fukudai = ${race.kyosomeiFukudai}`
      : cleanDbText(race.kyosomeiKakkonai)
        ? sql`ra.kyosomei_kakkonai = ${race.kyosomeiKakkonai}`
        : sql`false`;
    const result = await db.execute<{
      category: "damSire" | "sire" | "sireSire";
      currentHorseNumbers: string;
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
          coalesce(nullif(regexp_replace(se.umaban, '^0+', ''), ''), '0') as umaban,
          se.umaban::int as "umabanSort",
          se.wakuban,
          coalesce(nullif(regexp_replace(um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') as sire,
          coalesce(nullif(regexp_replace(um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') as "sireSire",
          coalesce(nullif(regexp_replace(um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') as "damSire"
        from ${runnerTable} se
        left join ${horseTable} um
          on um.ketto_toroku_bango = se.ketto_toroku_bango
        where
          se.kaisai_nen = ${race.kaisaiNen}
          and se.kaisai_tsukihi = ${race.kaisaiTsukihi}
          and se.keibajo_code = ${race.keibajoCode}
          and se.race_bango = ${race.raceBango}
      ),
      target_entries as (
        select 'sire'::text as category, sire as name, umaban, "umabanSort", wakuban
        from current_entries
        union all
        select 'damSire'::text as category, "damSire" as name, umaban, "umabanSort", wakuban
        from current_entries
        union all
        select 'sireSire'::text as category, "sireSire" as name, umaban, "umabanSort", wakuban
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
      matched_entries as (
        select
          se.wakuban,
          ra.race_bango,
          se.kakutei_chakujun,
          se.ketto_toroku_bango,
          coalesce(nullif(regexp_replace(um.ketto_joho_01b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') as sire,
          coalesce(nullif(regexp_replace(um.ketto_joho_03b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') as "sireSire",
          coalesce(nullif(regexp_replace(um.ketto_joho_05b, '^[[:space:]　]+|[[:space:]　]+$', '', 'g'), ''), '不明') as "damSire"
        from ${raceTable} ra
        join ${runnerTable} se
          on se.kaisai_nen = ra.kaisai_nen
          and se.kaisai_tsukihi = ra.kaisai_tsukihi
          and se.keibajo_code = ra.keibajo_code
          and se.race_bango = ra.race_bango
        left join ${horseTable} um
          on um.ketto_toroku_bango = se.ketto_toroku_bango
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
          and (${settings.includeRaceTitle} = false or ${raceTitleCondition})
          and (${settings.includeRaceSubtitle} = false or ${raceSubtitleCondition})
          and (${settings.includeAge} = false or ra.kyoso_shubetsu_code = ${race.kyosoShubetsuCode})
          and (
            ${settings.includeClass} = false
            or ${classCondition}
          )
          and (${settings.includeSex} = false or ra.kyoso_kigo_code = ${race.kyosoKigoCode})
          and (${settings.includeSurface} = false or ${trackCodeIn(surfaceCodes)})
          and (${settings.includeTurn} = false or ${trackCodeIn(turnCodes)})
          and (${settings.includeDistance} = false or ra.kyori = ${race.kyori})
      ),
      grouped_entries as (
        select
          'sire'::text as category,
          sire as name,
          wakuban,
          race_bango,
          kakutei_chakujun,
          ketto_toroku_bango
        from matched_entries
        union all
        select
          'damSire'::text as category,
          "damSire" as name,
          wakuban,
          race_bango,
          kakutei_chakujun,
          ketto_toroku_bango
        from matched_entries
        union all
        select
          'sireSire'::text as category,
          "sireSire" as name,
          wakuban,
          race_bango,
          kakutei_chakujun,
          ketto_toroku_bango
        from matched_entries
      ),
      stats as (
        select
          grouped_entries.category,
          grouped_entries.name,
          targets."currentHorseNumbers",
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
        from grouped_entries
        join targets
          on targets.category = grouped_entries.category
          and targets.name = grouped_entries.name
        where
          grouped_entries.name <> '不明'
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
        group by
          grouped_entries.category,
          grouped_entries.name,
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
  },
);

export const getSimilarRaceStats = cache(
  async (race: RaceDetail, settings: SimilarRaceStatsSettings): Promise<SimilarRaceStatsRow[]> => {
    const raceTable = race.source === "jra" ? jvdRa : nvdRa;
    const runnerTable = race.source === "jra" ? jvdSe : nvdSe;
    const raceDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
    const surfaceCodes = getTrackCodesBySurface(getTrackSurface(race.trackCode));
    const turnCodes = getTrackCodesByTurn(getTrackTurn(race.trackCode));
    const classCondition =
      cleanDbText(race.kyosoJokenCode) === "000" && settings.classConditionName
        ? sql`regexp_replace(ra.kyoso_joken_meisho, '[[:space:]　]+', ' ', 'g') like ${`%${settings.classConditionName}%`}`
        : sql`ra.kyoso_joken_code = ${race.kyosoJokenCode}`;
    const raceTitleCondition = cleanDbText(race.kyosomeiHondai)
      ? sql`ra.kyosomei_hondai = ${race.kyosomeiHondai}`
      : sql`false`;
    const raceSubtitleCondition = cleanDbText(race.kyosomeiFukudai)
      ? sql`ra.kyosomei_fukudai = ${race.kyosomeiFukudai}`
      : cleanDbText(race.kyosomeiKakkonai)
        ? sql`ra.kyosomei_kakkonai = ${race.kyosomeiKakkonai}`
        : sql`false`;
    const result = await db.execute<{
      category: "jockey" | "owner" | "trainer";
      currentHorseNumbers: string;
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
          se.wakuban,
          ra.race_bango,
          se.kakutei_chakujun,
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
          and (${settings.includeRaceTitle} = false or ${raceTitleCondition})
          and (${settings.includeRaceSubtitle} = false or ${raceSubtitleCondition})
          and (${settings.includeAge} = false or ra.kyoso_shubetsu_code = ${race.kyosoShubetsuCode})
          and (
            ${settings.includeClass} = false
            or ${classCondition}
          )
          and (${settings.includeSex} = false or ra.kyoso_kigo_code = ${race.kyosoKigoCode})
          and (${settings.includeSurface} = false or ${trackCodeIn(surfaceCodes)})
          and (${settings.includeTurn} = false or ${trackCodeIn(turnCodes)})
          and (${settings.includeDistance} = false or ra.kyori = ${race.kyori})
      ),
      grouped_entries as (
        select
          'jockey'::text as category,
          jockey as name,
          wakuban,
          race_bango,
          kakutei_chakujun,
          ketto_toroku_bango
        from matched_entries
        union all
        select
          'trainer'::text as category,
          trainer as name,
          wakuban,
          race_bango,
          kakutei_chakujun,
          ketto_toroku_bango
        from matched_entries
        union all
        select
          'owner'::text as category,
          owner as name,
          wakuban,
          race_bango,
          kakutei_chakujun,
          ketto_toroku_bango
        from matched_entries
      ),
      stats as (
        select
          grouped_entries.category,
          grouped_entries.name,
          targets."currentHorseNumbers",
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
        from grouped_entries
        join targets
          on targets.category = grouped_entries.category
          and targets.name = grouped_entries.name
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
        group by
          grouped_entries.category,
          grouped_entries.name,
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
  },
);
