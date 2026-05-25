import type { Pool } from "pg";

import { buildWin5LegInputsWithPool } from "../../pc-keiba-viewer/src/lib/win5/leg-inputs";
import { buildWin5LegsFromRaceJoho } from "../../pc-keiba-viewer/src/lib/win5/race-joho";
import type { Win5RaceLeg, Win5Schedule } from "../../pc-keiba-viewer/src/lib/win5/types";

interface RaceLegLookupRow {
  kaisai_kai: string;
  kaisai_nichime: string;
  keibajo_code: string;
  kyosomei_hondai: string | null;
  race_bango: string;
}

export const resolveWin5LegFromPostgres = async (
  pool: Pool,
  params: {
    kaisaiNen: string;
    kaisaiTsukihi: string;
    keibajoCode: string;
    raceBango: string;
  },
): Promise<Win5RaceLeg | null> => {
  const result = await pool.query<RaceLegLookupRow>(
    `
      select
        kaisai_kai,
        kaisai_nichime,
        keibajo_code,
        race_bango,
        kyosomei_hondai
      from jvd_ra
      where kaisai_nen = $1
        and kaisai_tsukihi = $2
        and keibajo_code = $3
        and ltrim(race_bango, '0') = ltrim($4, '0')
      order by kaisai_kai asc, kaisai_nichime asc
      limit 1
    `,
    [params.kaisaiNen, params.kaisaiTsukihi, params.keibajoCode, params.raceBango],
  );
  const row = result.rows[0];
  if (!row) {
    return null;
  }
  return {
    legIndex: 0,
    kaisaiKai: row.kaisai_kai,
    kaisaiNichime: row.kaisai_nichime,
    keibajoCode: row.keibajo_code,
    raceBango: row.race_bango.replace(/^0+/u, "") || row.race_bango,
    raceLabel: row.kyosomei_hondai?.trim() ?? undefined,
  };
};

export const enrichWin5ScheduleLegs = async (
  pool: Pool,
  schedule: Win5Schedule,
): Promise<Win5Schedule> => {
  const legs = await Promise.all(
    schedule.legs.map(async (leg, index) => {
      const resolved = await resolveWin5LegFromPostgres(pool, {
        kaisaiNen: schedule.kaisaiNen,
        kaisaiTsukihi: schedule.kaisaiTsukihi,
        keibajoCode: leg.keibajoCode,
        raceBango: leg.raceBango,
      });
      if (resolved === null) {
        return { ...leg, legIndex: index + 1 };
      }
      return {
        ...leg,
        ...resolved,
        legIndex: index + 1,
        keibajoName: leg.keibajoName,
        raceLabel: leg.raceLabel ?? resolved.raceLabel,
        startTime: leg.startTime,
      };
    }),
  );
  return { ...schedule, legs };
};

export const buildWin5LegInputsFromPostgres = async (pool: Pool, schedule: Win5Schedule) => {
  const enrichedSchedule = await enrichWin5ScheduleLegs(pool, schedule);
  return buildWin5LegInputsWithPool({ pool, schedule: enrichedSchedule });
};

export const buildWin5ScheduleFromJvdWfRow = (row: Record<string, string>): Win5Schedule | null => {
  const legs = buildWin5LegsFromRaceJoho([
    row.race_joho_1,
    row.race_joho_2,
    row.race_joho_3,
    row.race_joho_4,
    row.race_joho_5,
  ]);
  if (legs.length !== 5) {
    return null;
  }
  return {
    fetchedAt: new Date().toISOString(),
    kaisaiNen: row.kaisai_nen ?? "",
    kaisaiTsukihi: row.kaisai_tsukihi ?? "",
    legs,
    saleDeadline: null,
    source: "jvd_wf",
  };
};

export const getAverageWin5PayoutYen = async (pool: Pool): Promise<number> => {
  const result = await pool.query<{ average_payout: string | null }>(
    `
      select avg(
        nullif(
          btrim(substring(haraimodoshi_win5_001 from 11 for 9)),
          ''
        )::bigint
      )::text as average_payout
      from jvd_wf
      where coalesce(tekichu_nashi_flag, '0') = '0'
        and coalesce(fuseiritsu_flag, '0') = '0'
        and haraimodoshi_win5_001 is not null
    `,
  );
  const average = Number(result.rows[0]?.average_payout ?? 0);
  return Number.isFinite(average) && average > 0 ? average : 250_000;
};
