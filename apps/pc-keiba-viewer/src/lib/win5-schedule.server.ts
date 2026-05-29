import "server-only";
import { cache } from "react";

import { getPgPool } from "../db/client";
import { fetchWin5SchedulesFromJra } from "./win5/jra-parse";
import type { Win5Schedule } from "./win5/types";

const getJraWin5Schedules = cache(
  async (fallbackYear: string): Promise<Win5Schedule[]> =>
    fetchWin5SchedulesFromJra({ fallbackYear, fetchedAt: new Date().toISOString() }),
);

export const findJraWin5Schedule = async (
  kaisaiNen: string,
  kaisaiTsukihi: string,
): Promise<Win5Schedule | null> => {
  const schedules = await getJraWin5Schedules(kaisaiNen);
  return schedules.find((schedule) => schedule.kaisaiTsukihi === kaisaiTsukihi) ?? null;
};

export const listJraWin5SchedulesForYear = async (year: string): Promise<Win5Schedule[]> => {
  const schedules = await getJraWin5Schedules(year);
  return schedules.filter((schedule) => schedule.kaisaiNen === year);
};

export const enrichWin5ScheduleLegs = async (schedule: Win5Schedule): Promise<Win5Schedule> => {
  const pool = getPgPool();
  const legs = await Promise.all(
    schedule.legs.map(async (leg, index) => {
      const resolved = await pool.query<{
        kaisai_kai: string;
        kaisai_nichime: string;
        kyosomei_hondai: string | null;
      }>(
        `
          select
            kaisai_kai,
            kaisai_nichime,
            kyosomei_hondai
          from jvd_ra
          where kaisai_nen = $1
            and kaisai_tsukihi = $2
            and keibajo_code = $3
            and ltrim(race_bango, '0') = ltrim($4, '0')
          order by kaisai_kai asc, kaisai_nichime asc
          limit 1
        `,
        [schedule.kaisaiNen, schedule.kaisaiTsukihi, leg.keibajoCode, leg.raceBango],
      );
      const row = resolved.rows[0];
      const raceName = row?.kyosomei_hondai?.replace(/\s+/gu, " ").trim();
      return {
        ...leg,
        kaisaiKai: row?.kaisai_kai ?? leg.kaisaiKai,
        kaisaiNichime: row?.kaisai_nichime ?? leg.kaisaiNichime,
        legIndex: index + 1,
        raceLabel: raceName || leg.raceLabel,
      };
    }),
  );
  return { ...schedule, legs };
};

export const resolveWin5Schedule = async (
  kaisaiNen: string,
  kaisaiTsukihi: string,
  schedule: Win5Schedule,
): Promise<Win5Schedule> =>
  enrichWin5ScheduleLegs({
    ...schedule,
    kaisaiNen,
    kaisaiTsukihi,
  });
