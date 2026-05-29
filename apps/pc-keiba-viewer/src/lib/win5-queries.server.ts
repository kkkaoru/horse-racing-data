import "server-only";
import { sql } from "drizzle-orm";
import { cache } from "react";

import { getDb } from "../db/client";
import { safeGetCloudflareEnv } from "./cloudflare-context.server";
import {
  findJraWin5Schedule,
  listJraWin5SchedulesForYear,
  resolveWin5Schedule,
} from "./win5-schedule.server";
import { buildWin5PredictionPayload } from "./win5/prediction";
import { buildWin5LegsFromRaceJoho } from "./win5/race-joho";
import {
  WIN5_MODEL_VERSION,
  type Win5DaySummary,
  type Win5PredictionPayload,
  type Win5Schedule,
  type Win5YearSummary,
} from "./win5/types";

const getRealtimeDb = async (): Promise<PcKeibaD1Database | null> =>
  (await safeGetCloudflareEnv())?.REALTIME_DB ?? null;

const isMissingWin5D1TableError = (error: unknown): boolean => {
  const message = error instanceof Error ? error.message : String(error);
  return /no such table: win5_/u.test(message);
};

const stringifyDbValue = (value: unknown): string => {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return "";
};

const queryWin5D1 = async <T>(query: () => Promise<T>): Promise<T | null> => {
  try {
    return await query();
  } catch (error) {
    if (isMissingWin5D1TableError(error)) {
      return null;
    }
    throw error;
  }
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isWin5RaceLeg = (value: unknown): value is Win5Schedule["legs"][number] => {
  if (!isRecord(value)) return false;
  return (
    typeof value.legIndex === "number" &&
    typeof value.keibajoCode === "string" &&
    typeof value.kaisaiKai === "string" &&
    typeof value.kaisaiNichime === "string" &&
    typeof value.raceBango === "string"
  );
};

const filterLegs = (values: ReadonlyArray<unknown>): Win5Schedule["legs"] =>
  values.filter((value): value is Win5Schedule["legs"][number] => isWin5RaceLeg(value));

const isWin5PredictionPayload = (value: unknown): value is Win5PredictionPayload => {
  if (!isRecord(value)) return false;
  return (
    typeof value.modelVersion === "string" &&
    typeof value.kaisaiNen === "string" &&
    typeof value.kaisaiTsukihi === "string" &&
    Array.isArray(value.legs)
  );
};

const parseWin5PredictionPayload = (text: string): Win5PredictionPayload | null => {
  try {
    const parsed: unknown = JSON.parse(text);
    return isWin5PredictionPayload(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

const parseScheduleLegsJson = (legsJson: string): Win5Schedule["legs"] => {
  const parsed: unknown = JSON.parse(legsJson);
  if (Array.isArray(parsed)) {
    return filterLegs(parsed);
  }
  if (isRecord(parsed) && Array.isArray(parsed.legs)) {
    return filterLegs(parsed.legs);
  }
  return [];
};

const mapScheduleRow = (row: {
  kaisai_nen: string;
  kaisai_tsukihi: string;
  sale_deadline: string | null;
  source: string;
  legs_json: string;
  fetched_at: string;
}): Win5Schedule => ({
  fetchedAt: row.fetched_at,
  kaisaiNen: row.kaisai_nen,
  kaisaiTsukihi: row.kaisai_tsukihi,
  legs: parseScheduleLegsJson(row.legs_json),
  saleDeadline: row.sale_deadline,
  source: row.source === "jvd_wf" ? "jvd_wf" : "jra_web",
});

export const getWin5Years = cache(async (): Promise<Win5YearSummary[]> => {
  const db = getDb();
  const [result, jraSchedules] = await Promise.all([
    db.execute(sql`
      select
        kaisai_nen as year,
        count(*)::int as day_count
      from jvd_wf
      group by kaisai_nen
      order by kaisai_nen desc
    `),
    listJraWin5SchedulesForYear(String(new Date().getFullYear())),
  ]);

  const yearMap = new Map<string, number>();
  for (const row of result.rows) {
    yearMap.set(String(row.year), Number(row.day_count));
  }

  for (const schedule of jraSchedules) {
    if (!yearMap.has(schedule.kaisaiNen)) {
      yearMap.set(schedule.kaisaiNen, 0);
    }
  }

  return Array.from(yearMap.entries())
    .map(([year, dayCount]) => ({
      dayCount,
      year,
    }))
    .toSorted((left, right) => right.year.localeCompare(left.year));
});

export const getWin5DaySummaries = cache(async (year: string): Promise<Win5DaySummary[]> => {
  const db = getDb();
  const d1 = await getRealtimeDb();
  const [result, jraSchedules] = await Promise.all([
    db.execute(sql`
      select
        kaisai_nen,
        kaisai_tsukihi,
        race_joho_1,
        race_joho_2,
        race_joho_3,
        race_joho_4,
        race_joho_5
      from jvd_wf
      where kaisai_nen = ${year}
      order by kaisai_tsukihi asc
    `),
    listJraWin5SchedulesForYear(year),
  ]);

  const predictionDates = new Set<string>();
  if (d1) {
    const predictionRows = await queryWin5D1(() =>
      d1
        .prepare(
          `
          select kaisai_tsukihi
          from win5_predictions
          where kaisai_nen = ? and model_version = ?
        `,
        )
        .bind(year, WIN5_MODEL_VERSION)
        .all<{ kaisai_tsukihi: string }>(),
    );
    for (const row of predictionRows?.results ?? []) {
      predictionDates.add(row.kaisai_tsukihi);
    }
  }

  const dayMap = new Map<string, Win5DaySummary>();

  for (const row of result.rows) {
    const kaisaiTsukihi = String(row.kaisai_tsukihi);
    const legs = buildWin5LegsFromRaceJoho([
      stringifyDbValue(row.race_joho_1),
      stringifyDbValue(row.race_joho_2),
      stringifyDbValue(row.race_joho_3),
      stringifyDbValue(row.race_joho_4),
      stringifyDbValue(row.race_joho_5),
    ]);
    dayMap.set(kaisaiTsukihi, {
      day: kaisaiTsukihi.slice(2, 4),
      hasPrediction: predictionDates.has(kaisaiTsukihi),
      kaisaiNen: stringifyDbValue(row.kaisai_nen),
      kaisaiTsukihi,
      legCount: legs.length,
      month: kaisaiTsukihi.slice(0, 2),
    });
  }

  for (const schedule of jraSchedules) {
    if (dayMap.has(schedule.kaisaiTsukihi)) {
      continue;
    }
    dayMap.set(schedule.kaisaiTsukihi, {
      day: schedule.kaisaiTsukihi.slice(2, 4),
      hasPrediction: predictionDates.has(schedule.kaisaiTsukihi),
      kaisaiNen: schedule.kaisaiNen,
      kaisaiTsukihi: schedule.kaisaiTsukihi,
      legCount: schedule.legs.length,
      month: schedule.kaisaiTsukihi.slice(0, 2),
    });
  }

  return Array.from(dayMap.values()).toSorted((left, right) =>
    left.kaisaiTsukihi.localeCompare(right.kaisaiTsukihi),
  );
});

const buildScheduleFromJvdWf = async (
  kaisaiNen: string,
  kaisaiTsukihi: string,
): Promise<Win5Schedule | null> => {
  const db = getDb();
  const result = await db.execute(sql`
    select
      kaisai_nen,
      kaisai_tsukihi,
      race_joho_1,
      race_joho_2,
      race_joho_3,
      race_joho_4,
      race_joho_5
    from jvd_wf
    where kaisai_nen = ${kaisaiNen}
      and kaisai_tsukihi = ${kaisaiTsukihi}
    limit 1
  `);
  const row = result.rows[0];
  if (!row) {
    return null;
  }
  const legs = buildWin5LegsFromRaceJoho([
    stringifyDbValue(row.race_joho_1),
    stringifyDbValue(row.race_joho_2),
    stringifyDbValue(row.race_joho_3),
    stringifyDbValue(row.race_joho_4),
    stringifyDbValue(row.race_joho_5),
  ]);
  if (legs.length !== 5) {
    return null;
  }
  return {
    fetchedAt: new Date().toISOString(),
    kaisaiNen,
    kaisaiTsukihi,
    legs,
    saleDeadline: null,
    source: "jvd_wf",
  };
};

export const getWin5Schedule = cache(
  async (kaisaiNen: string, kaisaiTsukihi: string): Promise<Win5Schedule | null> => {
    const d1 = await getRealtimeDb();
    if (d1) {
      const row = await queryWin5D1(() =>
        d1
          .prepare(
            `
            select
              kaisai_nen,
              kaisai_tsukihi,
              sale_deadline,
              source,
              legs_json,
              fetched_at
            from win5_schedules
            where kaisai_nen = ? and kaisai_tsukihi = ?
          `,
          )
          .bind(kaisaiNen, kaisaiTsukihi)
          .first<{
            kaisai_nen: string;
            kaisai_tsukihi: string;
            sale_deadline: string | null;
            source: string;
            legs_json: string;
            fetched_at: string;
          }>(),
      );
      if (row) {
        return resolveWin5Schedule(kaisaiNen, kaisaiTsukihi, mapScheduleRow(row));
      }
    }

    const historicalSchedule = await buildScheduleFromJvdWf(kaisaiNen, kaisaiTsukihi);
    if (historicalSchedule) {
      return resolveWin5Schedule(kaisaiNen, kaisaiTsukihi, historicalSchedule);
    }

    const jraSchedule = await findJraWin5Schedule(kaisaiNen, kaisaiTsukihi);
    if (!jraSchedule) {
      return null;
    }
    return resolveWin5Schedule(kaisaiNen, kaisaiTsukihi, jraSchedule);
  },
);

export const getWin5Prediction = cache(
  async (kaisaiNen: string, kaisaiTsukihi: string): Promise<Win5PredictionPayload | null> => {
    const d1 = await getRealtimeDb();
    if (d1) {
      const row = await queryWin5D1(() =>
        d1
          .prepare(
            `
            select prediction_json
            from win5_predictions
            where kaisai_nen = ? and kaisai_tsukihi = ? and model_version = ?
          `,
          )
          .bind(kaisaiNen, kaisaiTsukihi, WIN5_MODEL_VERSION)
          .first<{ prediction_json: string }>(),
      );
      if (row?.prediction_json) {
        const parsed = parseWin5PredictionPayload(row.prediction_json);
        if (parsed !== null) return parsed;
      }
    }

    const schedule = await getWin5Schedule(kaisaiNen, kaisaiTsukihi);
    if (!schedule) {
      return null;
    }

    const { buildWin5LegInputsForSchedule } = await import("./win5-data.server");
    const { buildModelScoreLookupFromPool } = await import("./win5/model-score-lookup.server");
    const { getPgPool } = await import("../db/client");
    const modelScoreLookup = await buildModelScoreLookupFromPool({
      pool: getPgPool(),
      modelVersion: WIN5_MODEL_VERSION,
      source: "jra",
      kaisaiNen,
      kaisaiTsukihi,
    });
    const legInputs = await buildWin5LegInputsForSchedule({ schedule, modelScoreLookup });
    if (legInputs.length !== 5) {
      return null;
    }
    const averagePayoutYen = await getAverageWin5PayoutYen();
    return buildWin5PredictionPayload({
      averagePayoutYen,
      kaisaiNen,
      kaisaiTsukihi,
      legInputs,
    });
  },
);

export const getAverageWin5PayoutYen = cache(async (): Promise<number> => {
  const db = getDb();
  const result = await db.execute(sql`
    select avg(
      nullif(
        btrim(substring(haraimodoshi_win5_001 from 11 for 9)),
        ''
      )::bigint
    ) as average_payout
    from jvd_wf
    where coalesce(tekichu_nashi_flag, '0') = '0'
      and coalesce(fuseiritsu_flag, '0') = '0'
      and haraimodoshi_win5_001 is not null
  `);
  const average = Number(result.rows[0]?.average_payout ?? 0);
  return Number.isFinite(average) && average > 0 ? average : 250_000;
});

export const getTodayWin5DateParts = (): { year: string; month: string; day: string } => {
  const now = new Date();
  const jst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  return {
    year: String(jst.getUTCFullYear()),
    month: String(jst.getUTCMonth() + 1).padStart(2, "0"),
    day: String(jst.getUTCDate()).padStart(2, "0"),
  };
};
