// Run with bun. D1-reserved single-flight enqueue for canonical day-base repair.

import { enqueueDayBasePrewarm } from "./day-base-prewarm";
import type { Env, PredictCategory } from "./types";

interface DayBaseRepairParams {
  category: PredictCategory;
  env: Env;
  now?: Date;
  runYmd: string;
}

export type DayBaseRepairEnqueueOutcome = "already-enqueued" | "enqueued";

// Python's legal maximum layer/child chain is 35 minutes. A 45-minute lease
// prevents overlap while leaving ten minutes for Queue/Worker handoff, then
// permits self-healing if the accepted prewarm message disappears or fails.
export const DAY_BASE_REPAIR_LEASE_TTL_MS: number = 45 * 60 * 1000;

const REPAIR_INSERT_SQL: string = `insert into finish_position_day_base_repair_requests
  (category, run_ymd, requested_at)
values (?1, ?2, ?3)
on conflict(category, run_ymd) do update
set requested_at = excluded.requested_at
where finish_position_day_base_repair_requests.requested_at <= ?4
returning category`;
const REPAIR_DELETE_SQL: string =
  "delete from finish_position_day_base_repair_requests where category = ?1 and run_ymd = ?2";

export const clearDayBaseRepairReservation = async (params: DayBaseRepairParams): Promise<void> => {
  await params.env.FINISH_POSITION_CRON_DB.prepare(REPAIR_DELETE_SQL)
    .bind(params.category, params.runYmd)
    .run();
};

export const enqueueDayBaseRepairOnce = async (
  params: DayBaseRepairParams,
): Promise<DayBaseRepairEnqueueOutcome> => {
  const now = params.now ?? new Date();
  const staleBefore = new Date(now.getTime() - DAY_BASE_REPAIR_LEASE_TTL_MS);
  const reserved = await params.env.FINISH_POSITION_CRON_DB.prepare(REPAIR_INSERT_SQL)
    .bind(params.category, params.runYmd, now.toISOString(), staleBefore.toISOString())
    .first<{ category: string }>();
  if (reserved === null) return "already-enqueued";
  try {
    await enqueueDayBasePrewarm({
      category: params.category,
      daysAhead: 0,
      env: params.env,
      runYmd: params.runYmd,
    });
    return "enqueued";
  } catch (error) {
    await clearDayBaseRepairReservation(params);
    throw error;
  }
};
