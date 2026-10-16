// Run with bun. D1-reserved single-flight enqueue for canonical day-base repair.

import { enqueueDayBasePrewarm } from "./day-base-prewarm";
import { isOldDateRunYmd } from "./old-date-guard";
import type { Env, PredictCategory } from "./types";

interface DayBaseRepairParams {
  category: PredictCategory;
  env: Env;
  force?: boolean;
  generationId?: string;
  now?: Date;
  runYmd: string;
}

interface AcquireDayBaseRepairParams extends DayBaseRepairParams {
  generationId: string;
}

export type DayBaseRepairAcquireResult =
  | { generationId: string; status: "acquired" }
  | { generationId: string | null; status: "busy" }
  | { generationId: string; status: "skipped-old-date" };

export type DayBaseRepairEnqueueOutcome = "already-enqueued" | "enqueued" | "skipped-old-date";

// Python's legal maximum layer/child chain is 35 minutes. A 45-minute lease
// prevents overlap while leaving ten minutes for Queue/Worker handoff, then
// permits self-healing if the accepted prewarm message disappears or fails.
export const DAY_BASE_REPAIR_LEASE_TTL_MS: number = 45 * 60 * 1000;

const REPAIR_INSERT_SQL: string = `insert into finish_position_day_base_repair_requests
  (category, run_ymd, requested_at, repair_generation_id)
values (?1, ?2, ?3, ?4)
on conflict(category, run_ymd) do update
set requested_at = excluded.requested_at,
    repair_generation_id = excluded.repair_generation_id
where ?5 = 1
   or finish_position_day_base_repair_requests.requested_at <= ?6
   or finish_position_day_base_repair_requests.repair_generation_id = excluded.repair_generation_id
returning repair_generation_id`;
const REPAIR_DELETE_SQL: string = `delete from finish_position_day_base_repair_requests
where category = ?1 and run_ymd = ?2 and repair_generation_id = ?3`;
const REPAIR_OWNER_SELECT_SQL: string = `select repair_generation_id from finish_position_day_base_repair_requests
where category = ?1 and run_ymd = ?2`;
const REPAIR_HEARTBEAT_SQL: string = `update finish_position_day_base_repair_requests
set requested_at = ?4
where category = ?1 and run_ymd = ?2 and repair_generation_id = ?3 and requested_at > ?5`;

export const clearDayBaseRepairReservation = async (
  params: DayBaseRepairParams,
): Promise<boolean> => {
  if (params.generationId === undefined) return false;
  const result = await params.env.FINISH_POSITION_CRON_DB.prepare(REPAIR_DELETE_SQL)
    .bind(params.category, params.runYmd, params.generationId)
    .run();
  return result.meta.changes === 1;
};

export const acquireDayBaseRepair = async (
  params: AcquireDayBaseRepairParams,
): Promise<DayBaseRepairAcquireResult> => {
  const now = params.now ?? new Date();
  if (isOldDateRunYmd(params.runYmd, now)) {
    return { generationId: params.generationId, status: "skipped-old-date" };
  }
  const staleBefore = new Date(now.getTime() - DAY_BASE_REPAIR_LEASE_TTL_MS);
  const reserved = await params.env.FINISH_POSITION_CRON_DB.prepare(REPAIR_INSERT_SQL)
    .bind(
      params.category,
      params.runYmd,
      now.toISOString(),
      params.generationId,
      params.force === true ? 1 : 0,
      staleBefore.toISOString(),
    )
    .first<{ repair_generation_id: string }>();
  if (reserved !== null) return { generationId: reserved.repair_generation_id, status: "acquired" };
  const current = await params.env.FINISH_POSITION_CRON_DB.prepare(REPAIR_OWNER_SELECT_SQL)
    .bind(params.category, params.runYmd)
    .first<{ repair_generation_id: string | null }>();
  return { generationId: current?.repair_generation_id ?? null, status: "busy" };
};

export const heartbeatDayBaseRepair = async (
  params: AcquireDayBaseRepairParams,
): Promise<boolean> => {
  const now = params.now ?? new Date();
  const staleBefore = new Date(now.getTime() - DAY_BASE_REPAIR_LEASE_TTL_MS);
  const result = await params.env.FINISH_POSITION_CRON_DB.prepare(REPAIR_HEARTBEAT_SQL)
    .bind(
      params.category,
      params.runYmd,
      params.generationId,
      now.toISOString(),
      staleBefore.toISOString(),
    )
    .run();
  return result.meta.changes === 1;
};

export const enqueueDayBaseRepairOnce = async (
  params: DayBaseRepairParams,
): Promise<DayBaseRepairEnqueueOutcome> => {
  const now = params.now ?? new Date();
  const generationId = params.generationId ?? crypto.randomUUID();
  const { force, ...reservationParams } = params;
  const acquired = await acquireDayBaseRepair({ ...reservationParams, generationId, now });
  if (acquired.status === "skipped-old-date") return "skipped-old-date";
  if (acquired.status === "busy") return "already-enqueued";
  try {
    await enqueueDayBasePrewarm({
      category: params.category,
      daysAhead: 0,
      env: params.env,
      ...(force === true ? { force: true } : {}),
      generationId: acquired.generationId,
      requestedAt: now,
      runYmd: params.runYmd,
    });
    return "enqueued";
  } catch (error) {
    await clearDayBaseRepairReservation({ ...params, generationId: acquired.generationId });
    throw error;
  }
};
