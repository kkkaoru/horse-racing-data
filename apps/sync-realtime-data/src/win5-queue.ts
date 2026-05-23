import {
  buildWin5PredictionPayload,
} from "../../pc-keiba-viewer/src/lib/win5/prediction";
import { WIN5_MODEL_VERSION } from "../../pc-keiba-viewer/src/lib/win5/types";
import { getFinishPositionPool } from "./finish-position-lite-pool";
import type { Env, Win5ScheduleJob } from "./types";
import {
  getWin5Schedule,
  markWin5InferenceState,
  upsertWin5Prediction,
  upsertWin5Schedule,
} from "./win5-d1";
import {
  buildWin5LegInputsFromPostgres,
  buildWin5ScheduleFromJvdWfRow,
  enrichWin5ScheduleLegs,
  getAverageWin5PayoutYen,
} from "./win5-postgres";

export interface Win5PredictionJobSummary {
  kaisaiNen: string;
  kaisaiTsukihi: string;
  legCount: number;
  modelVersion: string;
  skipped?: boolean;
}

export const handleWin5PredictionJob = async (
  env: Env,
  job: Extract<Win5ScheduleJob, { type: "generate-win5-predictions" }>,
): Promise<Win5PredictionJobSummary> => {
  const updatedAt = job.predictedAt ?? new Date().toISOString();
  await markWin5InferenceState(env.REALTIME_DB, {
    kaisaiNen: job.kaisaiNen,
    kaisaiTsukihi: job.kaisaiTsukihi,
    incrementAttempt: true,
    status: "processing",
    updatedAt,
  });

  try {
    let schedule =
      (await getWin5Schedule(env.REALTIME_DB, job.kaisaiNen, job.kaisaiTsukihi)) ?? null;
    const pool = getFinishPositionPool(env);

    if (schedule === null) {
      const wfResult = await pool.query<Record<string, string>>(
        `
          select *
          from jvd_wf
          where kaisai_nen = $1 and kaisai_tsukihi = $2
          limit 1
        `,
        [job.kaisaiNen, job.kaisaiTsukihi],
      );
      const wfSchedule = wfResult.rows[0]
        ? buildWin5ScheduleFromJvdWfRow(wfResult.rows[0])
        : null;
      if (!wfSchedule) {
        throw new Error("WIN5 schedule not found");
      }
      schedule = await enrichWin5ScheduleLegs(pool, wfSchedule);
      await upsertWin5Schedule(env.REALTIME_DB, schedule);
    } else {
      schedule = await enrichWin5ScheduleLegs(pool, schedule);
      await upsertWin5Schedule(env.REALTIME_DB, schedule);
    }

    const legInputs = await buildWin5LegInputsFromPostgres(pool, schedule);
    if (legInputs.length !== 5) {
      throw new Error(`WIN5 runners incomplete: ${legInputs.length}/5 legs`);
    }

    const averagePayoutYen = await getAverageWin5PayoutYen(pool);
    const payload = buildWin5PredictionPayload({
      averagePayoutYen,
      kaisaiNen: job.kaisaiNen,
      kaisaiTsukihi: job.kaisaiTsukihi,
      legInputs,
      predictedAt: updatedAt,
    });
    await upsertWin5Prediction(env.REALTIME_DB, payload);
    await markWin5InferenceState(env.REALTIME_DB, {
      kaisaiNen: job.kaisaiNen,
      kaisaiTsukihi: job.kaisaiTsukihi,
      status: "completed",
      updatedAt,
    });

    return {
      kaisaiNen: job.kaisaiNen,
      kaisaiTsukihi: job.kaisaiTsukihi,
      legCount: payload.legs.length,
      modelVersion: WIN5_MODEL_VERSION,
    };
  } catch (error) {
    await markWin5InferenceState(env.REALTIME_DB, {
      kaisaiNen: job.kaisaiNen,
      kaisaiTsukihi: job.kaisaiTsukihi,
      lastError: error instanceof Error ? error.message : String(error),
      status: "failed",
      updatedAt,
    });
    throw error;
  }
};
