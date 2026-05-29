import "server-only";
import { getPgPool } from "../db/client";
import { buildWin5LegInputsWithPool, type Win5ModelScoreLookup } from "./win5/leg-inputs";
import type { Win5Schedule } from "./win5/types";

interface BuildWin5LegInputsForScheduleParams {
  schedule: Win5Schedule;
  modelScoreLookup?: Win5ModelScoreLookup;
}

export const buildWin5LegInputsForSchedule = async (params: BuildWin5LegInputsForScheduleParams) =>
  buildWin5LegInputsWithPool({
    pool: getPgPool(),
    schedule: params.schedule,
    modelScoreLookup: params.modelScoreLookup,
  });
