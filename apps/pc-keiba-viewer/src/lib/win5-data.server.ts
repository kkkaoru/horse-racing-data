import "server-only";

import { getPgPool } from "../db/client";
import { buildWin5LegInputsWithPool } from "./win5/leg-inputs";
import type { Win5Schedule } from "./win5/types";

export const buildWin5LegInputsForSchedule = async (schedule: Win5Schedule) =>
  buildWin5LegInputsWithPool(getPgPool(), schedule);
