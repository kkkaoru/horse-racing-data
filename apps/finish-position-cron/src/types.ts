// Run with bun. Type definitions for the finish-position-cron Worker.

import type { Container } from "@cloudflare/containers";

export type PredictCategory = "jra" | "nar" | "ban-ei";

export interface Env {
  FINISH_POSITION_PREDICT_CONTAINER: DurableObjectNamespace<Container<Env>>;
  FINISH_POSITION_CRON_DB: D1Database;
  NEON_DATABASE_URL: string;
  PREDICT_DAYS_AHEAD: string;
  TRIGGER_TOKEN: string;
  PREDICT_STATE: KVNamespace;
  PREDICT_QUEUE: Queue<PredictQueueMessage>;
}

export type CronAuditStatus = "started" | "success" | "error";

export interface CronAuditRecord {
  runDate: string;
  status: CronAuditStatus;
  racesPredicted: number;
  durationMs: number;
  error: string | null;
}

export interface PredictStartOptions {
  entrypoint: string[];
  envVars: Record<string, string>;
  enableInternet: boolean;
}

export interface RunDates {
  runDate: string;
  runYmd: string;
}

export interface PredictQueueMessage {
  runDate: string;
  runDateIso: string;
  runYmd: string;
  category: PredictCategory;
  daysAhead: number;
}

export interface PredictRunState {
  status: "started" | "success" | "error";
  startedAt: string;
  racesPredicted?: number;
  error?: string;
}
