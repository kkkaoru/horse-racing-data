// Run with bun.
import type { KVNamespace, Queue } from "@cloudflare/workers-types";

export type AlertSeverity = "warning" | "critical" | "recovery";

export interface Env {
  REALTIME: { fetch: typeof fetch };
  FINISH_POSITION_CRON: { fetch: typeof fetch };
  ALERT_QUEUE: Queue<AlertMessage>;
  STATE_KV: KVNamespace;
  REALTIME_ADMIN_TOKEN: string;
  FINISH_POSITION_CRON_TOKEN: string;
  ALERT_ACK_TOKEN: string;
  DISCORD_ALERT_WEBHOOK_URL?: string;
  SLACK_ALERT_WEBHOOK_URL?: string;
  CUSTOM_ALERT_WEBHOOK_URL?: string;
}

export interface QueueHealthMetrics {
  lastSuccessfulFetchResultsAt: string | null;
  lastSuccessfulFetchWeightsAt: string | null;
  racesQueuedNotFetchedToday: number;
  racesStuckOverThirtyMin: number;
}

export interface HealthCheck {
  name: string;
  ok: boolean;
  skipped?: boolean;
  value: number;
  threshold: number;
  message: string;
}

export interface AlertField {
  name: string;
  value: string;
}

export interface AlertMessage {
  checkName: string;
  incidentId?: string;
  severity: AlertSeverity;
  title: string;
  description: string;
  fields: AlertField[];
  timestampJst: string;
}

export interface CheckEvaluationInput {
  metrics: QueueHealthMetrics;
  nowJst: Date;
}

export interface PredictionReadinessRace {
  raceKey: string;
  source: string;
  keibajoCode: string;
  raceBango: string;
  raceStartAtJst: string;
  minutesToPost: number;
  deadline: "T-120" | "T-60" | "T-30" | "post";
  expectedCount: number;
  predictionCount: number;
  missingCount: number;
  oldestPredictionAt: string | null;
  newestPredictionAt: string | null;
  complete: boolean;
}

export interface PredictionReadinessResponse {
  checkedAt: string;
  runYmd: string;
  races: PredictionReadinessRace[];
}

export interface DeliveryCanaryRecord {
  id: string;
  enqueuedAt: string;
  consumedAt: string | null;
  deliveryLagMs: number | null;
}

export interface DeliveryCanaryResponse {
  checkedAt: string;
  canaries: DeliveryCanaryRecord[];
}
