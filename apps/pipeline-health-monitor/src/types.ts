// Run with bun.
import type { KVNamespace, Queue } from "@cloudflare/workers-types";

export type AlertSeverity = "warning" | "critical" | "recovery";

export interface Env {
  REALTIME: { fetch: typeof fetch };
  ALERT_QUEUE: Queue<AlertMessage>;
  STATE_KV: KVNamespace;
  REALTIME_ADMIN_TOKEN: string;
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
