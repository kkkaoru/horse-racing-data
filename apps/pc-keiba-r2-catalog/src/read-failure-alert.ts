// Runs with bun. Read-failure alerting only: never throws into the request path.
import type { AlertMessage } from "../../pipeline-health-monitor/src/types";

const JST_OFFSET_MS: number = 9 * 60 * 60 * 1000;
const CHECK_NAME: string = "catalog-read-failure";

export interface ReadFailureAlertInput {
  event: string;
  errorName?: string;
  status?: number;
  nowMs?: number;
}

export interface ReadFailureAlertQueue {
  send(message: unknown): Promise<void>;
}

export const buildReadFailureAlert = (input: ReadFailureAlertInput): AlertMessage => ({
  checkName: CHECK_NAME,
  severity: "warning",
  title: `Catalog read failed: ${input.event}`,
  description: `A trusted Catalog binding read failed after its bounded retry and answered 503: ${input.event}.`,
  fields: [
    { name: "event", value: input.event },
    ...(input.errorName === undefined ? [] : [{ name: "errorName", value: input.errorName }]),
    ...(input.status === undefined ? [] : [{ name: "status", value: String(input.status) }]),
  ],
  timestampJst: new Date((input.nowMs ?? Date.now()) + JST_OFFSET_MS)
    .toISOString()
    .replace("Z", "+09:00"),
});

// Alerting is best-effort: a queue failure must never change the response.
export const notifyReadFailure = async (
  queue: ReadFailureAlertQueue | undefined,
  input: ReadFailureAlertInput,
): Promise<void> => {
  if (queue === undefined) return;
  try {
    await queue.send(buildReadFailureAlert(input));
  } catch {
    console.error(JSON.stringify({ event: "catalog_read_failure_alert_failed" }));
  }
};
