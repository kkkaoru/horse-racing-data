// Runs with bun; guarded configuration-only DLQ attachment, never consumes or replays messages.
import { isDeepStrictEqual } from "node:util";

interface QueueRecord extends Record<string, unknown> {
  queue_id: string;
  queue_name: string;
  settings: Record<string, unknown>;
  consumers: unknown[];
  producers: unknown[];
}
interface WorkerConsumer extends Record<string, unknown> {
  consumer_id: string;
  type: "worker";
  script: string;
  settings: Record<string, unknown>;
}
export interface QueueProtectionPlan {
  expectedSource: unknown;
  consumerId: string;
  workerName: string;
  dlqId: string;
  dlqName: string;
  retentionSeconds: number;
}
export interface QueueConsumerUpdate {
  type: "worker";
  script_name: string;
  settings: Record<string, unknown>;
  dead_letter_queue: string;
}
export interface QueueProtectionPorts {
  source: () => Promise<unknown>;
  consumer: () => Promise<unknown>;
  dlq: () => Promise<unknown>;
  retain: (stage: "intent" | "acknowledgement" | "verified", value: unknown) => Promise<void>;
  update: (body: QueueConsumerUpdate) => Promise<unknown>;
}
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const queue = (value: unknown): QueueRecord => {
  if (
    !record(value) ||
    typeof value.queue_id !== "string" ||
    typeof value.queue_name !== "string" ||
    !record(value.settings) ||
    !Array.isArray(value.consumers) ||
    !Array.isArray(value.producers)
  )
    throw new Error("Invalid queue configuration");
  return {
    ...value,
    queue_id: value.queue_id,
    queue_name: value.queue_name,
    settings: value.settings,
    consumers: value.consumers,
    producers: value.producers,
  };
};
const consumer = (value: unknown): WorkerConsumer => {
  if (
    !record(value) ||
    typeof value.consumer_id !== "string" ||
    value.type !== "worker" ||
    typeof value.script !== "string" ||
    !record(value.settings)
  )
    throw new Error("Invalid worker consumer configuration");
  return {
    ...value,
    consumer_id: value.consumer_id,
    type: value.type,
    script: value.script,
    settings: value.settings,
  };
};
const noDlq = (value: WorkerConsumer): boolean =>
  value.dead_letter_queue === undefined || value.dead_letter_queue === "";
const sameConsumer = (left: WorkerConsumer, right: WorkerConsumer): boolean =>
  left.consumer_id === right.consumer_id &&
  left.script === right.script &&
  isDeepStrictEqual(left.settings, right.settings);
const sameQueue = (left: QueueRecord, right: QueueRecord): boolean =>
  left.queue_id === right.queue_id &&
  left.queue_name === right.queue_name &&
  isDeepStrictEqual(left.settings, right.settings) &&
  isDeepStrictEqual(left.producers, right.producers);
const validateDlq = (value: unknown, plan: QueueProtectionPlan): QueueRecord => {
  const current: QueueRecord = queue(value);
  if (
    current.queue_id !== plan.dlqId ||
    current.queue_name !== plan.dlqName ||
    current.settings.message_retention_period !== plan.retentionSeconds ||
    current.settings.delivery_delay !== 0 ||
    current.consumers.length !== 0
  )
    throw new Error("Dead-letter queue configuration mismatch");
  return current;
};
const sourceConsumer = (value: QueueRecord): WorkerConsumer => {
  if (value.consumers.length !== 1) throw new Error("Unexpected source consumer count");
  return consumer(value.consumers[0]);
};

/** Caller supplies an exclusive writer lock; a preflight is NOT an atomic provider-side CAS. No retry. */
export const attachQueueDeadLetter = async (
  plan: QueueProtectionPlan,
  ports: QueueProtectionPorts,
): Promise<void> => {
  if (
    !Number.isInteger(plan.retentionSeconds) ||
    plan.retentionSeconds < 60 ||
    plan.retentionSeconds > 1209600
  )
    throw new Error("Invalid dead-letter retention");
  const expected: QueueRecord = queue(plan.expectedSource);
  const baseline: WorkerConsumer = sourceConsumer(expected);
  if (
    baseline.consumer_id !== plan.consumerId ||
    baseline.script !== plan.workerName ||
    !noDlq(baseline) ||
    plan.dlqId === expected.queue_id ||
    plan.dlqName === expected.queue_name
  )
    throw new Error("Invalid queue protection scope");
  const [sourceValue, consumerValue, dlqValue] = await Promise.all([
    ports.source(),
    ports.consumer(),
    ports.dlq(),
  ]);
  const source: QueueRecord = queue(sourceValue);
  const current: WorkerConsumer = consumer(consumerValue);
  const summarized: WorkerConsumer = sourceConsumer(source);
  if (
    !sameQueue(source, expected) ||
    !sameConsumer(current, baseline) ||
    !sameConsumer(summarized, baseline) ||
    !noDlq(current) ||
    !noDlq(summarized)
  )
    throw new Error("Source queue configuration changed");
  const dlq: QueueRecord = validateDlq(dlqValue, plan);
  if (dlq.producers.length !== 0) throw new Error("Dead-letter queue already has producers");
  const body: QueueConsumerUpdate = {
    type: "worker",
    script_name: current.script,
    settings: current.settings,
    dead_letter_queue: plan.dlqName,
  };
  await ports.retain("intent", { source, consumer: current, dlq, body });
  const acknowledgement: unknown = await ports.update(body);
  await ports.retain("acknowledgement", acknowledgement);
  const [afterSourceValue, afterConsumerValue, afterDlqValue] = await Promise.all([
    ports.source(),
    ports.consumer(),
    ports.dlq(),
  ]);
  const afterSource: QueueRecord = queue(afterSourceValue);
  const afterConsumer: WorkerConsumer = consumer(afterConsumerValue);
  const afterSummary: WorkerConsumer = sourceConsumer(afterSource);
  const afterDlq: QueueRecord = validateDlq(afterDlqValue, plan);
  if (
    !sameQueue(afterSource, expected) ||
    !sameConsumer(afterSummary, baseline) ||
    !sameConsumer(afterConsumer, baseline) ||
    afterConsumer.dead_letter_queue !== plan.dlqName ||
    (!noDlq(afterSummary) && afterSummary.dead_letter_queue !== plan.dlqName)
  )
    throw new Error("Queue protection readback mismatch");
  await ports.retain("verified", {
    source: afterSource,
    consumer: afterConsumer,
    dlq: afterDlq,
    businessReplayEnabled: false,
  });
};
