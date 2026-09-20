// Runs with bun via Vitest; provider I/O and immutable receipt writes are mocked.
import { expect, test } from "vitest";
import { mockDeep } from "vitest-mock-extended";
import {
  attachQueueDeadLetter,
  type QueueProtectionPlan,
  type QueueProtectionPorts,
} from "./queue-protection";

const consumer = {
  consumer_id: "consumer",
  script: "worker",
  type: "worker",
  settings: {
    batch_size: 1,
    max_retries: 3,
    max_wait_time_ms: 5000,
    max_concurrency: 4,
    retry_delay: 0,
  },
};
const source = {
  queue_id: "source-id",
  queue_name: "source",
  settings: { message_retention_period: 345600, delivery_delay: 0 },
  producers: [{ type: "worker", script: "worker" }],
  consumers: [consumer],
};
const dlq = {
  queue_id: "dlq-id",
  queue_name: "dlq",
  settings: { message_retention_period: 1209600, delivery_delay: 0 },
  producers: [],
  consumers: [],
};
const plan: QueueProtectionPlan = {
  expectedSource: source,
  consumerId: "consumer",
  workerName: "worker",
  dlqId: "dlq-id",
  dlqName: "dlq",
  retentionSeconds: 1209600,
};

test("retains intent before a single update, preserves settings, and verifies independently", async () => {
  const ports = mockDeep<QueueProtectionPorts>();
  const stages: string[] = [];
  ports.source.mockResolvedValue(source);
  ports.consumer
    .mockResolvedValueOnce({ ...consumer, dead_letter_queue: "" })
    .mockResolvedValue({ ...consumer, dead_letter_queue: "dlq" });
  ports.dlq.mockResolvedValue(dlq);
  ports.retain.mockImplementation(async (stage) => {
    stages.push(stage);
  });
  ports.update.mockImplementation(async () => {
    stages.push("update");
    return { accepted: true };
  });
  await attachQueueDeadLetter(plan, ports);
  expect(stages).toStrictEqual(["intent", "update", "acknowledgement", "verified"]);
  expect(ports.update).toHaveBeenCalledOnce();
  expect(ports.update).toHaveBeenCalledWith({
    type: "worker",
    script_name: "worker",
    settings: {
      batch_size: 1,
      max_retries: 3,
      max_wait_time_ms: 5000,
      max_concurrency: 4,
      retry_delay: 0,
    },
    dead_letter_queue: "dlq",
  });
});

test.each([
  { retentionSeconds: 0 },
  { retentionSeconds: 1209601 },
  { retentionSeconds: 1.5 },
  { consumerId: "other" },
  { workerName: "other" },
  { dlqId: "source-id" },
  { dlqName: "source" },
  { expectedSource: null },
  { expectedSource: { ...source, queue_id: 1 } },
  { expectedSource: { ...source, queue_name: 1 } },
  { expectedSource: { ...source, settings: [] } },
  { expectedSource: { ...source, producers: null } },
  { expectedSource: { ...source, consumers: null } },
  { expectedSource: { ...source, consumers: [] } },
  { expectedSource: { ...source, consumers: [null] } },
  { expectedSource: { ...source, consumers: [{ ...consumer, consumer_id: 1 }] } },
  { expectedSource: { ...source, consumers: [{ ...consumer, type: "http_pull" }] } },
  { expectedSource: { ...source, consumers: [{ ...consumer, script: 1 }] } },
  { expectedSource: { ...source, consumers: [{ ...consumer, settings: null }] } },
  { expectedSource: { ...source, consumers: [{ ...consumer, dead_letter_queue: "existing" }] } },
])("rejects an invalid baseline/scope before network mutation", async (invalid) => {
  const ports = mockDeep<QueueProtectionPorts>();
  await expect(attachQueueDeadLetter({ ...plan, ...invalid }, ports)).rejects.toThrow();
  expect(ports.source).not.toHaveBeenCalled();
  expect(ports.update).not.toHaveBeenCalled();
});

test.each([
  { source: { ...source, queue_id: "other" } },
  { source: { ...source, queue_name: "other" } },
  { source: { ...source, settings: {} } },
  { source: { ...source, producers: [] } },
  { source: { ...source, consumers: [{ ...consumer, settings: {} }] } },
  { source: { ...source, consumers: [{ ...consumer, dead_letter_queue: "existing" }] } },
  { consumer: { ...consumer, consumer_id: "other" } },
  { consumer: { ...consumer, script: "other" } },
  { consumer: { ...consumer, settings: { max_retries: 100 } } },
  { consumer: { ...consumer, dead_letter_queue: "existing" } },
  { dlq: { ...dlq, queue_id: "other" } },
  { dlq: { ...dlq, queue_name: "other" } },
  { dlq: { ...dlq, settings: { message_retention_period: 345600 } } },
  { dlq: { ...dlq, settings: { message_retention_period: 1209600, delivery_delay: 1 } } },
  { dlq: { ...dlq, consumers: [consumer] } },
  { dlq: { ...dlq, producers: [{}] } },
])("rejects live drift/foreign configuration before intent and update", async (override) => {
  const ports = mockDeep<QueueProtectionPorts>();
  ports.source.mockResolvedValue(override.source ?? source);
  ports.consumer.mockResolvedValue(override.consumer ?? consumer);
  ports.dlq.mockResolvedValue(override.dlq ?? dlq);
  await expect(attachQueueDeadLetter(plan, ports)).rejects.toThrow();
  expect(ports.retain).not.toHaveBeenCalled();
  expect(ports.update).not.toHaveBeenCalled();
});

test.each(["intent", "update", "acknowledgement", "verified"])(
  "never retries after %s failure",
  async (failure) => {
    const ports = mockDeep<QueueProtectionPorts>();
    ports.source.mockResolvedValue(source);
    ports.consumer
      .mockResolvedValueOnce(consumer)
      .mockResolvedValue({ ...consumer, dead_letter_queue: "dlq" });
    ports.dlq.mockResolvedValue(dlq);
    ports.retain.mockImplementation(async (stage) => {
      if (stage === failure) throw new Error("disk failed");
    });
    ports.update.mockImplementation(async () => {
      if (failure === "update") throw new Error("lost acknowledgement");
      return {};
    });
    await expect(attachQueueDeadLetter(plan, ports)).rejects.toThrow();
    expect(ports.update).toHaveBeenCalledTimes(failure === "intent" ? 0 : 1);
  },
);

test.each([
  { source: { ...source, settings: {} } },
  { source: { ...source, consumers: [{ ...consumer, consumer_id: "other" }] } },
  { source: { ...source, consumers: [{ ...consumer, dead_letter_queue: "wrong" }] } },
  { consumer: { ...consumer, settings: {} } },
  { consumer: { ...consumer, dead_letter_queue: "wrong" } },
])("does not treat mutation acknowledgement as verified configuration", async (changed) => {
  const ports = mockDeep<QueueProtectionPorts>();
  ports.source.mockResolvedValueOnce(source).mockResolvedValue(changed.source ?? source);
  ports.consumer
    .mockResolvedValueOnce(consumer)
    .mockResolvedValue(changed.consumer ?? { ...consumer, dead_letter_queue: "dlq" });
  ports.dlq.mockResolvedValue(dlq);
  await expect(attachQueueDeadLetter(plan, ports)).rejects.toThrow("readback mismatch");
  expect(ports.update).toHaveBeenCalledOnce();
  expect(ports.retain.mock.calls.map(([stage]) => stage)).toStrictEqual([
    "intent",
    "acknowledgement",
  ]);
});
