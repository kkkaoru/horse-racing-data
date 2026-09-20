// Runs with bun via Vitest; pure gauge assessment has no network or storage I/O.
import { expect, test } from "vitest";
import { assessIngestionBacklog } from "./ingestion-backlog";

const policy = {
  retentionSeconds: 1209600,
  warningAgeSeconds: 3600,
  criticalAgeSeconds: 1036800,
  maxObservationAgeSeconds: 300,
};
test("fresh empty gauges are clear only at the observation time", () => {
  expect(
    assessIngestionBacklog({
      metrics: { backlog_count: 0, backlog_bytes: 0, oldest_message_timestamp_ms: 0 },
      observedAtMs: 2000000000,
      nowMs: 2000000000,
      policy,
    }),
  ).toStrictEqual({
    status: "clear",
    reason: "empty-at-observation",
    backlogCount: 0,
    oldestAgeSeconds: null,
    retentionRemainingSeconds: null,
  });
});
test.each([
  { age: 3599, status: "pending", remaining: 1206001 },
  { age: 3600, status: "warning", remaining: 1206000 },
  { age: 1036800, status: "critical", remaining: 172800 },
  { age: 1209601, status: "critical", remaining: 0 },
])("classifies backlog age $age without implying archival", ({ age, status, remaining }) => {
  const result = assessIngestionBacklog({
    metrics: {
      backlog_count: 2,
      backlog_bytes: 128,
      oldest_message_timestamp_ms: 2000000000 - age * 1000,
    },
    observedAtMs: 2000000000,
    nowMs: 2000000000,
    policy,
  });
  expect(result.status === status).toBe(true);
  expect(result.retentionRemainingSeconds === remaining).toBe(true);
  expect(result.backlogCount).toBe(2);
  expect(result.reason).toBe("unarchived-backlog");
});
test.each([
  null,
  [],
  {},
  { backlog_count: -1, backlog_bytes: 0, oldest_message_timestamp_ms: 0 },
  { backlog_count: 0, backlog_bytes: "0", oldest_message_timestamp_ms: 0 },
  { backlog_count: 0, backlog_bytes: 0, oldest_message_timestamp_ms: Number.NaN },
])("invalid gauges never produce a healthy state", (metrics) => {
  expect(
    assessIngestionBacklog({ metrics, observedAtMs: 2000000000, nowMs: 2000000000, policy }).reason,
  ).toBe("invalid-gauges");
});
test.each([
  { bytes: 1, oldest: 0 },
  { bytes: 0, oldest: 1 },
])("contradictory empty gauges fail closed", ({ bytes, oldest }) => {
  expect(
    assessIngestionBacklog({
      metrics: { backlog_count: 0, backlog_bytes: bytes, oldest_message_timestamp_ms: oldest },
      observedAtMs: 2000000000,
      nowMs: 2000000000,
      policy,
    }).reason,
  ).toBe("inconsistent-empty-gauges");
});
test.each([0, 2000000001])("invalid positive-backlog timestamps are unknown", (oldest) => {
  expect(
    assessIngestionBacklog({
      metrics: { backlog_count: 1, backlog_bytes: 1, oldest_message_timestamp_ms: oldest },
      observedAtMs: 2000000000,
      nowMs: 2000000000,
      policy,
    }).reason,
  ).toBe("invalid-oldest-timestamp");
});
test.each([
  { observed: 2000000001, now: 2000000000 },
  { observed: 1999699999, now: 2000000000 },
  { observed: -1, now: 2000000000 },
  { observed: 1, now: Number.NaN },
])("stale/invalid samples cannot report healthy", ({ observed, now }) => {
  expect(
    assessIngestionBacklog({
      metrics: { backlog_count: 0, backlog_bytes: 0, oldest_message_timestamp_ms: 0 },
      observedAtMs: observed,
      nowMs: now,
      policy,
    }).status,
  ).toBe("unknown");
});
test.each([
  { ...policy, warningAgeSeconds: 0 },
  { ...policy, maxObservationAgeSeconds: 0 },
  { ...policy, warningAgeSeconds: 1036800 },
  { ...policy, criticalAgeSeconds: 1209600 },
  { ...policy, retentionSeconds: -1 },
])("rejects invalid operational policy", (invalidPolicy) => {
  expect(() =>
    assessIngestionBacklog({ metrics: {}, observedAtMs: 1, nowMs: 1, policy: invalidPolicy }),
  ).toThrow("Invalid backlog monitoring policy");
});
