// Runs with bun via Vitest; pure planning never contacts notification destinations.
import { expect, test } from "vitest";
import {
  buildIngestionMonitorNotification,
  parseIngestionMonitorNotification,
} from "./ingestion-monitor-alert";

const nowMs: number = Date.parse("2026-09-16T03:00:00.000Z");
const empty = { backlog_count: 0, backlog_bytes: 0, oldest_message_timestamp_ms: 0 };
const stored = {
  eventId: "a".repeat(64),
  observedAtMs: 1,
  message: {
    checkName: "hot-ingestion-archive:observation",
    severity: "critical",
    title: "Monitor",
    description: "Unknown coverage",
    fields: [{ name: "Queue", value: "dlq" }],
    timestampJst: "2026-09-16T12:00:00.000+09:00",
  },
};
test("decodes stored notification without leaking extra properties", () => {
  expect(parseIngestionMonitorNotification({ ...stored, privateData: "omit" })).not.toHaveProperty(
    "privateData",
  );
  expect(parseIngestionMonitorNotification(stored).message.fields).toStrictEqual([
    { name: "Queue", value: "dlq" },
  ]);
});
test.each([
  null,
  [],
  {},
  { ...stored, eventId: 1 },
  { ...stored, eventId: "x".repeat(64) },
  { ...stored, eventId: `${"a".repeat(64)}\n` },
  { ...stored, observedAtMs: "1" },
  { ...stored, observedAtMs: 1.5 },
  { ...stored, observedAtMs: -1 },
  { ...stored, message: null },
])("rejects corrupt notification envelopes", (value) => {
  expect(() => parseIngestionMonitorNotification(value)).toThrow("Invalid monitor notification");
});
test.each([
  { ...stored.message, checkName: "other" },
  { ...stored.message, severity: "recovery" },
  { ...stored.message, title: 1 },
  { ...stored.message, description: "x".repeat(2049) },
  { ...stored.message, timestampJst: null },
  { ...stored.message, fields: null },
  { ...stored.message, fields: Array.from({ length: 11 }, () => ({ name: "n", value: "v" })) },
  { ...stored.message, fields: [null] },
  { ...stored.message, fields: [{ name: 1, value: "v" }] },
  { ...stored.message, fields: [{ name: "n", value: 1 }] },
])("rejects corrupt notification messages", (message) => {
  expect(() => parseIngestionMonitorNotification({ ...stored, message })).toThrow(
    "Invalid monitor notification",
  );
});

test.each([
  "bad",
  "2026-99-99T12:00:00.000+09:00",
  "2026-09-16 12:00:00.000+09:00",
  "2026-02-30T12:00:00.000+09:00",
])(
  "requires canonical ISO 8601 timestamps for the existing Discord embed contract",
  (timestampJst) => {
    expect(() =>
      parseIngestionMonitorNotification({
        ...stored,
        message: { ...stored.message, timestampJst },
      }),
    ).toThrow("Invalid monitor notification message");
  },
);

test("empty observations do not emit recovery or certify completeness", () => {
  expect(
    buildIngestionMonitorNotification({ metrics: empty, pending: [], observedAtMs: nowMs, nowMs }),
  ).toBe(null);
});
test("recent known pending work does not generate an incident", () => {
  expect(
    buildIngestionMonitorNotification({
      metrics: { backlog_count: 1, backlog_bytes: 10, oldest_message_timestamp_ms: nowMs - 1000 },
      pending: [{ requestId: "tracked", queuedAt: "2026-09-16T02:59:59.000Z" }],
      observedAtMs: nowMs,
      nowMs,
    }),
  ).toBe(null);
});
test.each([
  { queuedAt: "2026-09-16T02:00:00.000Z", severity: "warning" },
  { queuedAt: "2026-09-04T03:00:00.000Z", severity: "critical" },
])("known pending $queuedAt overrides empty broker gauges", ({ queuedAt, severity }) => {
  const alert = buildIngestionMonitorNotification({
    metrics: empty,
    pending: [{ requestId: "tracked", queuedAt }],
    observedAtMs: nowMs,
    nowMs,
  });
  expect(alert?.message.severity === severity).toBe(true);
  expect(alert?.message.checkName).toBe("hot-ingestion-archive:backlog");
  expect(alert?.message.timestampJst).toBe("2026-09-16T12:00:00.000+09:00");
});
test.each([3600, 1036800])("broker backlog alone produces a notification at age %s", (age) => {
  expect(
    buildIngestionMonitorNotification({
      metrics: {
        backlog_count: 1,
        backlog_bytes: 10,
        oldest_message_timestamp_ms: nowMs - age * 1000,
      },
      pending: [],
      observedAtMs: nowMs,
      nowMs,
    })?.message.checkName,
  ).toBe("hot-ingestion-archive:backlog");
});
test.each(["invalid", "2026-09-17T00:00:00.000Z"])(
  "invalid tracked timestamp cannot appear healthy",
  (queuedAt) => {
    const alert = buildIngestionMonitorNotification({
      metrics: empty,
      pending: [{ requestId: "tracked", queuedAt }],
      observedAtMs: nowMs,
      nowMs,
    });
    expect(alert?.message).toMatchObject({
      severity: "critical",
      checkName: "hot-ingestion-archive:observation",
    });
  },
);
test("over-bound pending data is an observation error", () => {
  expect(
    buildIngestionMonitorNotification({
      metrics: empty,
      pending: Array.from({ length: 101 }, () => ({
        requestId: "tracked",
        queuedAt: "2026-09-16T03:00:00.000Z",
      })),
      observedAtMs: nowMs,
      nowMs,
    })?.message.checkName,
  ).toBe("hot-ingestion-archive:observation");
});
test.each([null, {}])("invalid gauges alert rather than resolving known incidents", (metrics) => {
  expect(
    buildIngestionMonitorNotification({ metrics, pending: [], observedAtMs: nowMs, nowMs })?.message
      .severity,
  ).toBe("critical");
});
test("stale samples alert, while equivalent incidents coalesce within an hour", () => {
  const first = buildIngestionMonitorNotification({
    metrics: null,
    pending: [],
    observedAtMs: nowMs,
    nowMs,
  });
  const repeated = buildIngestionMonitorNotification({
    metrics: empty,
    pending: [],
    observedAtMs: nowMs - 301000,
    nowMs: nowMs + 1000,
  });
  const later = buildIngestionMonitorNotification({
    metrics: null,
    pending: [],
    observedAtMs: nowMs + 3600000,
    nowMs: nowMs + 3600000,
  });
  expect(first?.eventId === repeated?.eventId).toBe(true);
  expect(first?.eventId === later?.eventId).toBe(false);
});
test.each([-1, Number.NaN, 9000000000000000, Date.parse("+010000-01-01T00:00:00.000Z")])(
  "rejects unusable monitor clocks",
  (clock) => {
    expect(() =>
      buildIngestionMonitorNotification({
        metrics: empty,
        pending: [],
        observedAtMs: clock,
        nowMs: clock,
      }),
    ).toThrow("Invalid monitor clock");
  },
);
