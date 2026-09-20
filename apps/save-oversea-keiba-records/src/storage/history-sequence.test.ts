// This file runs with Bun.
import { expect, it } from "vitest";
import { buildHistorySequenceAdvance } from "./history-sequence";

it("advances an imported identity counter without setval or data changes", () => {
  const statement = buildHistorySequenceAdvance({
    table: "oversea_person_race_history",
    lastValue: "10",
    maximumId: "15",
    incrementBy: "1",
    maximumAdvance: 20,
  });
  expect(statement?.values).toStrictEqual(["oversea_person_race_history", "5"]);
  expect(statement?.text).toMatch(/nextval\(pg_get_serial_sequence\(\$1, 'history_id'\)\)/);
  expect(statement?.text).not.toMatch(/setval|update|delete|insert|alter/i);
});

it("does not allocate from a counter already ahead of imported rows", () => {
  expect(
    buildHistorySequenceAdvance({
      table: "oversea_horse_race_history",
      lastValue: "20",
      maximumId: "15",
      incrementBy: "1",
      maximumAdvance: 20,
    }),
  ).toBeNull();
});

it("does not allocate when the counter matches the largest ID", () => {
  expect(
    buildHistorySequenceAdvance({
      table: "oversea_person_race_history",
      lastValue: "15",
      maximumId: "15",
      incrementBy: "1",
      maximumAdvance: 20,
    }),
  ).toBeNull();
});

it("leaves a never-used empty table alone", () => {
  expect(
    buildHistorySequenceAdvance({
      table: "oversea_person_race_history",
      lastValue: null,
      maximumId: "0",
      incrementBy: "1",
      maximumAdvance: 0,
    }),
  ).toBeNull();
});

it("preserves bigint precision when planning allocation", () => {
  expect(
    buildHistorySequenceAdvance({
      table: "oversea_person_race_history",
      lastValue: "9007199254740993",
      maximumId: "9007199254740995",
      incrementBy: "1",
      maximumAdvance: 2,
    })?.values,
  ).toStrictEqual(["oversea_person_race_history", "2"]);
});

it.each([null, "unknown", "-1"])(
  "refuses unknown sequence position %s for a populated table",
  (lastValue) => {
    expect(() =>
      buildHistorySequenceAdvance({
        table: "oversea_person_race_history",
        lastValue,
        maximumId: "15",
        incrementBy: "1",
        maximumAdvance: 20,
      }),
    ).toThrow("History sequence position is unavailable; automatic repair is unsafe.");
  },
);

it("refuses an allocation larger than the explicit budget", () => {
  expect(() =>
    buildHistorySequenceAdvance({
      table: "oversea_person_race_history",
      lastValue: "1",
      maximumId: "15",
      incrementBy: "1",
      maximumAdvance: 5,
    }),
  ).toThrow("History sequence repair exceeds the explicitly allowed advancement budget.");
});

it.each([
  { table: "jvd_se" },
  { incrementBy: "-1" },
  { maximumId: "-1" },
  { maximumAdvance: -1 },
  { maximumAdvance: 0.5 },
])("rejects invalid metadata %j", (patch) => {
  expect(() =>
    buildHistorySequenceAdvance({
      table: "oversea_person_race_history",
      lastValue: "1",
      maximumId: "15",
      incrementBy: "1",
      maximumAdvance: 20,
      ...patch,
    }),
  ).toThrow("History sequence metadata or advancement budget is invalid.");
});
