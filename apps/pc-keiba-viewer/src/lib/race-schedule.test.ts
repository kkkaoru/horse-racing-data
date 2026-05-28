// bun run

import { expect, it } from "vitest";

import {
  SCHEDULE_TASK_KINDS,
  SCHEDULE_TASK_LABELS,
  buildSortedRaceScheduleSlots,
  enumerateRaceScheduleSlots,
  type RaceScheduleSourceRace,
} from "./race-schedule";

const narRace: RaceScheduleSourceRace = {
  keibajoCode: "42",
  raceStartAt: "2026-05-17T12:30:00+09:00",
  source: "nar",
};

const jraRace: RaceScheduleSourceRace = {
  keibajoCode: "05",
  raceStartAt: "2026-05-17T15:35:00+09:00",
  source: "jra",
};

it("exposes a stable order of supported kinds", () => {
  expect(SCHEDULE_TASK_KINDS).toStrictEqual([
    "odds",
    "horse-weight",
    "paddock",
    "result",
    "trend-cache-warm",
    "running-style-features",
    "running-style",
  ]);
});

it("maps each supported kind to a Japanese label", () => {
  expect(SCHEDULE_TASK_LABELS.odds).toBe("オッズ更新");
  expect(SCHEDULE_TASK_LABELS["horse-weight"]).toBe("馬体重取得");
  expect(SCHEDULE_TASK_LABELS.paddock).toBe("パドック取得");
  expect(SCHEDULE_TASK_LABELS.result).toBe("結果取得");
  expect(SCHEDULE_TASK_LABELS["trend-cache-warm"]).toBe("トレンドキャッシュ温め");
  expect(SCHEDULE_TASK_LABELS["running-style-features"]).toBe("脚質特徴量生成");
  expect(SCHEDULE_TASK_LABELS["running-style"]).toBe("脚質予測生成");
});

it("returns no slots when the race start timestamp is invalid", () => {
  expect(enumerateRaceScheduleSlots({ ...narRace, raceStartAt: "not-a-date" })).toStrictEqual([]);
});

it("places horse-weight slots on 3-minute UTC ticks within 20 minutes before race start", () => {
  const slots = enumerateRaceScheduleSlots(narRace);
  const horseWeightSlots = slots.filter((slot) => slot.kind === "horse-weight");
  expect(horseWeightSlots.map((slot) => slot.scheduledAt)).toStrictEqual([
    "2026-05-17T03:12:00.000Z",
    "2026-05-17T03:15:00.000Z",
    "2026-05-17T03:18:00.000Z",
    "2026-05-17T03:21:00.000Z",
    "2026-05-17T03:24:00.000Z",
    "2026-05-17T03:27:00.000Z",
  ]);
});

it("places paddock slots only for JRA races within the 35-min before / 2-min after window", () => {
  expect(
    enumerateRaceScheduleSlots(narRace).filter((slot) => slot.kind === "paddock"),
  ).toStrictEqual([]);
  const jraSlots = enumerateRaceScheduleSlots(jraRace).filter((slot) => slot.kind === "paddock");
  expect(jraSlots[0]?.scheduledAt).toBe("2026-05-17T06:00:00.000Z");
  expect(jraSlots.at(-1)?.scheduledAt).toBe("2026-05-17T06:36:00.000Z");
});

it("emits three result slots at 5-minute intervals starting at race start", () => {
  const resultSlots = enumerateRaceScheduleSlots(narRace).filter((slot) => slot.kind === "result");
  expect(resultSlots.map((slot) => slot.scheduledAt)).toStrictEqual([
    "2026-05-17T03:30:00.000Z",
    "2026-05-17T03:35:00.000Z",
    "2026-05-17T03:40:00.000Z",
  ]);
});

it("schedules trend cache warm exactly 20 minutes before race start", () => {
  const trendSlots = enumerateRaceScheduleSlots(narRace).filter(
    (slot) => slot.kind === "trend-cache-warm",
  );
  expect(trendSlots.map((slot) => slot.scheduledAt)).toStrictEqual(["2026-05-17T03:10:00.000Z"]);
});

it("schedules running style features exactly 90 minutes before race start", () => {
  const slots = enumerateRaceScheduleSlots(narRace).filter(
    (slot) => slot.kind === "running-style-features",
  );
  expect(slots.map((slot) => slot.scheduledAt)).toStrictEqual(["2026-05-17T02:00:00.000Z"]);
});

it("schedules running style prediction exactly 60 minutes before race start", () => {
  const slots = enumerateRaceScheduleSlots(narRace).filter((slot) => slot.kind === "running-style");
  expect(slots.map((slot) => slot.scheduledAt)).toStrictEqual(["2026-05-17T02:30:00.000Z"]);
});

it("returns no running style features slots when the race start timestamp is invalid", () => {
  const slots = enumerateRaceScheduleSlots({ ...narRace, raceStartAt: "not-a-date" });
  expect(slots.filter((slot) => slot.kind === "running-style-features")).toStrictEqual([]);
});

it("returns no running style prediction slots when the race start timestamp is invalid", () => {
  const slots = enumerateRaceScheduleSlots({ ...narRace, raceStartAt: "not-a-date" });
  expect(slots.filter((slot) => slot.kind === "running-style")).toStrictEqual([]);
});

it("enumerates NAR odds slots from sale start hourly up to one hour before race", () => {
  const oddsSlots = enumerateRaceScheduleSlots(narRace).filter((slot) => slot.kind === "odds");
  expect(oddsSlots[0]?.scheduledAt).toBe("2026-05-17T01:00:00.000Z");
  expect(oddsSlots.at(-1)?.scheduledAt).toBe("2026-05-17T03:29:00.000Z");
});

it("enumerates JRA odds slots from previous day 19:00 JST through one minute before race", () => {
  const oddsSlots = enumerateRaceScheduleSlots(jraRace).filter((slot) => slot.kind === "odds");
  expect(oddsSlots[0]?.scheduledAt).toBe("2026-05-16T10:00:00.000Z");
  expect(oddsSlots.at(-1)?.scheduledAt).toBe("2026-05-17T06:34:00.000Z");
});

it("uses the venue context to determine NAR night-race sale start", () => {
  const lastRaceStart = "2026-05-17T20:30:00+09:00";
  const firstOddsSlot = buildSortedRaceScheduleSlots([narRace], () => ({
    venueLastRaceStartAt: lastRaceStart,
  })).find((slot) => slot.kind === "odds");
  expect(firstOddsSlot?.scheduledAt).toBe("2026-05-17T03:00:00.000Z");
});

it("returns no horse-weight slots when the race start timestamp is invalid", () => {
  const slots = enumerateRaceScheduleSlots({ ...narRace, raceStartAt: "not-a-date" });
  expect(slots.filter((slot) => slot.kind === "horse-weight")).toStrictEqual([]);
});

it("returns no paddock slots for a jra race with an invalid race start timestamp", () => {
  const slots = enumerateRaceScheduleSlots({ ...jraRace, raceStartAt: "not-a-date" });
  expect(slots.filter((slot) => slot.kind === "paddock")).toStrictEqual([]);
});

it("returns no result slots when the race start timestamp is invalid", () => {
  const slots = enumerateRaceScheduleSlots({ ...narRace, raceStartAt: "not-a-date" });
  expect(slots.filter((slot) => slot.kind === "result")).toStrictEqual([]);
});

it("returns no trend cache warm slots when the race start timestamp is invalid", () => {
  const slots = enumerateRaceScheduleSlots({ ...narRace, raceStartAt: "not-a-date" });
  expect(slots.filter((slot) => slot.kind === "trend-cache-warm")).toStrictEqual([]);
});

it("returns slots sorted by scheduledAt then by race start time", () => {
  const earlierRace: RaceScheduleSourceRace = {
    keibajoCode: "42",
    raceStartAt: "2026-05-17T11:30:00+09:00",
    source: "nar",
  };
  const slots = buildSortedRaceScheduleSlots([narRace, earlierRace]);
  const sortedAt = slots.map((slot) => slot.scheduledAt);
  const expectedSorted = [...sortedAt].toSorted();
  expect(sortedAt).toStrictEqual(expectedSorted);
});
