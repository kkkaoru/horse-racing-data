// Run with bun.

import { expect, test } from "vitest";
import {
  assertBeforeRaceStartDeadline,
  isBeforeRaceStartDeadline,
  RaceDeadlineExceededError,
} from "./race-deadline";

test("race deadline remains open before the published race start", () => {
  expect(
    isBeforeRaceStartDeadline({
      nowMs: Date.parse("2026-08-24T14:59:59+09:00"),
      raceStartAtJst: "2026-08-24T15:00:00+09:00",
    }),
  ).toBe(true);
});

test("race deadline closes exactly at the published race start", () => {
  expect(
    isBeforeRaceStartDeadline({
      nowMs: Date.parse("2026-08-24T15:00:00+09:00"),
      raceStartAtJst: "2026-08-24T15:00:00+09:00",
    }),
  ).toBe(false);
});

test("race deadline stays closed after the published race start", () => {
  expect(
    isBeforeRaceStartDeadline({
      nowMs: Date.parse("2026-08-24T15:00:01+09:00"),
      raceStartAtJst: "2026-08-24T15:00:00+09:00",
    }),
  ).toBe(false);
});

test("race deadline fails closed when the published race start is missing", () => {
  expect(
    isBeforeRaceStartDeadline({
      nowMs: Date.parse("2026-08-24T14:59:59+09:00"),
      raceStartAtJst: undefined,
    }),
  ).toBe(false);
});

test("race deadline fails closed when the published race start is invalid", () => {
  expect(
    isBeforeRaceStartDeadline({
      nowMs: Date.parse("2026-08-24T14:59:59+09:00"),
      raceStartAtJst: "invalid",
    }),
  ).toBe(false);
});

test("race deadline assertion throws the typed deadline error after post time", () => {
  expect(() =>
    assertBeforeRaceStartDeadline({
      nowMs: Date.parse("2026-08-24T15:00:01+09:00"),
      raceStartAtJst: "2026-08-24T15:00:00+09:00",
    }),
  ).toThrow(RaceDeadlineExceededError);
});

test("race deadline assertion permits work before post time", () => {
  expect(() =>
    assertBeforeRaceStartDeadline({
      nowMs: Date.parse("2026-08-24T14:59:59+09:00"),
      raceStartAtJst: "2026-08-24T15:00:00+09:00",
    }),
  ).not.toThrow();
});
