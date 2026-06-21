// Run with bun (exercised via `bunx vitest run`).
import { expect, test } from "vitest";

import {
  BLINKER_PATTERN_LABELS,
  BLINKER_PATTERN_SHORT_LABELS,
  classifyBlinkerPattern,
  isWearingBlinker,
} from "./blinker-pattern";

test("isWearingBlinker returns true for the wearing value", () => {
  expect(isWearingBlinker("1")).toBe(true);
});

test("isWearingBlinker returns false for the not-wearing value", () => {
  expect(isWearingBlinker("0")).toBe(false);
});

test("isWearingBlinker returns false for null", () => {
  expect(isWearingBlinker(null)).toBe(false);
});

test("isWearingBlinker returns false for undefined", () => {
  expect(isWearingBlinker(undefined)).toBe(false);
});

test("isWearingBlinker returns false for a blank string", () => {
  expect(isWearingBlinker("")).toBe(false);
});

test("BLINKER_PATTERN_LABELS maps every pattern to its full Japanese label", () => {
  expect(BLINKER_PATTERN_LABELS).toStrictEqual({
    A: "初ブリンカー",
    B: "初ブリンカー(初出走)",
    C: "ブリンカー再装着",
    D: "ブリンカー再装着(3走以上ぶり)",
    E: "ブリンカー解除",
    F: "ブリンカー継続",
  });
});

test("BLINKER_PATTERN_SHORT_LABELS maps every pattern to its compact Japanese label", () => {
  expect(BLINKER_PATTERN_SHORT_LABELS).toStrictEqual({
    A: "初装着",
    B: "初装着(新馬)",
    C: "再装着",
    D: "再装着(久々)",
    E: "解除",
    F: "継続",
  });
});

test("classifyBlinkerPattern returns B when wearing now with no past races (debut)", () => {
  expect(classifyBlinkerPattern("1", [])).toBe("B");
});

test("classifyBlinkerPattern returns A when wearing now and never wore before (all zero)", () => {
  expect(classifyBlinkerPattern("1", ["0", "0", "0"])).toBe("A");
});

test("classifyBlinkerPattern returns A when wearing now and past values are null", () => {
  expect(classifyBlinkerPattern("1", [null, null])).toBe("A");
});

test("classifyBlinkerPattern returns C for a one-race gap (most recent off, prior on)", () => {
  expect(classifyBlinkerPattern("1", ["0", "1"])).toBe("C");
});

test("classifyBlinkerPattern returns C for a two-race gap", () => {
  expect(classifyBlinkerPattern("1", ["0", "0", "1"])).toBe("C");
});

test("classifyBlinkerPattern returns D for a three-race gap", () => {
  expect(classifyBlinkerPattern("1", ["0", "0", "0", "1"])).toBe("D");
});

test("classifyBlinkerPattern returns D for a four-race gap", () => {
  expect(classifyBlinkerPattern("1", ["0", "0", "0", "0", "1"])).toBe("D");
});

test("classifyBlinkerPattern returns F when wearing now and every past race wore it", () => {
  expect(classifyBlinkerPattern("1", ["1", "1"])).toBe("F");
});

test("classifyBlinkerPattern returns F when wearing now and the most recent three+ wore it", () => {
  expect(classifyBlinkerPattern("1", ["1", "1", "1", "0", "0"])).toBe("F");
});

test("classifyBlinkerPattern returns F when wearing now and only the last race wore it", () => {
  expect(classifyBlinkerPattern("1", ["1", "0", "0"])).toBe("F");
});

test("classifyBlinkerPattern returns F when wearing now and the last two races wore it", () => {
  expect(classifyBlinkerPattern("1", ["1", "1", "0", "1"])).toBe("F");
});

test("classifyBlinkerPattern returns E when not wearing now and every past race wore it", () => {
  expect(classifyBlinkerPattern("0", ["1", "1", "1"])).toBe("E");
});

test("classifyBlinkerPattern returns E for a single past wearing race when removed now", () => {
  expect(classifyBlinkerPattern("0", ["1"])).toBe("E");
});

test("classifyBlinkerPattern returns E when not wearing now and the most recent three+ wore it", () => {
  expect(classifyBlinkerPattern("0", ["1", "1", "1", "0"])).toBe("E");
});

test("classifyBlinkerPattern returns null when not wearing now and only the last two wore it", () => {
  expect(classifyBlinkerPattern("0", ["1", "1", "0"])).toBe(null);
});

test("classifyBlinkerPattern returns null when not wearing now and only some past races wore it", () => {
  expect(classifyBlinkerPattern("0", ["0", "1"])).toBe(null);
});

test("classifyBlinkerPattern returns null when not wearing now with no past races (debut)", () => {
  expect(classifyBlinkerPattern("0", [])).toBe(null);
});

test("classifyBlinkerPattern returns null when not wearing now and never wore before", () => {
  expect(classifyBlinkerPattern("0", ["0", "0"])).toBe(null);
});

test("classifyBlinkerPattern returns null when current is null and never wore before", () => {
  expect(classifyBlinkerPattern(null, ["0", "0"])).toBe(null);
});

test("classifyBlinkerPattern returns E when current is null and every past race wore it", () => {
  expect(classifyBlinkerPattern(null, ["1", "1"])).toBe("E");
});

test("classifyBlinkerPattern treats a null past value as not wearing inside the gap count", () => {
  expect(classifyBlinkerPattern("1", [null, "1"])).toBe("C");
});
