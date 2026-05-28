// Run with: bunx vitest run src/lib/win5/race-joho.test.ts
import { expect, test } from "vitest";

import { buildRaceJoho, buildWin5LegsFromRaceJoho, parseRaceJoho } from "./race-joho";

test("parseRaceJoho returns null for null input", () => {
  expect(parseRaceJoho(null)).toBeNull();
});

test("parseRaceJoho returns null for undefined input", () => {
  expect(parseRaceJoho(undefined)).toBeNull();
});

test("parseRaceJoho returns null when the value is not 8 digits", () => {
  expect(parseRaceJoho("12345")).toBeNull();
});

test("parseRaceJoho returns null when the value contains non-digit characters", () => {
  expect(parseRaceJoho("0501010aA")).toBeNull();
});

test("parseRaceJoho slices the 8-digit race id into the four components", () => {
  expect(parseRaceJoho("05010111")).toStrictEqual({
    keibajoCode: "05",
    kaisaiKai: "01",
    kaisaiNichime: "01",
    raceBango: "11",
  });
});

test("parseRaceJoho trims surrounding whitespace before validating", () => {
  expect(parseRaceJoho("  05010111  ")).toStrictEqual({
    keibajoCode: "05",
    kaisaiKai: "01",
    kaisaiNichime: "01",
    raceBango: "11",
  });
});

test("buildRaceJoho joins the leg fields into a canonical 8-character race id", () => {
  expect(
    buildRaceJoho({
      keibajoCode: "05",
      kaisaiKai: "01",
      kaisaiNichime: "01",
      raceBango: "11",
    }),
  ).toStrictEqual("05010111");
});

test("buildRaceJoho zero-pads single-digit raceBango", () => {
  expect(
    buildRaceJoho({
      keibajoCode: "05",
      kaisaiKai: "01",
      kaisaiNichime: "01",
      raceBango: "1",
    }),
  ).toStrictEqual("05010101");
});

test("buildWin5LegsFromRaceJoho skips invalid entries while indexing the survivors", () => {
  const legs = buildWin5LegsFromRaceJoho(["05010101", null, "06010102", "bad", "06010103"]);
  expect(legs).toStrictEqual([
    { legIndex: 1, keibajoCode: "05", kaisaiKai: "01", kaisaiNichime: "01", raceBango: "1" },
    { legIndex: 3, keibajoCode: "06", kaisaiKai: "01", kaisaiNichime: "01", raceBango: "2" },
    { legIndex: 5, keibajoCode: "06", kaisaiKai: "01", kaisaiNichime: "01", raceBango: "3" },
  ]);
});

test("buildWin5LegsFromRaceJoho preserves a literal raceBango of all zeros", () => {
  const legs = buildWin5LegsFromRaceJoho(["05010100"]);
  expect(legs).toStrictEqual([
    { legIndex: 1, keibajoCode: "05", kaisaiKai: "01", kaisaiNichime: "01", raceBango: "00" },
  ]);
});

test("buildWin5LegsFromRaceJoho returns an empty array when every entry is invalid", () => {
  expect(buildWin5LegsFromRaceJoho([null, undefined, "bad"])).toStrictEqual([]);
});
