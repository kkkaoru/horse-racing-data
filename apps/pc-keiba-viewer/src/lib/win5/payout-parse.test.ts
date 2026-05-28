// Run with: bunx vitest run src/lib/win5/payout-parse.test.ts
import { expect, test } from "vitest";

import { parseWin5PayoutField, planCoversWinningCombination } from "./payout-parse";

test("parseWin5PayoutField returns null for null input", () => {
  expect(parseWin5PayoutField(null)).toBeNull();
});

test("parseWin5PayoutField returns null for undefined input", () => {
  expect(parseWin5PayoutField(undefined)).toBeNull();
});

test("parseWin5PayoutField returns null when shorter than the required length", () => {
  expect(parseWin5PayoutField("12345")).toBeNull();
});

test("parseWin5PayoutField returns null when the payout digits are non-numeric", () => {
  expect(parseWin5PayoutField("0102030405XXXXXXXXX")).toBeNull();
});

test("parseWin5PayoutField extracts horse numbers stripping leading zeros", () => {
  const parsed = parseWin5PayoutField("0102030405000012345");
  expect(parsed?.winningHorseNumbers).toStrictEqual(["1", "2", "3", "4", "5"]);
});

test("parseWin5PayoutField parses payout amount and ticket count", () => {
  const parsed = parseWin5PayoutField("01020304050000123450000007");
  expect(parsed?.payoutYen).toStrictEqual(12345);
  expect(parsed?.winningTicketCount).toStrictEqual(7);
});

test("parseWin5PayoutField defaults ticket count to 0 when missing", () => {
  const parsed = parseWin5PayoutField("0102030405000012345");
  expect(parsed?.winningTicketCount).toStrictEqual(0);
});

test("parseWin5PayoutField keeps a literal 00 horse number as 0", () => {
  const parsed = parseWin5PayoutField("0001020304000099999");
  expect(parsed?.winningHorseNumbers).toStrictEqual(["0", "1", "2", "3", "4"]);
});

test("parseWin5PayoutField trims surrounding whitespace before parsing", () => {
  const parsed = parseWin5PayoutField("  0102030405000099999  ");
  expect(parsed?.payoutYen).toStrictEqual(99999);
});

test("planCoversWinningCombination returns false when selection count differs from winners", () => {
  expect(planCoversWinningCombination([{ horseNumbers: ["1"] }], ["1", "2", "3", "4", "5"])).toBe(
    false,
  );
});

test("planCoversWinningCombination returns true when each leg contains the winner", () => {
  expect(
    planCoversWinningCombination(
      [
        { horseNumbers: ["01"] },
        { horseNumbers: ["02"] },
        { horseNumbers: ["03"] },
        { horseNumbers: ["04"] },
        { horseNumbers: ["05"] },
      ],
      ["1", "2", "3", "4", "5"],
    ),
  ).toBe(true);
});

test("planCoversWinningCombination returns false when one leg misses the winner", () => {
  expect(
    planCoversWinningCombination(
      [
        { horseNumbers: ["1"] },
        { horseNumbers: ["2"] },
        { horseNumbers: ["3"] },
        { horseNumbers: ["4"] },
        { horseNumbers: ["9"] },
      ],
      ["1", "2", "3", "4", "5"],
    ),
  ).toBe(false);
});

test("planCoversWinningCombination normalizes zero-padded horse numbers in selections and winners", () => {
  expect(
    planCoversWinningCombination(
      [
        { horseNumbers: ["1", "2"] },
        { horseNumbers: ["02"] },
        { horseNumbers: ["3"] },
        { horseNumbers: ["4"] },
        { horseNumbers: ["5"] },
      ],
      ["01", "02", "03", "04", "05"],
    ),
  ).toBe(true);
});
